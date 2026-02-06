from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Dict, Iterator, Optional


from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


@dataclass(frozen=True)
class SsmTunnelConfig:
    target: str
    remote_host: str
    remote_port: int
    local_host: str
    local_port: int
    region: Optional[str] = None
    profile: Optional[str] = None


def _load_dotenv_file(path: Path) -> None:
    """
    Minimal .env loader so SSM works even when python-dotenv isn't installed.
    - Only sets keys that are not already in os.environ
    - Ignores comments/blank lines
    - Supports optional surrounding quotes
    """
    if load_dotenv is not None:
        load_dotenv(path)
        return

    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def _int_or(default: int, value: Optional[str]) -> int:
    try:
        return int(str(value)) if value is not None else int(default)
    except Exception:
        return int(default)


def _wait_for_port(host: str, port: int, *, timeout_s: float = 20.0) -> bool:
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=1.0):
                return True
        except Exception:
            time.sleep(0.2)
    return False


def _can_bind_local_port(port: int, host: str = "127.0.0.1") -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, int(port)))
            return True
    except Exception:
        return False


def _find_free_local_port(preferred: int, *, host: str = "127.0.0.1", max_tries: int = 50) -> int:
    preferred = int(preferred)
    if _can_bind_local_port(preferred, host=host):
        return preferred
    for i in range(1, max_tries + 1):
        cand = preferred + i
        if _can_bind_local_port(cand, host=host):
            return cand
    return preferred


def _aws_cli_available() -> bool:
    return shutil.which("aws") is not None


def _session_manager_plugin_available() -> bool:
    return shutil.which("session-manager-plugin") is not None


def _pg_isready_available() -> bool:
    return shutil.which("pg_isready") is not None


def _check_postgres_responding(host: str, port: int, *, timeout_s: float = 4.0) -> tuple[bool, str]:
    """
    Uses `pg_isready` (no credentials required) to verify the TCP forward actually reaches Postgres.
    Returns (ok, output).
    """
    if not _pg_isready_available():
        return True, ""
    try:
        res = subprocess.run(
            ["pg_isready", "-h", host, "-p", str(int(port))],
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_s)),
        )
        out = ((res.stdout or "") + (res.stderr or "")).strip()
        # exit codes: 0 accepting, 1 rejecting (still a response), 2 no response, 3 no attempt
        ok = res.returncode in (0, 1)
        return ok, out
    except Exception as e:
        return False, repr(e)


def _build_ssm_command(cfg: SsmTunnelConfig) -> list[str]:
    cmd = ["aws"]
    if cfg.profile:
        cmd += ["--profile", cfg.profile]
    if cfg.region:
        cmd += ["--region", cfg.region]

    # SSM expects StringList values for these parameters; JSON is the most robust.
    params = {
        "host": [cfg.remote_host],
        "portNumber": [str(cfg.remote_port)],
        "localPortNumber": [str(cfg.local_port)],
    }
    cmd += [
        "ssm",
        "start-session",
        "--target",
        cfg.target,
        "--document-name",
        "AWS-StartPortForwardingSessionToRemoteHost",
        "--parameters",
        json.dumps(params),
    ]
    return cmd


def _tunnel_config_from_env(cfg: Dict[str, object]) -> Optional[SsmTunnelConfig]:
    # Try loading .env.aws if available to ensure SSM vars are present
    env_aws = Path("infrastructure/.env.aws")
    if env_aws.exists():
        _load_dotenv_file(env_aws)

    settings = cfg.get("settings") if isinstance(cfg, dict) else None
    if not isinstance(settings, dict):
        return None
    if (settings.get("db_mode") or "").strip().lower() != "aws":
        return None

    overrides = settings.get("db_overrides") if isinstance(settings.get("db_overrides"), dict) else {}
    aws_overrides = overrides.get("aws") if isinstance(overrides.get("aws"), dict) else {}

    local_host = str(aws_overrides.get("host") or os.getenv("SSM_LOCAL_HOST") or "localhost")
    local_port = _int_or(55432, str(aws_overrides.get("port") or os.getenv("SSM_LOCAL_PORT") or "55432"))

    target = os.getenv("SSM_TARGET")
    remote_host = os.getenv("SSM_HOST")
    if not target or not remote_host:
        raise RuntimeError("AWS SSM config missing SSM_TARGET or SSM_HOST env vars.")

    # SSM port-forwarding needs the remote database port (almost always 5432).
    # Do NOT use `AWS_POSTGRES_PORT` here; in this repo it's used for local docker port mapping.
    remote_port = _int_or(5432, os.getenv("SSM_REMOTE_PORT"))
    region = os.getenv("AWS_REGION") or None
    profile = os.getenv("AWS_PROFILE") or None

    return SsmTunnelConfig(
        target=str(target),
        remote_host=str(remote_host),
        remote_port=int(remote_port),
        local_host=local_host,
        local_port=int(local_port),
        region=region,
        profile=profile,
    )


@contextmanager
def _env_overrides(values: Dict[str, str]) -> Iterator[None]:
    prev = {k: os.environ.get(k) for k in values}
    for k, v in values.items():
        os.environ[k] = v
    try:
        yield
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class _ProcOutputTail:
    def __init__(self, max_lines: int = 200) -> None:
        self._lines: deque[str] = deque(maxlen=max_lines)
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []

    def start(self, proc: subprocess.Popen[str]) -> None:
        for label, stream in (("stdout", proc.stdout), ("stderr", proc.stderr)):
            if stream is None:
                continue
            t = threading.Thread(target=self._drain, args=(label, stream), daemon=True)
            t.start()
            self._threads.append(t)

    def _drain(self, label: str, stream) -> None:
        try:
            for line in stream:
                cleaned = (line or "").rstrip("\n")
                if not cleaned:
                    continue
                with self._lock:
                    self._lines.append(f"[{label}] {cleaned}")
        except Exception:
            return

    def tail(self) -> str:
        with self._lock:
            return "\n".join(self._lines)


@contextmanager
def maybe_open_ssm_tunnel(cfg: Dict[str, object]) -> Iterator[None]:
    tcfg = _tunnel_config_from_env(cfg)
    if tcfg is None:
        yield
        return

    if not _aws_cli_available():
        raise RuntimeError("AWS CLI not found. Install awscli and session-manager-plugin to use SSM tunnels.")
    if not _session_manager_plugin_available():
        raise RuntimeError("session-manager-plugin not found. Install it to use `aws ssm start-session` port forwarding.")

    from rich.console import Console
    console = Console()

    # session-manager-plugin binds the local port on 127.0.0.1 (IPv4).
    # Using "localhost" can resolve to ::1 first on some systems, so force IPv4.
    connect_host = "127.0.0.1"

    # If the preferred local port is already taken, pick a nearby free one for this session.
    chosen_port = _find_free_local_port(tcfg.local_port, host="127.0.0.1")
    if chosen_port != tcfg.local_port:
        console.print(f"[yellow]SSM local port {tcfg.local_port} is busy; using {chosen_port} for this session.[/yellow]")
        tcfg = replace(tcfg, local_port=chosen_port)

    console.print(f"[dim]Opening AWS SSM tunnel to {tcfg.remote_host}:{tcfg.remote_port} via {tcfg.target}...[/dim]")

    cmd = _build_ssm_command(tcfg)
    tail = _ProcOutputTail(max_lines=250)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        env={**os.environ, "AWS_PAGER": ""},
        start_new_session=True,
    )
    tail.start(proc)

    try:
        if not _wait_for_port(connect_host, tcfg.local_port, timeout_s=25.0):
            if proc.poll() is not None:
                raise RuntimeError(f"SSM tunnel failed to start.\n{tail.tail()}")
            raise RuntimeError("SSM tunnel did not become ready before timeout.")

        ok, isready_out = _check_postgres_responding(connect_host, tcfg.local_port, timeout_s=4.0)
        if not ok:
            raise RuntimeError(
                "SSM port-forward is listening, but Postgres is not responding through the tunnel.\n"
                f"{isready_out or '(no output)'}\n\n"
                "This usually means the SSM target instance cannot reach "
                f"{tcfg.remote_host}:{tcfg.remote_port} (security group / routing / DNS / NACL), "
                "or the remote host/port is wrong.\n\n"
                f"{tail.tail()}"
            )

        overrides = {
            "DESTINATION__POSTGRES__CREDENTIALS__HOST": connect_host,
            "DESTINATION__POSTGRES__CREDENTIALS__PORT": str(tcfg.local_port),
            "POSTGRES_HOST_OVERRIDE": connect_host,
            "POSTGRES_PORT_OVERRIDE": str(tcfg.local_port),
        }
        with _env_overrides(overrides):
            yield
    finally:
        if proc.poll() is None:
            # Terminate the whole process group to avoid orphaned session-manager-plugin processes.
            try:
                import signal

                os.killpg(proc.pid, signal.SIGTERM)
            except Exception:
                proc.terminate()

            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    import signal

                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    proc.kill()
                proc.wait(timeout=5)
