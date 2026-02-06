from __future__ import annotations

import os
import socket
import subprocess
import time
from typing import List, Optional

import questionary
from rich.console import Console
from rich.panel import Panel

from .constants import DOCKER_COMPOSE_TEMPLATE
from .postgres_ops import test_postgres_connection

console = Console()


def check_docker_installed() -> bool:
    try:
        subprocess.run(["docker", "--version"], capture_output=True, check=True)
        return True
    except Exception:
        return False


def check_docker_running() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True)
        return True
    except Exception:
        return False


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _find_free_port(preferred: int, max_tries: int = 20) -> int:
    if not _port_in_use(preferred):
        return preferred
    for i in range(1, max_tries + 1):
        candidate = preferred + i
        if not _port_in_use(candidate):
            return candidate
    return preferred


def _compose_cmd() -> List[str]:
    try:
        subprocess.run(["docker", "compose", "version"], capture_output=True, check=True)
        return ["docker", "compose"]
    except Exception:
        return ["docker-compose"]


def init_docker_environment() -> None:
    console.clear()
    console.print(Panel("[bold green]Docker Postgres Setup[/bold green]"))

    if not check_docker_installed():
        console.print("[red]❌ Docker not found. Install Docker Desktop first.[/red]")
        input("\nPress Enter...")
        return
    if not check_docker_running():
        console.print("[red]❌ Docker daemon not running.[/red]")
        input("\nPress Enter...")
        return

    compose_file = "docker-compose.yml"
    if os.path.exists(compose_file):
        if not questionary.confirm("Overwrite existing docker-compose.yml and .env?").ask():
            return

    preferred_port = 5432
    free_port = _find_free_port(preferred_port)
    if free_port != preferred_port:
        console.print(f"[yellow]Port {preferred_port} is in use. Using {free_port} instead.[/yellow]")

    env_vars = {
        "POSTGRES_USER": questionary.text("Postgres User:", default="dlt_user").ask() or "dlt_user",
        "POSTGRES_PASSWORD": questionary.password("Password:", default="dlt_secure_password").ask() or "dlt_secure_password",
        "POSTGRES_DB": questionary.text("Database Name:", default="dlt_db").ask() or "dlt_db",
        "POSTGRES_PORT": questionary.text("Host Port:", default=str(free_port)).ask() or str(free_port),
    }

    with open(compose_file, "w", encoding="utf-8") as f:
        f.write(DOCKER_COMPOSE_TEMPLATE)

    with open(".env", "w", encoding="utf-8") as f:
        for k, v in env_vars.items():
            f.write(f"{k}={v}\n")

    console.print("[green]✅ docker-compose.yml + .env created[/green]")
    console.print("[dim]Next: choose “Start Docker Services” from the menu.[/dim]")
    input("\nPress Enter...")


def _container_id_for_service(service_name: str = "postgres") -> Optional[str]:
    cmd = _compose_cmd() + ["ps", "-q", service_name]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        cid = (res.stdout or "").strip()
        return cid or None
    except Exception:
        return None


def _wait_for_health(container_id: str, timeout_seconds: int = 120) -> bool:
    start = time.time()
    while time.time() - start < timeout_seconds:
        try:
            res = subprocess.run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", container_id],
                capture_output=True,
                text=True,
                check=True,
            )
            status = (res.stdout or "").strip()
            if status == "healthy":
                return True
            if status == "unhealthy":
                return False
        except Exception:
            pass
        time.sleep(2)
    return False


def start_docker_services() -> None:
    if not os.path.exists("docker-compose.yml"):
        console.print("[yellow]No docker-compose.yml found. Configure Docker first.[yellow]")
        input("\nPress Enter...")
        return

    console.print("[yellow]Starting Docker services...[/yellow]")
    cmd = _compose_cmd() + ["up", "-d"]
    try:
        subprocess.run(cmd, check=True)
        console.print("[green]✅ Services started. Waiting for DB health...[/green]")

        cid = _container_id_for_service("postgres")
        if cid:
            ok = _wait_for_health(cid, timeout_seconds=120)
            console.print("[green]✅ Database Healthy[/green]" if ok else "[red]❌ Database unhealthy/timeouts[/red]")
        else:
            console.print("[yellow]Could not determine container id; falling back to connection test.[/yellow]")
            for _ in range(20):
                if test_postgres_connection(cache_seconds=0):
                    console.print("[green]✅ Database Ready[/green]")
                    break
                time.sleep(2)

    except Exception as e:
        console.print(f"[red]Error starting Docker: {e}[/red]")

    input("\nPress Enter...")
