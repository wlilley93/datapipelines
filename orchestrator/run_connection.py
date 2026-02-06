# orchestrator/run_connection.py
from __future__ import annotations

import json
import logging
import os
import threading
import time
import traceback
import uuid
import warnings
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import questionary
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from connectors.runtime.events import RuntimeEvent, set_emitter
from connectors.runtime.loader import load as load_connector
from connectors.runtime.schema_drift import DriftPolicy, check_and_record_drift
from connectors.runtime.state import merge_state, normalise_state
from connectors.xero.auth import exchange_code_for_tokens
from connectors.xero.errors import InteractiveAuthRequired
from orchestrator.config import list_connections, sanitize_schema, save_config_v2
from orchestrator.run_logs import write_run_log
from orchestrator.secrets import get_secret, update_secret
from orchestrator.ui import fmt_seconds, format_event_line, render_api_error_panel
from orchestrator.ssm_tunnel import maybe_open_ssm_tunnel


# Helper to parse code from redirect URL
def _parse_code_from_redirect(redirect_url: str) -> Optional[str]:
    try:
        parsed = urlparse(redirect_url)
        qs = parse_qs(parsed.query)
        code_vals = qs.get("code")
        if code_vals and isinstance(code_vals, list):
            return code_vals[0]
    except Exception:
        return None
    return None


def _ensure_xero_creds_present(name: str, creds: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prompt for missing core Xero credentials so we can build the auth URL.
    """
    changed = False

    def _need(key: str, prompt: str) -> None:
        nonlocal changed
        existing = creds.get(key)
        if key == "redirect_uri" and not existing:
            existing = creds.get("redirectUri")
        if existing:
            if key == "redirect_uri":
                creds["redirect_uri"] = str(existing)
                creds.pop("redirectUri", None)
            return
        val = questionary.text(prompt).ask()
        if val:
            creds[key] = val.strip()
            if key == "redirect_uri":
                creds.pop("redirectUri", None)
            changed = True

    _need("client_id", "Enter Xero client_id:")
    _need("client_secret", "Enter Xero client_secret:")
    _need("redirect_uri", "Enter Xero redirect URI:")

    if changed:
        # update_secret merges by design in this codebase.
        update_secret(name, creds)

    return creds


ORIGIN = "cli"
console = Console()

# =============================================================================
# GLOBAL JSONL UI LOG (append-only)
# =============================================================================


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _default_ui_log_path(run_id: str, connection_name: str) -> str:
    base = os.getenv("UI_LOG_DIR", "run_logs")
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dir_path = os.path.join(base, day)
    _ensure_dir(dir_path)
    timestamp = datetime.now(timezone.utc).strftime("%H:%M")
    fname = f"{timestamp}_{connection_name}_{run_id}_events.jsonl"
    return os.path.join(dir_path, fname)


class UiEventLogger:
    def __init__(
        self, path: str, *, run_id: str, connection_name: str, connection_type: str
    ) -> None:
        self.path = path
        self.run_id = run_id
        self.connection_name = connection_name
        self.connection_type = connection_type
        self._lock = threading.Lock()
        self._last_status_at = 0.0
        self._last_status_line = ""

    def _base(self) -> Dict[str, Any]:
        return {
            "ts": _now_iso(),
            "run_id": self.run_id,
            "connection_name": self.connection_name,
            "connection_type": self.connection_type,
            "origin": ORIGIN,
        }

    def write(self, kind: str, payload: Dict[str, Any]) -> None:
        rec = self._base()
        rec["kind"] = kind
        rec.update(payload)
        line = json.dumps(rec, ensure_ascii=False)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def status(
        self, *, line: str, level: str, stream: str, throttle_s: float = 0.15
    ) -> None:
        now = time.monotonic()
        if line == self._last_status_line and (now - self._last_status_at) < (
            throttle_s * 3
        ):
            return
        if (now - self._last_status_at) < throttle_s:
            return
        self._last_status_at = now
        self._last_status_line = line
        self.write("ui.status", {"level": level, "stream": stream, "line": line})


def _safe_json(v: Any) -> Any:
    try:
        json.dumps(v)
        return v
    except Exception:
        return str(v)


def _event_to_dict(ev: RuntimeEvent) -> Dict[str, Any]:
    fields = ev.fields or {}
    return {
        "type": ev.type,
        "level": ev.level,
        "message": ev.message,
        "stream": ev.stream,
        "count": ev.count,
        "origin": ORIGIN,
        "fields": {k: _safe_json(v) for k, v in fields.items()},
    }


def _dump_all_thread_stacks(reason: str) -> str:
    import sys

    frames = sys._current_frames()
    parts: List[str] = [f"=== THREAD DUMP ({reason}) ==="]
    for th in threading.enumerate():
        ident = th.ident
        parts.append(f"\n--- Thread: {th.name} (ident={ident}) ---")
        frame = frames.get(ident)
        if frame is None:
            parts.append("  <no frame>")
            continue
        parts.append("".join(traceback.format_stack(frame)))
    return "\n".join(parts)


class JsonlLoggingHandler(logging.Handler):
    def __init__(self, ui: UiEventLogger) -> None:
        super().__init__()
        self.ui = ui

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:
            msg = str(record.msg)

        self.ui.write(
            "python.log",
            {
                "logger": record.name,
                "level": record.levelname,
                "message": msg,
                "pathname": record.pathname,
                "lineno": record.lineno,
                "funcName": record.funcName,
            },
        )


@contextmanager
def capture_logs_and_warnings(ui: UiEventLogger) -> Any:
    handler = JsonlLoggingHandler(ui)
    handler.setLevel(logging.INFO)

    root = logging.getLogger()
    old_level = root.level
    root.setLevel(
        min(old_level, logging.INFO) if isinstance(old_level, int) else logging.INFO
    )
    root.addHandler(handler)

    old_showwarning = warnings.showwarning

    def _showwarning(message, category, filename, lineno, file=None, line=None):
        ui.write(
            "python.warning",
            {
                "category": getattr(category, "__name__", str(category)),
                "message": str(message),
                "filename": filename,
                "lineno": lineno,
                "line": line,
            },
        )
        return old_showwarning(
            message, category, filename, lineno, file=file, line=line
        )

    warnings.showwarning = _showwarning  # type: ignore

    try:
        yield
    finally:
        warnings.showwarning = old_showwarning  # type: ignore
        root.removeHandler(handler)
        root.setLevel(old_level)


def _drift_policy_from_state(state: Dict[str, Any]) -> DriftPolicy:
    pol = (
        ((state.get("global") or {}).get("schema_drift_policy") or "warn")
        .strip()
        .lower()
    )
    if pol in ("allow", "warn", "fail"):
        return DriftPolicy(pol)
    return DriftPolicy.WARN


def _safe_url_hint(url: str) -> str:
    try:
        p = urlparse(url)
        host = p.netloc or ""
        path = p.path or ""
        hint = f"{host}{path}"
        return hint if hint else url
    except Exception:
        return url


def _status_from_event(ev: RuntimeEvent) -> str:
    msg = str(ev.message or "")
    stream = ev.stream or "default"
    fields = ev.fields or {}

    if msg.startswith("http.request.start"):
        method = fields.get("method") or "GET"
        url = fields.get("url")
        if isinstance(url, str) and url:
            return f"[{stream}] {method} {_safe_url_hint(url)}"
        return f"[{stream}] {method} request…"

    if msg.startswith("http.request.ok"):
        elapsed_ms = fields.get("elapsed_ms")
        items = fields.get("items_count")
        keys = fields.get("keys_count")
        bits: List[str] = []
        if isinstance(items, int):
            bits.append(f"{items} items")
        elif isinstance(keys, int):
            bits.append(f"{keys} keys")
        if isinstance(elapsed_ms, int):
            bits.append(f"{elapsed_ms}ms")
        suffix = f" ({', '.join(bits)})" if bits else ""
        return f"[{stream}] response ok{suffix}"

    if msg.startswith("http.request.error"):
        return f"[{stream}] request failed"

    if msg.startswith("paging.page.start"):
        page = fields.get("page")
        limit = fields.get("limit")
        if isinstance(page, int) and isinstance(limit, int):
            return f"[{stream}] paging page {page} (limit {limit})…"
        if isinstance(page, int):
            return f"[{stream}] paging page {page}…"
        return f"[{stream}] paging…"

    if msg.startswith("paging.done.last_page"):
        returned = fields.get("returned")
        if isinstance(returned, int):
            return f"[{stream}] paging done ({returned} last page)"
        return f"[{stream}] paging done"

    formatted = format_event_line(ev, include_level=False)
    formatted = (formatted or "").strip()
    if formatted:
        return formatted[:140] + ("…" if len(formatted) > 140 else "")

    return f"[{stream}] working…"


def _extract_schema_diff(ev: RuntimeEvent) -> Optional[Tuple[int, int]]:
    try:
        if str(ev.message or "") != "schema.diff":
            return None
        fields = ev.fields or {}
        added = fields.get("added_count")
        missing = fields.get("missing_count")
        if isinstance(added, int) and isinstance(missing, int):
            return added, missing
        if (
            isinstance(added, str)
            and added.isdigit()
            and isinstance(missing, str)
            and missing.isdigit()
        ):
            return int(added), int(missing)
        return None
    except Exception:
        return None


def _extract_schema_records_seen(ev: RuntimeEvent) -> Optional[int]:
    if ev.type != "count":
        return None
    if str(ev.message or "") != "schema.records_seen":
        return None
    return ev.count if isinstance(ev.count, int) else None


def _rows_inserted_from_stats(stats: Dict[str, Any], default: int) -> int:
    if not isinstance(stats, dict):
        return default

    def _first_numeric(
        keys: Tuple[str, ...], container: Dict[str, Any]
    ) -> Optional[int]:
        for k in keys:
            val = container.get(k)
            if isinstance(val, (int, float)):
                return int(val)
        return None

    top_level = _first_numeric(("rows_inserted", "inserted_rows", "rows_loaded"), stats)
    if top_level is not None:
        return top_level

    for nested_key in ("load", "load_counts", "load_stats"):
        nested = stats.get(nested_key)
        if isinstance(nested, dict):
            nested_val = _first_numeric(
                ("rows_inserted", "inserted_rows", "rows_loaded"), nested
            )
            if nested_val is not None:
                return nested_val

    return default


def _bookmark_snapshot(
    state: Dict[str, Any], keys: Optional[List[str]] = None
) -> Dict[str, Any]:
    g = state.get("global")
    if not isinstance(g, dict):
        return {}
    b = g.get("bookmarks")
    if not isinstance(b, dict):
        return {}
    if not keys:
        keys = [
            "companies",
            "deals",
            "emails",
            "meetings",
            "tasks",
            "products",
            "line_items",
        ]
    return {k: b.get(k) for k in keys}


def run_connection(
    cfg, conn: Dict[str, Any], *, retries: int = 2, pause_on_exit: bool = False
) -> Optional[Dict[str, Any]]:
    console.clear()
    name = conn["name"]
    ctype = conn["type"]
    schema = sanitize_schema(conn.get("schema") or name)

    state = normalise_state(conn.get("state") or {})

    drift_policy = _drift_policy_from_state(state)

    run_id = str(uuid.uuid4())
    ui_log_path = os.getenv("UI_EVENT_LOG_PATH", _default_ui_log_path(run_id, name))
    ui = UiEventLogger(
        path=ui_log_path, run_id=run_id, connection_name=name, connection_type=ctype
    )

    started_at = _now_iso()
    started_mono = time.monotonic()

    attempt = 0
    last_err: Optional[str] = None
    report_text = ""
    refreshed_creds: Optional[Dict[str, Any]] = None
    state_updates: Dict[str, Any] = {}

    spinner_line = ""
    spinner_level = "info"
    total_records = 0
    active_progress: Optional[Tuple[Progress, Any]] = None
    active_live: Optional[Live] = None
    last_event_mono = 0.0
    schema_diff_counts: Dict[str, Dict[str, int]] = {}
    schema_records_seen: Dict[str, int] = {}
    watchdog_stop = threading.Event()
    stream_counts: Dict[str, int] = {}
    result_stats: Dict[str, Any] = {}
    drift_summary: Optional[Dict[str, Any]] = None

    # Create log file if not exists
    log_dir = os.path.dirname(ui_log_path)
    os.makedirs(log_dir, exist_ok=True)

    ui.write(
        "run.start", {"schema": schema, "retries": retries, "started_at": started_at}
    )
    ui.write("state.debug.before_run", {"bookmarks": _bookmark_snapshot(state)})

    def header_panel() -> Panel:
        elapsed = fmt_seconds(time.monotonic() - started_mono)
        db_mode = cfg.get("settings", {}).get("db_mode", "local")
        target_label = "AWS RDS" if db_mode == "aws" else "Local Postgres"

        title = f"[bold blue]Syncing: {name}[/bold blue]  [dim]⏱ {elapsed}[/dim]"
        status_line = f"[dim]{ctype} → {target_label} (schema: {schema})[/dim]"

        # ✅ Always show an integer (0 is meaningful). Removes the confusing "Starting…" state.
        total_line = (
            f"[dim]Total records (reported):[/dim] [bold]{total_records}[/bold]"
        )

        body = f"{status_line}\n{total_line}"
        return Panel(
            body, border_style="blue", expand=True, padding=(1, 2), title=title
        )

    def render_group(progress: Progress) -> Group:
        return Group(header_panel(), progress)

    def cli_emitter(ev: RuntimeEvent) -> None:
        nonlocal \
            spinner_line, \
            spinner_level, \
            total_records, \
            active_progress, \
            active_live, \
            last_event_mono
        nonlocal schema_diff_counts, schema_records_seen

        last_event_mono = time.monotonic()

        stream = ev.stream or "default"

        ui.write("runtime.event", _event_to_dict(ev))

        if ev.type in ("count", "records", "rows") and isinstance(ev.count, int):
            stream_counts[stream] = int(ev.count)
            total_records = sum(
                int(v) for v in stream_counts.values() if isinstance(v, int)
            )

        diff = _extract_schema_diff(ev)
        if diff is not None:
            added, missing = diff
            schema_diff_counts[stream] = {"added": int(added), "missing": int(missing)}

        seen = _extract_schema_records_seen(ev)
        if seen is not None:
            schema_records_seen[stream] = int(seen)

        spinner_level = (ev.level or "info").lower()
        spinner_line = _status_from_event(ev)
        ui.status(line=spinner_line, level=spinner_level, stream=stream)

        if active_progress is not None:
            progress, task_id = active_progress
            progress.update(task_id, description=spinner_line)

        if active_live is not None and active_progress is not None:
            progress, _ = active_progress
            active_live.update(render_group(progress), refresh=True)

    state_before_run = deepcopy(state)

    try:
        with maybe_open_ssm_tunnel(cfg):
            with capture_logs_and_warnings(ui):
                while attempt <= retries:
                    attempt += 1
                    ui.write(
                        "attempt.start",
                        {"attempt": attempt, "max_attempts": retries + 1},
                    )
                    ui.write(
                        "state.debug.before_attempt",
                        {"attempt": attempt, "bookmarks": _bookmark_snapshot(state)},
                    )

                    try:
                        connector = load_connector(ctype)
                        creds = get_secret(name)
                        if ctype == "xero":
                            creds = _ensure_xero_creds_present(name, creds)

                        set_emitter(cli_emitter)

                        spinner = Progress(
                            SpinnerColumn(),
                            TextColumn("{task.description}"),
                            transient=True,
                            refresh_per_second=16,
                        )

                        with spinner as progress:
                            task_id = progress.add_task(
                                f"Starting (attempt {attempt}/{retries + 1})…",
                                total=None,
                            )
                            active_progress = (progress, task_id)

                            with Live(
                                render_group(progress),
                                console=console,
                                refresh_per_second=8,
                                transient=True,
                            ) as live:
                                active_live = live

                                ReadSelection = getattr(
                                    __import__(
                                        "connectors.runtime.protocol",
                                        fromlist=["ReadSelection"],
                                    ),
                                    "ReadSelection",
                                )

                                conn_selection = conn.get("selection")
                                streams_to_select = None
                                if isinstance(conn_selection, list):
                                    streams_to_select = [str(s) for s in conn_selection]

                                ui.write("connector.read.start", {"attempt": attempt})
                                result = connector.read(
                                    creds=creds,
                                    schema=schema,
                                    selection=ReadSelection(streams=streams_to_select),
                                    state=state,
                                )
                                ui.write("connector.read.done", {"attempt": attempt})

                        set_emitter(None)
                        active_progress = None
                        active_live = None

                        report_text = (result.report_text or "").strip()
                        refreshed_creds = result.refreshed_creds
                        state_updates = result.state_updates or {}
                        result_stats = result.stats or {}

                        state = merge_state(state, state_updates)
                        state["global"]["last_success_at"] = _now_iso()
                        state["global"]["last_run_at"] = state["global"][
                            "last_success_at"
                        ]

                        conn["state"] = state
                        conn["last_run_at"] = state["global"]["last_run_at"]
                        save_config_v2(cfg)

                        ui.write(
                            "state.debug.after_success_merge",
                            {"bookmarks": _bookmark_snapshot(state)},
                        )

                        if (
                            isinstance(result.observed_schema, dict)
                            and result.observed_schema
                        ):
                            drift_summary = check_and_record_drift(
                                connection_name=name,
                                observed_schema=result.observed_schema,
                                policy=drift_policy,
                            )

                        last_err = None
                        ui.write("attempt.success", {"attempt": attempt})
                        break

                    except InteractiveAuthRequired as e:
                        set_emitter(None)
                        active_progress = None

                        if active_live:
                            active_live.stop()
                            active_live = None

                        console.print(
                            Panel(
                                f"[bold yellow]Xero Authorization Required[/bold yellow]\n\n"
                                f"Your Xero connection '{name}' requires re-authorization.\n"
                                f"Please [link={e.auth_url}]click here[/link] to open the Xero consent page.\n\n"
                                f"After authorizing, copy the full redirect URL (or just the code parameter).\n",
                                title="[red]ACTION REQUIRED![/red]",
                                border_style="yellow",
                            )
                        )

                        user_response = questionary.text(
                            "Paste the full redirect URL or the authorization code here:"
                        ).ask()

                        if not user_response:
                            console.print(
                                "[red]Authorization cancelled or no input provided. Retrying...[/red]"
                            )
                            last_err = "Authorization cancelled or no input provided."
                            ui.write(
                                "attempt.error",
                                {
                                    "attempt": attempt,
                                    "error": last_err,
                                    "traceback": "User cancelled interactive auth.",
                                },
                            )
                            continue

                        parsed_code = _parse_code_from_redirect(user_response)
                        code = parsed_code if parsed_code else user_response

                        try:
                            xero_creds = get_secret(name)

                            client_id = xero_creds["client_id"]
                            client_secret = xero_creds["client_secret"]
                            redirect_uri = xero_creds.get(
                                "redirect_uri"
                            ) or xero_creds.get("redirectUri")

                            new_xero_tokens = exchange_code_for_tokens(
                                client_id=client_id,
                                client_secret=client_secret,
                                code=code,
                                redirect_uri=str(redirect_uri),
                            )

                            # update_secret merges; safe to call directly (no merge kwarg).
                            update_secret(name, new_xero_tokens)

                            console.print(
                                "[green]Authentication successful! Resuming pipeline...[/green]"
                            )
                            attempt = 0
                            continue

                        except Exception as auth_e:
                            console.print(f"[red]Authentication failed: {auth_e}[/red]")
                            last_err = f"Interactive authorization failed: {auth_e}"
                            ui.write(
                                "attempt.error",
                                {
                                    "attempt": attempt,
                                    "error": last_err,
                                    "traceback": traceback.format_exc()[:20000],
                                },
                            )
                            continue

                    except Exception as e:
                        set_emitter(None)
                        active_progress = None
                        active_live = None

                        if isinstance(e, KeyboardInterrupt):
                            ui.write(
                                "attempt.keyboard_interrupt",
                                {
                                    "attempt": attempt,
                                    "traceback": traceback.format_exc()[:20000],
                                },
                            )
                            raise

                        last_err = str(e)
                        ui.write(
                            "attempt.error",
                            {
                                "attempt": attempt,
                                "error": last_err,
                                "traceback": traceback.format_exc()[:20000],
                            },
                        )

                        console.print(render_api_error_panel(e, ctype))

                        if attempt <= retries:
                            sleep_s = min(2 ** (attempt - 1), 20)
                            ui.write(
                                "attempt.retrying",
                                {"attempt": attempt, "sleep_s": sleep_s},
                            )
                            console.print(f"[yellow]Retrying in {sleep_s}s...[/yellow]")
                            time.sleep(sleep_s)
    except Exception as e:
        last_err = str(e)
        ui.write(
            "attempt.error",
            {
                "attempt": max(1, attempt),
                "error": last_err,
                "traceback": traceback.format_exc()[:20000],
            },
        )
        console.print(render_api_error_panel(e, ctype))

    watchdog_stop.set()

    finished_at = _now_iso()
    duration_ms = int((time.monotonic() - started_mono) * 1000)
    success = last_err is None
    rows_inserted = _rows_inserted_from_stats(
        result_stats, total_records if success else 0
    )

    ui.write(
        "run.finish",
        {
            "success": success,
            "attempts": attempt,
            "duration_ms": duration_ms,
            "error": last_err,
            "stream_counts": stream_counts,
            "total_records": total_records,
            "rows_inserted": rows_inserted,
        },
    )

    payload: Dict[str, Any] = {
        "connection_name": name,
        "connection_type": ctype,
        "schema": schema,
        "success": success,
        "run_id": run_id,
        "ui_event_log_path": ui_log_path,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "attempts": attempt,
        "error": last_err,
        "state_before": state_before_run,
        "state_updates": state_updates,
        "schema_drift_policy": drift_policy.value,
        "schema_drift": drift_summary,
        "progress_counts": stream_counts,
        "progress_total_records": total_records,
        "schema_diff_counts": schema_diff_counts,
        "schema_records_seen": schema_records_seen,
        "rows_inserted": rows_inserted,
        "stats": result_stats,
        "summary": {
            "stream_totals": stream_counts,
            "records_reported": total_records,
            "rows_inserted": rows_inserted,
        },
    }
    if success:
        payload["report"] = report_text[:20000]

    log_path = write_run_log(payload)

    if success:
        if refreshed_creds:
            # update_secret merges; safe to call directly (no merge kwarg).
            update_secret(name, refreshed_creds)
            console.print("[yellow]🔄 Credentials refreshed and saved.[/yellow]")

        if stream_counts:
            t = Table(
                title="Connector progress (reported)",
                show_header=True,
                header_style="bold",
            )
            t.add_column("Stream")
            t.add_column("Count", justify="right")
            for s in sorted(stream_counts.keys()):
                t.add_row(s, str(stream_counts[s]))
            t.add_row("[bold]TOTAL[/bold]", f"[bold]{total_records}[/bold]")
            console.print(t)
        console.print(f"[dim]Rows inserted (reported): {rows_inserted}[/dim]")

        console.print(
            Panel(
                f"[green]✅ Success[/green]\n{report_text or '(no report)'}",
                title="Report",
            )
        )
        console.print(f"[dim]Run log: {log_path}[/dim]")
        console.print(f"[dim]UI event log: {ui_log_path} (run_id={run_id})[/dim]")
    else:
        state = normalise_state(conn.get("state") or state)
        state["global"]["last_run_at"] = finished_at
        conn["state"] = state
        conn["last_run_at"] = finished_at
        save_config_v2(cfg)

        console.print(f"[red]❌ Failed[/red]  (log: {log_path})")
        console.print(f"[dim]UI event log: {ui_log_path} (run_id={run_id})[/dim]")

    if pause_on_exit:
        input("\nPress Enter...")

    return payload


def run_all_connections(cfg: Dict[str, Any]) -> None:
    conns = [c for c in list_connections(cfg) if c.get("enabled", True)]
    if not conns:
        console.print("[yellow]No enabled connections.[yellow]")
        input("\nPress Enter...")
        return

    console.clear()
    console.print(Panel("[bold magenta]Run All Connections[/bold magenta]"))

    continue_on_error = True
    retries = int(questionary.text("Retries per connection:", default="2").ask() or "2")

    failures = 0
    for conn in conns:
        try:
            run_connection(cfg, conn, retries=retries, pause_on_exit=False)
        except Exception:
            failures += 1
            if not continue_on_error:
                break

    console.print(Panel(f"Run-all completed. Failures: {failures}", title="Summary"))
    input("\nPress Enter...")
