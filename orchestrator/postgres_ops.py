from __future__ import annotations

import os
import re
import subprocess
import tempfile
import time
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import questionary
import tomlkit
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table, Column
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.layout import Layout

from .constants import SECRETS_FILE
from .locking import file_lock
from .secrets import get_postgres_credentials, secure_secrets_file
from .config import load_config_v2, save_config_v2
from .schema_sync import (
    sync_schema_incremental,
    get_schema_sync_report,
    resume_schema_sync_if_needed,
    reset_schema_sync_state,
)
from orchestrator.ui import fmt_seconds
from .ssm_tunnel import maybe_open_ssm_tunnel

console = Console()

_LAST_PG_TEST_AT = 0.0
_LAST_PG_TEST_OK = False


def _make_spinner() -> Progress:
    # Ensure long status lines wrap in narrow terminals.
    return Progress(
        SpinnerColumn(),
        TextColumn("{task.description}", table_column=Column(ratio=1, overflow="fold")),
        transient=True,
    )


def test_postgres_connection(
    creds: Optional[Dict[str, Any]] = None, cache_seconds: int = 5
) -> bool:
    global _LAST_PG_TEST_AT, _LAST_PG_TEST_OK

    now = time.time()
    if now - _LAST_PG_TEST_AT < cache_seconds:
        return _LAST_PG_TEST_OK

    if creds is None:
        creds = get_postgres_credentials()
        if not creds:
            _LAST_PG_TEST_AT = now
            _LAST_PG_TEST_OK = False
            return False

    try:
        import psycopg2  # type: ignore

        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["database"],
            user=creds["username"],
            password=creds.get("password", ""),
            connect_timeout=int(creds.get("connect_timeout", 5)),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        finally:
            conn.close()

        _LAST_PG_TEST_OK = True
    except Exception as e:
        _LAST_PG_TEST_OK = False
        console.print(
            Panel(
                f"[red]Postgres connectivity test failed[/red]\n{repr(e)}", style="red"
            )
        )

    _LAST_PG_TEST_AT = now
    return _LAST_PG_TEST_OK


def configure_postgres() -> None:
    console.clear()
    console.print(Panel("[bold green]Postgres Configuration[/bold green]"))

    host = questionary.text("Host:", default="localhost").ask() or "localhost"
    port = questionary.text("Port:", default="5432").ask() or "5432"
    database = questionary.text("Database:", default="dlt_db").ask() or "dlt_db"
    user = questionary.text("User:", default="dlt_user").ask() or "dlt_user"
    password = questionary.password("Password:").ask() or ""

    creds = {
        "host": host,
        "port": int(port),
        "database": database,
        "username": user,
        "password": password,
        "connect_timeout": 15,
    }

    with file_lock(SECRETS_FILE) as f:
        content = f.read().strip()
        doc = tomlkit.parse(content) if content else tomlkit.document()

        if "destination" not in doc:
            doc["destination"] = tomlkit.table()
        if "postgres" not in doc["destination"]:
            doc["destination"]["postgres"] = tomlkit.table()

        doc["destination"]["postgres"]["credentials"] = creds

        f.seek(0)
        f.write(tomlkit.dumps(doc))
        f.truncate()

    secure_secrets_file()

    ok = test_postgres_connection(creds, cache_seconds=0)
    console.print(
        "[green]✅ Postgres connected & saved![/green]"
        if ok
        else "[red]❌ Connection failed, but config saved.[/red]"
    )
    input("\nPress Enter...")


def setup_agent_schema() -> None:
    console.clear()
    console.print(Panel("[bold cyan]Setup Agent Schema (Clara Agent)[/bold cyan]"))

    creds = get_postgres_credentials()
    if not creds:
        console.print(
            "[red]❌ No Postgres credentials found. Run 'Configure Postgres' first.[/red]"
        )
        input("\nPress Enter...")
        return

    try:
        import psycopg2

        conn = psycopg2.connect(
            host=creds["host"],
            port=int(creds["port"]),
            dbname=creds["database"],
            user=creds["username"],
            password=creds.get("password", ""),
            connect_timeout=int(creds.get("connect_timeout", 5)),
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            # 1. Create Schema
            console.print("[yellow]Creating schema 'clara_agent'...[/yellow]")
            cur.execute('CREATE SCHEMA IF NOT EXISTS "clara_agent";')

            user = creds["username"]

            # 2. Grant full control on agent schema to the user
            cur.execute(f'GRANT ALL ON SCHEMA "clara_agent" TO "{user}";')
            cur.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA "clara_agent" TO "{user}";')
            cur.execute(
                f'ALTER DEFAULT PRIVILEGES IN SCHEMA "clara_agent" GRANT ALL ON TABLES TO "{user}";'
            )

            # 3. Grant Read Access to ALL schemas (to ensure agent can query dlt data)
            console.print(
                "[yellow]Configuring read access across all schemas...[/yellow]"
            )

            # Fetch all non-system schemas
            cur.execute("""
                SELECT nspname 
                FROM pg_catalog.pg_namespace 
                WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                  AND nspname NOT LIKE 'pg_%%'
            """)
            schemas = [row[0] for row in cur.fetchall()]

            for schema in schemas:
                # Skip clara_agent as we handled it above with full permissions
                if schema == "clara_agent":
                    continue

                safe_schema = f'"{schema}"'
                try:
                    # Grant usage on schema
                    cur.execute(f'GRANT USAGE ON SCHEMA {safe_schema} TO "{user}";')
                    # Grant select on all existing tables
                    cur.execute(
                        f'GRANT SELECT ON ALL TABLES IN SCHEMA {safe_schema} TO "{user}";'
                    )
                    # Grant select on future tables (best effort)
                    cur.execute(
                        f'ALTER DEFAULT PRIVILEGES IN SCHEMA {safe_schema} GRANT SELECT ON TABLES TO "{user}";'
                    )
                except Exception as perm_err:
                    console.print(
                        f"[dim]Warning: Could not grant permissions on schema {schema}: {perm_err}[/dim]"
                    )

        conn.close()
        console.print("[green]✅ Schema 'clara_agent' created![/green]")
        console.print(
            f"[green]✅ Read permissions granted on {len(schemas)} schemas to '{user}'.[/green]"
        )
        console.print(
            f"[dim]The AI agent can now query all data in '{creds['database']}' using its dedicated schema for storage.[/dim]"
        )

    except Exception as e:
        console.print(
            Panel(f"[red]Failed to setup agent schema[/red]\n{repr(e)}", style="red")
        )

    input("\nPress Enter...")


def configure_db_target() -> None:
    console.clear()
    console.print(Panel("[bold cyan]Database Target[/bold cyan]"))

    cfg = load_config_v2()
    settings = cfg.setdefault("settings", {})
    db_mode = settings.get("db_mode", "local")
    db_overrides = settings.setdefault("db_overrides", {})

    choice = questionary.select(
        "Select database target:",
        choices=[
            "Local (secrets.toml)",
            "AWS (SSM/RDS tunnel)",
            "↩️ Back",
        ],
        default="AWS (SSM/RDS tunnel)" if db_mode == "aws" else "Local (secrets.toml)",
    ).ask()

    if not choice or "Back" in choice:
        return

    if choice.startswith("Local"):
        settings["db_mode"] = "local"
        save_config_v2(cfg)
        console.print("[green]✅ Database target set to local.[/green]")
        input("\nPress Enter...")
        return

    settings["db_mode"] = "aws"
    current = db_overrides.get("aws") or {}
    host_default = str(current.get("host") or "localhost")
    # Default to a non-standard local port to avoid clashing with a local Postgres on 5432.
    port_default = str(current.get("port") or "55432")
    host = (
        questionary.text("AWS host (SSM tunnel host):", default=host_default).ask()
        or host_default
    )
    port = questionary.text("AWS port:", default=port_default).ask() or port_default

    try:
        port_val: int | str = int(port)
    except ValueError:
        port_val = port

    db_overrides["aws"] = {"host": host, "port": port_val}
    settings["db_overrides"] = db_overrides
    save_config_v2(cfg)

    console.print(f"[green]✅ Database target set to AWS ({host}:{port}).[/green]")
    input("\nPress Enter...")


def copy_local_schema_to_aws() -> None:
    console.clear()
    console.print(Panel("[bold cyan]Schema Sync to AWS[/bold cyan]"))

    base = get_postgres_credentials() or {}
    if not base:
        console.print(
            "[red]❌ No Postgres credentials found. Run 'Configure Postgres' first.[/red]"
        )
        input("\nPress Enter...")
        return

    console.print("[dim]This syncs schemas/tables from local to AWS (no data).[/dim]")

    src_host = (
        questionary.text("Source host:", default="localhost").ask() or "localhost"
    )
    src_port = questionary.text("Source port:", default="5434").ask() or "5434"
    src_db = (
        questionary.text(
            "Source database:", default=str(base.get("database") or "dlt_db")
        ).ask()
        or "dlt_db"
    )
    src_user = (
        questionary.text(
            "Source user:", default=str(base.get("username") or "dlt_user")
        ).ask()
        or "dlt_user"
    )
    src_password = questionary.password(
        "Source password (leave blank to reuse saved):"
    ).ask()
    if not src_password:
        src_password = str(base.get("password") or "")

    dest_via_ssm = questionary.confirm(
        "Connect to AWS via an automatically-managed SSM tunnel?",
        default=True,
    ).ask()

    # Detect if we are likely in a tunnel session (env var set by ssm_tunnel.py)
    tunnel_host = os.environ.get("POSTGRES_HOST_OVERRIDE")
    tunnel_port = os.environ.get("POSTGRES_PORT_OVERRIDE")
    default_dst_host = tunnel_host if tunnel_host else "127.0.0.1"
    default_dst_port = tunnel_port if tunnel_port else "55432"

    if dest_via_ssm:
        # Host/port will be set by the tunnel (and may auto-adjust if the port is taken).
        dst_host = default_dst_host
        dst_port = default_dst_port
    else:
        dst_host = (
            questionary.text("Destination host:", default=default_dst_host).ask()
            or default_dst_host
        )
        dst_port = (
            questionary.text("Destination port:", default=default_dst_port).ask()
            or default_dst_port
        )
    dst_db = (
        questionary.text("Destination database:", default="db_analytics").ask()
        or "db_analytics"
    )
    dst_user = (
        questionary.text("Destination user:", default="usr_analytics").ask()
        or "usr_analytics"
    )
    dst_password = questionary.password("Destination password:").ask() or ""

    # Open an SSM tunnel just-in-time (after prompts) so the session doesn't sit idle while you type passwords.
    # If a tunnel is already active (env overrides are set), reuse it.
    need_tunnel = dest_via_ssm and not bool(
        os.environ.get("POSTGRES_PORT_OVERRIDE")
        or os.environ.get("DESTINATION__POSTGRES__CREDENTIALS__PORT")
    )
    tunnel_ctx = nullcontext()
    if need_tunnel:
        cfg = load_config_v2()
        if (cfg.get("settings", {}).get("db_mode") or "").strip().lower() != "aws":
            cfg_override = cfg.copy()
            cfg_override["settings"] = dict(cfg.get("settings") or {})
            cfg_override["settings"]["db_mode"] = "aws"
            tunnel_cfg = cfg_override
        else:
            tunnel_cfg = cfg
        tunnel_ctx = maybe_open_ssm_tunnel(tunnel_cfg)

    try:
        with tunnel_ctx:
            # If we are in an active tunnel session, prefer using the tunnel's concrete host/port (usually 127.0.0.1:NNNN).
            # This avoids "::1" IPv6 resolution issues and handles dynamic local-port selection.
            tunnel_host2 = os.environ.get("POSTGRES_HOST_OVERRIDE")
            tunnel_port2 = os.environ.get("POSTGRES_PORT_OVERRIDE")
            if tunnel_host2:
                dst_host = tunnel_host2
            if tunnel_port2:
                dst_port = str(tunnel_port2)
            if dest_via_ssm:
                console.print(
                    f"[dim]Using SSM tunnel endpoint: {dst_host}:{dst_port}[/dim]"
                )

            # Quick destination connectivity preflight (gives a clearer error than the schema diff step).
            try:
                import psycopg2  # type: ignore

                check_host = (
                    "127.0.0.1"
                    if (dst_host or "").strip().lower() == "localhost"
                    else dst_host
                )
                conn = psycopg2.connect(
                    host=check_host,
                    port=int(dst_port),
                    dbname=dst_db,
                    user=dst_user,
                    password=dst_password,
                    connect_timeout=8,
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1;")
                        cur.fetchone()
                finally:
                    conn.close()
            except Exception as e:
                console.print(
                    Panel(
                        "[red]Destination database is not reachable[/red]\n"
                        f"{repr(e)}\n\n"
                        "[dim]If you are using SSM, the SSM target likely cannot reach the RDS endpoint on 5432 (security group/routing/DNS), or credentials/dbname are wrong.\n"
                        "Tip: run `Settings → Database Settings → AWS Tunnel Doctor` to check/fix the RDS security group.[/dim]",
                        title="Connectivity Check Failed",
                    )
                )
                input("\nPress Enter...")
                return

            # Create a live layout for the progress display
            started_mono = time.monotonic()

            def make_header(status: str = "Starting...") -> Panel:
                elapsed = fmt_seconds(time.monotonic() - started_mono)
                title = f"[bold blue]Schema Sync[/bold blue]  [dim]⏱ {elapsed}[/dim]"
                body = f"Source: [dim]{src_host}:{src_port}/{src_db}[/dim]\nDest:   [dim]{dst_host}:{dst_port}/{dst_db}[/dim]\nStatus: [dim]{status}[/dim]"
                return Panel(
                    body, border_style="blue", expand=True, padding=(0, 1), title=title
                )

            spinner = _make_spinner()
            task_id = spinner.add_task("Checking for changes...", total=None)

            def render_group(status: str = "Checking for changes...") -> Group:
                return Group(
                    make_header(status),
                    spinner,
                )

            with Live(render_group(), console=console, refresh_per_second=8) as live:
                # Check if there's a failed sync that needs to be resumed
                resume_check = resume_schema_sync_if_needed("schema_sync")
                if resume_check["needs_resume"]:
                    console.print(
                        Panel(
                            f"[yellow]⚠️  Resuming failed sync[/yellow]\n"
                            f"Previous error: {resume_check['last_error']}\n"
                            f"Failed after {resume_check['attempts']} attempts",
                            title="Resume Detected",
                        )
                    )

                    # Ask user if they want to reset and start fresh instead of resuming
                    reset_choice = questionary.select(
                        "Would you like to reset the sync state and start fresh?",
                        choices=[
                            "No, continue with resume",
                            "Yes, reset and start fresh",
                        ],
                    ).ask()

                    if reset_choice and "start fresh" in reset_choice:
                        reset_result = reset_schema_sync_state("schema_sync")
                        if reset_result["status"] == "success":
                            console.print(
                                Panel(
                                    "[green]✅ Schema sync state reset successfully[/green]\n"
                                    "Starting fresh sync...",
                                    title="State Reset",
                                )
                            )
                            spinner.update(
                                task_id, description="Starting fresh sync..."
                            )
                            live.update(render_group("Starting fresh sync"))
                        else:
                            console.print(
                                Panel(
                                    f"[red]Failed to reset schema sync state[/red]\n"
                                    f"Error: {reset_result['message']}",
                                    title="Reset Failed",
                                )
                            )
                            # Continue with normal sync even if reset failed
                    else:
                        spinner.update(
                            task_id, description="Resuming from previous failure..."
                        )
                        live.update(render_group("Resuming sync"))

                # Perform the schema sync with state tracking and retry logic
                result = sync_schema_incremental(
                    source_host=src_host,
                    source_port=src_port,
                    source_db=src_db,
                    source_user=src_user,
                    source_password=src_password,
                    dest_host=dst_host,
                    dest_port=dst_port,
                    dest_db=dst_db,
                    dest_user=dst_user,
                    dest_password=dst_password,
                    sync_name="schema_sync",
                    max_retries=3,
                    base_backoff=1.0,
                    max_backoff=60.0,
                    use_incremental=True,  # Use incremental sync by default
                    status_cb=lambda msg: (
                        spinner.update(task_id, description=msg),
                        live.update(render_group(msg)),
                    ),
                )

                if result["status"] == "no_changes":
                    spinner.update(
                        task_id,
                        description="No schema changes detected - nothing to sync",
                    )
                    live.update(render_group("No changes detected"))

                    console.print(
                        Panel(
                            "[yellow]⚠️  No schema changes detected[/yellow]\n"
                            "Schema is already up to date with the last sync.",
                            title="Up to Date",
                        )
                    )
                elif result["status"] == "success":
                    spinner.update(
                        task_id, description="Schema sync completed successfully"
                    )
                    live.update(render_group("Sync completed"))

                    # Prepare detailed message based on sync type
                    incremental_sync = result.get("incremental_results") is not None
                    sync_type = "incrementally" if incremental_sync else "fully"

                    message_parts = [
                        "[green]✅ Schema Sync Completed[/green]",
                        f"Duration: {fmt_seconds(result['duration'])}",
                        f"Sync type: {sync_type}",
                        f"Attempts: {result.get('attempts', 1)}",
                    ]

                    if incremental_sync and result.get("incremental_results"):
                        inc_results = result["incremental_results"]
                        summary = inc_results.get("summary", {})
                        message_parts.append(
                            f"Changes applied: {summary.get('applied', 0)}"
                        )
                        message_parts.append(
                            f"Changes failed: {summary.get('failed', 0)}"
                        )

                    console.print(Panel("\n".join(message_parts), title="Success"))

                    # Show detailed sync report
                    report = get_schema_sync_report("schema_sync")
                    if report.get("last_sync_at"):
                        console.print(f"[dim]Last sync: {report['last_sync_at']}[/dim]")
                    if report.get("sync_count"):
                        console.print(f"[dim]Total syncs: {report['sync_count']}[/dim]")
                    if report.get("status"):
                        console.print(f"[dim]Status: {report['status']}[/dim]")

                else:
                    # Error case
                    spinner.update(
                        task_id,
                        description=f"Error: {result.get('message', 'Unknown error')}",
                    )
                    live.update(render_group("Error occurred"))

                    console.print(
                        Panel(
                            f"[red]Sync failed[/red]\n{result.get('message', 'Unknown error')}",
                            title="Error",
                        )
                    )

    except FileNotFoundError:
        console.print(
            Panel(
                "[red]❌ pg_dump/psql not found[/red]\n"
                "Install PostgreSQL client tools to perform schema sync.",
                title="Missing Dependencies",
            )
        )
    except Exception as e:
        console.print(
            Panel(f"[red]Sync failed[/red]\n{str(e)}", title="Unexpected Error")
        )

    input("\nPress Enter...")


def _read_simple_env(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'").strip('"')
                if k and k not in out:
                    out[k] = v
    except Exception:
        pass
    return out


def _truncate(s: str, limit: int = 8000) -> str:
    s = s or ""
    if len(s) <= limit:
        return s
    return s[-limit:]


def _rewrite_inserts_on_conflict_do_nothing(src_path: str, dst_path: str) -> None:
    """
    Makes a `pg_dump --inserts` SQL file idempotent by turning:
      INSERT INTO ...;
    into:
      INSERT INTO ... ON CONFLICT DO NOTHING;

    This enables "append" restores to succeed even when unique constraints are present.
    """
    in_insert = False
    buf: list[str] = []

    with (
        open(src_path, "r", encoding="utf-8") as src,
        open(dst_path, "w", encoding="utf-8") as dst,
    ):
        for line in src:
            if not in_insert and (
                line.startswith("INSERT INTO ") or line.startswith("INSERT INTO ONLY ")
            ):
                in_insert = True
                buf = [line]
                if line.rstrip().endswith(";"):
                    stmt = "".join(buf)
                    if "ON CONFLICT" not in stmt:
                        stmt = stmt.rstrip()
                        if stmt.endswith(";"):
                            stmt = stmt[:-1] + " ON CONFLICT DO NOTHING;\n"
                    dst.write(stmt)
                    in_insert = False
                    buf = []
                continue

            if in_insert:
                buf.append(line)
                if line.rstrip().endswith(";"):
                    stmt = "".join(buf)
                    if "ON CONFLICT" not in stmt:
                        stmt = stmt.rstrip()
                        if stmt.endswith(";"):
                            stmt = stmt[:-1] + " ON CONFLICT DO NOTHING;\n"
                    dst.write(stmt)
                    in_insert = False
                    buf = []
                continue

            dst.write(line)


def _get_db_sync_state(name: str = "local_to_aws") -> Dict[str, Any]:
    try:
        cfg = load_config_v2()
        syncs = cfg.get("db_syncs") if isinstance(cfg, dict) else None
        if isinstance(syncs, dict):
            st = syncs.get(name)
            if isinstance(st, dict):
                return dict(st)
    except Exception:
        pass
    return {}


def _set_db_sync_state(state: Dict[str, Any], name: str = "local_to_aws") -> None:
    try:
        cfg = load_config_v2()
        syncs = cfg.setdefault("db_syncs", {})
        if not isinstance(syncs, dict):
            cfg["db_syncs"] = {}
            syncs = cfg["db_syncs"]
        syncs[name] = dict(state)
        save_config_v2(cfg)
    except Exception:
        pass


def _psql_quote_ident(ident: str) -> str:
    ident = ident or ""
    return '"' + ident.replace('"', '""') + '"'


def _psql_quote_literal(val: str) -> str:
    val = val or ""
    return "'" + val.replace("'", "''") + "'"


def _truncate_all_tables_sql(
    *,
    include_schemas: Optional[list[str]] = None,
    exclude_schemas: Optional[list[str]] = None,
) -> str:
    """
    Generates SQL that truncates all tables in selected schemas.
    Requires sufficient privileges on destination.
    """
    include_schemas = include_schemas or []
    exclude_schemas = exclude_schemas or ["pg_catalog", "information_schema"]

    where_parts: list[str] = []
    where_parts.append(
        "n.nspname NOT IN ("
        + ",".join(_psql_quote_literal(s) for s in exclude_schemas)
        + ")"
    )
    if include_schemas:
        where_parts.append(
            "n.nspname IN ("
            + ",".join(_psql_quote_literal(s) for s in include_schemas)
            + ")"
        )
    where_sql = " AND ".join(where_parts)

    return f"""
DO $$
DECLARE
  stmt text;
BEGIN
  SELECT 'TRUNCATE TABLE ' || string_agg(format('%I.%I', n.nspname, c.relname), ', ') || ' RESTART IDENTITY CASCADE;'
    INTO stmt
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r' AND {where_sql};

  IF stmt IS NULL THEN
    RAISE NOTICE 'No tables matched for truncation.';
  ELSE
    EXECUTE stmt;
  END IF;
END $$;
""".strip()


def sync_local_data_to_aws() -> None:
    """
    Copy DATA ONLY from a local Postgres (typically Docker) into AWS Postgres via SSM.
    Assumes schema already exists on destination (use Schema Sync first).
    """
    from datetime import datetime

    console.clear()
    console.print(Panel("[bold cyan]Data Sync: Local → AWS (SSM)[/bold cyan]"))
    console.print(
        "[dim]This copies data only (no schema changes). Run Schema Sync first.[/dim]"
    )

    env_defaults = _read_simple_env(".env")
    src_host_default = "localhost"
    src_port_default = env_defaults.get("POSTGRES_PORT") or "5434"
    src_db_default = env_defaults.get("POSTGRES_DB") or "dlt_db"
    src_user_default = env_defaults.get("POSTGRES_USER") or "dlt_user"
    src_pass_default = env_defaults.get("POSTGRES_PASSWORD") or ""

    src_host = (
        questionary.text("Source host:", default=src_host_default).ask()
        or src_host_default
    )
    src_port = questionary.text(
        "Source port:", default=str(src_port_default)
    ).ask() or str(src_port_default)
    src_db = questionary.text(
        "Source database:", default=str(src_db_default)
    ).ask() or str(src_db_default)
    src_user = questionary.text(
        "Source user:", default=str(src_user_default)
    ).ask() or str(src_user_default)
    src_password = questionary.password(
        "Source password (leave blank to reuse .env value):"
    ).ask()
    if not src_password:
        src_password = str(src_pass_default or "")

    dest_db_default = "db_analytics"
    dest_user_default = "usr_analytics"
    dest_db = (
        questionary.text("Destination database (AWS):", default=dest_db_default).ask()
        or dest_db_default
    )
    dest_user = (
        questionary.text("Destination user (AWS):", default=dest_user_default).ask()
        or dest_user_default
    )
    dest_password = questionary.password("Destination password (AWS):").ask() or ""

    schema_raw = questionary.text(
        "Limit to schemas (comma-separated, blank = all non-system):",
        default="",
    ).ask()
    include_schemas: list[str] = []
    if schema_raw:
        include_schemas = [s.strip() for s in str(schema_raw).split(",") if s.strip()]

    mode = questionary.select(
        "Data mode:",
        choices=[
            "Append only (do not delete existing data)",
            "↩️ Back",
        ],
        default="Append only (do not delete existing data)",
    ).ask()
    if not mode or "Back" in mode:
        return
    truncate_first = mode.startswith("Truncate")

    if truncate_first:
        console.print(
            Panel(
                "[yellow]Warning[/yellow]\n"
                "This will TRUNCATE destination tables before loading.\n"
                "You must have privileges to truncate the target tables.",
                title="Destructive Operation",
            )
        )
        if not questionary.confirm("Continue?", default=False).ask():
            return

    os.makedirs("backups", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dump_path = os.path.join("backups", f"local_to_aws_data_{ts}.dump")

    started_mono = time.monotonic()
    spinner = _make_spinner()
    task_id = spinner.add_task("Preparing…", total=None)

    try:
        with spinner:
            spinner.update(task_id, description=f"Dumping local data to {dump_path}…")
            dump_env = os.environ.copy()
            dump_env["PGPASSWORD"] = src_password
            dump_cmd = [
                "pg_dump",
                "-Fc",
                "--data-only",
                "--no-owner",
                "--no-acl",
                "-h",
                src_host,
                "-p",
                str(src_port),
                "-U",
                src_user,
                "-d",
                src_db,
                "-f",
                dump_path,
            ]
            for s in include_schemas:
                dump_cmd += ["--schema", s]
            subprocess.run(
                dump_cmd, check=True, env=dump_env, text=True, capture_output=True
            )

            cfg = load_config_v2()
            if (cfg.get("settings", {}).get("db_mode") or "").strip().lower() != "aws":
                cfg_override = cfg.copy()
                cfg_override["settings"] = dict(cfg.get("settings") or {})
                cfg_override["settings"]["db_mode"] = "aws"
                tunnel_cfg = cfg_override
            else:
                tunnel_cfg = cfg

            spinner.update(task_id, description="Opening SSM tunnel…")
            with maybe_open_ssm_tunnel(tunnel_cfg):
                dst_host = os.environ.get("POSTGRES_HOST_OVERRIDE") or "127.0.0.1"
                dst_port = os.environ.get("POSTGRES_PORT_OVERRIDE") or "55432"

                if truncate_first:
                    spinner.update(
                        task_id, description="Truncating destination tables…"
                    )
                    trunc_sql = _truncate_all_tables_sql(
                        include_schemas=include_schemas
                    )
                    trunc_env = os.environ.copy()
                    trunc_env["PGPASSWORD"] = dest_password
                    trunc_res = subprocess.run(
                        [
                            "psql",
                            "-h",
                            dst_host,
                            "-p",
                            str(dst_port),
                            "-U",
                            dest_user,
                            "-d",
                            dest_db,
                            "-v",
                            "ON_ERROR_STOP=1",
                        ],
                        input=trunc_sql,
                        env=trunc_env,
                        text=True,
                        capture_output=True,
                    )
                    if trunc_res.returncode != 0:
                        raise RuntimeError(
                            f"psql truncate failed: {_truncate(trunc_res.stderr or trunc_res.stdout)}"
                        )

                spinner.update(task_id, description="Restoring data to AWS…")
                restore_env = os.environ.copy()
                restore_env["PGPASSWORD"] = dest_password
                restore_cmd = [
                    "pg_restore",
                    "--data-only",
                    "--no-owner",
                    "--no-privileges",
                    "--exit-on-error",
                    "-h",
                    dst_host,
                    "-p",
                    str(dst_port),
                    "-U",
                    dest_user,
                    "-d",
                    dest_db,
                    dump_path,
                ]
                restore_res = subprocess.run(
                    restore_cmd,
                    check=True,
                    env=restore_env,
                    text=True,
                    capture_output=True,
                )

        elapsed = fmt_seconds(time.monotonic() - started_mono)
        console.print(
            Panel(
                f"[green]✅ Data sync complete[/green]\nDuration: {elapsed}\nDump: {dump_path}",
                title="Success",
            )
        )
        if restore_res.stderr:
            console.print(
                Panel(_truncate(restore_res.stderr), title="pg_restore stderr (tail)")
            )

    except FileNotFoundError as e:
        console.print(
            Panel(
                f"[red]Missing dependency[/red]\n{e}\n\nInstall PostgreSQL client tools (`pg_dump`, `pg_restore`, `psql`).",
                title="Missing Tools",
            )
        )
    except subprocess.CalledProcessError as e:
        stderr = _truncate((e.stderr or "").strip())
        stdout = _truncate((e.stdout or "").strip())
        details = "\n".join(
            [
                p
                for p in [
                    stdout and f"stdout (tail):\n{stdout}",
                    stderr and f"stderr (tail):\n{stderr}",
                ]
                if p
            ]
        )
        console.print(
            Panel(f"[red]Command failed[/red]\n{details or str(e)}", title="Error")
        )
        console.print(f"[dim]Dump file kept at: {dump_path}[/dim]")
    except Exception as e:
        console.print(
            Panel(f"[red]Data sync failed[/red]\n{str(e)}", title="Unexpected Error")
        )
        console.print(f"[dim]Dump file kept at: {dump_path}[/dim]")

    input("\nPress Enter...")


class _TailReader:
    def __init__(self, max_lines: int = 200) -> None:
        from collections import deque
        import threading

        self._lines = deque(maxlen=max_lines)
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._last_seen_mono: float = 0.0
        self._sink = None

    def start(
        self, proc: subprocess.Popen[str], *, sink_path: Optional[Path] = None
    ) -> None:
        import threading

        if sink_path is not None:
            sink_path.parent.mkdir(parents=True, exist_ok=True)
            self._sink = open(sink_path, "a", encoding="utf-8")

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
                if self._sink is not None:
                    try:
                        self._sink.write(f"[{label}] {cleaned}\n")
                        self._sink.flush()
                    except Exception:
                        pass
                with self._lock:
                    self._lines.append(f"[{label}] {cleaned}")
                    self._last_seen_mono = time.monotonic()
        except Exception:
            return

    def last_line(self) -> str:
        with self._lock:
            return self._lines[-1] if self._lines else ""

    def seconds_since_output(self) -> float:
        with self._lock:
            if self._last_seen_mono <= 0.0:
                return float("inf")
            return max(0.0, time.monotonic() - self._last_seen_mono)

    def close(self) -> None:
        try:
            if self._sink is not None:
                self._sink.close()
        finally:
            self._sink = None


def sync_local_to_aws() -> None:
    """
    One button sync:
    - First run: schema + data
    - Subsequent runs: data (schema optional)
    """
    console.clear()
    console.print(Panel("[bold cyan]Sync Local → AWS (Schema + Data)[/bold cyan]"))

    state = _get_db_sync_state("local_to_aws")
    is_first_run = not bool(state.get("completed_at"))

    env_defaults = _read_simple_env(".env")
    src_host_default = "localhost"
    src_port_default = env_defaults.get("POSTGRES_PORT") or "5434"
    src_db_default = env_defaults.get("POSTGRES_DB") or "dlt_db"
    src_user_default = env_defaults.get("POSTGRES_USER") or "dlt_user"
    src_pass_default = env_defaults.get("POSTGRES_PASSWORD") or ""

    src_host = (
        questionary.text("Source host:", default=src_host_default).ask()
        or src_host_default
    )
    src_port = questionary.text(
        "Source port:", default=str(src_port_default)
    ).ask() or str(src_port_default)
    src_db = questionary.text(
        "Source database:", default=str(src_db_default)
    ).ask() or str(src_db_default)
    src_user = questionary.text(
        "Source user:", default=str(src_user_default)
    ).ask() or str(src_user_default)
    src_password = questionary.password(
        "Source password (leave blank to reuse .env value):"
    ).ask()
    if not src_password:
        src_password = str(src_pass_default or "")

    dest_db = (
        questionary.text("Destination database (AWS):", default="db_analytics").ask()
        or "db_analytics"
    )
    dest_user = (
        questionary.text("Destination user (AWS):", default="usr_analytics").ask()
        or "usr_analytics"
    )
    dest_password = questionary.password("Destination password (AWS):").ask() or ""

    include_schemas: list[str] = []
    schema_raw = questionary.text(
        "Limit to schemas (comma-separated, blank = all non-system):",
        default="",
    ).ask()
    if schema_raw:
        include_schemas = [s.strip() for s in str(schema_raw).split(",") if s.strip()]

    do_schema = bool(is_first_run)
    if not is_first_run:
        do_schema = bool(
            questionary.confirm("Run schema sync too?", default=False).ask()
        )

    data_mode_default = "Append only (do not delete existing data)"
    mode = questionary.select(
        "Data mode:",
        choices=[
            "Append only (do not delete existing data)",
            "↩️ Back",
        ],
        default=data_mode_default,
    ).ask()
    if not mode or "Back" in mode:
        return
    truncate_first = mode.startswith("Truncate")

    if truncate_first:
        console.print(
            Panel(
                "[yellow]Warning[/yellow]\n"
                "This will TRUNCATE destination tables before loading.\n"
                "You must have privileges to truncate the target tables.",
                title="Destructive Operation",
            )
        )
        if not questionary.confirm("Continue?", default=False).ask():
            return

    os.makedirs("backups", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dump_ext = "dump" if truncate_first else "sql"
    dump_path = os.path.join("backups", f"local_to_aws_data_{ts}.{dump_ext}")
    log_path = (
        Path("run_logs")
        / datetime.utcnow().strftime("%Y-%m-%d")
        / f"local_to_aws_{ts}_restore.log"
    )

    started_mono = time.monotonic()
    spinner = _make_spinner()
    task_id = spinner.add_task("Preparing…", total=None)

    cfg = load_config_v2()
    if (cfg.get("settings", {}).get("db_mode") or "").strip().lower() != "aws":
        cfg_override = cfg.copy()
        cfg_override["settings"] = dict(cfg.get("settings") or {})
        cfg_override["settings"]["db_mode"] = "aws"
        tunnel_cfg = cfg_override
    else:
        tunnel_cfg = cfg

    def status(msg: str) -> None:
        msg = (msg or "").replace("\n", " ").strip()
        spinner.update(task_id, description=msg)

    try:
        with spinner:
            status("Opening SSM tunnel…")
            with maybe_open_ssm_tunnel(tunnel_cfg):
                dst_host = os.environ.get("POSTGRES_HOST_OVERRIDE") or "127.0.0.1"
                dst_port = os.environ.get("POSTGRES_PORT_OVERRIDE") or "55432"

                status(f"Destination: {dst_host}:{dst_port}/{dest_db}")

                if do_schema:
                    # Full schema-only apply via pg_dump/psql (includes constraints/functions/triggers).
                    status("Schema: computing diff…")
                    schema_result = sync_schema_incremental(
                        source_host=src_host,
                        source_port=src_port,
                        source_db=src_db,
                        source_user=src_user,
                        source_password=src_password,
                        dest_host=dst_host,
                        dest_port=str(dst_port),
                        dest_db=dest_db,
                        dest_user=dest_user,
                        dest_password=dest_password,
                        sync_name="schema_sync",
                        max_retries=1,
                        use_incremental=False,
                        status_cb=lambda m: status(f"Schema: {m}"),
                    )
                    if schema_result.get("status") not in ("success", "no_changes"):
                        raise RuntimeError(
                            f"Schema sync failed: {schema_result.get('message')}"
                        )
                    status("Schema: done.")

                status(f"Data: dumping local to {dump_path}…")
                dump_env = os.environ.copy()
                dump_env["PGPASSWORD"] = src_password
                if truncate_first:
                    dump_cmd = [
                        "pg_dump",
                        "-Fc",
                        "--data-only",
                        "--no-owner",
                        "--no-acl",
                        "-h",
                        src_host,
                        "-p",
                        str(src_port),
                        "-U",
                        src_user,
                        "-d",
                        src_db,
                        "-f",
                        dump_path,
                    ]
                else:
                    # Append mode should be idempotent when destination already contains rows.
                    # Use INSERT statements so we can add `ON CONFLICT DO NOTHING`.
                    dump_cmd = [
                        "pg_dump",
                        "--data-only",
                        "--inserts",
                        "--column-inserts",
                        "--no-owner",
                        "--no-acl",
                        "-h",
                        src_host,
                        "-p",
                        str(src_port),
                        "-U",
                        src_user,
                        "-d",
                        src_db,
                        "-f",
                        dump_path,
                    ]
                for s in include_schemas:
                    dump_cmd += ["--schema", s]
                subprocess.run(
                    dump_cmd, check=True, env=dump_env, text=True, capture_output=True
                )

                if not truncate_first:
                    status("Data: preparing append-safe SQL (ON CONFLICT DO NOTHING)…")
                    tmp_path = dump_path + ".tmp"
                    _rewrite_inserts_on_conflict_do_nothing(dump_path, tmp_path)
                    os.replace(tmp_path, dump_path)

                if truncate_first:
                    status("Data: truncating destination tables…")
                    trunc_sql = _truncate_all_tables_sql(
                        include_schemas=include_schemas
                    )
                    trunc_env = os.environ.copy()
                    trunc_env["PGPASSWORD"] = dest_password
                    trunc_res = subprocess.run(
                        [
                            "psql",
                            "-h",
                            dst_host,
                            "-p",
                            str(dst_port),
                            "-U",
                            dest_user,
                            "-d",
                            dest_db,
                            "-v",
                            "ON_ERROR_STOP=1",
                        ],
                        input=trunc_sql,
                        env=trunc_env,
                        text=True,
                        capture_output=True,
                    )
                    if trunc_res.returncode != 0:
                        raise RuntimeError(
                            f"psql truncate failed: {_truncate(trunc_res.stderr or trunc_res.stdout)}"
                        )

                status(f"Data: restoring to AWS…  (log: {log_path.name})")
                restore_env = os.environ.copy()
                restore_env["PGPASSWORD"] = dest_password
                if truncate_first:
                    restore_cmd = [
                        "pg_restore",
                        "--data-only",
                        "--no-owner",
                        "--no-privileges",
                        "--exit-on-error",
                        "--verbose",
                        "-h",
                        dst_host,
                        "-p",
                        str(dst_port),
                        "-U",
                        dest_user,
                        "-d",
                        dest_db,
                        dump_path,
                    ]
                else:
                    restore_cmd = [
                        "psql",
                        "-h",
                        dst_host,
                        "-p",
                        str(dst_port),
                        "-U",
                        dest_user,
                        "-d",
                        dest_db,
                        "-v",
                        "ON_ERROR_STOP=1",
                        "-f",
                        dump_path,
                    ]
                proc = subprocess.Popen(
                    restore_cmd,
                    env=restore_env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                tail = _TailReader(max_lines=200)
                tail.start(proc, sink_path=log_path)
                last_update = 0.0
                restore_started = time.monotonic()
                while proc.poll() is None:
                    now = time.monotonic()
                    if now - last_update > 0.6:
                        ll = tail.last_line()
                        idle_s = tail.seconds_since_output()
                        elapsed = fmt_seconds(now - restore_started)
                        if ll:
                            if idle_s > 30:
                                status(
                                    f"Data: restoring… ⏱ {elapsed}  (no output {int(idle_s)}s)  {ll[:200]}"
                                )
                            else:
                                status(f"Data: restoring… ⏱ {elapsed}  {ll[:200]}")
                        else:
                            status(f"Data: restoring… ⏱ {elapsed}")
                        last_update = now
                    time.sleep(0.2)
                if proc.returncode != 0:
                    raise RuntimeError("pg_restore failed (see output above).")
                tail.close()

                elapsed = fmt_seconds(time.monotonic() - started_mono)
                status(f"Done in {elapsed}.")
                _set_db_sync_state(
                    {
                        "completed_at": datetime.utcnow().isoformat() + "Z",
                        "schema_ran": bool(do_schema),
                        "data_mode": "truncate" if truncate_first else "append",
                        "dest_db": dest_db,
                        "dest_user": dest_user,
                        "schemas": include_schemas,
                    },
                    "local_to_aws",
                )

        console.print(
            Panel(
                f"[green]✅ Sync complete[/green]\nDuration: {fmt_seconds(time.monotonic() - started_mono)}\nDump: {dump_path}\nRestore log: {str(log_path)}",
                title="Success",
            )
        )
    except Exception as e:
        console.print(Panel(f"[red]Sync failed[/red]\n{str(e)}", title="Error"))
        console.print(f"[dim]Dump file kept at: {dump_path}[/dim]")
        console.print(f"[dim]Restore log: {str(log_path)}[/dim]")

    input("\nPress Enter...")


def sync_local_db_to_aws() -> None:
    """
    One-shot full database copy (schema + data) from a local Postgres (typically Docker)
    into AWS Postgres via an SSM port-forward tunnel.
    """
    from datetime import datetime

    console.clear()
    console.print(Panel("[bold cyan]Full DB Sync: Local → AWS (SSM)[/bold cyan]"))

    env_defaults = _read_simple_env(".env")
    src_host_default = "localhost"
    src_port_default = env_defaults.get("POSTGRES_PORT") or "5435"
    src_db_default = env_defaults.get("POSTGRES_DB") or "dlt_db"
    src_user_default = env_defaults.get("POSTGRES_USER") or "dlt_user"
    src_pass_default = env_defaults.get("POSTGRES_PASSWORD") or ""

    console.print("[dim]Source defaults are read from .env when available.[/dim]")
    src_host = (
        questionary.text("Source host:", default=src_host_default).ask()
        or src_host_default
    )
    src_port = questionary.text(
        "Source port:", default=str(src_port_default)
    ).ask() or str(src_port_default)
    src_db = questionary.text(
        "Source database:", default=str(src_db_default)
    ).ask() or str(src_db_default)
    src_user = questionary.text(
        "Source user:", default=str(src_user_default)
    ).ask() or str(src_user_default)
    src_password = questionary.password(
        "Source password (leave blank to reuse .env value):"
    ).ask()
    if not src_password:
        src_password = str(src_pass_default or "")

    dest_base = get_postgres_credentials() or {}
    dest_db_default = str(dest_base.get("database") or "db_analytics")
    dest_user_default = str(dest_base.get("username") or "usr_analytics")

    dest_db = (
        questionary.text("Destination database (AWS):", default=dest_db_default).ask()
        or dest_db_default
    )
    dest_user = (
        questionary.text("Destination user (AWS):", default=dest_user_default).ask()
        or dest_user_default
    )
    dest_password = questionary.password("Destination password (AWS):").ask() or ""

    restore_mode = questionary.select(
        "Restore mode:",
        choices=[
            "Append only (no drops)",
            "↩️ Back",
        ],
        default="Append only (no drops)",
    ).ask()
    if not restore_mode or "Back" in restore_mode:
        return

    clean_restore = "Clean + replace" in restore_mode
    if clean_restore:
        console.print(
            Panel(
                "[yellow]Warning[/yellow]\n"
                "This will overwrite destination objects using `pg_restore --clean --if-exists`.\n"
                "Make sure you are targeting the correct AWS database.",
                title="Destructive Operation",
            )
        )
        if not questionary.confirm("Continue?", default=False).ask():
            return

    os.makedirs("backups", exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dump_path = os.path.join("backups", f"local_to_aws_{ts}.dump")

    started_mono = time.monotonic()
    spinner = _make_spinner()
    task_id = spinner.add_task("Preparing…", total=None)

    try:
        with spinner:
            spinner.update(task_id, description=f"Dumping local DB to {dump_path}…")
            dump_env = os.environ.copy()
            dump_env["PGPASSWORD"] = src_password
            subprocess.run(
                [
                    "pg_dump",
                    "-Fc",
                    "--no-owner",
                    "--no-acl",
                    "-h",
                    src_host,
                    "-p",
                    str(src_port),
                    "-U",
                    src_user,
                    "-d",
                    src_db,
                    "-f",
                    dump_path,
                ],
                check=True,
                env=dump_env,
                text=True,
                capture_output=True,
            )

            spinner.update(
                task_id, description="Opening SSM tunnel + restoring to AWS…"
            )
            need_tunnel = not bool(
                os.environ.get("POSTGRES_PORT_OVERRIDE")
                or os.environ.get("DESTINATION__POSTGRES__CREDENTIALS__PORT")
            )
            tunnel_ctx = nullcontext()
            if need_tunnel:
                cfg = load_config_v2()
                if (
                    cfg.get("settings", {}).get("db_mode") or ""
                ).strip().lower() != "aws":
                    cfg_override = cfg.copy()
                    cfg_override["settings"] = dict(cfg.get("settings") or {})
                    cfg_override["settings"]["db_mode"] = "aws"
                    tunnel_cfg = cfg_override
                else:
                    tunnel_cfg = cfg
                tunnel_ctx = maybe_open_ssm_tunnel(tunnel_cfg)

            with tunnel_ctx:
                dst_host = os.environ.get("POSTGRES_HOST_OVERRIDE") or "127.0.0.1"
                dst_port = os.environ.get("POSTGRES_PORT_OVERRIDE") or "55432"

                restore_env = os.environ.copy()
                restore_env["PGPASSWORD"] = dest_password
                restore_cmd = [
                    "pg_restore",
                    "--no-owner",
                    "--no-privileges",
                    "--exit-on-error",
                ]
                if clean_restore:
                    restore_cmd += ["--clean", "--if-exists"]
                restore_cmd += [
                    "-h",
                    dst_host,
                    "-p",
                    str(dst_port),
                    "-U",
                    dest_user,
                    "-d",
                    dest_db,
                    dump_path,
                ]
                restore_res = subprocess.run(
                    restore_cmd,
                    check=True,
                    env=restore_env,
                    text=True,
                    capture_output=True,
                )

        elapsed = fmt_seconds(time.monotonic() - started_mono)
        console.print(
            Panel(
                f"[green]✅ DB sync complete[/green]\nDuration: {elapsed}\nDump: {dump_path}",
                title="Success",
            )
        )
        if restore_res.stderr:
            console.print(
                Panel(_truncate(restore_res.stderr), title="pg_restore stderr (tail)")
            )

    except FileNotFoundError as e:
        console.print(
            Panel(
                f"[red]Missing dependency[/red]\n{e}\n\nInstall PostgreSQL client tools (`pg_dump`, `pg_restore`).",
                title="Missing Tools",
            )
        )
    except subprocess.CalledProcessError as e:
        stderr = _truncate((e.stderr or "").strip())
        stdout = _truncate((e.stdout or "").strip())
        details = "\n".join(
            [
                p
                for p in [
                    stdout and f"stdout (tail):\n{stdout}",
                    stderr and f"stderr (tail):\n{stderr}",
                ]
                if p
            ]
        )
        console.print(
            Panel(f"[red]Command failed[/red]\n{details or str(e)}", title="Error")
        )
        console.print(f"[dim]Dump file kept at: {dump_path}[/dim]")
    except Exception as e:
        console.print(
            Panel(f"[red]DB sync failed[/red]\n{str(e)}", title="Unexpected Error")
        )
        console.print(f"[dim]Dump file kept at: {dump_path}[/dim]")

    input("\nPress Enter...")
