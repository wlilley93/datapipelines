# Orchestrator Files Export

This document contains all files from the Orchestrator directory.

## File: __init__.py

```python
# Orchestrator package

```

## File: app.py

```python
#!/usr/bin/env python3
from __future__ import annotations

import os
import sys

import questionary
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.traceback import install

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv is optional, just continue if not installed
    pass

from .config import (
    delete_connection,
    get_connection,
    list_connections,
    load_config_v2,
    sanitize_name,
    sanitize_schema,
    save_config_v2,
)
from .constants import RUNS_DIR, SCHEMA_SNAPSHOTS_DIR
from .docker_ops import init_docker_environment, start_docker_services
from .postgres_ops import configure_postgres, test_postgres_connection
from .run_connection import run_connection  # type: ignore
from .run_history_ui import show_recent_runs
from .secrets import delete_secret, update_secret
from .ui import render_api_error_panel

install()
console = Console()


def run_all_connections(cfg):
    conns = [c for c in list_connections(cfg) if c.get("enabled", True)]
    if not conns:
        console.print("[yellow]No enabled connections.[/yellow]")
        input("\nPress Enter...")
        return

    console.clear()
    console.print(Panel("[bold magenta]Run All Connections[/bold magenta]"))

    # Always continue through all connectors; failures are counted and summarized.
    continue_on_error = True
    retries = int(questionary.text("Retries per connection:", default="2").ask() or "2")

    failures = 0
    for conn in conns:
        try:
            # Skip per-connector pause so the batch can progress automatically.
            run_connection(cfg, conn, retries=retries, pause_on_exit=False)
        except Exception:
            failures += 1
            if not continue_on_error:
                break

    console.print(Panel(f"Run-all completed. Failures: {failures}", title="Summary"))
    input("\nPress Enter...")


def flow_add_connector(cfg) -> None:
    console.clear()
    console.print(Panel("[bold green]Add New Connection[/bold green]"))

    raw_choice = questionary.select(
        "Which Service?",
        choices=[
            "hubspot - CRM data",
            "xero - Accounting",
            "trello - Project Management",
            "pandadoc - Documents",
            "fireflies - Transcripts",
            "intercom - Customer Support",
            "wassenger - Messaging Platform",
        ],
    ).ask()
    if not raw_choice:
        return

    connector_type = raw_choice.split(" ")[0].strip().lower()
    name_default = f"{connector_type}_main"
    name_raw = questionary.text("Unique name:", default=name_default).ask()
    if not name_raw:
        return
    name = sanitize_name(name_raw)

    if get_connection(cfg, name):
        console.print(f"[red]Name '{name}' already exists.[/red]")
        input("\nPress Enter...")
        return

    secrets = {}
    if connector_type == "xero":
        from connectors.xero.auth import build_auth_url, exchange_code_for_tokens, parse_code_from_redirect

        client_id = questionary.text("Client ID:").ask() or ""
        client_secret = questionary.password("Client Secret:").ask() or ""
        # Hardcoded for Xero auth helper flow
        redirect_uri = "https://developers.xero.com/tools/redirect"
        scopes = "offline_access accounting.transactions openid profile email"
        console.print(f"[cyan]Using redirect_uri={redirect_uri}[/cyan]")
        console.print(f"[cyan]Using scopes={scopes}[/cyan]")
        tenant_id = questionary.text("Tenant ID (optional):").ask() or ""
        auth_url = build_auth_url(client_id=client_id, redirect_uri=redirect_uri, scopes=scopes.split(), state=None)
        refresh_token = questionary.password("Refresh Token (optional):").ask() or ""
        auth_code = ""

        # If no refresh token provided, offer to exchange a code or build a consent URL.
        if not refresh_token:
            console.print("[yellow]No refresh token provided.[/yellow]")
            console.print("\n[cyan]Authorize via this link, then paste the redirect URL or code below (or rerun later):[/cyan]")
            # ✅ Don't print the full URL; show a clickable link label instead.
            console.print(f"[link={auth_url}]click here[/link]\n")
            redirect_response_url = questionary.text("Redirect response URL (optional):").ask() or ""
            auth_code = questionary.text("Auth code (optional):").ask() or ""
            code = auth_code or parse_code_from_redirect(redirect_response_url) or ""

            if not code:
                console.print(
                    "\n[cyan]No code/redirect provided. You can authorize later using the link above and rerun to save the token.[/cyan]\n"
                )
            else:
                try:
                    tokens = exchange_code_for_tokens(
                        client_id=client_id,
                        client_secret=client_secret,
                        code=code,
                        redirect_uri=redirect_uri,
                    )
                    refresh_token = tokens.get("refresh_token", "") or refresh_token
                    console.print("[green]Received refresh token from code exchange.[/green]")
                except Exception as e:
                    console.print(f"[red]Failed to exchange code for tokens: {e}[/red]")

        secrets["client_id"] = client_id
        secrets["client_secret"] = client_secret
        secrets["redirect_uri"] = redirect_uri
        secrets["scopes"] = scopes
        if tenant_id:
            secrets["tenant_id"] = tenant_id
        if refresh_token:
            secrets["refresh_token"] = refresh_token
    elif connector_type == "hubspot":
        secrets["access_token"] = questionary.password("Access Token:").ask()
    elif connector_type == "trello":
        console.print("[dim]Get API Key & Token from https://trello.com/app-key[/dim]")
        secrets["api_key"] = questionary.text("API Key:").ask()
        secrets["token"] = questionary.password("API Token:").ask()
    elif connector_type == "pandadoc":
        secrets["api_key"] = questionary.password("API Key:").ask()
    elif connector_type == "fireflies":
        secrets["api_key"] = questionary.password("API Key:").ask()
    elif connector_type == "intercom":
        secrets["access_token"] = questionary.password("Access Token:").ask()
        secrets["client_secret"] = questionary.password("Client Secret:").ask()
    elif connector_type == "wassenger":
        secrets["api_key"] = questionary.password("API Key:").ask()
        base_url = questionary.text("Base API URL (optional, defaults to https://api.wassenger.com/v1):").ask()
        if base_url and base_url.strip():
            secrets["base_url"] = base_url.strip()

    if connector_type == "xero":
        if not (secrets.get("client_id") and secrets.get("client_secret") and secrets.get("redirect_uri")):
            console.print("[red]Client ID, Client Secret, and Redirect URI are required.[/red]")
            input("\nPress Enter...")
            return
    elif not all(secrets.values()):
        console.print("[red]All fields required.[/red]")
        input("\nPress Enter...")
        return

    # Test credentials using runtime loader (skip if Xero without refresh token yet)
    should_test = True
    if connector_type == "xero" and not secrets.get("refresh_token"):
        should_test = False
        console.print(
            "[yellow]Skipping credential test until refresh_token is obtained. "
            "Authorize via the link above and update the connection with the resulting token.[/yellow]"
        )

    if should_test:
        try:
            from connectors.runtime.loader import load as load_connector

            connector = load_connector(connector_type)
            console.print("[yellow]Testing credentials...[/yellow]")
            msg = connector.check(secrets)
            console.print(f"[green]✅ {msg}[/green]")
        except Exception as e:
            console.print(render_api_error_panel(e, connector_type))
            if not questionary.confirm("Save connection anyway?").ask():
                return

    update_secret(name, secrets)

    schema_default = sanitize_schema(name.replace("-", "_"))
    schema_val = questionary.text("Postgres schema (namespace) for tables:", default=schema_default).ask() or schema_default

    from datetime import datetime, timezone

    cfg["connections"].append(
        {
            "name": name,
            "type": connector_type,
            "schema": sanitize_schema(schema_val),
            "enabled": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "state": {"version": 1, "global": {"schema_drift_policy": "warn"}, "streams": {}},
        }
    )
    save_config_v2(cfg)

    console.print(f"[green]✅ Added {name}[/green]")
    input("\nPress Enter...")


def flow_add_source(cfg) -> None:  # cfg unused; kept for symmetry
    console.print("[yellow]No separate sources supported. Use Add Connection for Xero credentials.[/yellow]")


def toggle_connection(cfg) -> None:
    conns = list_connections(cfg)
    if not conns:
        console.print("[yellow]No connections.[/yellow]")
        input("\nPress Enter...")
        return

    sel = questionary.select(
        "Toggle which connection?",
        choices=[f"{'✅' if c.get('enabled', True) else '⏸️'} {c['name']}" for c in conns],
    ).ask()
    if not sel:
        return

    name = sel.split(" ", 1)[1]
    conn = get_connection(cfg, name)
    if not conn:
        return
    conn["enabled"] = not conn.get("enabled", True)
    save_config_v2(cfg)

    console.print(f"[green]Updated:[/green] {name} enabled={conn['enabled']}")
    input("\nPress Enter...")


def reset_state(cfg) -> None:
    conns = list_connections(cfg)
    if not conns:
        console.print("[yellow]No connections.[yellow]")
        input("\nPress Enter...")
        return

    sel = questionary.select("Reset state for:", choices=[c["name"] for c in conns]).ask()
    if not sel:
        return

    conn = get_connection(cfg, sel)
    if not conn:
        return

    if not questionary.confirm(f"Really reset state for {sel}?", default=False).ask():
        return

    conn["state"] = {"version": 1, "global": {"schema_drift_policy": "warn"}, "streams": {}}
    save_config_v2(cfg)
    console.print("[green]State reset.[/green]")
    input("\nPress Enter...")


def delete_connection_flow(cfg) -> None:
    conns = list_connections(cfg)
    if not conns:
        console.print("[yellow]No connections.[/yellow]")
        input("\nPress Enter...")
        return

    sel = questionary.select("Delete which connection?", choices=[c["name"] for c in conns]).ask()
    if not sel:
        return

    if not questionary.confirm(f"Really delete connection '{sel}' (and its stored secrets)?", default=False).ask():
        return

    # Remove from config
    cfg = delete_connection(cfg, sel)
    save_config_v2(cfg)

    # Remove secrets (ignore if missing)
    delete_secret(sel)

    console.print(f"[green]Deleted connection:[/green] {sel}")
    input("\nPress Enter...")


def main() -> None:
    os.makedirs(".dlt", exist_ok=True)
    os.makedirs(RUNS_DIR, exist_ok=True)

    while True:
        cfg = load_config_v2()

        console.clear()
        console.print(
            Panel.fit(
                "[bold magenta]Clara Formations Data Wizard 🧙[/bold magenta]",
                subtitle="",
            )
        )

        pg_ok = test_postgres_connection()
        pg_status = "[green]Connected[/green]" if pg_ok else "[red]Not connected[/red]"

        conns = list_connections(cfg)
        enabled = sum(1 for c in conns if c.get("enabled", True))

        status_table = Table(show_header=False, box=None)
        status_table.add_row("🗃️  Postgres:", pg_status)
        status_table.add_row("🔌 Connections:", f"{len(conns)} total / {enabled} enabled")
        status_table.add_row("🧾 Run logs:", os.path.abspath(RUNS_DIR))
        status_table.add_row("🧬 Drift snapshots:", os.path.abspath(SCHEMA_SNAPSHOTS_DIR))
        console.print(Panel(status_table, title="Status"))

        action = questionary.select(
            "Choose action:",
            choices=[
                "▶️  Run One Sync",
                "📦 Run All (batch)",
                "📖 Generate All Documentation",
                "➕ Add Connection",
                "🗑️  Delete Connection",
                "⏯️  Enable/Disable Connection",
                "♻️  Reset Connection State",
                "⚙️  Configure Postgres",
                "🐳 Start Docker Services",
                "🐳 Configure Docker",
                "📜 View Run History (DB)",
                "❌ Exit",
            ],
        ).ask()

        if not action or "Exit" in action:
            sys.exit(0)

        if "Configure Docker" in action:
            init_docker_environment()
            continue
        if "Start Docker" in action:
            start_docker_services()
            continue
        if "Configure Postgres" in action:
            configure_postgres()
            continue
        if "Generate All Documentation" in action:
            try:
                from tools.generate_scripts.run_all_gen_scripts import run_all_gen_scripts

                console.print("[blue]Starting documentation generation...[/blue]")
                run_all_gen_scripts()
                console.print("[green]✅ Documentation generation completed![/green]")
            except ImportError:
                console.print("[red]❌ Error: Could not import documentation generation script[/red]")
                console.print("[yellow]Make sure tools/generate_scripts/run_all_gen_scripts.py exists[/yellow]")
            except Exception as e:
                console.print(f"[red]❌ Error running documentation generation: {e}[/red]")
            input("\nPress Enter...")
            continue
        if "Add Connection" in action:
            flow_add_connector(cfg)
            continue
        if "Delete Connection" in action:
            delete_connection_flow(cfg)
            continue
        if "Enable/Disable" in action:
            toggle_connection(cfg)
            continue
        if "Reset Connection State" in action:
            reset_state(cfg)
            continue
        if "Run History" in action:
            show_recent_runs()
            continue

        if not pg_ok:
            console.print("[yellow]Postgres not connected. Configure Postgres first.[/yellow]")
            input("\nPress Enter...")
            continue

        if "Run All" in action:
            run_all_connections(cfg)
            continue

        if "Run One Sync" in action:
            conns = [c for c in list_connections(cfg) if c.get("enabled", True)]
            if not conns:
                console.print("[yellow]No enabled connections.[/yellow]")
                input("\nPress Enter...")
                continue

            sel = questionary.select("Select:", choices=[c["name"] for c in conns]).ask()
            if not sel:
                continue

            conn = get_connection(cfg, sel)
            if not conn:
                continue

            run_connection(cfg, conn, retries=2)
            continue


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

```

## File: config.py

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from rich.console import Console

from connectors.runtime.state import normalise_state

from .constants import CONFIG_FILE, CONFIG_VERSION
from .locking import file_lock

console = Console()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_schema(schema: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() or ch == "_" else "_" for ch in (schema or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    s = s.strip("_")
    return s or "default_schema"


def sanitize_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() or ch in ("_", "-") else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "connection"


def save_config_v2(cfg: Dict[str, Any]) -> None:
    try:
        with file_lock(CONFIG_FILE) as f:
            f.seek(0)
            json.dump(cfg, f, indent=2)
            f.truncate()
    except Exception as e:
        console.print(f"[red]ERROR: Failed to save config: {e}[/red]")


def load_config_v2() -> Dict[str, Any]:
    """
    Config v2 format:
    {
      "config_version": 2,
      "connections": [
        {"name":..., "type":..., "schema":..., "enabled": true, "created_at":..., "state": {...}}
      ]
    }

    Migration from v1 (list of connections) is automatic.
    """
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return {"config_version": CONFIG_VERSION, "connections": []}
            data = json.loads(raw)
    except FileNotFoundError:
        return {"config_version": CONFIG_VERSION, "connections": []}
    except Exception as e:
        console.print(f"[red]ERROR: Corrupted {CONFIG_FILE}: {e}[/red]")
        return {"config_version": CONFIG_VERSION, "connections": []}

    # v1 -> v2 migration
    if isinstance(data, list):
        migrated = {"config_version": CONFIG_VERSION, "connections": []}
        for c in data:
            if not isinstance(c, dict):
                continue
            conn = dict(c)
            conn.setdefault("schema", conn.get("name", "").replace("-", "_"))
            conn.setdefault("enabled", True)
            conn.setdefault("created_at", _now_iso())
            conn.setdefault("state", {"version": 1, "global": {}, "streams": {}})
            migrated["connections"].append(conn)
        save_config_v2(migrated)
        return migrated

    if not isinstance(data, dict):
        return {"config_version": CONFIG_VERSION, "connections": []}

    data.setdefault("config_version", CONFIG_VERSION)
    data.setdefault("connections", [])
    if not isinstance(data["connections"], list):
        data["connections"] = []

    for conn in data["connections"]:
        if not isinstance(conn, dict):
            continue
        conn.setdefault("schema", conn.get("name", "").replace("-", "_"))
        conn.setdefault("enabled", True)
        conn.setdefault("created_at", _now_iso())
        conn.setdefault("state", {"version": 1, "global": {}, "streams": {}})
        conn["state"] = normalise_state(conn.get("state"))

    return data


def list_connections(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    conns = cfg.get("connections") or []
    return [c for c in conns if isinstance(c, dict)]


def get_connection(cfg: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    for c in list_connections(cfg):
        if c.get("name") == name:
            return c
    return None


def delete_connection(cfg: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Remove a connection by name and return updated cfg."""
    conns = list_connections(cfg)
    cfg["connections"] = [c for c in conns if c.get("name") != name]
    return cfg

```

## File: constants.py

```python
from __future__ import annotations

import os

CONFIG_FILE = "config.json"
SECRETS_FILE = ".dlt/secrets.toml"
RUNS_DIR = "runs"
CONFIG_VERSION = 2

SCHEMA_SNAPSHOTS_DIR = os.path.join(".dlt", "schema_snapshots")

DOCKER_COMPOSE_TEMPLATE = """\
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    ports:
      - "${POSTGRES_PORT}:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"]
      interval: 5s
      timeout: 5s
      retries: 15
    restart: unless-stopped
  adminer:
    image: adminer:latest
    ports:
      - "8000:8080"
    depends_on:
      - postgres
    restart: unless-stopped
  metabase:
    image: metabase/metabase:latest
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: ${POSTGRES_DB}
      MB_DB_PORT: 5432
      MB_DB_USER: ${POSTGRES_USER}
      MB_DB_PASS: ${POSTGRES_PASSWORD}
      MB_DB_HOST: postgres
    depends_on:
      - postgres
    restart: unless-stopped

volumes:
  pgdata:
"""

```

## File: docker_ops.py

```python
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

```

## File: locking.py

```python
from __future__ import annotations

import os
from contextlib import contextmanager

import portalocker


@contextmanager
def file_lock(file_path: str):
    """Cross-platform exclusive lock around config/secrets writes."""
    abs_path = os.path.abspath(file_path)
    parent = os.path.dirname(abs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if not os.path.exists(abs_path):
        open(abs_path, "w", encoding="utf-8").close()

    with open(abs_path, "r+", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            yield f
        finally:
            portalocker.unlock(f)

```

## File: postgres_ops.py

```python
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import questionary
import tomlkit
from rich.console import Console
from rich.panel import Panel

from .constants import SECRETS_FILE
from .locking import file_lock
from .secrets import get_postgres_credentials, secure_secrets_file

console = Console()

_LAST_PG_TEST_AT = 0.0
_LAST_PG_TEST_OK = False


def test_postgres_connection(creds: Optional[Dict[str, Any]] = None, cache_seconds: int = 5) -> bool:
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
        console.print(Panel(f"[red]Postgres connectivity test failed[/red]\n{repr(e)}", style="red"))

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
    console.print("[green]✅ Postgres connected & saved![/green]" if ok else "[red]❌ Connection failed, but config saved.[/red]")
    input("\nPress Enter...")

```

## File: run_connection.py

```python
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
    def __init__(self, path: str, *, run_id: str, connection_name: str, connection_type: str) -> None:
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

    def status(self, *, line: str, level: str, stream: str, throttle_s: float = 0.15) -> None:
        now = time.monotonic()
        if line == self._last_status_line and (now - self._last_status_at) < (throttle_s * 3):
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
    root.setLevel(min(old_level, logging.INFO) if isinstance(old_level, int) else logging.INFO)
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
        return old_showwarning(message, category, filename, lineno, file=file, line=line)

    warnings.showwarning = _showwarning  # type: ignore

    try:
        yield
    finally:
        warnings.showwarning = old_showwarning  # type: ignore
        root.removeHandler(handler)
        root.setLevel(old_level)


def _drift_policy_from_state(state: Dict[str, Any]) -> DriftPolicy:
    pol = ((state.get("global") or {}).get("schema_drift_policy") or "warn").strip().lower()
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
        if isinstance(added, str) and added.isdigit() and isinstance(missing, str) and missing.isdigit():
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

    def _first_numeric(keys: Tuple[str, ...], container: Dict[str, Any]) -> Optional[int]:
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
            nested_val = _first_numeric(("rows_inserted", "inserted_rows", "rows_loaded"), nested)
            if nested_val is not None:
                return nested_val

    return default


def _bookmark_snapshot(state: Dict[str, Any], keys: Optional[List[str]] = None) -> Dict[str, Any]:
    g = state.get("global")
    if not isinstance(g, dict):
        return {}
    b = g.get("bookmarks")
    if not isinstance(b, dict):
        return {}
    if not keys:
        keys = ["companies", "deals", "emails", "meetings", "tasks", "products", "line_items"]
    return {k: b.get(k) for k in keys}


def run_connection(cfg: Dict[str, Any], conn: Dict[str, Any], *, retries: int = 2, pause_on_exit: bool = True) -> None:
    console.clear()
    name = conn["name"]
    ctype = conn["type"]
    schema = sanitize_schema(conn.get("schema") or name)

    state = normalise_state(conn.get("state") or {})
    drift_policy = _drift_policy_from_state(state)

    run_id = str(uuid.uuid4())
    ui_log_path = os.getenv("UI_EVENT_LOG_PATH", _default_ui_log_path(run_id, name))
    ui = UiEventLogger(path=ui_log_path, run_id=run_id, connection_name=name, connection_type=ctype)

    started_at = _now_iso()
    started_mono = time.monotonic()

    attempt = 0
    last_err: Optional[str] = None
    report_text = ""
    refreshed_creds: Optional[Dict[str, Any]] = None
    state_updates: Dict[str, Any] = {}
    drift_summary: Optional[Dict[str, Any]] = None
    result_stats: Dict[str, Any] = {}

    stream_counts: Dict[str, int] = {}
    total_records: int = 0

    schema_diff_counts: Dict[str, Dict[str, int]] = {}
    schema_records_seen: Dict[str, int] = {}

    spinner_line = "Starting…"
    spinner_level = "info"

    active_progress: Optional[Tuple[Progress, int]] = None
    active_live: Optional[Live] = None

    last_event_mono = time.monotonic()
    watchdog_stop = threading.Event()

    def _watchdog_loop(quiet_dump_s: int = 30) -> None:
        nonlocal last_event_mono
        while not watchdog_stop.is_set():
            time.sleep(1.0)
            quiet_for = time.monotonic() - last_event_mono
            if quiet_for >= quiet_dump_s:
                dump = _dump_all_thread_stacks(f"{quiet_dump_s}s_no_events")
                ui.write("watchdog.thread_dump", {"quiet_s": quiet_dump_s, "dump": dump[:20000]})
                last_event_mono = time.monotonic()

    watchdog_thread = threading.Thread(target=_watchdog_loop, name="connector-watchdog", daemon=True)
    watchdog_thread.start()

    ui.write("run.start", {"schema": schema, "retries": retries, "started_at": started_at})
    ui.write("state.debug.before_run", {"bookmarks": _bookmark_snapshot(state)})

    def header_panel() -> Panel:
        elapsed = fmt_seconds(time.monotonic() - started_mono)
        title = f"[bold blue]Syncing: {name}[/bold blue]  [dim]⏱ {elapsed}[/dim]"
        status_line = f"[dim]{ctype} → schema: {schema}[/dim]"

        # ✅ Always show an integer (0 is meaningful). Removes the confusing "Starting…" state.
        total_line = f"[dim]Total records (reported):[/dim] [bold]{total_records}[/bold]"

        body = f"{status_line}\n{total_line}"
        return Panel(body, border_style="blue", expand=True, padding=(1, 2), title=title)

    def render_group(progress: Progress) -> Group:
        return Group(header_panel(), progress)

    def cli_emitter(ev: RuntimeEvent) -> None:
        nonlocal spinner_line, spinner_level, total_records, active_progress, active_live, last_event_mono
        nonlocal schema_diff_counts, schema_records_seen

        last_event_mono = time.monotonic()

        stream = ev.stream or "default"

        ui.write("runtime.event", _event_to_dict(ev))

        if ev.type in ("count", "records", "rows") and isinstance(ev.count, int):
            stream_counts[stream] = int(ev.count)
            total_records = sum(int(v) for v in stream_counts.values() if isinstance(v, int))

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

    with capture_logs_and_warnings(ui):
        while attempt <= retries:
            attempt += 1
            ui.write("attempt.start", {"attempt": attempt, "max_attempts": retries + 1})
            ui.write("state.debug.before_attempt", {"attempt": attempt, "bookmarks": _bookmark_snapshot(state)})

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
                    task_id = progress.add_task(f"Starting (attempt {attempt}/{retries+1})…", total=None)
                    active_progress = (progress, task_id)

                    with Live(render_group(progress), console=console, refresh_per_second=8, transient=True) as live:
                        active_live = live

                        ReadSelection = getattr(
                            __import__("connectors.runtime.protocol", fromlist=["ReadSelection"]),
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
                state["global"]["last_run_at"] = state["global"]["last_success_at"]

                conn["state"] = state
                conn["last_run_at"] = state["global"]["last_run_at"]
                save_config_v2(cfg)

                ui.write("state.debug.after_success_merge", {"bookmarks": _bookmark_snapshot(state)})

                if isinstance(result.observed_schema, dict) and result.observed_schema:
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

                user_response = questionary.text("Paste the full redirect URL or the authorization code here:").ask()

                if not user_response:
                    console.print("[red]Authorization cancelled or no input provided. Retrying...[/red]")
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
                    redirect_uri = xero_creds.get("redirect_uri") or xero_creds.get("redirectUri")

                    new_xero_tokens = exchange_code_for_tokens(
                        client_id=client_id,
                        client_secret=client_secret,
                        code=code,
                        redirect_uri=str(redirect_uri),
                    )

                    # update_secret merges; safe to call directly (no merge kwarg).
                    update_secret(name, new_xero_tokens)

                    console.print("[green]Authentication successful! Resuming pipeline...[/green]")
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
                    ui.write("attempt.retrying", {"attempt": attempt, "sleep_s": sleep_s})
                    console.print(f"[yellow]Retrying in {sleep_s}s...[/yellow]")
                    time.sleep(sleep_s)

    watchdog_stop.set()

    finished_at = _now_iso()
    duration_ms = int((time.monotonic() - started_mono) * 1000)
    success = last_err is None
    rows_inserted = _rows_inserted_from_stats(result_stats, total_records if success else 0)

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
            t = Table(title="Connector progress (reported)", show_header=True, header_style="bold")
            t.add_column("Stream")
            t.add_column("Count", justify="right")
            for s in sorted(stream_counts.keys()):
                t.add_row(s, str(stream_counts[s]))
            t.add_row("[bold]TOTAL[/bold]", f"[bold]{total_records}[/bold]")
            console.print(t)
        console.print(f"[dim]Rows inserted (reported): {rows_inserted}[/dim]")

        console.print(Panel(f"[green]✅ Success[/green]\n{report_text or '(no report)'}", title="Report"))
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

```

## File: run_history_db.py

```python
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .secrets import get_postgres_credentials


def ensure_run_history_table() -> None:
    """
    Optional: a place to later store runs in Postgres.
    Not wired by default yet (safe no-op if psycopg2 missing or creds absent).
    """
    creds = get_postgres_credentials()
    if not creds:
        return
    try:
        import psycopg2  # type: ignore
    except Exception:
        return

    ddl = """
    CREATE TABLE IF NOT EXISTS orchestrator_run_history (
      id BIGSERIAL PRIMARY KEY,
      connection_name TEXT NOT NULL,
      connection_type TEXT NOT NULL,
      schema_name TEXT NOT NULL,
      success BOOLEAN NOT NULL,
      started_at TIMESTAMPTZ NOT NULL,
      finished_at TIMESTAMPTZ NOT NULL,
      duration_ms BIGINT NOT NULL,
      attempts INT NOT NULL,
      error TEXT NULL,
      progress_counts JSONB NULL,
      state_updates JSONB NULL,
      report TEXT NULL,
      raw JSONB NOT NULL
    );
    """

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
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def write_run_history_row(payload: Dict[str, Any]) -> None:
    """
    Optional: call this from run_connection if you want DB persistence.
    """
    creds = get_postgres_credentials()
    if not creds:
        return
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except Exception:
        return

    ensure_run_history_table()

    started_at = payload.get("started_at")
    finished_at = payload.get("finished_at")
    duration_ms = int(payload.get("duration_ms") or 0)

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
            cur.execute(
                """
                INSERT INTO orchestrator_run_history
                (connection_name, connection_type, schema_name, success, started_at, finished_at, duration_ms,
                 attempts, error, progress_counts, state_updates, report, raw)
                VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    payload.get("connection_name"),
                    payload.get("connection_type"),
                    payload.get("schema"),
                    bool(payload.get("success")),
                    started_at,
                    finished_at,
                    duration_ms,
                    int(payload.get("attempts") or 0),
                    payload.get("error"),
                    psycopg2.extras.Json(payload.get("progress_counts") or {}),
                    psycopg2.extras.Json(payload.get("state_updates") or {}),
                    payload.get("report"),
                    psycopg2.extras.Json(payload),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def list_recent_runs(limit: int = 25) -> List[Dict[str, Any]]:
    creds = get_postgres_credentials()
    if not creds:
        return []
    try:
        import psycopg2  # type: ignore
        import psycopg2.extras  # type: ignore
    except Exception:
        return []

    ensure_run_history_table()

    conn = psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds.get("password", ""),
        connect_timeout=int(creds.get("connect_timeout", 5)),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, connection_name, connection_type, schema_name, success, started_at, finished_at,
                       duration_ms, attempts, error, progress_counts
                FROM orchestrator_run_history
                ORDER BY id DESC
                LIMIT %s
                """,
                (int(limit),),
            )
            rows = cur.fetchall() or []
            return [dict(r) for r in rows]
    finally:
        conn.close()

```

## File: run_history_ui.py

```python
from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .run_history_db import list_recent_runs

console = Console()


def show_recent_runs() -> None:
    rows = list_recent_runs(limit=25)
    if not rows:
        console.print(Panel("[yellow]No DB run history available yet.[/yellow]\nTip: you can enable DB logging later.", title="Run History"))
        input("\nPress Enter...")
        return

    t = Table(title="Recent runs (Postgres)", show_header=True, header_style="bold")
    t.add_column("ID", justify="right")
    t.add_column("Conn")
    t.add_column("Type")
    t.add_column("Schema")
    t.add_column("OK")
    t.add_column("Duration (ms)", justify="right")
    t.add_column("Attempts", justify="right")

    for r in rows:
        t.add_row(
            str(r.get("id")),
            str(r.get("connection_name")),
            str(r.get("connection_type")),
            str(r.get("schema_name")),
            "✅" if r.get("success") else "❌",
            str(r.get("duration_ms")),
            str(r.get("attempts")),
        )

    console.print(t)
    input("\nPress Enter...")

```

## File: run_logs.py

```python
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from .constants import RUNS_DIR


def _runs_path_for_today() -> str:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(RUNS_DIR, day)
    os.makedirs(path, exist_ok=True)
    return path


def write_run_log(payload: Dict[str, Any]) -> str:
    path = _runs_path_for_today()
    # Include time in filename for easy sorting: HH:MM (24-hour format)
    timestamp = datetime.now(timezone.utc).strftime("%H:%M")
    name = payload.get("connection_name", "unknown")
    run_id = payload.get("run_id", "unknown")
    filename = f"{timestamp}_{name}_{run_id}.json".replace(os.sep, "_")
    full = os.path.join(path, filename)
    with open(full, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return full

```

## File: secrets.py

```python
from __future__ import annotations

import os
from typing import Any, Dict, Optional, Mapping

import tomlkit
from rich.console import Console

from .constants import SECRETS_FILE
from .locking import file_lock

console = Console()


def secure_secrets_file() -> None:
    if os.path.exists(SECRETS_FILE) and os.name != "nt":
        try:
            os.chmod(SECRETS_FILE, 0o600)
        except Exception:
            pass


def _as_plain_dict(value: Any) -> Dict[str, Any]:
    """
    tomlkit may return container-ish objects (tables) that act like dicts.
    We normalize to a plain python dict, shallowly.
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def update_secret(source_name: str, secret_data: Dict[str, Any], *, merge: bool = True) -> None:
    """
    Update a connection's secrets.

    Default behavior is MERGE into existing secrets (merge=True):
      - This prevents flows like Xero OAuth refresh/code exchange from overwriting
        and deleting core fields (client_id/client_secret/redirect_uri).

    If merge=False:
      - We REPLACE the entire secret object for that source_name.
    """
    os.makedirs(".dlt", exist_ok=True)

    with file_lock(SECRETS_FILE) as f:
        content = f.read().strip()
        doc = tomlkit.parse(content) if content else tomlkit.document()

        if "sources" not in doc:
            doc["sources"] = tomlkit.table()

        existing_raw = doc["sources"].get(source_name)
        existing = _as_plain_dict(existing_raw)
        incoming = _as_plain_dict(secret_data)

        if merge:
            # Shallow merge is intentional: token refresh updates a few keys,
            # while preserving client_id/client_secret/redirect_uri/scopes/etc.
            merged = dict(existing)
            merged.update(incoming)
            doc["sources"][source_name] = merged
        else:
            # Replace entirely
            doc["sources"][source_name] = incoming

        f.seek(0)
        f.write(tomlkit.dumps(doc))
        f.truncate()

    secure_secrets_file()


def get_secret(source_name: str) -> Dict[str, Any]:
    if not os.path.exists(SECRETS_FILE):
        raise Exception("Secrets file missing. Configure Postgres/Connections first.")
    with open(SECRETS_FILE, "r", encoding="utf-8") as f:
        doc = tomlkit.parse(f.read())
    return dict(doc.get("sources", {}).get(source_name, {}))


def get_postgres_credentials() -> Optional[Dict[str, Any]]:
    if not os.path.exists(SECRETS_FILE):
        return None
    try:
        with open(SECRETS_FILE, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
        if "destination" in doc and "postgres" in doc["destination"]:
            return dict(doc["destination"]["postgres"]["credentials"])
    except Exception:
        return None
    return None


def delete_secret(source_name: str) -> None:
    """Remove a source entry from secrets.toml."""
    if not os.path.exists(SECRETS_FILE):
        return
    with file_lock(SECRETS_FILE) as f:
        content = f.read().strip()
        if not content:
            return
        doc = tomlkit.parse(content)
        sources = doc.get("sources")
        if sources and source_name in sources:
            del sources[source_name]
            f.seek(0)
            f.write(tomlkit.dumps(doc))
            f.truncate()
    secure_secrets_file()

```

## File: ui.py

```python
from __future__ import annotations

from typing import Any, Dict, List, Optional

from rich.panel import Panel

from connectors.runtime.events import RuntimeEvent


def is_htmlish(content_type: str, body_preview: str) -> bool:
    ct = (content_type or "").lower()
    lower_body = (body_preview or "").lower()
    return ("text/html" in ct) or ("<html" in lower_body)


def render_api_error_panel(e: Exception, service_type: str) -> Panel:
    resp = getattr(e, "response", None)

    if resp is not None:
        status = getattr(resp, "status_code", "unknown")
        ct = (getattr(resp, "headers", {}) or {}).get("Content-Type", "") or ""

        try:
            body_preview = (resp.text or "")[:800]
        except Exception:
            body_preview = ""

        if is_htmlish(ct, body_preview):
            lower_body = (body_preview or "").lower()
            if service_type.lower() == "trello" and "invalid token" in lower_body:
                return Panel(
                    "[red]Invalid Trello Token[/red]\n"
                    "Fix: generate a new token at trello.com/app-key and update the connector secrets.",
                    style="red",
                )
            if service_type.lower() == "fireflies" and ("apollo server" in lower_body or "fireflies" in lower_body):
                return Panel(
                    "[red]Fireflies Endpoint Error[/red]\n"
                    "You hit a web/UI endpoint rather than the GraphQL API.\n"
                    "Fix: use https://api.fireflies.ai/graphql",
                    style="red",
                )
            return Panel(
                f"[red]HTML Response Error[/red]\n"
                f"Endpoint returned HTML instead of JSON.\n"
                f"Status: {status}\n\n[dim]{body_preview}[/dim]",
                style="red",
            )

        if status in (401, 403):
            hint = "Unauthorized/Forbidden. Check token/API key and required scopes."
            return Panel(
                f"[red]Auth Error {status}[/red]\n{hint}\n\n[dim]{body_preview}[/dim]",
                style="red",
            )

        if status == 429:
            ra = (getattr(resp, "headers", {}) or {}).get("Retry-After")
            hint = f"Rate limited. Retry-After: {ra}s" if ra else "Rate limited. Back off and retry."
            return Panel(
                f"[red]Rate Limit (429)[/red]\n{hint}\n\n[dim]{body_preview}[/dim]",
                style="red",
            )

        return Panel(
            f"[red]API Error {status}[/red]\n[dim]{body_preview}[/dim]",
            style="red",
        )

    return Panel(f"[red]Error[/red]\n{str(e)}", style="red")


def fmt_seconds(s: float) -> str:
    s = max(0.0, float(s))
    mm = int(s // 60)
    ss = int(s % 60)
    return f"{mm}:{ss:02d}"


def truncate(s: Any, n: int = 96) -> str:
    s2 = str(s)
    return s2 if len(s2) <= n else (s2[: n - 1] + "…")


def format_event_line(ev: RuntimeEvent, *, include_level: bool = False) -> str:
    """
    This is the regression fix: render ev.fields so you can see url/method/op/etc again.
    """
    stream = ev.stream or "default"
    level = (ev.level or "info").lower().strip()
    msg = str(ev.message)

    f: Dict[str, Any] = ev.fields or {}
    parts: List[str] = []

    if include_level and level != "info":
        parts.append(f"[{level}]")

    parts.append(f"[{stream}] {msg}")

    key_order = [
        "op",
        "method",
        "url",
        "board_id",
        "elapsed_ms",
        "items_count",
        "keys_count",
        "count",
        "page",
        "limit",
        "before",
        "since",
        "filter",
        "fields",
        "param_keys",
        "error_type",
        "error",
    ]

    def one(k: str) -> Optional[str]:
        if k == "count":
            if isinstance(ev.count, int):
                return f"count={ev.count}"
            return None
        if k not in f:
            return None
        v = f.get(k)
        if v is None:
            return None
        if k == "url":
            return f"url={truncate(v, 160)}"
        if k == "param_keys" and isinstance(v, (list, tuple)):
            return f"params={truncate(','.join(map(str, v)), 120)}"
        if k == "error":
            return f"error={truncate(v, 180)}"
        return f"{k}={truncate(v, 120) if isinstance(v, str) else v}"

    for k in key_order:
        got = one(k)
        if got:
            parts.append(got)

    return "  ".join(parts)

```

