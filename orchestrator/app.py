#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from contextlib import nullcontext

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
from .postgres_ops import (
    configure_postgres,
    test_postgres_connection,
    configure_db_target,
    copy_local_schema_to_aws,
    sync_local_db_to_aws,
    sync_local_data_to_aws,
    sync_local_to_aws,
)
from .aws_doctor import aws_tunnel_doctor
from .run_connection import run_connection  # type: ignore
from .run_history_ui import show_recent_runs
from .secrets import delete_secret, update_secret
from .ui import render_api_error_panel
from .ssm_tunnel import maybe_open_ssm_tunnel

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
    retries = 2  # Assume 2 retries for batch syncs

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
            "calendly - Scheduling",
            "↩️ Back",
        ],
    ).ask()
    if not raw_choice or "Back" in raw_choice:
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
    elif connector_type == "calendly":
        console.print("[dim]Create a Personal Access Token in Calendly (Integrations → API & Webhooks).[/dim]")
        secrets["access_token"] = questionary.password("Personal Access Token:").ask()
        base_url = questionary.text("Base API URL (optional, defaults to https://api.calendly.com):").ask()
        if base_url and base_url.strip():
            secrets["base_url"] = base_url.strip()
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

    options = [f"{'✅' if c.get('enabled', True) else '⏸️'} {c['name']}" for c in conns]
    options.append("↩️ Back")
    sel = questionary.select(
        "Toggle which connection?",
        choices=options,
    ).ask()
    if not sel or "Back" in sel:
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

    options = [c["name"] for c in conns]
    options.append("↩️ Back")
    sel = questionary.select("Reset state for:", choices=options).ask()
    if not sel or "Back" in sel:
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

    options = [c["name"] for c in conns]
    options.append("↩️ Back")
    sel = questionary.select("Delete which connection?", choices=options).ask()
    if not sel or "Back" in sel:
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

        with maybe_open_ssm_tunnel(cfg):
            pg_ok = test_postgres_connection()

        pg_status = "[green]Connected[/green]" if pg_ok else "[red]Not connected[/red]"

        db_mode = cfg.get("settings", {}).get("db_mode", "local")
        mode_label = "AWS" if db_mode == "aws" else "Local"

        conns = list_connections(cfg)
        enabled = sum(1 for c in conns if c.get("enabled", True))

        status_table = Table(show_header=False, box=None)
        status_table.add_row(f"🗃️  Postgres ({mode_label}):", pg_status)
        status_table.add_row("🔌 Connections:", f"{len(conns)} total / {enabled} enabled")
        status_table.add_row("🧾 Run logs:", os.path.abspath(RUNS_DIR))
        status_table.add_row("🧬 Drift snapshots:", os.path.abspath(SCHEMA_SNAPSHOTS_DIR))
        console.print(Panel(status_table, title="Status"))

        action = questionary.select(
            "Choose action:",
            choices=[
                "🚀 Run Selected Syncs",
                "🎯 Run One Sync",
                "⚙️  Settings",
                "👋 Exit",
            ],
        ).ask()

        if not action or "Exit" in action:
            sys.exit(0)

        if not pg_ok and ("Run" in action):
            console.print("[yellow]Postgres not connected. Configure Postgres first.[/yellow]")
            input("\nPress Enter...")
            continue

        if "Run Selected Syncs" in action:
            run_all_connections(cfg)
            continue

        if "Run One Sync" in action:
            conns = [c for c in list_connections(cfg) if c.get("enabled", True)]
            if not conns:
                console.print("[yellow]No enabled connections.[/yellow]")
                input("\nPress Enter...")
                continue

            options = [c["name"] for c in conns]
            options.append("↩️ Back")
            sel = questionary.select("Select:", choices=options).ask()
            if not sel or "Back" in sel:
                continue

            conn = get_connection(cfg, sel)
            if not conn:
                continue

            run_connection(cfg, conn, retries=2)
            continue

        if "Settings" in action:
            while True:
                console.clear()
                console.print(Panel("[bold cyan]Settings[/bold cyan]"))
                settings_action = questionary.select(
                    "Choose settings category:",
                    choices=[
                        "🔄 Sync Settings",
                        "🗄️  Database Settings",
                        "💻 Codebase Settings",
                        "↩️ Back",
                    ],
                ).ask()

                if not settings_action or "Back" in settings_action:
                    break

                if "Sync Settings" in settings_action:
                    while True:
                        console.clear()
                        console.print(Panel("[bold cyan]Sync Settings[/bold cyan]"))
                        sync_op = questionary.select(
                            "Choose operation:",
                            choices=[
                                "✨ Add Connection",
                                "✅ Enable/Disable Connection",
                                "🗑️  Delete Connection",
                                "↩️ Back",
                            ],
                        ).ask()

                        if not sync_op or "Back" in sync_op:
                            break

                        if "Add Connection" in sync_op:
                            flow_add_connector(cfg)
                        elif "Enable/Disable" in sync_op:
                            toggle_connection(cfg)
                        elif "Delete Connection" in sync_op:
                            delete_connection_flow(cfg)

                elif "Database Settings" in settings_action:
                    while True:
                        console.clear()
                        console.print(Panel("[bold cyan]Database Settings[/bold cyan]"))
                        db_op = questionary.select(
                            "Choose operation:",
                            choices=[
                            "🐳 Start Docker Services",
                            "🛠️ Configure Docker",
                            "🗃️ Configure Postgres",
                            "🔀 Switch Database Target",
                            "🔄 Schema Sync to AWS",
                            "🚚 Sync Local → AWS (Schema + Data)",
                            "⬆️ Full DB Sync to AWS",
                            "📦 Data Sync to AWS",
                            "🩺 AWS Tunnel Doctor",
                            "♻️ Reset Connection State to Database",
                            "📜 View Run History (DB)",
                                "↩️ Back",
                            ],
                        ).ask()

                        if not db_op or "Back" in db_op:
                            break

                        # Reload config to capture any changes (e.g. DB target switch)
                        cfg = load_config_v2()

                        # Force AWS mode for "Schema Sync" if not already set,
                        # so the tunnel opens for the destination.
                        is_schema_sync = "Schema Sync" in db_op
                        is_full_db_sync = "Full DB Sync" in db_op

                        # Schema Sync + Full DB Sync open the tunnel just-in-time (after prompts),
                        # so avoid starting an SSM session early while the user is still answering prompts.
                        if is_schema_sync or is_full_db_sync:
                            ctx = nullcontext()
                        else:
                            ctx = maybe_open_ssm_tunnel(cfg)

                        with ctx:
                            if "Start Docker" in db_op:
                                start_docker_services()
                            elif "Configure Docker" in db_op:
                                init_docker_environment()
                            elif "Configure Postgres" in db_op:
                                configure_postgres()
                            elif "Switch Database Target" in db_op:
                                configure_db_target()
                            elif "Schema Sync" in db_op:
                                copy_local_schema_to_aws()
                            elif "Sync Local → AWS" in db_op:
                                sync_local_to_aws()
                            elif "Full DB Sync" in db_op:
                                sync_local_db_to_aws()
                            elif "Data Sync" in db_op:
                                sync_local_data_to_aws()
                            elif "AWS Tunnel Doctor" in db_op:
                                aws_tunnel_doctor()
                            elif "Reset Connection State" in db_op:
                                reset_state(cfg)
                            elif "View Run History" in db_op:
                                show_recent_runs()

                elif "Codebase Settings" in settings_action:
                    while True:
                        console.clear()
                        console.print(Panel("[bold cyan]Codebase Settings[/bold cyan]"))
                        code_op = questionary.select(
                            "Choose operation:",
                            choices=[
                                "📚 Generate All Documentation",
                                "↩️ Back",
                            ],
                        ).ask()

                        if not code_op or "Back" in code_op:
                            break

                        if "Generate All Documentation" in code_op:
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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
