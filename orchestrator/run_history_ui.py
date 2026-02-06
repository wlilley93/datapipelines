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
