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
