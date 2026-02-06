from __future__ import annotations

from typing import Any, Optional

from .constants import _CONNECTOR_NAME


def _emit_event(
    event_type: str,
    message: str,
    *,
    stream: Optional[str] = None,
    count: Optional[int] = None,
    level: str = "info",
    **fields: Any,
) -> None:
    """Emit structured event to runtime bus. Fails silently."""
    try:
        from connectors.runtime.events import emit
    except Exception:
        return

    try:
        emit(
            event_type,
            message,
            connector=_CONNECTOR_NAME,
            stream=stream,
            count=count,
            level=level,
            **(fields or {}),
        )
    except Exception:
        pass


def debug(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    _emit_event("message", message, stream=stream, level="debug", **fields)


def info(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    _emit_event("message", message, stream=stream, level="info", **fields)


def warn(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    _emit_event("message", message, stream=stream, level="warn", **fields)


def error(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    _emit_event("message", message, stream=stream, level="error", **fields)


def records(stream: str, count: int, *, message: str = "records") -> None:
    """
    Emit a record-counting event that the orchestrator UI understands.

    The orchestrator updates "Total records (reported)" ONLY when it receives
    events with:
      - type in {"count","records","rows"}
      - an integer `count`

    We use event_type="records" so run_connection.py will pick it up.
    """
    try:
        _emit_event("records", message, stream=stream, count=int(count), level="info")
    except Exception:
        pass
