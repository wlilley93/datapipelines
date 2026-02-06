from __future__ import annotations

from typing import Any, Optional

from .constants import CONNECTOR_NAME


def emit_event(
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
            connector=CONNECTOR_NAME,
            stream=stream,
            count=count,
            level=level,
            **(fields or {}),
        )
    except Exception:
        pass


def debug(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="debug", **fields)


def info(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="info", **fields)


def warn(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="warn", **fields)


def error(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="error", **fields)
