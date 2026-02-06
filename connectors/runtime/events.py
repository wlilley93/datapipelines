"""
Runtime event bus for connector → orchestrator progress reporting.

Design rules:
- CLI-agnostic: no Rich / printing here.
- Lightweight: connectors can emit small progress messages without depending on UI.
- Safe default: if no emitter is configured, events are ignored.

Typical usage inside a connector:
  from connectors.runtime.events import emit

  emit("progress", "Fetching boards…", stream="boards")
  emit("count", "Cards fetched", stream="cards", count=250)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

EventEmitter = Callable[["RuntimeEvent"], None]

_EMITTER: Optional[EventEmitter] = None


@dataclass(frozen=True)
class RuntimeEvent:
    type: str
    message: str
    connector: Optional[str] = None
    stream: Optional[str] = None
    count: Optional[int] = None
    level: str = "info"  # info|warn|error|debug
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    fields: Dict[str, Any] = field(default_factory=dict)


def set_emitter(fn: Optional[EventEmitter]) -> None:
    """
    Install a process-wide event emitter.

    The CLI should set this before running a connector.
    """
    global _EMITTER
    _EMITTER = fn


def emit(
    event_type: str,
    message: str,
    *,
    connector: Optional[str] = None,
    stream: Optional[str] = None,
    count: Optional[int] = None,
    level: str = "info",
    **fields: Any,
) -> None:
    """
    Emit a runtime event. No-op if no emitter is installed.
    """
    fn = _EMITTER
    if fn is None:
        return

    try:
        fn(
            RuntimeEvent(
                type=str(event_type),
                message=str(message),
                connector=connector,
                stream=stream,
                count=count,
                level=str(level),
                fields=fields or {},
            )
        )
    except Exception:
        # Never allow progress reporting to crash a connector run.
        return
