from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional, Union


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------------
# Message types (Airbyte-ish)
# -----------------------------
@dataclass(frozen=True)
class LogMessage:
    type: Literal["LOG"] = "LOG"
    level: Literal["DEBUG", "INFO", "WARN", "ERROR"] = "INFO"
    message: str = ""
    emitted_at: str = _utc_now_iso()


@dataclass(frozen=True)
class RecordMessage:
    type: Literal["RECORD"] = "RECORD"
    stream: str = ""
    record: Dict[str, Any] = None  # type: ignore[assignment]
    emitted_at: str = _utc_now_iso()

    def __post_init__(self) -> None:
        # dataclass(frozen=True) means we need object.__setattr__
        if self.record is None:
            object.__setattr__(self, "record", {})


@dataclass(frozen=True)
class StateMessage:
    type: Literal["STATE"] = "STATE"
    state: Dict[str, Any] = None  # type: ignore[assignment]
    emitted_at: str = _utc_now_iso()

    def __post_init__(self) -> None:
        if self.state is None:
            object.__setattr__(self, "state", {})


@dataclass(frozen=True)
class SchemaMessage:
    type: Literal["SCHEMA"] = "SCHEMA"
    stream: str = ""
    schema: Dict[str, Any] = None  # type: ignore[assignment]
    emitted_at: str = _utc_now_iso()

    def __post_init__(self) -> None:
        if self.schema is None:
            object.__setattr__(self, "schema", {})


@dataclass(frozen=True)
class TraceMessage:
    type: Literal["TRACE"] = "TRACE"
    kind: Literal["STATS", "ERROR", "INFO"] = "INFO"
    data: Dict[str, Any] = None  # type: ignore[assignment]
    emitted_at: str = _utc_now_iso()

    def __post_init__(self) -> None:
        if self.data is None:
            object.__setattr__(self, "data", {})


Message = Union[LogMessage, RecordMessage, StateMessage, SchemaMessage, TraceMessage]


# -----------------------------
# Serialisation helpers
# -----------------------------
def as_dict(msg: Message) -> Dict[str, Any]:
    # dataclasses: easiest path is __dict__ since all fields are JSON-friendly
    return dict(msg.__dict__)


def to_jsonl(msg: Message) -> str:
    return json.dumps(as_dict(msg), ensure_ascii=False, default=str)


def try_parse(d: Dict[str, Any]) -> Optional[Message]:
    """
    Best-effort parser for JSON objects that look like our messages.
    Useful for reading run artifacts later, not required for runtime.
    """
    t = (d.get("type") or "").upper()
    if t == "LOG":
        return LogMessage(level=d.get("level", "INFO"), message=d.get("message", ""), emitted_at=d.get("emitted_at", _utc_now_iso()))
    if t == "RECORD":
        return RecordMessage(stream=d.get("stream", ""), record=d.get("record") or {}, emitted_at=d.get("emitted_at", _utc_now_iso()))
    if t == "STATE":
        return StateMessage(state=d.get("state") or {}, emitted_at=d.get("emitted_at", _utc_now_iso()))
    if t == "SCHEMA":
        return SchemaMessage(stream=d.get("stream", ""), schema=d.get("schema") or {}, emitted_at=d.get("emitted_at", _utc_now_iso()))
    if t == "TRACE":
        return TraceMessage(kind=d.get("kind", "INFO"), data=d.get("data") or {}, emitted_at=d.get("emitted_at", _utc_now_iso()))
    return None
