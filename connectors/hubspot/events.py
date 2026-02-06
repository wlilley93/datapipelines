# connectors/hubspot/events.py
from __future__ import annotations

from typing import Any, Dict, Optional

from connectors.runtime.events import emit


def http_start(*, stream: str, method: str, url: str, extra: Optional[Dict[str, Any]] = None) -> None:
    emit(
        "message",
        "http.request.start",
        stream=stream,
        method=method,
        url=url,
        **(extra or {}),
    )


def http_ok(
    *,
    stream: str,
    method: str,
    url: str,
    elapsed_ms: int,
    items_count: Optional[int] = None,
    keys_count: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    fields: Dict[str, Any] = dict(extra or {})
    fields.update({"method": method, "url": url, "elapsed_ms": elapsed_ms})
    if isinstance(items_count, int):
        fields["items_count"] = items_count
    if isinstance(keys_count, int):
        fields["keys_count"] = keys_count
    emit("message", "http.request.ok", stream=stream, **fields)


def http_error(
    *,
    stream: str,
    method: str,
    url: str,
    status: Optional[int],
    error: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    fields: Dict[str, Any] = dict(extra or {})
    fields.update({"method": method, "url": url, "status": status, "error": error})
    emit("message", "http.request.error", stream=stream, **fields)


def paging_start(*, stream: str, page: int, limit: int) -> None:
    emit("message", "paging.page.start", stream=stream, page=page, limit=limit)


def paging_done(*, stream: str, returned: int) -> None:
    emit("message", "paging.done.last_page", stream=stream, returned=returned)


def records_seen(*, stream: str, count: int) -> None:
    emit("count", "schema.records_seen", stream=stream, count=count)


def schema_diff(*, stream: str, added_count: int, missing_count: int) -> None:
    emit(
        "message",
        "schema.diff",
        stream=stream,
        added_count=added_count,
        missing_count=missing_count,
    )
