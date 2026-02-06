from __future__ import annotations

from connectors.runtime.events import emit


def http_request(*, stream: str, method: str, url: str) -> None:
    emit("message", "http.request.start", stream=stream, method=method, url=url)


def records_seen(*, stream: str, count: int) -> None:
    emit("count", "schema.records_seen", stream=stream, count=count)
