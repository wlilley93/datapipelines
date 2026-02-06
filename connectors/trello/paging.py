from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from .constants import PAGE_LIMIT
from .events import debug, warn
from .http import as_list, request_json_logged
from .utils_bridge import DEFAULT_TIMEOUT

def paged_list(
    session,
    url: str,
    *,
    params: Dict[str, Any],
    limit: int = PAGE_LIMIT,
    before: Optional[str] = None,
    since: Optional[str] = None,
    stream: Optional[str] = None,
    board_id: Optional[str] = None,
    op: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterable[Dict[str, Any]]:
    """Paginate using Trello 'before' cursor (backward pagination)."""
    cursor = before
    page = 0

    while True:
        page += 1
        request_params = dict(params)
        request_params["limit"] = limit
        if cursor:
            request_params["before"] = cursor
        if since:
            request_params["since"] = since

        debug(
            "paging.page.start",
            stream=stream,
            op=op,
            page=page,
            board_id=board_id,
            limit=limit,
            cursor=str(cursor)[:24] if cursor else None,
        )

        data = request_json_logged(
            session,
            "GET",
            url,
            params=request_params,
            timeout=timeout,
            stream=stream,
            board_id=board_id,
            op=op or "paged_list",
        )
        items = as_list(data, stream=stream or "unknown", op=op or "paged_list", board_id=board_id)

        if not items:
            debug("paging.done.empty", stream=stream, op=op, page=page, board_id=board_id)
            break

        for item in items:
            if isinstance(item, dict):
                yield item

        if len(items) < limit:
            debug("paging.done.partial", stream=stream, op=op, page=page, board_id=board_id, returned=len(items))
            break

        last_item = items[-1]
        next_cursor = last_item.get("id") if isinstance(last_item, dict) else None

        if not next_cursor or next_cursor == cursor:
            warn("paging.done.stalled", stream=stream, op=op, page=page, board_id=board_id, returned=len(items))
            break

        cursor = next_cursor
