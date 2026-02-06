from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Set, Tuple

from .errors import WassengerPagingLoopError
from .events import info, warn
from .http import extract_rows


def _pagination_hint(payload: Any) -> Tuple[Optional[int], Optional[int]]:
    """
    Best-effort extraction of (page, total_pages) from common API shapes.
    If unavailable, returns (None, None).

    Why:
    Some APIs cap page size or ignore `limit`, so "returned < requested_limit"
    is NOT a reliable signal that there are no more pages. If the API provides
    pagination metadata, we can stop safely when page >= total_pages.
    """
    if not isinstance(payload, dict):
        return None, None

    # Common shapes
    for container_key in ("pagination", "page", "meta", "_meta"):
        container = payload.get(container_key)
        if isinstance(container, dict):
            # Try common fields inside nested objects
            page = container.get("page") or container.get("currentPage") or container.get("current_page")
            total_pages = (
                container.get("totalPages")
                or container.get("total_pages")
                or container.get("pages")
                or container.get("pageCount")
            )
            try:
                page_i = int(page) if page is not None else None
            except Exception:
                page_i = None
            try:
                total_pages_i = int(total_pages) if total_pages is not None else None
            except Exception:
                total_pages_i = None
            if page_i is not None or total_pages_i is not None:
                return page_i, total_pages_i

    # Flat fields
    page = payload.get("page") or payload.get("currentPage") or payload.get("current_page")
    total_pages = payload.get("totalPages") or payload.get("total_pages") or payload.get("pages") or payload.get("pageCount")

    try:
        page_i = int(page) if page is not None else None
    except Exception:
        page_i = None
    try:
        total_pages_i = int(total_pages) if total_pages is not None else None
    except Exception:
        total_pages_i = None

    return page_i, total_pages_i


def paged_get(
    fetch_page_fn,
    *,
    stream: str,
    page_size: int,
    max_pages: int,
    collection_key: Optional[str],
) -> Iterable[Dict[str, Any]]:
    """
    Generic page-based pagination loop.

    fetch_page_fn(page:int, limit:int) -> payload

    IMPORTANT:
    Do NOT assume `returned < requested_limit` means "no more pages".
    Many APIs cap/ignore `limit` (e.g. always returning 20 rows), which would
    otherwise truncate extraction to a single page forever.
    """
    page = 1
    pages_seen: Set[int] = set()

    while True:
        if page > max_pages:
            warn("paging.max_pages_reached", stream=stream, page=page, max_pages=max_pages)
            break
        if page in pages_seen:
            raise WassengerPagingLoopError(f"Paging loop detected for {stream} at page={page}")
        pages_seen.add(page)

        info("paging.page.start", stream=stream, page=page, limit=page_size, max_pages=max_pages)
        payload = fetch_page_fn(page, page_size)
        rows = extract_rows(payload, collection_key=collection_key)

        if not rows:
            info("paging.done.empty_page", stream=stream, page=page, returned=0)
            break

        returned = 0
        for r in rows:
            if isinstance(r, dict):
                returned += 1
                yield r

        info("paging.page.done", stream=stream, page=page, returned=returned)

        # If the API provides explicit pagination metadata, honor it.
        cur_page, total_pages = _pagination_hint(payload)
        if cur_page is not None and total_pages is not None and cur_page >= total_pages:
            info("paging.done.meta", stream=stream, page=page, total_pages=total_pages)
            break

        # Otherwise, keep going until we hit an empty page (or max_pages).
        page += 1
