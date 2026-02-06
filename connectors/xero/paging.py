from __future__ import annotations

import time
from typing import Any, Dict, Iterable, Optional, Set, Tuple

from .events import error, info, warn


def paged_collection(
    fetch_page_fn,
    *,
    collection_key: str,
    stream: str,
    max_pages: int,
    progress_every_pages: int = 5,
) -> Iterable[Dict[str, Any]]:
    """
    Generic page= pagination loop for Xero “collection” endpoints.

    Observability notes:
      - We emit paging.page.start on EVERY page so the UI never looks frozen.
      - We emit paging.progress periodically (default: every 5 pages) for a calmer log stream.
      - If you want the old quieter behaviour, set progress_every_pages=25.
    """
    page = 1
    pages_seen: Set[int] = set()
    last_progress_emit = 0.0

    while True:
        if page > max_pages:
            warn("paging.max_pages_reached", stream=stream, page=page, max_pages=max_pages)
            break

        if page in pages_seen:
            error("paging.loop_detected", stream=stream, page=page)
            break

        pages_seen.add(page)

        # Emit a "heartbeat" every page so it never appears stuck.
        info("paging.page.start", stream=stream, page=page, max_pages=max_pages)

        payload = fetch_page_fn(page) or {}

        rows = payload.get(collection_key) or []
        if not rows:
            info("paging.done.last_page", stream=stream, page=page, returned=0)
            break

        returned = 0
        for r in rows:
            if isinstance(r, dict):
                returned += 1
                yield r

        # Periodic progress (every N pages) plus a time-based fallback.
        now = time.monotonic()
        if page == 1 or (progress_every_pages > 0 and page % progress_every_pages == 0) or (now - last_progress_emit) > 10:
            info("paging.progress", stream=stream, page=page, returned=returned)
            last_progress_emit = now

        page += 1


def first_non_empty_collection_key(payload: Dict[str, Any], candidates: Tuple[str, ...]) -> Optional[str]:
    for k in candidates:
        v = payload.get(k)
        if isinstance(v, list):
            return k
    return None
