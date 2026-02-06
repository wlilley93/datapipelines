from __future__ import annotations

from typing import Any, Dict, Iterable

from .constants import DEFAULT_PAGE_SIZE


def iter_paged_results(
    fetch_page,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    start_page: int = 1,
) -> Iterable[Dict[str, Any]]:
    """
    PandaDoc documents list API is page-based.
    fetch_page(page:int, count:int) => response dict.
    """
    page = start_page
    max_pages = 1000  # Prevent infinite loops

    while page <= max_pages:
        try:
            data = fetch_page(page, page_size) or {}
            rows = data.get("results") or []

            # If no results returned, we've reached the end
            if not rows:
                break

            # If rows is not a list, there might be an API error response
            if not isinstance(rows, list):
                break

            for row in rows:
                if isinstance(row, dict):
                    yield row
                else:
                    # If a row is not a dict, it might be an error response
                    # Skip non-dict rows to avoid processing errors
                    continue

            # If we got fewer results than page_size, this is likely the last page
            if len(rows) < page_size:
                break

            page += 1
        except Exception as e:
            # Log the error but don't crash - just break out of pagination
            import logging
            logging.warning(f"Pandadoc pagination error on page {page}: {str(e)}")
            break

    if page > max_pages:
        import logging
        logging.warning(f"Pandadoc pagination exceeded max pages ({max_pages}), stopping")
