from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, Optional, Tuple
from urllib.parse import parse_qs, urlparse

# NOTE:
# - `iterate_pages` is a generic loop helper.
# - `list_all` is the Intercom-specific paginator used by stream resources.
# - `extract_items` is a helper used by schema_expand.py.


@dataclass(frozen=True)
class PageAdvance:
    """
    Represents how to request the next page.

    - params: query params for the next request
    - fingerprint: stable progress marker to detect non-advancing loops
    """
    params: Dict[str, Any]
    fingerprint: str


def iterate_pages(
    *,
    fetch_page: Callable[[Dict[str, Any]], Dict[str, Any]],
    initial_params: Dict[str, Any],
    get_next: Callable[[Dict[str, Any], Dict[str, Any]], Optional[PageAdvance]],
    max_pages: Optional[int] = None,
) -> Iterator[Tuple[int, Dict[str, Any], Dict[str, Any]]]:
    """
    Why: centralizes “advance-or-stop” pagination with a loop guard so you
    don’t refetch page 1 forever if tokens aren’t applied.

    Yields: (page_index, request_params, response_json)
    """
    page_idx = 0
    params = dict(initial_params)
    prev_fingerprint: Optional[str] = None

    while True:
        if max_pages is not None and page_idx >= max_pages:
            return

        resp = fetch_page(params)
        yield page_idx, dict(params), resp

        adv = get_next(params, resp)
        if adv is None:
            return

        if prev_fingerprint is not None and adv.fingerprint == prev_fingerprint:
            raise RuntimeError(
                "Pagination is not advancing (fingerprint repeated). "
                "This would cause an infinite loop. Check next-page extraction."
            )

        prev_fingerprint = adv.fingerprint
        params = dict(adv.params)
        page_idx += 1


def _extract_query_param(url: str, key: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(url).query)
        vals = q.get(key)
        if not vals:
            return None
        return vals[0]
    except Exception:
        return None


def extract_items(resp: Any, fallback_keys: tuple[str, ...] = ("data",)) -> list[Any]:
    """
    Best-effort extraction of list items from typical Intercom responses.

    - Most endpoints: {"data": [...]}
    - Some endpoints: {"conversations": [...]}, {"contacts":[...]}, {"companies":[...]}
    """
    if isinstance(resp, list):
        return resp
    if not isinstance(resp, dict):
        return []

    for k in fallback_keys:
        v = resp.get(k)
        if isinstance(v, list):
            return v

    for k in ("data", "conversations", "contacts", "companies", "tickets", "admins", "teams", "segments", "tags", "articles"):
        v = resp.get(k)
        if isinstance(v, list):
            return v

    return []


def list_all(
    rid: str,
    cfg: Any,
    session: Any,
    *,
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
    stream: str,
    max_pages: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Iterate all items for an Intercom list endpoint.

    Why:
    - Intercom uses a `pages` object with `next` either:
      - dict containing `starting_after`
      - or a URL string containing `starting_after`
    - Some endpoints return items under different keys; we use extract_items().
    """

    from .http import req_json  # local import avoids broad import chains

    def fetch(p: Dict[str, Any]) -> Dict[str, Any]:
        data = req_json(rid, cfg, session, method="GET", url=url, headers=headers, params=p, stream=stream)
        return data or {}

    def get_next(p: Dict[str, Any], resp: Dict[str, Any]) -> Optional[PageAdvance]:
        pages = resp.get("pages") or {}
        nxt = pages.get("next")
        if not nxt:
            return None

        if isinstance(nxt, dict):
            sa = nxt.get("starting_after")
            if not sa:
                return None
            new_params = dict(p)
            new_params["starting_after"] = sa
            return PageAdvance(params=new_params, fingerprint=str(sa))

        if isinstance(nxt, str):
            sa = _extract_query_param(nxt, "starting_after")
            if not sa:
                return None
            new_params = dict(p)
            new_params["starting_after"] = sa
            return PageAdvance(params=new_params, fingerprint=str(sa))

        return None

    initial = dict(params or {})

    for _page_idx, _req_params, resp in iterate_pages(
        fetch_page=fetch,
        initial_params=initial,
        get_next=get_next,
        max_pages=max_pages,
    ):
        for item in extract_items(resp, fallback_keys=("data",)):
            if isinstance(item, dict):
                yield item
