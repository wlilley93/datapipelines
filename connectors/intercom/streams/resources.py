from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import requests

from ..http import req_json
from ..pagination import list_all


# =========================================
# SECTION 0 — PUBLIC EXPORTS
# Why: keep a stable surface area to prevent repeated “cannot import name …” failures.
# =========================================
__all__ = [
    # Core resources
    "contacts_resource",
    "companies_resource",
    "conversations_resource",
    "tickets_resource",
    # Additional streams
    "admins_resource",
    "teams_resource",
    "segments_resource",
    "tags_resource",
    "help_center_collections_resource",
    "help_center_sections_resource",
    "articles_resource",
    # Transformer
    "conversation_parts_transformer",
]


# =========================================
# SECTION 1 — CALL SIGNATURE SHIM
# Why: allow calls like:
#   - resource(cfg, cursor=..., **kw)
#   - resource(rid, cfg, session, headers, cursor=..., **kw)
# Without exploding on unexpected args/kwargs.
# =========================================
@dataclass(frozen=True)
class _ResolvedCall:
    rid: str
    cfg: Any
    cursor: Any
    session: Optional[requests.Session]
    headers: Optional[Dict[str, str]]
    kwargs: Dict[str, Any]


def _looks_like_cfg(obj: Any) -> bool:
    if obj is None:
        return False
    for attr in ("access_token", "token", "api_key", "base_url", "intercom_version"):
        if hasattr(obj, attr):
            return True
    return False


def _resolve_call(args: Tuple[Any, ...], kwargs: Dict[str, Any], *, default_rid: str = "intercom") -> _ResolvedCall:
    cursor = kwargs.pop("cursor", None)
    rid = str(kwargs.pop("rid", default_rid))

    session: Optional[requests.Session] = kwargs.pop("session", None)
    headers: Optional[Dict[str, str]] = kwargs.pop("headers", None)

    cfg: Any = kwargs.pop("cfg", None)

    # Style A: (cfg, ...)
    if cfg is None and len(args) >= 1 and _looks_like_cfg(args[0]):
        cfg = args[0]
        return _ResolvedCall(rid=rid, cfg=cfg, cursor=cursor, session=session, headers=headers, kwargs=kwargs)

    # Style B: (rid, cfg, session, headers, ...)
    if cfg is None and len(args) >= 2 and _looks_like_cfg(args[1]):
        rid = str(args[0])
        cfg = args[1]
        if session is None and len(args) >= 3 and isinstance(args[2], requests.Session):
            session = args[2]
        if headers is None and len(args) >= 4 and isinstance(args[3], dict):
            headers = args[3]
        return _ResolvedCall(rid=rid, cfg=cfg, cursor=cursor, session=session, headers=headers, kwargs=kwargs)

    # Fallback: try last arg as cfg
    if cfg is None and len(args) >= 1 and _looks_like_cfg(args[-1]):
        cfg = args[-1]
        if len(args) >= 2:
            rid = str(args[0])
        return _ResolvedCall(rid=rid, cfg=cfg, cursor=cursor, session=session, headers=headers, kwargs=kwargs)

    if cfg is None:
        raise TypeError(
            "Intercom resource called without a recognizable cfg. "
            "Expected (cfg, ...) or (rid, cfg, session, headers, ...)."
        )

    return _ResolvedCall(rid=rid, cfg=cfg, cursor=cursor, session=session, headers=headers, kwargs=kwargs)


# =========================================
# SECTION 2 — CONFIG ACCESS + DEFAULTS
# Why: streams should work even if cfg doesn’t have every knob defined.
# =========================================
def _get(cfg: Any, name: str, default: Any) -> Any:
    return getattr(cfg, name, default)


def _base_url(cfg: Any) -> str:
    return (_get(cfg, "base_url", "https://api.intercom.io") or "https://api.intercom.io").rstrip("/")


def _auth_headers(cfg: Any) -> Dict[str, str]:
    token = _get(cfg, "access_token", None) or _get(cfg, "token", None) or _get(cfg, "api_key", None)
    headers: Dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": str(_get(cfg, "intercom_version", "2.10")),
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _timeout_s(cfg: Any) -> float:
    return float(_get(cfg, "request_timeout_s", 60.0))


def _hydrate_workers(cfg: Any) -> int:
    return int(_get(cfg, "hydrate_workers", 4))


def _hydrate_max_in_flight(cfg: Any) -> int:
    v = _get(cfg, "hydrate_max_in_flight", None)
    if v is not None:
        return int(v)
    return max(8, _hydrate_workers(cfg) * 4)


def _hydrate_wait_timeout_s(cfg: Any) -> float:
    return float(_get(cfg, "hydrate_wait_timeout_s", 120.0))


def _fail_hard(cfg: Any) -> bool:
    return bool(_get(cfg, "fail_hard", False))


def _default_per_page(cfg: Any) -> int:
    return int(_get(cfg, "page_size", 150))


# =========================================
# SECTION 3 — REQUEST WRAPPER
# Why: enforce timeout for *every* request through your existing req_json.
# =========================================
def _req_json(
    *,
    rid: str,
    cfg: Any,
    sess: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
    stream: str,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Any:
    return req_json(
        rid,
        cfg,
        sess,
        method=method,
        url=url,
        headers=headers,
        params=params,
        payload=payload,
        stream=stream,
        timeout=_timeout_s(cfg),
    )


# =========================================
# SECTION 4 — BOUNDED HYDRATION
# Why: avoid “stuck on starting” due to hung socket reads / futures never completing.
# =========================================
@dataclass(frozen=True)
class _HydrateJob:
    id: str
    url: str


def _hydrate_one(
    job: _HydrateJob, *, rid: str, cfg: Any, sess: requests.Session, headers: Dict[str, str], stream: str
) -> Dict[str, Any]:
    details = _req_json(rid=rid, cfg=cfg, sess=sess, method="GET", url=job.url, headers=headers, stream=stream)
    if isinstance(details, dict) and "id" not in details:
        details["id"] = job.id
    return details


def _drain_futures(
    *,
    in_flight: Sequence[Future],
    stream: str,
    wait_timeout_s: float,
) -> Iterator[Tuple[Optional[Dict[str, Any]], Optional[BaseException]]]:
    start = time.time()
    remaining = list(in_flight)

    while remaining:
        budget = max(0.0, wait_timeout_s - (time.time() - start))
        if budget <= 0.0:
            for fut in remaining:
                fut.cancel()
                yield None, TimeoutError(f"{stream}: hydrate timed out; canceled future")
            return

        try:
            for fut in as_completed(remaining, timeout=budget):
                remaining.remove(fut)
                try:
                    yield fut.result(), None
                except BaseException as e:  # noqa: BLE001
                    yield None, e
        except TimeoutError:
            continue


def _hydrate_many(*, rid: str, cfg: Any, stream: str, jobs: Iterable[_HydrateJob]) -> Iterator[Dict[str, Any]]:
    headers = _auth_headers(cfg)
    max_workers = _hydrate_workers(cfg)
    max_in_flight = _hydrate_max_in_flight(cfg)
    wait_timeout_s = _hydrate_wait_timeout_s(cfg)

    with requests.Session() as sess, ThreadPoolExecutor(max_workers=max_workers) as pool:
        in_flight: List[Future] = []

        def submit(job: _HydrateJob) -> None:
            in_flight.append(pool.submit(_hydrate_one, job, rid=rid, cfg=cfg, sess=sess, headers=headers, stream=stream))

        for job in jobs:
            submit(job)
            if len(in_flight) >= max_in_flight:
                for result, err in _drain_futures(in_flight=in_flight, stream=stream, wait_timeout_s=wait_timeout_s):
                    if err is not None:
                        if _fail_hard(cfg):
                            raise err
                        continue
                    if result is not None:
                        yield result
                in_flight.clear()

        if in_flight:
            for result, err in _drain_futures(in_flight=in_flight, stream=stream, wait_timeout_s=wait_timeout_s):
                if err is not None:
                    if _fail_hard(cfg):
                        raise err
                    continue
                if result is not None:
                    yield result


# =========================================
# SECTION 5 — LIST + SEARCH HELPERS
# Why: use list_all paginator for full scans and a search helper for incremental.
# =========================================
def _list_endpoint(
    *,
    rid: str,
    cfg: Any,
    sess: requests.Session,
    url: str,
    headers: Dict[str, str],
    stream: str,
    params: Optional[Dict[str, Any]] = None,
) -> Iterator[Dict[str, Any]]:
    try:
        yield from list_all(rid, cfg, sess, url=url, headers=headers, params=params or {}, stream=stream)
    except BaseException:
        if _fail_hard(cfg):
            raise
        return


def _search_endpoint(
    *,
    rid: str,
    cfg: Any,
    sess: requests.Session,
    url: str,
    headers: Dict[str, str],
    stream: str,
    query: Dict[str, Any],
    page_size: int,
) -> Iterator[Dict[str, Any]]:
    """
    Generic search paginator for Intercom-style search endpoints.

    Expects responses of the form:
      { "data": [...], "pages": { "next": { "starting_after": "..." } } }
    """
    payload: Dict[str, Any] = {
        "query": query or {},
        "pagination": {"per_page": page_size},
    }

    try:
        while True:
            resp = _req_json(
                rid=rid,
                cfg=cfg,
                sess=sess,
                method="POST",
                url=url,
                headers=headers,
                stream=stream,
                payload=payload,
            ) or {}

            items = resp.get("data") or []
            if not isinstance(items, list):
                items = []

            for item in items:
                if isinstance(item, dict):
                    yield item

            pages = resp.get("pages") or {}
            nxt = pages.get("next")
            if not nxt:
                break

            starting_after: Optional[str] = None
            if isinstance(nxt, dict):
                starting_after = nxt.get("starting_after")

            if not starting_after:
                break

            payload["pagination"]["starting_after"] = starting_after

    except BaseException:
        if _fail_hard(cfg):
            raise
        return


def _obj_id(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    v = obj.get("id")
    if v is None:
        return None
    return str(v)


# =========================================
# SECTION 6 — STREAM SPECS
# Why: define all streams once; each resource uses the same machinery.
#       - updated_since_param: query param to pass cursor on list endpoints
#       - search_url/search_updated_field: POST search endpoint + field for cursor
# =========================================
@dataclass(frozen=True)
class _StreamSpec:
    name: str
    stream: str
    list_url: str
    per_page: bool
    detail_path: Optional[str]
    max_per_page: Optional[int] = None
    updated_since_param: Optional[str] = None  # e.g. "updated_since"
    search_url: Optional[str] = None           # e.g. ".../contacts/search"
    search_updated_field: Optional[str] = None # e.g. "updated_at"


def _specs(cfg: Any) -> Dict[str, _StreamSpec]:
    b = _base_url(cfg)
    return {
        # NOTE: companies are kept as “full list” only; no search_url / updated_since_param
        "companies": _StreamSpec(
            name="companies",
            stream="intercom_companies",
            list_url=f"{b}/companies",
            per_page=True,
            detail_path="companies",
            max_per_page=50,
            updated_since_param=None,
            search_url=None,
            search_updated_field=None,
        ),
        "contacts": _StreamSpec(
            name="contacts",
            stream="intercom_contacts",
            list_url=f"{b}/contacts",
            per_page=True,
            detail_path="contacts",
            max_per_page=None,
            # If your list endpoint supports incremental via query param, set this.
            updated_since_param=None,
            # Incremental via search endpoint:
            search_url=f"{b}/contacts/search",
            search_updated_field="updated_at",
        ),
        "conversations": _StreamSpec(
            name="conversations",
            stream="intercom_conversations",
            list_url=f"{b}/conversations",
            per_page=True,
            detail_path="conversations",
            max_per_page=None,
            updated_since_param=None,
            search_url=f"{b}/conversations/search",
            search_updated_field="updated_at",
        ),
        "tickets": _StreamSpec(
            name="tickets",
            stream="intercom_tickets",
            list_url=f"{b}/tickets",
            per_page=True,
            detail_path="tickets",
            max_per_page=None,
            updated_since_param=None,
            search_url=f"{b}/tickets/search",
            search_updated_field="updated_at",
        ),
        "admins": _StreamSpec(
            name="admins",
            stream="intercom_admins",
            list_url=f"{b}/admins",
            per_page=False,
            detail_path="admins",
        ),
        "teams": _StreamSpec(
            name="teams",
            stream="intercom_teams",
            list_url=f"{b}/teams",
            per_page=False,
            detail_path="teams",
        ),
        "segments": _StreamSpec(
            name="segments",
            stream="intercom_segments",
            list_url=f"{b}/segments",
            per_page=False,
            detail_path="segments",
        ),
        "tags": _StreamSpec(
            name="tags",
            stream="intercom_tags",
            list_url=f"{b}/tags",
            per_page=False,
            detail_path=None,
        ),
        "help_center_collections": _StreamSpec(
            name="help_center_collections",
            stream="intercom_help_center_collections",
            list_url=f"{b}/help_center/collections",
            per_page=False,
            detail_path="help_center/collections",
        ),
        "help_center_sections": _StreamSpec(
            name="help_center_sections",
            stream="intercom_help_center_sections",
            list_url=f"{b}/help_center/sections",
            per_page=False,
            detail_path="help_center/sections",
        ),
        "articles": _StreamSpec(
            name="articles",
            stream="intercom_articles",
            list_url=f"{b}/articles",
            per_page=True,
            detail_path="articles",
            max_per_page=None,
            updated_since_param=None,
            search_url=f"{b}/articles/search",
            search_updated_field="updated_at",
        ),
    }


def _stream_items(*, rid: str, cfg: Any, spec: _StreamSpec, cursor: Optional[int] = None) -> Iterator[Dict[str, Any]]:
    """
    Unified entry point for a stream:
    - First run (cursor is None): full list using list endpoint.
    - Incremental (cursor set):
        * If search_url/search_updated_field set: use POST search with updated_at > cursor.
        * Else if updated_since_param set: pass cursor as query param to list endpoint.
        * Else: fall back to full list.
    """
    headers = _auth_headers(cfg)
    base = _base_url(cfg)

    params: Dict[str, Any] = {}
    per_page: Optional[int] = None
    if spec.per_page:
        per_page = _default_per_page(cfg)
        if spec.max_per_page is not None:
            per_page = min(per_page, int(spec.max_per_page))
        params["per_page"] = per_page

    with requests.Session() as sess:
        # Decide how to fetch: search vs list
        if cursor is not None and spec.search_url and spec.search_updated_field:
            # Incremental via search endpoint (updated_at > cursor)
            effective_page_size = per_page or _default_per_page(cfg)
            query = {
                "field": spec.search_updated_field,
                "operator": ">",
                "value": int(cursor),
            }
            listed = _search_endpoint(
                rid=rid,
                cfg=cfg,
                sess=sess,
                url=spec.search_url,
                headers=headers,
                stream=spec.stream,
                query=query,
                page_size=effective_page_size,
            )
        else:
            # Full (or param-based incremental) list endpoint
            if cursor is not None and spec.updated_since_param:
                params[spec.updated_since_param] = int(cursor)

            listed = _list_endpoint(
                rid=rid,
                cfg=cfg,
                sess=sess,
                url=spec.list_url,
                headers=headers,
                stream=spec.stream,
                params=params,
            )

        # If no detail_path, the listed records are the final payload.
        if not spec.detail_path:
            for item in listed:
                if isinstance(item, dict):
                    yield item
            return

        # Otherwise, hydrate by ID into detail objects.
        def jobs() -> Iterator[_HydrateJob]:
            for item in listed:
                _id = _obj_id(item)
                if not _id:
                    continue
                yield _HydrateJob(id=_id, url=f"{base}/{spec.detail_path}/{_id}")

        yield from _hydrate_many(rid=rid, cfg=cfg, stream=spec.stream, jobs=jobs())


# =========================================
# SECTION 7 — RESOURCES
# =========================================
def contacts_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    set_max_updated = call.kwargs.get("set_max_updated")
    cursor = call.cursor

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["contacts"], cursor=cursor):
        if bump:
            bump("contacts", 1)
        if set_max_updated and isinstance(row, dict):
            v = row.get("updated_at")
            if isinstance(v, int):
                set_max_updated(v)
        yield row


def companies_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    set_max_updated = call.kwargs.get("set_max_updated")
    cursor = call.cursor  # currently unused but accepted for symmetry

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["companies"], cursor=cursor):
        if bump:
            bump("companies", 1)
        if set_max_updated and isinstance(row, dict):
            v = row.get("updated_at")
            if isinstance(v, int):
                set_max_updated(v)
        yield row


def conversations_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    set_max_updated = call.kwargs.get("set_max_updated")
    cursor = call.cursor

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["conversations"], cursor=cursor):
        if bump:
            bump("conversations", 1)
        if set_max_updated and isinstance(row, dict):
            v = row.get("updated_at")
            if isinstance(v, int):
                set_max_updated(v)
        yield row


def tickets_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    set_max_updated = call.kwargs.get("set_max_updated")
    cursor = call.cursor

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["tickets"], cursor=cursor):
        if bump:
            bump("tickets", 1)
        if set_max_updated and isinstance(row, dict):
            v = row.get("updated_at")
            if isinstance(v, int):
                set_max_updated(v)
        yield row


def admins_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor  # accepted but not used

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["admins"], cursor=cursor):
        if bump:
            bump("admins", 1)
        yield row


def teams_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor  # accepted but not used

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["teams"], cursor=cursor):
        if bump:
            bump("teams", 1)
        yield row


def segments_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor  # accepted but not used

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["segments"], cursor=cursor):
        if bump:
            bump("segments", 1)
        yield row


def tags_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor  # accepted but not used

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["tags"], cursor=cursor):
        if bump:
            bump("tags", 1)
        yield row


def help_center_collections_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor  # accepted but not used

    for row in _stream_items(
        rid=call.rid,
        cfg=call.cfg,
        spec=_specs(call.cfg)["help_center_collections"],
        cursor=cursor,
    ):
        if bump:
            bump("help_center_collections", 1)
        yield row


def help_center_sections_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor  # accepted but not used

    for row in _stream_items(
        rid=call.rid,
        cfg=call.cfg,
        spec=_specs(call.cfg)["help_center_sections"],
        cursor=cursor,
    ):
        if bump:
            bump("help_center_sections", 1)
        yield row


def articles_resource(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    call = _resolve_call(args, kwargs)
    bump = call.kwargs.get("bump")
    cursor = call.cursor

    for row in _stream_items(rid=call.rid, cfg=call.cfg, spec=_specs(call.cfg)["articles"], cursor=cursor):
        if bump:
            bump("articles", 1)
        yield row


# =========================================
# SECTION 8 — TRANSFORMER: CONVERSATION PARTS
# =========================================
def _emit_parts_from_conversation(conversation: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    conv_id = conversation.get("id")
    conv_created_at = conversation.get("created_at")
    conv_updated_at = conversation.get("updated_at")

    parts_container = conversation.get("conversation_parts")

    parts: Optional[List[Dict[str, Any]]] = None
    if isinstance(parts_container, dict):
        maybe = parts_container.get("conversation_parts")
        if isinstance(maybe, list):
            parts = [p for p in maybe if isinstance(p, dict)]
    elif isinstance(parts_container, list):
        parts = [p for p in parts_container if isinstance(p, dict)]

    if not parts:
        return

    for idx, part in enumerate(parts):
        part_id = part.get("id") or f"{conv_id}:{idx}"

        author = part.get("author") if isinstance(part.get("author"), dict) else None
        attachments = part.get("attachments")
        if not isinstance(attachments, list):
            attachments = []

        yield {
            "conversation_id": conv_id,
            "conversation_created_at": conv_created_at,
            "conversation_updated_at": conv_updated_at,
            "part_id": part_id,
            "part_created_at": part.get("created_at"),
            "part_updated_at": part.get("updated_at"),
            "part_type": part.get("part_type") or part.get("type"),
            "body": part.get("body"),
            "author_id": author.get("id") if author else None,
            "author_type": author.get("type") if author else None,
            "attachments_count": len(attachments),
            "raw": part,
        }


def conversation_parts_transformer(*args: Any, **kwargs: Any) -> Iterator[Dict[str, Any]]:
    # Extract the cursor from kwargs since it will be passed from pipeline.py
    cursor_parts = kwargs.pop("cursor_parts", None)
    conversations_resource_fn = kwargs.pop("conversations_resource_fn", None)
    bump = kwargs.pop("bump", None)
    set_max_updated = kwargs.pop("set_max_updated", None)

    # Handle the case where cfg is passed as part of the arguments
    cfg = None
    for arg in args:
        if _looks_like_cfg(arg):
            cfg = arg
            break

    if conversations_resource_fn is not None:
        source = conversations_resource_fn() if callable(conversations_resource_fn) else conversations_resource_fn
        for convo in source:
            if isinstance(convo, dict):
                for out in _emit_parts_from_conversation(convo):
                    part_updated_at = out.get("part_updated_at") or out.get("part_created_at")

                    if cursor_parts is not None and isinstance(part_updated_at, int):
                        if part_updated_at <= cursor_parts:
                            continue

                    if set_max_updated and isinstance(part_updated_at, int):
                        set_max_updated(part_updated_at)

                    if bump:
                        bump("conversation_parts", 1)
                    yield out
        return

    if len(args) >= 1 and isinstance(args[0], dict):
        for out in _emit_parts_from_conversation(args[0]):
            part_updated_at = out.get("part_updated_at") or out.get("part_created_at")

            if set_max_updated and isinstance(part_updated_at, int):
                set_max_updated(part_updated_at)

            if bump:
                bump("conversation_parts", 1)
            yield out
        return

    return
