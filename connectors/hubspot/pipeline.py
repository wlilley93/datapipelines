# connectors/hubspot/pipeline.py
from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import dlt
import requests

from .client import HubSpotClient
from .constants import DEFAULT_OBJECTS, DEFAULT_PAGE_LIMIT
from .errors import TransientHttpError
from .events import paging_done, paging_start, records_seen
from .incremental import advance_cursor_from_results, should_do_full
from .normalization import normalize_object_record
from .schema_tracker import extract_object_schemas, split_standard_vs_custom
from .state import (
    HubSpotRuntimeConfig,
    get_last_cursor,
    get_runtime_config,
    set_last_cursor,
    set_runtime_config,
)

# =============================================================================
# CONFIG
# =============================================================================

BATCH_READ_INPUT_LIMIT_DEFAULT = int(os.getenv("HUBSPOT_BATCH_READ_INPUT_LIMIT", "50"))
BATCH_READ_INPUT_LIMIT_DEFAULT = max(1, min(100, BATCH_READ_INPUT_LIMIT_DEFAULT))

BATCH_READ_INPUT_LIMIT_BY_OBJECT: Dict[str, int] = {
    "meetings": int(os.getenv("HUBSPOT_BATCH_READ_INPUT_LIMIT_MEETINGS", "20")),
    "emails": int(os.getenv("HUBSPOT_BATCH_READ_INPUT_LIMIT_EMAILS", "20")),
    "notes": int(os.getenv("HUBSPOT_BATCH_READ_INPUT_LIMIT_NOTES", "50")),
}
for k, v in list(BATCH_READ_INPUT_LIMIT_BY_OBJECT.items()):
    BATCH_READ_INPUT_LIMIT_BY_OBJECT[k] = max(1, min(100, int(v)))

HTTP_MAX_RETRIES = 8

MAX_PROPERTIES_PER_BATCH_READ = int(os.getenv("HUBSPOT_MAX_PROPS_PER_BATCH_READ", "200"))
MAX_PROPERTIES_PER_BATCH_READ = max(1, MAX_PROPERTIES_PER_BATCH_READ)

FORCE_FULL_ALL = os.getenv("HUBSPOT_FORCE_FULL_ALL", "").strip().lower() in ("1", "true", "yes", "y", "on")

LASTMODIFIED_PROPERTY = "hs_lastmodifieddate"

CORE_PROPERTIES_BY_OBJECT: Dict[str, List[str]] = {
    "contacts": [
        "firstname",
        "lastname",
        "email",
        "phone",
        "mobilephone",
        "jobtitle",
        "company",
        "website",
        "lifecyclestage",
        "lead_status",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        LASTMODIFIED_PROPERTY,
    ],
    "companies": [
        "name",
        "domain",
        "phone",
        "city",
        "state",
        "country",
        "industry",
        "numberofemployees",
        "annualrevenue",
        "lifecyclestage",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        LASTMODIFIED_PROPERTY,
    ],
    "deals": [
        "dealname",
        "dealstage",
        "pipeline",
        "amount",
        "closedate",
        "hubspot_owner_id",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        LASTMODIFIED_PROPERTY,
    ],
    "tickets": [
        "subject",
        "content",
        "hs_pipeline",
        "hs_pipeline_stage",
        "hs_ticket_priority",
        "hubspot_owner_id",
        "createdate",
        "lastmodifieddate",
        "hs_object_id",
        LASTMODIFIED_PROPERTY,
    ],
    "engagements_tasks": ["hs_task_subject", "hs_task_status", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "engagements_notes": ["hs_note_body", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "engagements_calls": ["hs_call_body", "hs_call_status", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "engagements_meetings": ["hs_meeting_title", "hs_meeting_start_time", "hs_meeting_end_time", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "calls": ["hs_call_body", "hs_call_status", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "emails": ["hs_email_subject", "hs_email_status", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "meetings": ["hs_meeting_title", "hs_meeting_start_time", "hs_meeting_end_time", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "notes": ["hs_note_body", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "tasks": ["hs_task_subject", "hs_task_status", "hubspot_owner_id", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "products": ["name", "price", "hs_sku", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
    "line_items": ["name", "quantity", "price", "amount", "createdate", "lastmodifieddate", "hs_object_id", LASTMODIFIED_PROPERTY],
}

# =============================================================================
# Public API used by connector.py
# =============================================================================


def test_connection(*, token: str) -> None:
    c = HubSpotClient(token=token)
    c.get_json(stream="hubspot", path="/crm/v3/schemas")


def run_pipeline(
    *,
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    destination: Optional[str] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    token = (creds or {}).get("access_token") or (creds or {}).get("token")
    if not token or not isinstance(token, str):
        raise ValueError("HubSpot creds missing access_token/token")
    return _run_pipeline_with_token(token=token, schema=schema, state=state, destination=destination)


# =============================================================================
# Cursor + filter helpers (avoid HubSpot 400s)
# =============================================================================


def _parse_cursor_to_epoch_ms(cursor: Optional[Any]) -> Optional[int]:
    if cursor is None:
        return None

    if isinstance(cursor, (int, float)):
        v = float(cursor)
        if v <= 0:
            return None
        return int(v * 1000) if v < 1_000_000_000_000 else int(v)

    if isinstance(cursor, str):
        s = cursor.strip()
        if not s:
            return None

        if s.isdigit():
            if len(s) >= 13:
                try:
                    return int(s)
                except Exception:
                    return None
            if len(s) == 10:
                try:
                    return int(s) * 1000
                except Exception:
                    return None

        try:
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None

    return None


def _make_lastmodified_filter(cursor: Optional[Any]) -> Optional[Dict[str, Any]]:
    ms = _parse_cursor_to_epoch_ms(cursor)
    if ms is None:
        return None
    return {"filterGroups": [{"filters": [{"propertyName": LASTMODIFIED_PROPERTY, "operator": "GTE", "value": str(ms)}]}]}


# =============================================================================
# HTTP hardening helpers
# =============================================================================


def _sleep_backoff(attempt: int, base: float = 0.8, cap: float = 30.0) -> None:
    delay = min(cap, base * (2**attempt))
    delay = delay * (0.7 + random.random() * 0.6)
    time.sleep(delay)


def _is_retryable_http(status: Optional[int]) -> bool:
    return status in (408, 409, 425, 429, 500, 502, 503, 504)


def _request_with_retries(fn, *, max_retries: int = HTTP_MAX_RETRIES):
    last_exc: Optional[BaseException] = None
    for attempt in range(max_retries + 1):
        try:
            return fn()

        except TransientHttpError as e:
            last_exc = e
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            raise

        except requests.HTTPError as e:
            last_exc = e
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None)

            if status in (401, 403):
                raise

            if _is_retryable_http(status) and attempt < max_retries:
                retry_after = None
                try:
                    ra = (resp.headers or {}).get("Retry-After") if resp is not None else None
                    if ra and str(ra).isdigit():
                        retry_after = int(ra)
                except Exception:
                    retry_after = None

                if retry_after is not None:
                    time.sleep(min(60, retry_after))
                else:
                    _sleep_backoff(attempt)
                continue
            raise

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < max_retries:
                _sleep_backoff(attempt)
                continue
            raise

    if last_exc:
        raise last_exc
    raise RuntimeError("request failed with unknown error")


def _is_auth_error(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        code = getattr(resp, "status_code", None)
        return code in (401, 403)
    return False


def _http_status_from_exc(exc: BaseException) -> Optional[int]:
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        return getattr(resp, "status_code", None)
    return None


def _chunked(xs: List[Any], n: int) -> Iterator[List[Any]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


def _sanitize_property_names(props: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for p in props or []:
        if not isinstance(p, str):
            continue
        p = p.strip()
        if not p or p in seen:
            continue
        if any(ch.isspace() for ch in p):
            continue
        if any(ord(ch) < 32 for ch in p):
            continue
        seen.add(p)
        out.append(p)
    return out


def _object_path(obj: str, obj_type_id: Optional[str]) -> str:
    return f"/crm/v3/objects/{obj_type_id or obj}"


def _object_search_path(obj: str, obj_type_id: Optional[str]) -> str:
    return f"{_object_path(obj, obj_type_id)}/search"


def _object_batch_read_path(obj: str, obj_type_id: Optional[str]) -> str:
    return f"{_object_path(obj, obj_type_id)}/batch/read"


def _properties_path(obj: str, obj_type_id: Optional[str]) -> str:
    return f"/crm/v3/properties/{obj_type_id or obj}"


def _raise_request_error(obj: str, url_path: str, payload_or_params: Dict[str, Any], exc: BaseException, *, kind: str) -> None:
    if _is_auth_error(exc):
        raise exc

    status = _http_status_from_exc(exc)
    status_txt = f" status={status}" if status is not None else ""

    corr = None
    resp_text = None
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        try:
            resp_text = resp.text if resp is not None else None
        except Exception:
            resp_text = None
        try:
            if resp_text and resp_text.strip().startswith("{"):
                parsed = json.loads(resp_text)
                if isinstance(parsed, dict):
                    corr = parsed.get("correlationId") or parsed.get("correlation_id")
        except Exception:
            corr = None

    corr_txt = f" correlationId={corr}" if corr else ""
    safe = json.dumps(payload_or_params, ensure_ascii=False)[:2000]
    msg = str(exc)
    extra = f"\nHubSpot response: {resp_text[:2000]}" if resp_text else ""
    raise RuntimeError(
        f"HubSpot {kind} request failed for {obj} at {url_path}.{status_txt}{corr_txt}\n"
        f"{kind.title()} (truncated): {safe}\n"
        f"Original error: {msg}{extra}"
    ) from exc


# =============================================================================
# Value sanitation
# =============================================================================


def _is_dateish_key(key: str) -> bool:
    k = (key or "").lower()
    if not k:
        return False
    return (
        "date" in k
        or k.endswith("_at")
        or k.endswith("_time")
        or "timestamp" in k
        or k in ("createdate", "closedate", "lastmodifieddate", LASTMODIFIED_PROPERTY)
    )


def _coerce_epoch_to_iso_utc(v: str) -> Optional[str]:
    s = v.strip()
    if not s.isdigit():
        return None
    try:
        if len(s) >= 13:
            ms = int(s)
            dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        if len(s) == 10:
            sec = int(s)
            dt = datetime.fromtimestamp(sec, tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None
    return None


def _clean_value(key: str, v: Any) -> Any:
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        if _is_dateish_key(key):
            iso = _coerce_epoch_to_iso_utc(s)
            if iso is not None:
                return iso
        return v
    return v


def _clean_properties(props: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in props.items():
        if not isinstance(k, str) or not k:
            continue
        out[k] = _clean_value(k, v)
    return out


# =============================================================================
# Schema discovery
# =============================================================================

_PROPERTIES_CACHE: Dict[str, List[str]] = {}


def _get_all_property_names(
    client: HubSpotClient,
    *,
    stream: str,
    obj: str,
    obj_type_id: Optional[str],
) -> List[str]:
    cache_key = obj_type_id or obj
    if cache_key in _PROPERTIES_CACHE:
        return _PROPERTIES_CACHE[cache_key]

    path = _properties_path(obj, obj_type_id)

    def _do():
        return client.get_json(stream=f"{stream}.properties", path=path)

    try:
        data = _request_with_retries(_do)
    except BaseException as e:
        _raise_request_error(obj=obj, url_path=path, payload_or_params={}, exc=e, kind="params")

    results = data.get("results") or []
    names: List[str] = []
    if isinstance(results, list):
        for r in results:
            if isinstance(r, dict):
                n = r.get("name")
                if isinstance(n, str) and n.strip():
                    names.append(n.strip())

    names = _sanitize_property_names(list(dict.fromkeys(names + [LASTMODIFIED_PROPERTY, "lastmodifieddate"])))
    _PROPERTIES_CACHE[cache_key] = names
    return names


def _build_columns_hint(*, all_props: List[str], core_props: List[str]) -> Dict[str, Any]:
    cols: Dict[str, Any] = {"id": {"data_type": "text"}, "properties": {"data_type": "json"}}
    for p in _sanitize_property_names(core_props):
        cols[p] = {"data_type": "text"}
    for p in _sanitize_property_names(all_props):
        cols[p] = {"data_type": "text"}
    return cols


# =============================================================================
# Paging
# =============================================================================

@dataclass(frozen=True)
class _Page:
    raw_results: List[Dict[str, Any]]
    returned: int
    next_after: Optional[str]


def _list_page(
    client: HubSpotClient,
    *,
    stream: str,
    obj: str,
    obj_type_id: Optional[str],
    after: Optional[str],
    limit: int,
    properties: List[str],
) -> _Page:
    props = _sanitize_property_names(list(dict.fromkeys((properties or []) + [LASTMODIFIED_PROPERTY])))
    params: Dict[str, Any] = {"limit": limit, "properties": ",".join(props)}
    if after:
        params["after"] = after

    path = _object_path(obj, obj_type_id)

    def _do():
        return client.get_json(stream=stream, path=path, params=params)

    try:
        data = _request_with_retries(_do)
    except BaseException as e:
        _raise_request_error(obj=obj, url_path=path, payload_or_params=params, exc=e, kind="params")

    results = data.get("results") or []
    if not isinstance(results, list):
        results = []

    paging = data.get("paging") or {}
    nxt = None
    if isinstance(paging, dict):
        nxt = (paging.get("next") or {}).get("after")

    return _Page(
        raw_results=[r for r in results if isinstance(r, dict)],
        returned=len(results),
        next_after=nxt if isinstance(nxt, str) and nxt else None,
    )


def _search_page(
    client: HubSpotClient,
    *,
    stream: str,
    obj: str,
    obj_type_id: Optional[str],
    after: Optional[str],
    limit: int,
    cursor: Optional[Any],
    properties: List[str],
) -> _Page:
    props = _sanitize_property_names(list(dict.fromkeys((properties or []) + [LASTMODIFIED_PROPERTY])))
    payload: Dict[str, Any] = {"limit": limit, "properties": props}
    if after:
        payload["after"] = after

    filt = _make_lastmodified_filter(cursor)
    if filt is not None:
        payload.update(filt)

    path = _object_search_path(obj, obj_type_id)

    def _do():
        return client.post_json(stream=stream, path=path, payload=payload)

    try:
        data = _request_with_retries(_do)
    except BaseException as e:
        status = _http_status_from_exc(e)
        if status == 400:
            raise RuntimeError(f"HUBSPOT_SEARCH_400 for {obj} at {path}") from e
        _raise_request_error(obj=obj, url_path=path, payload_or_params=payload, exc=e, kind="payload")

    results = data.get("results") or []
    if not isinstance(results, list):
        results = []

    paging = data.get("paging") or {}
    nxt = None
    if isinstance(paging, dict):
        nxt = (paging.get("next") or {}).get("after")

    return _Page(
        raw_results=[r for r in results if isinstance(r, dict)],
        returned=len(results),
        next_after=nxt if isinstance(nxt, str) and nxt else None,
    )


def _batch_read_merge(
    client: HubSpotClient,
    *,
    stream: str,
    obj: str,
    obj_type_id: Optional[str],
    ids: List[str],
    all_properties: List[str],
) -> List[Dict[str, Any]]:
    if not ids:
        return []

    path = _object_batch_read_path(obj, obj_type_id)
    props = _sanitize_property_names(all_properties)
    if not props:
        return []

    merged: Dict[str, Dict[str, Any]] = {}

    prop_chunks = list(_chunked(props, max(1, MAX_PROPERTIES_PER_BATCH_READ)))

    input_limit = BATCH_READ_INPUT_LIMIT_BY_OBJECT.get(obj, BATCH_READ_INPUT_LIMIT_DEFAULT)
    input_limit = max(1, min(100, int(input_limit)))

    for id_chunk in _chunked(ids, input_limit):
        if not id_chunk:
            continue
        chunk_acc: Dict[str, Dict[str, Any]] = {}

        for p_chunk in prop_chunks:
            payload = {"properties": p_chunk, "inputs": [{"id": i} for i in id_chunk]}

            def _do():
                return client.post_json(stream=f"{stream}.hydrate", path=path, payload=payload)

            try:
                data = _request_with_retries(_do)
            except BaseException as e:
                status = _http_status_from_exc(e)
                if status == 400 and (not p_chunk or not id_chunk):
                    continue
                _raise_request_error(obj=obj, url_path=path, payload_or_params=payload, exc=e, kind="payload")

            res = data.get("results") or []
            if not isinstance(res, list):
                continue

            for r in res:
                if not isinstance(r, dict):
                    continue
                rid = r.get("id")
                if not isinstance(rid, str) or not rid.strip():
                    continue
                rid = rid.strip()

                existing = chunk_acc.get(rid) or {"id": rid}

                for k, v in r.items():
                    if k == "properties":
                        continue
                    if k not in existing:
                        existing[k] = v

                props_dict = r.get("properties")
                if isinstance(props_dict, dict):
                    existing_props = existing.get("properties")
                    if not isinstance(existing_props, dict):
                        existing_props = {}
                    existing_props.update(props_dict)
                    existing["properties"] = existing_props

                chunk_acc[rid] = existing

        for rid, rec in chunk_acc.items():
            if rid in merged:
                a = merged[rid]
                a_props = a.get("properties")
                if not isinstance(a_props, dict):
                    a_props = {}
                b_props = rec.get("properties")
                if isinstance(b_props, dict):
                    a_props.update(b_props)
                a["properties"] = a_props
                for k, v in rec.items():
                    if k not in a:
                        a[k] = v
            else:
                merged[rid] = rec

    return list(merged.values())


# =============================================================================
# Pipeline runner
# =============================================================================


def _run_pipeline_with_token(
    *,
    token: str,
    schema: str,
    state: Dict[str, Any],
    destination: Optional[str],
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    cfg: HubSpotRuntimeConfig = get_runtime_config(state)
    destination = destination or os.getenv("DLT_DESTINATION", None)

    # Capture cursors at run start for debugging (confirms whether state is being passed in).
    start_cursors = {k: get_last_cursor(state, k) for k in ("companies", "deals", "meetings", "emails")}

    client = HubSpotClient(token=token)
    auth_skipped: List[str] = []

    def _get_schemas():
        return client.get_json(stream="object_schemas", path="/crm/v3/schemas")

    schemas_payload = _request_with_retries(_get_schemas)
    all_schemas = extract_object_schemas(schemas_payload)
    std_schemas, custom_schemas = split_standard_vs_custom(all_schemas)

    chosen_objects: List[str] = list(cfg.objects) if cfg.objects else list(DEFAULT_OBJECTS)
    excluded = set(cfg.excluded_streams or [])
    include_custom = bool(cfg.include_custom_objects)

    std_by_name = {s.name: s for s in std_schemas}
    custom_by_name = {s.name: s for s in custom_schemas}

    if include_custom:
        for custom_name in sorted(custom_by_name.keys()):
            if custom_name not in chosen_objects and custom_name not in excluded:
                chosen_objects.append(custom_name)

    resources: List[Any] = []
    resources.append(_resource_object_schemas(schemas_payload))

    for obj in chosen_objects:
        if obj in excluded:
            continue

        s = std_by_name.get(obj) or custom_by_name.get(obj)
        obj_type_id = None
        if s is not None and str(s.object_type_id).startswith("2-"):
            obj_type_id = s.object_type_id

        resources.append(
            _resource_objects(
                client,
                obj,
                obj_type_id=obj_type_id,
                state=state,
                cfg=cfg,
                auth_skipped=auth_skipped,
            )
        )

    p = dlt.pipeline(pipeline_name="hubspot", dataset_name=schema, destination=destination)
    info = p.run(resources)

    if auth_skipped:
        merged_excluded = sorted(set(cfg.excluded_streams or []).union(set(auth_skipped)))
        cfg = HubSpotRuntimeConfig(
            objects=list(cfg.objects),
            excluded_streams=list(merged_excluded),
            include_custom_objects=cfg.include_custom_objects,
            incremental=cfg.incremental,
            force_full_objects=list(cfg.force_full_objects),
        )
        set_runtime_config(state, cfg)

    # ✅ IMPORTANT: return the mutated state explicitly.
    # This makes state persistence in the orchestrator straightforward and avoids accidental rebuild mistakes.
    global_state = state.get("global")
    if not isinstance(global_state, dict):
        global_state = {}
        state["global"] = global_state

    bookmarks = global_state.get("bookmarks")
    if not isinstance(bookmarks, dict):
        bookmarks = {}
        global_state["bookmarks"] = bookmarks

    hubspot_runtime = global_state.get("hubspot_runtime")
    if not isinstance(hubspot_runtime, dict):
        hubspot_runtime = {}
        global_state["hubspot_runtime"] = hubspot_runtime

    state_updates: Dict[str, Any] = {"global": {"bookmarks": bookmarks, "hubspot_runtime": hubspot_runtime}}

    loads = getattr(info, "loads_ids", None)
    loads_txt = f"{len(loads)} load package(s)" if isinstance(loads, list) else "load step completed"

    report_lines = [
        "HubSpot sync completed.",
        f"Excluded streams: {sorted(list(set(cfg.excluded_streams or [])))}",
        f"Custom objects discovered: {len(custom_schemas)}",
        f"Pipeline hubspot {loads_txt}",
        f"Force full all (env HUBSPOT_FORCE_FULL_ALL): {FORCE_FULL_ALL}",
        f"Max props per batch read (env HUBSPOT_MAX_PROPS_PER_BATCH_READ): {MAX_PROPERTIES_PER_BATCH_READ}",
        f"Batch read input limit default (env HUBSPOT_BATCH_READ_INPUT_LIMIT): {BATCH_READ_INPUT_LIMIT_DEFAULT}",
        f"Cursor field: {LASTMODIFIED_PROPERTY}",
        f"Start cursors (debug): {start_cursors}",
    ]
    if auth_skipped:
        report_lines.append(f"Streams skipped (auth): {sorted(set(auth_skipped))}")
        report_lines.append("NOTE: Skipped streams indicate missing token scopes/portal permissions.")

    return "\n".join(report_lines), None, state_updates


@dlt.resource(name="object_schemas", write_disposition="replace")
def _resource_object_schemas(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    results = payload.get("results") or []
    for r in results:
        if isinstance(r, dict):
            yield r


def _fetch_associations(
    client: HubSpotClient,
    *,
    stream: str,
    from_object_type: str,
    object_id: str,
    to_object_type: str,
) -> List[str]:
    """
    Fetch all associated object IDs for a given object.

    Returns:
        List of associated object IDs
    """
    associated_ids = []
    after = None

    try:
        while True:
            resp = client.get_associations(
                stream=stream,
                from_object_type=from_object_type,
                object_id=object_id,
                to_object_type=to_object_type,
                after=after,
                limit=500
            )

            results = resp.get("results", [])
            for result in results:
                if isinstance(result, dict):
                    to_id = result.get("toObjectId") or result.get("id")
                    if to_id:
                        associated_ids.append(str(to_id))

            # Check for pagination
            paging = resp.get("paging", {})
            next_page = paging.get("next", {})
            after = next_page.get("after")

            if not after:
                break

    except Exception:
        # If associations fetch fails, just return empty list
        # (some objects might not have associations permissions)
        pass

    return associated_ids


def _resource_objects(
    client: HubSpotClient,
    obj: str,
    *,
    obj_type_id: Optional[str],
    state: Dict[str, Any],
    cfg: HubSpotRuntimeConfig,
    auth_skipped: List[str],
) -> Any:
    core_props = _sanitize_property_names(
        CORE_PROPERTIES_BY_OBJECT.get(obj, ["hs_object_id", "createdate", "lastmodifieddate", LASTMODIFIED_PROPERTY])
    )
    paging_props = _sanitize_property_names([LASTMODIFIED_PROPERTY])

    all_props = _get_all_property_names(client, stream=obj, obj=obj, obj_type_id=obj_type_id)
    columns_hint = _build_columns_hint(all_props=all_props, core_props=core_props)

    @dlt.resource(
        name=obj,
        write_disposition="merge",
        primary_key="id",
        columns=columns_hint,
    )
    def _gen() -> Iterable[Dict[str, Any]]:
        stream = obj
        cursor = get_last_cursor(state, stream)

        force_full = FORCE_FULL_ALL or (obj in (cfg.force_full_objects or []))
        do_full = force_full or should_do_full(state_cursor=cursor) or (not bool(cfg.incremental))

        total_seen = 0
        page = 0
        after: Optional[str] = None
        search_broken_fallback_to_full = False

        try:
            while True:
                page += 1
                paging_start(stream=stream, page=page, limit=DEFAULT_PAGE_LIMIT)

                if do_full or search_broken_fallback_to_full:
                    pg = _list_page(
                        client,
                        stream=stream,
                        obj=obj,
                        obj_type_id=obj_type_id,
                        after=after,
                        limit=DEFAULT_PAGE_LIMIT,
                        properties=paging_props,
                    )
                    page_results = pg.raw_results
                else:
                    try:
                        pg = _search_page(
                            client,
                            stream=stream,
                            obj=obj,
                            obj_type_id=obj_type_id,
                            after=after,
                            limit=DEFAULT_PAGE_LIMIT,
                            cursor=cursor,
                            properties=paging_props,
                        )
                    except RuntimeError as e:
                        if "HUBSPOT_SEARCH_400" in str(e):
                            search_broken_fallback_to_full = True
                            after = None
                            continue
                        raise
                    page_results = pg.raw_results

                ids: List[str] = []
                for r in page_results:
                    rid = r.get("id")
                    if isinstance(rid, str) and rid.strip():
                        ids.append(rid.strip())

                hydrated = _batch_read_merge(
                    client,
                    stream=stream,
                    obj=obj,
                    obj_type_id=obj_type_id,
                    ids=ids,
                    all_properties=all_props,
                )

                if hydrated:
                    cursor = advance_cursor_from_results(cursor, hydrated, property_name=LASTMODIFIED_PROPERTY)
                    if cursor:
                        set_last_cursor(state, stream, cursor)

                for raw in hydrated:
                    row = normalize_object_record(raw)

                    rid = raw.get("id")
                    row["id"] = rid.strip() if isinstance(rid, str) and rid.strip() else str(row.get("id") or "")

                    props = raw.get("properties")
                    if isinstance(props, dict):
                        cleaned_props = _clean_properties(props)
                        row["properties"] = cleaned_props

                        for k in core_props:
                            if k in cleaned_props:
                                row[k] = cleaned_props.get(k)

                        for k, v in cleaned_props.items():
                            if isinstance(k, str) and k in columns_hint:
                                row[k] = v

                    # Fetch associations for deals -> line_items and line_items -> deals
                    if obj == "deals" and row.get("id"):
                        line_item_ids = _fetch_associations(
                            client,
                            stream=stream,
                            from_object_type="deals",
                            object_id=row["id"],
                            to_object_type="line_items"
                        )
                        # Store associations as a child table using DLT's nested data structure
                        if line_item_ids:
                            row["assoc_line_items_ids"] = [{"value": lid} for lid in line_item_ids]

                    elif obj == "line_items" and row.get("id"):
                        deal_ids = _fetch_associations(
                            client,
                            stream=stream,
                            from_object_type="line_items",
                            object_id=row["id"],
                            to_object_type="deals"
                        )
                        # Store associations as a child table
                        if deal_ids:
                            row["assoc_deals_ids"] = [{"value": did} for did in deal_ids]

                    total_seen += 1
                    yield row

                records_seen(stream=stream, count=total_seen)

                if pg.next_after:
                    after = pg.next_after
                    continue

                paging_done(stream=stream, returned=pg.returned)
                break

        except Exception as e:
            if _is_auth_error(e):
                if stream not in auth_skipped:
                    auth_skipped.append(stream)
                return
            raise

    return _gen
