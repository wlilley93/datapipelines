# Intercom Connector Files Export

This document contains all files from the Intercom connector directory.

## File: __init__.py

```python
"""
Intercom connector package.

Why:
- Some orchestrators / “legacy connector” loaders import `run_pipeline` directly from the
  connector package (connectors.intercom.run_pipeline). Exporting it here avoids the
  "Legacy connector 'intercom' missing run_pipeline(...)" runtime error.
"""

from __future__ import annotations

from .connector import connector  # re-export factory
from .pipeline import run_pipeline, test_connection  # re-export legacy entrypoints

__all__ = ["connector", "run_pipeline", "test_connection"]

```

## File: client.py

```python
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class IntercomClientConfig:
    access_token: str
    base_url: str = "https://api.intercom.io"
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    max_retries: int = 3
    retry_backoff_s: float = 0.75  # base backoff; grows per attempt
    intercom_version: str = "2.14"


class IntercomClient:
    """
    Why: a small standalone client for unit tests or alternate entrypoints.
    Note: the main connector pipeline uses http.req_json + shared session.
    """

    def __init__(self, cfg: IntercomClientConfig) -> None:
        self._cfg = cfg
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {cfg.access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Intercom-Version": cfg.intercom_version,
                "User-Agent": "intercom-connector/1.0",
            }
        )

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self._cfg.base_url.rstrip('/')}/{path.lstrip('/')}"
        return self._request_json("GET", url, params=params)

    def _request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        last_exc: Optional[BaseException] = None

        for attempt in range(self._cfg.max_retries):
            try:
                resp = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=body,
                    timeout=(self._cfg.connect_timeout_s, self._cfg.read_timeout_s),
                )

                # why: Intercom rate-limits; respect Retry-After when present.
                if resp.status_code == 429:
                    retry_after = _parse_retry_after_s(resp.headers.get("Retry-After"))
                    time.sleep(retry_after if retry_after is not None else _backoff(self._cfg.retry_backoff_s, attempt))
                    continue

                # why: transient server errors should be retried.
                if 500 <= resp.status_code <= 599:
                    time.sleep(_backoff(self._cfg.retry_backoff_s, attempt))
                    continue

                resp.raise_for_status()

                if not resp.content:
                    return {}

                return resp.json()

            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc

                # why: 4xx (except 429) are usually permanent; don't hammer.
                if isinstance(exc, requests.HTTPError):
                    status = getattr(exc.response, "status_code", None)
                    if status is not None and 400 <= status <= 499 and status != 429:
                        raise

                time.sleep(_backoff(self._cfg.retry_backoff_s, attempt))

        raise RuntimeError(f"Intercom request failed after retries: {method} {url}") from last_exc


def _backoff(base_s: float, attempt: int) -> float:
    # why: simple exponential-ish backoff; enough to avoid hammering.
    return base_s * (2**attempt)


def _parse_retry_after_s(v: Optional[str]) -> Optional[float]:
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None

```

## File: config.py

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        n = int(v)
        return n if n > 0 else default
    except Exception:
        return default


@dataclass(frozen=True)
class IntercomConfig:
    # Auth (secret) — MUST remain env/creds-driven
    access_token: Optional[str]

    # Pull everything defaults
    sync_contacts: bool = True
    sync_companies: bool = True
    sync_conversations: bool = True
    sync_parts: bool = True
    sync_tickets: bool = True

    # Optional extras (default off)
    sync_admins: bool = False
    sync_teams: bool = False
    sync_segments: bool = False
    sync_tags: bool = False
    sync_help_center_collections: bool = False
    sync_help_center_sections: bool = False
    sync_articles: bool = False

    force_full: bool = False
    start_at_epoch: Optional[int] = None

    # Companies completeness strategy
    companies_use_scroll: bool = True  # FIX: used by companies stream

    # Dynamic columns
    expand_custom_attributes: bool = True
    max_custom_attr_columns: int = 200
    flatten_max_depth: int = 2

    # Performance + resilience
    hydrate_workers: int = 4
    hydrate_max_in_flight: int = 8

    # Request knobs used by http.req_json wrapper
    request_timeout_s: float = 60.0
    request_connect_timeout_s: float = 10.0
    request_retries: int = 2
    retry_sleep_s: float = 1.0

    rate_limit_remaining_floor: int = 2
    rate_limit_max_sleep: int = 60

    # API version header
    intercom_version: str = "2.14"

    @staticmethod
    def from_env_and_creds(creds: dict) -> "IntercomConfig":
        token = (
            creds.get("access_token")
            or creds.get("api_key")
            or creds.get("token")
            or creds.get("INTERCOM_ACCESS_TOKEN")
            or os.getenv("INTERCOM_ACCESS_TOKEN")
        )

        start_raw = (os.getenv("INTERCOM_START_AT_EPOCH") or "").strip()
        try:
            start_epoch = int(start_raw) if start_raw else None
        except Exception:
            start_epoch = None

        workers = _env_int("INTERCOM_HYDRATE_WORKERS", 4)
        max_in_flight = _env_int("INTERCOM_HYDRATE_MAX_IN_FLIGHT", max(8, workers * 2))

        return IntercomConfig(
            access_token=token,
            sync_contacts=_env_bool("INTERCOM_SYNC_CONTACTS", True),
            sync_companies=_env_bool("INTERCOM_SYNC_COMPANIES", True),
            sync_conversations=_env_bool("INTERCOM_SYNC_CONVERSATIONS", True),
            sync_parts=_env_bool("INTERCOM_SYNC_PARTS", True),
            sync_tickets=_env_bool("INTERCOM_SYNC_TICKETS", True),
            sync_admins=_env_bool("INTERCOM_SYNC_ADMINS", False),
            sync_teams=_env_bool("INTERCOM_SYNC_TEAMS", False),
            sync_segments=_env_bool("INTERCOM_SYNC_SEGMENTS", False),
            sync_tags=_env_bool("INTERCOM_SYNC_TAGS", False),
            sync_help_center_collections=_env_bool("INTERCOM_SYNC_HELP_CENTER_COLLECTIONS", False),
            sync_help_center_sections=_env_bool("INTERCOM_SYNC_HELP_CENTER_SECTIONS", False),
            sync_articles=_env_bool("INTERCOM_SYNC_ARTICLES", False),
            force_full=_env_bool("INTERCOM_FORCE_FULL", False),
            start_at_epoch=start_epoch,
            companies_use_scroll=_env_bool("INTERCOM_COMPANIES_USE_SCROLL", True),
            expand_custom_attributes=_env_bool("INTERCOM_EXPAND_CUSTOM_ATTRIBUTES", True),
            max_custom_attr_columns=_env_int("INTERCOM_MAX_CUSTOM_ATTR_COLUMNS", 200),
            flatten_max_depth=_env_int("INTERCOM_FLATTEN_MAX_DEPTH", 2),
            hydrate_workers=workers,
            hydrate_max_in_flight=max_in_flight,
            request_timeout_s=float((_env_int("INTERCOM_REQUEST_TIMEOUT_S", 60))),
            request_connect_timeout_s=float((_env_int("INTERCOM_REQUEST_CONNECT_TIMEOUT_S", 10))),
            request_retries=_env_int("INTERCOM_REQUEST_RETRIES", 2),
            retry_sleep_s=float((_env_int("INTERCOM_RETRY_SLEEP_S", 1))),
            rate_limit_remaining_floor=_env_int("INTERCOM_RATE_LIMIT_REMAINING_FLOOR", 2),
            rate_limit_max_sleep=_env_int("INTERCOM_RATE_LIMIT_MAX_SLEEP", 60),
            intercom_version=(os.getenv("INTERCOM_VERSION") or "2.14").strip() or "2.14",
        )

```

## File: connector.py

```python
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from connectors.runtime.protocol import Connector, ConnectorCapabilities, ReadResult, ReadSelection

from .pipeline import run_pipeline as _run_pipeline
from .pipeline import test_connection as _test_connection


class IntercomConnector(Connector):
    """Intercom connector implementing the standard Connector protocol."""

    name = "intercom"
    capabilities = ConnectorCapabilities(
        selection=False,
        incremental=True,
        full_refresh=True,
        schema=False,
        observed_schema=False,
    )

    def check(self, creds: Dict[str, Any]) -> str:
        return _test_connection(creds)

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates, stats = _run_pipeline(
            creds=creds,
            schema=schema,
            state=state,
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=None,
            stats=stats,
        )


def connector() -> Connector:
    return IntercomConnector()


# =============================================================================
# Legacy entrypoints
# Why: the error indicates the orchestrator is using the legacy loader which
# expects module-level functions, not the Connector protocol object.
# =============================================================================
def test_connection(creds: Dict[str, Any]) -> str:
    return _test_connection(creds)


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    return _run_pipeline(creds=creds, schema=schema, state=state, selection=selection)

```

## File: constants.py

```python
# connectors/intercom/constants.py

from __future__ import annotations

BASE_URL = "https://api.intercom.io"
CONNECTOR_NAME = "intercom"

# Conservative defaults (Intercom endpoints vary by model/tenant)
PAGE_SIZE_CONTACTS_LIST = 50
PAGE_SIZE_CONVERSATIONS_LIST = 50

# Search endpoints use pagination.per_page (often up to 150, but contacts search is commonly smaller)
PAGE_SIZE_SEARCH_DEFAULT = 150
PAGE_SIZE_SEARCH_CONTACTS_DEFAULT = 50

# State keys (per-stream cursors for correctness)
STATE_CURSOR_CONTACTS = "intercom_contacts_last_updated_at"
STATE_CURSOR_COMPANIES = "intercom_companies_last_updated_at"
STATE_CURSOR_CONVERSATIONS = "intercom_conversations_last_updated_at"
STATE_CURSOR_TICKETS = "intercom_tickets_last_updated_at"
STATE_CURSOR_PARTS = "intercom_parts_last_updated_at"

# Env toggles
ENV_SYNC_CONTACTS = "INTERCOM_SYNC_CONTACTS"
ENV_SYNC_COMPANIES = "INTERCOM_SYNC_COMPANIES"
ENV_SYNC_CONVERSATIONS = "INTERCOM_SYNC_CONVERSATIONS"
ENV_SYNC_PARTS = "INTERCOM_SYNC_PARTS"
ENV_SYNC_TICKETS = "INTERCOM_SYNC_TICKETS"

ENV_FORCE_FULL = "INTERCOM_FORCE_FULL"
ENV_START_AT_EPOCH = "INTERCOM_START_AT_EPOCH"

ENV_HYDRATE_WORKERS = "INTERCOM_HYDRATE_WORKERS"
ENV_HYDRATE_MAX_IN_FLIGHT = "INTERCOM_HYDRATE_MAX_IN_FLIGHT"

ENV_RATE_LIMIT_REMAINING_FLOOR = "INTERCOM_RATE_LIMIT_REMAINING_FLOOR"
ENV_RATE_LIMIT_MAX_SLEEP = "INTERCOM_RATE_LIMIT_MAX_SLEEP"

ENV_EXPAND_CUSTOM_ATTRIBUTES = "INTERCOM_EXPAND_CUSTOM_ATTRIBUTES"
ENV_MAX_CUSTOM_ATTR_COLUMNS = "INTERCOM_MAX_CUSTOM_ATTR_COLUMNS"
ENV_FLATTEN_MAX_DEPTH = "INTERCOM_FLATTEN_MAX_DEPTH"

ENV_EVENT_LOG_PATH = "CONNECTOR_UI_EVENT_LOG"
ENV_RUN_ID = "RUN_ID"

# Companies “pull everything” option:
ENV_COMPANIES_USE_SCROLL = "INTERCOM_COMPANIES_USE_SCROLL"  # default true

```

## File: events.py

```python
from __future__ import annotations

from connectors.runtime.events import emit


def http_request(*, stream: str, method: str, url: str) -> None:
    emit("message", "http.request.start", stream=stream, method=method, url=url)


def records_seen(*, stream: str, count: int) -> None:
    emit("count", "schema.records_seen", stream=stream, count=count)

```

## File: http.py

```python
from __future__ import annotations

import json
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from connectors.runtime.events import emit


# =========================================
# SECTION A — SMALL UTILITIES
# Why: keep consistent run IDs + timestamping + log path plumbing.
# =========================================
def run_id() -> str:
    rid = (os.getenv("RUN_ID") or "").strip()
    return rid or uuid.uuid4().hex


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_log_path() -> str:
    """
    Prefer the orchestrator/UI log path env var if provided.
    Why: your CLI prints UI event log paths and operators expect this env name.
    """
    return (
        os.getenv("CONNECTOR_UI_EVENT_LOG")
        or os.getenv("CONNECTOR_EVENT_LOG")  # backward compat
        or "connector_events.jsonl"
    )


def _safe_jsonable(fields: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (fields or {}).items():
        try:
            json.dumps(v, default=str)
            out[k] = v
        except Exception:
            out[k] = repr(v)
    return out


def log_event(rid: str, *, level: str, event: str, **fields: Any) -> None:
    payload_fields = _safe_jsonable(fields)

    rec = {
        "ts": _now_iso(),
        "rid": rid,
        "level": level,
        "event": event,
        **payload_fields,
    }

    # 1) File log (best-effort)
    try:
        with open(event_log_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")
    except Exception:
        pass

    # 2) Runtime event bus (best-effort)
    try:
        emit(
            "message",
            event,
            level=str(level).lower(),
            **payload_fields,
        )
    except Exception:
        pass


# =========================================
# SECTION B — AUTH + CONFIG ACCESS
# Why: avoid importing config in this file; accept cfg duck-typed.
# =========================================
def _get(cfg: Any, name: str, default: Any) -> Any:
    return getattr(cfg, name, default)


def auth_headers(cfg: Any) -> Dict[str, str]:
    token = _get(cfg, "access_token", None) or _get(cfg, "token", None) or _get(cfg, "api_key", None)
    version = str(_get(cfg, "intercom_version", "2.10"))
    headers: Dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": version,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


# =========================================
# SECTION C — TIMEOUTS + RETRIES DEFAULTS
# Why: prevent “stuck on starting” by ensuring requests ALWAYS have timeouts.
# =========================================
@dataclass(frozen=True)
class _Timeouts:
    connect: float
    read: float


def _timeouts(cfg: Any, timeout: Optional[float]) -> _Timeouts:
    connect_s = float(_get(cfg, "request_connect_timeout_s", 10.0))
    if timeout is not None:
        read_s = float(timeout)
    else:
        read_s = float(_get(cfg, "request_timeout_s", 60.0))
    return _Timeouts(connect=connect_s, read=read_s)


def _max_retries(cfg: Any) -> int:
    return int(_get(cfg, "request_retries", 2))


def _base_backoff_s(cfg: Any) -> float:
    return float(_get(cfg, "retry_sleep_s", 1.0))


def _jitter() -> float:
    return random.random() * 0.25


# =========================================
# SECTION D — HTTP CORE
# Why: centralized, safe requests wrapper with:
#   - ALWAYS enforced timeouts
#   - retries (429 / 5xx / transient network)
#   - structured logging + UI event emission
# =========================================
_TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def req_json(
    rid: str,
    cfg: Any,
    session: requests.Session,
    *,
    method: str,
    url: str,
    headers: Dict[str, str],
    stream: str,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> Any:
    t = _timeouts(cfg, timeout)
    retries = _max_retries(cfg)
    backoff = _base_backoff_s(cfg)

    last_err: Optional[BaseException] = None
    last_status: Optional[int] = None
    last_body_preview: Optional[str] = None

    for attempt in range(retries + 1):
        try:
            log_event(
                rid,
                level="INFO",
                event="http.request.start",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                connect_timeout_s=t.connect,
                read_timeout_s=t.read,
            )

            t0 = time.monotonic()
            resp = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=payload,
                timeout=(t.connect, t.read),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)

            last_status = resp.status_code

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else (backoff * (attempt + 1))
                log_event(
                    rid,
                    level="WARN",
                    event="http.request.429",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    retry_after=retry_after,
                    sleep_s=sleep_s,
                )
                if attempt >= retries:
                    resp.raise_for_status()
                time.sleep(sleep_s + _jitter())
                continue

            if resp.status_code in _TRANSIENT_STATUSES and attempt < retries:
                last_body_preview = resp.text[:1000] if resp.text else None
                log_event(
                    rid,
                    level="WARN",
                    event="http.request.transient",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    sleep_s=(backoff * (attempt + 1)),
                )
                time.sleep(backoff * (attempt + 1) + _jitter())
                continue

            resp.raise_for_status()

            if not resp.content:
                log_event(
                    rid,
                    level="INFO",
                    event="http.request.ok",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    keys_count=0,
                )
                return None

            try:
                data = resp.json()
            except Exception:
                body_preview = resp.text[:2000] if resp.text else ""
                log_event(
                    rid,
                    level="ERROR",
                    event="http.request.json_decode_error",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    body_preview=body_preview,
                )
                raise ValueError(f"Non-JSON response from Intercom: status={resp.status_code} body={body_preview}")

            keys_count = len(data) if isinstance(data, dict) else None
            items_count = None
            if isinstance(data, dict):
                for k in ("data", "conversations", "tickets", "contacts", "companies"):
                    v = data.get(k)
                    if isinstance(v, list):
                        items_count = len(v)
                        break

            log_event(
                rid,
                level="INFO",
                event="http.request.ok",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                status=resp.status_code,
                elapsed_ms=elapsed_ms,
                keys_count=keys_count,
                items_count=items_count,
            )
            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            log_event(
                rid,
                level="WARN",
                event="http.request.network_error",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                error=repr(e)[:2000],
                connect_timeout_s=t.connect,
                read_timeout_s=t.read,
            )
            if attempt >= retries:
                raise
            time.sleep(backoff * (attempt + 1) + _jitter())
            continue

        except requests.HTTPError as e:
            last_err = e
            try:
                last_body_preview = e.response.text[:2000] if e.response is not None and e.response.text else None
            except Exception:
                last_body_preview = None

            log_event(
                rid,
                level="ERROR" if attempt >= retries else "WARN",
                event="http.request.http_error",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                status=last_status,
                body_preview=last_body_preview,
                error=repr(e)[:2000],
            )

            if last_status in _TRANSIENT_STATUSES and attempt < retries:
                time.sleep(backoff * (attempt + 1) + _jitter())
                continue
            raise

        except Exception as e:
            last_err = e
            log_event(
                rid,
                level="ERROR",
                event="http.request.unhandled_error",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                status=last_status,
                error=repr(e)[:2000],
            )
            raise

    msg = f"req_json failed: stream={stream} url={url} status={last_status} err={repr(last_err)}"
    raise RuntimeError(msg)

```

## File: pagination.py

```python
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

```

## File: pipeline.py

```python
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

import dlt

from connectors.runtime.protocol import ReadSelection
from connectors.utils import get_shared_session

from .config import IntercomConfig
from .constants import (
    BASE_URL,
    CONNECTOR_NAME,
    STATE_CURSOR_COMPANIES,
    STATE_CURSOR_CONTACTS,
    STATE_CURSOR_CONVERSATIONS,
    STATE_CURSOR_PARTS,
    STATE_CURSOR_TICKETS,
)
from .events import records_seen  # ✅ NEW: emits RuntimeEvent(count) the orchestrator can sum
from .http import auth_headers, event_log_path, log_event, req_json, run_id
from .schema_expand import fetch_data_attributes
from .streams import (
    admins_resource,
    articles_resource,
    companies_resource,
    contacts_resource,
    conversations_resource,
    conversation_parts_transformer,
    help_center_collections_resource,
    help_center_sections_resource,
    segments_resource,
    tags_resource,
    teams_resource,
    tickets_resource,
)


def test_connection(creds: Dict[str, Any]) -> str:
    rid = run_id()
    cfg = IntercomConfig.from_env_and_creds(creds)
    session = get_shared_session(CONNECTOR_NAME)
    headers = auth_headers(cfg)
    _ = req_json(rid, cfg, session, method="GET", url=f"{BASE_URL}/me", headers=headers, stream="check")
    return "Intercom Connected"


def _cursor_from_state(state: Dict[str, Any], key: str) -> Optional[int]:
    cur = state.get(key)
    try:
        return int(cur) if cur is not None else None
    except Exception:
        return None


def _effective_cursor(cfg: IntercomConfig, state_cursor: Optional[int]) -> Optional[int]:
    # why: force_full should ignore state cursor but still respect an optional safety cap.
    if cfg.force_full:
        return cfg.start_at_epoch
    if state_cursor is None:
        return cfg.start_at_epoch
    return state_cursor


def _as_named_resource(name: str, thing: Any) -> Any:
    """
    Wrap iterables/generators into a named DLT resource to avoid:
      dlt.extract.exceptions.ResourceNameMissing

    If `thing` is already a DLT resource, return it as-is.
    """
    if hasattr(thing, "name") and hasattr(thing, "pipe"):
        return thing
    return dlt.resource(thing, name=name)


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    rid = run_id()
    cfg = IntercomConfig.from_env_and_creds(creds)

    # why: shared session provides consistent pooling + adapter behavior in your runtime
    session = get_shared_session(CONNECTOR_NAME)
    headers = auth_headers(cfg)

    stream_counts: Dict[str, int] = {}

    # ✅ NEW: small throttle so we don't spam count events
    _last_emit_at: Dict[str, float] = {}
    _last_emit_count: Dict[str, int] = {}
    _emit_every_s = 0.35
    _emit_every_n = 25

    def bump(name: str, n: int = 1) -> None:
        stream_counts[name] = stream_counts.get(name, 0) + n

        # ✅ NEW: emit progress to orchestrator so header stops saying "Starting…"
        now = time.monotonic()
        cur = int(stream_counts[name])
        prev_at = _last_emit_at.get(name, 0.0)
        prev_count = _last_emit_count.get(name, 0)

        if (cur - prev_count) >= _emit_every_n or (now - prev_at) >= _emit_every_s:
            records_seen(stream=name, count=cur)
            _last_emit_at[name] = now
            _last_emit_count[name] = cur

    # Per-stream cursors (correctness + avoids cross-stream "cursor leapfrogging")
    state_c_contacts = _cursor_from_state(state, STATE_CURSOR_CONTACTS)
    state_c_companies = _cursor_from_state(state, STATE_CURSOR_COMPANIES)
    state_c_convos = _cursor_from_state(state, STATE_CURSOR_CONVERSATIONS)
    state_c_parts = _cursor_from_state(state, STATE_CURSOR_PARTS)
    state_c_tickets = _cursor_from_state(state, STATE_CURSOR_TICKETS)

    cursor_contacts = _effective_cursor(cfg, state_c_contacts)
    cursor_companies = _effective_cursor(cfg, state_c_companies)
    cursor_convos = _effective_cursor(cfg, state_c_convos)
    cursor_parts = _effective_cursor(cfg, state_c_parts)
    cursor_tickets = _effective_cursor(cfg, state_c_tickets)

    # Track max(updated_at) seen per stream so we can advance state safely at end
    max_contacts = cursor_contacts or 0
    max_companies = cursor_companies or 0
    max_convos = cursor_convos or 0
    max_parts = cursor_parts or 0
    max_tickets = cursor_tickets or 0

    def set_max_contacts(v: int) -> None:
        nonlocal max_contacts
        if v and v > max_contacts:
            max_contacts = v

    def set_max_companies(v: int) -> None:
        nonlocal max_companies
        if v and v > max_companies:
            max_companies = v

    def set_max_convos(v: int) -> None:
        nonlocal max_convos
        if v and v > max_convos:
            max_convos = v

    def set_max_parts(v: int) -> None:
        nonlocal max_parts
        if v and v > max_parts:
            max_parts = v

    def set_max_tickets(v: int) -> None:
        nonlocal max_tickets
        if v and v > max_tickets:
            max_tickets = v

    log_event(
        rid,
        level="INFO",
        event="run_start",
        schema=schema,
        force_full=cfg.force_full,
        start_at_epoch=cfg.start_at_epoch,
        cursors_state={
            STATE_CURSOR_CONTACTS: state_c_contacts,
            STATE_CURSOR_COMPANIES: state_c_companies,
            STATE_CURSOR_CONVERSATIONS: state_c_convos,
            STATE_CURSOR_PARTS: state_c_parts,
            STATE_CURSOR_TICKETS: state_c_tickets,
        },
        cursors_effective={
            STATE_CURSOR_CONTACTS: cursor_contacts,
            STATE_CURSOR_COMPANIES: cursor_companies,
            STATE_CURSOR_CONVERSATIONS: cursor_convos,
            STATE_CURSOR_PARTS: cursor_parts,
            STATE_CURSOR_TICKETS: cursor_tickets,
        },
        sync_contacts=getattr(cfg, "sync_contacts", True),
        sync_parts=getattr(cfg, "sync_parts", True),
        sync_conversations=getattr(cfg, "sync_conversations", True),
        sync_tickets=getattr(cfg, "sync_tickets", True),
        sync_admins=getattr(cfg, "sync_admins", False),
        sync_teams=getattr(cfg, "sync_teams", False),
        sync_segments=getattr(cfg, "sync_segments", False),
        sync_tags=getattr(cfg, "sync_tags", False),
        sync_help_center_collections=getattr(cfg, "sync_help_center_collections", False),
        sync_help_center_sections=getattr(cfg, "sync_help_center_sections", False),
        sync_articles=getattr(cfg, "sync_articles", False),
        hydrate_workers=cfg.hydrate_workers,
        hydrate_max_in_flight=cfg.hydrate_max_in_flight,
        expand_custom_attributes=cfg.expand_custom_attributes,
        max_custom_attr_columns=cfg.max_custom_attr_columns,
        flatten_max_depth=cfg.flatten_max_depth,
        companies_use_scroll=cfg.companies_use_scroll,
    )

    # why: best-effort schema enrichment for custom attributes
    data_attr_defs = fetch_data_attributes(rid, cfg, session, headers) if cfg.expand_custom_attributes else {}

    # --- Build raw iterables/generators ---
    raw_contacts = contacts_resource(
        rid,
        cfg,
        session,
        headers,
        cursor=cursor_contacts,
        bump=bump,
        set_max_updated=set_max_contacts,
        data_attr_defs=data_attr_defs,
    )

    raw_companies = companies_resource(
        rid,
        cfg,
        session,
        headers,
        cursor=cursor_companies,
        bump=bump,
        set_max_updated=set_max_companies,
        data_attr_defs=data_attr_defs,
    )

    raw_conversations = conversations_resource(
        rid,
        cfg,
        session,
        headers,
        cursor=cursor_convos,
        bump=bump,
        set_max_updated=set_max_convos,
        data_attr_defs=data_attr_defs,
    )

    raw_parts = conversation_parts_transformer(
        cfg,
        conversations_resource_fn=raw_conversations,
        cursor_parts=cursor_parts,
        bump=bump,
        set_max_updated=set_max_parts,
    )

    raw_tickets = tickets_resource(
        rid,
        cfg,
        session,
        headers,
        cursor=cursor_tickets,
        bump=bump,
        set_max_updated=set_max_tickets,
    )

    raw_admins = admins_resource(rid, cfg, session, headers, bump=bump)
    raw_teams = teams_resource(rid, cfg, session, headers, bump=bump)
    raw_segments = segments_resource(rid, cfg, session, headers, bump=bump)
    raw_tags = tags_resource(rid, cfg, session, headers, bump=bump)
    raw_hc_collections = help_center_collections_resource(rid, cfg, session, headers, bump=bump)
    raw_hc_sections = help_center_sections_resource(rid, cfg, session, headers, bump=bump)
    raw_articles = articles_resource(rid, cfg, session, headers, bump=bump)

    # --- Wrap into named DLT resources (prevents ResourceNameMissing) ---
    contacts = _as_named_resource("contacts", raw_contacts)
    companies = _as_named_resource("companies", raw_companies)
    conversations = _as_named_resource("conversations", raw_conversations)
    parts = _as_named_resource("conversation_parts", raw_parts)
    tickets = _as_named_resource("tickets", raw_tickets)

    admins = _as_named_resource("admins", raw_admins)
    teams = _as_named_resource("teams", raw_teams)
    segments = _as_named_resource("segments", raw_segments)
    tags = _as_named_resource("tags", raw_tags)
    help_center_collections = _as_named_resource("help_center_collections", raw_hc_collections)
    help_center_sections = _as_named_resource("help_center_sections", raw_hc_sections)
    articles = _as_named_resource("articles", raw_articles)

    resources: List[Any] = []

    if getattr(cfg, "sync_contacts", True):
        resources.append(contacts)

    if getattr(cfg, "sync_companies", True):
        resources.append(companies)

    if getattr(cfg, "sync_conversations", True):
        resources.append(conversations)
        if getattr(cfg, "sync_parts", True):
            resources.append(parts)

    if getattr(cfg, "sync_tickets", True):
        resources.append(tickets)

    # Optional “extra” streams
    if getattr(cfg, "sync_admins", False):
        resources.append(admins)
    if getattr(cfg, "sync_teams", False):
        resources.append(teams)
    if getattr(cfg, "sync_segments", False):
        resources.append(segments)
    if getattr(cfg, "sync_tags", False):
        resources.append(tags)
    if getattr(cfg, "sync_help_center_collections", False):
        resources.append(help_center_collections)
    if getattr(cfg, "sync_help_center_sections", False):
        resources.append(help_center_sections)
    if getattr(cfg, "sync_articles", False):
        resources.append(articles)

    pipeline = dlt.pipeline(pipeline_name=CONNECTOR_NAME, destination="postgres", dataset_name=schema)

    try:
        info = pipeline.run(resources)
        log_event(rid, level="INFO", event="pipeline_ok", loads=getattr(info, "loads_ids", None))
    except Exception as e:
        log_event(rid, level="ERROR", event="pipeline_err", error=str(e)[:2000])
        raise

    # ✅ NEW: final flush so the UI ends on correct totals
    for s, c in stream_counts.items():
        try:
            records_seen(stream=s, count=int(c))
        except Exception:
            pass

    # Cursor updates (advance-only, per stream)
    state_updates: Dict[str, Any] = {}

    if max_contacts and (state_c_contacts is None or max_contacts > int(state_c_contacts)):
        state_updates[STATE_CURSOR_CONTACTS] = int(max_contacts)

    if max_companies and (state_c_companies is None or max_companies > int(state_c_companies)):
        state_updates[STATE_CURSOR_COMPANIES] = int(max_companies)

    if max_convos and (state_c_convos is None or max_convos > int(state_c_convos)):
        state_updates[STATE_CURSOR_CONVERSATIONS] = int(max_convos)

    if max_parts and (state_c_parts is None or max_parts > int(state_c_parts)):
        state_updates[STATE_CURSOR_PARTS] = int(max_parts)

    if max_tickets and (state_c_tickets is None or max_tickets > int(state_c_tickets)):
        state_updates[STATE_CURSOR_TICKETS] = int(max_tickets)

    if state_updates:
        log_event(rid, level="INFO", event="cursor_update", updates=state_updates)

    report_lines = [
        "Intercom sync completed.",
        f"Contacts: {'on' if getattr(cfg, 'sync_contacts', True) else 'off'}",
        f"Companies: {'on' if getattr(cfg, 'sync_companies', True) else 'off'} (scroll: {cfg.companies_use_scroll})",
        f"Conversations: {'on' if getattr(cfg, 'sync_conversations', True) else 'off'}",
        f"Conversation parts: {'on' if (getattr(cfg, 'sync_conversations', True) and getattr(cfg, 'sync_parts', True)) else 'off'}",
        f"Tickets: {'on' if getattr(cfg, 'sync_tickets', True) else 'off'}",
        f"Admins: {'on' if getattr(cfg, 'sync_admins', False) else 'off'}",
        f"Teams: {'on' if getattr(cfg, 'sync_teams', False) else 'off'}",
        f"Segments: {'on' if getattr(cfg, 'sync_segments', False) else 'off'}",
        f"Tags: {'on' if getattr(cfg, 'sync_tags', False) else 'off'}",
        f"HC Collections: {'on' if getattr(cfg, 'sync_help_center_collections', False) else 'off'}",
        f"HC Sections: {'on' if getattr(cfg, 'sync_help_center_sections', False) else 'off'}",
        f"Articles: {'on' if getattr(cfg, 'sync_articles', False) else 'off'}",
        f"Force full: {cfg.force_full}",
        f"Cursor updates: {state_updates or 'none'}",
        f"Dynamic attrs: {cfg.expand_custom_attributes} (max {cfg.max_custom_attr_columns})",
        f"Flatten depth: {cfg.flatten_max_depth}",
        f"Hydrate workers: {cfg.hydrate_workers} (max in-flight {cfg.hydrate_max_in_flight})",
        f"Stream counts: {stream_counts or 'n/a'}",
        f"Run id: {rid}",
        f"UI event log: {event_log_path()}",
    ]

    log_event(rid, level="INFO", event="stream_counts", counts=stream_counts)
    log_event(rid, level="INFO", event="run_done")

    return "\n".join(report_lines), None, state_updates, {"stream_counts": stream_counts}

```

## File: schema.py

```python
from __future__ import annotations

from typing import Any, Dict, List

STREAM_FIELDS: Dict[str, List[str]] = {
    "contacts": [
        "id",
        "type",
        "created_at",
        "updated_at",
        "email",
        "name",
        "role",
        "custom_attributes",
    ],
    "companies": [
        "id",
        "name",
        "created_at",
        "updated_at",
        "monthly_spend",
        "custom_attributes",
    ],
    "conversations": [
        "id",
        "state",
        "created_at",
        "updated_at",
        "waiting_since",
        "priority",
        "source",
    ],
    "conversation_parts": [
        "id",
        "conversation_id",
        "part_type",
        "body",
        "created_at",
        "updated_at",
        "author",
        "attachments",
    ],
    "tickets": [
        "id",
        "created_at",
        "updated_at",
        "ticket_state",
        "ticket_attributes",
    ],
}


def get_observed_schema() -> Dict[str, Any]:
    streams = {}
    for stream, fields in STREAM_FIELDS.items():
        properties = {f: {"type": "string"} for f in fields}
        if "updated_at" in properties:
            properties["updated_at"] = {"type": "integer"}
        if "created_at" in properties:
            properties["created_at"] = {"type": "integer"}
        if "custom_attributes" in properties:
            properties["custom_attributes"] = {"type": "object"}
        if "monthly_spend" in properties:
            properties["monthly_spend"] = {"type": "number"}
        if "author" in properties:
            properties["author"] = {"type": "object"}
        if "attachments" in properties:
            properties["attachments"] = {"type": "array"}

        properties["raw"] = {"type": "object"}

        streams[stream] = {
            "primary_key": ["id"],
            "properties": properties,
        }

    return {"streams": streams}

```

## File: schema_expand.py

```python
from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from .config import IntercomConfig
from .constants import BASE_URL
from .http import log_event, req_json
from .pagination import extract_items


def sanitize_col(name: str) -> str:
    out = []
    for ch in str(name):
        if ch.isalnum() or ch == "_":
            out.append(ch.lower())
        elif ch in (" ", "-", ".", ":", "/"):
            out.append("_")
    s = "".join(out).strip("_")
    while "__" in s:
        s = s.replace("__", "_")
    return s or "field"


def flatten_primitives(obj: Any, *, prefix: str = "", depth: int = 0, max_depth: int = 2) -> Dict[str, Any]:
    if depth > max_depth:
        return {}

    out: Dict[str, Any] = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            key = sanitize_col(k)
            p = f"{prefix}{key}" if prefix else key
            out.update(flatten_primitives(v, prefix=f"{p}__", depth=depth + 1, max_depth=max_depth))
        return out

    if isinstance(obj, list):
        return {}

    if obj is None or isinstance(obj, (str, int, float, bool)):
        if prefix.endswith("__"):
            out[prefix[:-2]] = obj
        elif prefix:
            out[prefix] = obj
        return out

    return {}


def _coerce_by_type(v: Any, t: Optional[str]) -> Any:
    if v is None:
        return None
    tt = (t or "").lower()
    try:
        if tt in ("integer", "int"):
            return int(v)
        if tt in ("float", "number", "decimal"):
            return float(v)
        if tt in ("boolean", "bool"):
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in ("1", "true", "yes", "y", "on")
        return v
    except Exception:
        return v


def fetch_data_attributes(
    rid: str, cfg: IntercomConfig, session, headers: Dict[str, str]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Returns: { model: { attribute_name: attribute_definition_dict } }

    Behavior:
    - BEST-EFFORT: skip unsupported models (422) instead of failing the run.
    """
    candidate_models = ("contact", "user", "lead", "company", "conversation", "ticket")
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for model in candidate_models:
        stream = f"data_attributes:{model}"
        url = f"{BASE_URL}/data_attributes"
        params = {"model": model}

        try:
            resp = req_json(rid, cfg, session, method="GET", url=url, headers=headers, params=params, stream=stream)
        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            body = None
            try:
                body = getattr(getattr(e, "response", None), "text", None)
            except Exception:
                body = None

            if status == 422:
                log_event(
                    rid,
                    level="WARN",
                    event="data_attributes_model_unsupported",
                    model=model,
                    status=status,
                    body=(body[:500] if isinstance(body, str) else None),
                )
                out[model] = {}
                continue

            raise
        except Exception as e:
            log_event(rid, level="ERROR", event="data_attributes_load_err", model=model, error=str(e)[:2000])
            raise

        items = extract_items(resp, fallback_keys=("data", "data_attributes"))

        defs: Dict[str, Dict[str, Any]] = {}
        for it in items[: cfg.max_custom_attr_columns]:
            if not isinstance(it, dict):
                continue
            name = it.get("name") or it.get("full_name") or it.get("label")
            if not name:
                continue
            defs[str(name)] = it

        out[model] = defs
        log_event(rid, level="INFO", event="data_attributes_loaded", model=model, count=len(defs))

    return out


def expand_custom_attributes(
    attrs: Any,
    *,
    cfg: IntercomConfig,
    attr_defs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if not isinstance(attrs, dict):
        return {}

    out: Dict[str, Any] = {}
    count = 0

    for k, v in attrs.items():
        if count >= cfg.max_custom_attr_columns:
            break

        d = attr_defs.get(k) or {}
        dtype = d.get("data_type") or d.get("type")

        col = f"ca__{sanitize_col(k)}"
        out[col] = _coerce_by_type(v, str(dtype) if dtype else None)
        count += 1

    return out

```

## File: selection.py

```python
from __future__ import annotations

from typing import Any, Set


def normalize_selection(selection: Any) -> Set[str]:
    """Normalize selection to a set of stream names. Empty set means all selected."""
    if selection is None:
        return set()

    for attr in ("streams", "tables", "resources"):
        val = getattr(selection, attr, None)
        if isinstance(val, (list, set, tuple)):
            return set(str(x) for x in val) if val else set()

    if isinstance(selection, dict):
        for key in ("streams", "tables", "resources"):
            val = selection.get(key)
            if isinstance(val, list):
                return set(str(x) for x in val) if val else set()

    return set()


def is_stream_enabled(stream: str, selection: Set[str], exclude_streams: Set[str] | None = None) -> bool:
    if exclude_streams and stream in exclude_streams:
        return False
    return (not selection) or (stream in selection)

```

## File: state.py

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class StateStore:
    """
    Why: your logs show conversations repeatedly fetching page 1.
    This store exists so the stream can checkpoint paging cursor after each page.

    Swap get/set with your persistence (db/file/kv). This default is in-memory.
    """

    _state: Dict[str, Any]

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._state[key] = value

    def snapshot(self) -> Dict[str, Any]:
        return dict(self._state)

```

## File: streams/__init__.py

```python
"""
Streams package.

Why:
- `pipeline.py` imports from `connectors.intercom.streams` expecting symbols like
  `admins_resource`, etc.
- Because `streams/` is a package, Python resolves `connectors.intercom.streams` to this
  package, not to any sibling `streams.py` file.
- Therefore, this package must export the public stream API itself.
"""

from __future__ import annotations

from .resources import (
    admins_resource,
    articles_resource,
    companies_resource,
    contacts_resource,
    conversation_parts_transformer,
    conversations_resource,
    help_center_collections_resource,
    help_center_sections_resource,
    segments_resource,
    tags_resource,
    teams_resource,
    tickets_resource,
)

__all__ = [
    "contacts_resource",
    "companies_resource",
    "conversations_resource",
    "tickets_resource",
    "admins_resource",
    "teams_resource",
    "segments_resource",
    "tags_resource",
    "help_center_collections_resource",
    "help_center_sections_resource",
    "articles_resource",
    "conversation_parts_transformer",
]

```

## File: streams/conversations.py

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from ..client import IntercomClient
from ..pagination import PageAdvance, iterate_pages
from ..state import StateStore


@dataclass(frozen=True)
class ConversationsStreamConfig:
    page_size: int = 150
    state_key: str = "intercom_conversations_starting_after"
    max_pages_per_run: Optional[int] = None


class ConversationsStream:
    def __init__(
        self,
        client: IntercomClient,
        state: StateStore,
        cfg: ConversationsStreamConfig = ConversationsStreamConfig(),
    ) -> None:
        self._client = client
        self._state = state
        self._cfg = cfg

    def read(self) -> Iterator[Dict[str, Any]]:
        starting_after = self._state.get(self._cfg.state_key)

        initial_params: Dict[str, Any] = {"per_page": self._cfg.page_size}
        if starting_after:
            initial_params["starting_after"] = starting_after

        def fetch(params: Dict[str, Any]) -> Dict[str, Any]:
            return self._client.get("/conversations", params=params)

        def get_next(_params: Dict[str, Any], resp: Dict[str, Any]) -> Optional[PageAdvance]:
            pages = resp.get("pages") or {}
            nxt = pages.get("next")
            if not nxt:
                return None

            if isinstance(nxt, dict):
                nxt_starting_after = nxt.get("starting_after")
                if not nxt_starting_after:
                    return None
                new_params = dict(initial_params)
                new_params["starting_after"] = nxt_starting_after
                return PageAdvance(params=new_params, fingerprint=str(nxt_starting_after))

            if isinstance(nxt, str):
                nxt_starting_after = _extract_query_param(nxt, "starting_after")
                if not nxt_starting_after:
                    return None
                new_params = dict(initial_params)
                new_params["starting_after"] = nxt_starting_after
                return PageAdvance(params=new_params, fingerprint=str(nxt_starting_after))

            return None

        for _page_idx, req_params, resp in iterate_pages(
            fetch_page=fetch,
            initial_params=initial_params,
            get_next=get_next,
            max_pages=self._cfg.max_pages_per_run,
        ):
            conversations = resp.get("conversations") or resp.get("data") or []
            if not isinstance(conversations, list):
                conversations = []

            for c in conversations:
                if isinstance(c, dict):
                    yield c

            last_id = _last_conversation_id(conversations)
            if last_id:
                self._state.set(self._cfg.state_key, last_id)
            else:
                cur = req_params.get("starting_after")
                if cur:
                    self._state.set(self._cfg.state_key, cur)


def _extract_query_param(url: str, key: str) -> Optional[str]:
    try:
        from urllib.parse import urlparse, parse_qs

        q = parse_qs(urlparse(url).query)
        vals = q.get(key)
        if not vals:
            return None
        return vals[0]
    except Exception:
        return None


def _last_conversation_id(conversations: List[Any]) -> Optional[str]:
    for item in reversed(conversations):
        if isinstance(item, dict):
            v = item.get("id")
            if isinstance(v, str) and v:
                return v
    return None

```

## File: streams/data_attributes.py

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from ..client import IntercomClient


SUPPORTED_MODELS = {"contact", "company", "conversation"}


@dataclass(frozen=True)
class DataAttributesStreamConfig:
    models: List[str]


class DataAttributesStream:
    def __init__(self, client: IntercomClient, cfg: Optional[DataAttributesStreamConfig] = None) -> None:
        self._client = client
        self._cfg = cfg or DataAttributesStreamConfig(models=["contact", "company", "conversation"])

    def read(self) -> Iterator[Dict[str, Any]]:
        for model in self._cfg.models:
            if model not in SUPPORTED_MODELS:
                continue
            resp = self._client.get("/data_attributes", params={"model": model})
            yield {"model": model, "data": resp.get("data", [])}

```

## File: streams/resources.py

```python
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

```

