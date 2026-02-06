# Pandadoc Connector Files Export

This document contains all files from the Pandadoc connector directory.

## File: __init__.py

```python
from __future__ import annotations

from .connector import PandaDocConnector, connector  # noqa: F401

__all__ = ["PandaDocConnector", "connector"]

```

## File: connector.py

```python
from __future__ import annotations

from typing import Any, Dict

from connectors.runtime.protocol import Connector, ReadResult, ReadSelection

from .pipeline import run_pipeline, test_connection
from .schema import observed_schema


class PandaDocConnector(Connector):
    name = "pandadoc"

    def check(self, creds: Dict[str, Any]) -> str:
        return test_connection(creds)

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates = run_pipeline(
            creds=creds,
            schema=schema,
            state=state,
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=observed_schema(),
        )


def connector() -> Connector:
    return PandaDocConnector()

```

## File: constants.py

```python
from __future__ import annotations

PROD_BASE = "https://api.pandadoc.com"
SANDBOX_BASE = "https://api-sandbox.pandadoc.com"

CONNECTOR_NAME = "pandadoc"

# API paths
DOCS_LIST_PATH = "/public/v1/documents"
DOC_DETAILS_PATH_TMPL = "/public/v1/documents/{id}/details"

# Paging defaults
DEFAULT_PAGE_SIZE = 100

# Rate limit / pacing (kept from original behavior)
SANDBOX_SLEEP_SECONDS = 6.2  # ~10 rpm
PROD_DETAILS_SLEEP_SECONDS = 0.12
PROD_LIST_SLEEP_SECONDS = 0.03

# 429 retry
DEFAULT_MAX_429_RETRIES = 8
MAX_BACKOFF_SECONDS = 30

# State keys (keep legacy compatibility)
STATE_MODIFIED_FROM = "pandadoc_modified_from"   # preferred / current
STATE_LAST_SUCCESS_AT = "last_success_at"
STATE_START_DATE = "start_date"

```

## File: errors.py

```python
from __future__ import annotations


class PandaDocError(Exception):
    pass


class UnauthorizedError(PandaDocError):
    pass


class TooManyRequestsError(PandaDocError):
    pass

```

## File: events.py

```python
from __future__ import annotations

from typing import Any, Optional

from .constants import CONNECTOR_NAME


def emit_event(
    event_type: str,
    message: str,
    *,
    stream: Optional[str] = None,
    count: Optional[int] = None,
    level: str = "info",
    **fields: Any,
) -> None:
    """Emit structured event to runtime bus. Fails silently."""
    try:
        from connectors.runtime.events import emit
    except Exception:
        return

    try:
        emit(
            event_type,
            message,
            connector=CONNECTOR_NAME,
            stream=stream,
            count=count,
            level=level,
            **(fields or {}),
        )
    except Exception:
        pass


def debug(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="debug", **fields)


def info(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="info", **fields)


def warn(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="warn", **fields)


def error(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="error", **fields)

```

## File: http.py

```python
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from .constants import (
    DEFAULT_MAX_429_RETRIES,
    MAX_BACKOFF_SECONDS,
    PROD_DETAILS_SLEEP_SECONDS,
    PROD_LIST_SLEEP_SECONDS,
    SANDBOX_BASE,
    SANDBOX_SLEEP_SECONDS,
)
from .errors import TooManyRequestsError, UnauthorizedError
from .events import debug, warn
from .utils_bridge import DEFAULT_TIMEOUT


def base_url(creds: Dict[str, Any], *, prod_base: str, sandbox_base: str) -> str:
    if creds.get("base_url"):
        return str(creds["base_url"]).rstrip("/")
    if creds.get("sandbox") is True or str(creds.get("environment", "")).lower() == "sandbox":
        return sandbox_base
    return prod_base


def headers(creds: Dict[str, Any]) -> Dict[str, str]:
    return {
        "Authorization": f"API-Key {creds['api_key']}",
        "accept": "application/json",
    }


def sleep_for_limits(base: str, endpoint_kind: str, *, sandbox_base: str) -> None:
    if base == sandbox_base:
        time.sleep(SANDBOX_SLEEP_SECONDS)
        return
    if endpoint_kind == "details":
        time.sleep(PROD_DETAILS_SLEEP_SECONDS)
    else:
        time.sleep(PROD_LIST_SLEEP_SECONDS)


def request_with_429_retry(
    session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = DEFAULT_MAX_429_RETRIES,
) -> Any:
    backoff = 1.0

    for attempt in range(0, max_retries):
        try:
            resp = session.request(method, url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            warn(
                "http.request.failed",
                stream="pandadoc",
                attempt=attempt + 1,
                error=str(e),
            )
            if attempt == max_retries - 1:
                raise Exception(f"Failed to connect to PandaDoc API after {max_retries} attempts: {str(e)}")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue

        if resp.status_code != 429:
            if resp.status_code == 401:
                raise UnauthorizedError("PandaDoc unauthorized (check API key / workspace access)")
            resp.raise_for_status()
            return resp.json() if resp.content else {}

        retry_after = resp.headers.get("Retry-After")
        if retry_after and str(retry_after).isdigit():
            sleep_s = int(retry_after)
        else:
            sleep_s = backoff
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

        if sleep_s > 60:
            warn(
                "http.429_long_wait",
                stream="pandadoc",
                sleep_seconds=sleep_s,
                message=f"PandaDoc API requested long wait ({sleep_s}s Retry-After)",
            )
            # Cap sleep to avoid indefinite hanging if the API returns something crazy,
            # but still respect it mostly. If it's > 5 minutes, something is wrong.
            if sleep_s > 300:
                sleep_s = 300
                warn(
                    "http.429_wait_capped",
                    stream="pandadoc",
                    sleep_seconds=sleep_s,
                    message=f"PandaDoc API wait capped to {sleep_s}s",
                )

        warn(
            "http.429_backoff",
            stream="pandadoc",
            attempt=attempt + 1,
            sleep_seconds=sleep_s,
        )
        time.sleep(sleep_s)

    raise TooManyRequestsError("PandaDoc: too many 429 responses. Reduce backfill window or slow down.")


def extract_links(obj: Any) -> Any:
    if isinstance(obj, dict) and isinstance(obj.get("links"), list):
        return obj["links"]
    return []


def ui_link_guess(document_id: str) -> str:
    return f"https://app.pandadoc.com/a/#/documents/{document_id}"

```

## File: paging.py

```python
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

```

## File: pipeline.py

```python
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import dlt

from connectors.runtime.protocol import ReadSelection

from .constants import (
    DOC_DETAILS_PATH_TMPL,
    DOCS_LIST_PATH,
    PROD_BASE,
    SANDBOX_BASE,
    STATE_LAST_SUCCESS_AT,
    STATE_MODIFIED_FROM,
    STATE_START_DATE,
)
from .events import emit_event, info, warn
from .http import base_url, extract_links, headers, request_with_429_retry, sleep_for_limits, ui_link_guess
from .paging import iter_paged_results
from .schema import observed_schema
from .selection import is_selected, normalize_selection
from .time_utils import is_later, iso_z, parse_iso, utc_now_iso
from .utils_bridge import add_metadata, requests_retry_session


def _from_state(state: Dict[str, Any], key: str) -> Optional[Any]:
    """Fetch a key from either legacy flat state or protocol's global block."""
    if not isinstance(state, dict):
        return None
    if key in state:
        return state.get(key)
    glob = state.get("global")
    if isinstance(glob, dict):
        return glob.get(key)
    return None


def _compute_modified_from(state: Dict[str, Any]) -> Optional[str]:
    """
    Compatibility / no-reinit guarantee:
      - Prefer pandadoc_modified_from
      - Otherwise last_success_at
      - Otherwise start_date
    """
    start_date = parse_iso(_from_state(state, STATE_START_DATE))
    last_success = parse_iso(_from_state(state, STATE_LAST_SUCCESS_AT))
    cursor = parse_iso(_from_state(state, STATE_MODIFIED_FROM)) or last_success or start_date
    return iso_z(cursor) if cursor else None


def _updated_cursor(prev_cursor_iso: Optional[str], new_iso: str) -> str:
    if prev_cursor_iso and not is_later(new_iso, prev_cursor_iso):
        return prev_cursor_iso
    return new_iso


def test_connection(creds: Dict[str, Any]) -> str:
    base = base_url(creds, prod_base=PROD_BASE, sandbox_base=SANDBOX_BASE)
    session = requests_retry_session()
    h = headers(creds)

    resp = session.get(
        f"{base}{DOCS_LIST_PATH}",
        headers=h,
        params={"count": 1, "page": 1},
        timeout=10,
    )
    if resp.status_code == 401:
        raise Exception("Invalid PandaDoc API Key")
    resp.raise_for_status()
    return f"PandaDoc Connected ({'sandbox' if base == SANDBOX_BASE else 'prod'})"


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    base = base_url(creds, prod_base=PROD_BASE, sandbox_base=SANDBOX_BASE)
    session = requests_retry_session()
    h = headers(creds)

    selected = normalize_selection(selection)
    want_documents = is_selected(selected, "documents")
    want_recipients = is_selected(selected, "recipients")
    want_tokens = is_selected(selected, "tokens")

    if not (want_documents or want_recipients or want_tokens):
        warn("sync.no_streams_selected", stream="pandadoc")
        return "No streams selected", None, {}

    modified_from = _compute_modified_from(state)
    prev_cursor = _from_state(state, STATE_MODIFIED_FROM)
    if prev_cursor is not None:
        prev_cursor = str(prev_cursor)
    max_seen_modified: Optional[str] = None

    info(
        "sync.start",
        stream="pandadoc",
        base=base,
        has_modified_from=bool(modified_from),
        want_documents=want_documents,
        want_recipients=want_recipients,
        want_tokens=want_tokens,
    )

    def fetch_docs_page(page: int, count: int) -> Dict[str, Any]:
        emit_event("message", "paging.page.start", stream="pandadoc", page=page, limit=count)
        params: Dict[str, Any] = {"count": count, "page": page, "order_by": "date_modified"}
        if modified_from:
            params["modified_from"] = modified_from

        sleep_for_limits(base, "list", sandbox_base=SANDBOX_BASE)
        return request_with_429_retry(
            session,
            "GET",
            f"{base}{DOCS_LIST_PATH}",
            headers=h,
            params=params,
        ) or {}

    def fetch_details(doc_id: str) -> Dict[str, Any]:
        sleep_for_limits(base, "details", sandbox_base=SANDBOX_BASE)
        path = DOC_DETAILS_PATH_TMPL.format(id=doc_id)
        try:
            # Add timeout to prevent hanging
            response = request_with_429_retry(
                session,
                "GET",
                f"{base}{path}",
                headers=h,
                params=None,
            ) or {}
            return response
        except Exception as e:
            warn(
                "fetch.details.failed",
                stream="pandadoc",
                doc_id=doc_id,
                error=str(e),
            )
            # Return empty dict instead of failing the entire sync
            return {}

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="documents")
    def documents() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_modified
        if not want_documents and not want_recipients and not want_tokens:
            return

        count = 0
        for d in iter_paged_results(fetch_docs_page):
            count += 1
            doc_id = d.get("id")
            
            # Emit progress more frequently and with detail so we know it's not stuck
            if count % 5 == 0:
                emit_event("count", "scanned", stream="documents", count=count)
            
            # Emit a processing event (debug/info) so logs show movement
            # Using 'debug' level so it doesn't clutter the main UI status too much unless needed,
            # but if it's slow, the status line will show this.
            emit_event("message", f"processing doc {doc_id}", stream="documents", level="debug", doc_id=doc_id)

            if want_documents:
                dm = d.get("date_modified")
                if dm:
                    dm_iso = parse_iso(dm)
                    if dm_iso:
                        iso_str = iso_z(dm_iso)
                        if max_seen_modified is None or is_later(iso_str, max_seen_modified):
                            max_seen_modified = iso_str
                yield add_metadata(
                    {
                        "id": doc_id,
                        "name": d.get("name"),
                        "status": d.get("status"),
                        "date_created": d.get("date_created"),
                        "date_modified": d.get("date_modified"),
                        "expiration_date": d.get("expiration_date"),
                        "version": d.get("version"),
                        "uuid": d.get("uuid"),
                        "api_links": extract_links(d),
                        "ui_link_guess": ui_link_guess(doc_id) if doc_id else None,
                    },
                    "pandadoc",
                )

            if not doc_id or not (want_recipients or want_tokens):
                continue

            det = fetch_details(str(doc_id))
            yield {
                "id": doc_id,
                "_details": det,
                "_skip_documents_write": True,
            }

    @dlt.transformer(
        data_from=documents,
        write_disposition="merge",
        primary_key=("document_id", "recipient_key"),
        table_name="recipients",
    )
    def recipients(doc_row: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_recipients:
            return

        det = doc_row.get("_details") if isinstance(doc_row, dict) else None
        doc_id = doc_row.get("id")
        if not doc_id or not isinstance(det, dict):
            return

        recs = det.get("recipients") or det.get("document", {}).get("recipients") or []
        if isinstance(recs, dict):
            recs = recs.get("recipients") or []
        if not isinstance(recs, list):
            return

        for r in recs:
            if not isinstance(r, dict):
                continue

            recipient_key = (
                r.get("id")
                or r.get("email")
                or f"{r.get('first_name','')}-{r.get('last_name','')}".strip("-")
                or "unknown"
            )

            yield add_metadata(
                {
                    "document_id": str(doc_id),
                    "recipient_key": str(recipient_key),
                    "recipient_id": r.get("id"),
                    "email": r.get("email"),
                    "first_name": r.get("first_name") or r.get("firstName"),
                    "last_name": r.get("last_name") or r.get("lastName"),
                    "role": r.get("role"),
                    "signing_order": r.get("signing_order") or r.get("signingOrder"),
                    "status": r.get("status"),
                    "completed_at": r.get("completed_at") or r.get("completedAt"),
                    "metadata": r.get("metadata"),
                },
                "pandadoc",
            )

    @dlt.transformer(
        data_from=documents,
        write_disposition="merge",
        primary_key=("document_id", "token_name"),
        table_name="tokens",
    )
    def tokens(doc_row: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_tokens:
            return

        det = doc_row.get("_details") if isinstance(doc_row, dict) else None
        doc_id = doc_row.get("id")
        if not doc_id or not isinstance(det, dict):
            return

        toks = det.get("tokens") or det.get("document", {}).get("tokens") or []
        if isinstance(toks, dict):
            toks = toks.get("tokens") or []
        if not isinstance(toks, list):
            return

        for t in toks:
            if not isinstance(t, dict):
                continue
            name = t.get("name") or t.get("token") or t.get("key") or "unknown"
            yield add_metadata(
                {
                    "document_id": str(doc_id),
                    "token_name": str(name),
                    "value": t.get("value"),
                    "raw": json.dumps(t, ensure_ascii=False, default=str),
                },
                "pandadoc",
            )

    resources: List[Any] = []

    if want_documents and (want_recipients or want_tokens):
        resources = [documents, recipients, tokens]
    elif want_documents:
        @dlt.resource(write_disposition="merge", primary_key="id", table_name="documents")
        def documents_only():
            for row in documents():
                if isinstance(row, dict) and row.get("_skip_documents_write"):
                    continue
                yield row

        resources = [documents_only]
    else:
        resources = [documents, recipients, tokens]

    pipeline = dlt.pipeline(pipeline_name="pandadoc", destination="postgres", dataset_name=schema)
    info("pipeline.run.start", stream="pandadoc", destination="postgres", dataset=schema)
    try:
        run_info = pipeline.run(resources)
    except Exception as e:
        import logging
        logging.error(f"Pandadoc pipeline run failed: {str(e)}")
        raise
    info("pipeline.run.done", stream="pandadoc")

    # Advance cursor to newest date_modified seen; fallback to prev_cursor or now if none.
    cursor_candidate = max_seen_modified or prev_cursor or utc_now_iso()
    cursor_out = _updated_cursor(prev_cursor, cursor_candidate)

    state_updates: Dict[str, Any] = {STATE_MODIFIED_FROM: cursor_out, STATE_LAST_SUCCESS_AT: cursor_out}
    return (
        "PandaDoc sync completed.\n"
        "- Tables: documents, recipients, tokens\n"
        "- Recipients + tokens derived from Document Details via transformer (no full snapshot buffering)\n"
        f"{run_info}",
        None,
        state_updates,
    )

```

## File: schema.py

```python
from __future__ import annotations

from typing import Any, Dict


def observed_schema() -> Dict[str, Any]:
    return {
        "streams": {
            "documents": {
                "primary_key": ["id"],
                "fields": {
                    "id": "string",
                    "name": "string",
                    "status": "string",
                    "date_created": "string",
                    "date_modified": "string",
                    "expiration_date": "string",
                    "version": "number",
                    "uuid": "string",
                    "api_links": "array",
                    "ui_link_guess": "string",
                    "_dlt_load_time": "string",
                    "_dlt_source": "string",
                },
            },
            "recipients": {
                "primary_key": ["document_id", "recipient_key"],
                "fields": {
                    "document_id": "string",
                    "recipient_key": "string",
                    "recipient_id": "string",
                    "email": "string",
                    "first_name": "string",
                    "last_name": "string",
                    "role": "string",
                    "signing_order": "number",
                    "status": "string",
                    "completed_at": "string",
                    "metadata": "object",
                    "_dlt_load_time": "string",
                    "_dlt_source": "string",
                },
            },
            "tokens": {
                "primary_key": ["document_id", "token_name"],
                "fields": {
                    "document_id": "string",
                    "token_name": "string",
                    "value": "string",
                    "raw": "string",
                    "_dlt_load_time": "string",
                    "_dlt_source": "string",
                },
            },
        }
    }

```

## File: selection.py

```python
from __future__ import annotations

from typing import Any, Set


def normalize_selection(selection: Any) -> Set[str]:
    """
    Normalize selection to a set of stream names.
    Empty set means all selected.
    """
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


def is_selected(selection: Set[str], stream: str) -> bool:
    return (not selection) or (stream in selection)

```

## File: time_utils.py

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_later(a: str, b: str) -> bool:
    da = parse_iso(a)
    db = parse_iso(b)
    if da and db:
        return da > db
    return a > b

```

## File: utils_bridge.py

```python
from __future__ import annotations

# Bridge imports to shared connector utilities to avoid circular imports,
# matching the Trello and Fireflies split-package pattern.

from connectors.utils import DEFAULT_TIMEOUT, add_metadata, requests_retry_session  # type: ignore[F401]

```

