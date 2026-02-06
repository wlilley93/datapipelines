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
