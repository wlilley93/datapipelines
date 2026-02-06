from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dlt
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from connectors.runtime.protocol import ReadSelection

from .auth import build_auth_url, exchange_code_for_tokens, parse_code_from_redirect
from .constants import (
    ACCOUNTING_BASE,
    TOKEN_URL,
    DEFAULT_MAX_PAGES_PER_STREAM,
    DEFAULT_LOOKBACK_MINUTES,
    MAX_LOOKBACK_MINUTES,
)
from .errors import XeroAuthError, InteractiveAuthRequired
from .events import info, warn
from .http import get_tenant_id, request_json_with_429_retry, xero_headers
from .paging import paged_collection
from .selection import is_selected, normalize_selection
from .time_utils import clamp_int, if_modified_since_header, is_later, lookback_dt, parse_iso, utc_now_iso
from .utils_bridge import add_metadata


# ----------------------------
# AUTH / TOKEN LIFECYCLE
# ----------------------------

_DEFAULT_TOKEN_LEEWAY_SECONDS = 120  # refresh a bit early


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt_maybe(s: Any) -> Optional[datetime]:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _token_is_fresh(creds: Dict[str, Any]) -> bool:
    access = creds.get("access_token")
    if not access:
        return False

    expires_at = _parse_dt_maybe(creds.get("expires_at"))
    if not expires_at:
        return False

    return expires_at > (_utc_now() + timedelta(seconds=_DEFAULT_TOKEN_LEEWAY_SECONDS))


def _set_expires_at(creds: Dict[str, Any], expires_in: Any) -> Dict[str, Any]:
    out = dict(creds)
    try:
        seconds = int(expires_in)
        out["expires_at"] = (_utc_now() + timedelta(seconds=max(0, seconds))).isoformat()
    except Exception:
        pass
    return out


def refresh_token(creds: Dict[str, Any]) -> Dict[str, Any]:
    auth = base64.b64encode(f"{creds['client_id']}:{creds['client_secret']}".encode()).decode()
    resp = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": creds["refresh_token"]},
        timeout=30,
    )
    resp.raise_for_status()
    new_data = resp.json()
    out = dict(creds)
    out.update(
        {
            "access_token": new_data["access_token"],
            "refresh_token": new_data["refresh_token"],
            "expires_in": new_data.get("expires_in"),
        }
    )
    out = _set_expires_at(out, out.get("expires_in"))
    return out


def _require_auth_fields(creds: Dict[str, Any]) -> Tuple[str, str, str]:
    cid = creds.get("client_id")
    cs = creds.get("client_secret")
    redirect_uri = creds.get("redirect_uri") or creds.get("redirectUri")
    if not (cid and cs and redirect_uri):
        raise Exception("Xero creds must include client_id, client_secret, and redirect_uri")
    return str(cid), str(cs), str(redirect_uri)


def _default_scopes(creds: Dict[str, Any]) -> List[str]:
    scopes = creds.get("scopes") or creds.get("scope")
    if isinstance(scopes, str) and scopes.strip():
        return scopes.strip().split()
    return ["offline_access", "accounting.transactions", "openid", "profile", "email"]


def ensure_tokens(creds: Dict[str, Any]) -> Dict[str, Any]:
    cid, cs, redirect_uri = _require_auth_fields(creds)

    if _token_is_fresh(creds):
        return dict(creds)

    refresh = creds.get("refresh_token")
    refresh_error: Optional[Exception] = None
    if refresh:
        try:
            return refresh_token(creds)
        except Exception as e:
            refresh_error = e
            warn("auth.refresh_failed_will_reauth", error=str(e))

    code = creds.get("auth_code")
    if not code:
        redirect_resp = creds.get("redirect_response_url") or creds.get("redirect_url")
        if redirect_resp:
            code = parse_code_from_redirect(str(redirect_resp))

    if code:
        tokens = exchange_code_for_tokens(client_id=cid, client_secret=cs, code=str(code), redirect_uri=redirect_uri)
        out = dict(creds)
        out.update(tokens)
        out = _set_expires_at(out, out.get("expires_in"))
        return out

    if refresh_error and isinstance(refresh_error, requests.exceptions.HTTPError):
        try:
            resp = refresh_error.response
            if resp is not None:
                warn("auth.refresh_error_response", status=resp.status_code, body=resp.text[:500])
        except Exception:
            pass

    scopes = _default_scopes(creds)
    url = build_auth_url(client_id=cid, redirect_uri=redirect_uri, scopes=scopes, state=creds.get("state"))
    raise InteractiveAuthRequired(url, scopes)


# ----------------------------
# SESSION (DISABLE urllib3 RETRY SLEEPS)
# ----------------------------

def _requests_session_no_retries() -> requests.Session:
    """
    We deliberately disable urllib3 Retry behaviour here.

    Why:
      - urllib3 can sleep internally on Retry-After (429) which looks like a hang and
        bypasses our own retry/observability.
      - request_json_with_429_retry already implements pacing/backoff + emits events.
    """
    s = requests.Session()

    # total=0 disables retries; respect_retry_after_header=False prevents hidden sleeps.
    retry = Retry(
        total=0,
        connect=0,
        read=0,
        redirect=0,
        status=0,
        respect_retry_after_header=False,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ----------------------------
# CURSOR WITH RESET SUPPORT
# ----------------------------

def _cursor_dt(state: Dict[str, Any]) -> Optional[datetime]:
    """
    Extract cursor datetime from state, checking multiple locations for backward compatibility.
    """
    streams = state.get("streams") if isinstance(state.get("streams"), dict) else {}
    x = streams.get("xero") if isinstance(streams.get("xero"), dict) else {}
    s = x.get("if_modified_since")
    dt = parse_iso(s) if isinstance(s, str) else None
    if dt:
        return dt

    dt = parse_iso(state.get("xero_if_modified_since"))
    if dt:
        return dt

    return parse_iso(state.get("last_success_at")) or parse_iso(state.get("start_date"))


def _safe_advance_cursor(prev: Optional[str], candidate: str, allow_reset: bool = False) -> str:
    """
    Safely advance cursor, with optional reset capability.
    
    Args:
        prev: Previous cursor value
        candidate: New cursor value
        allow_reset: If True, allows cursor to move backward (for full refresh/reset)
    
    Returns:
        The appropriate cursor value
    """
    if not prev or allow_reset:
        return candidate
    try:
        return candidate if is_later(candidate, prev) else prev
    except Exception:
        return prev


def _get_effective_cursor(state: Dict[str, Any]) -> Tuple[Optional[str], bool]:
    """
    Determine the effective cursor and whether this is a reset/full refresh.
    
    Returns:
        (cursor_string, is_reset)
    """
    # Check for explicit reset flag
    force_reset = bool(state.get("xero_force_reset", False))
    
    # Check for explicit start_date override
    start_date = state.get("xero_start_date") or state.get("start_date")
    if start_date:
        start_dt = parse_iso(start_date) if isinstance(start_date, str) else None
        if start_dt:
            info(
                "cursor.override_detected",
                stream="xero",
                start_date=start_date,
                force_reset=force_reset
            )
            return if_modified_since_header(start_dt), True
    
    # Check for full_refresh flag (common pattern)
    if state.get("full_refresh", False):
        force_reset = True
        info("cursor.full_refresh_requested", stream="xero")
    
    # Use existing cursor if not resetting
    prev_cursor_dt = _cursor_dt(state)
    if prev_cursor_dt and not force_reset:
        return None, False
    
    # Default to Jan 1, 2020 for full refresh/reset
    if force_reset or not prev_cursor_dt:
        default_start = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        info(
            "cursor.reset_to_default",
            stream="xero",
            default_start=default_start.isoformat(),
            force_reset=force_reset
        )
        return if_modified_since_header(default_start), True
    
    return None, False


# ----------------------------
# SHAPE STABILIZATION (FIXES DRIFTED CHILD TABLE MERGE FAILURES)
# ----------------------------

def _drop_keys_deep(obj: Any, keys: set[str]) -> Any:
    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in obj.items():
            if k in keys:
                continue
            out[k] = _drop_keys_deep(v, keys)
        return out
    if isinstance(obj, list):
        return [_drop_keys_deep(v, keys) for v in obj]
    return obj


def _stabilize_row(row: Dict[str, Any], *, state: Dict[str, Any]) -> Dict[str, Any]:
    include_tracking = bool(state.get("xero_include_tracking", False))
    include_allocations = bool(state.get("xero_include_allocations", False))

    drop: set[str] = set()
    if not include_tracking:
        drop.add("Tracking")
    if not include_allocations:
        drop.add("Allocations")

    if not drop:
        return row

    stabilized = _drop_keys_deep(row, drop)
    return stabilized if isinstance(stabilized, dict) else row


class StreamCounter:
    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}

    def inc(self, stream: str, n: int = 1) -> None:
        self._counts[stream] = self._counts.get(stream, 0) + n

    def all(self) -> Dict[str, int]:
        return dict(self._counts)


def test_connection(creds: Dict[str, Any]) -> str:
    c = ensure_tokens(creds)
    preferred = creds.get("tenant_id") or creds.get("tenantId")
    tenant_id = get_tenant_id(c["access_token"], preferred_tenant_id=str(preferred) if preferred else None)
    return f"Connected to tenant {tenant_id}"


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    state = state or {}

    selected = normalize_selection(selection)
    if "journals" in selected:
        selected.discard("journals")

    lookback_minutes = clamp_int(
        state.get("lookback_minutes"),
        default=DEFAULT_LOOKBACK_MINUTES,
        lo=0,
        hi=MAX_LOOKBACK_MINUTES,
    )
    max_pages = clamp_int(
        state.get("xero_max_pages_per_stream"),
        default=DEFAULT_MAX_PAGES_PER_STREAM,
        lo=1,
        hi=2_000_000,
    )

    new_creds = ensure_tokens(creds)
    preferred_tenant = creds.get("tenant_id") or creds.get("tenantId") or state.get("xero_tenant_id")
    tenant_id = get_tenant_id(
        new_creds["access_token"],
        preferred_tenant_id=str(preferred_tenant) if preferred_tenant else None,
    )

    # IMPORTANT: avoid requests_retry_session() so urllib3 doesn't sleep internally.
    session = _requests_session_no_retries()

    # NEW: Get effective cursor with reset support
    override_cursor, is_reset = _get_effective_cursor(state)
    
    if override_cursor:
        if_modified_since = override_cursor
    else:
        prev_cursor_dt = _cursor_dt(state)
        if prev_cursor_dt:
            effective_dt = lookback_dt(prev_cursor_dt, lookback_minutes)
            if_modified_since = if_modified_since_header(effective_dt)
        else:
            if_modified_since = None

    headers = xero_headers(new_creds["access_token"], tenant_id, if_modified_since=if_modified_since)

    info(
        "sync.start",
        stream="xero",
        tenant_id=tenant_id,
        if_modified_since=if_modified_since,
        is_reset=is_reset,
        lookback_minutes=lookback_minutes if not is_reset else 0,
        max_pages_per_stream=max_pages,
        selected_streams=sorted(list(selected)) if selected else ["* (excluding journals)"],
    )

    counter = StreamCounter()

    def get_once(path: str, *, stream: str) -> Dict[str, Any]:
        url = f"{ACCOUNTING_BASE}/{path}"
        return (
            request_json_with_429_retry(session, "GET", url, headers=headers, state=state, stream=stream, op="get_once")
            or {}
        )

    def get_paged(path: str, *, stream: str, collection_key: str) -> Iterable[Dict[str, Any]]:
        url = f"{ACCOUNTING_BASE}/{path}"

        def fetch(page: int) -> Dict[str, Any]:
            return (
                request_json_with_429_retry(
                    session,
                    "GET",
                    url,
                    headers=headers,
                    state=state,
                    params={"page": page},
                    stream=stream,
                    op="get_paged",
                )
                or {}
            )

        return paged_collection(fetch, collection_key=collection_key, stream=stream, max_pages=max_pages)

    want_org = is_selected(selected, "organisation")
    want_users = is_selected(selected, "users")
    want_currencies = is_selected(selected, "currencies")
    want_tax_rates = is_selected(selected, "tax_rates")
    want_tracking = is_selected(selected, "tracking_categories")
    want_accounts = is_selected(selected, "accounts")
    want_contacts = is_selected(selected, "contacts")
    want_items = is_selected(selected, "items")
    want_invoices = is_selected(selected, "invoices")
    want_credit_notes = is_selected(selected, "credit_notes")
    want_payments = is_selected(selected, "payments")
    want_bank_tx = is_selected(selected, "bank_transactions")
    want_manual_journals = is_selected(selected, "manual_journals")
    want_purchase_orders = is_selected(selected, "purchase_orders")
    want_prepayments = is_selected(selected, "prepayments")
    want_overpayments = is_selected(selected, "overpayments")

    if not any(
        [
            want_org,
            want_users,
            want_currencies,
            want_tax_rates,
            want_tracking,
            want_accounts,
            want_contacts,
            want_items,
            want_invoices,
            want_credit_notes,
            want_payments,
            want_bank_tx,
            want_manual_journals,
            want_purchase_orders,
            want_prepayments,
            want_overpayments,
        ]
    ):
        warn("sync.no_streams_selected", stream="xero")
        return "No streams selected", new_creds, {}

    def _yield_rows(payload: Dict[str, Any], key: str, stream: str) -> Iterable[Dict[str, Any]]:
        for row in payload.get(key) or []:
            if isinstance(row, dict):
                row = _stabilize_row(row, state=state)
                counter.inc(stream)
                yield add_metadata(row, "xero")

    @dlt.resource(
        write_disposition="replace",
        primary_key="OrganisationID",
        table_name="organisation",
        columns={"Addresses": {"data_type": "text"}}
    )
    def organisation():
        if not want_org:
            return
        payload = get_once("Organisation", stream="organisation")

        # Process the organization data and flatten nested structures to prevent
        # nested table creation conflicts
        for row in _yield_rows(payload, "Organisations", "organisation"):
            if isinstance(row, dict):
                # Flatten nested addresses to prevent organisation__addresses table creation
                if "Addresses" in row:
                    # Convert nested addresses to a JSON string to prevent child table creation
                    addresses = row["Addresses"]
                    if isinstance(addresses, list) and len(addresses) > 0:
                        # Store as JSON string to preserve data but prevent nested table
                        row["Addresses"] = json.dumps(addresses)
                    else:
                        # No addresses - set to empty JSON array string
                        row["Addresses"] = "[]"
                else:
                    # No addresses field - set to empty JSON array string
                    row["Addresses"] = "[]"

                if "Phones" in row and not row["Phones"]:
                    row["Phones"] = []
            yield row

    @dlt.resource(write_disposition="merge", primary_key="UserID", table_name="users")
    def users():
        if not want_users:
            return
        payload = get_once("Users", stream="users")
        yield from _yield_rows(payload, "Users", "users")

    @dlt.resource(write_disposition="merge", primary_key="Code", table_name="currencies")
    def currencies():
        if not want_currencies:
            return
        payload = get_once("Currencies", stream="currencies")
        yield from _yield_rows(payload, "Currencies", "currencies")

    @dlt.resource(write_disposition="merge", primary_key="TaxType", table_name="tax_rates")
    def tax_rates():
        if not want_tax_rates:
            return
        payload = get_once("TaxRates", stream="tax_rates")
        yield from _yield_rows(payload, "TaxRates", "tax_rates")

    @dlt.resource(write_disposition="merge", primary_key="TrackingCategoryID", table_name="tracking_categories")
    def tracking_categories():
        if not want_tracking:
            return
        payload = get_once("TrackingCategories", stream="tracking_categories")
        yield from _yield_rows(payload, "TrackingCategories", "tracking_categories")

    @dlt.resource(write_disposition="merge", primary_key="AccountID", table_name="accounts")
    def accounts():
        if not want_accounts:
            return
        payload = get_once("Accounts", stream="accounts")
        yield from _yield_rows(payload, "Accounts", "accounts")

    @dlt.resource(write_disposition="merge", primary_key="ContactID", table_name="contacts")
    def contacts():
        if not want_contacts:
            return
        try:
            for row in get_paged("Contacts", stream="contacts", collection_key="Contacts"):
                if isinstance(row, dict):
                    # Ensure nested structures are handled consistently to prevent
                    # schema evolution conflicts with child tables
                    if "ContactPersons" in row and not row["ContactPersons"]:
                        row["ContactPersons"] = []
                    if "Addresses" in row and not row["Addresses"]:
                        row["Addresses"] = []
                    if "Phones" in row and not row["Phones"]:
                        row["Phones"] = []
                    row = _stabilize_row(row, state=state)
                    counter.inc("contacts")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="contacts", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="ItemID", table_name="items")
    def items():
        if not want_items:
            return
        try:
            for row in get_paged("Items", stream="items", collection_key="Items"):
                if isinstance(row, dict):
                    row = _stabilize_row(row, state=state)
                    counter.inc("items")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="items", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="InvoiceID", table_name="invoices")
    def invoices():
        if not want_invoices:
            return
        try:
            for row in get_paged("Invoices", stream="invoices", collection_key="Invoices"):
                if isinstance(row, dict):
                    # Ensure nested structures are handled consistently to prevent
                    # schema evolution conflicts with child tables
                    if "Overpayments" in row and not row["Overpayments"]:
                        # Explicitly set to empty list to maintain schema consistency
                        row["Overpayments"] = []
                    row = _stabilize_row(row, state=state)
                    counter.inc("invoices")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="invoices", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="CreditNoteID", table_name="credit_notes")
    def credit_notes():
        if not want_credit_notes:
            return
        try:
            for row in get_paged("CreditNotes", stream="credit_notes", collection_key="CreditNotes"):
                if isinstance(row, dict):
                    # Ensure nested structures are handled consistently to prevent
                    # schema evolution conflicts with child tables
                    if "Allocations" in row and not row["Allocations"]:
                        row["Allocations"] = []
                    row = _stabilize_row(row, state=state)
                    counter.inc("credit_notes")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="credit_notes", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="PaymentID", table_name="payments")
    def payments():
        if not want_payments:
            return
        try:
            for row in get_paged("Payments", stream="payments", collection_key="Payments"):
                if isinstance(row, dict):
                    # Ensure nested structures are handled consistently to prevent
                    # schema evolution conflicts with child tables
                    if "Allocations" in row and not row["Allocations"]:
                        row["Allocations"] = []
                    row = _stabilize_row(row, state=state)
                    counter.inc("payments")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="payments", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="BankTransactionID", table_name="bank_transactions")
    def bank_transactions():
        if not want_bank_tx:
            return
        try:
            for row in get_paged("BankTransactions", stream="bank_transactions", collection_key="BankTransactions"):
                if isinstance(row, dict):
                    row = _stabilize_row(row, state=state)
                    counter.inc("bank_transactions")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="bank_transactions", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="ManualJournalID", table_name="manual_journals")
    def manual_journals():
        if not want_manual_journals:
            return
        try:
            for row in get_paged("ManualJournals", stream="manual_journals", collection_key="ManualJournals"):
                if isinstance(row, dict):
                    row = _stabilize_row(row, state=state)
                    counter.inc("manual_journals")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="manual_journals", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="PurchaseOrderID", table_name="purchase_orders")
    def purchase_orders():
        if not want_purchase_orders:
            return
        try:
            for row in get_paged("PurchaseOrders", stream="purchase_orders", collection_key="PurchaseOrders"):
                if isinstance(row, dict):
                    row = _stabilize_row(row, state=state)
                    counter.inc("purchase_orders")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="purchase_orders", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="PrepaymentID", table_name="prepayments")
    def prepayments():
        if not want_prepayments:
            return
        try:
            for row in get_paged("Prepayments", stream="prepayments", collection_key="Prepayments"):
                if isinstance(row, dict):
                    # Ensure nested structures are handled consistently to prevent
                    # schema evolution conflicts with child tables
                    if "Allocations" in row and not row["Allocations"]:
                        row["Allocations"] = []
                    row = _stabilize_row(row, state=state)
                    counter.inc("prepayments")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="prepayments", error=str(e))

    @dlt.resource(write_disposition="merge", primary_key="OverpaymentID", table_name="overpayments")
    def overpayments():
        if not want_overpayments:
            return
        try:
            for row in get_paged("Overpayments", stream="overpayments", collection_key="Overpayments"):
                if isinstance(row, dict):
                    # Ensure nested structures are handled consistently to prevent
                    # schema evolution conflicts with child tables
                    if "Allocations" in row and not row["Allocations"]:
                        row["Allocations"] = []
                    row = _stabilize_row(row, state=state)
                    counter.inc("overpayments")
                    yield add_metadata(row, "xero")
        except XeroAuthError as e:
            warn("stream.skip_auth", stream="overpayments", error=str(e))

    resources: List[Any] = []
    if want_org:
        resources.append(organisation())
    if want_users:
        resources.append(users())
    if want_currencies:
        resources.append(currencies())
    if want_tax_rates:
        resources.append(tax_rates())
    if want_tracking:
        resources.append(tracking_categories())
    if want_accounts:
        resources.append(accounts())
    if want_contacts:
        resources.append(contacts())
    if want_items:
        resources.append(items())
    if want_invoices:
        resources.append(invoices())
    if want_credit_notes:
        resources.append(credit_notes())
    if want_payments:
        resources.append(payments())
    if want_bank_tx:
        resources.append(bank_transactions())
    if want_manual_journals:
        resources.append(manual_journals())
    if want_purchase_orders:
        resources.append(purchase_orders())
    if want_prepayments:
        resources.append(prepayments())
    if want_overpayments:
        resources.append(overpayments())

    # distinct pipeline name per schema to avoid state/schema conflicts between different xero connections
    pipeline_name = f"xero_{schema}"
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="postgres", dataset_name=schema)
    info("pipeline.run.start", stream="xero", destination="postgres", dataset=schema, pipeline_name=pipeline_name)

    # Run with evolve schema contract to handle structural changes gracefully
    # Use schema updates to handle nested table schema conflicts
    try:
        load_info = pipeline.run(resources, schema_contract="evolve")
    except Exception as e:
        # Handle schema evolution conflicts that cause table existence issues
        error_msg = str(e).lower()
        error_type = type(e).__name__.lower()

        # Check for undefined table errors (both checking error message and type)
        is_undefined_table_error = (
            ("undefinedtable" in error_msg or "does not exist" in error_msg or "undefined relation" in error_msg)
            and ("__" in error_msg or "organisation__addresses" in error_msg or "nested" in error_msg)
        ) or "undefined" in error_type

        if is_undefined_table_error:
            # This catches any nested table issue (e.g., invoices__overpayments, organisation__addresses, etc.)
            info("pipeline.schema_conflict_detected", stream="xero", error=str(e))
            # Reset pipeline schema to handle the orphaned table reference
            # Create a new pipeline instance with a fresh schema state
            import shutil
            import os
            from dlt.common import logger
            # pipeline_name is already defined above as f"xero_{schema}"
            pipeline_dir = os.path.expanduser(f"~/.dlt/pipelines/{pipeline_name}")
            if os.path.exists(pipeline_dir):
                try:
                    shutil.rmtree(pipeline_dir)
                    logger.info(f"Reset pipeline state for {pipeline_name}")
                except Exception as reset_error:
                    logger.warning(f"Could not reset pipeline state: {reset_error}")

            # Create a fresh pipeline and run again
            fresh_pipeline = dlt.pipeline(pipeline_name=pipeline_name, destination="postgres", dataset_name=schema)
            load_info = fresh_pipeline.run(resources, schema_contract="evolve")
        else:
            raise
    info("pipeline.run.done", stream="xero", counts=counter.all())

    now = utc_now_iso()
    prev_cursor = None
    if isinstance(state.get("xero_if_modified_since"), str):
        prev_cursor = state["xero_if_modified_since"]
    else:
        streams_state = state.get("streams") if isinstance(state.get("streams"), dict) else {}
        x_state = streams_state.get("xero") if isinstance(streams_state.get("xero"), dict) else {}
        if isinstance(x_state.get("if_modified_since"), str):
            prev_cursor = x_state["if_modified_since"]

    # NEW: Use allow_reset parameter when appropriate
    cursor_out = _safe_advance_cursor(prev_cursor, now, allow_reset=is_reset)

    state_updates: Dict[str, Any] = {
        "xero_if_modified_since": cursor_out,
        "xero_tenant_id": tenant_id
    }
    
    # Remove reset flags after first run
    if state.get("xero_force_reset"):
        state_updates["xero_force_reset"] = False
    if state.get("full_refresh"):
        state_updates["full_refresh"] = False
    
    state_updates.setdefault("streams", {})
    if isinstance(state_updates["streams"], dict):
        state_updates["streams"].setdefault("xero", {})
        if isinstance(state_updates["streams"]["xero"], dict):
            state_updates["streams"]["xero"]["if_modified_since"] = cursor_out
            state_updates["streams"]["xero"]["tenant_id"] = tenant_id
            state_updates["streams"]["xero"]["counts"] = counter.all()

    sync_type = "FULL REFRESH" if is_reset else "INCREMENTAL"
    report = (
        f"Xero sync completed ({sync_type}).\n"
        f"- Tenant: {tenant_id}\n"
        f"- If-Modified-Since (effective): {if_modified_since or 'none (full)'}\n"
        f"- Cursor written: {cursor_out}\n"
        f"- Streams selected: {sorted(list(selected)) if selected else 'ALL (excluding journals)'}\n"
        f"- Counts: {counter.all()}\n"
        f"{load_info}"
    )

    return report, new_creds, state_updates