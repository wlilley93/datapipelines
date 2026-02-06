from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import dlt
from connectors.runtime.protocol import ReadSelection

from .constants import (
    DEFAULT_BASE_URL,
    DEFAULT_MAX_PAGES_PER_STREAM,
    DEFAULT_PAGE_SIZE,
    DEFAULT_STREAMS,
    STATE_GLOBAL_CURSOR_FALLBACK,
    STATE_START_DATE,
)
from .events import info, records, warn
from .http import auth_headers, base_url, request_json_with_429_retry
from .paging import paged_get
from .selection import is_selected, normalize_selection
from .time_utils import is_later, parse_iso, utc_now_iso
from .utils_bridge import add_metadata, requests_retry_session


def _get_stream_cursor(state: Dict[str, Any], stream: str) -> Optional[str]:
    streams = state.get("streams")
    if isinstance(streams, dict):
        s = streams.get(stream)
        if isinstance(s, dict) and isinstance(s.get("cursor"), str):
            return s["cursor"]
    return None


def _set_stream_cursor(state_updates: Dict[str, Any], stream: str, cursor: str) -> None:
    state_updates.setdefault("streams", {})
    if isinstance(state_updates["streams"], dict):
        state_updates["streams"].setdefault(stream, {})
        if isinstance(state_updates["streams"][stream], dict):
            state_updates["streams"][stream]["cursor"] = cursor


def _cursor_fallback(state: Dict[str, Any]) -> Optional[str]:
    for key in (STATE_GLOBAL_CURSOR_FALLBACK, STATE_START_DATE):
        v = state.get(key)
        if isinstance(v, str) and v.strip():
            return v
    return None


def _safe_advance(prev: Optional[str], candidate: str) -> str:
    if not prev:
        return candidate
    try:
        return candidate if is_later(candidate, prev) else prev
    except Exception:
        return prev


def _coerce_contact_id(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("id", "_id"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    for k in ("wid", "waId", "jid", "jId", "whatsappId", "whatsapp_id"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    for k in ("phone", "number", "msisdn"):
        v = obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    for key in ("contact", "user", "profile"):
        v = obj.get(key)
        if isinstance(v, dict):
            cid = _coerce_contact_id(v)
            if cid:
                return cid

    return None


def _normalize_contact_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cid = _coerce_contact_id(row)
    if not cid:
        for k in ("chatId", "chat_id", "contactId", "contact_id"):
            v = row.get(k)
            if isinstance(v, str) and v.strip():
                cid = v.strip()
                break

    if not cid:
        return None

    out = dict(row)
    out["id"] = str(cid)
    return out


def test_connection(creds: Dict[str, Any]) -> str:
    session = requests_retry_session()
    h = auth_headers(creds)
    base = base_url(creds, {}, default=DEFAULT_BASE_URL)

    test_path = creds.get("test_path") or "/messages"
    url = f"{base}{str(test_path)}"
    request_json_with_429_retry(
        session,
        "GET",
        url,
        headers=h,
        state={},
        params={"page": 1, "limit": 1},
        stream="wassenger",
        op="test_connection",
    )
    return f"Wassenger Connected ({base})"


def _is_stream_endpoint_accessible(
    session: Any,
    headers: Dict[str, str],
    base: str,
    streams: Dict[str, Any],
    stream_name: str,
    state: Dict[str, Any],
    creds: Dict[str, Any],
) -> bool:
    """
    Check if a stream's endpoint is accessible by making a test request.
    Returns True if the endpoint is accessible, False otherwise.
    """
    from .constants import STREAM_ENDPOINT_CANDIDATES
    from .http import request_json_with_429_retry
    from .time_utils import parse_iso
    import re

    # Get stream configuration
    stream_cfg = streams.get(stream_name, {})

    # If there are specific endpoint candidates for this stream, try them
    candidates = STREAM_ENDPOINT_CANDIDATES.get(stream_name, [])

    # If no candidates are defined, use the default path from config
    if not candidates:
        path = str(stream_cfg.get("path") or f"/{stream_name.rstrip('s')}").strip()
        if not path.startswith("/"):
            path = "/" + path
        candidates = [path]

    # Get device ID if needed for template substitution
    device_id = state.get("wassenger_device_id")
    if not device_id:
        # Try to get the device ID from existing state or defaults
        device_id = state.get("wassenger_device_id") or creds.get("wassenger_device_id", "default")

    # Get chat ID if needed (for chat-related endpoints)
    # This is more complex, so for now we'll try a placeholder
    chat_wid = "test_chat"
    webhook_id = "test_webhook"
    campaign_id = "test_campaign"

    # Try each candidate endpoint
    for candidate_path in candidates:
        # Replace template placeholders with actual values
        try:
            path = candidate_path.format(
                deviceId=device_id,
                chatWid=chat_wid,
                webhookId=webhook_id,
                campaignId=campaign_id
            )
        except KeyError:
            # If there are template parameters but we don't have values for them,
            # try to use some default values
            path = candidate_path.replace("{deviceId}", device_id) \
                                .replace("{chatWid}", chat_wid) \
                                .replace("{webhookId}", webhook_id) \
                                .replace("{campaignId}", campaign_id)

        if not path.startswith("/"):
            path = "/" + path

        # Try a minimal request with small page size to test accessibility
        try:
            url = f"{base}{path}"
            # Try the endpoint with minimal parameters to check if it exists
            response = request_json_with_429_retry(
                session,
                "GET",
                url,
                headers=headers,
                state=state,
                params={"page": 1, "limit": 1},  # Minimal request
                stream=stream_name,
                op="endpoint_test",
            )
            # If we get a response without error, endpoint is accessible
            # Even if it returns empty data, that's still accessible
            return True
        except Exception as e:
            # Check if it's specifically a 404 error (endpoint doesn't exist)
            # We don't want to catch other errors like rate limits, network issues, etc.
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                # This candidate endpoint doesn't exist, try the next one
                continue
            elif hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 404:
                # This candidate endpoint doesn't exist, try the next one
                continue
            else:
                # Some other error occurred - the endpoint might exist but have other issues
                # We'll consider it accessible since it's not a 404
                return True

    # If none of the candidate endpoints worked, return False
    return False


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    state = state or {}
    selected = normalize_selection(selection)

    streams_cfg = state.get("wassenger_streams")
    streams = streams_cfg if isinstance(streams_cfg, dict) and streams_cfg else DEFAULT_STREAMS

    want_streams = {name: is_selected(selected, name) for name in streams.keys()}
    if not any(want_streams.values()):
        warn("sync.no_streams_selected", stream="wassenger")
        return "No streams selected", None, {}

    session = requests_retry_session()
    h = auth_headers(creds)
    base = base_url(creds, state, default=DEFAULT_BASE_URL)

    page_size = int(state.get("wassenger_page_size", DEFAULT_PAGE_SIZE))
    max_pages = int(state.get("wassenger_max_pages_per_stream", DEFAULT_MAX_PAGES_PER_STREAM))

    fallback_cursor = _cursor_fallback(state)
    run_started_at = utc_now_iso()

    # Filter selected streams to only include those with accessible endpoints
    selected_stream_names = []
    for stream_name, is_wanted in want_streams.items():
        if is_wanted:
            # Test if the endpoint for this stream is accessible
            if _is_stream_endpoint_accessible(session, h, base, streams, stream_name, state, creds):
                selected_stream_names.append(stream_name)
            else:
                warn(f"sync.stream_not_accessible", stream=stream_name, base=base)

    info(
        "sync.start",
        stream="wassenger",
        base=base,
        selected_streams=selected_stream_names,
        page_size=page_size,
        max_pages_per_stream=max_pages,
        run_started_at=run_started_at,
    )

    # Keep counts stable and explicit (including zero-count streams).
    counts: Dict[str, int] = {name: 0 for name in selected_stream_names}
    state_updates: Dict[str, Any] = {}
    diag: Dict[str, Any] = {"dropped": {"contacts_missing_id": 0}}

    def fetch_page(path: str, *, stream: str, since: Optional[str], page: int, limit: int) -> Any:
        params: Dict[str, Any] = {"page": page, "limit": limit}
        since_param_name = str(state.get("wassenger_since_param", "since"))
        if since:
            params[since_param_name] = since

        url = f"{base}{path}"
        return request_json_with_429_retry(
            session,
            "GET",
            url,
            headers=h,
            state=state,
            params=params,
            stream=stream,
            op="list",
        )

    # -------------------------------------------------------------------------------------
    # messages (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    messages_cfg = streams.get("messages", {})
    messages_path = str(messages_cfg.get("path") or "/messages").strip()
    if not messages_path.startswith("/"):
        messages_path = "/" + messages_path

    messages_collection_key = messages_cfg.get("collection_key")
    messages_collection_key = messages_collection_key if isinstance(messages_collection_key, str) else None
    messages_cursor_field = str(messages_cfg.get("cursor_field") or "createdAt")

    prev_messages_cursor = _get_stream_cursor(state, "messages") or fallback_cursor
    max_seen_messages: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="messages")
    def messages() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_messages
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(messages_path, stream="messages", since=prev_messages_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="messages",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=messages_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["messages"] = n
            if n % 100 == 0:
                records("messages", n, message="progress")

            candidate = row.get(messages_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_messages = _safe_advance(max_seen_messages, cand_iso)

            yield add_metadata(row, "wassenger")

        records("messages", n, message="done")

    # -------------------------------------------------------------------------------------
    # contacts (TRANSFORMER FUNCTION) — derived from messages
    # -------------------------------------------------------------------------------------
    @dlt.transformer(
        data_from=messages,
        write_disposition="merge",
        primary_key="id",
        table_name="contacts",
        columns={
            "business_info__business_hours": {"data_type": "text", "nullable": True},
        },
    )
    def contacts(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        def emit_contact(obj: Any) -> Iterable[Dict[str, Any]]:
            if not isinstance(obj, dict):
                return
            normalized = _normalize_contact_row(obj)
            if not normalized:
                # Try to create a basic contact from phone number or other identifier if no standard contact fields exist
                # This handles cases where Wassenger message format doesn't have complete contact objects
                contact_id = None
                for key in ("phone", "number", "msisdn", "waId", "jid", "id"):
                    v = obj.get(key)
                    if isinstance(v, str) and v.strip():
                        contact_id = v.strip()
                        break

                if contact_id:
                    normalized = {
                        "id": contact_id,
                        "phone": obj.get("phone") or obj.get("number") or obj.get("msisdn"),
                        "waId": obj.get("waId") or obj.get("jid"),
                        "name": obj.get("name") or obj.get("displayName") or obj.get("display_name")
                    }
                    # Clean up empty values
                    normalized = {k: v for k, v in normalized.items() if v}
                else:
                    diag["dropped"]["contacts_missing_id"] += 1
                    return

            normalized.setdefault("createdAt", msg.get("createdAt"))
            normalized.setdefault("updatedAt", msg.get("updatedAt") or msg.get("createdAt"))

            # Count + emit progress for derived stream as well.
            counts["contacts"] = int(counts.get("contacts", 0)) + 1
            if counts["contacts"] % 250 == 0:
                records("contacts", counts["contacts"], message="progress")

            yield add_metadata(normalized, "wassenger")

        # Check standard contact fields
        for key in ("from", "to", "sender", "recipient", "contact", "user", "profile"):
            yield from emit_contact(msg.get(key)) or ()

        # Also check for contacts in nested objects
        chat = msg.get("chat") or msg.get("conversation") or {}
        if isinstance(chat, dict):
            yield from emit_contact(chat.get("contact") or chat.get("user")) or ()

        # Check for contact in other common nested locations
        for nested_key in ("from", "to"):
            nested_obj = msg.get(nested_key)
            if isinstance(nested_obj, dict):
                # Check for additional nested contact information
                nested_contact = nested_obj.get("contact") or nested_obj.get("profile") or nested_obj.get("user")
                if nested_contact:
                    yield from emit_contact(nested_contact) or ()

    # -------------------------------------------------------------------------------------
    # conversations (TRANSFORMER FUNCTION) — derived from messages
    # -------------------------------------------------------------------------------------
    @dlt.transformer(
        data_from=messages,
        write_disposition="merge",
        primary_key="id",
        table_name="conversations",
        columns={
            "last_auto_reply": {"data_type": "text", "nullable": True},
            "last_auto_reply_at": {"data_type": "timestamp", "nullable": True},
            "owner__previous_department": {"data_type": "text", "nullable": True},
            "owner__auto_assign_department": {"data_type": "text", "nullable": True},
            "owner__auto_assign_department_at": {"data_type": "timestamp", "nullable": True},
            "group__community": {"data_type": "text", "nullable": True},
        },
    )
    def conversations(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        chat_id: Optional[str] = None

        # First, try the standard keys we've already been looking for
        for key in ("chatId", "chat_id", "conversationId", "conversation_id", "threadId", "thread_id"):
            v = msg.get(key)
            if isinstance(v, str) and v.strip():
                chat_id = v.strip()
                break

        # If not found, try additional common field names in Wassenger API
        if not chat_id:
            # Check for chatId or conversationId in the 'from' or 'to' objects
            for contact_field in ("from", "to", "sender", "recipient", "contact", "user"):
                contact_obj = msg.get(contact_field)
                if isinstance(contact_obj, dict):
                    for key in ("chatId", "chat_id", "conversationId", "conversation_id"):
                        v = contact_obj.get(key)
                        if isinstance(v, str) and v.strip():
                            chat_id = v.strip()
                            break
                    if chat_id:
                        break

        # If still not found, check for contact-based chat identification
        if not chat_id:
            from_contact = msg.get("from") or msg.get("sender")
            to_contact = msg.get("to") or msg.get("recipient")

            # Try to generate a unique chat ID based on contact IDs if available
            from_id = None
            if isinstance(from_contact, dict):
                for key in ("id", "_id", "wid", "waId", "jid", "jId", "whatsappId", "whatsapp_id", "phone", "number", "msisdn"):
                    v = from_contact.get(key)
                    if isinstance(v, str) and v.strip():
                        from_id = v.strip()
                        break

            to_id = None
            if isinstance(to_contact, dict):
                for key in ("id", "_id", "wid", "waId", "jid", "jId", "whatsappId", "whatsapp_id", "phone", "number", "msisdn"):
                    v = to_contact.get(key)
                    if isinstance(v, str) and v.strip():
                        to_id = v.strip()
                        break

            # Create a conversation ID by combining contact IDs (in sorted order to ensure consistency)
            if from_id and to_id:
                # Sort alphabetically to ensure consistency (A-B chat is same as B-A)
                sorted_contacts = sorted([from_id, to_id])
                chat_id = f"chat_{sorted_contacts[0]}_{sorted_contacts[1]}"
            elif from_id:
                # Use sender's ID as conversation ID if only sender is known
                chat_id = f"chat_{from_id}"
            elif to_id:
                # Use recipient's ID as conversation ID if only recipient is known
                chat_id = f"chat_{to_id}"

        # If still not found, check in embedded chat object with more field names
        if not chat_id:
            chat = msg.get("chat") or msg.get("conversation") or msg.get("thread") or {}
            if isinstance(chat, dict):
                for k in ("id", "_id", "chatId", "conversationId", "threadId", "chat_id", "conversation_id", "thread_id"):
                    v = chat.get(k)
                    if isinstance(v, str) and v.strip():
                        chat_id = v.strip()
                        break

        # If still not found, try to extract from 'chatId' in root message or related objects
        if not chat_id:
            for key in ("contactId", "contact_id", "profileId", "profile_id", "waId", "wid"):
                v = msg.get(key)
                if isinstance(v, str) and v.strip():
                    chat_id = f"chat_{v.strip()}"
                    break

        # If we still don't have a valid chat_id, try to create the most basic conversation possible
        if not chat_id:
            # Use the message's phone numbers or IDs to create a conversation key
            # This handles cases where Wassenger API doesn't provide explicit conversation IDs
            phone_numbers = []

            # Check for phone numbers in common locations
            for field_name in ("from", "to", "sender", "recipient"):
                contact = msg.get(field_name)
                if isinstance(contact, dict):
                    for phone_field in ("phone", "number", "msisdn", "waId", "jid"):
                        phone_val = contact.get(phone_field)
                        if isinstance(phone_val, str) and phone_val.strip():
                            # Normalize phone numbers (remove spaces, special characters)
                            normalized_phone = ''.join(c for c in phone_val.strip() if c.isdigit() or c == '+')
                            if normalized_phone:
                                phone_numbers.append(normalized_phone)

            # Check for direct phone numbers in the top-level message
            for phone_field in ("phone", "number", "msisdn", "from_phone", "to_number"):
                phone_val = msg.get(phone_field)
                if isinstance(phone_val, str) and phone_val.strip():
                    normalized_phone = ''.join(c for c in phone_val.strip() if c.isdigit() or c == '+')
                    if normalized_phone:
                        phone_numbers.append(normalized_phone)

            if phone_numbers:
                # Create conversation ID based on phone numbers (sorted for consistency)
                unique_numbers = sorted(set(phone_numbers))  # Remove duplicates and sort
                chat_id = f"conv_{'_'.join(unique_numbers[:2])}"  # Use first two unique numbers

        if not chat_id:
            # If we still can't determine a conversation ID, return without creating a conversation
            # This maintains the original behavior for cases where conversation ID cannot be determined
            return

        row = {
            "id": chat_id,
            "createdAt": msg.get("createdAt"),
            "updatedAt": msg.get("updatedAt") or msg.get("createdAt"),
            "lastMessageId": msg.get("id"),
            "lastMessageAt": msg.get("createdAt"),
        }

        counts["conversations"] = int(counts.get("conversations", 0)) + 1
        if counts["conversations"] % 250 == 0:
            records("conversations", counts["conversations"], message="progress")

        yield add_metadata(row, "wassenger")

    # -------------------------------------------------------------------------------------
    # devices (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    devices_cfg = streams.get("devices", {})
    devices_path = str(devices_cfg.get("path") or "/devices").strip()
    if not devices_path.startswith("/"):
        devices_path = "/" + devices_path

    dev_collection_key = devices_cfg.get("collection_key")
    dev_collection_key = dev_collection_key if isinstance(dev_collection_key, str) else None
    dev_cursor_field = str(devices_cfg.get("cursor_field") or "updatedAt")

    prev_dev_cursor = _get_stream_cursor(state, "devices") or fallback_cursor
    max_seen_devices: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="devices")
    def devices() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_devices
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(devices_path, stream="devices", since=prev_dev_cursor, page=p, limit=lim)

        # Special handling for devices - it might return a single device object directly
        # instead of an array in a collection key
        payload = _fetch(1, page_size)

        # Check if the response is a single device object with an 'id' field
        if isinstance(payload, dict) and payload.get('id'):
            # This looks like a single device response, not an array
            row = payload
            if isinstance(row, dict):
                n = 1
                counts["devices"] = n
                candidate = row.get(dev_cursor_field) or row.get("updatedAt") or row.get("createdAt")
                if isinstance(candidate, str) and candidate:
                    dt = parse_iso(candidate)
                    cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                    max_seen_devices = _safe_advance(max_seen_devices, cand_iso)
                yield add_metadata(row, "wassenger")
        else:
            # Handle using the standard paged approach for array responses
            for row in paged_get(
                _fetch,
                stream="devices",
                page_size=page_size,
                max_pages=max_pages,
                collection_key=dev_collection_key,
            ):
                if not isinstance(row, dict):
                    continue

                n += 1
                counts["devices"] = n
                if n % 100 == 0:
                    records("devices", n, message="progress")

                candidate = row.get(dev_cursor_field) or row.get("updatedAt") or row.get("createdAt")
                if isinstance(candidate, str) and candidate:
                    dt = parse_iso(candidate)
                    cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                    max_seen_devices = _safe_advance(max_seen_devices, cand_iso)

                yield add_metadata(row, "wassenger")

        records("devices", n, message="done")

    # -------------------------------------------------------------------------------------
    # team (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    team_cfg = streams.get("team", {})
    team_path = str(team_cfg.get("path") or "/team").strip()
    if not team_path.startswith("/"):
        team_path = "/" + team_path

    team_collection_key = team_cfg.get("collection_key")
    team_collection_key = team_collection_key if isinstance(team_collection_key, str) else None
    team_cursor_field = str(team_cfg.get("cursor_field") or "createdAt")

    prev_team_cursor = _get_stream_cursor(state, "team") or fallback_cursor
    max_seen_team: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="team")
    def team() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_team
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(team_path, stream="team", since=prev_team_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="team",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=team_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["team"] = n
            if n % 100 == 0:
                records("team", n, message="progress")

            candidate = row.get(team_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_team = _safe_advance(max_seen_team, cand_iso)

            yield add_metadata(row, "wassenger")

        records("team", n, message="done")

    # -------------------------------------------------------------------------------------
    # departments (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    dept_cfg = streams.get("departments", {})
    dept_path = str(dept_cfg.get("path") or "/departments").strip()
    if not dept_path.startswith("/"):
        dept_path = "/" + dept_path

    dept_collection_key = dept_cfg.get("collection_key")
    dept_collection_key = dept_collection_key if isinstance(dept_collection_key, str) else None
    dept_cursor_field = str(dept_cfg.get("cursor_field") or "createdAt")

    prev_dept_cursor = _get_stream_cursor(state, "departments") or fallback_cursor
    max_seen_depts: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="departments")
    def departments() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_depts
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(dept_path, stream="departments", since=prev_dept_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="departments",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=dept_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["departments"] = n
            if n % 100 == 0:
                records("departments", n, message="progress")

            candidate = row.get(dept_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_depts = _safe_advance(max_seen_depts, cand_iso)

            yield add_metadata(row, "wassenger")

        records("departments", n, message="done")

    # -------------------------------------------------------------------------------------
    # contacts_direct (RESOURCE FUNCTION) - direct API fetch
    # -------------------------------------------------------------------------------------
    contacts_cfg = streams.get("contacts", {})
    contacts_path = str(contacts_cfg.get("path") or "/contacts").strip()
    if not contacts_path.startswith("/"):
        contacts_path = "/" + contacts_path

    contacts_collection_key = contacts_cfg.get("collection_key")
    contacts_collection_key = contacts_collection_key if isinstance(contacts_collection_key, str) else None
    contacts_cursor_field = str(contacts_cfg.get("cursor_field") or "createdAt")

    prev_contacts_cursor = _get_stream_cursor(state, "contacts") or fallback_cursor
    max_seen_contacts: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="contacts_direct")
    def contacts_direct() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_contacts
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(contacts_path, stream="contacts_direct", since=prev_contacts_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="contacts_direct",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=contacts_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["contacts_direct"] = n
            if n % 100 == 0:
                records("contacts_direct", n, message="progress")

            candidate = row.get(contacts_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_contacts = _safe_advance(max_seen_contacts, cand_iso)

            yield add_metadata(row, "wassenger")

        records("contacts_direct", n, message="done")

    # -------------------------------------------------------------------------------------
    # analytics (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    analytics_cfg = streams.get("analytics", {})
    analytics_path = str(analytics_cfg.get("path") or "/analytics").strip()
    if not analytics_path.startswith("/"):
        analytics_path = "/" + analytics_path

    analytics_collection_key = analytics_cfg.get("collection_key")
    analytics_collection_key = analytics_collection_key if isinstance(analytics_collection_key, str) else None
    analytics_cursor_field = str(analytics_cfg.get("cursor_field") or "date")

    prev_analytics_cursor = _get_stream_cursor(state, "analytics") or fallback_cursor
    max_seen_analytics: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="analytics")
    def analytics() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_analytics
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(analytics_path, stream="analytics", since=prev_analytics_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="analytics",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=analytics_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["analytics"] = n
            if n % 100 == 0:
                records("analytics", n, message="progress")

            candidate = row.get(analytics_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_analytics = _safe_advance(max_seen_analytics, cand_iso)

            yield add_metadata(row, "wassenger")

        records("analytics", n, message="done")

    # -------------------------------------------------------------------------------------
    # campaigns (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    campaigns_cfg = streams.get("campaigns", {})
    campaigns_path = str(campaigns_cfg.get("path") or "/campaigns").strip()
    if not campaigns_path.startswith("/"):
        campaigns_path = "/" + campaigns_path

    campaigns_collection_key = campaigns_cfg.get("collection_key")
    campaigns_collection_key = campaigns_collection_key if isinstance(campaigns_collection_key, str) else None
    campaigns_cursor_field = str(campaigns_cfg.get("cursor_field") or "createdAt")

    prev_campaigns_cursor = _get_stream_cursor(state, "campaigns") or fallback_cursor
    max_seen_campaigns: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="campaigns")
    def campaigns() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_campaigns
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(campaigns_path, stream="campaigns", since=prev_campaigns_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="campaigns",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=campaigns_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["campaigns"] = n
            if n % 100 == 0:
                records("campaigns", n, message="progress")

            candidate = row.get(campaigns_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_campaigns = _safe_advance(max_seen_campaigns, cand_iso)

            yield add_metadata(row, "wassenger")

        records("campaigns", n, message="done")

    # -------------------------------------------------------------------------------------
    # chat_participants (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    chat_participants_cfg = streams.get("chat_participants", {})
    chat_participants_path = str(chat_participants_cfg.get("path") or "/chat/{deviceId}/chats/{chatWid}/participants").strip()
    if not chat_participants_path.startswith("/"):
        chat_participants_path = "/" + chat_participants_path

    chat_participants_collection_key = chat_participants_cfg.get("collection_key")
    chat_participants_collection_key = chat_participants_collection_key if isinstance(chat_participants_collection_key, str) else None
    chat_participants_cursor_field = str(chat_participants_cfg.get("cursor_field") or "createdAt")

    prev_chat_participants_cursor = _get_stream_cursor(state, "chat_participants") or fallback_cursor
    max_seen_chat_participants: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="chat_participants")
    def chat_participants() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_chat_participants
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(chat_participants_path, stream="chat_participants", since=prev_chat_participants_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="chat_participants",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=chat_participants_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["chat_participants"] = n
            if n % 100 == 0:
                records("chat_participants", n, message="progress")

            candidate = row.get(chat_participants_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_chat_participants = _safe_advance(max_seen_chat_participants, cand_iso)

            yield add_metadata(row, "wassenger")

        records("chat_participants", n, message="done")

    # -------------------------------------------------------------------------------------
    # chat_sync (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    chat_sync_cfg = streams.get("chat_sync", {})
    chat_sync_path = str(chat_sync_cfg.get("path") or "/chat/{deviceId}/chats/{chatWid}/sync").strip()
    if not chat_sync_path.startswith("/"):
        chat_sync_path = "/" + chat_sync_path

    chat_sync_collection_key = chat_sync_cfg.get("collection_key")
    chat_sync_collection_key = chat_sync_collection_key if isinstance(chat_sync_collection_key, str) else None
    chat_sync_cursor_field = str(chat_sync_cfg.get("cursor_field") or "updatedAt")

    prev_chat_sync_cursor = _get_stream_cursor(state, "chat_sync") or fallback_cursor
    max_seen_chat_sync: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="chat_sync")
    def chat_sync() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_chat_sync
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(chat_sync_path, stream="chat_sync", since=prev_chat_sync_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="chat_sync",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=chat_sync_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["chat_sync"] = n
            if n % 100 == 0:
                records("chat_sync", n, message="progress")

            candidate = row.get(chat_sync_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_chat_sync = _safe_advance(max_seen_chat_sync, cand_iso)

            yield add_metadata(row, "wassenger")

        records("chat_sync", n, message="done")

    # -------------------------------------------------------------------------------------
    # files (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    files_cfg = streams.get("files", {})
    files_path = str(files_cfg.get("path") or "/files").strip()
    if not files_path.startswith("/"):
        files_path = "/" + files_path

    files_collection_key = files_cfg.get("collection_key")
    files_collection_key = files_collection_key if isinstance(files_collection_key, str) else None
    files_cursor_field = str(files_cfg.get("cursor_field") or "createdAt")

    prev_files_cursor = _get_stream_cursor(state, "files") or fallback_cursor
    max_seen_files: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="files")
    def files() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_files
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(files_path, stream="files", since=prev_files_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="files",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=files_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["files"] = n
            if n % 100 == 0:
                records("files", n, message="progress")

            candidate = row.get(files_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_files = _safe_advance(max_seen_files, cand_iso)

            yield add_metadata(row, "wassenger")

        records("files", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_autoassign (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_autoassign_cfg = streams.get("device_autoassign", {})
    device_autoassign_path = str(device_autoassign_cfg.get("path") or "/devices/{deviceId}/autoassign").strip()
    if not device_autoassign_path.startswith("/"):
        device_autoassign_path = "/" + device_autoassign_path

    device_autoassign_collection_key = device_autoassign_cfg.get("collection_key")
    device_autoassign_collection_key = device_autoassign_collection_key if isinstance(device_autoassign_collection_key, str) else None
    device_autoassign_cursor_field = str(device_autoassign_cfg.get("cursor_field") or "updatedAt")

    prev_device_autoassign_cursor = _get_stream_cursor(state, "device_autoassign") or fallback_cursor
    max_seen_device_autoassign: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_autoassign")
    def device_autoassign() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_autoassign
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_autoassign_path, stream="device_autoassign", since=prev_device_autoassign_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_autoassign",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_autoassign_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_autoassign"] = n
            if n % 100 == 0:
                records("device_autoassign", n, message="progress")

            candidate = row.get(device_autoassign_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_autoassign = _safe_advance(max_seen_device_autoassign, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_autoassign", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_autoreplies (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_autoreplies_cfg = streams.get("device_autoreplies", {})
    device_autoreplies_path = str(device_autoreplies_cfg.get("path") or "/devices/{deviceId}/autoreplies").strip()
    if not device_autoreplies_path.startswith("/"):
        device_autoreplies_path = "/" + device_autoreplies_path

    device_autoreplies_collection_key = device_autoreplies_cfg.get("collection_key")
    device_autoreplies_collection_key = device_autoreplies_collection_key if isinstance(device_autoreplies_collection_key, str) else None
    device_autoreplies_cursor_field = str(device_autoreplies_cfg.get("cursor_field") or "updatedAt")

    prev_device_autoreplies_cursor = _get_stream_cursor(state, "device_autoreplies") or fallback_cursor
    max_seen_device_autoreplies: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_autoreplies")
    def device_autoreplies() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_autoreplies
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_autoreplies_path, stream="device_autoreplies", since=prev_device_autoreplies_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_autoreplies",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_autoreplies_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_autoreplies"] = n
            if n % 100 == 0:
                records("device_autoreplies", n, message="progress")

            candidate = row.get(device_autoreplies_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_autoreplies = _safe_advance(max_seen_device_autoreplies, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_autoreplies", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_catalog (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_catalog_cfg = streams.get("device_catalog", {})
    device_catalog_path = str(device_catalog_cfg.get("path") or "/devices/{deviceId}/catalog").strip()
    if not device_catalog_path.startswith("/"):
        device_catalog_path = "/" + device_catalog_path

    device_catalog_collection_key = device_catalog_cfg.get("collection_key")
    device_catalog_collection_key = device_catalog_collection_key if isinstance(device_catalog_collection_key, str) else None
    device_catalog_cursor_field = str(device_catalog_cfg.get("cursor_field") or "updatedAt")

    prev_device_catalog_cursor = _get_stream_cursor(state, "device_catalog") or fallback_cursor
    max_seen_device_catalog: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_catalog")
    def device_catalog() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_catalog
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_catalog_path, stream="device_catalog", since=prev_device_catalog_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_catalog",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_catalog_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_catalog"] = n
            if n % 100 == 0:
                records("device_catalog", n, message="progress")

            candidate = row.get(device_catalog_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_catalog = _safe_advance(max_seen_device_catalog, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_catalog", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_channels (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_channels_cfg = streams.get("device_channels", {})
    device_channels_path = str(device_channels_cfg.get("path") or "/devices/{deviceId}/channels").strip()
    if not device_channels_path.startswith("/"):
        device_channels_path = "/" + device_channels_path

    device_channels_collection_key = device_channels_cfg.get("collection_key")
    device_channels_collection_key = device_channels_collection_key if isinstance(device_channels_collection_key, str) else None
    device_channels_cursor_field = str(device_channels_cfg.get("cursor_field") or "updatedAt")

    prev_device_channels_cursor = _get_stream_cursor(state, "device_channels") or fallback_cursor
    max_seen_device_channels: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_channels")
    def device_channels() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_channels
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_channels_path, stream="device_channels", since=prev_device_channels_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_channels",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_channels_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_channels"] = n
            if n % 100 == 0:
                records("device_channels", n, message="progress")

            candidate = row.get(device_channels_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_channels = _safe_advance(max_seen_device_channels, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_channels", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_groups (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_groups_cfg = streams.get("device_groups", {})
    device_groups_path = str(device_groups_cfg.get("path") or "/devices/{deviceId}/groups").strip()
    if not device_groups_path.startswith("/"):
        device_groups_path = "/" + device_groups_path

    device_groups_collection_key = device_groups_cfg.get("collection_key")
    device_groups_collection_key = device_groups_collection_key if isinstance(device_groups_collection_key, str) else None
    device_groups_cursor_field = str(device_groups_cfg.get("cursor_field") or "createdAt")

    prev_device_groups_cursor = _get_stream_cursor(state, "device_groups") or fallback_cursor
    max_seen_device_groups: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_groups")
    def device_groups() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_groups
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_groups_path, stream="device_groups", since=prev_device_groups_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_groups",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_groups_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_groups"] = n
            if n % 100 == 0:
                records("device_groups", n, message="progress")

            candidate = row.get(device_groups_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_groups = _safe_advance(max_seen_device_groups, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_groups", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_health (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_health_cfg = streams.get("device_health", {})
    device_health_path = str(device_health_cfg.get("path") or "/devices/{deviceId}/health").strip()
    if not device_health_path.startswith("/"):
        device_health_path = "/" + device_health_path

    device_health_collection_key = device_health_cfg.get("collection_key")
    device_health_collection_key = device_health_collection_key if isinstance(device_health_collection_key, str) else None
    device_health_cursor_field = str(device_health_cfg.get("cursor_field") or "updatedAt")

    prev_device_health_cursor = _get_stream_cursor(state, "device_health") or fallback_cursor
    max_seen_device_health: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_health")
    def device_health() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_health
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_health_path, stream="device_health", since=prev_device_health_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_health",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_health_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_health"] = n
            if n % 100 == 0:
                records("device_health", n, message="progress")

            candidate = row.get(device_health_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_health = _safe_advance(max_seen_device_health, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_health", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_invoices (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_invoices_cfg = streams.get("device_invoices", {})
    device_invoices_path = str(device_invoices_cfg.get("path") or "/devices/{deviceId}/invoices").strip()
    if not device_invoices_path.startswith("/"):
        device_invoices_path = "/" + device_invoices_path

    device_invoices_collection_key = device_invoices_cfg.get("collection_key")
    device_invoices_collection_key = device_invoices_collection_key if isinstance(device_invoices_collection_key, str) else None
    device_invoices_cursor_field = str(device_invoices_cfg.get("cursor_field") or "createdAt")

    prev_device_invoices_cursor = _get_stream_cursor(state, "device_invoices") or fallback_cursor
    max_seen_device_invoices: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_invoices")
    def device_invoices() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_invoices
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_invoices_path, stream="device_invoices", since=prev_device_invoices_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_invoices",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_invoices_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_invoices"] = n
            if n % 100 == 0:
                records("device_invoices", n, message="progress")

            candidate = row.get(device_invoices_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_invoices = _safe_advance(max_seen_device_invoices, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_invoices", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_labels (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_labels_cfg = streams.get("device_labels", {})
    device_labels_path = str(device_labels_cfg.get("path") or "/devices/{deviceId}/labels").strip()
    if not device_labels_path.startswith("/"):
        device_labels_path = "/" + device_labels_path

    device_labels_collection_key = device_labels_cfg.get("collection_key")
    device_labels_collection_key = device_labels_collection_key if isinstance(device_labels_collection_key, str) else None
    device_labels_cursor_field = str(device_labels_cfg.get("cursor_field") or "createdAt")

    prev_device_labels_cursor = _get_stream_cursor(state, "device_labels") or fallback_cursor
    max_seen_device_labels: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_labels")
    def device_labels() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_labels
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_labels_path, stream="device_labels", since=prev_device_labels_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_labels",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_labels_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_labels"] = n
            if n % 100 == 0:
                records("device_labels", n, message="progress")

            candidate = row.get(device_labels_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_labels = _safe_advance(max_seen_device_labels, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_labels", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_meeting_links (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_meeting_links_cfg = streams.get("device_meeting_links", {})
    device_meeting_links_path = str(device_meeting_links_cfg.get("path") or "/devices/{deviceId}/meeting-links").strip()
    if not device_meeting_links_path.startswith("/"):
        device_meeting_links_path = "/" + device_meeting_links_path

    device_meeting_links_collection_key = device_meeting_links_cfg.get("collection_key")
    device_meeting_links_collection_key = device_meeting_links_collection_key if isinstance(device_meeting_links_collection_key, str) else None
    device_meeting_links_cursor_field = str(device_meeting_links_cfg.get("cursor_field") or "createdAt")

    prev_device_meeting_links_cursor = _get_stream_cursor(state, "device_meeting_links") or fallback_cursor
    max_seen_device_meeting_links: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_meeting_links")
    def device_meeting_links() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_meeting_links
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_meeting_links_path, stream="device_meeting_links", since=prev_device_meeting_links_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_meeting_links",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_meeting_links_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_meeting_links"] = n
            if n % 100 == 0:
                records("device_meeting_links", n, message="progress")

            candidate = row.get(device_meeting_links_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_meeting_links = _safe_advance(max_seen_device_meeting_links, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_meeting_links", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_profile (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_profile_cfg = streams.get("device_profile", {})
    device_profile_path = str(device_profile_cfg.get("path") or "/devices/{deviceId}/profile").strip()
    if not device_profile_path.startswith("/"):
        device_profile_path = "/" + device_profile_path

    device_profile_collection_key = device_profile_cfg.get("collection_key")
    device_profile_collection_key = device_profile_collection_key if isinstance(device_profile_collection_key, str) else None
    device_profile_cursor_field = str(device_profile_cfg.get("cursor_field") or "updatedAt")

    prev_device_profile_cursor = _get_stream_cursor(state, "device_profile") or fallback_cursor
    max_seen_device_profile: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_profile")
    def device_profile() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_profile
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_profile_path, stream="device_profile", since=prev_device_profile_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_profile",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_profile_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_profile"] = n
            if n % 100 == 0:
                records("device_profile", n, message="progress")

            candidate = row.get(device_profile_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_profile = _safe_advance(max_seen_device_profile, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_profile", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_queue (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_queue_cfg = streams.get("device_queue", {})
    device_queue_path = str(device_queue_cfg.get("path") or "/devices/{deviceId}/queue").strip()
    if not device_queue_path.startswith("/"):
        device_queue_path = "/" + device_queue_path

    device_queue_collection_key = device_queue_cfg.get("collection_key")
    device_queue_collection_key = device_queue_collection_key if isinstance(device_queue_collection_key, str) else None
    device_queue_cursor_field = str(device_queue_cfg.get("cursor_field") or "updatedAt")

    prev_device_queue_cursor = _get_stream_cursor(state, "device_queue") or fallback_cursor
    max_seen_device_queue: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_queue")
    def device_queue() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_queue
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_queue_path, stream="device_queue", since=prev_device_queue_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_queue",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_queue_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_queue"] = n
            if n % 100 == 0:
                records("device_queue", n, message="progress")

            candidate = row.get(device_queue_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_queue = _safe_advance(max_seen_device_queue, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_queue", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_quick_replies (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_quick_replies_cfg = streams.get("device_quick_replies", {})
    device_quick_replies_path = str(device_quick_replies_cfg.get("path") or "/devices/{deviceId}/quickReplies").strip()
    if not device_quick_replies_path.startswith("/"):
        device_quick_replies_path = "/" + device_quick_replies_path

    device_quick_replies_collection_key = device_quick_replies_cfg.get("collection_key")
    device_quick_replies_collection_key = device_quick_replies_collection_key if isinstance(device_quick_replies_collection_key, str) else None
    device_quick_replies_cursor_field = str(device_quick_replies_cfg.get("cursor_field") or "updatedAt")

    prev_device_quick_replies_cursor = _get_stream_cursor(state, "device_quick_replies") or fallback_cursor
    max_seen_device_quick_replies: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_quick_replies")
    def device_quick_replies() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_quick_replies
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_quick_replies_path, stream="device_quick_replies", since=prev_device_quick_replies_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_quick_replies",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_quick_replies_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_quick_replies"] = n
            if n % 100 == 0:
                records("device_quick_replies", n, message="progress")

            candidate = row.get(device_quick_replies_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_quick_replies = _safe_advance(max_seen_device_quick_replies, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_quick_replies", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_scan (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_scan_cfg = streams.get("device_scan", {})
    device_scan_path = str(device_scan_cfg.get("path") or "/devices/{deviceId}/scan").strip()
    if not device_scan_path.startswith("/"):
        device_scan_path = "/" + device_scan_path

    device_scan_collection_key = device_scan_cfg.get("collection_key")
    device_scan_collection_key = device_scan_collection_key if isinstance(device_scan_collection_key, str) else None
    device_scan_cursor_field = str(device_scan_cfg.get("cursor_field") or "updatedAt")

    prev_device_scan_cursor = _get_stream_cursor(state, "device_scan") or fallback_cursor
    max_seen_device_scan: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_scan")
    def device_scan() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_scan
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_scan_path, stream="device_scan", since=prev_device_scan_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_scan",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_scan_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_scan"] = n
            if n % 100 == 0:
                records("device_scan", n, message="progress")

            candidate = row.get(device_scan_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_scan = _safe_advance(max_seen_device_scan, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_scan", n, message="done")

    # -------------------------------------------------------------------------------------
    # device_team (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    device_team_cfg = streams.get("device_team", {})
    device_team_path = str(device_team_cfg.get("path") or "/devices/{deviceId}/team").strip()
    if not device_team_path.startswith("/"):
        device_team_path = "/" + device_team_path

    device_team_collection_key = device_team_cfg.get("collection_key")
    device_team_collection_key = device_team_collection_key if isinstance(device_team_collection_key, str) else None
    device_team_cursor_field = str(device_team_cfg.get("cursor_field") or "createdAt")

    prev_device_team_cursor = _get_stream_cursor(state, "device_team") or fallback_cursor
    max_seen_device_team: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="device_team")
    def device_team() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_device_team
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(device_team_path, stream="device_team", since=prev_device_team_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="device_team",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=device_team_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["device_team"] = n
            if n % 100 == 0:
                records("device_team", n, message="progress")

            candidate = row.get(device_team_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_device_team = _safe_advance(max_seen_device_team, cand_iso)

            yield add_metadata(row, "wassenger")

        records("device_team", n, message="done")

    # -------------------------------------------------------------------------------------
    # waba_prices (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    waba_prices_cfg = streams.get("waba_prices", {})
    waba_prices_path = str(waba_prices_cfg.get("path") or "/waba/prices").strip()
    if not waba_prices_path.startswith("/"):
        waba_prices_path = "/" + waba_prices_path

    waba_prices_collection_key = waba_prices_cfg.get("collection_key")
    waba_prices_collection_key = waba_prices_collection_key if isinstance(waba_prices_collection_key, str) else None
    waba_prices_cursor_field = str(waba_prices_cfg.get("cursor_field") or "updatedAt")

    prev_waba_prices_cursor = _get_stream_cursor(state, "waba_prices") or fallback_cursor
    max_seen_waba_prices: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="waba_prices")
    def waba_prices() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_waba_prices
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(waba_prices_path, stream="waba_prices", since=prev_waba_prices_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="waba_prices",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=waba_prices_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["waba_prices"] = n
            if n % 100 == 0:
                records("waba_prices", n, message="progress")

            candidate = row.get(waba_prices_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_waba_prices = _safe_advance(max_seen_waba_prices, cand_iso)

            yield add_metadata(row, "wassenger")

        records("waba_prices", n, message="done")

    # -------------------------------------------------------------------------------------
    # waba_templates (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    waba_templates_cfg = streams.get("waba_templates", {})
    waba_templates_path = str(waba_templates_cfg.get("path") or "/waba/templates").strip()
    if not waba_templates_path.startswith("/"):
        waba_templates_path = "/" + waba_templates_path

    waba_templates_collection_key = waba_templates_cfg.get("collection_key")
    waba_templates_collection_key = waba_templates_collection_key if isinstance(waba_templates_collection_key, str) else None
    waba_templates_cursor_field = str(waba_templates_cfg.get("cursor_field") or "updatedAt")

    prev_waba_templates_cursor = _get_stream_cursor(state, "waba_templates") or fallback_cursor
    max_seen_waba_templates: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="waba_templates")
    def waba_templates() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_waba_templates
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(waba_templates_path, stream="waba_templates", since=prev_waba_templates_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="waba_templates",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=waba_templates_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["waba_templates"] = n
            if n % 100 == 0:
                records("waba_templates", n, message="progress")

            candidate = row.get(waba_templates_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_waba_templates = _safe_advance(max_seen_waba_templates, cand_iso)

            yield add_metadata(row, "wassenger")

        records("waba_templates", n, message="done")

    # -------------------------------------------------------------------------------------
    # webhooks (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    webhooks_cfg = streams.get("webhooks", {})
    webhooks_path = str(webhooks_cfg.get("path") or "/webhooks").strip()
    if not webhooks_path.startswith("/"):
        webhooks_path = "/" + webhooks_path

    webhooks_collection_key = webhooks_cfg.get("collection_key")
    webhooks_collection_key = webhooks_collection_key if isinstance(webhooks_collection_key, str) else None
    webhooks_cursor_field = str(webhooks_cfg.get("cursor_field") or "createdAt")

    prev_webhooks_cursor = _get_stream_cursor(state, "webhooks") or fallback_cursor
    max_seen_webhooks: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="webhooks")
    def webhooks() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_webhooks
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(webhooks_path, stream="webhooks", since=prev_webhooks_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="webhooks",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=webhooks_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["webhooks"] = n
            if n % 100 == 0:
                records("webhooks", n, message="progress")

            candidate = row.get(webhooks_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_webhooks = _safe_advance(max_seen_webhooks, cand_iso)

            yield add_metadata(row, "wassenger")

        records("webhooks", n, message="done")

    # -------------------------------------------------------------------------------------
    # webhook_logs (RESOURCE FUNCTION)
    # -------------------------------------------------------------------------------------
    webhook_logs_cfg = streams.get("webhook_logs", {})
    webhook_logs_path = str(webhook_logs_cfg.get("path") or "/webhooks/{webhookId}/logs").strip()
    if not webhook_logs_path.startswith("/"):
        webhook_logs_path = "/" + webhook_logs_path

    webhook_logs_collection_key = webhook_logs_cfg.get("collection_key")
    webhook_logs_collection_key = webhook_logs_collection_key if isinstance(webhook_logs_collection_key, str) else None
    webhook_logs_cursor_field = str(webhook_logs_cfg.get("cursor_field") or "createdAt")

    prev_webhook_logs_cursor = _get_stream_cursor(state, "webhook_logs") or fallback_cursor
    max_seen_webhook_logs: Optional[str] = None

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="webhook_logs")
    def webhook_logs() -> Iterable[Dict[str, Any]]:
        nonlocal max_seen_webhook_logs
        n = 0

        def _fetch(p: int, lim: int) -> Any:
            return fetch_page(webhook_logs_path, stream="webhook_logs", since=prev_webhook_logs_cursor, page=p, limit=lim)

        for row in paged_get(
            _fetch,
            stream="webhook_logs",
            page_size=page_size,
            max_pages=max_pages,
            collection_key=webhook_logs_collection_key,
        ):
            if not isinstance(row, dict):
                continue

            n += 1
            counts["webhook_logs"] = n
            if n % 100 == 0:
                records("webhook_logs", n, message="progress")

            candidate = row.get(webhook_logs_cursor_field) or row.get("updatedAt") or row.get("createdAt")
            if isinstance(candidate, str) and candidate:
                dt = parse_iso(candidate)
                cand_iso = dt.isoformat() if isinstance(dt, datetime) else candidate
                max_seen_webhook_logs = _safe_advance(max_seen_webhook_logs, cand_iso)

            yield add_metadata(row, "wassenger")

        records("webhook_logs", n, message="done")

    # -------------------------------------------------------------------------------------
    # build resources list — IMPORTANT: add FUNCTIONS, not instances
    # -------------------------------------------------------------------------------------
    resources: List[Any] = []

    if want_streams.get("messages", False):
        resources.append(messages)

    # Add both contacts from direct API and from messages
    if want_streams.get("contacts", False):
        # The original transformer from messages
        resources.append(contacts)
        _set_stream_cursor(state_updates, "contacts", run_started_at)

    # Add the new direct contacts API resource as a separate table
    if want_streams.get("contacts_direct", False):
        resources.append(contacts_direct)


    if want_streams.get("conversations", False):
        resources.append(conversations)
        _set_stream_cursor(state_updates, "conversations", run_started_at)

    if want_streams.get("devices", False):
        resources.append(devices)

    if want_streams.get("team", False):
        resources.append(team)

    if want_streams.get("departments", False):
        resources.append(departments)

    if want_streams.get("analytics", False):
        resources.append(analytics)

    if want_streams.get("campaigns", False):
        resources.append(campaigns)

    if want_streams.get("chat_participants", False):
        resources.append(chat_participants)

    if want_streams.get("chat_sync", False):
        resources.append(chat_sync)

    if want_streams.get("files", False):
        resources.append(files)

    if want_streams.get("device_autoassign", False):
        resources.append(device_autoassign)

    if want_streams.get("device_autoreplies", False):
        resources.append(device_autoreplies)

    if want_streams.get("device_catalog", False):
        resources.append(device_catalog)

    if want_streams.get("device_channels", False):
        resources.append(device_channels)

    if want_streams.get("device_groups", False):
        resources.append(device_groups)

    if want_streams.get("device_health", False):
        resources.append(device_health)

    if want_streams.get("device_invoices", False):
        resources.append(device_invoices)

    if want_streams.get("device_labels", False):
        resources.append(device_labels)

    if want_streams.get("device_meeting_links", False):
        resources.append(device_meeting_links)

    if want_streams.get("device_profile", False):
        resources.append(device_profile)

    if want_streams.get("device_queue", False):
        resources.append(device_queue)

    if want_streams.get("device_quick_replies", False):
        resources.append(device_quick_replies)

    if want_streams.get("device_scan", False):
        resources.append(device_scan)

    if want_streams.get("device_team", False):
        resources.append(device_team)

    if want_streams.get("waba_prices", False):
        resources.append(waba_prices)

    if want_streams.get("waba_templates", False):
        resources.append(waba_templates)

    if want_streams.get("webhooks", False):
        resources.append(webhooks)

    if want_streams.get("webhook_logs", False):
        resources.append(webhook_logs)

    pipeline = dlt.pipeline(pipeline_name="wassenger", destination="postgres", dataset_name=schema)
    info("pipeline.run.start", stream="wassenger", destination="postgres", dataset=schema)
    load_info = pipeline.run(resources)
    info("pipeline.run.done", stream="wassenger", counts=counts, diag=diag)

    # -------------------------------------------------------------------------------------
    # IMPORTANT: persist resource cursors AFTER the generators have been consumed by pipeline.run(...)
    # -------------------------------------------------------------------------------------
    messages_cursor_out = _safe_advance(prev_messages_cursor, max_seen_messages) if max_seen_messages else (
        prev_messages_cursor or run_started_at
    )
    _set_stream_cursor(state_updates, "messages", messages_cursor_out)

    dev_cursor_out = _safe_advance(prev_dev_cursor, max_seen_devices) if max_seen_devices else (prev_dev_cursor or run_started_at)
    _set_stream_cursor(state_updates, "devices", dev_cursor_out)

    analytics_cursor_out = _safe_advance(prev_analytics_cursor, max_seen_analytics) if max_seen_analytics else (prev_analytics_cursor or run_started_at)
    _set_stream_cursor(state_updates, "analytics", analytics_cursor_out)

    campaigns_cursor_out = _safe_advance(prev_campaigns_cursor, max_seen_campaigns) if max_seen_campaigns else (prev_campaigns_cursor or run_started_at)
    _set_stream_cursor(state_updates, "campaigns", campaigns_cursor_out)

    chat_participants_cursor_out = _safe_advance(prev_chat_participants_cursor, max_seen_chat_participants) if max_seen_chat_participants else (prev_chat_participants_cursor or run_started_at)
    _set_stream_cursor(state_updates, "chat_participants", chat_participants_cursor_out)

    chat_sync_cursor_out = _safe_advance(prev_chat_sync_cursor, max_seen_chat_sync) if max_seen_chat_sync else (prev_chat_sync_cursor or run_started_at)
    _set_stream_cursor(state_updates, "chat_sync", chat_sync_cursor_out)

    files_cursor_out = _safe_advance(prev_files_cursor, max_seen_files) if max_seen_files else (prev_files_cursor or run_started_at)
    _set_stream_cursor(state_updates, "files", files_cursor_out)

    device_autoassign_cursor_out = _safe_advance(prev_device_autoassign_cursor, max_seen_device_autoassign) if max_seen_device_autoassign else (prev_device_autoassign_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_autoassign", device_autoassign_cursor_out)

    device_autoreplies_cursor_out = _safe_advance(prev_device_autoreplies_cursor, max_seen_device_autoreplies) if max_seen_device_autoreplies else (prev_device_autoreplies_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_autoreplies", device_autoreplies_cursor_out)

    device_catalog_cursor_out = _safe_advance(prev_device_catalog_cursor, max_seen_device_catalog) if max_seen_device_catalog else (prev_device_catalog_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_catalog", device_catalog_cursor_out)

    device_channels_cursor_out = _safe_advance(prev_device_channels_cursor, max_seen_device_channels) if max_seen_device_channels else (prev_device_channels_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_channels", device_channels_cursor_out)

    device_groups_cursor_out = _safe_advance(prev_device_groups_cursor, max_seen_device_groups) if max_seen_device_groups else (prev_device_groups_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_groups", device_groups_cursor_out)

    device_health_cursor_out = _safe_advance(prev_device_health_cursor, max_seen_device_health) if max_seen_device_health else (prev_device_health_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_health", device_health_cursor_out)

    device_invoices_cursor_out = _safe_advance(prev_device_invoices_cursor, max_seen_device_invoices) if max_seen_device_invoices else (prev_device_invoices_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_invoices", device_invoices_cursor_out)

    device_labels_cursor_out = _safe_advance(prev_device_labels_cursor, max_seen_device_labels) if max_seen_device_labels else (prev_device_labels_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_labels", device_labels_cursor_out)

    device_meeting_links_cursor_out = _safe_advance(prev_device_meeting_links_cursor, max_seen_device_meeting_links) if max_seen_device_meeting_links else (prev_device_meeting_links_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_meeting_links", device_meeting_links_cursor_out)

    device_profile_cursor_out = _safe_advance(prev_device_profile_cursor, max_seen_device_profile) if max_seen_device_profile else (prev_device_profile_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_profile", device_profile_cursor_out)

    device_queue_cursor_out = _safe_advance(prev_device_queue_cursor, max_seen_device_queue) if max_seen_device_queue else (prev_device_queue_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_queue", device_queue_cursor_out)

    device_quick_replies_cursor_out = _safe_advance(prev_device_quick_replies_cursor, max_seen_device_quick_replies) if max_seen_device_quick_replies else (prev_device_quick_replies_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_quick_replies", device_quick_replies_cursor_out)

    device_scan_cursor_out = _safe_advance(prev_device_scan_cursor, max_seen_device_scan) if max_seen_device_scan else (prev_device_scan_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_scan", device_scan_cursor_out)

    device_team_cursor_out = _safe_advance(prev_device_team_cursor, max_seen_device_team) if max_seen_device_team else (prev_device_team_cursor or run_started_at)
    _set_stream_cursor(state_updates, "device_team", device_team_cursor_out)

    waba_prices_cursor_out = _safe_advance(prev_waba_prices_cursor, max_seen_waba_prices) if max_seen_waba_prices else (prev_waba_prices_cursor or run_started_at)
    _set_stream_cursor(state_updates, "waba_prices", waba_prices_cursor_out)

    waba_templates_cursor_out = _safe_advance(prev_waba_templates_cursor, max_seen_waba_templates) if max_seen_waba_templates else (prev_waba_templates_cursor or run_started_at)
    _set_stream_cursor(state_updates, "waba_templates", waba_templates_cursor_out)

    webhooks_cursor_out = _safe_advance(prev_webhooks_cursor, max_seen_webhooks) if max_seen_webhooks else (prev_webhooks_cursor or run_started_at)
    _set_stream_cursor(state_updates, "webhooks", webhooks_cursor_out)

    webhook_logs_cursor_out = _safe_advance(prev_webhook_logs_cursor, max_seen_webhook_logs) if max_seen_webhook_logs else (prev_webhook_logs_cursor or run_started_at)
    _set_stream_cursor(state_updates, "webhook_logs", webhook_logs_cursor_out)

    state_updates["last_success_at"] = run_started_at

    report = (
        "Wassenger sync completed.\n"
        f"- Base: {base}\n"
        f"- Streams: {selected_stream_names}\n"
        f"- Counts: {counts}\n"
        f"- Dropped: {diag.get('dropped')}\n"
        f"{load_info}"
    )
    return report, None, state_updates
