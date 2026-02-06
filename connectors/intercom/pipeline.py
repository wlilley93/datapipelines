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
