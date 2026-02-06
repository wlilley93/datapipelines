from __future__ import annotations

from datetime import timedelta, timezone, datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import dlt

from connectors.runtime.protocol import ReadSelection

from .actions import fetch_changed_card_ids_for_board
from .cards import (
    SinceUnsupportedError,
    hydrate_card_with_fallback,
    pace_request,
    yield_board_cards_light_list_only,
    yield_board_cards_with_fallback,
)
from .constants import (
    CARD_LIST_FILTER,
    DEFAULT_ALL_CARDS_MIGRATION_MAX_CARDS,
    DEFAULT_ALL_CARDS_MIGRATION_PER_BOARD_CAP,
    DEFAULT_ENABLE_ALL_CARDS_MIGRATION_BACKFILL,
    DEFAULT_ENABLE_PERIODIC_SWEEP,
    DEFAULT_HYDRATE_INTERVAL_MS,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_MAX_CHANGED_CARD_IDS,
    DEFAULT_PER_BOARD_CHANGED_CARD_CAP,
    DEFAULT_SCHEMA_LIST_ITEMS_CAP,
    DEFAULT_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM,
    DEFAULT_SWEEP_EVERY_HOURS,
    MAX_ALL_CARDS_MIGRATION_MAX_CARDS,
    MAX_ALL_CARDS_MIGRATION_PER_BOARD_CAP,
    MAX_HYDRATE_INTERVAL_MS,
    MAX_LOOKBACK_MINUTES,
    MAX_MAX_CHANGED_CARD_IDS,
    MAX_PER_BOARD_CHANGED_CARD_CAP,
    MAX_SCHEMA_LIST_ITEMS_CAP,
    MAX_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM,
    MAX_SWEEP_EVERY_HOURS,
    SCHEMA_HASH_KEY,
    SCHEMA_STATE_KEY,
    TRELLO_API,
)
from .events import debug, emit_event, info, warn
from .http import as_dict, as_list, request_json_logged
from .schema_tracker import SchemaCaps, SchemaTracker, stable_hash
from .selection import is_selected, normalize_selection
from .time_utils import (
    clamp_int,
    compute_effective_since_from_cursor,
    is_later_timestamp,
    normalize_cursor_iso,
    parse_iso,
    utc_now,
    utc_now_iso,
)
from .utils_bridge import DEFAULT_TIMEOUT, add_metadata, requests_retry_session

def get_observed_schema() -> Dict[str, Any]:
    return {
        "streams": {
            "boards": {"primary_key": ["id"], "properties": {"id": {"type": "string"}}},
            "organizations": {"primary_key": ["id"], "properties": {"id": {"type": "string"}}},
            "lists": {"primary_key": ["id"], "properties": {"id": {"type": "string"}, "_board_id": {"type": "string"}}},
            "members": {"primary_key": ["id"], "properties": {"id": {"type": "string"}, "_board_id": {"type": "string"}}},
            "cards": {"primary_key": ["id"], "properties": {"id": {"type": "string"}, "_board_id": {"type": "string"}}},
            "checklists": {"primary_key": ["id"], "properties": {"id": {"type": "string"}, "_board_id": {"type": "string"}}},
        }
    }

def get_actions_cursor(state: Dict[str, Any]) -> Optional[str]:
    streams = state.get("streams")
    if isinstance(streams, dict):
        actions = streams.get("actions")
        if isinstance(actions, dict):
            cursor = actions.get("cursor")
            out = normalize_cursor_iso(cursor)
            if out:
                return out
    for legacy_key in ("trello_actions_since", "start_date"):
        out = normalize_cursor_iso(state.get(legacy_key))
        if out:
            return out
    return None

def get_known_bad_since_boards(state: Dict[str, Any]) -> Set[str]:
    streams = state.get("streams")
    if not isinstance(streams, dict):
        return set()
    cards = streams.get("cards")
    if not isinstance(cards, dict):
        return set()
    m = cards.get("since_supported_by_board")
    if not isinstance(m, dict):
        return set()
    return {str(bid) for bid, supported in m.items() if supported is False}

def get_last_sweep_map(state: Dict[str, Any]) -> Dict[str, str]:
    streams = state.get("streams")
    if not isinstance(streams, dict):
        return {}
    cards = streams.get("cards")
    if not isinstance(cards, dict):
        return {}
    m = cards.get("last_sweep_at_by_board")
    return m if isinstance(m, dict) else {}

def get_cards_all_migrated_at(state: Dict[str, Any]) -> Optional[str]:
    streams = state.get("streams")
    if not isinstance(streams, dict):
        return None
    cards = streams.get("cards")
    if not isinstance(cards, dict):
        return None
    return normalize_cursor_iso(cards.get("all_endpoint_migrated_at"))

class StreamCounter:
    def __init__(self, *, log_interval: int = 250):
        self._counts: Dict[str, int] = {}
        self._log_interval = max(1, int(log_interval))
    def inc(self, stream: str, n: int = 1) -> int:
        cur = self._counts.get(stream, 0) + int(n)
        self._counts[stream] = cur
        if cur % self._log_interval == 0:
            emit_event("count", f"{stream}.progress", stream=stream, count=cur, level="debug")
        return cur
    def get(self, stream: str) -> int:
        return self._counts.get(stream, 0)
    def all(self) -> Dict[str, int]:
        return dict(self._counts)

def test_connection(creds: Dict[str, str]) -> str:
    session = requests_retry_session()
    params = {"key": creds["api_key"], "token": creds["token"], "fields": "username,fullName"}
    data = request_json_logged(
        session,
        "GET",
        f"{TRELLO_API}/members/me",
        params=params,
        timeout=DEFAULT_TIMEOUT,
        stream="trello",
        op="test_connection",
    )
    user = as_dict(data, stream="trello", op="test_connection")
    return f"Connected as {user.get('fullName')} (@{user.get('username')})"

def run_pipeline(
    creds: Dict[str, str],
    schema: str,
    state: Optional[Dict[str, Any]] = None,
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any]]:
    session = requests_retry_session()
    base_params = {"key": creds["api_key"], "token": creds["token"]}
    state = state or {}
    run_started_at = utc_now()
    lookback_minutes = clamp_int(state.get("lookback_minutes"), default=DEFAULT_LOOKBACK_MINUTES, lo=0, hi=MAX_LOOKBACK_MINUTES)
    hydrate_interval_ms = clamp_int(state.get("trello_hydrate_min_interval_ms"), default=DEFAULT_HYDRATE_INTERVAL_MS, lo=0, hi=MAX_HYDRATE_INTERVAL_MS)
    max_changed_card_ids = clamp_int(state.get("trello_max_changed_card_ids"), default=DEFAULT_MAX_CHANGED_CARD_IDS, lo=1_000, hi=MAX_MAX_CHANGED_CARD_IDS)
    per_board_changed_card_cap = clamp_int(state.get("trello_per_board_changed_card_cap"), default=DEFAULT_PER_BOARD_CHANGED_CARD_CAP, lo=250, hi=MAX_PER_BOARD_CHANGED_CARD_CAP)
    enable_periodic_sweep = bool(state.get("trello_enable_periodic_sweep", DEFAULT_ENABLE_PERIODIC_SWEEP))
    sweep_every_hours = clamp_int(state.get("trello_sweep_every_hours"), default=DEFAULT_SWEEP_EVERY_HOURS, lo=1, hi=MAX_SWEEP_EVERY_HOURS)
    schema_sample_max = clamp_int(state.get("trello_schema_sample_max_records_per_stream"), default=DEFAULT_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM, lo=0, hi=MAX_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM)
    schema_list_cap = clamp_int(state.get("trello_schema_list_items_cap"), default=DEFAULT_SCHEMA_LIST_ITEMS_CAP, lo=0, hi=MAX_SCHEMA_LIST_ITEMS_CAP)
    enable_all_cards_migration_backfill = bool(state.get("trello_enable_all_cards_migration_backfill", DEFAULT_ENABLE_ALL_CARDS_MIGRATION_BACKFILL))
    migration_max_cards = clamp_int(state.get("trello_all_cards_migration_max_cards"), default=DEFAULT_ALL_CARDS_MIGRATION_MAX_CARDS, lo=0, hi=MAX_ALL_CARDS_MIGRATION_MAX_CARDS)
    migration_per_board_cap = clamp_int(state.get("trello_all_cards_migration_per_board_cap"), default=DEFAULT_ALL_CARDS_MIGRATION_PER_BOARD_CAP, lo=0, hi=MAX_ALL_CARDS_MIGRATION_PER_BOARD_CAP)

    previous_cursor = get_actions_cursor(state)
    is_first_run = not bool(previous_cursor)
    since_effective = compute_effective_since_from_cursor(previous_cursor, lookback_minutes) if previous_cursor else None
    known_bad_since = get_known_bad_since_boards(state)
    last_sweep_map = get_last_sweep_map(state)

    selected = normalize_selection(selection)
    want_boards = is_selected(selected, "boards")
    want_orgs = is_selected(selected, "organizations")
    want_lists = is_selected(selected, "lists")
    want_members = is_selected(selected, "members")
    want_cards = is_selected(selected, "cards")
    want_checklists = is_selected(selected, "checklists")
    selected_streams = [n for n, w in [("boards", want_boards), ("organizations", want_orgs), ("lists", want_lists), ("members", want_members), ("cards", want_cards), ("checklists", want_checklists)] if w]

    prior_snapshot = state.get(SCHEMA_STATE_KEY) if isinstance(state.get(SCHEMA_STATE_KEY), dict) else None
    tracker = SchemaTracker(prior_snapshot=prior_snapshot, caps=SchemaCaps(max_records_per_stream=int(schema_sample_max), list_items_cap=int(schema_list_cap)))

    cards_all_migrated_at = get_cards_all_migrated_at(state)
    should_run_migration_backfill = (
        want_cards
        and enable_all_cards_migration_backfill
        and (not is_first_run)
        and (cards_all_migrated_at is None)
        and migration_max_cards > 0
        and migration_per_board_cap > 0
    )

    info(
        "sync.start",
        stream="trello",
        schema=schema,
        lookback_minutes=lookback_minutes,
        incremental=not is_first_run,
        selected_streams=selected_streams,
        enable_periodic_sweep=enable_periodic_sweep,
        sweep_every_hours=sweep_every_hours,
        max_changed_card_ids=max_changed_card_ids,
        per_board_changed_card_cap=per_board_changed_card_cap,
        run_started_at=run_started_at.isoformat(),
        schema_sample_max_records_per_stream=schema_sample_max,
        schema_list_items_cap=schema_list_cap,
        card_list_filter=CARD_LIST_FILTER,
        enable_all_cards_migration_backfill=should_run_migration_backfill,
        migration_max_cards=migration_max_cards,
        migration_per_board_cap=migration_per_board_cap,
    )

    counter = StreamCounter(log_interval=250)

    needs_boards = any([want_boards, want_lists, want_members, want_cards, want_checklists])
    boards_cache: List[Dict[str, Any]] = []

    if needs_boards:
        info("boards.fetch.start", stream="boards")
        boards_raw = request_json_logged(
            session,
            "GET",
            f"{TRELLO_API}/members/me/boards",
            params={**base_params, "fields": "all"},
            timeout=DEFAULT_TIMEOUT,
            stream="boards",
            op="boards_list",
        )
        boards_cache = [b for b in as_list(boards_raw, stream="boards", op="boards_list") if isinstance(b, dict) and b.get("id")]
        emit_event("count", "boards.discovered", stream="boards", count=len(boards_cache), level="info")

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="boards")
    def boards():
        if not want_boards:
            return
        for board in boards_cache:
            counter.inc("boards")
            tracker.observe("boards", board)
            yield add_metadata(dict(board), "trello")
        emit_event("count", "boards.done", stream="boards", count=counter.get("boards"), level="info")

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="organizations")
    def organizations():
        if not want_orgs:
            return
        raw = request_json_logged(
            session,
            "GET",
            f"{TRELLO_API}/members/me/organizations",
            params={**base_params, "fields": "all"},
            timeout=DEFAULT_TIMEOUT,
            stream="organizations",
            op="orgs_list",
        )
        for org in as_list(raw, stream="organizations", op="orgs_list"):
            if isinstance(org, dict):
                rec = dict(org)
                counter.inc("organizations")
                tracker.observe("organizations", rec)
                yield add_metadata(rec, "trello")
        emit_event("count", "organizations.done", stream="organizations", count=counter.get("organizations"), level="info")

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="lists")
    def lists():
        if not want_lists:
            return
        params = {**base_params, "fields": "id,name,closed,idBoard,pos,subscribed"}
        for board in boards_cache:
            bid = board["id"]
            raw = request_json_logged(
                session,
                "GET",
                f"{TRELLO_API}/boards/{bid}/lists",
                params=params,
                timeout=DEFAULT_TIMEOUT,
                stream="lists",
                board_id=bid,
                op="lists_by_board",
            )
            for lst in as_list(raw, stream="lists", op="lists_by_board", board_id=bid):
                if isinstance(lst, dict):
                    rec = dict(lst)
                    rec["_board_id"] = bid
                    counter.inc("lists")
                    tracker.observe("lists", rec)
                    yield add_metadata(rec, "trello")
        emit_event("count", "lists.done", stream="lists", count=counter.get("lists"), level="info")

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="members")
    def members():
        if not want_members:
            return
        params = {**base_params, "fields": "all"}
        for board in boards_cache:
            bid = board["id"]
            raw = request_json_logged(
                session,
                "GET",
                f"{TRELLO_API}/boards/{bid}/members",
                params=params,
                timeout=DEFAULT_TIMEOUT,
                stream="members",
                board_id=bid,
                op="members_by_board",
            )
            for member in as_list(raw, stream="members", op="members_by_board", board_id=bid):
                if isinstance(member, dict):
                    rec = dict(member)
                    rec["_board_id"] = bid
                    counter.inc("members")
                    tracker.observe("members", rec)
                    yield add_metadata(rec, "trello")
        emit_event("count", "members.done", stream="members", count=counter.get("members"), level="info")

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="checklists")
    def checklists():
        if not want_checklists:
            return
        params = {**base_params, "fields": "all", "checkItem_fields": "all"}
        for board in boards_cache:
            bid = board["id"]
            from .paging import paged_list  # local import to avoid cycles
            for checklist in paged_list(
                session,
                f"{TRELLO_API}/boards/{bid}/checklists",
                params=params,
                limit=1000,
                stream="checklists",
                board_id=bid,
                op="checklists_by_board",
                timeout=DEFAULT_TIMEOUT,
            ):
                rec = dict(checklist)
                rec["_board_id"] = bid
                counter.inc("checklists")
                tracker.observe("checklists", rec)
                yield add_metadata(rec, "trello")
        emit_event("count", "checklists.done", stream="checklists", count=counter.get("checklists"), level="info")

    @dlt.resource(write_disposition="merge", primary_key="id", table_name="cards")
    def cards():
        nonlocal known_bad_since, last_sweep_map
        remaining_budget = int(max_changed_card_ids)
        budget_hit = False
        updated_known_bad_since: Set[str] = set(known_bad_since)
        updated_last_sweep_map: Dict[str, str] = dict(last_sweep_map)
        max_action_date: Optional[str] = None
        ignored_max_action_date_due_to_truncation: Optional[str] = None
        migration_backfill_truncated = False
        migration_backfill_emitted_cards = 0
        if should_run_migration_backfill:
            warn(
                "cards.migration_backfill.start",
                stream="cards",
                reason="migrating_to_cards_all_endpoint_without_requiring_initial_sync",
                migration_max_cards=migration_max_cards,
                migration_per_board_cap=migration_per_board_cap,
            )
            remaining_migration_budget = int(migration_max_cards)
            for board in boards_cache:
                if remaining_migration_budget <= 0:
                    migration_backfill_truncated = True
                    break
                bid = board["id"]
                per_board_budget = min(int(migration_per_board_cap), remaining_migration_budget)
                emitted_this_board = 0
                for card in yield_board_cards_light_list_only(session, bid, board, base_params):
                    if emitted_this_board >= per_board_budget or remaining_migration_budget <= 0:
                        migration_backfill_truncated = True
                        break
                    rec = dict(card)
                    rec["_board_id"] = bid
                    counter.inc("cards")
                    tracker.observe("cards", rec)
                    yield add_metadata(rec, "trello")
                    emitted_this_board += 1
                    migration_backfill_emitted_cards += 1
                    remaining_migration_budget -= 1
                if migration_backfill_truncated:
                    break
            warn(
                "cards.migration_backfill.done",
                stream="cards",
                emitted_cards=migration_backfill_emitted_cards,
                truncated=migration_backfill_truncated,
            )

        for board in boards_cache:
            bid = board["id"]
            if is_first_run:
                for card in yield_board_cards_with_fallback(
                    session,
                    bid,
                    board,
                    base_params,
                    hydrate_interval_ms,
                    allow_since=False,
                    since=None,
                    is_full_scan=True,
                ):
                    rec = dict(card)
                    rec["_board_id"] = bid
                    counter.inc("cards")
                    tracker.observe("cards", rec)
                    yield add_metadata(rec, "trello")
                continue

            if remaining_budget > 0 and since_effective:
                per_board_budget = min(remaining_budget, int(per_board_changed_card_cap))
                changed_ids, board_max_action_date, hit = fetch_changed_card_ids_for_board(
                    session,
                    bid,
                    base_params,
                    since=since_effective,
                    max_ids_remaining_global=remaining_budget,
                    max_ids_this_board=per_board_budget,
                )
                if hit and board_max_action_date:
                    if ignored_max_action_date_due_to_truncation is None or is_later_timestamp(
                        board_max_action_date, ignored_max_action_date_due_to_truncation
                    ):
                        ignored_max_action_date_due_to_truncation = board_max_action_date
                if board_max_action_date and not hit:
                    if max_action_date is None or is_later_timestamp(board_max_action_date, max_action_date):
                        max_action_date = board_max_action_date
                remaining_budget = max(0, remaining_budget - len(changed_ids))
                budget_hit = bool(hit or remaining_budget == 0)
                if budget_hit:
                    warn(
                        "cards.delta.budget_hit",
                        stream="cards",
                        board_id=bid,
                        max_changed_card_ids=max_changed_card_ids,
                        remaining_budget=remaining_budget,
                        per_board_changed_card_cap=per_board_changed_card_cap,
                    )
                if changed_ids:
                    last_call_at = 0.0
                    for cid in changed_ids:
                        last_call_at = pace_request(int(hydrate_interval_ms), last_call_at)
                        card = hydrate_card_with_fallback(session, cid, base_params=base_params, board_id=bid)
                        if card:
                            rec = dict(card)
                            rec["_board_id"] = bid
                            counter.inc("cards")
                            tracker.observe("cards", rec)
                            yield add_metadata(rec, "trello")
            if remaining_budget == 0:
                continue

            if bid in updated_known_bad_since:
                continue
            if not enable_periodic_sweep or not since_effective:
                continue
            now = utc_now()
            last_sweep_iso = updated_last_sweep_map.get(bid)
            last_sweep_dt = parse_iso(last_sweep_iso) if isinstance(last_sweep_iso, str) else None
            due = last_sweep_dt is None or (now - last_sweep_dt) >= timedelta(hours=int(sweep_every_hours))
            if not due:
                continue
            try:
                saw_any = False
                for card in yield_board_cards_with_fallback(
                    session,
                    bid,
                    board,
                    base_params,
                    hydrate_interval_ms,
                    allow_since=True,
                    since=since_effective,
                    is_full_scan=False,
                ):
                    saw_any = True
                    rec = dict(card)
                    rec["_board_id"] = bid
                    counter.inc("cards")
                    tracker.observe("cards", rec)
                    yield add_metadata(rec, "trello")
                updated_last_sweep_map[bid] = utc_now_iso()
                debug("cards.sweep.recorded", stream="cards", board_id=bid, saw_any=saw_any)
            except SinceUnsupportedError:
                updated_known_bad_since.add(bid)
                warn("cards.sweep.disabled_for_board", stream="cards", board_id=bid, reason="since_unsupported_actions_only")
            except Exception as e:
                warn("cards.sweep.failed", stream="cards", board_id=bid, error=str(e)[:500])

        if budget_hit:
            warn(
                "actions.cursor.not_advanced_due_to_truncation",
                stream="trello",
                previous_cursor=previous_cursor,
                observed_max_action_date_while_truncating=ignored_max_action_date_due_to_truncation,
            )
        emit_event("count", "cards.done", stream="cards", count=counter.get("cards"), level="info")
        cards._state_side_effects = {  # type: ignore[attr-defined]
            "max_action_date": max_action_date,
            "known_bad_since": sorted(updated_known_bad_since),
            "last_sweep_map": updated_last_sweep_map,
            "budget_hit": budget_hit,
            "migration_backfill_ran": bool(should_run_migration_backfill),
            "migration_backfill_truncated": bool(migration_backfill_truncated),
            "migration_backfill_emitted_cards": int(migration_backfill_emitted_cards),
        }

    resources: List[Any] = []
    if want_boards:
        resources.append(boards())
    if want_orgs:
        resources.append(organizations())
    if want_lists:
        resources.append(lists())
    if want_members:
        resources.append(members())
    if want_cards:
        resources.append(cards())
    if want_checklists:
        resources.append(checklists())
    if not resources:
        warn("sync.no_streams_selected", stream="trello")
        return "No streams selected", None, {}

    pipeline = dlt.pipeline(pipeline_name="trello", destination="postgres", dataset_name=schema)
    info("pipeline.run.start", stream="trello", destination="postgres", dataset=schema)
    info_obj = pipeline.run(resources)
    info("pipeline.run.done", stream="trello")

    state_updates: Dict[str, Any] = {"streams": {}}
    updated_known_bad_since = list(known_bad_since)
    updated_last_sweep_map = dict(last_sweep_map)
    budget_hit = False
    max_action_date: Optional[str] = None
    migration_backfill_ran = False
    migration_backfill_truncated = False

    if want_cards and hasattr(cards, "_state_side_effects"):
        fx = getattr(cards, "_state_side_effects")
        if isinstance(fx, dict):
            mad = fx.get("max_action_date")
            if isinstance(mad, str) and mad:
                max_action_date = mad
            kbs = fx.get("known_bad_since")
            if isinstance(kbs, list):
                updated_known_bad_since = [str(x) for x in kbs]
            lsm = fx.get("last_sweep_map")
            if isinstance(lsm, dict):
                updated_last_sweep_map = {str(k): str(v) for k, v in lsm.items() if isinstance(v, str)}
            budget_hit = bool(fx.get("budget_hit"))
            migration_backfill_ran = bool(fx.get("migration_backfill_ran"))
            migration_backfill_truncated = bool(fx.get("migration_backfill_truncated"))

    if budget_hit:
        cursor_out = previous_cursor
    else:
        if previous_cursor:
            if max_action_date and is_later_timestamp(max_action_date, previous_cursor):
                cursor_out = max_action_date
            else:
                cursor_out = previous_cursor
        else:
            first_cursor_dt = run_started_at - timedelta(minutes=int(lookback_minutes))
            cursor_out = first_cursor_dt.astimezone(timezone.utc).isoformat()

    cursor_out = normalize_cursor_iso(cursor_out) or cursor_out
    if cursor_out:
        state_updates["streams"]["actions"] = {"cursor": cursor_out}
        state_updates["trello_actions_since"] = cursor_out

    cards_state: Dict[str, Any] = {
        "since_supported_by_board": {bid: False for bid in updated_known_bad_since},
        "last_sweep_at_by_board": updated_last_sweep_map,
    }
    if migration_backfill_ran:
        cards_state["all_endpoint_backfill_truncated"] = bool(migration_backfill_truncated)
        if not migration_backfill_truncated:
            cards_state["all_endpoint_migrated_at"] = utc_now_iso()

    state_updates["streams"]["cards"] = cards_state
    state_updates["trello_last_run_truncated"] = bool(budget_hit)

    if int(schema_sample_max) <= 0:
        emit_event("message", "schema.diff.disabled", stream="trello", level="info")
        schema_diff_for_report: Dict[str, Any] = {}
    else:
        current_snapshot = tracker.snapshot()
        current_hash = stable_hash(current_snapshot)
        prior_hash = state.get(SCHEMA_HASH_KEY)
        schema_diff = tracker.diff_against_prior()
        if schema_diff:
            for stream_name, changes in schema_diff.items():
                added_n = len((changes or {}).get("added") or {})
                removed_n = len((changes or {}).get("removed") or {})
                changed_n = len((changes or {}).get("changed_types") or {})
                seen_n = int(tracker.records_seen(stream_name))
                emit_event("message", "schema.diff", stream=stream_name, level="info", added_count=added_n, missing_count=removed_n)
                emit_event("count", "schema.records_seen", stream=stream_name, count=seen_n, level="info")
                emit_event(
                    "message",
                    "schema.diff.stream",
                    stream=stream_name,
                    level="info",
                    added=added_n,
                    removed=removed_n,
                    changed_types=changed_n,
                    sampled_records=seen_n,
                )
        else:
            emit_event("message", "schema.diff.none", stream="trello", level="info")
        if prior_hash != current_hash:
            state_updates[SCHEMA_STATE_KEY] = current_snapshot
            state_updates[SCHEMA_HASH_KEY] = current_hash
        schema_diff_for_report = schema_diff

    for stream_name, count in counter.all().items():
        emit_event("count", f"{stream_name}.final", stream=stream_name, count=count, level="info")

    def format_schema_diff_for_report(d: Dict[str, Any], limit: int = 25) -> str:
        if not d:
            return "Schema diff: none\n"
        lines: List[str] = ["Schema diff (per stream; removals are 'missing_in_sample'):"]
        for s in sorted(d.keys()):
            ch = d[s] or {}
            added = list((ch.get("added") or {}).keys())
            removed = list((ch.get("removed") or {}).keys())
            changed = list((ch.get("changed_types") or {}).keys())
            lines.append(f"- {s}: +{len(added)}  missing_in_sample:{len(removed)}  ~{len(changed)}")
            if added:
                lines.append(f"  + {', '.join(added[:limit])}{' ...' if len(added) > limit else ''}")
            if removed:
                lines.append(f"  missing {', '.join(removed[:limit])}{' ...' if len(removed) > limit else ''}")
            if changed:
                lines.append(f"  ~ {', '.join(changed[:limit])}{' ...' if len(changed) > limit else ''}")
        return "\n".join(lines) + "\n"

    report = (
        "Trello sync completed.\n"
        f"Tables: {', '.join(selected_streams)}\n"
        f"Records: {counter.all()}\n"
        f"{info_obj}\n"
        f"{format_schema_diff_for_report(schema_diff_for_report)}"
    )

    return report, None, state_updates
