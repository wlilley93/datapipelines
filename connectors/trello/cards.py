from __future__ import annotations
import time
from typing import Any, Dict, Iterable, List, Optional, Set
from .constants import (
    CARD_FIELDS_LIGHT,
    CARD_FIELDS_RICH,
    CARD_LIST_FILTER,
    DEFAULT_HYDRATE_INTERVAL_MS,
    TRELLO_API,
)
from .errors import SinceUnsupportedError
from .events import error, info, warn
from .http import as_dict, as_list, get_response_details, request_json_logged
from .utils_bridge import DEFAULT_TIMEOUT

def is_too_many_checklists_error(err: Exception) -> bool:
    if "Too many checklists" in str(err):
        return True
    status, text = get_response_details(err)
    return status == 403 and "Too many checklists" in (text or "")[:2000]

def is_since_unsupported_for_cards(err: Exception, *, url: str, params: Dict[str, Any]) -> bool:
    if "/boards/" not in url or "/cards/" not in url:
        return False
    if not params.get("since"):
        return False
    status, text = get_response_details(err)
    if status != 400:
        return False
    lowered = (text or "").lower().strip()
    if lowered:
        return "since" in lowered
    return True

def pace_request(min_interval_ms: int, last_call_at: float) -> float:
    if min_interval_ms <= 0:
        return time.monotonic()
    now = time.monotonic()
    elapsed = now - last_call_at
    wait_seconds = (min_interval_ms / 1000.0) - elapsed
    if wait_seconds > 0:
        time.sleep(wait_seconds)
    return time.monotonic()

def cards_list_url(board_id: str, *, card_filter: str = CARD_LIST_FILTER) -> str:
    return f"{TRELLO_API}/boards/{board_id}/cards/{card_filter}"

def build_card_params_rich(base_params: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **base_params,
        "fields": "all",
        "attachments": "true",
        "attachment_fields": "all",
        "members": "true",
        "member_fields": "all",
        "stickers": "true",
        "customFieldItems": "true",
    }

def build_card_params_light(base_params: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **base_params,
        "fields": CARD_FIELDS_LIGHT,
        "members": "false",
        "attachments": "false",
        "stickers": "false",
    }

def list_board_cards_once(
    session,
    board_id: str,
    *,
    params: Dict[str, Any],
    since: Optional[str],
    stream: str,
    op: str,
    card_filter: str = CARD_LIST_FILTER,
) -> List[Dict[str, Any]]:
    url = cards_list_url(board_id, card_filter=card_filter)
    request_params = dict(params)
    request_params.pop("filter", None)
    if since:
        request_params["since"] = since
    try:
        data = request_json_logged(
            session,
            "GET",
            url,
            params=request_params,
            timeout=DEFAULT_TIMEOUT,
            stream=stream,
            board_id=board_id,
            op=op,
        )
    except Exception as e:
        if is_since_unsupported_for_cards(e, url=url, params=request_params):
            raise SinceUnsupportedError("Trello rejected 'since' for cards listing") from e
        raise
    cards = as_list(data, stream=stream, op=op, board_id=board_id)
    return [c for c in cards if isinstance(c, dict)]

def fetch_single_card(
    session,
    card_id: str,
    *,
    base_params: Dict[str, Any],
    fields: str,
    stream: Optional[str] = None,
    board_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    url = f"{TRELLO_API}/cards/{card_id}"
    params = {
        **base_params,
        "fields": fields,
        "members": "true",
        "member_fields": "all",
        "attachments": "true",
        "attachment_fields": "all",
        "stickers": "true",
        "customFieldItems": "true",
    }
    data = request_json_logged(
        session,
        "GET",
        url,
        params=params,
        timeout=DEFAULT_TIMEOUT,
        stream=stream,
        board_id=board_id,
        op="card_fetch",
    )
    result = as_dict(data, stream=stream or "cards", op="card_fetch", board_id=board_id)
    return result or None

def hydrate_card_with_fallback(
    session,
    card_id: str,
    *,
    base_params: Dict[str, Any],
    board_id: str,
) -> Optional[Dict[str, Any]]:
    try:
        return fetch_single_card(
            session,
            card_id,
            base_params=base_params,
            fields=CARD_FIELDS_RICH,
            stream="cards",
            board_id=board_id,
        )
    except Exception as e:
        if is_too_many_checklists_error(e):
            warn("cards.hydrate.403", stream="cards", board_id=board_id, card_id=card_id)
            try:
                return fetch_single_card(
                    session,
                    card_id,
                    base_params=base_params,
                    fields=CARD_FIELDS_LIGHT,
                    stream="cards",
                    board_id=board_id,
                )
            except Exception as inner:
                warn("cards.hydrate.failed", stream="cards", board_id=board_id, card_id=card_id, error=str(inner)[:500])
                return None
        warn("cards.hydrate.failed", stream="cards", board_id=board_id, card_id=card_id, error=str(e)[:500])
        return None

def yield_board_cards_with_fallback(
    session,
    board_id: str,
    board: Dict[str, Any],
    base_params: Dict[str, Any],
    hydrate_interval_ms: int,
    *,
    allow_since: bool,
    since: Optional[str],
    is_full_scan: bool,
) -> Iterable[Dict[str, Any]]:
    board_name = board.get("name", "")
    info(
        "cards.board.start",
        stream="cards",
        board_id=board_id,
        board_name=board_name,
        closed=board.get("closed"),
        allow_since=allow_since,
        mode="full_scan" if is_full_scan else "sweep",
        card_filter=CARD_LIST_FILTER,
    )

    # Stage 1: rich list
    try:
        cards = list_board_cards_once(
            session,
            board_id,
            params=build_card_params_rich(base_params),
            since=(since if allow_since else None),
            stream="cards",
            op="cards_list_rich",
        )
        for c in cards:
            yield c
        info("cards.board.done", stream="cards", board_id=board_id, stage=1, mode="rich")
        return
    except SinceUnsupportedError:
        warn("cards.since.unsupported", stream="cards", board_id=board_id)
        raise
    except Exception as e:
        if not is_too_many_checklists_error(e):
            error("cards.board.error", stream="cards", board_id=board_id, stage=1, error=str(e)[:1000])
            raise
        warn("cards.board.403", stream="cards", board_id=board_id, stage=1)

    # Stage 2: light list
    try:
        cards = list_board_cards_once(
            session,
            board_id,
            params=build_card_params_light(base_params),
            since=(since if allow_since else None),
            stream="cards",
            op="cards_list_light",
        )
        for c in cards:
            yield c
        info("cards.board.done", stream="cards", board_id=board_id, stage=2, mode="light")
        return
    except SinceUnsupportedError:
        warn("cards.since.unsupported", stream="cards", board_id=board_id)
        raise
    except Exception as e:
        if not is_too_many_checklists_error(e):
            error("cards.board.error", stream="cards", board_id=board_id, stage=2, error=str(e)[:1000])
            raise
        warn("cards.board.403", stream="cards", board_id=board_id, stage=2)

    # Stage 3: hydration (full scan only)
    if not is_full_scan:
        warn("cards.sweep.skipped.after_403", stream="cards", board_id=board_id, reason="403_in_sweep_mode")
        return
    warn("cards.board.fallback", stream="cards", board_id=board_id, stage=3, mode="hydration", interval_ms=hydrate_interval_ms)
    ids = list_board_cards_once(
        session,
        board_id,
        params={**base_params, "fields": "id"},
        since=None,
        stream="cards",
        op="cards_list_ids",
    )
    card_ids = [c.get("id") for c in ids if isinstance(c.get("id"), str) and c.get("id")]
    last_call_at = 0.0
    interval = int(hydrate_interval_ms or DEFAULT_HYDRATE_INTERVAL_MS)
    for cid in card_ids:
        last_call_at = pace_request(interval, last_call_at)
        card = hydrate_card_with_fallback(session, cid, base_params=base_params, board_id=board_id)
        if card:
            yield card
    info("cards.board.done", stream="cards", board_id=board_id, stage=3, mode="hydration")

def yield_board_cards_light_list_only(
    session,
    board_id: str,
    board: Dict[str, Any],
    base_params: Dict[str, Any],
) -> Iterable[Dict[str, Any]]:
    board_name = board.get("name", "")
    info(
        "cards.migration_backfill.board.start",
        stream="cards",
        board_id=board_id,
        board_name=board_name,
        card_filter=CARD_LIST_FILTER,
        mode="light_list_only",
    )
    try:
        cards = list_board_cards_once(
            session,
            board_id,
            params=build_card_params_light(base_params),
            since=None,
            stream="cards",
            op="cards_migration_backfill_light_list",
        )
        for c in cards:
            yield c
        info("cards.migration_backfill.board.done", stream="cards", board_id=board_id, returned=len(cards))
    except Exception as e:
        warn("cards.migration_backfill.board.failed", stream="cards", board_id=board_id, error=str(e)[:500])
