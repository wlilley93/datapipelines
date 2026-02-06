# Trello Connector Files Export

This document contains all files from the Trello connector directory.

## File: __init__.py

```python
from .connector import connector, TrelloConnector
from .pipeline import run_pipeline, test_connection

__all__ = ["TrelloConnector", "connector", "run_pipeline", "test_connection"]

```

## File: actions.py

```python
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from .constants import PAGE_LIMIT, TRELLO_API
from .paging import paged_list
from .time_utils import is_later_timestamp

def extract_changed_card_id(action: Dict[str, Any]) -> Optional[str]:
    data = action.get("data")
    if isinstance(data, dict):
        card = data.get("card")
        if isinstance(card, dict):
            cid = card.get("id")
            if isinstance(cid, str) and cid:
                return cid
        cid = data.get("cardId")
        if isinstance(cid, str) and cid:
            return cid
    cid = action.get("idCard")
    if isinstance(cid, str) and cid:
        return cid
    return None

def fetch_changed_card_ids_for_board(
    session,
    board_id: str,
    base_params: Dict[str, Any],
    *,
    since: str,
    max_ids_remaining_global: int,
    max_ids_this_board: int,
) -> Tuple[List[str], Optional[str], bool]:
    """
    Returns: (changed_card_ids_in_deterministic_order, max_action_date_seen, budget_hit_on_this_board)
    Ordering: most recent action date per card (desc), tie-break by card id (asc)
    """
    url = f"{TRELLO_API}/boards/{board_id}/actions"
    params = {
        **base_params,
        "filter": (
            "createCard,updateCard,commentCard,copyCard,moveCardFromBoard,moveCardToBoard,"
            "addMemberToCard,removeMemberFromCard,addAttachmentToCard,deleteAttachmentFromCard,"
            "updateCheckItemStateOnCard,addChecklistToCard,removeChecklistFromCard"
        ),
        "fields": "date,data,type",
    }
    card_latest_date: Dict[str, str] = {}
    max_date: Optional[str] = None
    budget_hit = False
    remaining_global = max(0, int(max_ids_remaining_global))
    remaining_board = max(0, int(max_ids_this_board))

    for action in paged_list(
        session,
        url,
        params=params,
        limit=PAGE_LIMIT,
        since=since,
        stream="cards",
        board_id=board_id,
        op="actions_change_feed",
    ):
        ad = action.get("date")
        if isinstance(ad, str) and ad:
            if max_date is None or is_later_timestamp(ad, max_date):
                max_date = ad
        cid = extract_changed_card_id(action)
        if not cid:
            continue
        existing = card_latest_date.get(cid)
        if existing is None:
            if remaining_global <= 0 or remaining_board <= 0:
                budget_hit = True
                break
            card_latest_date[cid] = ad if isinstance(ad, str) and ad else "1970-01-01T00:00:00+00:00"
            remaining_global -= 1
            remaining_board -= 1
        else:
            if isinstance(ad, str) and ad and is_later_timestamp(ad, existing):
                card_latest_date[cid] = ad

    changed_ids = sorted(card_latest_date.keys())
    changed_ids.sort(key=lambda x: card_latest_date.get(x, "1970-01-01T00:00:00+00:00"), reverse=True)
    return changed_ids, max_date, budget_hit

```

## File: cards.py

```python
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

```

## File: connector.py

```python
from __future__ import annotations
from typing import Any, Dict
from connectors.runtime.protocol import Connector, ReadResult, ReadSelection
from .pipeline import get_observed_schema, run_pipeline, test_connection

class TrelloConnector(Connector):
    name = "trello"

    def check(self, creds: Dict[str, Any]) -> str:
        return test_connection(creds)  # type: ignore[arg-type]

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates = run_pipeline(
            creds=creds,  # type: ignore[arg-type]
            schema=schema,
            state=state,
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=get_observed_schema(),
        )

def connector() -> Connector:
    return TrelloConnector()

```

## File: constants.py

```python
from __future__ import annotations
from typing import Final

TRELLO_API: Final[str] = "https://api.trello.com/1"
CONNECTOR_NAME: Final[str] = "trello"

# Pagination (safe for actions/checklists; NOT used for cards list endpoint)
PAGE_LIMIT: Final[int] = 1000

# Rate limiting / pacing
DEFAULT_HYDRATE_INTERVAL_MS: Final[int] = 150
MAX_HYDRATE_INTERVAL_MS: Final[int] = 5000

# Lookback for incremental sync (handles Trello eventual consistency)
DEFAULT_LOOKBACK_MINUTES: Final[int] = 60
MAX_LOOKBACK_MINUTES: Final[int] = 24 * 60

# Progress logging threshold
COUNT_LOG_INTERVAL: Final[int] = 250

# Delta hydration safety limits
DEFAULT_MAX_CHANGED_CARD_IDS: Final[int] = 50_000
MAX_MAX_CHANGED_CARD_IDS: Final[int] = 250_000

# Fairness: cap changed-card IDs per board
DEFAULT_PER_BOARD_CHANGED_CARD_CAP: Final[int] = 5_000
MAX_PER_BOARD_CHANGED_CARD_CAP: Final[int] = 50_000

# Optional periodic sweep
DEFAULT_ENABLE_PERIODIC_SWEEP: Final[bool] = True
DEFAULT_SWEEP_EVERY_HOURS: Final[int] = 168  # weekly
MAX_SWEEP_EVERY_HOURS: Final[int] = 24 * 180  # 180 days

# One-time backfill when migrating to /boards/{id}/cards/all
DEFAULT_ENABLE_ALL_CARDS_MIGRATION_BACKFILL: Final[bool] = True
DEFAULT_ALL_CARDS_MIGRATION_MAX_CARDS: Final[int] = 50_000
MAX_ALL_CARDS_MIGRATION_MAX_CARDS: Final[int] = 250_000
DEFAULT_ALL_CARDS_MIGRATION_PER_BOARD_CAP: Final[int] = 10_000
MAX_ALL_CARDS_MIGRATION_PER_BOARD_CAP: Final[int] = 50_000

# Card list filter (path segment): "all" includes open+closed
CARD_LIST_FILTER: Final[str] = "all"

# Card fetch field sets
CARD_FIELDS_RICH: Final[str] = "all"
CARD_FIELDS_LIGHT: Final[str] = (
    "id,name,desc,closed,due,dueComplete,dateLastActivity,idBoard,idList,idMembers,idLabels,labels,shortUrl,url,pos"
)

# Schema diff tracking
DEFAULT_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM: Final[int] = 2000
MAX_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM: Final[int] = 50_000

DEFAULT_SCHEMA_LIST_ITEMS_CAP: Final[int] = 50
MAX_SCHEMA_LIST_ITEMS_CAP: Final[int] = 500

# State keys
SCHEMA_STATE_KEY: Final[str] = "trello_schema_snapshot"
SCHEMA_HASH_KEY: Final[str] = "trello_schema_snapshot_hash"

```

## File: errors.py

```python
from __future__ import annotations

class SinceUnsupportedError(Exception):
    """Raised when Trello rejects the 'since' parameter for cards listing for a board."""

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

from time import perf_counter
from typing import Any, Dict, Optional, Tuple

from .events import debug, warn
from .utils_bridge import DEFAULT_TIMEOUT, request_json

def sanitize_params_for_logging(params: Dict[str, Any]) -> Dict[str, Any]:
    safe: Dict[str, Any] = {"param_keys": sorted(str(k) for k in params.keys())}
    for key in ("fields", "limit", "before", "since", "members", "attachments", "customFieldItems", "filter"):
        if key in params:
            val = params[key]
            safe[key] = val if isinstance(val, (str, int, float, bool)) else str(val)
    return safe

def get_response_details(err: Exception) -> Tuple[Optional[int], str]:
    resp = getattr(err, "response", None)
    status_code = getattr(resp, "status_code", None) if resp is not None else None
    text = ""
    try:
        text = getattr(resp, "text", "") or ""
    except Exception:
        text = ""
    return status_code if isinstance(status_code, int) else None, text

def request_json_logged(
    session,
    method: str,
    url: str,
    *,
    params: Dict[str, Any],
    timeout: int = DEFAULT_TIMEOUT,
    stream: Optional[str] = None,
    board_id: Optional[str] = None,
    op: Optional[str] = None,
) -> Any:
    t0 = perf_counter()
    debug(
        "http.request.start",
        stream=stream,
        op=op,
        method=method,
        url=url,
        board_id=board_id,
        **sanitize_params_for_logging(params),
    )

    try:
        data = request_json(session, method, url, params=params, timeout=timeout)
        elapsed_ms = int((perf_counter() - t0) * 1000)

        size_info: Dict[str, Any] = {}
        if isinstance(data, list):
            size_info["items_count"] = len(data)
        elif isinstance(data, dict):
            size_info["keys_count"] = len(data)
        else:
            size_info["type"] = type(data).__name__

        debug(
            "http.request.ok",
            stream=stream,
            op=op,
            elapsed_ms=elapsed_ms,
            board_id=board_id,
            **size_info,
        )
        return data

    except Exception as e:
        elapsed_ms = int((perf_counter() - t0) * 1000)
        debug(
            "http.request.error",
            stream=stream,
            op=op,
            elapsed_ms=elapsed_ms,
            board_id=board_id,
            error_type=type(e).__name__,
            error=str(e)[:1000],
        )
        raise

def as_list(data: Any, *, stream: str, op: str, board_id: Optional[str] = None) -> list[Any]:
    if isinstance(data, list):
        return data
    warn("http.payload.unexpected", stream=stream, op=op, board_id=board_id, expected="list", got=type(data).__name__)
    return []

def as_dict(data: Any, *, stream: str, op: str, board_id: Optional[str] = None) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    warn("http.payload.unexpected", stream=stream, op=op, board_id=board_id, expected="dict", got=type(data).__name__)
    return {}

```

## File: paging.py

```python
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from .constants import PAGE_LIMIT
from .events import debug, warn
from .http import as_list, request_json_logged
from .utils_bridge import DEFAULT_TIMEOUT

def paged_list(
    session,
    url: str,
    *,
    params: Dict[str, Any],
    limit: int = PAGE_LIMIT,
    before: Optional[str] = None,
    since: Optional[str] = None,
    stream: Optional[str] = None,
    board_id: Optional[str] = None,
    op: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterable[Dict[str, Any]]:
    """Paginate using Trello 'before' cursor (backward pagination)."""
    cursor = before
    page = 0

    while True:
        page += 1
        request_params = dict(params)
        request_params["limit"] = limit
        if cursor:
            request_params["before"] = cursor
        if since:
            request_params["since"] = since

        debug(
            "paging.page.start",
            stream=stream,
            op=op,
            page=page,
            board_id=board_id,
            limit=limit,
            cursor=str(cursor)[:24] if cursor else None,
        )

        data = request_json_logged(
            session,
            "GET",
            url,
            params=request_params,
            timeout=timeout,
            stream=stream,
            board_id=board_id,
            op=op or "paged_list",
        )
        items = as_list(data, stream=stream or "unknown", op=op or "paged_list", board_id=board_id)

        if not items:
            debug("paging.done.empty", stream=stream, op=op, page=page, board_id=board_id)
            break

        for item in items:
            if isinstance(item, dict):
                yield item

        if len(items) < limit:
            debug("paging.done.partial", stream=stream, op=op, page=page, board_id=board_id, returned=len(items))
            break

        last_item = items[-1]
        next_cursor = last_item.get("id") if isinstance(last_item, dict) else None

        if not next_cursor or next_cursor == cursor:
            warn("paging.done.stalled", stream=stream, op=op, page=page, board_id=board_id, returned=len(items))
            break

        cursor = next_cursor

```

## File: pipeline.py

```python
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

```

## File: schema_tracker.py

```python
from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

def _type_tag(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "other"

def _merge_type(a: Optional[str], b: str) -> str:
    if not a:
        return b
    if a == b:
        return a
    if {a, b}.issubset({"int", "float"}):
        return "number"
    if a == "null":
        return b
    if b == "null":
        return a
    return "mixed"

@dataclass
class SchemaCaps:
    max_records_per_stream: int
    list_items_cap: int

class SchemaTracker:
    """
    Lightweight shape/type observer with hard caps.
    Tracks per stream: { field_path -> type_tag } with bounded recursion.
    """

    def __init__(self, *, prior_snapshot: Optional[Dict[str, Any]], caps: SchemaCaps):
        self._caps = caps
        self._seen_records: Dict[str, int] = {}
        self._streams: Dict[str, Dict[str, str]] = {}
        self._prior = prior_snapshot if isinstance(prior_snapshot, dict) else {}

    def records_seen(self, stream: Optional[str] = None) -> Any:
        if stream is None:
            return dict(self._seen_records)
        return self._seen_records.get(stream, 0)

    def _can_observe_stream(self, stream: str) -> bool:
        return self._seen_records.get(stream, 0) < int(self._caps.max_records_per_stream)

    def observe(self, stream: str, record: Dict[str, Any]) -> None:
        if not isinstance(record, dict):
            return
        if not self._can_observe_stream(stream):
            return

        self._seen_records[stream] = self._seen_records.get(stream, 0) + 1
        smap = self._streams.setdefault(stream, {})
        self._observe_value(smap, prefix="", value=record)

    def _observe_value(self, smap: Dict[str, str], prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                if not isinstance(k, str) or not k:
                    continue
                path = f"{prefix}.{k}" if prefix else k
                smap[path] = _merge_type(smap.get(path), _type_tag(v))

                if isinstance(v, dict):
                    self._observe_value(smap, prefix=path, value=v)
                elif isinstance(v, list):
                    self._observe_list(smap, prefix=path, value=v)
            return

        if prefix:
            smap[prefix] = _merge_type(smap.get(prefix), _type_tag(value))

    def _observe_list(self, smap: Dict[str, str], prefix: str, value: list[Any]) -> None:
        elem_types: Optional[str] = None
        n = 0
        for item in value:
            n += 1
            if int(self._caps.list_items_cap) > 0 and n > int(self._caps.list_items_cap):
                break
            t = _type_tag(item)
            elem_types = _merge_type(elem_types, t)

            if isinstance(item, dict):
                for k, v in item.items():
                    if not isinstance(k, str) or not k:
                        continue
                    child_path = f"{prefix}[].{k}"
                    smap[child_path] = _merge_type(smap.get(child_path), _type_tag(v))
                    if isinstance(v, dict):
                        self._observe_value(smap, prefix=child_path, value=v)

        if elem_types:
            key = f"{prefix}[]"
            smap[key] = _merge_type(smap.get(key), elem_types)

    def snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for stream, fields in self._streams.items():
            out[stream] = {k: fields[k] for k in sorted(fields.keys())}
        return out

    def diff_against_prior(self) -> Dict[str, Any]:
        prior = self._prior if isinstance(self._prior, dict) else {}
        current = self.snapshot()
        all_streams = sorted(set(prior.keys()) | set(current.keys()))
        diff: Dict[str, Any] = {}

        for s in all_streams:
            p = prior.get(s) if isinstance(prior.get(s), dict) else {}
            c = current.get(s) if isinstance(current.get(s), dict) else {}

            p_keys = set(p.keys())
            c_keys = set(c.keys())

            added = {k: c[k] for k in sorted(c_keys - p_keys)}
            removed = {k: p[k] for k in sorted(p_keys - c_keys)}
            changed: Dict[str, Any] = {}
            for k in sorted(p_keys & c_keys):
                if p.get(k) != c.get(k):
                    changed[k] = {"from": p.get(k), "to": c.get(k)}

            if added or removed or changed:
                diff[s] = {"added": added, "removed": removed, "changed_types": changed}

        return diff

def stable_hash(obj: Any) -> str:
    data = json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(data).hexdigest()

```

## File: selection.py

```python
from __future__ import annotations
from typing import Any, Set

def normalize_selection(selection: Any) -> Set[str]:
    """
    Normalize selection to a set of stream names.
    Empty set means "all streams selected".
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

def is_selected(selected: Set[str], stream_name: str) -> bool:
    return len(selected) == 0 or stream_name in selected

```

## File: time_utils.py

```python
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()

def parse_iso(s: Any) -> Optional[datetime]:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def normalize_cursor_iso(value: Any) -> Optional[str]:
    dt = parse_iso(value)
    return dt.isoformat() if dt else None

def is_later_timestamp(a: str, b: str) -> bool:
    dt_a = parse_iso(a)
    dt_b = parse_iso(b)
    if dt_a and dt_b:
        return dt_a > dt_b
    return str(a) > str(b)

def clamp_int(val: Any, *, default: int, lo: int, hi: int) -> int:
    try:
        n = int(val)
    except Exception:
        return default
    return max(lo, min(hi, n))

def compute_effective_since_from_cursor(cursor: str, lookback_minutes: int) -> Optional[str]:
    dt = parse_iso(cursor)
    if not dt:
        return None
    adjusted = dt - timedelta(minutes=int(lookback_minutes))
    return adjusted.astimezone(timezone.utc).isoformat()

```

## File: utils_bridge.py

```python
from __future__ import annotations

# Re-export shared utilities from top-level connectors.utils
from connectors.utils import DEFAULT_TIMEOUT, add_metadata, request_json, requests_retry_session  # type: ignore[F401]

```

