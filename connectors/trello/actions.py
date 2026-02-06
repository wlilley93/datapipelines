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
