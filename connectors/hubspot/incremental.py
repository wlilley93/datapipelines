# connectors/hubspot/incremental.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def should_do_full(*, state_cursor: Optional[str]) -> bool:
    return not state_cursor


def make_modified_since_filter(cursor: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    HubSpot search filters use ms timestamps typically; we store cursor as ms string.
    """
    if not cursor:
        return None

    try:
        ms = int(cursor)
    except Exception:
        return None

    # hs_lastmodifieddate is commonly available across CRM objects.
    return {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "hs_lastmodifieddate",
                        "operator": "GTE",
                        "value": str(ms),
                    }
                ]
            }
        ]
    }


def advance_cursor_from_results(
    cursor: Optional[str],
    results: list[Dict[str, Any]],
    *,
    property_name: str = "hs_lastmodifieddate",
) -> str:
    """
    Returns an updated ms cursor based on max(hs_lastmodifieddate) observed.
    Falls back to now if no data.
    """
    best = 0
    if cursor and str(cursor).isdigit():
        best = int(cursor)

    for r in results:
        props = r.get("properties") or {}
        v = props.get(property_name)
        if v is None:
            continue
        try:
            # hubspot sends ms string for hs_lastmodifieddate in properties
            best = max(best, int(v))
        except Exception:
            continue

    if best <= 0:
        best = _now_ms()
    return str(best)
