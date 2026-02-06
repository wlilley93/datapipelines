# connectors/hubspot/normalization.py
from __future__ import annotations

from typing import Any, Dict


def normalize_object_record(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    HubSpot returns objects like:
      {id: "...", properties: {...}, createdAt, updatedAt, archived}
    We flatten the properties into the top-level row for dlt.
    """
    out: Dict[str, Any] = {}
    if not isinstance(obj, dict):
        return out

    out["id"] = obj.get("id")
    out["_archived"] = obj.get("archived")

    # keep timestamps in snake_case
    out["created_at"] = obj.get("createdAt")
    out["updated_at"] = obj.get("updatedAt")

    props = obj.get("properties") or {}
    if isinstance(props, dict):
        out.update(props)

    # marker used by some hubspot endpoints indicating partial associations
    if "associations" in obj and obj.get("associations") is None:
        out["_associations_incomplete"] = True

    return out
