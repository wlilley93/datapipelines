# connectors/hubspot/properties.py
from __future__ import annotations

from typing import Any, Dict, List

from .constants import DEFAULT_PROPERTY_DATATYPE


def plan_properties(schema_properties: List[Dict[str, Any]]) -> List[str]:
    """
    Returns the property names we should request on object fetches.
    """
    names: List[str] = []
    for p in schema_properties:
        n = p.get("name")
        if isinstance(n, str) and n:
            names.append(n)
    # Keep stable order, no duplicates
    seen = set()
    out: List[str] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def build_dlt_columns(schema_properties: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Big win: provides type hints so columns materialize even if this run yields sparse data.
    We keep everything as text by default; it's safe and avoids inference issues.
    """
    cols: Dict[str, Dict[str, Any]] = {}
    for p in schema_properties:
        name = p.get("name")
        if not isinstance(name, str) or not name:
            continue
        cols[name] = {"data_type": DEFAULT_PROPERTY_DATATYPE}

    # Always include hubspot keys we rely on
    cols.setdefault("hs_object_id", {"data_type": "text"})
    cols.setdefault("hs_lastmodifieddate", {"data_type": "bigint"})
    cols.setdefault("createdate", {"data_type": "bigint"})
    return cols
