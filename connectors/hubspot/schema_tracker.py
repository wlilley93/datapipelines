# connectors/hubspot/schema_tracker.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class ObjectSchema:
    name: str
    object_type_id: str
    properties: List[Dict[str, Any]]


def extract_object_schemas(payload: Dict[str, Any]) -> List[ObjectSchema]:
    """
    Payload from GET /crm/v3/schemas contains 'results' list.
    """
    results = payload.get("results") or []
    out: List[ObjectSchema] = []
    for s in results:
        if not isinstance(s, dict):
            continue
        name = s.get("name") or s.get("fullyQualifiedName") or s.get("labels", {}).get("singular") or ""
        object_type_id = s.get("objectTypeId") or ""
        props = s.get("properties") or []
        if isinstance(props, list) and name and object_type_id:
            out.append(ObjectSchema(name=str(name), object_type_id=str(object_type_id), properties=[p for p in props if isinstance(p, dict)]))
    return out


def split_standard_vs_custom(schemas: List[ObjectSchema]) -> Tuple[List[ObjectSchema], List[ObjectSchema]]:
    """
    HubSpot custom objectTypeIds typically look like "2-1234567".
    """
    std: List[ObjectSchema] = []
    custom: List[ObjectSchema] = []
    for s in schemas:
        if str(s.object_type_id).startswith("2-"):
            custom.append(s)
        else:
            std.append(s)
    return std, custom
