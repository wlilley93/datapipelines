from __future__ import annotations

from typing import Any, Dict, List

STREAM_FIELDS: Dict[str, List[str]] = {
    "contacts": [
        "id",
        "type",
        "created_at",
        "updated_at",
        "email",
        "name",
        "role",
        "custom_attributes",
    ],
    "companies": [
        "id",
        "name",
        "created_at",
        "updated_at",
        "monthly_spend",
        "custom_attributes",
    ],
    "conversations": [
        "id",
        "state",
        "created_at",
        "updated_at",
        "waiting_since",
        "priority",
        "source",
    ],
    "conversation_parts": [
        "id",
        "conversation_id",
        "part_type",
        "body",
        "created_at",
        "updated_at",
        "author",
        "attachments",
    ],
    "tickets": [
        "id",
        "created_at",
        "updated_at",
        "ticket_state",
        "ticket_attributes",
    ],
}


def get_observed_schema() -> Dict[str, Any]:
    streams = {}
    for stream, fields in STREAM_FIELDS.items():
        properties = {f: {"type": "string"} for f in fields}
        if "updated_at" in properties:
            properties["updated_at"] = {"type": "integer"}
        if "created_at" in properties:
            properties["created_at"] = {"type": "integer"}
        if "custom_attributes" in properties:
            properties["custom_attributes"] = {"type": "object"}
        if "monthly_spend" in properties:
            properties["monthly_spend"] = {"type": "number"}
        if "author" in properties:
            properties["author"] = {"type": "object"}
        if "attachments" in properties:
            properties["attachments"] = {"type": "array"}

        properties["raw"] = {"type": "object"}

        streams[stream] = {
            "primary_key": ["id"],
            "properties": properties,
        }

    return {"streams": streams}
