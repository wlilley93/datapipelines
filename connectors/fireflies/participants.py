from __future__ import annotations

import hashlib
from typing import Any, Dict, Generator, List, Optional

from .utils_bridge import add_metadata


def generate_participant_key(
    transcript_id: str,
    participant_id: Optional[str],
    email: Optional[str],
    name: Optional[str],
) -> str:
    if participant_id:
        return str(participant_id)
    if email:
        return email
    if name:
        composite = f"{transcript_id}:{name}"
        return hashlib.sha256(composite.encode()).hexdigest()[:16]
    return f"{transcript_id}:unknown"


def parse_participant_item(item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, str):
        if "@" in item:
            return {"email": item, "name": None, "id": None, "role": None}
        return {"name": item, "email": None, "id": None, "role": None}

    if isinstance(item, dict):
        if "node" in item and isinstance(item["node"], dict):
            item = item["node"]

        return {
            "id": item.get("id"),
            "email": item.get("email") or item.get("mail") or item.get("emailAddress"),
            "name": item.get("name") or item.get("displayName") or item.get("fullName"),
            "role": item.get("role") or item.get("type"),
        }

    return None


def extract_participants(transcript_id: str, raw: Any) -> Generator[Dict[str, Any], None, None]:
    items: List[Any] = []

    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        nested = raw.get("results") or raw.get("edges") or raw.get("nodes")
        items = nested if isinstance(nested, list) else [raw]
    elif isinstance(raw, str):
        items = [raw]
    else:
        return

    for item in items:
        participant = parse_participant_item(item)
        if participant is None:
            continue

        pid = participant.get("id")
        email = participant.get("email")
        name = participant.get("name")
        key = generate_participant_key(transcript_id, pid, email, name)

        yield add_metadata(
            {
                "transcript_id": transcript_id,
                "participant_key": key,
                "participant_id": pid,
                "email": email,
                "name": name,
                "role": participant.get("role"),
            },
            "fireflies",
        )
