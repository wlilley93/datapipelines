from __future__ import annotations

from typing import Any, Dict


def observed_schema() -> Dict[str, Any]:
    # Minimal schema snapshot; the payloads can be quite nested.
    #
    # NOTE:
    # - contacts/conversations may be fetched from API OR derived from messages.
    # - devices/team/departments are fetched directly from API endpoints.
    return {
        "streams": {
            "messages": {"primary_key": ["id"], "fields": {"id": "string"}},
            "conversations": {"primary_key": ["id"], "fields": {"id": "string"}},
            "contacts": {"primary_key": ["id"], "fields": {"id": "string"}},
            "devices": {"primary_key": ["id"], "fields": {"id": "string"}},
            "team": {"primary_key": ["id"], "fields": {"id": "string"}},
            "departments": {"primary_key": ["id"], "fields": {"id": "string"}},
        }
    }
