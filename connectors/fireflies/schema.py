from __future__ import annotations

from typing import Any, Dict


def observed_schema() -> Dict[str, Any]:
    # Minimal schema snapshot; the payloads can be quite nested.
    # Pin a known “sometimes-empty” nested column so dlt doesn’t warn when it’s absent in a batch.
    return {
        "streams": {
            "messages": {
                "primary_key": ["id"],
                "fields": {
                    "id": "string",
                    # dlt sometimes materializes this nested path as a column even if no rows contain it
                    # in the current load; pin it to avoid type inference warnings.
                    "media__file__preview__image": "json",
                },
            },
            "conversations": {"primary_key": ["id"], "fields": {"id": "string"}},
            "contacts": {"primary_key": ["id"], "fields": {"id": "string"}},
        }
    }


def get_observed_schema() -> Dict[str, Any]:
    """
    Adapter for the Connector API.

    The FirefliesConnector calls this name; we delegate to `observed_schema`
    to preserve backwards compatibility with any earlier callers.
    """
    return observed_schema()
