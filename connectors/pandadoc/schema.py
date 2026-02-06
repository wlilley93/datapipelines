from __future__ import annotations

from typing import Any, Dict


def observed_schema() -> Dict[str, Any]:
    return {
        "streams": {
            "documents": {
                "primary_key": ["id"],
                "fields": {
                    "id": "string",
                    "name": "string",
                    "status": "string",
                    "date_created": "string",
                    "date_modified": "string",
                    "expiration_date": "string",
                    "version": "number",
                    "uuid": "string",
                    "api_links": "array",
                    "ui_link_guess": "string",
                    "_dlt_load_time": "string",
                    "_dlt_source": "string",
                },
            },
            "recipients": {
                "primary_key": ["document_id", "recipient_key"],
                "fields": {
                    "document_id": "string",
                    "recipient_key": "string",
                    "recipient_id": "string",
                    "email": "string",
                    "first_name": "string",
                    "last_name": "string",
                    "role": "string",
                    "signing_order": "number",
                    "status": "string",
                    "completed_at": "string",
                    "metadata": "object",
                    "_dlt_load_time": "string",
                    "_dlt_source": "string",
                },
            },
            "tokens": {
                "primary_key": ["document_id", "token_name"],
                "fields": {
                    "document_id": "string",
                    "token_name": "string",
                    "value": "string",
                    "raw": "string",
                    "_dlt_load_time": "string",
                    "_dlt_source": "string",
                },
            },
        }
    }
