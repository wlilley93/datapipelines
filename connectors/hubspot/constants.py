# connectors/hubspot/constants.py
from __future__ import annotations

HUBSPOT_BASE_URL = "https://api.hubapi.com"

# Core objects we try by default. Custom objects are discovered separately via /crm/v3/schemas
DEFAULT_OBJECTS = [
    "contacts",
    "companies",
    "deals",
    "line_items",
    "products",
    "tickets",
    "calls",
    "emails",
    "meetings",
    "notes",
    "tasks",
]

# HubSpot search max page size is typically 100 (varies by endpoint), keep conservative.
DEFAULT_PAGE_LIMIT = 100

# timeouts: (connect, read)
DEFAULT_TIMEOUT = (10, 60)

# If an account has huge property sets, materialize them as text unless we can do better.
DEFAULT_PROPERTY_DATATYPE = "text"
