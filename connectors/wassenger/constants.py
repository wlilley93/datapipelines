from __future__ import annotations

CONNECTOR_NAME = "wassenger"

# NOTE: Override via creds["base_url"] or state["wassenger_base_url"].
DEFAULT_BASE_URL = "https://api.wassenger.com/v1"

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES_PER_STREAM = 10000

# pacing / retry
DEFAULT_REQUEST_PACE_SECONDS = 0.02
MAX_REQUEST_PACE_SECONDS = 2.0

DEFAULT_MAX_429_RETRIES = 10
DEFAULT_BACKOFF_INITIAL_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 60.0

# State keys
STATE_GLOBAL_CURSOR_FALLBACK = "last_success_at"  # used only if stream cursor missing
STATE_START_DATE = "start_date"

# If we manage to discover a device id, we persist it here for future runs.
STATE_DEVICE_ID = "wassenger_device_id"

# Canonical streams (names) this connector exposes.
STREAM_NAMES = ("messages", "contacts", "conversations", "devices", "team", "departments", "contacts_direct", "analytics", "campaigns", "chat_participants", "chat_sync", "files", "device_autoassign", "device_autoreplies", "device_catalog", "device_channels", "device_groups", "device_health", "device_invoices", "device_labels", "device_meeting_links", "device_profile", "device_queue", "device_quick_replies", "device_scan", "device_team", "waba_prices", "waba_templates", "webhooks", "webhook_logs")

# Base stream config (cursor/collection may be adjusted after endpoint resolution).
# Include streams that are most likely to be available on most Wassenger accounts.
DEFAULT_STREAMS = {
    "messages": {"cursor_field": "createdAt", "collection_key": "data"},
    "devices": {"cursor_field": "updatedAt", "collection_key": "data"},
    "conversations": {"cursor_field": "createdAt", "collection_key": "data"},  # derived from messages
    "contacts": {"cursor_field": "createdAt", "collection_key": "data"},      # derived from messages
    # Optional streams (available but not enabled by default)
    # These can be enabled by including them in state["wassenger_streams"] config
    # "team", "departments", "contacts_direct" and others can be added to
    # state["wassenger_streams"] if needed
}

# Candidate endpoints we will probe in order (templated with {deviceId} where relevant).
# NOTE: Wassenger's REST API surface varies by plan/account; probes decide what's real.
STREAM_ENDPOINT_CANDIDATES = {
    "messages": [
        "/messages",
        # Some deployments scope messages by device:
        "/devices/{deviceId}/messages",
        "/device/{deviceId}/messages",
        "/chat/{deviceId}/messages",
    ],
    "devices": [
        "/devices",
        "/device",
    ],
    "conversations": [
        # Common naming patterns seen in 3rd party integrations
        "/chats",
        "/chat/{deviceId}/chats",
        "/devices/{deviceId}/chats",
        "/device/{deviceId}/chats",
        "/chats?deviceId={deviceId}",
        "/chats?device={deviceId}",
        # Sometimes "sync" endpoints exist
        "/chat/{deviceId}/chats/sync",
        "/devices/{deviceId}/chats/sync",
    ],
    "contacts": [
        "/contacts",
        "/addressbook",
        "/chat/{deviceId}/contacts",
        "/devices/{deviceId}/contacts",
        "/device/{deviceId}/contacts",
        "/chat/{deviceId}/addressbook",
        "/devices/{deviceId}/addressbook",
        "/contacts?deviceId={deviceId}",
        "/contacts?device={deviceId}",
        "/addressbook?deviceId={deviceId}",
        "/addressbook?device={deviceId}",
        "/chat/{deviceId}/contacts/sync",
        "/devices/{deviceId}/contacts/sync",
    ],
    "team": [
        "/team",
        "/users",
        "/members",
        "/team/users",
        "/team/members",
        "/agents",
    ],
    "departments": [
        "/departments",
        "/teams",
        "/team/departments",
    ],
    "analytics": [
        "/analytics",
    ],
    "campaigns": [
        "/campaigns",
        "/campaigns/{campaignId}",
    ],
    "chat_participants": [
        "/chat/{deviceId}/chats/{chatWid}/participants",
    ],
    "chat_sync": [
        "/chat/{deviceId}/chats/{chatWid}/sync",
        "/devices/{deviceId}/chats/sync",
    ],
    "files": [
        "/files",
        "/chat/{deviceId}/files",
        "/devices/{deviceId}/files",
    ],
    "device_autoassign": [
        "/devices/{deviceId}/autoassign",
    ],
    "device_autoreplies": [
        "/devices/{deviceId}/autoreplies",
    ],
    "device_catalog": [
        "/devices/{deviceId}/catalog",
    ],
    "device_channels": [
        "/devices/{deviceId}/channels",
    ],
    "device_groups": [
        "/devices/{deviceId}/groups",
    ],
    "device_health": [
        "/devices/{deviceId}/health",
    ],
    "device_invoices": [
        "/devices/{deviceId}/invoices",
    ],
    "device_labels": [
        "/devices/{deviceId}/labels",
    ],
    "device_meeting_links": [
        "/devices/{deviceId}/meeting-links",
    ],
    "device_profile": [
        "/devices/{deviceId}/profile",
    ],
    "device_queue": [
        "/devices/{deviceId}/queue",
    ],
    "device_quick_replies": [
        "/devices/{deviceId}/quickReplies",
    ],
    "device_scan": [
        "/devices/{deviceId}/scan",
    ],
    "device_team": [
        "/devices/{deviceId}/team",
    ],
    "waba_prices": [
        "/waba/prices",
    ],
    "waba_templates": [
        "/waba/templates",
    ],
    "webhooks": [
        "/webhooks",
    ],
    "webhook_logs": [
        "/webhooks/{webhookId}/logs",
    ],
}
