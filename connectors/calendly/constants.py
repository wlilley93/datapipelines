from __future__ import annotations

import os

CONNECTOR_NAME = "calendly"

DEFAULT_BASE_URL = os.getenv("CALENDLY_BASE_URL", "https://api.calendly.com").rstrip("/")

# Calendly list endpoints typically max out at 100 per page.
DEFAULT_PAGE_SIZE = max(1, min(100, int(os.getenv("CALENDLY_PAGE_SIZE", "100"))))

# Backfill / incremental controls (scheduled events)
DEFAULT_BACKFILL_START_AT = os.getenv("CALENDLY_BACKFILL_START_AT", "2010-01-01T00:00:00Z")
DEFAULT_BACKFILL_WINDOW_DAYS = max(1, min(366, int(os.getenv("CALENDLY_BACKFILL_WINDOW_DAYS", "30"))))
DEFAULT_LOOKBACK_DAYS = max(0, min(3650, int(os.getenv("CALENDLY_LOOKBACK_DAYS", "14"))))
DEFAULT_FUTURE_DAYS = max(0, min(3650, int(os.getenv("CALENDLY_FUTURE_DAYS", "365"))))

# HTTP hardening
HTTP_MAX_RETRIES = max(0, min(12, int(os.getenv("CALENDLY_HTTP_MAX_RETRIES", "8"))))
MAX_RETRY_AFTER_SECONDS = max(1, min(600, int(os.getenv("CALENDLY_MAX_RETRY_AFTER_SECONDS", "120"))))

# Client-side pacing to reduce 429s (optional)
MIN_INTERVAL_SECONDS = float(os.getenv("CALENDLY_MIN_INTERVAL_SECONDS", "0") or 0.0)

# Optional API version header (leave empty to omit)
CALENDLY_API_VERSION = os.getenv("CALENDLY_API_VERSION", "").strip()

# Optional concurrency for per-event hydration (invitees/cancellations)
INVITEE_WORKERS = max(1, min(32, int(os.getenv("CALENDLY_INVITEE_WORKERS", "8"))))

