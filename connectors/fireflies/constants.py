from __future__ import annotations

FIREFLIES_GQL = "https://api.fireflies.ai/graphql"
CONNECTOR_NAME = "fireflies"

# Pagination (Fireflies docs commonly enforce limit <= 50)
DEFAULT_PAGE_SIZE = 50
MIN_PAGE_SIZE = 5

# Rate limiting / pacing
BASE_PAGE_DELAY_SECONDS = 0.10
RATE_LIMIT_BACKOFF_MULTIPLIER = 2.0
MAX_PAGE_DELAY_SECONDS = 5.0

# Safety limits
DEFAULT_MAX_PAGES = 200_000
DEFAULT_MAX_RECORDS = 2_000_000

# Incremental sync
DEFAULT_LOOKBACK_MINUTES = 60
MAX_LOOKBACK_MINUTES = 24 * 60

# Deduplication window size (rolling)
DEDUPE_WINDOW_SIZE = 10_000

# Logging frequency
LOG_EVERY_N_PAGES = 25

# Checkpoint logging (your request)
UNIQUE_CHECKPOINT_EVERY_N = 5_000

# Guardrails: keep an exact run-wide seen-id set until this cap; beyond that we switch to window-only.
GLOBAL_SEEN_IDS_SOFT_CAP = 250_000

# Content-fingerprint dedupe: run-wide until cap, then window-only (protect memory)
GLOBAL_FINGERPRINTS_SOFT_CAP = 250_000

# Meeting-key collision tracking
MEETING_COLLISION_TOP_N = 5

# State keys
STREAMS_KEY = "streams"
TRANSCRIPTS_STREAM = "transcripts"
CURSOR_DATE_KEY = "cursor_date"
LEGACY_CURSOR_KEY = "fireflies_last_date"
