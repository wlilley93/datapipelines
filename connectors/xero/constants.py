from __future__ import annotations

TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"
ACCOUNTING_BASE = "https://api.xero.com/api.xro/2.0"

_CONNECTOR_NAME = "xero"

# Pacing (small sleeps reduce 429s on “tight” tenants); set low to increase throughput
DEFAULT_REQUEST_PACE_SECONDS = 0.01
MAX_REQUEST_PACE_SECONDS = 2.0

# 429 retry behaviour
DEFAULT_MAX_429_RETRIES = 10
DEFAULT_BACKOFF_INITIAL_SECONDS = 1.0
DEFAULT_BACKOFF_MAX_SECONDS = 60.0

# Pagination caps (safety)
DEFAULT_MAX_PAGES_PER_STREAM = 10000

# Cursor lookback (eventual consistency)
DEFAULT_LOOKBACK_MINUTES = 60
MAX_LOOKBACK_MINUTES = 24 * 60
