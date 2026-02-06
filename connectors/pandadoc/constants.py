from __future__ import annotations

PROD_BASE = "https://api.pandadoc.com"
SANDBOX_BASE = "https://api-sandbox.pandadoc.com"

CONNECTOR_NAME = "pandadoc"

# API paths
DOCS_LIST_PATH = "/public/v1/documents"
DOC_DETAILS_PATH_TMPL = "/public/v1/documents/{id}/details"

# Paging defaults
DEFAULT_PAGE_SIZE = 100

# Rate limit / pacing (kept from original behavior)
SANDBOX_SLEEP_SECONDS = 6.2  # ~10 rpm
PROD_DETAILS_SLEEP_SECONDS = 0.12
PROD_LIST_SLEEP_SECONDS = 0.03

# 429 retry
DEFAULT_MAX_429_RETRIES = 8
MAX_BACKOFF_SECONDS = 30

# State keys (keep legacy compatibility)
STATE_MODIFIED_FROM = "pandadoc_modified_from"   # preferred / current
STATE_LAST_SUCCESS_AT = "last_success_at"
STATE_START_DATE = "start_date"
