from __future__ import annotations
from typing import Final

TRELLO_API: Final[str] = "https://api.trello.com/1"
CONNECTOR_NAME: Final[str] = "trello"

# Pagination (safe for actions/checklists; NOT used for cards list endpoint)
PAGE_LIMIT: Final[int] = 1000

# Rate limiting / pacing
DEFAULT_HYDRATE_INTERVAL_MS: Final[int] = 150
MAX_HYDRATE_INTERVAL_MS: Final[int] = 5000

# Lookback for incremental sync (handles Trello eventual consistency)
DEFAULT_LOOKBACK_MINUTES: Final[int] = 60
MAX_LOOKBACK_MINUTES: Final[int] = 24 * 60

# Progress logging threshold
COUNT_LOG_INTERVAL: Final[int] = 250

# Delta hydration safety limits
DEFAULT_MAX_CHANGED_CARD_IDS: Final[int] = 50_000
MAX_MAX_CHANGED_CARD_IDS: Final[int] = 250_000

# Fairness: cap changed-card IDs per board
DEFAULT_PER_BOARD_CHANGED_CARD_CAP: Final[int] = 5_000
MAX_PER_BOARD_CHANGED_CARD_CAP: Final[int] = 50_000

# Optional periodic sweep
DEFAULT_ENABLE_PERIODIC_SWEEP: Final[bool] = True
DEFAULT_SWEEP_EVERY_HOURS: Final[int] = 168  # weekly
MAX_SWEEP_EVERY_HOURS: Final[int] = 24 * 180  # 180 days

# One-time backfill when migrating to /boards/{id}/cards/all
DEFAULT_ENABLE_ALL_CARDS_MIGRATION_BACKFILL: Final[bool] = True
DEFAULT_ALL_CARDS_MIGRATION_MAX_CARDS: Final[int] = 50_000
MAX_ALL_CARDS_MIGRATION_MAX_CARDS: Final[int] = 250_000
DEFAULT_ALL_CARDS_MIGRATION_PER_BOARD_CAP: Final[int] = 10_000
MAX_ALL_CARDS_MIGRATION_PER_BOARD_CAP: Final[int] = 50_000

# Card list filter (path segment): "all" includes open+closed
CARD_LIST_FILTER: Final[str] = "all"

# Card fetch field sets
CARD_FIELDS_RICH: Final[str] = "all"
CARD_FIELDS_LIGHT: Final[str] = (
    "id,name,desc,closed,due,dueComplete,dateLastActivity,idBoard,idList,idMembers,idLabels,labels,shortUrl,url,pos"
)

# Schema diff tracking
DEFAULT_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM: Final[int] = 2000
MAX_SCHEMA_SAMPLE_MAX_RECORDS_PER_STREAM: Final[int] = 50_000

DEFAULT_SCHEMA_LIST_ITEMS_CAP: Final[int] = 50
MAX_SCHEMA_LIST_ITEMS_CAP: Final[int] = 500

# State keys
SCHEMA_STATE_KEY: Final[str] = "trello_schema_snapshot"
SCHEMA_HASH_KEY: Final[str] = "trello_schema_snapshot_hash"
