from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        n = int(v)
        return n if n > 0 else default
    except Exception:
        return default


@dataclass(frozen=True)
class IntercomConfig:
    # Auth (secret) — MUST remain env/creds-driven
    access_token: Optional[str]

    # Pull everything defaults
    sync_contacts: bool = True
    sync_companies: bool = True
    sync_conversations: bool = True
    sync_parts: bool = True
    sync_tickets: bool = True

    # Optional extras (default off)
    sync_admins: bool = False
    sync_teams: bool = False
    sync_segments: bool = False
    sync_tags: bool = False
    sync_help_center_collections: bool = False
    sync_help_center_sections: bool = False
    sync_articles: bool = False

    force_full: bool = False
    start_at_epoch: Optional[int] = None

    # Companies completeness strategy
    companies_use_scroll: bool = True  # FIX: used by companies stream

    # Dynamic columns
    expand_custom_attributes: bool = True
    max_custom_attr_columns: int = 200
    flatten_max_depth: int = 2

    # Performance + resilience
    hydrate_workers: int = 4
    hydrate_max_in_flight: int = 8

    # Request knobs used by http.req_json wrapper
    request_timeout_s: float = 60.0
    request_connect_timeout_s: float = 10.0
    request_retries: int = 2
    retry_sleep_s: float = 1.0

    rate_limit_remaining_floor: int = 2
    rate_limit_max_sleep: int = 60

    # API version header
    intercom_version: str = "2.14"

    @staticmethod
    def from_env_and_creds(creds: dict) -> "IntercomConfig":
        token = (
            creds.get("access_token")
            or creds.get("api_key")
            or creds.get("token")
            or creds.get("INTERCOM_ACCESS_TOKEN")
            or os.getenv("INTERCOM_ACCESS_TOKEN")
        )

        start_raw = (os.getenv("INTERCOM_START_AT_EPOCH") or "").strip()
        try:
            start_epoch = int(start_raw) if start_raw else None
        except Exception:
            start_epoch = None

        workers = _env_int("INTERCOM_HYDRATE_WORKERS", 4)
        max_in_flight = _env_int("INTERCOM_HYDRATE_MAX_IN_FLIGHT", max(8, workers * 2))

        return IntercomConfig(
            access_token=token,
            sync_contacts=_env_bool("INTERCOM_SYNC_CONTACTS", True),
            sync_companies=_env_bool("INTERCOM_SYNC_COMPANIES", True),
            sync_conversations=_env_bool("INTERCOM_SYNC_CONVERSATIONS", True),
            sync_parts=_env_bool("INTERCOM_SYNC_PARTS", True),
            sync_tickets=_env_bool("INTERCOM_SYNC_TICKETS", True),
            sync_admins=_env_bool("INTERCOM_SYNC_ADMINS", False),
            sync_teams=_env_bool("INTERCOM_SYNC_TEAMS", False),
            sync_segments=_env_bool("INTERCOM_SYNC_SEGMENTS", False),
            sync_tags=_env_bool("INTERCOM_SYNC_TAGS", False),
            sync_help_center_collections=_env_bool("INTERCOM_SYNC_HELP_CENTER_COLLECTIONS", False),
            sync_help_center_sections=_env_bool("INTERCOM_SYNC_HELP_CENTER_SECTIONS", False),
            sync_articles=_env_bool("INTERCOM_SYNC_ARTICLES", False),
            force_full=_env_bool("INTERCOM_FORCE_FULL", False),
            start_at_epoch=start_epoch,
            companies_use_scroll=_env_bool("INTERCOM_COMPANIES_USE_SCROLL", True),
            expand_custom_attributes=_env_bool("INTERCOM_EXPAND_CUSTOM_ATTRIBUTES", True),
            max_custom_attr_columns=_env_int("INTERCOM_MAX_CUSTOM_ATTR_COLUMNS", 200),
            flatten_max_depth=_env_int("INTERCOM_FLATTEN_MAX_DEPTH", 2),
            hydrate_workers=workers,
            hydrate_max_in_flight=max_in_flight,
            request_timeout_s=float((_env_int("INTERCOM_REQUEST_TIMEOUT_S", 60))),
            request_connect_timeout_s=float((_env_int("INTERCOM_REQUEST_CONNECT_TIMEOUT_S", 10))),
            request_retries=_env_int("INTERCOM_REQUEST_RETRIES", 2),
            retry_sleep_s=float((_env_int("INTERCOM_RETRY_SLEEP_S", 1))),
            rate_limit_remaining_floor=_env_int("INTERCOM_RATE_LIMIT_REMAINING_FLOOR", 2),
            rate_limit_max_sleep=_env_int("INTERCOM_RATE_LIMIT_MAX_SLEEP", 60),
            intercom_version=(os.getenv("INTERCOM_VERSION") or "2.14").strip() or "2.14",
        )
