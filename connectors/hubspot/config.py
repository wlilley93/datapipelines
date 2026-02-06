from __future__ import annotations

from typing import Any, Dict, List, Set

from .constants import (
    DEFAULT_DEDUPE_SET_CAP,
    DEFAULT_EXCLUDED_STREAMS,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_SCHEMA_MAX_LIST_ITEMS_PER_FIELD,
    DEFAULT_SCHEMA_MAX_RECORDS_PER_STREAM,
    MAX_DEDUPE_SET_CAP,
    MAX_LOOKBACK_MINUTES,
    MAX_SCHEMA_MAX_LIST_ITEMS_PER_FIELD,
    MAX_SCHEMA_MAX_RECORDS_PER_STREAM,
    PROPERTY_CACHE_TTL_HOURS_DEFAULT,
    PROPERTY_CACHE_TTL_HOURS_MAX,
)

try:
    from pydantic import BaseModel, Field
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore
    Field = lambda default=None, **_: default  # type: ignore


class HubSpotConfig(BaseModel):  # type: ignore[misc]
    lookback_minutes: int = Field(default=DEFAULT_LOOKBACK_MINUTES, ge=0, le=MAX_LOOKBACK_MINUTES)
    include_all_properties: bool = Field(default=True)
    max_properties_per_object: int = Field(default=400, ge=25, le=2000)
    exclude_streams: List[str] = Field(default_factory=lambda: sorted(DEFAULT_EXCLUDED_STREAMS))

    property_cache_ttl_hours: int = Field(
        default=PROPERTY_CACHE_TTL_HOURS_DEFAULT, ge=1, le=PROPERTY_CACHE_TTL_HOURS_MAX
    )

    dedupe_set_cap: int = Field(default=DEFAULT_DEDUPE_SET_CAP, ge=10_000, le=MAX_DEDUPE_SET_CAP)

    include_properties: Dict[str, List[str]] = Field(default_factory=dict)
    exclude_properties: Dict[str, List[str]] = Field(default_factory=dict)

    mark_associations_incomplete: bool = Field(default=True)

    schema_max_records_per_stream: int = Field(
        default=DEFAULT_SCHEMA_MAX_RECORDS_PER_STREAM, ge=0, le=MAX_SCHEMA_MAX_RECORDS_PER_STREAM
    )
    schema_max_list_items_per_field: int = Field(
        default=DEFAULT_SCHEMA_MAX_LIST_ITEMS_PER_FIELD, ge=0, le=MAX_SCHEMA_MAX_LIST_ITEMS_PER_FIELD
    )

    advance_cursor_to_run_start_on_empty_incremental: bool = Field(default=True)


def load_config(state: Dict[str, Any]) -> HubSpotConfig:
    """
    Load config from state.

    - If pydantic is available, parse/validate from state.
    - If pydantic is NOT available, HubSpotConfig is a plain object subclass and cannot accept kwargs; return defaults.
    """
    has_pydantic_v2 = hasattr(HubSpotConfig, "model_validate")
    has_pydantic_v1 = hasattr(HubSpotConfig, "parse_obj")

    if has_pydantic_v2:
        try:
            return HubSpotConfig.model_validate(state or {})  # type: ignore[attr-defined]
        except Exception as e:
            raise ValueError(f"Invalid HubSpot config in state: {e}")

    if has_pydantic_v1:
        try:
            return HubSpotConfig.parse_obj(state or {})  # type: ignore[attr-defined]
        except Exception as e:
            raise ValueError(f"Invalid HubSpot config in state: {e}")

    return HubSpotConfig()  # type: ignore[call-arg]
