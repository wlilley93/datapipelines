from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Set


SyncMode = Literal["full_refresh", "incremental"]
DestinationSyncMode = Literal["append", "merge", "overwrite"]


@dataclass(frozen=True)
class Stream:
    """
    Minimal stream contract.

    - name: stream/table name
    - json_schema: optional JSONSchema-like dict describing record shape
    - primary_key: optional list of field names (composite key)
    - cursor_field: optional field name used for incremental
    - supported_sync_modes: advertised connector support
    """

    name: str
    json_schema: Optional[Dict[str, Any]] = None
    primary_key: Optional[List[str]] = None
    cursor_field: Optional[str] = None
    supported_sync_modes: List[SyncMode] = field(default_factory=lambda: ["full_refresh"])


@dataclass(frozen=True)
class Catalog:
    """
    Declared streams available from a connector.
    """

    streams: List[Stream] = field(default_factory=list)

    def stream_names(self) -> Set[str]:
        return {s.name for s in self.streams if s and s.name}


@dataclass(frozen=True)
class ConfiguredStream:
    """
    Stream with chosen sync configuration.
    """

    name: str
    sync_mode: SyncMode = "full_refresh"
    destination_sync_mode: DestinationSyncMode = "merge"
    cursor_field: Optional[str] = None


@dataclass(frozen=True)
class ConfiguredCatalog:
    """
    Airbyte-ish configured catalog: the exact plan for a sync.
    """

    streams: List[ConfiguredStream] = field(default_factory=list)

    def stream_names(self) -> Set[str]:
        return {s.name for s in self.streams if s and s.name}


# -----------------------------
# Helpers
# -----------------------------
def default_configured_catalog(catalog: Catalog) -> ConfiguredCatalog:
    """
    Convert a Catalog into a reasonable default configured plan:
      - use incremental when supported and cursor_field exists, else full_refresh
      - destination sync defaults to merge
    """
    configured: List[ConfiguredStream] = []
    for s in catalog.streams:
        sync_mode: SyncMode = "full_refresh"
        if "incremental" in (s.supported_sync_modes or []) and s.cursor_field:
            sync_mode = "incremental"
        configured.append(
            ConfiguredStream(
                name=s.name,
                sync_mode=sync_mode,
                destination_sync_mode="merge",
                cursor_field=s.cursor_field,
            )
        )
    return ConfiguredCatalog(streams=configured)


def validate_stream_selection(catalog: Catalog, selected_streams: Optional[List[str]]) -> List[str]:
    """
    Validates and returns the effective stream list:
      - if selected_streams is empty/None => all streams from catalog
      - else => ensure every name exists in catalog
    """
    available = catalog.stream_names()
    if not selected_streams:
        return sorted(list(available))

    cleaned = []
    missing = []
    for n in selected_streams:
        if not n:
            continue
        if n not in available:
            missing.append(n)
        else:
            cleaned.append(n)

    if missing:
        raise ValueError(f"Unknown streams requested: {missing}. Available: {sorted(list(available))}")

    return cleaned
