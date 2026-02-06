from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ConnectorCapabilities:
    """
    Capability flags the orchestrator can use to adjust UX/behaviour.

    selection: supports stream selection (subset reads)
    incremental: supports incremental reads driven by state/cursors
    full_refresh: supports full refresh reads
    schema: can emit a catalog/schema (declared streams)
    observed_schema: can emit observed schema snapshots for drift detection
    """
    selection: bool = False
    incremental: bool = False
    full_refresh: bool = True
    schema: bool = False
    observed_schema: bool = False


@dataclass(frozen=True)
class ReadSelection:
    """
    Orchestrator-to-connector read intent.

    streams: optional list of stream names to sync (None => all)
    since: optional ISO timestamp string for incremental boundaries (connector-defined)
    full_refresh: if True, forces full refresh even if incremental is supported
    """
    streams: Optional[List[str]] = None
    since: Optional[str] = None
    full_refresh: bool = False


@dataclass(frozen=True)
class ReadResult:
    """
    Connector-to-orchestrator result.

    report_text: human readable summary to show in CLI + store in run logs
    refreshed_creds: optional rotated credentials to persist back to secrets
    state_updates: state blob to merge into connection state on success
    observed_schema: optional snapshot { "streams": { "<name>": { "properties": ... } } }
                    used for drift detection (only if connector provides it)
    stats: optional machine-readable counters/metrics
    """
    report_text: str = ""
    refreshed_creds: Optional[Dict[str, Any]] = None
    state_updates: Optional[Dict[str, Any]] = None
    observed_schema: Optional[Dict[str, Any]] = None
    stats: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        # Normalise None -> {} for merge-friendly behaviour
        if self.state_updates is None:
            object.__setattr__(self, "state_updates", {})
        if self.stats is None:
            object.__setattr__(self, "stats", {})


class Connector:
    """
    Minimal protocol connector interface.

    - check(creds) -> str
    - read(creds, schema, selection, state) -> ReadResult
    """

    capabilities: ConnectorCapabilities = ConnectorCapabilities()

    def check(self, creds: Dict[str, Any]) -> str:  # pragma: no cover
        raise NotImplementedError

    def read(  # pragma: no cover
        self,
        *,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        raise NotImplementedError
