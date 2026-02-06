from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from connectors.runtime.protocol import Connector, ConnectorCapabilities, ReadResult, ReadSelection

from .pipeline import run_pipeline as _run_pipeline
from .pipeline import test_connection as _test_connection


class IntercomConnector(Connector):
    """Intercom connector implementing the standard Connector protocol."""

    name = "intercom"
    capabilities = ConnectorCapabilities(
        selection=False,
        incremental=True,
        full_refresh=True,
        schema=False,
        observed_schema=False,
    )

    def check(self, creds: Dict[str, Any]) -> str:
        return _test_connection(creds)

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates, stats = _run_pipeline(
            creds=creds,
            schema=schema,
            state=state,
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=None,
            stats=stats,
        )


def connector() -> Connector:
    return IntercomConnector()


# =============================================================================
# Legacy entrypoints
# Why: the error indicates the orchestrator is using the legacy loader which
# expects module-level functions, not the Connector protocol object.
# =============================================================================
def test_connection(creds: Dict[str, Any]) -> str:
    return _test_connection(creds)


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    return _run_pipeline(creds=creds, schema=schema, state=state, selection=selection)
