from __future__ import annotations

from typing import Any, Mapping

from connectors.runtime.protocol import Connector, ReadResult, ReadSelection

from .pipeline import run_pipeline, test_connection
from .schema import get_observed_schema


class FirefliesConnector(Connector):
    """Fireflies connector implementing the standard Connector protocol."""

    name = "fireflies"

    def check(self, creds: Mapping[str, Any]) -> str:
        return test_connection(dict(creds))

    def read(
        self,
        creds: Mapping[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Mapping[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates, stats = run_pipeline(
            creds=dict(creds),
            schema=schema,
            state=dict(state),
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=get_observed_schema(),
            stats=stats,
        )


def connector() -> Connector:
    """Factory function for connector instantiation."""
    return FirefliesConnector()
