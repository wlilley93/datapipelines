from __future__ import annotations

from typing import Any, Dict

from connectors.runtime.protocol import Connector, ReadResult, ReadSelection

from .pipeline import run_pipeline, test_connection
from .schema import observed_schema


class WassengerConnector(Connector):
    name = "wassenger"

    def check(self, creds: Dict[str, Any]) -> str:
        return test_connection(creds)

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates = run_pipeline(
            creds=creds,
            schema=schema,
            state=state,
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=observed_schema(),
        )


def connector() -> Connector:
    return WassengerConnector()
