from __future__ import annotations
from typing import Any, Dict
from connectors.runtime.protocol import Connector, ReadResult, ReadSelection
from .pipeline import get_observed_schema, run_pipeline, test_connection

class TrelloConnector(Connector):
    name = "trello"

    def check(self, creds: Dict[str, Any]) -> str:
        return test_connection(creds)  # type: ignore[arg-type]

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates = run_pipeline(
            creds=creds,  # type: ignore[arg-type]
            schema=schema,
            state=state,
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=get_observed_schema(),
        )

def connector() -> Connector:
    return TrelloConnector()
