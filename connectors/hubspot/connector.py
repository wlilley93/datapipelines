# connectors/hubspot/connector.py
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from connectors.runtime.protocol import Connector, ReadResult, ReadSelection

from .pipeline import run_pipeline, test_connection


class HubSpotConnector(Connector):
    name = "hubspot"

    def read(
        self,
        *,
        creds: Dict[str, Any],
        schema: str,
        selection: Optional[ReadSelection] = None,
        state: Optional[Dict[str, Any]] = None,
    ) -> ReadResult:
        state = state or {}
        destination = os.getenv("DLT_DESTINATION", "postgres")  # Default to postgres as configured in secrets
        report, refreshed_creds, state_updates = run_pipeline(creds=creds, schema=schema, state=state, destination=destination)

        # observed_schema: optional — your orchestrator uses it for drift summaries.
        # Keeping empty dict so it won't crash if expected.
        observed_schema: Dict[str, Any] = {}

        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=observed_schema,
        )


def connector() -> HubSpotConnector:
    return HubSpotConnector()
