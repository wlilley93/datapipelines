from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .protocol import Connector, ConnectorCapabilities, ReadResult, ReadSelection


@dataclass(frozen=True)
class LegacyRunResult:
    """
    Normalised legacy result from run_pipeline.

    Legacy connectors may return:
      - report
      - (report, new_creds)
      - (report, new_creds, state_updates)
    """
    report_text: str
    refreshed_creds: Optional[Dict[str, Any]]
    state_updates: Dict[str, Any]


def _normalise_legacy_result(result: Any) -> LegacyRunResult:
    if isinstance(result, tuple):
        if len(result) == 3:
            report, new_creds, state_updates = result
            return LegacyRunResult(
                report_text=str(report),
                refreshed_creds=(new_creds if isinstance(new_creds, dict) else None),
                state_updates=(state_updates if isinstance(state_updates, dict) else {}) or {},
            )
        if len(result) == 2:
            report, new_creds = result
            return LegacyRunResult(
                report_text=str(report),
                refreshed_creds=(new_creds if isinstance(new_creds, dict) else None),
                state_updates={},
            )
    return LegacyRunResult(report_text=str(result), refreshed_creds=None, state_updates={})


class LegacyConnectorAdapter(Connector):
    """
    Adapts legacy connectors (test_connection + run_pipeline) to the protocol Connector interface.
    """

    def __init__(self, module, name: str):
        self._module = module
        self._name = name

        # Best-effort capabilities:
        # - selection is not supported unless the connector explicitly exposes it (legacy doesn't)
        # - incremental is possible if connector accepts state and uses cursors; we can't reliably detect that,
        #   but we default incremental=True because orchestrator already passes state; connectors may ignore it.
        # - observed_schema is False (legacy connectors currently don't emit it)
        self.capabilities = ConnectorCapabilities(
            selection=False,
            incremental=True,
            full_refresh=True,
            schema=False,
            observed_schema=False,
        )

    def check(self, creds: Dict[str, Any]) -> str:
        fn = getattr(self._module, "test_connection", None)
        if not callable(fn):
            raise Exception(f"Legacy connector '{self._name}' missing test_connection(creds)")
        return str(fn(creds))

    def read(
        self,
        *,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        fn = getattr(self._module, "run_pipeline", None)
        if not callable(fn):
            raise Exception(f"Legacy connector '{self._name}' missing run_pipeline(...)")

        # Legacy connectors do not support selection yet; we ignore selection.streams
        # (future improvement: legacy connectors can accept selection and honour it)
        try:
            result = fn(creds, schema, state)
        except TypeError:
            result = fn(creds, schema)

        norm = _normalise_legacy_result(result)
        return ReadResult(
            report_text=norm.report_text,
            refreshed_creds=norm.refreshed_creds,
            state_updates=norm.state_updates,
            observed_schema=None,
            stats={},
        )


def load(connector_type: str) -> Connector:
    """
    Load a connector by type string, e.g. "hubspot", "trello".

    Resolution order:
      1) connectors.<type>.connector() -> protocol Connector instance
      2) legacy module adapted via LegacyConnectorAdapter
    """
    if not connector_type or not isinstance(connector_type, str):
        raise ValueError("connector_type must be a non-empty string")

    mod_name = f"connectors.{connector_type.lower()}"
    module = importlib.import_module(mod_name)

    # New-style protocol connector factory
    factory = getattr(module, "connector", None)
    if callable(factory):
        obj = factory()
        if not isinstance(obj, Connector):
            # duck-typing: allow objects that implement the interface even if not subclassed
            if not (hasattr(obj, "check") and hasattr(obj, "read")):
                raise TypeError(f"{mod_name}.connector() did not return a protocol Connector")
        return obj  # type: ignore[return-value]

    # Legacy adapter
    return LegacyConnectorAdapter(module, connector_type)


# API compatibility alias (matches report suggestion + prevents call-site drift)
load_connector = load
