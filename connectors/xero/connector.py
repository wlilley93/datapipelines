from __future__ import annotations

from typing import Any, Dict, Tuple

from connectors.runtime.protocol import Connector, ReadResult, ReadSelection

from .pipeline import run_pipeline as _typed_run_pipeline, test_connection
from .schema import observed_schema_for_xero


class XeroConnector(Connector):
    name = "xero"

    def check(self, creds: Dict[str, Any]) -> str:
        return test_connection(creds)

    def read(
        self,
        creds: Dict[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Dict[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates = _typed_run_pipeline(creds, schema, state, selection=selection)

        # Pull stream counts from state_updates (written by pipeline.py).
        stream_counts: Dict[str, int] = {}
        try:
            x_state = (state_updates.get("streams") or {}).get("xero") or {}
            counts = x_state.get("counts")
            if isinstance(counts, dict):
                stream_counts = {str(k): int(v) for k, v in counts.items() if isinstance(v, (int, float, str))}
        except Exception:
            stream_counts = {}

        total_rows = 0
        try:
            total_rows = sum(int(v) for v in stream_counts.values())
        except Exception:
            total_rows = 0

        stats = {
            # The orchestrator often keys off these.
            "rows_loaded": total_rows,
            "rows_inserted": total_rows,
            "stream_counts": stream_counts,
        }

        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=observed_schema_for_xero(),
            stats=stats,
        )


def connector() -> Connector:
    return XeroConnector()


def run_pipeline(*args: Any, **kwargs: Any) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """
    Legacy runtime compatibility shim.
    """
    if len(args) >= 3 and isinstance(args[0], dict) and isinstance(args[1], str) and isinstance(args[2], dict):
        creds = args[0]
        schema = args[1]
        state = args[2]
        selection = kwargs.get("selection")
        return _typed_run_pipeline(creds, schema, state, selection=selection)

    if len(args) >= 2:
        a, b = args[0], args[1]

        def get(obj: Any, key: str, default: Any = None) -> Any:
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        for connection, ctx in ((a, b), (b, a)):
            schema = get(ctx, "schema", None) or get(ctx, "destination_schema", None)
            state = get(ctx, "state", None)

            creds = (
                get(connection, "creds", None)
                or get(connection, "credentials", None)
                or get(connection, "auth", None)
                or get(connection, "config", None)
            )
            if creds is None:
                creds = get(ctx, "creds", None) or get(ctx, "credentials", None) or get(ctx, "config", None)

            selection = get(ctx, "selection", None) or get(ctx, "streams", None) or kwargs.get("selection")

            if schema is not None and state is not None and creds is not None:
                return _typed_run_pipeline(dict(creds), str(schema), dict(state), selection=selection)

    if "creds" in kwargs and "schema" in kwargs and "state" in kwargs:
        return _typed_run_pipeline(
            dict(kwargs["creds"] or {}),
            str(kwargs["schema"]),
            dict(kwargs["state"] or {}),
            selection=kwargs.get("selection"),
        )

    raise TypeError("Unsupported run_pipeline call signature for xero legacy shim.")
