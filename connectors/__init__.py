"""
Public convenience exports for the connectors package.

Connectors themselves live in `connectors/`.
The Airbyte-ish protocol/runtime lives in `connectors/runtime/`.

This file keeps imports stable for callers:
  from connectors import ReadSelection, ReadResult, load
"""
from __future__ import annotations

from connectors.runtime.protocol import (  # noqa: F401
    Connector,
    ConnectorCapabilities,
    ReadResult,
    ReadSelection,
)
from connectors.runtime.loader import load  # noqa: F401

__all__ = [
    "Connector",
    "ConnectorCapabilities",
    "ReadSelection",
    "ReadResult",
    "load",
]
