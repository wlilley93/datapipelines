"""
Runtime package: Airbyte-ish protocol + helpers.

Exports:
- Protocol types: Connector, ConnectorCapabilities, ReadSelection, ReadResult
- Loader: load (and alias load_connector)
- State helpers + schema drift module
"""
from __future__ import annotations

from .protocol import Connector, ConnectorCapabilities, ReadResult, ReadSelection
from .loader import load, load as load_connector

__all__ = [
    "Connector",
    "ConnectorCapabilities",
    "ReadSelection",
    "ReadResult",
    "load",
    "load_connector",
]
