"""
Intercom connector package.

Why:
- Some orchestrators / “legacy connector” loaders import `run_pipeline` directly from the
  connector package (connectors.intercom.run_pipeline). Exporting it here avoids the
  "Legacy connector 'intercom' missing run_pipeline(...)" runtime error.
"""

from __future__ import annotations

from .connector import connector  # re-export factory
from .pipeline import run_pipeline, test_connection  # re-export legacy entrypoints

__all__ = ["connector", "run_pipeline", "test_connection"]
