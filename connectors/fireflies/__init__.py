from __future__ import annotations

# Preserve loader compatibility:
# - `import connectors.fireflies` works
# - `connectors.fireflies.connector()` is callable

from .connector import FirefliesConnector, connector

__all__ = ["FirefliesConnector", "connector"]
