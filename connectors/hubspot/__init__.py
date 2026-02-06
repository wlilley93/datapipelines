# connectors/hubspot/init.py
# (kept for backwards-compat / legacy import patterns)
from .connector import HubSpotConnector, connector

__all__ = ["HubSpotConnector", "connector"]
