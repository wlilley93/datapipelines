"""
Streams package.

Why:
- `pipeline.py` imports from `connectors.intercom.streams` expecting symbols like
  `admins_resource`, etc.
- Because `streams/` is a package, Python resolves `connectors.intercom.streams` to this
  package, not to any sibling `streams.py` file.
- Therefore, this package must export the public stream API itself.
"""

from __future__ import annotations

from .resources import (
    admins_resource,
    articles_resource,
    companies_resource,
    contacts_resource,
    conversation_parts_transformer,
    conversations_resource,
    help_center_collections_resource,
    help_center_sections_resource,
    segments_resource,
    tags_resource,
    teams_resource,
    tickets_resource,
)

__all__ = [
    "contacts_resource",
    "companies_resource",
    "conversations_resource",
    "tickets_resource",
    "admins_resource",
    "teams_resource",
    "segments_resource",
    "tags_resource",
    "help_center_collections_resource",
    "help_center_sections_resource",
    "articles_resource",
    "conversation_parts_transformer",
]
