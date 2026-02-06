from __future__ import annotations

class SinceUnsupportedError(Exception):
    """Raised when Trello rejects the 'since' parameter for cards listing for a board."""
