from __future__ import annotations


class FirefliesError(Exception):
    pass


class PaginationLoopError(FirefliesError):
    """Raised when pagination repeats the same page (loop)."""
    pass


class HtmlResponseError(FirefliesError):
    """Raised when endpoint returns HTML (blocked/wrong URL)."""
    pass
