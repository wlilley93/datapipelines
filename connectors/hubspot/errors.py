from __future__ import annotations


class AuthError(RuntimeError):
    """Non-retryable auth/scopes error."""


class RateLimitError(RuntimeError):
    def __init__(self, retry_after_s: float, message: str = "Rate limited"):
        super().__init__(message)
        self.retry_after_s = float(max(0.0, retry_after_s))


class TransientHttpError(RuntimeError):
    def __init__(self, status_code: int, body_excerpt: str):
        super().__init__(f"Transient HTTP {status_code}")
        self.status_code = status_code
        self.body_excerpt = body_excerpt
