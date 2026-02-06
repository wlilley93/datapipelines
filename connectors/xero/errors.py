from __future__ import annotations

from typing import List


class XeroAuthError(Exception):
    pass


class XeroRateLimitError(Exception):
    pass


class XeroNoTenantsError(Exception):
    pass


class XeroPagingLoopError(Exception):
    pass


class InteractiveAuthRequired(Exception):
    def __init__(self, auth_url: str, scopes: List[str]):
        self.auth_url = auth_url
        self.scopes = scopes
        super().__init__(f"Interactive authorization required. Please visit {auth_url}")
