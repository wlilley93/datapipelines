from __future__ import annotations


class WassengerError(Exception):
    pass


class WassengerAuthError(WassengerError):
    pass


class WassengerRateLimitError(WassengerError):
    pass


class WassengerPagingLoopError(WassengerError):
    pass


class WassengerNoWorkingEndpointError(WassengerError):
    pass
