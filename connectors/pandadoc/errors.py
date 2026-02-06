from __future__ import annotations


class PandaDocError(Exception):
    pass


class UnauthorizedError(PandaDocError):
    pass


class TooManyRequestsError(PandaDocError):
    pass
