from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def is_later(a: str, b: str) -> bool:
    da = parse_iso(a)
    db = parse_iso(b)
    if da is not None and db is not None:
        return da > db
    return a > b


def clamp_int(val: Any, *, default: int, lo: int, hi: int) -> int:
    try:
        n = int(val)
    except Exception:
        return default
    return max(lo, min(hi, n))


def lookback_dt(dt: datetime, minutes: int) -> datetime:
    return dt - timedelta(minutes=max(0, minutes))


def if_modified_since_header(dt: datetime) -> str:
    """
    Xero supports an ISO-like timestamp for If-Modified-Since in many SDKs.
    We send "YYYY-MM-DDTHH:MM:SS" in UTC (no timezone suffix) — consistent with
    the prior implementation and common Xero examples.
    """
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
