from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def normalize_iso(s: Optional[str]) -> Optional[str]:
    dt = parse_iso(s)
    return dt.isoformat() if dt else None


def clamp_int(val: Any, *, default: int, lo: int, hi: int) -> int:
    try:
        n = int(val)
    except Exception:
        return default
    return max(lo, min(hi, n))


def iso_minus_minutes(dt: datetime, minutes: int) -> str:
    return (dt - timedelta(minutes=max(0, minutes))).astimezone(timezone.utc).isoformat()
