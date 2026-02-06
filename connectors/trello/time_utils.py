from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def utc_now_iso() -> str:
    return utc_now().isoformat()

def parse_iso(s: Any) -> Optional[datetime]:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def normalize_cursor_iso(value: Any) -> Optional[str]:
    dt = parse_iso(value)
    return dt.isoformat() if dt else None

def is_later_timestamp(a: str, b: str) -> bool:
    dt_a = parse_iso(a)
    dt_b = parse_iso(b)
    if dt_a and dt_b:
        return dt_a > dt_b
    return str(a) > str(b)

def clamp_int(val: Any, *, default: int, lo: int, hi: int) -> int:
    try:
        n = int(val)
    except Exception:
        return default
    return max(lo, min(hi, n))

def compute_effective_since_from_cursor(cursor: str, lookback_minutes: int) -> Optional[str]:
    dt = parse_iso(cursor)
    if not dt:
        return None
    adjusted = dt - timedelta(minutes=int(lookback_minutes))
    return adjusted.astimezone(timezone.utc).isoformat()
