from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .constants import CURSOR_DATE_KEY, LEGACY_CURSOR_KEY, STREAMS_KEY, TRANSCRIPTS_STREAM
from .time_utils import normalize_iso, parse_iso


@dataclass
class FirefliesStateManager:
    """
    Manage cursor state for Fireflies incremental sync.

    - cursor_date persisted across runs
    - skip/offset is per-run only
    - cursor only advances if records were processed
    - cursor is monotonic
    """

    state: Dict[str, Any]

    def __post_init__(self) -> None:
        self._updates: Dict[str, Any] = {"streams": {}}
        self._max_date_seen_iso: Optional[str] = None
        self._records_processed = 0

    def get_date_cursor(self) -> Optional[str]:
        streams = self.state.get(STREAMS_KEY)
        if isinstance(streams, dict):
            transcripts = streams.get(TRANSCRIPTS_STREAM)
            if isinstance(transcripts, dict):
                v = normalize_iso(transcripts.get(CURSOR_DATE_KEY))
                if v:
                    dt = parse_iso(v)
                    if dt and dt.year >= 2000:
                        return v

        for key in (LEGACY_CURSOR_KEY, "last_success_at", "start_date"):
            v = normalize_iso(self.state.get(key))
            if v:
                dt = parse_iso(v)
                if dt and dt.year >= 2000:
                    return v

        g = self.state.get("global")
        if isinstance(g, dict):
            v = normalize_iso(g.get("last_success_at"))
            if v:
                dt = parse_iso(v)
                if dt and dt.year >= 2000:
                    return v

        return None

    def track_record_date(self, date_str: Optional[str]) -> None:
        self._records_processed += 1
        norm = normalize_iso(date_str)
        if not norm:
            return

        if self._max_date_seen_iso is None:
            self._max_date_seen_iso = norm
            return

        a = parse_iso(self._max_date_seen_iso)
        b = parse_iso(norm)
        if a and b and b > a:
            self._max_date_seen_iso = norm

    def finalize(self) -> None:
        prev = self.get_date_cursor()

        if self._records_processed > 0 and self._max_date_seen_iso:
            if prev:
                prev_dt = parse_iso(prev)
                seen_dt = parse_iso(self._max_date_seen_iso)
                if prev_dt and seen_dt and seen_dt <= prev_dt:
                    cursor_out = prev
                else:
                    cursor_out = self._max_date_seen_iso
            else:
                cursor_out = self._max_date_seen_iso
        else:
            cursor_out = prev

        if cursor_out:
            self._updates["streams"][TRANSCRIPTS_STREAM] = {CURSOR_DATE_KEY: cursor_out}
            self._updates[LEGACY_CURSOR_KEY] = cursor_out

    def updates(self) -> Dict[str, Any]:
        out = dict(self._updates)
        if not out.get("streams"):
            out.pop("streams", None)
        return out
