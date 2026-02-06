from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .constants import (
    LOG_EVERY_N_PAGES,
    MEETING_COLLISION_TOP_N,
    UNIQUE_CHECKPOINT_EVERY_N,
)
from .dedupe import DedupeTracker, FingerprintDedupeTracker, PageSignature
from .errors import PaginationLoopError
from .events import debug, error, info, warn
from .gql import FirefliesClient, build_transcripts_list_query
from .participants import extract_participants
from .time_utils import iso_minus_minutes, normalize_iso, parse_iso, utc_now
from .utils_bridge import add_metadata


@dataclass
class TranscriptWithParticipants:
    transcript: Dict[str, Any]
    participants: List[Dict[str, Any]]
    meeting_index: Dict[str, Any]
    meeting: Dict[str, Any]


def _sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _normalize_meeting_link(url: Optional[str]) -> Optional[str]:
    if not url or not isinstance(url, str):
        return None

    url = url.strip()
    if not url:
        return None

    try:
        u = urlparse(url)
        scheme = (u.scheme or "").lower()
        netloc = (u.netloc or "").lower()

        drop_keys = {
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_term",
            "utm_content",
            "ref",
            "fbclid",
            "gclid",
            "igshid",
            "mkt_tok",
        }
        q = [(k, v) for (k, v) in parse_qsl(u.query, keep_blank_values=True) if k not in drop_keys]
        query = urlencode(q, doseq=True)

        path = re.sub(r"/+$", "", u.path or "")
        fragment = ""

        return urlunparse((scheme, netloc, path, u.params, query, fragment))
    except Exception:
        return url


def compute_meeting_key(node: Dict[str, Any]) -> Tuple[str, str]:
    calendar_id = node.get("calendar_id")
    if calendar_id:
        return f"cal:{calendar_id}", "calendar_id"

    norm_link = _normalize_meeting_link(node.get("meeting_link"))
    if norm_link:
        return f"link:{_sha16(norm_link)}", "meeting_link"

    host = str(node.get("host_email") or "").lower().strip()
    date = str(node.get("date") or "").strip()
    duration = str(node.get("duration") or "").strip()
    title = str(node.get("title") or "").strip()

    minute_key = date
    dt = parse_iso(date)
    if dt:
        minute_key = dt.replace(second=0, microsecond=0).isoformat()

    raw = f"{host}|{minute_key}|{duration}|{title}"
    return f"f:{_sha16(raw)}", "fallback"


def _canonical_summary(summary: Any) -> Any:
    # summary can be dict or string or None. We normalize to stable JSON-like value.
    if summary is None:
        return None
    if isinstance(summary, (str, int, float, bool)):
        return summary
    if isinstance(summary, list):
        return [_canonical_summary(x) for x in summary]
    if isinstance(summary, dict):
        # sort keys for stable serialization
        return {str(k): _canonical_summary(summary[k]) for k in sorted(summary.keys(), key=lambda x: str(x))}
    return str(summary)


def compute_content_fingerprint(node: Dict[str, Any]) -> str:
    """
    Fingerprint transcript content to collapse duplicates with different UUIDs.

    Excludes:
      - id
      - transcript_url (can vary per user/view)
    Includes:
      - title, date (normalized ISO), duration
      - calendar_id
      - normalized meeting_link
      - host_email
      - summary (canonicalized)
      - participants list (sorted) IF present (but only if API returns it)
        (If not present, fingerprint relies on other fields.)
    """
    title = (node.get("title") or "").strip()
    date_norm = normalize_iso(str(node.get("date") or "").strip())
    duration = node.get("duration")
    try:
        duration_n = int(duration) if duration is not None else None
    except Exception:
        duration_n = str(duration) if duration is not None else None

    calendar_id = str(node.get("calendar_id") or "").strip() or None
    host_email = str(node.get("host_email") or "").strip().lower() or None
    meeting_link = _normalize_meeting_link(node.get("meeting_link")) or None

    summary = _canonical_summary(node.get("summary"))

    participants = node.get("participants")
    participants_norm: Optional[List[str]] = None
    if isinstance(participants, list):
        ps: List[str] = []
        for p in participants:
            if isinstance(p, str):
                ps.append(p.strip().lower())
            else:
                ps.append(str(p).strip().lower())
        participants_norm = sorted([x for x in ps if x])

    payload = {
        "title": title or None,
        "date": date_norm,
        "duration": duration_n,
        "calendar_id": calendar_id,
        "host_email": host_email,
        "meeting_link": meeting_link,
        "summary": summary,
        "participants": participants_norm,
    }

    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return _sha16(raw)


class TranscriptStreamer:
    """
    Pagination:
      - limit/skip with stable toDate pinned for the whole run (prevents offset drift).

    Dedupes:
      - transcript ID dedupe (hard)
      - NEW: content fingerprint dedupe (collapse identical JSONB fields even with different UUIDs)
        -> prefers first record seen; later identical records are marked _skip_write, but we still emit
           transcript_meeting_index for traceability.
    """

    def __init__(
        self,
        client: FirefliesClient,
        *,
        date_cursor_iso: Optional[str],
        lookback_minutes: int,
        max_pages: int,
        max_records: int,
        include_participants: bool,
        include_raw_participants: bool,
        include_summary: bool,
        enable_meeting_dedupe: bool,
        dedupe_identical_transcripts: bool,
    ):
        self._client = client
        self._lookback_minutes = int(max(0, lookback_minutes))
        self._max_pages = int(max_pages)
        self._max_records = int(max_records)
        self._include_participants = bool(include_participants)
        self._include_raw_participants = bool(include_raw_participants)
        self._include_summary = bool(include_summary)
        self._enable_meeting_dedupe = bool(enable_meeting_dedupe)
        self._dedupe_identical = bool(dedupe_identical_transcripts)

        self.stats: Dict[str, int] = {
            "pages": 0,
            "items_seen_including_dupes": 0,
            "unique_transcript_ids": 0,
            "duplicates_skipped": 0,
            "content_duplicates_collapsed": 0,
            "participants": 0,
            "meeting_keys": 0,
            "meeting_collisions": 0,
        }

        self._id_dedupe = DedupeTracker()
        self._fp_dedupe = FingerprintDedupeTracker() if self._dedupe_identical else None

        self._query = build_transcripts_list_query(
            include_participants=self._include_participants,  # if true, fingerprint can include participants
            include_summary=self._include_summary,
        )

        self._to_date_iso = utc_now().isoformat()

        self._from_date_iso: Optional[str] = None
        if date_cursor_iso:
            dt = parse_iso(date_cursor_iso)
            if dt:
                self._from_date_iso = iso_minus_minutes(dt, self._lookback_minutes)

        self._skip = 0

        # Checkpoint trackers
        self._checkpoint_seen_ids: set[str] = set()
        self._checkpoint_total = 0
        self._checkpoint_dupes = 0
        self._checkpoint_tracked_globally = True
        self._checkpoint_meeting_counts: Dict[str, int] = {}

    def _emit_checkpoint(self) -> None:
        total = self.stats["items_seen_including_dupes"]
        unique = self.stats["unique_transcript_ids"]
        dupes = self.stats["duplicates_skipped"]
        ratio = (unique / total) if total else 1.0

        meeting_collisions = 0
        top: List[Tuple[str, int]] = []
        if self._checkpoint_meeting_counts:
            meeting_collisions = sum(max(0, c - 1) for c in self._checkpoint_meeting_counts.values())
            top = sorted(self._checkpoint_meeting_counts.items(), key=lambda kv: kv[1], reverse=True)[:MEETING_COLLISION_TOP_N]

        info(
            "transcripts.checkpoint",
            stream="transcripts",
            processed_total=total,
            unique_transcript_ids=unique,
            duplicates_total=dupes,
            content_duplicates_collapsed=self.stats["content_duplicates_collapsed"],
            unique_ratio=round(ratio, 6),
            checkpoint_size=self._checkpoint_total,
            checkpoint_dupes=self._checkpoint_dupes,
            checkpoint_ids_unique=(self._checkpoint_dupes == 0),
            global_uniqueness_exact=self._checkpoint_tracked_globally,
            meeting_collisions_in_checkpoint=meeting_collisions,
            top_meeting_collisions=[{"meeting_key": k, "count": c} for (k, c) in top],
        )

        self._checkpoint_seen_ids.clear()
        self._checkpoint_total = 0
        self._checkpoint_dupes = 0
        self._checkpoint_meeting_counts.clear()

    def __iter__(self) -> Generator[TranscriptWithParticipants, None, None]:
        info(
            "transcripts.stream.start",
            stream="transcripts",
            fromDate=self._from_date_iso,
            toDate=self._to_date_iso,
            lookback_minutes=self._lookback_minutes,
            include_participants=self._include_participants,
            include_summary=self._include_summary,
            enable_meeting_dedupe=self._enable_meeting_dedupe,
            dedupe_identical_transcripts=self._dedupe_identical,
            page_size=self._client.page_size,
        )

        while True:
            if self.stats["pages"] >= self._max_pages:
                warn("transcripts.stream.limit", stream="transcripts", limit_type="max_pages", limit=self._max_pages)
                break

            if self.stats["unique_transcript_ids"] >= self._max_records:
                warn("transcripts.stream.limit", stream="transcripts", limit_type="max_records", limit=self._max_records)
                break

            variables: Dict[str, Any] = {
                "limit": self._client.page_size,
                "skip": self._skip,
                "fromDate": self._from_date_iso,
                "toDate": self._to_date_iso,
            }

            payload = self._client.execute(self._query, variables, stream="transcripts", op="fetch_page")
            self.stats["pages"] += 1

            if self.stats["pages"] == 1 or self.stats["pages"] % LOG_EVERY_N_PAGES == 0:
                info(
                    "transcripts.stream.progress",
                    stream="transcripts",
                    page=self.stats["pages"],
                    skip=self._skip,
                    items_seen_including_dupes=self.stats["items_seen_including_dupes"],
                    unique_transcript_ids=self.stats["unique_transcript_ids"],
                    dupes=self.stats["duplicates_skipped"],
                    content_duplicates_collapsed=self.stats["content_duplicates_collapsed"],
                    meeting_keys=self.stats["meeting_keys"],
                    meeting_collisions=self.stats["meeting_collisions"],
                )

            items = payload.get("data", {}).get("transcripts") or []
            if not isinstance(items, list):
                error("transcripts.stream.bad_shape", stream="transcripts", got_type=type(items).__name__)
                raise Exception("Unexpected transcripts response shape; expected list")

            if not items:
                debug("transcripts.stream.empty_page", stream="transcripts", page=self.stats["pages"], skip=self._skip)
                break

            first_id = str((items[0] or {}).get("id") or "")
            last_id = str((items[-1] or {}).get("id") or "")
            sig = PageSignature(first_id=first_id, last_id=last_id, count=len(items))
            if first_id and last_id and self._id_dedupe.record_page_signature(sig):
                error(
                    "transcripts.stream.pagination_loop",
                    stream="transcripts",
                    page=self.stats["pages"],
                    skip=self._skip,
                    page_sig=sig.compact_hash(),
                    first_id=first_id,
                    last_id=last_id,
                    count=len(items),
                )
                raise PaginationLoopError("Fireflies pagination loop detected: repeating page signature")

            for node in items:
                node = node or {}
                transcript_id = node.get("id")
                if not transcript_id:
                    continue
                tid = str(transcript_id)

                self.stats["items_seen_including_dupes"] += 1
                self._checkpoint_total += 1
                if tid in self._checkpoint_seen_ids:
                    self._checkpoint_dupes += 1
                else:
                    self._checkpoint_seen_ids.add(tid)

                is_dup_id, tracked_globally = self._id_dedupe.was_seen(tid)
                if not tracked_globally:
                    self._checkpoint_tracked_globally = False

                if is_dup_id:
                    self.stats["duplicates_skipped"] += 1
                    if self._checkpoint_total >= UNIQUE_CHECKPOINT_EVERY_N:
                        self._emit_checkpoint()
                    continue

                self.stats["unique_transcript_ids"] += 1

                meeting_key, basis = compute_meeting_key(node)
                if self._enable_meeting_dedupe:
                    prev = self._checkpoint_meeting_counts.get(meeting_key, 0)
                    self._checkpoint_meeting_counts[meeting_key] = prev + 1
                    if prev == 0:
                        self.stats["meeting_keys"] += 1
                    else:
                        self.stats["meeting_collisions"] += 1

                # NEW: content fingerprint dedupe (prefer first)
                content_fp = compute_content_fingerprint(node)
                is_content_dup = False
                if self._fp_dedupe is not None:
                    is_content_dup, _ = self._fp_dedupe.was_seen(content_fp)
                    if is_content_dup:
                        self.stats["content_duplicates_collapsed"] += 1

                if self._checkpoint_total >= UNIQUE_CHECKPOINT_EVERY_N:
                    self._emit_checkpoint()

                # If it’s a content-duplicate, we:
                # - still emit transcript_meeting_index (trace)
                # - still emit meetings row (merge on meeting_key is idempotent)
                # - DO NOT write transcripts row (mark _skip_write)
                # - DO NOT emit participants (redundant)
                participants: List[Dict[str, Any]] = []
                participants_raw = node.get("participants") if self._include_participants else None

                transcript = add_metadata(
                    {
                        "id": tid,
                        "meeting_key": meeting_key if self._enable_meeting_dedupe else None,
                        "meeting_key_basis": basis if self._enable_meeting_dedupe else None,
                        "content_fingerprint": content_fp,
                        "title": node.get("title"),
                        "date": node.get("date"),
                        "duration": node.get("duration"),
                        "transcript_url": node.get("transcript_url"),
                        "meeting_link": node.get("meeting_link"),
                        "calendar_id": node.get("calendar_id"),
                        "host_email": node.get("host_email"),
                        "summary": node.get("summary") if self._include_summary else None,
                        "_participants_raw": (participants_raw if self._include_raw_participants else None),
                        "_skip_write": bool(is_content_dup),
                    },
                    "fireflies",
                )

                if (not is_content_dup) and self._include_participants and participants_raw:
                    for p in extract_participants(tid, participants_raw):
                        if self._enable_meeting_dedupe:
                            p = dict(p)
                            p["meeting_key"] = meeting_key
                        participants.append(p)
                        self.stats["participants"] += 1

                meeting_index = add_metadata(
                    {
                        "meeting_key": meeting_key,
                        "transcript_id": tid,
                        "meeting_key_basis": basis,
                        "date": node.get("date"),
                        "calendar_id": node.get("calendar_id"),
                        "meeting_link": node.get("meeting_link"),
                        "host_email": node.get("host_email"),
                        "content_fingerprint": content_fp,
                        "is_content_duplicate": bool(is_content_dup),
                    },
                    "fireflies",
                )

                meeting = add_metadata(
                    {
                        "meeting_key": meeting_key,
                        "calendar_id": node.get("calendar_id"),
                        "meeting_link": _normalize_meeting_link(node.get("meeting_link")),
                        "host_email": node.get("host_email"),
                        "title": node.get("title"),
                        "date": node.get("date"),
                        "duration": node.get("duration"),
                    },
                    "fireflies",
                )

                yield TranscriptWithParticipants(
                    transcript=transcript,
                    participants=participants,
                    meeting_index=meeting_index,
                    meeting=meeting,
                )

            self._skip += len(items)
            if len(items) < self._client.page_size:
                break

        if self._checkpoint_total > 0:
            self._emit_checkpoint()

        info(
            "transcripts.stream.done",
            stream="transcripts",
            pages=self.stats["pages"],
            items_seen_including_dupes=self.stats["items_seen_including_dupes"],
            unique_transcript_ids=self.stats["unique_transcript_ids"],
            duplicates_skipped=self.stats["duplicates_skipped"],
            content_duplicates_collapsed=self.stats["content_duplicates_collapsed"],
            participants=self.stats["participants"],
            meeting_keys=self.stats["meeting_keys"],
            meeting_collisions=self.stats["meeting_collisions"],
            global_uniqueness_exact=self._checkpoint_tracked_globally,
        )
