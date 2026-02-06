# Fireflies Connector Files Export

This document contains all files from the Fireflies connector directory.

## File: __init__.py

```python
from __future__ import annotations

# Preserve loader compatibility:
# - `import connectors.fireflies` works
# - `connectors.fireflies.connector()` is callable

from .connector import FirefliesConnector, connector

__all__ = ["FirefliesConnector", "connector"]

```

## File: actions.py

```python
from __future__ import annotations

# Fireflies currently does not expose an "actions change feed" equivalent in this connector.
# This module is intentionally reserved for future parity with other incremental connectors.

```

## File: connector.py

```python
from __future__ import annotations

from typing import Any, Mapping

from connectors.runtime.protocol import Connector, ReadResult, ReadSelection

from .pipeline import run_pipeline, test_connection
from .schema import get_observed_schema


class FirefliesConnector(Connector):
    """Fireflies connector implementing the standard Connector protocol."""

    name = "fireflies"

    def check(self, creds: Mapping[str, Any]) -> str:
        return test_connection(dict(creds))

    def read(
        self,
        creds: Mapping[str, Any],
        schema: str,
        selection: ReadSelection,
        state: Mapping[str, Any],
    ) -> ReadResult:
        report, refreshed_creds, state_updates, stats = run_pipeline(
            creds=dict(creds),
            schema=schema,
            state=dict(state),
            selection=selection,
        )
        return ReadResult(
            report_text=report,
            refreshed_creds=refreshed_creds,
            state_updates=state_updates,
            observed_schema=get_observed_schema(),
            stats=stats,
        )


def connector() -> Connector:
    """Factory function for connector instantiation."""
    return FirefliesConnector()

```

## File: constants.py

```python
from __future__ import annotations

FIREFLIES_GQL = "https://api.fireflies.ai/graphql"
CONNECTOR_NAME = "fireflies"

# Pagination (Fireflies docs commonly enforce limit <= 50)
DEFAULT_PAGE_SIZE = 50
MIN_PAGE_SIZE = 5

# Rate limiting / pacing
BASE_PAGE_DELAY_SECONDS = 0.10
RATE_LIMIT_BACKOFF_MULTIPLIER = 2.0
MAX_PAGE_DELAY_SECONDS = 5.0

# Safety limits
DEFAULT_MAX_PAGES = 200_000
DEFAULT_MAX_RECORDS = 2_000_000

# Incremental sync
DEFAULT_LOOKBACK_MINUTES = 60
MAX_LOOKBACK_MINUTES = 24 * 60

# Deduplication window size (rolling)
DEDUPE_WINDOW_SIZE = 10_000

# Logging frequency
LOG_EVERY_N_PAGES = 25

# Checkpoint logging (your request)
UNIQUE_CHECKPOINT_EVERY_N = 5_000

# Guardrails: keep an exact run-wide seen-id set until this cap; beyond that we switch to window-only.
GLOBAL_SEEN_IDS_SOFT_CAP = 250_000

# Content-fingerprint dedupe: run-wide until cap, then window-only (protect memory)
GLOBAL_FINGERPRINTS_SOFT_CAP = 250_000

# Meeting-key collision tracking
MEETING_COLLISION_TOP_N = 5

# State keys
STREAMS_KEY = "streams"
TRANSCRIPTS_STREAM = "transcripts"
CURSOR_DATE_KEY = "cursor_date"
LEGACY_CURSOR_KEY = "fireflies_last_date"

```

## File: dedupe.py

```python
from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Set, Tuple

from .constants import DEDUPE_WINDOW_SIZE, GLOBAL_FINGERPRINTS_SOFT_CAP, GLOBAL_SEEN_IDS_SOFT_CAP


@dataclass
class PageSignature:
    """
    Page signature for offset pagination loop detection.

    We hash (first_id, last_id, count). This is enough to catch the real-world
    “same page repeating” failure without expensive full-page hashing.
    """

    first_id: str
    last_id: str
    count: int

    def compact_hash(self) -> str:
        raw = f"{self.first_id}:{self.last_id}:{self.count}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class _RollingSet:
    """
    Bounded set+deque eviction structure:
    - O(1) membership checks
    - bounded memory via FIFO eviction
    """

    def __init__(self, max_size: int):
        self._max_size = int(max_size)
        self._set: Set[str] = set()
        self._order: Deque[str] = deque()

    def seen(self, value: str) -> bool:
        if value in self._set:
            return True
        self._set.add(value)
        self._order.append(value)
        while len(self._set) > self._max_size:
            oldest = self._order.popleft()
            self._set.discard(oldest)
        return False


class DedupeTracker:
    """
    Track seen transcript IDs + recent page signatures.

    - Rolling window catches local duplicates quickly.
    - Optional global set enables exact run-wide uniqueness until cap is exceeded.
    """

    def __init__(
        self,
        max_size: int = DEDUPE_WINDOW_SIZE,
        *,
        global_seen_soft_cap: int = GLOBAL_SEEN_IDS_SOFT_CAP,
        page_sig_window: int = 200,
    ):
        self._id_window = _RollingSet(max_size=max_size)

        self._global_seen_soft_cap = int(global_seen_soft_cap)
        self._global_seen: Optional[Set[str]] = set()  # becomes None after cap exceeded

        self._page_sigs: Set[str] = set()
        self._page_sig_order: Deque[str] = deque()
        self._page_sig_window = int(page_sig_window)

    def was_seen(self, id_value: str) -> Tuple[bool, bool]:
        """
        Returns (is_duplicate, tracked_globally).
        """
        dup_window = self._id_window.seen(id_value)

        tracked_globally = self._global_seen is not None
        dup_global = False
        if self._global_seen is not None:
            dup_global = id_value in self._global_seen
            self._global_seen.add(id_value)
            if len(self._global_seen) > self._global_seen_soft_cap:
                self._global_seen = None
                tracked_globally = False

        return (dup_window or dup_global), tracked_globally

    def record_page_signature(self, sig: PageSignature) -> bool:
        """
        Returns True if this page signature has been seen recently (likely loop).
        """
        h = sig.compact_hash()
        if h in self._page_sigs:
            return True

        self._page_sigs.add(h)
        self._page_sig_order.append(h)

        while len(self._page_sigs) > self._page_sig_window:
            oldest = self._page_sig_order.popleft()
            self._page_sigs.discard(oldest)

        return False


class FingerprintDedupeTracker:
    """
    Track content fingerprints to collapse “same transcript JSONB fields” duplicates
    even when transcript UUID differs.

    Semantics mirror DedupeTracker:
    - rolling window always on
    - global set on until cap, then disabled
    """

    def __init__(
        self,
        max_size: int = DEDUPE_WINDOW_SIZE,
        *,
        global_seen_soft_cap: int = GLOBAL_FINGERPRINTS_SOFT_CAP,
    ):
        self._fp_window = _RollingSet(max_size=max_size)
        self._global_seen_soft_cap = int(global_seen_soft_cap)
        self._global_seen: Optional[Set[str]] = set()

    def was_seen(self, fingerprint: str) -> Tuple[bool, bool]:
        dup_window = self._fp_window.seen(fingerprint)

        tracked_globally = self._global_seen is not None
        dup_global = False
        if self._global_seen is not None:
            dup_global = fingerprint in self._global_seen
            self._global_seen.add(fingerprint)
            if len(self._global_seen) > self._global_seen_soft_cap:
                self._global_seen = None
                tracked_globally = False

        return (dup_window or dup_global), tracked_globally

```

## File: errors.py

```python
from __future__ import annotations


class FirefliesError(Exception):
    pass


class PaginationLoopError(FirefliesError):
    """Raised when pagination repeats the same page (loop)."""
    pass


class HtmlResponseError(FirefliesError):
    """Raised when endpoint returns HTML (blocked/wrong URL)."""
    pass

```

## File: events.py

```python
from __future__ import annotations

from typing import Any, Optional

from .constants import CONNECTOR_NAME


def emit_event(
    event_type: str,
    message: str,
    *,
    stream: Optional[str] = None,
    count: Optional[int] = None,
    level: str = "info",
    **fields: Any,
) -> None:
    """Emit structured event to runtime bus. Fails silently."""
    try:
        from connectors.runtime.events import emit
    except Exception:
        return

    try:
        emit(
            event_type,
            message,
            connector=CONNECTOR_NAME,
            stream=stream,
            count=count,
            level=level,
            **(fields or {}),
        )
    except Exception:
        pass


def debug(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="debug", **fields)


def info(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="info", **fields)


def warn(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="warn", **fields)


def error(message: str, *, stream: Optional[str] = None, **fields: Any) -> None:
    emit_event("message", message, stream=stream, level="error", **fields)

```

## File: gql.py

```python
from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, Optional, Sequence, Union

from .constants import (
    BASE_PAGE_DELAY_SECONDS,
    DEFAULT_PAGE_SIZE,
    FIREFLIES_GQL,
    MAX_PAGE_DELAY_SECONDS,
    MIN_PAGE_SIZE,
    RATE_LIMIT_BACKOFF_MULTIPLIER,
)
from .errors import HtmlResponseError
from .events import debug, error, warn
from .utils_bridge import DEFAULT_TIMEOUT, requests_retry_session


def redact_body(text: str, max_len: int = 500) -> str:
    if not text:
        return ""

    patterns = [
        (r'"api_key"\s*:\s*"[^"]*"', '"api_key":"[REDACTED]"'),
        (r'"token"\s*:\s*"[^"]*"', '"token":"[REDACTED]"'),
        (r'Bearer\s+[A-Za-z0-9\-_\.]+', "Bearer [REDACTED]"),
    ]

    redacted = text
    for pattern, replacement in patterns:
        redacted = re.sub(pattern, replacement, redacted, flags=re.IGNORECASE)

    return redacted[:max_len]


_PathKey = Union[str, int]


def get_nested(obj: Any, path: Sequence[_PathKey], default: Any = None) -> Any:
    """
    Safely traverse nested dict/list structures.

    Supports dict keys (str/int) and list indices (int or digit-strings).
    """
    cur: Any = obj
    for key in path:
        if cur is None:
            return default

        # Dict navigation
        if isinstance(cur, dict):
            key2: _PathKey = key
            if isinstance(key, str) and key.isdigit() and key not in cur:
                key2 = int(key)
            cur = cur.get(key2)
            continue

        # List navigation
        if isinstance(cur, list):
            idx: Optional[int] = None
            if isinstance(key, int):
                idx = key
            elif isinstance(key, str) and key.isdigit():
                idx = int(key)

            if idx is None or idx < 0 or idx >= len(cur):
                return default
            cur = cur[idx]
            continue

        return default

    return cur if cur is not None else default


_SENSITIVE_KEYS = {"api_key", "token", "authorization", "auth", "bearer", "apikey"}


def _safe_variables_for_log(variables: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (variables or {}).items():
        key_norm = str(k).lower()
        if key_norm in _SENSITIVE_KEYS:
            out[k] = "[REDACTED]"
            continue
        if isinstance(v, str) and len(v) > 200:
            out[k] = v[:200] + "..."
        else:
            out[k] = v
    return out


def build_transcripts_list_query(*, include_participants: bool, include_summary: bool) -> str:
    participant_fields = "participants" if include_participants else ""

    summary_fields = ""
    if include_summary:
        summary_fields = """
          summary {
            overview
            action_items
            outline
            keywords
          }
        """

    return f"""
    query Transcripts(
      $limit: Int
      $skip: Int
      $fromDate: DateTime
      $toDate: DateTime
    ) {{
      transcripts(
        limit: $limit
        skip: $skip
        fromDate: $fromDate
        toDate: $toDate
      ) {{
        id
        title
        date
        duration
        transcript_url
        meeting_link
        calendar_id
        host_email
        {participant_fields}
        {summary_fields}
      }}
    }}
    """


class FirefliesClient:
    """GraphQL client with pacing and basic rate-limit backoff."""

    def __init__(self, api_key: str):
        # Keep retries low to avoid long hangs on bad networks.
        self._session = requests_retry_session(retries=2, backoff_factor=0.3)
        self._timeout = int(os.getenv("FIREFLIES_HTTP_TIMEOUT") or DEFAULT_TIMEOUT)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._current_delay = float(BASE_PAGE_DELAY_SECONDS)
        self._current_page_size = int(DEFAULT_PAGE_SIZE)
        self._rate_limit_hits = 0
        self._slow_response_count = 0  # Track consecutive slow responses
        self._slow_response_threshold = 5000  # ms threshold for slow responses

    @property
    def page_size(self) -> int:
        return self._current_page_size

    def reduce_page_size(self) -> bool:
        new_size = max(MIN_PAGE_SIZE, self._current_page_size // 2)
        if new_size == self._current_page_size:
            return False
        self._current_page_size = new_size
        return True

    def _adaptive_pace(self) -> None:
        time.sleep(self._current_delay)

    def _on_rate_limit(self, retry_after: float) -> None:
        self._rate_limit_hits += 1
        sleep_time = max(retry_after, self._current_delay * RATE_LIMIT_BACKOFF_MULTIPLIER)

        warn(
            "graphql.rate_limit_backoff",
            stream="fireflies",
            retry_after=retry_after,
            new_delay=min(sleep_time, MAX_PAGE_DELAY_SECONDS),
            total_hits=self._rate_limit_hits,
        )

        time.sleep(min(sleep_time, MAX_PAGE_DELAY_SECONDS) + 1)

        self._current_delay = min(
            self._current_delay * RATE_LIMIT_BACKOFF_MULTIPLIER,
            MAX_PAGE_DELAY_SECONDS,
        )

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        stream: Optional[str] = None,
        op: Optional[str] = None,
        max_retries: int = 3,
    ) -> Dict[str, Any]:
        attempts = 0
        variables = variables or {}

        while True:
            attempts += 1
            self._adaptive_pace()

            t0 = time.perf_counter()
            debug("graphql.request.start", stream=stream, op=op, variables=_safe_variables_for_log(variables),
                  page_size=self._current_page_size, delay=self._current_delay, attempt=attempts)

            resp = self._session.post(
                FIREFLIES_GQL,
                json={"query": query, "variables": variables},
                headers=self._headers,
                timeout=self._timeout,
            )

            duration_ms = int((time.perf_counter() - t0) * 1000)
            content_type = (resp.headers.get("Content-Type") or "").lower()

            # Track slow responses and adjust pacing and page size accordingly
            if duration_ms > self._slow_response_threshold:
                self._slow_response_count += 1
                # Gradually increase delay when experiencing consecutive slow responses
                if self._slow_response_count >= 3:  # 3 consecutive slow responses
                    new_delay = min(self._current_delay * 1.5, MAX_PAGE_DELAY_SECONDS)
                    if new_delay != self._current_delay:
                        debug(
                            "graphql.slow_response_adjustment",
                            stream=stream,
                            op=op,
                            old_delay=self._current_delay,
                            new_delay=new_delay,
                            slow_response_count=self._slow_response_count,
                        )
                        self._current_delay = new_delay
                        self._slow_response_count = 0  # Reset counter after adjustment

                # Also reduce page size gradually when experiencing slow responses
                if self._current_page_size > MIN_PAGE_SIZE and self._slow_response_count % 5 == 0:  # Every 5th slow response
                    old_page_size = self._current_page_size
                    self._current_page_size = max(MIN_PAGE_SIZE, int(self._current_page_size * 0.75))
                    if self._current_page_size != old_page_size:
                        debug(
                            "graphql.page_size_reduction",
                            stream=stream,
                            op=op,
                            old_page_size=old_page_size,
                            new_page_size=self._current_page_size,
                            duration_ms=duration_ms,
                        )
            else:
                # Increase page size gradually if we get consistently fast responses
                if self._slow_response_count == 0 and self._current_page_size < DEFAULT_PAGE_SIZE:
                    # Every 5 successful fast responses, increase page size
                    if hasattr(self, '_fast_response_count'):
                        self._fast_response_count += 1
                    else:
                        self._fast_response_count = 1

                    if self._fast_response_count >= 5:
                        old_page_size = self._current_page_size
                        self._current_page_size = min(DEFAULT_PAGE_SIZE, int(self._current_page_size * 1.25))
                        debug(
                            "graphql.page_size_increase",
                            stream=stream,
                            op=op,
                            old_page_size=old_page_size,
                            new_page_size=self._current_page_size,
                        )
                        self._fast_response_count = 0  # Reset the counter
                elif self._slow_response_count > 0:
                    # Reset the fast response counter when encountering slow responses
                    if hasattr(self, '_fast_response_count'):
                        delattr(self, '_fast_response_count')

                # Decrease delay if we get fast responses (gradually return to baseline)
                if self._slow_response_count > 0:
                    self._slow_response_count = max(0, self._slow_response_count - 1)
                if self._current_delay > BASE_PAGE_DELAY_SECONDS and self._slow_response_count == 0:
                    # Gradually reduce delay back to baseline
                    self._current_delay = max(BASE_PAGE_DELAY_SECONDS, self._current_delay * 0.9)

            if duration_ms > 10000:  # Very slow response (>10s)
                error(
                    "graphql.very_slow_response",
                    stream=stream,
                    op=op,
                    duration_ms=duration_ms,
                    status_code=resp.status_code,
                    page_size=self._current_page_size,
                    current_delay=self._current_delay,
                )
            elif duration_ms > 5000:  # Slow response (>5s)
                warn(
                    "graphql.slow_response",
                    stream=stream,
                    op=op,
                    duration_ms=duration_ms,
                    status_code=resp.status_code,
                    page_size=self._current_page_size,
                    current_delay=self._current_delay,
                )

            if "text/html" in content_type:
                error("graphql.html_response", stream=stream, op=op, status_code=resp.status_code)
                raise HtmlResponseError("HTML response from Fireflies (blocked request or wrong endpoint)")

            debug(
                "graphql.response",
                stream=stream,
                op=op,
                status_code=resp.status_code,
                duration_ms=duration_ms,
            )

            if resp.status_code == 429:
                retry_after_str = resp.headers.get("Retry-After", "10")
                try:
                    retry_after = float(retry_after_str)
                except ValueError:
                    retry_after = 10.0
                self._on_rate_limit(retry_after)
                self._slow_response_count = 0  # Reset slow response counter on rate limit
                if attempts <= max_retries:
                    warn(
                        "graphql.retry",
                        stream=stream,
                        op=op,
                        attempt=attempts,
                        max_retries=max_retries,
                        status_code=resp.status_code,
                    )
                    continue
                resp.raise_for_status()

            resp.raise_for_status()

            payload = resp.json() if resp.content else {}
            if isinstance(payload, dict) and payload.get("errors"):
                errs = payload["errors"] or []
                first = errs[0] if errs else {}
                msg = str(first.get("message") or first)
                error("graphql.error", stream=stream, op=op, error=msg[:500], raw=redact_body(str(first)))
                raise Exception(msg)

            return payload

    def get_user_me(self) -> Dict[str, Any]:
        query = "query { user { user_id email name } }"
        payload = self.execute(query, stream="test", op="user_me")
        return get_nested(payload, ["data", "user"]) or {}

```

## File: participants.py

```python
from __future__ import annotations

import hashlib
from typing import Any, Dict, Generator, List, Optional

from .utils_bridge import add_metadata


def generate_participant_key(
    transcript_id: str,
    participant_id: Optional[str],
    email: Optional[str],
    name: Optional[str],
) -> str:
    if participant_id:
        return str(participant_id)
    if email:
        return email
    if name:
        composite = f"{transcript_id}:{name}"
        return hashlib.sha256(composite.encode()).hexdigest()[:16]
    return f"{transcript_id}:unknown"


def parse_participant_item(item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, str):
        if "@" in item:
            return {"email": item, "name": None, "id": None, "role": None}
        return {"name": item, "email": None, "id": None, "role": None}

    if isinstance(item, dict):
        if "node" in item and isinstance(item["node"], dict):
            item = item["node"]

        return {
            "id": item.get("id"),
            "email": item.get("email") or item.get("mail") or item.get("emailAddress"),
            "name": item.get("name") or item.get("displayName") or item.get("fullName"),
            "role": item.get("role") or item.get("type"),
        }

    return None


def extract_participants(transcript_id: str, raw: Any) -> Generator[Dict[str, Any], None, None]:
    items: List[Any] = []

    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        nested = raw.get("results") or raw.get("edges") or raw.get("nodes")
        items = nested if isinstance(nested, list) else [raw]
    elif isinstance(raw, str):
        items = [raw]
    else:
        return

    for item in items:
        participant = parse_participant_item(item)
        if participant is None:
            continue

        pid = participant.get("id")
        email = participant.get("email")
        name = participant.get("name")
        key = generate_participant_key(transcript_id, pid, email, name)

        yield add_metadata(
            {
                "transcript_id": transcript_id,
                "participant_key": key,
                "participant_id": pid,
                "email": email,
                "name": name,
                "role": participant.get("role"),
            },
            "fireflies",
        )

```

## File: pipeline.py

```python
from __future__ import annotations

import textwrap
from typing import Any, Dict, Iterable, Optional, Tuple

import dlt

from connectors.runtime.protocol import ReadSelection

from .constants import (
    CONNECTOR_NAME,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_RECORDS,
)
from .events import info, warn
from .gql import FirefliesClient, build_transcripts_list_query, get_nested
from .selection import is_stream_enabled, normalize_selection
from .state import FirefliesStateManager
from .streaming import TranscriptStreamer


def _require_api_key(creds: Dict[str, Any]) -> str:
    api_key = (creds or {}).get("api_key") or creds.get("token")
    if not api_key:
        raise Exception("Fireflies API key missing (expected creds.api_key)")
    return str(api_key)


def test_connection(creds: Dict[str, Any]) -> str:
    """
    Light-weight probe: fetch 1 transcript to validate the API key and endpoint.
    """
    api_key = _require_api_key(creds)
    client = FirefliesClient(api_key)

    query = build_transcripts_list_query(include_participants=False, include_summary=False)
    variables = {"limit": 1, "skip": 0}

    payload = client.execute(query, variables, stream="transcripts", op="test_connection")
    first_id = get_nested(payload, ["data", "transcripts", 0, "id"])
    if not first_id:
        warn("test_connection.no_transcripts", stream="transcripts")
    return "Fireflies Connected"


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Execute Fireflies sync via the transcript streamer and load to Postgres with DLT.
    """
    api_key = _require_api_key(creds)
    client = FirefliesClient(api_key)

    selected = normalize_selection(selection)
    want_transcripts = is_stream_enabled("transcripts", selected, set())
    want_participants = is_stream_enabled("transcript_participants", selected, set())
    want_meeting_index = is_stream_enabled("meeting_index", selected, set())
    want_meetings = is_stream_enabled("meetings", selected, set())

    if not (want_transcripts or want_participants or want_meeting_index or want_meetings):
        warn("sync.no_streams_selected", stream="fireflies")
        return "No streams selected", None, {}, {}

    state_mgr = FirefliesStateManager(state or {})
    date_cursor = state_mgr.get_date_cursor()

    streamer = TranscriptStreamer(
        client=client,
        date_cursor_iso=date_cursor,
        lookback_minutes=DEFAULT_LOOKBACK_MINUTES,
        max_pages=DEFAULT_MAX_PAGES,
        max_records=DEFAULT_MAX_RECORDS,
        include_participants=want_participants,
        include_raw_participants=False,
        include_summary=True,
        enable_meeting_dedupe=True,
        dedupe_identical_transcripts=True,
    )

    @dlt.resource(name="fireflies_transcripts_raw", write_disposition="append")
    def transcripts_raw() -> Iterable[Dict[str, Any]]:
        for bundle in streamer:
            state_mgr.track_record_date(bundle.transcript.get("date"))
            yield {
                "transcript": bundle.transcript,
                "participants": bundle.participants,
                "meeting_index": bundle.meeting_index,
                "meeting": bundle.meeting,
            }

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key="id",
        table_name="transcripts",
    )
    def transcripts_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_transcripts:
            return
        t = (bundle or {}).get("transcript") or {}
        if t.get("_skip_write"):
            return
        yield t

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key=("transcript_id", "participant_key"),
        table_name="transcript_participants",
    )
    def participants_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_participants:
            return
        t = (bundle or {}).get("transcript") or {}
        if t.get("_skip_write"):
            return
        ps = (bundle or {}).get("participants") or []
        for p in ps:
            yield p

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key=("meeting_key", "transcript_id"),
        table_name="meeting_index",
    )
    def meeting_index_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_meeting_index:
            return
        idx = (bundle or {}).get("meeting_index")
        if idx:
            yield idx

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key="meeting_key",
        table_name="meetings",
    )
    def meetings_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_meetings:
            return
        m = (bundle or {}).get("meeting")
        if m:
            yield m

    resources = [transcripts_raw, transcripts_table]
    if want_participants:
        resources.append(participants_table)
    if want_meeting_index:
        resources.append(meeting_index_table)
    if want_meetings:
        resources.append(meetings_table)

    info(
        "pipeline.run.start",
        stream="fireflies",
        cursor=date_cursor,
        lookback_minutes=DEFAULT_LOOKBACK_MINUTES,
        max_pages=DEFAULT_MAX_PAGES,
        max_records=DEFAULT_MAX_RECORDS,
        want_transcripts=want_transcripts,
        want_participants=want_participants,
        want_meeting_index=want_meeting_index,
        want_meetings=want_meetings,
    )

    pipeline = dlt.pipeline(pipeline_name=CONNECTOR_NAME, destination="postgres", dataset_name=schema)
    info_obj = pipeline.run(resources)

    state_mgr.finalize()
    state_updates = state_mgr.updates()

    stats: Dict[str, Any] = dict(streamer.stats)
    rows_inserted = stats.get("unique_transcript_ids", 0)
    if want_participants:
        rows_inserted += stats.get("participants", 0)
    if want_meeting_index:
        rows_inserted += stats.get("unique_transcript_ids", 0)
    if want_meetings:
        rows_inserted += stats.get("meeting_keys", 0)
    stats["rows_inserted"] = rows_inserted
    report_lines = [
        "Fireflies sync completed.",
        f"Pages: {stats.get('pages', 0)}",
        f"Transcripts: {stats.get('unique_transcript_ids', 0)} "
        f"(dupes skipped: {stats.get('duplicates_skipped', 0)}, "
        f"content-collapsed: {stats.get('content_duplicates_collapsed', 0)})",
    ]
    if want_participants:
        report_lines.append(f"Participants: {stats.get('participants', 0)}")
    if want_meeting_index or want_meetings:
        report_lines.append(
            f"Meeting keys: {stats.get('meeting_keys', 0)} (collisions: {stats.get('meeting_collisions', 0)})"
        )
    report_lines.append(f"Estimated rows inserted: {rows_inserted}")
    if date_cursor:
        report_lines.append(f"Cursor in: {date_cursor}")
    cursor_out = (
        state_updates.get("streams", {})
        .get("transcripts", {})
        .get("cursor_date")
    )
    if cursor_out:
        report_lines.append(f"Cursor out: {cursor_out}")
    if info_obj:
        report_lines.append(str(info_obj))

    report_text = textwrap.dedent("\n".join(report_lines)).strip()

    return report_text, None, state_updates, stats

```

## File: schema.py

```python
from __future__ import annotations

from typing import Any, Dict


def observed_schema() -> Dict[str, Any]:
    # Minimal schema snapshot; the payloads can be quite nested.
    # Pin a known “sometimes-empty” nested column so dlt doesn’t warn when it’s absent in a batch.
    return {
        "streams": {
            "messages": {
                "primary_key": ["id"],
                "fields": {
                    "id": "string",
                    # dlt sometimes materializes this nested path as a column even if no rows contain it
                    # in the current load; pin it to avoid type inference warnings.
                    "media__file__preview__image": "json",
                },
            },
            "conversations": {"primary_key": ["id"], "fields": {"id": "string"}},
            "contacts": {"primary_key": ["id"], "fields": {"id": "string"}},
        }
    }


def get_observed_schema() -> Dict[str, Any]:
    """
    Adapter for the Connector API.

    The FirefliesConnector calls this name; we delegate to `observed_schema`
    to preserve backwards compatibility with any earlier callers.
    """
    return observed_schema()

```

## File: selection.py

```python
from __future__ import annotations

from typing import Any, Set


def normalize_selection(selection: Any) -> Set[str]:
    """Normalize selection to a set of stream names. Empty set means all selected."""
    if selection is None:
        return set()

    for attr in ("streams", "tables", "resources"):
        val = getattr(selection, attr, None)
        if isinstance(val, (list, set, tuple)):
            return set(str(x) for x in val) if val else set()

    if isinstance(selection, dict):
        for key in ("streams", "tables", "resources"):
            val = selection.get(key)
            if isinstance(val, list):
                return set(str(x) for x in val) if val else set()

    return set()


def is_stream_enabled(stream: str, selection: Set[str], exclude_streams: Set[str]) -> bool:
    if stream in exclude_streams:
        return False
    return (not selection) or (stream in selection)

```

## File: state.py

```python
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

```

## File: streaming.py

```python
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

```

## File: time_utils.py

```python
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

```

## File: utils_bridge.py

```python
from __future__ import annotations

# Bridge shared utilities through connectors.utils to avoid circular imports,
# matching the Trello package pattern.

from connectors.utils import DEFAULT_TIMEOUT, add_metadata, requests_retry_session  # type: ignore

__all__ = ["DEFAULT_TIMEOUT", "add_metadata", "requests_retry_session"]

```

