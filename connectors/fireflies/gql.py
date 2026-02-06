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
