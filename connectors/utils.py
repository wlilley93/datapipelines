"""
Shared utilities for all connectors.

Goals:
- One consistent HTTP stack (sessions + retry + timeouts)
- Safe metadata stamping for every record
- Cross-connector primitives: request_json(), rate-limit helpers

Deliberately connector-only:
- NO Rich / Questionary / CLI rendering
- Any "pretty panels" or CLI messaging belongs in cli.py

Backwards compatible with existing connectors.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Default timeout used across connectors unless overridden.
# Backward-compatible: keep CONNECTOR_HTTP_TIMEOUT_SECONDS.
# Convenience alias: ORCH_HTTP_TIMEOUT (if set, takes precedence).
DEFAULT_TIMEOUT: int = int(os.getenv("ORCH_HTTP_TIMEOUT") or os.getenv("CONNECTOR_HTTP_TIMEOUT_SECONDS") or "30")


# -----------------------------
# Rate limiting (optional helper)
# -----------------------------
@dataclass
class RateLimitConfig:
    """
    Simple pacing config (client-side).

    min_interval_seconds:
      Ensures at least this much time between calls in this process.
      Use to reduce 429 churn on chatty APIs.
    """
    min_interval_seconds: float = 0.0


class SimpleRateLimiter:
    """
    Minimal, process-local rate limiter.
    Not distributed; just helps avoid hammering an API.
    """

    def __init__(self, cfg: RateLimitConfig):
        self.cfg = cfg
        self._last_at: float = 0.0

    def wait(self) -> None:
        if not self.cfg.min_interval_seconds:
            return
        now = time.time()
        delta = now - self._last_at
        if delta < self.cfg.min_interval_seconds:
            time.sleep(self.cfg.min_interval_seconds - delta)
        self._last_at = time.time()


# -----------------------------
# HTTP sessions + retries
# -----------------------------
def requests_retry_session(
    retries: int = 6,
    backoff_factor: float = 0.6,
    status_forcelist: Tuple[int, ...] = (408, 409, 425, 429, 500, 502, 503, 504),
    allowed_methods: Tuple[str, ...] = ("HEAD", "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"),
    session: Optional[requests.Session] = None,
) -> requests.Session:
    """
    Creates a requests.Session with a sensible retry strategy.

    Notes:
    - Retries 429 and common transient 5xx codes.
    - Allows POST retries because many connectors use POST for idempotent reads (search/list).
    """
    sess = session or requests.Session()

    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=set(m.upper() for m in allowed_methods),
        raise_on_status=False,  # we handle status codes ourselves for better messages
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=25, pool_maxsize=25)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


# Optional in-process session cache (connection pooling across connector calls)
_SESSION_CACHE: Dict[str, requests.Session] = {}


def get_shared_session(key: str = "default") -> requests.Session:
    """
    Returns a cached session for this Python process.
    Useful if a connector makes many calls and you want pooling + reuse.
    """
    k = key or "default"
    if k not in _SESSION_CACHE:
        _SESSION_CACHE[k] = requests_retry_session()
    return _SESSION_CACHE[k]


# -----------------------------
# Request helpers
# -----------------------------
def _is_html_response(resp: requests.Response) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    head = (resp.text[:200].lower() if resp.text else "")
    return ("text/html" in ct) or ("text/plain" in ct and "<html" in head)


def request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Any] = None,
    data: Optional[Any] = None,
    timeout: Optional[int] = None,
    allow_empty: bool = True,
    rate_limiter: Optional[SimpleRateLimiter] = None,
    json: Optional[Any] = None,  # backward compatibility alias
) -> Any:
    """
    Perform an HTTP request expecting JSON. Raises with a useful Exception on errors.

    - Detects and errors on HTML responses (common wrong endpoint/auth failures).
    - Uses session retry config (above).
    - Optional client-side pacing via `rate_limiter`.
    """
    if rate_limiter is not None:
        rate_limiter.wait()

    eff_timeout = int(timeout if timeout is not None else DEFAULT_TIMEOUT)

    payload = json_body if json_body is not None else json

    resp = session.request(
        method=method.upper(),
        url=url,
        headers=headers,
        params=params,
        json=payload,
        data=data,
        timeout=eff_timeout,
    )

    # Fast-path for success
    if 200 <= resp.status_code < 300:
        if not resp.content:
            return {} if allow_empty else None
        if _is_html_response(resp):
            raise requests.HTTPError("Endpoint returned HTML instead of JSON", response=resp)
        try:
            return resp.json()
        except Exception as e:
            raise requests.HTTPError(f"Failed to decode JSON: {e}", response=resp)

    # Non-2xx: better errors
    if _is_html_response(resp):
        raise requests.HTTPError(f"HTML response (status {resp.status_code})", response=resp)

    try:
        payload = resp.json()
        msg = json.dumps(payload, ensure_ascii=False)[:2000]
    except Exception:
        msg = (resp.text or "")[:2000]

    raise requests.HTTPError(f"HTTP {resp.status_code}: {msg}", response=resp)


# -----------------------------
# Data helpers
# -----------------------------
def add_metadata(record: Dict[str, Any], source_name: str) -> Dict[str, Any]:
    """
    Adds standard metadata fields to each record.
    """
    record["_dlt_load_time"] = datetime.now(timezone.utc).isoformat()
    record["_dlt_source"] = source_name
    return record


def rate_limit_delay(retries: int = 0, cap_seconds: int = 60) -> float:
    """
    Exponential backoff helper for manual retry loops.

    Returns the delay chosen (in seconds) so callers can log it however they want.
    """
    delay = float(min(2 ** max(0, retries), cap_seconds))
    time.sleep(delay)
    return delay
