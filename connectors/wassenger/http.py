from __future__ import annotations

import random
import time
from typing import Any, Dict, Optional

from .constants import (
    DEFAULT_BACKOFF_INITIAL_SECONDS,
    DEFAULT_BACKOFF_MAX_SECONDS,
    DEFAULT_MAX_429_RETRIES,
    DEFAULT_REQUEST_PACE_SECONDS,
    MAX_REQUEST_PACE_SECONDS,
)
from .errors import WassengerAuthError, WassengerRateLimitError
from .events import error, info, warn
from .time_utils import clamp_int
from .utils_bridge import DEFAULT_TIMEOUT


def base_url(creds: Dict[str, Any], state: Dict[str, Any], *, default: str) -> str:
    if creds.get("base_url"):
        return str(creds["base_url"]).rstrip("/")
    if state.get("wassenger_base_url"):
        return str(state["wassenger_base_url"]).rstrip("/")
    return default.rstrip("/")


def auth_headers(creds: Dict[str, Any]) -> Dict[str, str]:
    """
    Accepts:
      - creds["api_key"]
      - creds["token"]
      - creds["access_token"]
    """
    token = creds.get("api_key") or creds.get("token") or creds.get("access_token")
    if not token:
        raise WassengerAuthError("Missing Wassenger token (api_key/token/access_token).")
    return {
        "Authorization": f"Bearer {str(token)}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _pace(state: Dict[str, Any]) -> None:
    pace = float(state.get("wassenger_request_pace_seconds", DEFAULT_REQUEST_PACE_SECONDS))
    pace = max(0.0, min(pace, MAX_REQUEST_PACE_SECONDS))
    if pace > 0:
        time.sleep(pace)


def extract_rows(payload: Any, *, collection_key: Optional[str]) -> list[Any]:
    """
    Wassenger endpoints vary: sometimes list responses are raw arrays,
    sometimes wrapped in {data:[...]}, and sometimes use other keys.
    """
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if collection_key and isinstance(payload.get(collection_key), list):
            return payload[collection_key]

        # Common fallback keys (include more than before)
        for k in (
            "data",
            "results",
            "items",
            "devices",
            "chats",
            "contacts",
            "messages",
            "rows",
        ):
            if isinstance(payload.get(k), list):
                return payload[k]

    return []


def request_json_with_429_retry(
    session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    state: Dict[str, Any],
    params: Optional[Dict[str, Any]] = None,
    stream: Optional[str] = None,
    op: Optional[str] = None,
) -> Any:
    max_retries = clamp_int(
        state.get("wassenger_max_429_retries"),
        default=DEFAULT_MAX_429_RETRIES,
        lo=0,
        hi=50,
    )
    backoff = float(state.get("wassenger_backoff_initial_seconds", DEFAULT_BACKOFF_INITIAL_SECONDS))
    backoff_max = float(state.get("wassenger_backoff_max_seconds", DEFAULT_BACKOFF_MAX_SECONDS))

    for attempt in range(0, max_retries + 1):
        _pace(state)

        info(
            "http.request.start",
            stream=stream,
            op=op,
            method=method,
            url=url,
            attempt=attempt,
            params=params or {},
        )

        started = time.monotonic()
        try:
            resp = session.request(method, url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            error(
                "http.request.error",
                stream=stream,
                op=op,
                method=method,
                url=url,
                attempt=attempt,
                elapsed_ms=elapsed_ms,
                error=str(e),
            )
            raise

        elapsed_ms = int((time.monotonic() - started) * 1000)

        if resp.status_code in (401, 403):
            error(
                "http.request.error",
                stream=stream,
                op=op,
                method=method,
                url=url,
                attempt=attempt,
                elapsed_ms=elapsed_ms,
                status=resp.status_code,
            )
            raise WassengerAuthError(f"Wassenger auth failed ({resp.status_code}).")

        if resp.status_code != 429:
            try:
                resp.raise_for_status()
            except Exception as e:
                body_preview = ""
                try:
                    body_preview = (resp.text or "")[:500]
                except Exception:
                    body_preview = ""
                error(
                    "http.request.error",
                    stream=stream,
                    op=op,
                    method=method,
                    url=url,
                    attempt=attempt,
                    elapsed_ms=elapsed_ms,
                    status=resp.status_code,
                    error=str(e),
                    body_preview=body_preview,
                )
                raise

            data = resp.json() if resp.content else {}
            info(
                "http.request.ok",
                stream=stream,
                op=op,
                method=method,
                url=url,
                attempt=attempt,
                elapsed_ms=elapsed_ms,
            )
            return data

        retry_after = resp.headers.get("Retry-After")
        sleep_s: Optional[float] = None
        if retry_after:
            try:
                sleep_s = float(retry_after)
            except Exception:
                sleep_s = None

        if sleep_s is None:
            jitter = random.uniform(0.0, 0.25 * backoff)
            sleep_s = min(backoff + jitter, backoff_max)

        warn(
            "http.rate_limited",
            stream=stream,
            op=op,
            attempt=attempt,
            max_retries=max_retries,
            retry_after=retry_after,
            sleep_seconds=float(sleep_s),
            url=url,
            params=params or {},
        )
        time.sleep(max(0.0, float(sleep_s)))
        backoff = min(backoff * 2, backoff_max)

    raise WassengerRateLimitError("Wassenger: too many 429 responses; reduce scope or increase pacing/backoff.")
