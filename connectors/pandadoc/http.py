from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from .constants import (
    DEFAULT_MAX_429_RETRIES,
    MAX_BACKOFF_SECONDS,
    PROD_DETAILS_SLEEP_SECONDS,
    PROD_LIST_SLEEP_SECONDS,
    SANDBOX_BASE,
    SANDBOX_SLEEP_SECONDS,
)
from .errors import TooManyRequestsError, UnauthorizedError
from .events import debug, warn
from .utils_bridge import DEFAULT_TIMEOUT


def base_url(creds: Dict[str, Any], *, prod_base: str, sandbox_base: str) -> str:
    if creds.get("base_url"):
        return str(creds["base_url"]).rstrip("/")
    if creds.get("sandbox") is True or str(creds.get("environment", "")).lower() == "sandbox":
        return sandbox_base
    return prod_base


def headers(creds: Dict[str, Any]) -> Dict[str, str]:
    return {
        "Authorization": f"API-Key {creds['api_key']}",
        "accept": "application/json",
    }


def sleep_for_limits(base: str, endpoint_kind: str, *, sandbox_base: str) -> None:
    if base == sandbox_base:
        time.sleep(SANDBOX_SLEEP_SECONDS)
        return
    if endpoint_kind == "details":
        time.sleep(PROD_DETAILS_SLEEP_SECONDS)
    else:
        time.sleep(PROD_LIST_SLEEP_SECONDS)


def request_with_429_retry(
    session,
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = DEFAULT_MAX_429_RETRIES,
) -> Any:
    backoff = 1.0

    for attempt in range(0, max_retries):
        try:
            resp = session.request(method, url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            warn(
                "http.request.failed",
                stream="pandadoc",
                attempt=attempt + 1,
                error=str(e),
            )
            if attempt == max_retries - 1:
                raise Exception(f"Failed to connect to PandaDoc API after {max_retries} attempts: {str(e)}")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue

        if resp.status_code != 429:
            if resp.status_code == 401:
                raise UnauthorizedError("PandaDoc unauthorized (check API key / workspace access)")
            resp.raise_for_status()
            return resp.json() if resp.content else {}

        retry_after = resp.headers.get("Retry-After")
        if retry_after and str(retry_after).isdigit():
            sleep_s = int(retry_after)
        else:
            sleep_s = backoff
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

        if sleep_s > 60:
            warn(
                "http.429_long_wait",
                stream="pandadoc",
                sleep_seconds=sleep_s,
                message=f"PandaDoc API requested long wait ({sleep_s}s Retry-After)",
            )
            # Cap sleep to avoid indefinite hanging if the API returns something crazy,
            # but still respect it mostly. If it's > 5 minutes, something is wrong.
            if sleep_s > 300:
                sleep_s = 300
                warn(
                    "http.429_wait_capped",
                    stream="pandadoc",
                    sleep_seconds=sleep_s,
                    message=f"PandaDoc API wait capped to {sleep_s}s",
                )

        warn(
            "http.429_backoff",
            stream="pandadoc",
            attempt=attempt + 1,
            sleep_seconds=sleep_s,
        )
        time.sleep(sleep_s)

    raise TooManyRequestsError("PandaDoc: too many 429 responses. Reduce backfill window or slow down.")


def extract_links(obj: Any) -> Any:
    if isinstance(obj, dict) and isinstance(obj.get("links"), list):
        return obj["links"]
    return []


def ui_link_guess(document_id: str) -> str:
    return f"https://app.pandadoc.com/a/#/documents/{document_id}"
