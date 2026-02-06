from __future__ import annotations

import random
import time
from typing import Any, Dict, Optional

import requests

from .constants import (
    CONNECTIONS_URL,
    DEFAULT_BACKOFF_INITIAL_SECONDS,
    DEFAULT_BACKOFF_MAX_SECONDS,
    DEFAULT_MAX_429_RETRIES,
    DEFAULT_REQUEST_PACE_SECONDS,
    MAX_REQUEST_PACE_SECONDS,
)
from .errors import XeroAuthError, XeroNoTenantsError, XeroRateLimitError
from .events import error, info, warn
from .time_utils import clamp_int


def get_tenant_id(access_token: str, *, preferred_tenant_id: Optional[str] = None) -> str:
    resp = requests.get(
        CONNECTIONS_URL,
        headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    conns = resp.json() or []
    if not conns:
        raise XeroNoTenantsError("No Xero tenants found (user hasn’t authorised an organisation).")

    if preferred_tenant_id:
        for c in conns:
            if str(c.get("tenantId")) == str(preferred_tenant_id):
                return str(c["tenantId"])

    return str(conns[0]["tenantId"])


def xero_headers(access_token: str, tenant_id: str, if_modified_since: Optional[str] = None) -> Dict[str, str]:
    h = {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }
    if if_modified_since:
        h["If-Modified-Since"] = if_modified_since
    return h


def _pace(state: Dict[str, Any]) -> None:
    pace = float(state.get("xero_request_pace_seconds", DEFAULT_REQUEST_PACE_SECONDS))
    pace = max(0.0, min(pace, MAX_REQUEST_PACE_SECONDS))
    if pace > 0:
        time.sleep(pace)


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
    """
    Robust request wrapper with strong observability:
      - http.request.start / ok / error events
      - 429 backoff events
      - raises explicit auth / rate-limit errors

    IMPORTANT:
      - The session used here should NOT be doing its own urllib3 Retry sleeps,
        otherwise "retry_after" sleeps can occur inside adapter.send() and look like a hang.
    """
    max_retries = clamp_int(
        state.get("xero_max_429_retries"),
        default=DEFAULT_MAX_429_RETRIES,
        lo=0,
        hi=50,
    )
    backoff = float(state.get("xero_backoff_initial_seconds", DEFAULT_BACKOFF_INITIAL_SECONDS))
    backoff_max = float(state.get("xero_backoff_max_seconds", DEFAULT_BACKOFF_MAX_SECONDS))

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
            resp = session.request(method, url, headers=headers, params=params, timeout=30)
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

        if resp.status_code == 401:
            error(
                "http.request.error",
                stream=stream,
                op=op,
                method=method,
                url=url,
                attempt=attempt,
                elapsed_ms=elapsed_ms,
                error_type="401",
            )
            raise XeroAuthError("Xero unauthorized (token/scopes). Try reconnecting.")
        if resp.status_code == 403:
            error(
                "http.request.error",
                stream=stream,
                op=op,
                method=method,
                url=url,
                attempt=attempt,
                elapsed_ms=elapsed_ms,
                error_type="403",
            )
            raise XeroAuthError("Xero forbidden (likely missing scopes/tenant access).")

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
            items_count = None
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        items_count = len(v)
                        break

            info(
                "http.request.ok",
                stream=stream,
                op=op,
                method=method,
                url=url,
                attempt=attempt,
                elapsed_ms=elapsed_ms,
                items_count=items_count,
            )
            return data

        retry_after = resp.headers.get("Retry-After")
        sleep_s = None
        if retry_after:
            try:
                # Clamp Retry-After so a tenant that tells us "wait 5 minutes" doesn't look hung forever.
                sleep_s = min(float(retry_after), float(backoff_max))
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

    raise XeroRateLimitError("Xero: too many 429 responses; reduce scope or increase pacing/backoff.")
