from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, Optional
from urllib.parse import urlparse

import requests

from connectors.utils import DEFAULT_TIMEOUT, RateLimitConfig, SimpleRateLimiter, request_json

from .constants import CALENDLY_API_VERSION, HTTP_MAX_RETRIES, MAX_RETRY_AFTER_SECONDS
from .events import debug, warn


_RETRYABLE_STATUSES = {408, 409, 425, 429, 500, 502, 503, 504}


def _sleep_backoff(attempt: int, *, base: float = 0.8, cap: float = 60.0) -> None:
    delay = min(cap, base * (2**max(0, attempt)))
    delay = delay * (0.7 + random.random() * 0.6)
    time.sleep(delay)


def _status_from_exc(exc: BaseException) -> Optional[int]:
    if isinstance(exc, requests.HTTPError):
        resp = getattr(exc, "response", None)
        return getattr(resp, "status_code", None)
    return None


def _retry_after_seconds(exc: BaseException) -> Optional[int]:
    if not isinstance(exc, requests.HTTPError):
        return None
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    ra = (getattr(resp, "headers", {}) or {}).get("Retry-After")
    if ra is None:
        return None
    try:
        s = str(ra).strip()
        if not s.isdigit():
            return None
        return int(s)
    except Exception:
        return None


def _is_auth_error(exc: BaseException) -> bool:
    status = _status_from_exc(exc)
    return status in (401, 403)


def _coerce_url(base_url: str, path_or_url: str) -> str:
    p = (path_or_url or "").strip()
    if not p:
        raise ValueError("Empty path_or_url")
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if not p.startswith("/"):
        p = "/" + p
    return base_url.rstrip("/") + p


def _uri_id(uri: Any) -> Optional[str]:
    if not isinstance(uri, str) or not uri.strip():
        return None
    try:
        p = urlparse(uri)
        path = (p.path or "").strip("/")
        if not path:
            return None
        return path.split("/")[-1] or None
    except Exception:
        return None


def unwrap_resource(payload: Any) -> Dict[str, Any]:
    """
    Calendly often wraps single-object responses as {"resource": {...}}.
    """
    if isinstance(payload, dict):
        r = payload.get("resource")
        if isinstance(r, dict):
            return r
        return payload
    return {}


@dataclass(frozen=True)
class CalendlyClient:
    base_url: str
    token: str
    session: requests.Session
    rate_limiter: Optional[SimpleRateLimiter] = None
    api_version: str = CALENDLY_API_VERSION

    def headers(self) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_version:
            h["Calendly-Version"] = self.api_version
        return h

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        stream: str,
        op: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        timeout: int = DEFAULT_TIMEOUT,
        max_retries: int = HTTP_MAX_RETRIES,
        allow_404: bool = False,
    ) -> Any:
        url = _coerce_url(self.base_url, path_or_url)

        last_exc: Optional[BaseException] = None
        for attempt in range(max_retries + 1):
            started = time.monotonic()
            try:
                debug("http.request", stream=stream, op=op, method=method.upper(), url=url, attempt=attempt + 1)
                return request_json(
                    self.session,
                    method,
                    url,
                    headers=self.headers(),
                    params=params,
                    json_body=json_body,
                    timeout=timeout,
                    rate_limiter=self.rate_limiter,
                )
            except BaseException as e:
                last_exc = e
                status = _status_from_exc(e)
                elapsed_ms = int((time.monotonic() - started) * 1000)

                # Optional endpoint support: treat 404 as "not available".
                if allow_404 and status == 404:
                    warn("http.404_skipped", stream=stream, op=op, method=method.upper(), url=url, elapsed_ms=elapsed_ms)
                    return None

                if _is_auth_error(e):
                    raise

                if isinstance(e, requests.HTTPError):
                    # Retry only transient statuses
                    if status in _RETRYABLE_STATUSES and attempt < max_retries:
                        ra = _retry_after_seconds(e)
                        if ra is not None:
                            sleep_s = min(MAX_RETRY_AFTER_SECONDS, max(0, int(ra)))
                            warn(
                                "http.retry_after",
                                stream=stream,
                                op=op,
                                status=status,
                                sleep_seconds=sleep_s,
                                url=url,
                                attempt=attempt + 1,
                            )
                            time.sleep(sleep_s)
                        else:
                            warn("http.retry_backoff", stream=stream, op=op, status=status, url=url, attempt=attempt + 1)
                            _sleep_backoff(attempt)
                        continue
                    raise

                if isinstance(e, (requests.Timeout, requests.ConnectionError)) and attempt < max_retries:
                    warn("http.retry_network", stream=stream, op=op, url=url, attempt=attempt + 1, error=str(e))
                    _sleep_backoff(attempt)
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError("Calendly request failed with unknown error")

    def get_resource(self, path_or_url: str, *, stream: str, op: str, allow_404: bool = False) -> Optional[Dict[str, Any]]:
        data = self.request("GET", path_or_url, stream=stream, op=op, allow_404=allow_404)
        if data is None:
            return None
        return unwrap_resource(data)

    def iter_collection(
        self,
        path_or_url: str,
        *,
        stream: str,
        op: str,
        params: Optional[Dict[str, Any]] = None,
        item_key: str = "collection",
        page_size: int = 100,
        allow_404: bool = False,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate a Calendly list endpoint, supporting both next_page_token and next_page URL styles.
        """
        base_url = _coerce_url(self.base_url, path_or_url)
        next_url: Optional[str] = None
        page_token: Optional[str] = None
        page = 0

        while True:
            page += 1

            if next_url:
                payload = self.request("GET", next_url, stream=stream, op=op, params=None, allow_404=allow_404)
            else:
                eff: Dict[str, Any] = dict(params or {})
                eff.setdefault("count", int(page_size))
                if page_token:
                    eff["page_token"] = page_token
                payload = self.request("GET", base_url, stream=stream, op=op, params=eff, allow_404=allow_404)

            if payload is None:
                return

            if not isinstance(payload, dict):
                return

            items = payload.get(item_key)
            if items is None and item_key != "collection":
                items = payload.get("collection")
            if not isinstance(items, list):
                items = []

            for item in items:
                if isinstance(item, dict):
                    yield item

            pag = payload.get("pagination") or {}
            next_url = None
            page_token = None

            if isinstance(pag, dict):
                # Prefer full next_page URL if available (safest)
                np = pag.get("next_page")
                if isinstance(np, str) and np.strip():
                    next_url = np.strip()
                    continue

                # Fallback to token if no URL provided
                nt = pag.get("next_page_token")
                if isinstance(nt, str) and nt.strip():
                    page_token = nt.strip()
                    continue

            return

    def uri_id(self, uri: Any) -> Optional[str]:
        return _uri_id(uri)


def rate_limiter_from_min_interval(min_interval_seconds: float) -> Optional[SimpleRateLimiter]:
    try:
        s = float(min_interval_seconds)
    except Exception:
        return None
    if s <= 0:
        return None
    return SimpleRateLimiter(RateLimitConfig(min_interval_seconds=s))

