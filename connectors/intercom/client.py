from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class IntercomClientConfig:
    access_token: str
    base_url: str = "https://api.intercom.io"
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    max_retries: int = 3
    retry_backoff_s: float = 0.75  # base backoff; grows per attempt
    intercom_version: str = "2.14"


class IntercomClient:
    """
    Why: a small standalone client for unit tests or alternate entrypoints.
    Note: the main connector pipeline uses http.req_json + shared session.
    """

    def __init__(self, cfg: IntercomClientConfig) -> None:
        self._cfg = cfg
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {cfg.access_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Intercom-Version": cfg.intercom_version,
                "User-Agent": "intercom-connector/1.0",
            }
        )

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self._cfg.base_url.rstrip('/')}/{path.lstrip('/')}"
        return self._request_json("GET", url, params=params)

    def _request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        last_exc: Optional[BaseException] = None

        for attempt in range(self._cfg.max_retries):
            try:
                resp = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=body,
                    timeout=(self._cfg.connect_timeout_s, self._cfg.read_timeout_s),
                )

                # why: Intercom rate-limits; respect Retry-After when present.
                if resp.status_code == 429:
                    retry_after = _parse_retry_after_s(resp.headers.get("Retry-After"))
                    time.sleep(retry_after if retry_after is not None else _backoff(self._cfg.retry_backoff_s, attempt))
                    continue

                # why: transient server errors should be retried.
                if 500 <= resp.status_code <= 599:
                    time.sleep(_backoff(self._cfg.retry_backoff_s, attempt))
                    continue

                resp.raise_for_status()

                if not resp.content:
                    return {}

                return resp.json()

            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as exc:
                last_exc = exc

                # why: 4xx (except 429) are usually permanent; don't hammer.
                if isinstance(exc, requests.HTTPError):
                    status = getattr(exc.response, "status_code", None)
                    if status is not None and 400 <= status <= 499 and status != 429:
                        raise

                time.sleep(_backoff(self._cfg.retry_backoff_s, attempt))

        raise RuntimeError(f"Intercom request failed after retries: {method} {url}") from last_exc


def _backoff(base_s: float, attempt: int) -> float:
    # why: simple exponential-ish backoff; enough to avoid hammering.
    return base_s * (2**attempt)


def _parse_retry_after_s(v: Optional[str]) -> Optional[float]:
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None
