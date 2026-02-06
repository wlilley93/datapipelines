# connectors/hubspot/client.py
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

import requests

from connectors.utils import requests_retry_session

from .constants import DEFAULT_TIMEOUT, HUBSPOT_BASE_URL
from .errors import TransientHttpError
from .events import http_error, http_ok, http_start
from .redaction import redact_text


def _auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _safe_body_excerpt(resp: requests.Response, limit: int = 2000) -> str:
    try:
        # Use .text (requests handles decoding) and redact just in case
        t = resp.text or ""
        return redact_text(t, max_len=limit)
    except Exception:
        return ""


class HubSpotClient:
    def __init__(self, *, token: str, timeout: Tuple[int, int] = DEFAULT_TIMEOUT):
        self.token = token
        self.timeout = timeout
        self.session = requests_retry_session()

    def get_json(self, *, stream: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = urljoin(HUBSPOT_BASE_URL, path)
        http_start(stream=stream, method="GET", url=url)
        t0 = time.monotonic()

        try:
            r = self.session.get(url, headers=_auth_headers(self.token), params=params, timeout=self.timeout)
            elapsed = int((time.monotonic() - t0) * 1000)
            r.raise_for_status()

            if not r.content:
                http_ok(stream=stream, method="GET", url=url, elapsed_ms=elapsed, keys_count=0)
                return {}

            try:
                data = r.json()
            except Exception as e:
                # HubSpot (or proxies) can occasionally return truncated JSON; treat as transient.
                excerpt = _safe_body_excerpt(r)
                http_error(
                    stream=stream,
                    method="GET",
                    url=url,
                    status=getattr(r, "status_code", None),
                    error=f"json_decode_error: {e}",
                    extra={"body_excerpt": excerpt[:500]},
                )
                raise TransientHttpError(status_code=int(getattr(r, "status_code", 0) or 0), body_excerpt=excerpt) from e

            http_ok(
                stream=stream,
                method="GET",
                url=url,
                elapsed_ms=elapsed,
                keys_count=len(data) if isinstance(data, dict) else None,
            )
            return data if isinstance(data, dict) else {"value": data}

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            http_error(stream=stream, method="GET", url=url, status=status, error=str(e))
            raise
        except Exception as e:
            http_error(stream=stream, method="GET", url=url, status=None, error=str(e))
            raise

    def post_json(self, *, stream: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = urljoin(HUBSPOT_BASE_URL, path)
        # IMPORTANT: emit BEFORE the request so UI never sits "Starting…" during long SSL reads
        http_start(stream=stream, method="POST", url=url, extra={"payload_keys": len(payload)})

        t0 = time.monotonic()
        try:
            r = self.session.post(
                url,
                headers=_auth_headers(self.token),
                data=json.dumps(payload),
                timeout=self.timeout,
            )
            elapsed = int((time.monotonic() - t0) * 1000)
            r.raise_for_status()

            if not r.content:
                http_ok(stream=stream, method="POST", url=url, elapsed_ms=elapsed, items_count=0)
                return {}

            try:
                data = r.json()
            except Exception as e:
                excerpt = _safe_body_excerpt(r)
                http_error(
                    stream=stream,
                    method="POST",
                    url=url,
                    status=getattr(r, "status_code", None),
                    error=f"json_decode_error: {e}",
                    extra={"body_excerpt": excerpt[:500]},
                )
                raise TransientHttpError(status_code=int(getattr(r, "status_code", 0) or 0), body_excerpt=excerpt) from e

            # best-effort items count extraction
            items = None
            if isinstance(data, dict):
                arr = data.get("results")
                if isinstance(arr, list):
                    items = len(arr)

            http_ok(stream=stream, method="POST", url=url, elapsed_ms=elapsed, items_count=items)
            return data if isinstance(data, dict) else {"value": data}

        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            http_error(stream=stream, method="POST", url=url, status=status, error=str(e))
            raise
        except Exception as e:
            http_error(stream=stream, method="POST", url=url, status=None, error=str(e))
            raise

    def get_associations(
        self,
        *,
        stream: str,
        from_object_type: str,
        object_id: str,
        to_object_type: str,
        after: Optional[str] = None,
        limit: int = 500
    ) -> Dict[str, Any]:
        """
        Fetch associations for a given object using HubSpot v4 Associations API.

        Args:
            stream: The stream name for logging
            from_object_type: Source object type (e.g., 'deals')
            object_id: The ID of the source object
            to_object_type: Target object type (e.g., 'line_items')
            after: Pagination cursor
            limit: Number of results per page (max 500)

        Returns:
            Dict containing association results and pagination info
        """
        path = f"/crm/v4/objects/{from_object_type}/{object_id}/associations/{to_object_type}"
        params = {"limit": min(limit, 500)}
        if after:
            params["after"] = after

        return self.get_json(stream=stream, path=path, params=params)
