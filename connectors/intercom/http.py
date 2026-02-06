from __future__ import annotations

import json
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from connectors.runtime.events import emit


# =========================================
# SECTION A — SMALL UTILITIES
# Why: keep consistent run IDs + timestamping + log path plumbing.
# =========================================
def run_id() -> str:
    rid = (os.getenv("RUN_ID") or "").strip()
    return rid or uuid.uuid4().hex


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_log_path() -> str:
    """
    Prefer the orchestrator/UI log path env var if provided.
    Why: your CLI prints UI event log paths and operators expect this env name.
    """
    return (
        os.getenv("CONNECTOR_UI_EVENT_LOG")
        or os.getenv("CONNECTOR_EVENT_LOG")  # backward compat
        or "connector_events.jsonl"
    )


def _safe_jsonable(fields: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (fields or {}).items():
        try:
            json.dumps(v, default=str)
            out[k] = v
        except Exception:
            out[k] = repr(v)
    return out


def log_event(rid: str, *, level: str, event: str, **fields: Any) -> None:
    payload_fields = _safe_jsonable(fields)

    rec = {
        "ts": _now_iso(),
        "rid": rid,
        "level": level,
        "event": event,
        **payload_fields,
    }

    # 1) File log (best-effort)
    try:
        with open(event_log_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, default=str) + "\n")
    except Exception:
        pass

    # 2) Runtime event bus (best-effort)
    try:
        emit(
            "message",
            event,
            level=str(level).lower(),
            **payload_fields,
        )
    except Exception:
        pass


# =========================================
# SECTION B — AUTH + CONFIG ACCESS
# Why: avoid importing config in this file; accept cfg duck-typed.
# =========================================
def _get(cfg: Any, name: str, default: Any) -> Any:
    return getattr(cfg, name, default)


def auth_headers(cfg: Any) -> Dict[str, str]:
    token = _get(cfg, "access_token", None) or _get(cfg, "token", None) or _get(cfg, "api_key", None)
    version = str(_get(cfg, "intercom_version", "2.10"))
    headers: Dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Intercom-Version": version,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


# =========================================
# SECTION C — TIMEOUTS + RETRIES DEFAULTS
# Why: prevent “stuck on starting” by ensuring requests ALWAYS have timeouts.
# =========================================
@dataclass(frozen=True)
class _Timeouts:
    connect: float
    read: float


def _timeouts(cfg: Any, timeout: Optional[float]) -> _Timeouts:
    connect_s = float(_get(cfg, "request_connect_timeout_s", 10.0))
    if timeout is not None:
        read_s = float(timeout)
    else:
        read_s = float(_get(cfg, "request_timeout_s", 60.0))
    return _Timeouts(connect=connect_s, read=read_s)


def _max_retries(cfg: Any) -> int:
    return int(_get(cfg, "request_retries", 2))


def _base_backoff_s(cfg: Any) -> float:
    return float(_get(cfg, "retry_sleep_s", 1.0))


def _jitter() -> float:
    return random.random() * 0.25


# =========================================
# SECTION D — HTTP CORE
# Why: centralized, safe requests wrapper with:
#   - ALWAYS enforced timeouts
#   - retries (429 / 5xx / transient network)
#   - structured logging + UI event emission
# =========================================
_TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def req_json(
    rid: str,
    cfg: Any,
    session: requests.Session,
    *,
    method: str,
    url: str,
    headers: Dict[str, str],
    stream: str,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> Any:
    t = _timeouts(cfg, timeout)
    retries = _max_retries(cfg)
    backoff = _base_backoff_s(cfg)

    last_err: Optional[BaseException] = None
    last_status: Optional[int] = None
    last_body_preview: Optional[str] = None

    for attempt in range(retries + 1):
        try:
            log_event(
                rid,
                level="INFO",
                event="http.request.start",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                connect_timeout_s=t.connect,
                read_timeout_s=t.read,
            )

            t0 = time.monotonic()
            resp = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=payload,
                timeout=(t.connect, t.read),
            )
            elapsed_ms = int((time.monotonic() - t0) * 1000)

            last_status = resp.status_code

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else (backoff * (attempt + 1))
                log_event(
                    rid,
                    level="WARN",
                    event="http.request.429",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    retry_after=retry_after,
                    sleep_s=sleep_s,
                )
                if attempt >= retries:
                    resp.raise_for_status()
                time.sleep(sleep_s + _jitter())
                continue

            if resp.status_code in _TRANSIENT_STATUSES and attempt < retries:
                last_body_preview = resp.text[:1000] if resp.text else None
                log_event(
                    rid,
                    level="WARN",
                    event="http.request.transient",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    sleep_s=(backoff * (attempt + 1)),
                )
                time.sleep(backoff * (attempt + 1) + _jitter())
                continue

            resp.raise_for_status()

            if not resp.content:
                log_event(
                    rid,
                    level="INFO",
                    event="http.request.ok",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    keys_count=0,
                )
                return None

            try:
                data = resp.json()
            except Exception:
                body_preview = resp.text[:2000] if resp.text else ""
                log_event(
                    rid,
                    level="ERROR",
                    event="http.request.json_decode_error",
                    stream=stream,
                    method=method,
                    url=url,
                    attempt=attempt,
                    status=resp.status_code,
                    elapsed_ms=elapsed_ms,
                    body_preview=body_preview,
                )
                raise ValueError(f"Non-JSON response from Intercom: status={resp.status_code} body={body_preview}")

            keys_count = len(data) if isinstance(data, dict) else None
            items_count = None
            if isinstance(data, dict):
                for k in ("data", "conversations", "tickets", "contacts", "companies"):
                    v = data.get(k)
                    if isinstance(v, list):
                        items_count = len(v)
                        break

            log_event(
                rid,
                level="INFO",
                event="http.request.ok",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                status=resp.status_code,
                elapsed_ms=elapsed_ms,
                keys_count=keys_count,
                items_count=items_count,
            )
            return data

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            log_event(
                rid,
                level="WARN",
                event="http.request.network_error",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                error=repr(e)[:2000],
                connect_timeout_s=t.connect,
                read_timeout_s=t.read,
            )
            if attempt >= retries:
                raise
            time.sleep(backoff * (attempt + 1) + _jitter())
            continue

        except requests.HTTPError as e:
            last_err = e
            try:
                last_body_preview = e.response.text[:2000] if e.response is not None and e.response.text else None
            except Exception:
                last_body_preview = None

            log_event(
                rid,
                level="ERROR" if attempt >= retries else "WARN",
                event="http.request.http_error",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                status=last_status,
                body_preview=last_body_preview,
                error=repr(e)[:2000],
            )

            if last_status in _TRANSIENT_STATUSES and attempt < retries:
                time.sleep(backoff * (attempt + 1) + _jitter())
                continue
            raise

        except Exception as e:
            last_err = e
            log_event(
                rid,
                level="ERROR",
                event="http.request.unhandled_error",
                stream=stream,
                method=method,
                url=url,
                attempt=attempt,
                status=last_status,
                error=repr(e)[:2000],
            )
            raise

    msg = f"req_json failed: stream={stream} url={url} status={last_status} err={repr(last_err)}"
    raise RuntimeError(msg)
