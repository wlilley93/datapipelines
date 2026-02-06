from __future__ import annotations

from time import perf_counter
from typing import Any, Dict, Optional, Tuple

from .events import debug, warn
from .utils_bridge import DEFAULT_TIMEOUT, request_json

def sanitize_params_for_logging(params: Dict[str, Any]) -> Dict[str, Any]:
    safe: Dict[str, Any] = {"param_keys": sorted(str(k) for k in params.keys())}
    for key in ("fields", "limit", "before", "since", "members", "attachments", "customFieldItems", "filter"):
        if key in params:
            val = params[key]
            safe[key] = val if isinstance(val, (str, int, float, bool)) else str(val)
    return safe

def get_response_details(err: Exception) -> Tuple[Optional[int], str]:
    resp = getattr(err, "response", None)
    status_code = getattr(resp, "status_code", None) if resp is not None else None
    text = ""
    try:
        text = getattr(resp, "text", "") or ""
    except Exception:
        text = ""
    return status_code if isinstance(status_code, int) else None, text

def request_json_logged(
    session,
    method: str,
    url: str,
    *,
    params: Dict[str, Any],
    timeout: int = DEFAULT_TIMEOUT,
    stream: Optional[str] = None,
    board_id: Optional[str] = None,
    op: Optional[str] = None,
) -> Any:
    t0 = perf_counter()
    debug(
        "http.request.start",
        stream=stream,
        op=op,
        method=method,
        url=url,
        board_id=board_id,
        **sanitize_params_for_logging(params),
    )

    try:
        data = request_json(session, method, url, params=params, timeout=timeout)
        elapsed_ms = int((perf_counter() - t0) * 1000)

        size_info: Dict[str, Any] = {}
        if isinstance(data, list):
            size_info["items_count"] = len(data)
        elif isinstance(data, dict):
            size_info["keys_count"] = len(data)
        else:
            size_info["type"] = type(data).__name__

        debug(
            "http.request.ok",
            stream=stream,
            op=op,
            elapsed_ms=elapsed_ms,
            board_id=board_id,
            **size_info,
        )
        return data

    except Exception as e:
        elapsed_ms = int((perf_counter() - t0) * 1000)
        debug(
            "http.request.error",
            stream=stream,
            op=op,
            elapsed_ms=elapsed_ms,
            board_id=board_id,
            error_type=type(e).__name__,
            error=str(e)[:1000],
        )
        raise

def as_list(data: Any, *, stream: str, op: str, board_id: Optional[str] = None) -> list[Any]:
    if isinstance(data, list):
        return data
    warn("http.payload.unexpected", stream=stream, op=op, board_id=board_id, expected="list", got=type(data).__name__)
    return []

def as_dict(data: Any, *, stream: str, op: str, board_id: Optional[str] = None) -> Dict[str, Any]:
    if isinstance(data, dict):
        return data
    warn("http.payload.unexpected", stream=stream, op=op, board_id=board_id, expected="dict", got=type(data).__name__)
    return {}
