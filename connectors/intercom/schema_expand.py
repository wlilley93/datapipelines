from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from .config import IntercomConfig
from .constants import BASE_URL
from .http import log_event, req_json
from .pagination import extract_items


def sanitize_col(name: str) -> str:
    out = []
    for ch in str(name):
        if ch.isalnum() or ch == "_":
            out.append(ch.lower())
        elif ch in (" ", "-", ".", ":", "/"):
            out.append("_")
    s = "".join(out).strip("_")
    while "__" in s:
        s = s.replace("__", "_")
    return s or "field"


def flatten_primitives(obj: Any, *, prefix: str = "", depth: int = 0, max_depth: int = 2) -> Dict[str, Any]:
    if depth > max_depth:
        return {}

    out: Dict[str, Any] = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            key = sanitize_col(k)
            p = f"{prefix}{key}" if prefix else key
            out.update(flatten_primitives(v, prefix=f"{p}__", depth=depth + 1, max_depth=max_depth))
        return out

    if isinstance(obj, list):
        return {}

    if obj is None or isinstance(obj, (str, int, float, bool)):
        if prefix.endswith("__"):
            out[prefix[:-2]] = obj
        elif prefix:
            out[prefix] = obj
        return out

    return {}


def _coerce_by_type(v: Any, t: Optional[str]) -> Any:
    if v is None:
        return None
    tt = (t or "").lower()
    try:
        if tt in ("integer", "int"):
            return int(v)
        if tt in ("float", "number", "decimal"):
            return float(v)
        if tt in ("boolean", "bool"):
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in ("1", "true", "yes", "y", "on")
        return v
    except Exception:
        return v


def fetch_data_attributes(
    rid: str, cfg: IntercomConfig, session, headers: Dict[str, str]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Returns: { model: { attribute_name: attribute_definition_dict } }

    Behavior:
    - BEST-EFFORT: skip unsupported models (422) instead of failing the run.
    """
    candidate_models = ("contact", "user", "lead", "company", "conversation", "ticket")
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for model in candidate_models:
        stream = f"data_attributes:{model}"
        url = f"{BASE_URL}/data_attributes"
        params = {"model": model}

        try:
            resp = req_json(rid, cfg, session, method="GET", url=url, headers=headers, params=params, stream=stream)
        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            body = None
            try:
                body = getattr(getattr(e, "response", None), "text", None)
            except Exception:
                body = None

            if status == 422:
                log_event(
                    rid,
                    level="WARN",
                    event="data_attributes_model_unsupported",
                    model=model,
                    status=status,
                    body=(body[:500] if isinstance(body, str) else None),
                )
                out[model] = {}
                continue

            raise
        except Exception as e:
            log_event(rid, level="ERROR", event="data_attributes_load_err", model=model, error=str(e)[:2000])
            raise

        items = extract_items(resp, fallback_keys=("data", "data_attributes"))

        defs: Dict[str, Dict[str, Any]] = {}
        for it in items[: cfg.max_custom_attr_columns]:
            if not isinstance(it, dict):
                continue
            name = it.get("name") or it.get("full_name") or it.get("label")
            if not name:
                continue
            defs[str(name)] = it

        out[model] = defs
        log_event(rid, level="INFO", event="data_attributes_loaded", model=model, count=len(defs))

    return out


def expand_custom_attributes(
    attrs: Any,
    *,
    cfg: IntercomConfig,
    attr_defs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    if not isinstance(attrs, dict):
        return {}

    out: Dict[str, Any] = {}
    count = 0

    for k, v in attrs.items():
        if count >= cfg.max_custom_attr_columns:
            break

        d = attr_defs.get(k) or {}
        dtype = d.get("data_type") or d.get("type")

        col = f"ca__{sanitize_col(k)}"
        out[col] = _coerce_by_type(v, str(dtype) if dtype else None)
        count += 1

    return out
