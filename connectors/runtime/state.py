from __future__ import annotations

from typing import Any, Dict


def normalise_state(state: Any) -> Dict[str, Any]:
    """
    Normalise incoming connection state into a predictable structure.

    Preferred (protocol) shape:
      {
        "version": 1,
        "global": {...},
        "streams": {
          "<stream_name>": {...}
        }
      }

    Legacy connectors may have a flat dict; we wrap it into global.
    """
    if not isinstance(state, dict):
        return {"version": 1, "global": {}, "streams": {}}

    # Already protocol-shaped
    if "global" in state or "streams" in state:
        g = state.get("global")
        s = state.get("streams")
        return {
            "version": int(state.get("version") or 1),
            "global": g if isinstance(g, dict) else {},
            "streams": s if isinstance(s, dict) else {},
        }

    # Legacy flat dict -> wrap
    return {"version": 1, "global": dict(state), "streams": {}}


def merge_state(previous_state: Any, state_updates: Any) -> Dict[str, Any]:
    """
    Merge state updates into previous state.

    Rules:
      - We never destructively delete keys.
      - dict values merge shallowly at global + per-stream level.
      - Non-dict update values overwrite.
      - If updates are legacy-flat, they merge into global.
    """
    prev = normalise_state(previous_state)
    upd = normalise_state(state_updates)

    out: Dict[str, Any] = {
        "version": max(int(prev.get("version") or 1), int(upd.get("version") or 1)),
        "global": dict(prev.get("global") or {}),
        "streams": dict(prev.get("streams") or {}),
    }

    # merge global
    out_global = out["global"]
    for k, v in (upd.get("global") or {}).items():
        if isinstance(v, dict) and isinstance(out_global.get(k), dict):
            out_global[k] = {**out_global[k], **v}
        else:
            out_global[k] = v

    # merge streams
    out_streams = out["streams"]
    for stream_name, stream_update in (upd.get("streams") or {}).items():
        if not isinstance(stream_name, str) or not stream_name:
            continue

        existing = out_streams.get(stream_name)
        if isinstance(existing, dict) and isinstance(stream_update, dict):
            out_streams[stream_name] = {**existing, **stream_update}
        else:
            out_streams[stream_name] = stream_update

    return out
