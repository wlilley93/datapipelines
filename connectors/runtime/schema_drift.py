from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


SNAPSHOT_DIR = os.path.join(".dlt", "schema_snapshots")


class DriftPolicy(str, Enum):
    """
    Drift handling policy:
      - allow: record snapshots but do not warn/fail
      - warn: record + return summary marking breaking vs non-breaking
      - fail: raise if breaking drift detected
    """

    ALLOW = "allow"
    WARN = "warn"
    FAIL = "fail"


@dataclass(frozen=True)
class DriftDiff:
    added_fields: Dict[str, List[str]]
    removed_fields: Dict[str, List[str]]
    changed_types: Dict[str, List[Tuple[str, str, str]]]
    is_breaking: bool


def _ensure_dir() -> None:
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)


def _snapshot_path(connection_name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in (connection_name or "unknown"))
    return os.path.join(SNAPSHOT_DIR, f"{safe}.json")


def load_snapshot(connection_name: str) -> Optional[Dict[str, Any]]:
    _ensure_dir()
    path = _snapshot_path(connection_name)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_snapshot(connection_name: str, schema: Dict[str, Any]) -> str:
    _ensure_dir()
    path = _snapshot_path(connection_name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema or {}, f, indent=2, ensure_ascii=False, default=str)
    return path


def _field_map(stream_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Best-effort: accept JSONSchema-ish shapes:
      { "properties": { "field": { "type": "string" } ... } }
    """
    props = stream_schema.get("properties")
    if isinstance(props, dict):
        return props
    return {}


def _field_type(field_schema: Any) -> str:
    if isinstance(field_schema, dict):
        t = field_schema.get("type")
        if isinstance(t, str):
            return t
        if isinstance(t, list):
            return "|".join(str(x) for x in t)
    return "unknown"


def diff_schemas(old: Dict[str, Any], new: Dict[str, Any]) -> DriftDiff:
    """
    Compare two observed schema snapshots.

    Expected minimal shape:
      {
        "streams": {
          "<stream_name>": { "properties": { ... } }
        }
      }
    """
    old_streams = old.get("streams") if isinstance(old.get("streams"), dict) else {}
    new_streams = new.get("streams") if isinstance(new.get("streams"), dict) else {}

    added: Dict[str, List[str]] = {}
    removed: Dict[str, List[str]] = {}
    changed: Dict[str, List[Tuple[str, str, str]]] = {}

    all_streams = set(old_streams.keys()) | set(new_streams.keys())

    for s in sorted(all_streams):
        o = old_streams.get(s) if isinstance(old_streams.get(s), dict) else {}
        n = new_streams.get(s) if isinstance(new_streams.get(s), dict) else {}

        o_fields = _field_map(o)
        n_fields = _field_map(n)

        o_keys = set(o_fields.keys())
        n_keys = set(n_fields.keys())

        add_fields = sorted(list(n_keys - o_keys))
        rem_fields = sorted(list(o_keys - n_keys))

        if add_fields:
            added[s] = add_fields
        if rem_fields:
            removed[s] = rem_fields

        for k in sorted(list(o_keys & n_keys)):
            ot = _field_type(o_fields.get(k))
            nt = _field_type(n_fields.get(k))
            if ot != nt:
                changed.setdefault(s, []).append((k, ot, nt))

    is_breaking = bool(removed) or bool(changed)
    return DriftDiff(
        added_fields=added,
        removed_fields=removed,
        changed_types=changed,
        is_breaking=is_breaking,
    )


def enforce_policy(diff: DriftDiff, policy: DriftPolicy) -> None:
    """
    Apply drift policy. Only FAIL blocks execution.
    """
    if policy == DriftPolicy.ALLOW:
        return
    if policy == DriftPolicy.WARN:
        return
    if policy == DriftPolicy.FAIL and diff.is_breaking:
        raise Exception("Schema drift detected (breaking). Policy=fail.")


def summarise(diff: DriftDiff, max_lines: int = 40) -> str:
    """
    Human-readable diff summary for logs.
    """
    lines: List[str] = []
    if diff.added_fields:
        lines.append("Added fields:")
        for s, fs in diff.added_fields.items():
            lines.append(f"  - {s}: +{len(fs)} ({', '.join(fs[:10])}{'…' if len(fs) > 10 else ''})")
    if diff.removed_fields:
        lines.append("Removed fields:")
        for s, fs in diff.removed_fields.items():
            lines.append(f"  - {s}: -{len(fs)} ({', '.join(fs[:10])}{'…' if len(fs) > 10 else ''})")
    if diff.changed_types:
        lines.append("Type changes:")
        for s, changes in diff.changed_types.items():
            preview = ", ".join([f"{k}:{ot}->{nt}" for (k, ot, nt) in changes[:6]])
            lines.append(f"  - {s}: {len(changes)} ({preview}{'…' if len(changes) > 6 else ''})")

    if not lines:
        return "No schema drift detected."

    if len(lines) > max_lines:
        lines = lines[: max_lines - 1] + ["…(truncated)"]
    return "\n".join(lines)


def check_and_record_drift(
    *,
    connection_name: str,
    observed_schema: Dict[str, Any],
    policy: DriftPolicy = DriftPolicy.WARN,
) -> Dict[str, Any]:
    """
    Loads the last snapshot (if any), diffs against `observed_schema`, enforces policy,
    and saves the new snapshot.

    Returns a structured summary for run logs / CLI display.
    """
    prev = load_snapshot(connection_name) or {}
    diff = diff_schemas(prev, observed_schema or {})
    enforce_policy(diff, policy)
    path = save_snapshot(connection_name, observed_schema or {})

    return {
        "policy": policy.value,
        "snapshot_path": path,
        "is_breaking": diff.is_breaking,
        "summary": summarise(diff),
        "diff": {
            "added_fields": diff.added_fields,
            "removed_fields": diff.removed_fields,
            "changed_types": diff.changed_types,
        },
    }
