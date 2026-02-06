from __future__ import annotations
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

def _type_tag(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "other"

def _merge_type(a: Optional[str], b: str) -> str:
    if not a:
        return b
    if a == b:
        return a
    if {a, b}.issubset({"int", "float"}):
        return "number"
    if a == "null":
        return b
    if b == "null":
        return a
    return "mixed"

@dataclass
class SchemaCaps:
    max_records_per_stream: int
    list_items_cap: int

class SchemaTracker:
    """
    Lightweight shape/type observer with hard caps.
    Tracks per stream: { field_path -> type_tag } with bounded recursion.
    """

    def __init__(self, *, prior_snapshot: Optional[Dict[str, Any]], caps: SchemaCaps):
        self._caps = caps
        self._seen_records: Dict[str, int] = {}
        self._streams: Dict[str, Dict[str, str]] = {}
        self._prior = prior_snapshot if isinstance(prior_snapshot, dict) else {}

    def records_seen(self, stream: Optional[str] = None) -> Any:
        if stream is None:
            return dict(self._seen_records)
        return self._seen_records.get(stream, 0)

    def _can_observe_stream(self, stream: str) -> bool:
        return self._seen_records.get(stream, 0) < int(self._caps.max_records_per_stream)

    def observe(self, stream: str, record: Dict[str, Any]) -> None:
        if not isinstance(record, dict):
            return
        if not self._can_observe_stream(stream):
            return

        self._seen_records[stream] = self._seen_records.get(stream, 0) + 1
        smap = self._streams.setdefault(stream, {})
        self._observe_value(smap, prefix="", value=record)

    def _observe_value(self, smap: Dict[str, str], prefix: str, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                if not isinstance(k, str) or not k:
                    continue
                path = f"{prefix}.{k}" if prefix else k
                smap[path] = _merge_type(smap.get(path), _type_tag(v))

                if isinstance(v, dict):
                    self._observe_value(smap, prefix=path, value=v)
                elif isinstance(v, list):
                    self._observe_list(smap, prefix=path, value=v)
            return

        if prefix:
            smap[prefix] = _merge_type(smap.get(prefix), _type_tag(value))

    def _observe_list(self, smap: Dict[str, str], prefix: str, value: list[Any]) -> None:
        elem_types: Optional[str] = None
        n = 0
        for item in value:
            n += 1
            if int(self._caps.list_items_cap) > 0 and n > int(self._caps.list_items_cap):
                break
            t = _type_tag(item)
            elem_types = _merge_type(elem_types, t)

            if isinstance(item, dict):
                for k, v in item.items():
                    if not isinstance(k, str) or not k:
                        continue
                    child_path = f"{prefix}[].{k}"
                    smap[child_path] = _merge_type(smap.get(child_path), _type_tag(v))
                    if isinstance(v, dict):
                        self._observe_value(smap, prefix=child_path, value=v)

        if elem_types:
            key = f"{prefix}[]"
            smap[key] = _merge_type(smap.get(key), elem_types)

    def snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for stream, fields in self._streams.items():
            out[stream] = {k: fields[k] for k in sorted(fields.keys())}
        return out

    def diff_against_prior(self) -> Dict[str, Any]:
        prior = self._prior if isinstance(self._prior, dict) else {}
        current = self.snapshot()
        all_streams = sorted(set(prior.keys()) | set(current.keys()))
        diff: Dict[str, Any] = {}

        for s in all_streams:
            p = prior.get(s) if isinstance(prior.get(s), dict) else {}
            c = current.get(s) if isinstance(current.get(s), dict) else {}

            p_keys = set(p.keys())
            c_keys = set(c.keys())

            added = {k: c[k] for k in sorted(c_keys - p_keys)}
            removed = {k: p[k] for k in sorted(p_keys - c_keys)}
            changed: Dict[str, Any] = {}
            for k in sorted(p_keys & c_keys):
                if p.get(k) != c.get(k):
                    changed[k] = {"from": p.get(k), "to": c.get(k)}

            if added or removed or changed:
                diff[s] = {"added": added, "removed": removed, "changed_types": changed}

        return diff

def stable_hash(obj: Any) -> str:
    data = json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(data).hexdigest()
