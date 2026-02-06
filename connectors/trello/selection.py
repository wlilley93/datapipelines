from __future__ import annotations
from typing import Any, Set

def normalize_selection(selection: Any) -> Set[str]:
    """
    Normalize selection to a set of stream names.
    Empty set means "all streams selected".
    """
    if selection is None:
        return set()

    for attr in ("streams", "tables", "resources"):
        val = getattr(selection, attr, None)
        if isinstance(val, (list, set, tuple)):
            return set(str(x) for x in val) if val else set()

    if isinstance(selection, dict):
        for key in ("streams", "tables", "resources"):
            val = selection.get(key)
            if isinstance(val, list):
                return set(str(x) for x in val) if val else set()

    return set()

def is_selected(selected: Set[str], stream_name: str) -> bool:
    return len(selected) == 0 or stream_name in selected
