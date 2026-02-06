from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class StateStore:
    """
    Why: your logs show conversations repeatedly fetching page 1.
    This store exists so the stream can checkpoint paging cursor after each page.

    Swap get/set with your persistence (db/file/kv). This default is in-memory.
    """

    _state: Dict[str, Any]

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._state[key] = value

    def snapshot(self) -> Dict[str, Any]:
        return dict(self._state)
