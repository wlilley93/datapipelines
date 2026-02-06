from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional, Set, Tuple

from .constants import DEDUPE_WINDOW_SIZE, GLOBAL_FINGERPRINTS_SOFT_CAP, GLOBAL_SEEN_IDS_SOFT_CAP


@dataclass
class PageSignature:
    """
    Page signature for offset pagination loop detection.

    We hash (first_id, last_id, count). This is enough to catch the real-world
    “same page repeating” failure without expensive full-page hashing.
    """

    first_id: str
    last_id: str
    count: int

    def compact_hash(self) -> str:
        raw = f"{self.first_id}:{self.last_id}:{self.count}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


class _RollingSet:
    """
    Bounded set+deque eviction structure:
    - O(1) membership checks
    - bounded memory via FIFO eviction
    """

    def __init__(self, max_size: int):
        self._max_size = int(max_size)
        self._set: Set[str] = set()
        self._order: Deque[str] = deque()

    def seen(self, value: str) -> bool:
        if value in self._set:
            return True
        self._set.add(value)
        self._order.append(value)
        while len(self._set) > self._max_size:
            oldest = self._order.popleft()
            self._set.discard(oldest)
        return False


class DedupeTracker:
    """
    Track seen transcript IDs + recent page signatures.

    - Rolling window catches local duplicates quickly.
    - Optional global set enables exact run-wide uniqueness until cap is exceeded.
    """

    def __init__(
        self,
        max_size: int = DEDUPE_WINDOW_SIZE,
        *,
        global_seen_soft_cap: int = GLOBAL_SEEN_IDS_SOFT_CAP,
        page_sig_window: int = 200,
    ):
        self._id_window = _RollingSet(max_size=max_size)

        self._global_seen_soft_cap = int(global_seen_soft_cap)
        self._global_seen: Optional[Set[str]] = set()  # becomes None after cap exceeded

        self._page_sigs: Set[str] = set()
        self._page_sig_order: Deque[str] = deque()
        self._page_sig_window = int(page_sig_window)

    def was_seen(self, id_value: str) -> Tuple[bool, bool]:
        """
        Returns (is_duplicate, tracked_globally).
        """
        dup_window = self._id_window.seen(id_value)

        tracked_globally = self._global_seen is not None
        dup_global = False
        if self._global_seen is not None:
            dup_global = id_value in self._global_seen
            self._global_seen.add(id_value)
            if len(self._global_seen) > self._global_seen_soft_cap:
                self._global_seen = None
                tracked_globally = False

        return (dup_window or dup_global), tracked_globally

    def record_page_signature(self, sig: PageSignature) -> bool:
        """
        Returns True if this page signature has been seen recently (likely loop).
        """
        h = sig.compact_hash()
        if h in self._page_sigs:
            return True

        self._page_sigs.add(h)
        self._page_sig_order.append(h)

        while len(self._page_sigs) > self._page_sig_window:
            oldest = self._page_sig_order.popleft()
            self._page_sigs.discard(oldest)

        return False


class FingerprintDedupeTracker:
    """
    Track content fingerprints to collapse “same transcript JSONB fields” duplicates
    even when transcript UUID differs.

    Semantics mirror DedupeTracker:
    - rolling window always on
    - global set on until cap, then disabled
    """

    def __init__(
        self,
        max_size: int = DEDUPE_WINDOW_SIZE,
        *,
        global_seen_soft_cap: int = GLOBAL_FINGERPRINTS_SOFT_CAP,
    ):
        self._fp_window = _RollingSet(max_size=max_size)
        self._global_seen_soft_cap = int(global_seen_soft_cap)
        self._global_seen: Optional[Set[str]] = set()

    def was_seen(self, fingerprint: str) -> Tuple[bool, bool]:
        dup_window = self._fp_window.seen(fingerprint)

        tracked_globally = self._global_seen is not None
        dup_global = False
        if self._global_seen is not None:
            dup_global = fingerprint in self._global_seen
            self._global_seen.add(fingerprint)
            if len(self._global_seen) > self._global_seen_soft_cap:
                self._global_seen = None
                tracked_globally = False

        return (dup_window or dup_global), tracked_globally
