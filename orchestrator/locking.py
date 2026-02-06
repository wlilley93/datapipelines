from __future__ import annotations

import os
from contextlib import contextmanager

import portalocker


@contextmanager
def file_lock(file_path: str):
    """Cross-platform exclusive lock around config/secrets writes."""
    abs_path = os.path.abspath(file_path)
    parent = os.path.dirname(abs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    if not os.path.exists(abs_path):
        open(abs_path, "w", encoding="utf-8").close()

    with open(abs_path, "r+", encoding="utf-8") as f:
        portalocker.lock(f, portalocker.LOCK_EX)
        try:
            yield f
        finally:
            portalocker.unlock(f)
