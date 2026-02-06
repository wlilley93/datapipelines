from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict

from .constants import RUNS_DIR


def _runs_path_for_today() -> str:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(RUNS_DIR, day)
    os.makedirs(path, exist_ok=True)
    return path


def write_run_log(payload: Dict[str, Any]) -> str:
    path = _runs_path_for_today()
    # Include time in filename for easy sorting: HH:MM (24-hour format)
    timestamp = datetime.now(timezone.utc).strftime("%H:%M")
    name = payload.get("connection_name", "unknown")
    run_id = payload.get("run_id", "unknown")
    filename = f"{timestamp}_{name}_{run_id}.json".replace(os.sep, "_")
    full = os.path.join(path, filename)
    with open(full, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return full
