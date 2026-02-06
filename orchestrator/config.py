from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from rich.console import Console

from connectors.runtime.state import normalise_state

from .constants import CONFIG_FILE, CONFIG_VERSION
from .locking import file_lock

console = Console()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_schema(schema: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() or ch == "_" else "_" for ch in (schema or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    s = s.strip("_")
    return s or "default_schema"


def sanitize_name(name: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() or ch in ("_", "-") else "_" for ch in (name or "").strip())
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_") or "connection"


def save_config_v2(cfg: Dict[str, Any]) -> None:
    try:
        with file_lock(CONFIG_FILE) as f:
            f.seek(0)
            json.dump(cfg, f, indent=2)
            f.truncate()
    except Exception as e:
        console.print(f"[red]ERROR: Failed to save config: {e}[/red]")


def load_config_v2() -> Dict[str, Any]:
    """
    Config v2 format:
    {
      "config_version": 2,
      "connections": [
        {"name":..., "type":..., "schema":..., "enabled": true, "created_at":..., "state": {...}}
      ]
    }

    Migration from v1 (list of connections) is automatic.
    """
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return {"config_version": CONFIG_VERSION, "connections": []}
            data = json.loads(raw)
    except FileNotFoundError:
        return {"config_version": CONFIG_VERSION, "connections": []}
    except Exception as e:
        console.print(f"[red]ERROR: Corrupted {CONFIG_FILE}: {e}[/red]")
        return {"config_version": CONFIG_VERSION, "connections": []}

    # v1 -> v2 migration
    if isinstance(data, list):
        migrated = {"config_version": CONFIG_VERSION, "connections": []}
        for c in data:
            if not isinstance(c, dict):
                continue
            conn = dict(c)
            conn.setdefault("schema", conn.get("name", "").replace("-", "_"))
            conn.setdefault("enabled", True)
            conn.setdefault("created_at", _now_iso())
            conn.setdefault("state", {"version": 1, "global": {}, "streams": {}})
            migrated["connections"].append(conn)
        save_config_v2(migrated)
        return migrated

    if not isinstance(data, dict):
        return {"config_version": CONFIG_VERSION, "connections": []}

    data.setdefault("config_version", CONFIG_VERSION)
    data.setdefault("connections", [])
    if not isinstance(data["connections"], list):
        data["connections"] = []
    data.setdefault("settings", {})
    if not isinstance(data["settings"], dict):
        data["settings"] = {}
    settings = data["settings"]
    settings.setdefault("db_mode", "local")
    settings.setdefault("db_overrides", {})
    if not isinstance(settings["db_overrides"], dict):
        settings["db_overrides"] = {}

    for conn in data["connections"]:
        if not isinstance(conn, dict):
            continue
        conn.setdefault("schema", conn.get("name", "").replace("-", "_"))
        conn.setdefault("enabled", True)
        conn.setdefault("created_at", _now_iso())
        conn.setdefault("state", {"version": 1, "global": {}, "streams": {}})
        conn["state"] = normalise_state(conn.get("state"))

    return data


def list_connections(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    conns = cfg.get("connections") or []
    return [c for c in conns if isinstance(c, dict)]


def get_connection(cfg: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    for c in list_connections(cfg):
        if c.get("name") == name:
            return c
    return None


def delete_connection(cfg: Dict[str, Any], name: str) -> Dict[str, Any]:
    """Remove a connection by name and return updated cfg."""
    conns = list_connections(cfg)
    cfg["connections"] = [c for c in conns if c.get("name") != name]
    return cfg
