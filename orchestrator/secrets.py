from __future__ import annotations

import os
from typing import Any, Dict, Optional, Mapping

import tomlkit
from rich.console import Console

from .constants import SECRETS_FILE
from .config import load_config_v2
from .locking import file_lock

console = Console()


def secure_secrets_file() -> None:
    if os.path.exists(SECRETS_FILE) and os.name != "nt":
        try:
            os.chmod(SECRETS_FILE, 0o600)
        except Exception:
            pass


def _as_plain_dict(value: Any) -> Dict[str, Any]:
    """
    tomlkit may return container-ish objects (tables) that act like dicts.
    We normalize to a plain python dict, shallowly.
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def update_secret(source_name: str, secret_data: Dict[str, Any], *, merge: bool = True) -> None:
    """
    Update a connection's secrets.

    Default behavior is MERGE into existing secrets (merge=True):
      - This prevents flows like Xero OAuth refresh/code exchange from overwriting
        and deleting core fields (client_id/client_secret/redirect_uri).

    If merge=False:
      - We REPLACE the entire secret object for that source_name.
    """
    os.makedirs(".dlt", exist_ok=True)

    with file_lock(SECRETS_FILE) as f:
        content = f.read().strip()
        doc = tomlkit.parse(content) if content else tomlkit.document()

        if "sources" not in doc:
            doc["sources"] = tomlkit.table()

        existing_raw = doc["sources"].get(source_name)
        existing = _as_plain_dict(existing_raw)
        incoming = _as_plain_dict(secret_data)

        if merge:
            # Shallow merge is intentional: token refresh updates a few keys,
            # while preserving client_id/client_secret/redirect_uri/scopes/etc.
            merged = dict(existing)
            merged.update(incoming)
            doc["sources"][source_name] = merged
        else:
            # Replace entirely
            doc["sources"][source_name] = incoming

        f.seek(0)
        f.write(tomlkit.dumps(doc))
        f.truncate()

    secure_secrets_file()


def get_secret(source_name: str) -> Dict[str, Any]:
    if not os.path.exists(SECRETS_FILE):
        raise Exception("Secrets file missing. Configure Postgres/Connections first.")
    with open(SECRETS_FILE, "r", encoding="utf-8") as f:
        doc = tomlkit.parse(f.read())
    return dict(doc.get("sources", {}).get(source_name, {}))


def get_postgres_credentials() -> Optional[Dict[str, Any]]:
    if not os.path.exists(SECRETS_FILE):
        return None
    try:
        with open(SECRETS_FILE, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
        if "destination" in doc and "postgres" in doc["destination"]:
            creds = dict(doc["destination"]["postgres"]["credentials"])
            # Optional CLI-configured overrides (db_mode + db_overrides)
            try:
                cfg = load_config_v2()
                settings = cfg.get("settings") or {}
                db_mode = (settings.get("db_mode") or "local").strip()
                db_overrides = settings.get("db_overrides") or {}
                mode_overrides = db_overrides.get(db_mode) or {}
                host_override = mode_overrides.get("host")
                port_override = mode_overrides.get("port")
                if host_override:
                    creds["host"] = str(host_override)
                if port_override:
                    try:
                        creds["port"] = int(port_override)
                    except (TypeError, ValueError):
                        pass
            except Exception:
                pass
            # Allow environment overrides for Docker compatibility
            if os.getenv("POSTGRES_HOST_OVERRIDE"):
                creds["host"] = os.getenv("POSTGRES_HOST_OVERRIDE")
            if os.getenv("POSTGRES_PORT_OVERRIDE"):
                try:
                    creds["port"] = int(os.getenv("POSTGRES_PORT_OVERRIDE", ""))
                except (TypeError, ValueError):
                    pass
            return creds
    except Exception:
        return None
    return None


def delete_secret(source_name: str) -> None:
    """Remove a source entry from secrets.toml."""
    if not os.path.exists(SECRETS_FILE):
        return
    with file_lock(SECRETS_FILE) as f:
        content = f.read().strip()
        if not content:
            return
        doc = tomlkit.parse(content)
        sources = doc.get("sources")
        if sources and source_name in sources:
            del sources[source_name]
            f.seek(0)
            f.write(tomlkit.dumps(doc))
            f.truncate()
    secure_secrets_file()
