# connectors/hubspot/state.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class HubSpotRuntimeConfig:
    """
    Runtime tweakables stored in connection state, so behaviour is consistent across runs.

    NOTE: assoc must use default_factory (fixes: "mutable default <class 'list'> for field assoc...").
    """
    objects: list[str] = field(default_factory=list)
    excluded_streams: list[str] = field(default_factory=list)
    include_custom_objects: bool = True
    incremental: bool = True
    # if set, forces an initial "full" pull for specified objects even if state exists.
    force_full_objects: list[str] = field(default_factory=list)


def get_global(state: Dict[str, Any]) -> Dict[str, Any]:
    g = state.get("global")
    if not isinstance(g, dict):
        state["global"] = {}
        g = state["global"]
    return g


def get_bookmarks(state: Dict[str, Any]) -> Dict[str, Any]:
    g = get_global(state)
    b = g.get("bookmarks")
    if not isinstance(b, dict):
        g["bookmarks"] = {}
        b = g["bookmarks"]
    return b


def get_runtime_config(state: Dict[str, Any]) -> HubSpotRuntimeConfig:
    g = get_global(state)
    cfg = g.get("hubspot_runtime") or {}
    if not isinstance(cfg, dict):
        cfg = {}
    return HubSpotRuntimeConfig(
        objects=list(cfg.get("objects") or []),
        excluded_streams=list(cfg.get("excluded_streams") or []),
        include_custom_objects=bool(cfg.get("include_custom_objects", True)),
        incremental=bool(cfg.get("incremental", True)),
        force_full_objects=list(cfg.get("force_full_objects") or []),
    )


def set_runtime_config(state: Dict[str, Any], cfg: HubSpotRuntimeConfig) -> None:
    g = get_global(state)
    g["hubspot_runtime"] = {
        "objects": list(cfg.objects),
        "excluded_streams": list(cfg.excluded_streams),
        "include_custom_objects": bool(cfg.include_custom_objects),
        "incremental": bool(cfg.incremental),
        "force_full_objects": list(cfg.force_full_objects),
    }


def get_last_cursor(state: Dict[str, Any], stream: str) -> Optional[str]:
    b = get_bookmarks(state)
    v = b.get(stream)
    return v if isinstance(v, str) and v else None


def set_last_cursor(state: Dict[str, Any], stream: str, cursor: str) -> None:
    b = get_bookmarks(state)
    b[stream] = cursor
