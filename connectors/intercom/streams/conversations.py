from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from ..client import IntercomClient
from ..pagination import PageAdvance, iterate_pages
from ..state import StateStore


@dataclass(frozen=True)
class ConversationsStreamConfig:
    page_size: int = 150
    state_key: str = "intercom_conversations_starting_after"
    max_pages_per_run: Optional[int] = None


class ConversationsStream:
    def __init__(
        self,
        client: IntercomClient,
        state: StateStore,
        cfg: ConversationsStreamConfig = ConversationsStreamConfig(),
    ) -> None:
        self._client = client
        self._state = state
        self._cfg = cfg

    def read(self) -> Iterator[Dict[str, Any]]:
        starting_after = self._state.get(self._cfg.state_key)

        initial_params: Dict[str, Any] = {"per_page": self._cfg.page_size}
        if starting_after:
            initial_params["starting_after"] = starting_after

        def fetch(params: Dict[str, Any]) -> Dict[str, Any]:
            return self._client.get("/conversations", params=params)

        def get_next(_params: Dict[str, Any], resp: Dict[str, Any]) -> Optional[PageAdvance]:
            pages = resp.get("pages") or {}
            nxt = pages.get("next")
            if not nxt:
                return None

            if isinstance(nxt, dict):
                nxt_starting_after = nxt.get("starting_after")
                if not nxt_starting_after:
                    return None
                new_params = dict(initial_params)
                new_params["starting_after"] = nxt_starting_after
                return PageAdvance(params=new_params, fingerprint=str(nxt_starting_after))

            if isinstance(nxt, str):
                nxt_starting_after = _extract_query_param(nxt, "starting_after")
                if not nxt_starting_after:
                    return None
                new_params = dict(initial_params)
                new_params["starting_after"] = nxt_starting_after
                return PageAdvance(params=new_params, fingerprint=str(nxt_starting_after))

            return None

        for _page_idx, req_params, resp in iterate_pages(
            fetch_page=fetch,
            initial_params=initial_params,
            get_next=get_next,
            max_pages=self._cfg.max_pages_per_run,
        ):
            conversations = resp.get("conversations") or resp.get("data") or []
            if not isinstance(conversations, list):
                conversations = []

            for c in conversations:
                if isinstance(c, dict):
                    yield c

            last_id = _last_conversation_id(conversations)
            if last_id:
                self._state.set(self._cfg.state_key, last_id)
            else:
                cur = req_params.get("starting_after")
                if cur:
                    self._state.set(self._cfg.state_key, cur)


def _extract_query_param(url: str, key: str) -> Optional[str]:
    try:
        from urllib.parse import urlparse, parse_qs

        q = parse_qs(urlparse(url).query)
        vals = q.get(key)
        if not vals:
            return None
        return vals[0]
    except Exception:
        return None


def _last_conversation_id(conversations: List[Any]) -> Optional[str]:
    for item in reversed(conversations):
        if isinstance(item, dict):
            v = item.get("id")
            if isinstance(v, str) and v:
                return v
    return None
