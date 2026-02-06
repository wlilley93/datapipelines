from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

from ..client import IntercomClient


SUPPORTED_MODELS = {"contact", "company", "conversation"}


@dataclass(frozen=True)
class DataAttributesStreamConfig:
    models: List[str]


class DataAttributesStream:
    def __init__(self, client: IntercomClient, cfg: Optional[DataAttributesStreamConfig] = None) -> None:
        self._client = client
        self._cfg = cfg or DataAttributesStreamConfig(models=["contact", "company", "conversation"])

    def read(self) -> Iterator[Dict[str, Any]]:
        for model in self._cfg.models:
            if model not in SUPPORTED_MODELS:
                continue
            resp = self._client.get("/data_attributes", params={"model": model})
            yield {"model": model, "data": resp.get("data", [])}
