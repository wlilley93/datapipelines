from __future__ import annotations

import re
from typing import List, Tuple

_REDACT_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r'("access_token"\s*:\s*")[^"]+(")', re.IGNORECASE), r"\1[REDACTED]\2"),
    (re.compile(r'("refresh_token"\s*:\s*")[^"]+(")', re.IGNORECASE), r"\1[REDACTED]\2"),
    (re.compile(r'("api_key"\s*:\s*")[^"]+(")', re.IGNORECASE), r"\1[REDACTED]\2"),
    (re.compile(r'("authorization"\s*:\s*")[^"]+(")', re.IGNORECASE), r"\1[REDACTED]\2"),
    (re.compile(r"(authorization\s*:\s*bearer\s+)[^\s]+", re.IGNORECASE), r"\1[REDACTED]"),
]


def redact_text(text: str, max_len: int = 1200) -> str:
    if not text:
        return ""
    out = text
    for pat, repl in _REDACT_PATTERNS:
        out = pat.sub(repl, out)
    return out[:max_len]
