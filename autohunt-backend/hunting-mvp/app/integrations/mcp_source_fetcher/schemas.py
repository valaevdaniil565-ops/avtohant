from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class NormalizedItem:
    text: str
    row_index: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceFetchResult:
    ok: bool
    source_type: str
    source_url: str
    items: list[NormalizedItem] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class UrlMatch:
    url: str
    source_type: str
