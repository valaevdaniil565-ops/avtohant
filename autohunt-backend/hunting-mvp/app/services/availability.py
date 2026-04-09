from __future__ import annotations

import re
from typing import Any

_UNAVAILABLE_PATTERNS = (
    re.compile(r"(?i)\bзанят\w*\b"),
    re.compile(r"(?i)\bна\s+проекте\b"),
    re.compile(r"(?i)\bвышел\b"),
    re.compile(r"(?i)\bустроил\w*\b"),
    re.compile(r"(?i)\bне\s+ищет\b"),
    re.compile(r"(?i)\bнеактуаль\w*\b"),
    re.compile(r"(?i)\bне\s+на\s+бенче\b"),
    re.compile(r"(?i)\bunavailable\b"),
    re.compile(r"(?i)\bon\s+project\b"),
    re.compile(r"(?i)\bengaged\b"),
    re.compile(r"(?i)\ballocated\b"),
    re.compile(r"(?i)\bbillable\b"),
)

_AVAILABLE_PATTERNS = (
    re.compile(r"(?i)\bbench\b"),
    re.compile(r"(?i)\bavailable\b"),
    re.compile(r"(?i)\bfree\b"),
    re.compile(r"(?i)\bсвобод\w*\b"),
    re.compile(r"(?i)\bна\s+бенче\b"),
    re.compile(r"(?i)\bосвободил\w*\b"),
    re.compile(r"(?i)\bищу\s+проект\b"),
    re.compile(r"(?i)\bищу\s+работу\b"),
    re.compile(r"(?i)\bдоступ\w*\b"),
)


def resolve_specialist_is_available(data: dict[str, Any], raw_text: str) -> bool:
    text = (raw_text or "").strip()
    low_text = text.lower()

    if any(p.search(low_text) for p in _UNAVAILABLE_PATTERNS):
        return False

    weeks = data.get("availability_weeks")
    if isinstance(weeks, (int, float)) and weeks > 0:
        return False

    if any(p.search(low_text) for p in _AVAILABLE_PATTERNS):
        return True

    return bool(data.get("is_available", True))
