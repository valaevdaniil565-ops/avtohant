from __future__ import annotations

import json
import logging
import re
from typing import Any

from app.pipeline import preprocess_for_llm

log = logging.getLogger(__name__)

_ROLE_LIKE_NAME_RE = re.compile(
    r"(?i)\b("
    r"developer|engineer|designer|manager|architect|analyst|consultant|specialist|candidate|"
    r"разработчик|дизайнер|менеджер|архитектор|аналитик|специалист|кандидат|"
    r"frontend|backend|fullstack|qa|devops|python|java|react|flutter|android|ios|mobile|middle|senior|junior|lead"
    r")\b"
)


def safe_json_loads(model_text: str) -> dict[str, Any]:
    try:
        t = (model_text or "").strip()
        if t.startswith("```"):
            t = t.strip("`")
            t = t.replace("json", "", 1).strip()
        start = t.find("{")
        end = t.rfind("}")
        if start != -1 and end != -1 and end > start:
            t = t[start : end + 1]
        parsed = json.loads(t)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        log.warning("safe_json_loads: failed to parse model response (len=%s)", len(model_text or ""))
        return {}


def truncate_text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text_value = re.sub(r"\s+", " ", str(value)).strip()
    if not text_value:
        return None
    return text_value[:limit]


def looks_like_stack_blob(value: str) -> bool:
    candidate = (value or "").strip()
    if not candidate:
        return False
    if len(candidate) > 120:
        return True
    if re.search(r"(?i)\b(langs?|databases?|devops|stack|tools?|frameworks?)\s*:", candidate):
        return True
    if candidate.count(",") >= 5 or candidate.count("\n") >= 2:
        return True
    return False


def looks_like_urlish(value: str) -> bool:
    candidate = (value or "").strip().lower()
    return bool(candidate and ("http://" in candidate or "https://" in candidate or "docs.google.com/" in candidate or "t.me/" in candidate))


def normalize_stack_token(value: str) -> str:
    s = (value or "").strip().lower()
    s = s.replace(".net", "dotnet").replace("c#", "csharp")
    s = re.sub(r"[^a-z0-9а-я]+", "", s)
    aliases = {
        "са": "systemanalyst",
        "ca": "systemanalyst",
        "системныйаналитик": "systemanalyst",
        "systemanalyst": "systemanalyst",
        "ба": "businessanalyst",
        "бизнесаналитик": "businessanalyst",
        "businessanalyst": "businessanalyst",
        "qaengineer": "qa",
    }
    return aliases.get(s, s)


def sanitize_stack_values(value: Any) -> list[str]:
    raw_values: list[str] = []
    if isinstance(value, list):
        raw_values = [str(v) for v in value if str(v).strip()]
    elif isinstance(value, str) and value.strip():
        raw_values = [value]

    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        parts = re.split(r"[\n,;]|(?:\s{2,})", raw)
        for part in parts:
            cleaned = re.sub(r"(?i)^(langs?|databases?|devops|stack|tools?|frameworks?)\s*:\s*", "", part).strip()
            if not cleaned:
                continue
            if ":" in cleaned and len(cleaned) > 40:
                continue
            if len(cleaned) > 80:
                continue
            key = normalize_stack_token(cleaned)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
    return out[:20]


def coerce_entity_role(role: Any, *, stack: list[str], raw_unit_text: str, kind: str) -> str:
    candidate = str(role or "").strip()
    if candidate and not looks_like_stack_blob(candidate) and not looks_like_urlish(candidate):
        return truncate_text(candidate, 255) or "Unknown"

    pre = preprocess_for_llm(raw_unit_text, kind=kind)
    hinted = str((pre.hints.get("role") or "")).strip()
    if hinted and not looks_like_stack_blob(hinted) and not looks_like_urlish(hinted):
        return truncate_text(hinted, 255) or "Unknown"

    for token in stack:
        if re.search(r"(?i)\b(developer|engineer|analyst|designer|manager|architect|разработ|аналит|дизайн|менедж|архитект)\w*\b", token):
            return truncate_text(token, 255) or "Unknown"

    for token in stack:
        if not looks_like_urlish(token):
            return truncate_text(token, 255) or "Unknown"

    return "Unknown"


def coerce_person_name(fields: dict[str, Any], raw_unit_text: str, *, role: str | None = None) -> str | None:
    def _clean_name(value: Any) -> str | None:
        candidate = truncate_text(value, 120)
        if not candidate:
            return None
        if looks_like_urlish(candidate):
            return None
        if role and candidate.casefold() == str(role).strip().casefold():
            return None
        if any(ch.isdigit() for ch in candidate):
            return None
        if _ROLE_LIKE_NAME_RE.search(candidate):
            return None
        return candidate

    direct = _clean_name(fields.get("name"))
    if direct:
        return direct

    for line in (raw_unit_text or "").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        norm_key = re.sub(r"[^a-zа-яё0-9]+", " ", key.strip().lower()).strip()
        if any(token in norm_key for token in ("имя", "фио", "кандидат", "специалист", "name", "candidate", "specialist")):
            named = _clean_name(value)
            if named:
                return named

    for line in (raw_unit_text or "").splitlines()[:12]:
        candidate = line.strip()
        if not candidate or ":" in candidate or looks_like_urlish(candidate):
            continue
        if len(candidate.split()) > 4:
            continue
        if _ROLE_LIKE_NAME_RE.search(candidate):
            continue
        if re.match(r"^[A-ZА-ЯЁ][a-zа-яё]+(?:\s+[A-ZА-ЯЁ][a-zа-яё.]+){0,2}$", candidate):
            return candidate
    return None


def structured_rate(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def structured_description(fields: dict[str, Any], raw_unit_text: str) -> str:
    parts: list[str] = []
    for key in ("description", "requirements_text", "responsibilities_text", "english", "availability", "resume_url"):
        value = fields.get(key)
        if value:
            parts.append(str(value))
    parts.append(raw_unit_text.strip())
    return "\n".join(p for p in parts if p).strip()[:2000]


def build_structured_specialist_item(fields: dict[str, Any], raw_unit_text: str) -> dict[str, Any]:
    stack = sanitize_stack_values(fields.get("stack"))
    role = coerce_entity_role(fields.get("role"), stack=stack, raw_unit_text=raw_unit_text, kind="BENCH")
    stack = stack or ([role] if role != "Unknown" else [])
    description = structured_description(fields, raw_unit_text)
    availability = str(fields.get("availability") or "").lower()
    is_available = not bool(re.search(r"(?i)\b(not available|занят|недоступ|hired|off)\b", availability))
    return {
        "name": coerce_person_name(fields, raw_unit_text, role=role),
        "role": role,
        "stack": stack,
        "grade": truncate_text(fields.get("grade"), 50),
        "experience_years_min": None,
        "experience_years_max": None,
        "rate_min": structured_rate(fields.get("rate_min")),
        "rate_max": None,
        "currency": truncate_text(fields.get("currency"), 10),
        "rate_period": None,
        "rate_is_net": None,
        "location": truncate_text(fields.get("location"), 255),
        "work_format": truncate_text(fields.get("work_format"), 120),
        "timezone": None,
        "availability_weeks": 0 if availability in {"asap", "now", "сразу"} else None,
        "is_available": is_available,
        "contacts": [],
        "source_urls": [str(fields["resume_url"])] if fields.get("resume_url") else [],
        "languages": [str(fields["english"])] if fields.get("english") else [],
        "relocation": None,
        "description": description or None,
    }


def build_structured_vacancy_item(fields: dict[str, Any], raw_unit_text: str) -> dict[str, Any]:
    stack = sanitize_stack_values(fields.get("stack"))
    role = coerce_entity_role(fields.get("role"), stack=stack, raw_unit_text=raw_unit_text, kind="VACANCY")
    stack = stack or ([role] if role != "Unknown" else [])
    description = structured_description(fields, raw_unit_text)
    return {
        "role": role,
        "stack": stack,
        "grade": truncate_text(fields.get("grade"), 50),
        "experience_years_min": None,
        "experience_years_max": None,
        "rate_min": structured_rate(fields.get("rate_min")),
        "rate_max": None,
        "currency": truncate_text(fields.get("currency"), 10),
        "rate_period": None,
        "rate_is_net": None,
        "company": truncate_text(fields.get("company"), 255),
        "client": truncate_text(fields.get("client"), 255),
        "location": truncate_text(fields.get("location"), 255),
        "work_format": truncate_text(fields.get("work_format"), 120),
        "timezone": None,
        "employment_type": truncate_text(fields.get("employment_type"), 120),
        "start_date": None,
        "duration_months": None,
        "responsibilities": [str(fields["responsibilities_text"])] if fields.get("responsibilities_text") else [],
        "requirements": [str(fields["requirements_text"])] if fields.get("requirements_text") else [],
        "nice_to_have": [],
        "benefits": [],
        "contacts": [],
        "source_urls": [],
        "is_closed": False,
        "close_reason": None,
        "description": description,
    }
