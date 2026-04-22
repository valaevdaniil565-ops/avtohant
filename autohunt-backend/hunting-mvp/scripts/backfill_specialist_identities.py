from __future__ import annotations

import argparse
import json
import re
from typing import Any

from sqlalchemy import text

from app.config import get_settings
from app.db.repo import Repo, build_search_text
from app.services.availability import resolve_specialist_is_available
from app.use_cases import matching as matching_use_cases
from app.use_cases.entities import _resolve_own_bench_url
from app.use_cases.extraction import build_structured_specialist_item


_URL_RE = re.compile(r"https?://[^\s,;]+", re.IGNORECASE)
_GRADE_RE = re.compile(r"(?i)\b(intern|junior|middle\+?|senior|lead|principal|staff|architect|head)\b")
_BLOCK_START_RE = re.compile(r"(?m)^\s*[✅✔]\s*")
_ROLEISH_NAME_STOPWORDS = {"авто", "qa", "aqa", "java", "python", "php", "devops", "project", "product", "ux/ui", "ui/ux"}
_ROLE_MARKERS = {
    "qa", "aqa", "designer", "developer", "engineer", "manager", "analyst", "architect", "devops",
    "java", "python", "php", "golang", "go", "react", "frontend", "backend", "fullstack", "ux/ui", "ui/ux",
    "ios", "android", "flutter", "kotlin", "1c", "sap", "project", "product",
}
_ROLE_PREFIX_MODIFIERS = {"авто", "auto", "manual", "graphic", "графический", "ux/ui", "ui/ux"}


def _parse_embedding_text(value: Any) -> list[float]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    out: list[float] = []
    for item in parsed:
        try:
            out.append(float(item))
        except Exception:
            return []
    return out


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-zа-яё0-9]+", "", str(value or "").strip().lower())


def _header_matches(normalized: str, *tokens: str) -> bool:
    return any(token in normalized for token in tokens)


def _first_url(value: Any) -> str | None:
    if isinstance(value, list):
        for item in value:
            match = _first_url(item)
            if match:
                return match
        return None
    match = _URL_RE.search(str(value or ""))
    return match.group(0).strip() if match else None


def _extract_fields_from_row_map(row_map: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for raw_key, raw_value in (row_map or {}).items():
        key = _normalize_header(str(raw_key or ""))
        value = raw_value
        if value in (None, ""):
            continue
        if _header_matches(key, "имя", "фио", "candidate", "specialist", "fullname", "resourcename"):
            fields.setdefault("name", str(value).strip())
        elif _header_matches(key, "стек", "stack", "technology", "skills"):
            fields.setdefault("stack", value)
        elif _header_matches(key, "роль", "позиция", "role", "position"):
            fields.setdefault("role", str(value).strip())
        elif _header_matches(key, "грейд", "grade", "seniority"):
            fields.setdefault("grade", str(value).strip())
        elif _header_matches(key, "локация", "город", "location", "city"):
            fields.setdefault("location", str(value).strip())
        elif _header_matches(key, "готовностьквыходу", "availability", "availablefrom", "доступность"):
            fields.setdefault("availability", str(value).strip())
        elif _header_matches(key, "английский", "english", "englishlevel"):
            fields.setdefault("english", str(value).strip())
        elif _header_matches(key, "ставка", "rate", "salary", "price"):
            fields.setdefault("rate_min", _extract_rate_min(value))
        elif _header_matches(key, "currency", "валюта"):
            fields.setdefault("currency", str(value).strip())
        elif _header_matches(key, "ссылканарезюме", "ссылканацв", "resumeurl", "cvurl", "resume", "cv", "doclink"):
            resume_url = _first_url(value)
            if resume_url:
                fields.setdefault("resume_url", resume_url)
    return fields


def _extract_labeled_value(raw_text: str, *labels: str) -> str | None:
    patterns = [re.escape(label) for label in labels if label]
    if not patterns:
        return None
    pattern = re.compile(rf"(?im)^(?:{'|'.join(patterns)})\s*:\s*(.+?)\s*$")
    match = pattern.search(raw_text or "")
    if not match:
        return None
    value = str(match.group(1) or "").strip()
    return value or None


def _extract_rate_min(value: Any) -> int | None:
    text_value = str(value or "")
    numbers = re.findall(r"\d+", text_value.replace(" ", ""))
    if not numbers:
        return None
    try:
        return int(numbers[0])
    except Exception:
        return None


def _split_bench_blocks(raw_text: str) -> list[str]:
    text_value = str(raw_text or "").strip()
    if not text_value:
        return []
    if _BLOCK_START_RE.search(text_value):
        pieces = re.split(r"(?m)(?=^\s*[✅✔]\s*)", text_value)
        return [piece.strip() for piece in pieces if piece.strip()]
    if "\n\n" in text_value:
        return [piece.strip() for piece in re.split(r"\n\s*\n", text_value) if piece.strip()]
    return [text_value]


def _pick_indexed_block(raw_text: str, source_meta: dict[str, Any]) -> str:
    blocks = _split_bench_blocks(raw_text)
    if not blocks:
        return str(raw_text or "")
    source_index = source_meta.get("source_index")
    try:
        idx = int(source_index)
    except Exception:
        idx = 1
    if 1 <= idx <= len(blocks):
        return blocks[idx - 1]
    return blocks[0]


def _extract_first_line_role_name(block_text: str) -> dict[str, Any]:
    first_line = ""
    for line in (block_text or "").splitlines():
        candidate = line.strip().lstrip("✅✔").strip()
        if candidate:
            first_line = candidate
            break
    if not first_line:
        return {}

    out: dict[str, Any] = {}
    grade_match = _GRADE_RE.search(first_line)
    if grade_match:
        out["grade"] = grade_match.group(1)
        role_segment = first_line[: grade_match.start()].strip(" -/|,")
    else:
        role_segment = first_line

    parts = role_segment.split()
    role_start_idx = None
    for idx, token in enumerate(parts):
        normalized = token.strip().lower()
        if normalized in _ROLE_MARKERS:
            role_start_idx = idx
            break

    if role_start_idx is None:
        role_tokens = parts
    else:
        name_tokens = parts[:role_start_idx]
        role_tokens = parts[role_start_idx:]
        while name_tokens and name_tokens[-1].strip().lower() in _ROLE_PREFIX_MODIFIERS:
            role_tokens.insert(0, name_tokens.pop())
        if name_tokens and len(name_tokens) == 1 and name_tokens[0].strip().lower() not in _ROLEISH_NAME_STOPWORDS:
            out["name"] = name_tokens[0]
        elif len(name_tokens) >= 2 and all(token[:1].isupper() for token in name_tokens[:2]):
            out["name"] = " ".join(name_tokens[:2]).strip()

    if role_tokens:
        out["role"] = " ".join(role_tokens).strip()
    elif role_segment:
        out["role"] = role_segment.strip()
    return out


def _extract_fields_from_raw_text(raw_text: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    cleaned_text = re.sub(r"(?m)^\s*[·•▪]\s*", "", raw_text or "")
    name = _extract_labeled_value(cleaned_text, "Имя", "Name", "Candidate", "Specialist")
    role = _extract_labeled_value(cleaned_text, "Роль", "Role")
    stack = _extract_labeled_value(cleaned_text, "Стек", "Stack")
    grade = _extract_labeled_value(cleaned_text, "Грейд", "Grade")
    location = _extract_labeled_value(cleaned_text, "Локация", "Location", "Город", "City")
    availability = _extract_labeled_value(cleaned_text, "Готовность к выходу", "Availability")
    english = _extract_labeled_value(cleaned_text, "Уровень английского", "English")
    resume_url = _extract_labeled_value(cleaned_text, "Резюме", "Resume", "CV", "Ссылки на резюме")
    rate = _extract_labeled_value(cleaned_text, "Ставка", "Rate", "Salary")

    first_line_fields = _extract_first_line_role_name(cleaned_text)
    if not name and first_line_fields.get("name"):
        name = str(first_line_fields["name"])
    if not role and first_line_fields.get("role"):
        role = str(first_line_fields["role"])
    if not grade and first_line_fields.get("grade"):
        grade = str(first_line_fields["grade"])

    if not location:
        location_match = re.search(r"(?im)\bлокация\b\s*:?\s*(.+?)\s*$", cleaned_text)
        if location_match:
            location = location_match.group(1).strip()
    if not rate:
        rate_match = re.search(r"(?im)\b(?:рейт|ставка|rate|salary)\b\s*:?\s*(.+?)\s*$", cleaned_text)
        if rate_match:
            rate = rate_match.group(1).strip()

    if name:
        fields["name"] = name
    if role:
        fields["role"] = role
    if stack:
        fields["stack"] = stack
    if grade:
        fields["grade"] = grade
    if location:
        fields["location"] = location
    if availability:
        fields["availability"] = availability
    if english:
        fields["english"] = english
    if rate:
        fields["rate_min"] = _extract_rate_min(rate)
    if resume_url:
        fields["resume_url"] = _first_url(resume_url) or resume_url
    return fields


def _collect_structured_fields(source_meta: dict[str, Any], raw_text: str, specialist_original_text: str = "") -> dict[str, Any] | None:
    structured_fields = source_meta.get("structured_fields")
    if isinstance(structured_fields, dict) and structured_fields:
        return dict(structured_fields)

    row_map = source_meta.get("row_map")
    fields: dict[str, Any] = {}
    if isinstance(row_map, dict) and row_map:
        fields.update(_extract_fields_from_row_map(row_map))

    indexed_text = _pick_indexed_block(raw_text, source_meta)
    raw_fields = _extract_fields_from_raw_text(indexed_text)
    for key, value in raw_fields.items():
        if value not in (None, "", []):
            fields.setdefault(key, value)

    if not fields and specialist_original_text:
        original_fields = _extract_fields_from_raw_text(specialist_original_text)
        for key, value in original_fields.items():
            if value not in (None, "", []):
                fields.setdefault(key, value)

    return fields or None


def _load_specialist_sources(engine, *, include_own_registry: bool) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
          src.id AS source_id,
          src.entity_id AS old_specialist_id,
          src.source_type,
          src.chat_title,
          src.external_url,
          src.external_kind,
          src.external_locator,
          src.raw_text,
          src.source_meta,
          src.created_at,
          COALESCE(sp.is_internal, FALSE) AS old_is_internal,
          sp.original_text AS specialist_original_text
        FROM sources src
        LEFT JOIN specialists sp ON sp.id = src.entity_id
        WHERE src.entity_type = 'specialist'
          AND (:include_own_registry OR src.source_type <> 'own_registry_sync')
        ORDER BY src.created_at ASC, src.id ASC
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"include_own_registry": include_own_registry}).mappings().all()
    return [dict(row) for row in rows]


def _is_internal_source(row: dict[str, Any]) -> bool:
    source_type = str(row.get("source_type") or "").strip().lower()
    external_kind = str(row.get("external_kind") or "").strip().lower()
    chat_title = str(row.get("chat_title") or "").strip().lower()
    if source_type == "own_registry_sync":
        return True
    if external_kind == "own_registry":
        return True
    if chat_title == "own_specialists_registry":
        return True
    return bool(row.get("old_is_internal"))


def _update_source_entity_id(engine, *, source_id: str, specialist_id: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE sources SET entity_id = CAST(:entity_id AS uuid) WHERE id = CAST(:source_id AS uuid)"),
            {"entity_id": specialist_id, "source_id": source_id},
        )


def _hide_orphan_specialists(engine) -> int:
    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                UPDATE specialists s
                SET status = 'hidden',
                    updated_at = NOW()
                WHERE s.status <> 'hidden'
                  AND NOT EXISTS (
                    SELECT 1
                    FROM sources src
                    WHERE src.entity_type = 'specialist'
                      AND src.entity_id = s.id
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM own_specialists_registry reg
                    WHERE reg.specialist_id = s.id
                      AND reg.is_active = TRUE
                  )
                """
            )
        )
    return int(result.rowcount or 0)


def _load_active_vacancies(engine) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
          id,
          role,
          stack,
          grade,
          rate_min,
          rate_max,
          currency,
          company,
          location,
          description,
          original_text,
          embedding::text AS embedding_text
        FROM vacancies
        WHERE status = 'active'
          AND (expires_at IS NULL OR expires_at > NOW())
        ORDER BY updated_at DESC
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query).mappings().all()
    return [dict(row) for row in rows]


def _rebuild_matches(engine) -> int:
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM matches"))

    own_bench_url = _resolve_own_bench_url(engine)
    total = 0
    for vacancy in _load_active_vacancies(engine):
        query_emb = _parse_embedding_text(vacancy.get("embedding_text"))
        hits = matching_use_cases.search_specialists(
            engine,
            query_emb,
            build_search_text(vacancy),
            50,
            own_bench_url=own_bench_url,
            vector_dim=768,
            vector_str_fn=lambda values: "[" + ",".join(f"{float(v):.8f}" for v in values) + "]",
        )
        ranked_hits, _ = matching_use_cases.rank_specialist_hits(vacancy, hits)
        if ranked_hits:
            matching_use_cases.upsert_matches(engine, str(vacancy["id"]), ranked_hits[:20])
            total += min(len(ranked_hits), 20)
    return total


def run_backfill(*, apply_changes: bool, include_own_registry: bool, rebuild_matches: bool) -> dict[str, int]:
    settings = get_settings()
    repo = Repo(settings.DATABASE_URL)
    engine = repo.engine

    rows = _load_specialist_sources(engine, include_own_registry=include_own_registry)
    stats = {
        "sources_total": len(rows),
        "sources_rebuilt": 0,
        "sources_relinked": 0,
        "sources_skipped": 0,
        "orphan_specialists_hidden": 0,
        "matches_rebuilt": 0,
    }

    for row in rows:
        raw_text = str(row.get("raw_text") or "").strip()
        source_meta = row.get("source_meta") or {}
        if not isinstance(source_meta, dict):
            source_meta = {}

        fields = _collect_structured_fields(source_meta, raw_text, str(row.get("specialist_original_text") or ""))
        if not fields:
            stats["sources_skipped"] += 1
            continue

        item = build_structured_specialist_item(fields, raw_text)
        resume_url = _first_url(fields.get("resume_url")) or _first_url(item.get("source_urls"))
        if resume_url:
            item["resume_url"] = resume_url
            item.setdefault("source_urls", [resume_url])
        item["is_internal"] = _is_internal_source(row)
        item["is_available"] = resolve_specialist_is_available(item, raw_text)
        status = "active" if bool(item.get("is_available", True)) else "hired"

        specialist_id = repo.upsert_specialist(item, raw_text, None, status)
        stats["sources_rebuilt"] += 1

        old_specialist_id = str(row.get("old_specialist_id") or "").strip()
        if specialist_id != old_specialist_id:
            stats["sources_relinked"] += 1
            if apply_changes:
                _update_source_entity_id(engine, source_id=str(row["source_id"]), specialist_id=specialist_id)

    if apply_changes:
        stats["orphan_specialists_hidden"] = _hide_orphan_specialists(engine)
        if rebuild_matches:
            stats["matches_rebuilt"] = _rebuild_matches(engine)

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild specialist identities from saved sources and optionally rebuild matches.")
    parser.add_argument("--apply", action="store_true", help="Apply database changes. Without this flag script runs in dry-run mode.")
    parser.add_argument("--include-own-registry", action="store_true", help="Also rebuild sources from own_registry_sync.")
    parser.add_argument("--skip-match-rebuild", action="store_true", help="Do not rebuild matches after relinking sources.")
    args = parser.parse_args()

    stats = run_backfill(
        apply_changes=bool(args.apply),
        include_own_registry=bool(args.include_own_registry),
        rebuild_matches=not bool(args.skip_match_rebuild),
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
