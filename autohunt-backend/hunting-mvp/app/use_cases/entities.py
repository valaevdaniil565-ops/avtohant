from __future__ import annotations

import json
import re
from typing import Any, Optional

from sqlalchemy import text

from app.db.repo import build_search_text
from app.services.app_settings import OWN_BENCH_SOURCE_URL_KEY, get_setting
from app.use_cases import matching as matching_use_cases

_MOJIBAKE_RE = re.compile(r"[ÐÑ]|Р[А-яA-Za-zЁё]|С[А-яA-Za-zЁё]|вЂ|Ѓ|�")


def _parse_embedding_text(value: Any) -> list[float]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [float(item) for item in parsed]
    except Exception:
        pass
    return []


def _looks_broken_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(_MOJIBAKE_RE.search(text))


def _repair_text_value(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    candidates = [text]
    for encoding in ("cp1251", "latin1"):
        try:
            repaired = text.encode(encoding, errors="ignore").decode("utf-8", errors="ignore").strip()
        except Exception:
            repaired = ""
        if repaired:
            candidates.append(repaired)

    def _score(candidate: str) -> tuple[int, int, int]:
        cyr = len(re.findall(r"[А-Яа-яЁё]", candidate))
        lat = len(re.findall(r"[A-Za-z]", candidate))
        penalty = 1000 if _looks_broken_text(candidate) else 0
        return (cyr + lat - penalty, cyr, lat)

    best = max(candidates, key=_score)
    if _looks_broken_text(best):
        return ""
    return best


def _clean_text(value: Any, fallback: str = "") -> str:
    cleaned = _repair_text_value(value)
    return cleaned or fallback


def _clean_list(values: Any) -> list[str]:
    raw_values = values if isinstance(values, list) else [values]
    cleaned: list[str] = []
    for value in raw_values:
        text = _clean_text(value)
        if text:
            cleaned.append(text)
    return cleaned


def _entity_select(table: str, *, include_company: bool = False, include_internal: bool = False) -> str:
    extra_cols: list[str] = []
    if include_company:
        extra_cols.append("e.company")
        extra_cols.append("e.is_strategic")
        extra_cols.append("e.close_reason")
        extra_cols.append("e.closed_at")
    if include_internal:
        extra_cols.append("e.is_internal")
        extra_cols.append("e.hired_at")
    extra_sql = ",\n          ".join(extra_cols)
    if extra_sql:
        extra_sql = ",\n          " + extra_sql
    return f"""
        SELECT
          e.id,
          e.role,
          e.stack::text AS stack,
          e.grade,
          e.experience_years,
          e.rate_min,
          e.rate_max,
          e.currency,
          e.location,
          e.description,
          e.original_text,
          e.status,
          e.created_at,
          e.updated_at,
          e.expires_at,
          e.embedding::text AS embedding_text
          {extra_sql},
          (
            SELECT s.message_url
            FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS source_url,
          (
            SELECT COALESCE(s.source_meta ->> 'source_display', s.message_url, s.external_url)
            FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS source_display,
          (
            SELECT s.source_meta::text
            FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS source_meta,
          (
            SELECT s.chat_title
            FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS chat_title,
          (
            SELECT s.sender_name
            FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS sender_name
        FROM {table} e
    """


def list_vacancies(engine, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    query = text(
        _entity_select("vacancies", include_company=True)
        + """
        WHERE e.status <> 'hidden'
        ORDER BY e.updated_at DESC
        LIMIT :limit OFFSET :offset
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"entity_type": "vacancy", "limit": limit, "offset": offset}).mappings().all()
    return [dict(row) for row in rows]


def get_vacancy(engine, vacancy_id: str) -> dict[str, Any] | None:
    query = text(
        _entity_select("vacancies", include_company=True)
        + """
        WHERE e.id = CAST(:entity_id AS uuid)
        LIMIT 1
        """
    )
    with engine.begin() as connection:
        row = connection.execute(query, {"entity_type": "vacancy", "entity_id": vacancy_id}).mappings().first()
    return dict(row) if row else None


def list_specialists(engine, *, limit: int = 50, offset: int = 0, bench_scope: str = "all") -> list[dict[str, Any]]:
    scope_sql = ""
    if bench_scope == "own":
        scope_sql = "AND COALESCE(e.is_internal, FALSE) = TRUE"
    elif bench_scope == "partner":
        scope_sql = "AND COALESCE(e.is_internal, FALSE) = FALSE"
    query = text(
        _entity_select("specialists", include_internal=True)
        + """
        WHERE e.status <> 'hidden'
        """
        + f"""
        {scope_sql}
        """
        + """
        ORDER BY e.updated_at DESC
        LIMIT :limit OFFSET :offset
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"entity_type": "specialist", "limit": limit, "offset": offset}).mappings().all()
    return [dict(row) for row in rows]


def get_specialist(engine, specialist_id: str) -> dict[str, Any] | None:
    query = text(
        _entity_select("specialists", include_internal=True)
        + """
        WHERE e.id = CAST(:entity_id AS uuid)
        LIMIT 1
        """
    )
    with engine.begin() as connection:
        row = connection.execute(query, {"entity_type": "specialist", "entity_id": specialist_id}).mappings().first()
    return dict(row) if row else None


def list_sources_for_entity(engine, entity_type: str, entity_id: str) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
          id,
          entity_type,
          entity_id,
          chat_title,
          sender_id,
          sender_name,
          message_url,
          external_url,
          external_kind,
          external_locator,
          source_type,
          raw_text,
          source_meta::text AS source_meta,
          created_at
        FROM sources
        WHERE entity_type = :entity_type
          AND entity_id = CAST(:entity_id AS uuid)
        ORDER BY created_at DESC
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"entity_type": entity_type, "entity_id": entity_id}).mappings().all()
    return [dict(row) for row in rows]


def _resolve_own_bench_url(engine) -> str:
    return str(get_setting(engine, OWN_BENCH_SOURCE_URL_KEY) or "").strip()


def get_vacancy_matches(engine, ollama, vacancy_id: str, *, limit: int = 20) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    vacancy = get_vacancy(engine, vacancy_id)
    if vacancy is None:
        return None, []
    query_emb = _parse_embedding_text(vacancy.get("embedding_text"))
    if not query_emb and ollama is not None:
        query_emb = ollama.embed(build_search_text(vacancy))
    hits = matching_use_cases.search_specialists(
        engine,
        query_emb,
        build_search_text(vacancy),
        limit,
        own_bench_url=_resolve_own_bench_url(engine),
        vector_dim=768,
        vector_str_fn=lambda values: "[" + ",".join(f"{float(v):.8f}" for v in values) + "]",
    )
    ranked_hits, _ = matching_use_cases.rank_specialist_hits(vacancy, hits)
    return vacancy, ranked_hits[:limit]


def get_specialist_matches(engine, ollama, specialist_id: str, *, limit: int = 20) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    specialist = get_specialist(engine, specialist_id)
    if specialist is None:
        return None, []
    query_emb = _parse_embedding_text(specialist.get("embedding_text"))
    if not query_emb and ollama is not None:
        query_emb = ollama.embed(build_search_text(specialist))
    hits = matching_use_cases.search_vacancies(
        engine,
        query_emb,
        build_search_text(specialist),
        limit,
        vector_dim=768,
        vector_str_fn=lambda values: "[" + ",".join(f"{float(v):.8f}" for v in values) + "]",
    )
    ranked_hits = matching_use_cases.rank_vacancy_hits(specialist, hits)
    return specialist, ranked_hits[:limit]


def _format_rate(hit: dict[str, Any]) -> str:
    rate_min = hit.get("rate_min")
    rate_max = hit.get("rate_max")
    currency = _clean_text(hit.get("currency"))
    if rate_min and rate_max:
        return f"от {rate_min} до {rate_max} {currency}".strip()
    if rate_min:
        return f"от {rate_min} {currency}".strip()
    if rate_max:
        return f"до {rate_max} {currency}".strip()
    return "—"


def _extract_manual_label(text_value: Any, labels: list[str]) -> str:
    source = _repair_text_value(text_value)
    if not source:
        return ""
    for label in labels:
        pattern = re.compile(rf"(?im)^(?:{label})\s*[:\-]\s*(.+?)\s*$")
        match = pattern.search(source)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _extract_manual_specialist_card_details(hit: dict[str, Any]) -> dict[str, str]:
    source_display = _repair_text_value(hit.get("source_display"))
    original_text = _repair_text_value(hit.get("original_text"))
    name = (
        _extract_manual_label(original_text, ["Имя", "Name", "Column 1"])
        or _extract_manual_label(source_display.replace("; ", "\n"), ["Специалист", "Кандидат", "Имя"])
    )
    location = (
        _extract_manual_label(original_text, ["Локация", "Location", "Город", "City"])
        or _clean_text(hit.get("location"))
    )
    sheet = (
        _extract_manual_label(source_display.replace("; ", "\n"), ["Лист", "Sheet"])
        or _extract_manual_label(source_display.replace("; ", "\n"), ["Таблица", "Table"])
    )
    row = (
        _extract_manual_label(original_text, ["Row"])
        or _extract_manual_label(source_display.replace("; ", "\n"), ["Строка", "Row"])
    )
    index = _extract_manual_label(source_display.replace("; ", "\n"), ["Индекс", "Index"])
    identity_parts = [part for part in [name, location] if part and part != "—"]
    reference_parts: list[str] = []
    if sheet:
        reference_parts.append(f"Лист: {sheet}")
    if row:
        reference_parts.append(f"Строка: {row}")
    elif index:
        reference_parts.append(f"Позиция: {index}")
    return {
        "display_name": _clean_text(name),
        "identity_hint": _clean_text(" · ".join(identity_parts)),
        "reference": _clean_text(" · ".join(reference_parts)),
    }


def _pick_manual_specialist_source_url(hit: dict[str, Any]) -> str | None:
    source_meta = hit.get("source_meta") or {}
    if not isinstance(source_meta, dict):
        source_meta = {}
    structured = source_meta.get("structured_fields") or {}
    row_map = source_meta.get("row_map") or {}
    if not isinstance(structured, dict):
        structured = {}
    if not isinstance(row_map, dict):
        row_map = {}
    for value in (
        structured.get("resume_url"),
        row_map.get("resume_url"),
        hit.get("specialist_live_resume_url"),
        hit.get("url"),
        hit.get("source_url"),
    ):
        text_value = str(value or "").strip()
        if text_value:
            return text_value
    return None


def _manual_item_from_specialist(hit: dict[str, Any], *, source_bucket: str, source_bucket_label: str) -> dict[str, Any]:
    components = hit.get("score_components") or {}
    details = _extract_manual_specialist_card_details(hit)
    return {
        "id": str(hit.get("id") or ""),
        "title": _clean_text(hit.get("role"), "Unknown"),
        "display_name": details["display_name"] or None,
        "role_label": _clean_text(hit.get("role"), "Unknown"),
        "subtitle": " • ".join(part for part in [_clean_text(hit.get("grade")), _clean_text(hit.get("location"))] if part) or None,
        "meta": _format_rate(hit),
        "source_url": _pick_manual_specialist_source_url(hit),
        "identity_hint": details["identity_hint"] or None,
        "reference": details["reference"] or None,
        "tags": _clean_list(hit.get("stack") or []),
        "overlap": _clean_list(components.get("stack_overlap") or []),
        "score": max(0, min(100, round(float(hit.get("sim") or 0.0) * 100))),
        "kind_label": "Бенч",
        "source_bucket": source_bucket,
        "source_bucket_label": _clean_text(source_bucket_label),
    }


def _manual_item_from_vacancy(hit: dict[str, Any]) -> dict[str, Any]:
    components = hit.get("score_components") or {}
    return {
        "id": str(hit.get("id") or ""),
        "title": _clean_text(hit.get("role"), "Unknown"),
        "display_name": None,
        "role_label": None,
        "subtitle": " • ".join(part for part in [_clean_text(hit.get("grade")), _clean_text(hit.get("location"))] if part) or None,
        "meta": _format_rate(hit),
        "source_url": str(hit.get("url") or hit.get("source_url") or "").strip() or None,
        "identity_hint": None,
        "reference": None,
        "tags": _clean_list(hit.get("stack") or []),
        "overlap": _clean_list(components.get("stack_overlap") or []),
        "score": max(0, min(100, round(float(hit.get("sim") or 0.0) * 100))),
        "kind_label": "Вакансия",
        "source_bucket": None,
        "source_bucket_label": None,
    }


def preview_manual_matches(engine, *, mode: str, text_value: str, limit: int = 10, rate: int | None = None) -> dict[str, Any]:
    query_entity, profile = matching_use_cases.build_manual_query_entity(text_value, mode=mode, rate_value=rate)
    matched_profile = str(profile.get("role") or "").strip() if profile else None

    if mode == "vacancy":
        title = "Топ-10 специалистов под вакансию"
        if not profile:
            return {
                "mode": mode,
                "title": title,
                "description": "Совпадений нет. Профиль вакансии не удалось определить по сохранённой карте соответствий.",
                "matched_profile": None,
                "items": [],
                "sections": [
                    {"id": "own", "title": "Наш бенч", "empty_text": "Нет специалиста", "items": []},
                    {"id": "partner", "title": "Партнёрский бенч", "empty_text": "Нет специалиста", "items": []},
                ],
            }

        all_hits = matching_use_cases.list_active_specialists_for_matching(engine, own_bench_url=_resolve_own_bench_url(engine))
        ranked_hits, _ = matching_use_cases.rank_specialist_hits(query_entity, all_hits)
        filtered = [hit for hit in ranked_hits if float(hit.get("sim") or 0.0) >= matching_use_cases.MATCH_THRESHOLD]
        if rate is not None:
            filtered = [hit for hit in filtered if float((hit.get("score_components") or {}).get("rate_score") or 0.0) > 0.0]
        own_hits = [hit for hit in filtered if bool(hit.get("is_own_bench_source"))][:limit]
        partner_hits = [hit for hit in filtered if not bool(hit.get("is_own_bench_source"))][:limit]
        own_items = [_manual_item_from_specialist(hit, source_bucket="own", source_bucket_label="Наш бенч") for hit in own_hits]
        partner_items = [_manual_item_from_specialist(hit, source_bucket="partner", source_bucket_label="Партнёрский бенч") for hit in partner_hits]
        return {
            "mode": mode,
            "title": title,
            "description": "Сначала показан наш бенч, затем партнёрский. Если точного совпадения нет, раздел остаётся пустым.",
            "matched_profile": matched_profile,
            "items": [*own_items, *partner_items],
            "sections": [
                {"id": "own", "title": "Наш бенч", "empty_text": "Нет специалиста", "items": own_items},
                {"id": "partner", "title": "Партнёрский бенч", "empty_text": "Нет специалиста", "items": partner_items},
            ],
        }

    title = "Топ-10 вакансий для специалиста"
    if not profile:
        return {
            "mode": mode,
            "title": title,
            "description": "Совпадений нет. Профиль специалиста не удалось определить по сохранённой карте соответствий.",
            "matched_profile": None,
            "items": [],
            "sections": [{"id": "vacancies", "title": "Вакансии", "empty_text": "Совпадений нет", "items": []}],
        }

    all_hits = matching_use_cases.list_active_vacancies_for_matching(engine)
    ranked_hits = matching_use_cases.rank_vacancy_hits(query_entity, all_hits)
    filtered = [hit for hit in ranked_hits if float(hit.get("sim") or 0.0) >= matching_use_cases.MATCH_THRESHOLD]
    if rate is not None:
        filtered = [hit for hit in filtered if float((hit.get("score_components") or {}).get("rate_score") or 0.0) > 0.0]
    filtered = filtered[:limit]
    items = [_manual_item_from_vacancy(hit) for hit in filtered]
    return {
        "mode": mode,
        "title": title,
        "description": "Показаны только точные совпадения по сохранённой карте соответствий.",
        "matched_profile": matched_profile,
        "items": items,
        "sections": [{"id": "vacancies", "title": "Вакансии", "empty_text": "Совпадений нет", "items": items}],
    }
