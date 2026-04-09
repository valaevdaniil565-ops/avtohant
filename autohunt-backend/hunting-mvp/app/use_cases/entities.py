from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text

from app.db.repo import build_search_text
from app.services.app_settings import OWN_BENCH_SOURCE_URL_KEY, get_setting
from app.use_cases import matching as matching_use_cases


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
          e.stack,
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
          ) AS source_display
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


def list_specialists(engine, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    query = text(
        _entity_select("specialists", include_internal=True)
        + """
        WHERE e.status <> 'hidden'
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
          source_meta,
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
