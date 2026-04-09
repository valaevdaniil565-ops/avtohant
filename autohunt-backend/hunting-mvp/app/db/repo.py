# app/db/repo.py
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Optional

from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)

VECTOR_DIM = 768


def generate_synthetic_id(role: str, stack: list[str], grade: Optional[str], rate_hint: Optional[int]) -> str:
    role_n = (role or "").strip().lower()
    grade_n = (grade or "").strip().lower()
    stack_n = sorted([s.strip().lower() for s in (stack or []) if s and s.strip()])
    rate_bucket = (rate_hint or 0) // 10000
    normalized = f"{role_n}|{stack_n}|{grade_n}|{rate_bucket}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def build_search_text(entity: dict) -> str:
    parts: list[str] = []
    if entity.get("role"):
        parts.append(f"Роль: {entity['role']}")
    if entity.get("stack"):
        parts.append("Технологии: " + ", ".join(entity["stack"]))
    if entity.get("grade"):
        parts.append(f"Уровень: {entity['grade']}")
    exp = entity.get("experience_years") or entity.get("experience_years_min")
    if exp:
        parts.append(f"Опыт: {exp} лет")
    if entity.get("description"):
        parts.append(str(entity["description"])[:500])
    return " | ".join(parts)


@dataclass
class SearchHit:
    id: str
    role: str
    stack: list[str]
    grade: Optional[str]
    rate_min: Optional[int]
    rate_max: Optional[int]
    currency: Optional[str]
    location: Optional[str]
    similarity: float
    source_url: Optional[str]


class Repo:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, pool_pre_ping=True)

    def upsert_vacancy(
        self,
        data: dict,
        original_text: str,
        embedding: Optional[list[float]],
        status: str,
        expires_days: int = 30,
    ) -> str:
        role = data.get("role") or "Unknown"
        stack = data.get("stack") or []
        grade = data.get("grade")
        exp = data.get("experience_years_min")
        if data.get("experience_years_min") == data.get("experience_years_max"):
            exp = data.get("experience_years_min")
        rate_hint = data.get("rate_min") or data.get("rate_max")
        syn = generate_synthetic_id(role, stack, grade, rate_hint)

        emb_json = json.dumps(embedding) if embedding and len(embedding) == VECTOR_DIM else None
        if embedding and len(embedding) != VECTOR_DIM:
            log.warning("Embedding dim %s != %s, storing NULL embedding", len(embedding), VECTOR_DIM)

        q = text(
            """
            INSERT INTO vacancies(
              synthetic_id, role, stack, grade, experience_years,
              rate_min, rate_max, currency, company, location,
              description, original_text, embedding, status, expires_at, close_reason, closed_at
            )
            VALUES (
              CAST(:syn AS varchar), CAST(:role AS text), CAST(:stack AS jsonb), CAST(:grade AS varchar), :exp,
              :rmin, :rmax, COALESCE(CAST(:cur AS varchar), 'RUB'), CAST(:company AS text), CAST(:loc AS text),
              CAST(:desc AS text), CAST(:orig AS text),
              CASE WHEN CAST(:emb AS text) IS NULL THEN NULL ELSE CAST(CAST(:emb AS text) AS vector) END,
              CAST(:status AS varchar),
              NOW() + (:expires_days || ' days')::interval,
              CAST(:close_reason AS text),
              CASE WHEN CAST(:status AS varchar)='closed' THEN NOW() ELSE NULL END
            )
            ON CONFLICT(synthetic_id) DO UPDATE
              SET updated_at = NOW()
            RETURNING id
            """
        )
        with self.engine.begin() as c:
            vid = c.execute(
                q,
                {
                    "syn": syn,
                    "role": role,
                    "stack": json.dumps(stack),
                    "grade": grade,
                    "exp": exp,
                    "rmin": data.get("rate_min"),
                    "rmax": data.get("rate_max"),
                    "cur": data.get("currency"),
                    "company": data.get("company"),
                    "loc": data.get("location"),
                    "desc": data.get("description") or None,
                    "orig": original_text,
                    "emb": emb_json,
                    "status": status,
                    "expires_days": expires_days,
                    "close_reason": data.get("close_reason"),
                },
            ).scalar_one()
        return str(vid)

    def upsert_specialist(
        self,
        data: dict,
        original_text: str,
        embedding: Optional[list[float]],
        status: str,
        expires_days: int = 30,
    ) -> str:
        role = data.get("role") or "Unknown"
        stack = data.get("stack") or []
        grade = data.get("grade")
        exp = data.get("experience_years_min")
        if data.get("experience_years_min") == data.get("experience_years_max"):
            exp = data.get("experience_years_min")
        rate_hint = data.get("rate_min") or data.get("rate_max")
        syn = generate_synthetic_id(role, stack, grade, rate_hint)

        emb_json = json.dumps(embedding) if embedding and len(embedding) == VECTOR_DIM else None
        if embedding and len(embedding) != VECTOR_DIM:
            log.warning("Embedding dim %s != %s, storing NULL embedding", len(embedding), VECTOR_DIM)

        q = text(
            """
            INSERT INTO specialists(
              synthetic_id, role, stack, grade, experience_years,
              rate_min, rate_max, currency, location,
              description, original_text, embedding, status, expires_at, hired_at
            )
            VALUES (
              CAST(:syn AS varchar), CAST(:role AS text), CAST(:stack AS jsonb), CAST(:grade AS varchar), :exp,
              :rmin, :rmax, COALESCE(CAST(:cur AS varchar), 'RUB'), CAST(:loc AS text),
              CAST(:desc AS text), CAST(:orig AS text),
              CASE WHEN CAST(:emb AS text) IS NULL THEN NULL ELSE CAST(CAST(:emb AS text) AS vector) END,
              CAST(:status AS varchar),
              NOW() + (:expires_days || ' days')::interval,
              CASE WHEN CAST(:status AS varchar)='hired' THEN NOW() ELSE NULL END
            )
            ON CONFLICT(synthetic_id) DO UPDATE
              SET updated_at = NOW()
            RETURNING id
            """
        )
        with self.engine.begin() as c:
            sid = c.execute(
                q,
                {
                    "syn": syn,
                    "role": role,
                    "stack": json.dumps(stack),
                    "grade": grade,
                    "exp": exp,
                    "rmin": data.get("rate_min"),
                    "rmax": data.get("rate_max"),
                    "cur": data.get("currency"),
                    "loc": data.get("location"),
                    "desc": data.get("description") or None,
                    "orig": original_text,
                    "emb": emb_json,
                    "status": status,
                    "expires_days": expires_days,
                },
            ).scalar_one()
        return str(sid)

    def insert_source(
        self,
        entity_type: str,
        entity_id: str,
        channel_id: int,
        message_id: int,
        chat_title: Optional[str],
        sender_id: Optional[int],
        sender_name: Optional[str],
        message_url: Optional[str],
        source_type: str,
        raw_text: Optional[str],
    ) -> None:
        q = text(
            """
            INSERT INTO sources(
              entity_type, entity_id,
              channel_id, message_id,
              chat_title, sender_id, sender_name,
              message_url, source_type, raw_text
            )
            VALUES (
              :etype, :eid,
              :cid, :mid,
              :ctitle, :sid, :sname,
              :url, :stype, :raw
            )
            ON CONFLICT(channel_id, message_id) DO NOTHING
            """
        )
        with self.engine.begin() as c:
            c.execute(
                q,
                {
                    "etype": entity_type,
                    "eid": entity_id,
                    "cid": channel_id,
                    "mid": message_id,
                    "ctitle": chat_title,
                    "sid": sender_id,
                    "sname": sender_name,
                    "url": message_url,
                    "stype": source_type,
                    "raw": raw_text,
                },
            )

    def search_specialists(self, query_emb: Optional[list[float]], query_text: str, limit: int = 10) -> list[SearchHit]:
        if query_emb and len(query_emb) == VECTOR_DIM:
            emb_json = json.dumps(query_emb)
            q = text(
                """
                SELECT
                  s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location,
                  GREATEST(0.0, LEAST(1.0, 1 - (s.embedding <=> CAST(:q AS vector)))) AS sim,
                  (
                    SELECT message_url FROM sources
                    WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                  ) AS url
                FROM specialists s
                WHERE s.status='active'
                  AND (s.expires_at IS NULL OR s.expires_at > NOW())
                  AND s.embedding IS NOT NULL
                ORDER BY s.embedding <=> CAST(:q AS vector)
                LIMIT :lim
                """
            )
            with self.engine.begin() as c:
                rows = c.execute(q, {"q": emb_json, "lim": limit}).mappings().all()
            return [
                SearchHit(
                    id=str(r["id"]),
                    role=r["role"],
                    stack=r["stack"] or [],
                    grade=r["grade"],
                    rate_min=r["rate_min"],
                    rate_max=r["rate_max"],
                    currency=r["currency"],
                    location=r["location"],
                    similarity=float(r["sim"] or 0.0),
                    source_url=r["url"],
                )
                for r in rows
            ]

        q = text(
            """
            SELECT
              s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location,
              0.0 AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url
            FROM specialists s
            WHERE s.status='active'
              AND (s.expires_at IS NULL OR s.expires_at > NOW())
              AND (
                LOWER(s.role) LIKE LOWER(:q)
                OR LOWER(COALESCE(s.description,'')) LIKE LOWER(:q)
                OR LOWER(s.original_text) LIKE LOWER(:q)
              )
            ORDER BY s.updated_at DESC
            LIMIT :lim
            """
        )
        with self.engine.begin() as c:
            rows = c.execute(q, {"q": f"%{query_text[:80]}%", "lim": limit}).mappings().all()
        return [
            SearchHit(
                id=str(r["id"]),
                role=r["role"],
                stack=r["stack"] or [],
                grade=r["grade"],
                rate_min=r["rate_min"],
                rate_max=r["rate_max"],
                currency=r["currency"],
                location=r["location"],
                similarity=float(r["sim"] or 0.0),
                source_url=r["url"],
            )
            for r in rows
        ]

    def search_vacancies(self, query_emb: Optional[list[float]], query_text: str, limit: int = 10) -> list[SearchHit]:
        if query_emb and len(query_emb) == VECTOR_DIM:
            emb_json = json.dumps(query_emb)
            q = text(
                """
                SELECT
                  v.id, v.role, v.stack, v.grade, v.rate_min, v.rate_max, v.currency, v.location,
                  GREATEST(0.0, LEAST(1.0, 1 - (v.embedding <=> CAST(:q AS vector)))) AS sim,
                  (
                    SELECT message_url FROM sources
                    WHERE entity_type='vacancy' AND entity_id=v.id AND message_url IS NOT NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                  ) AS url
                FROM vacancies v
                WHERE v.status='active'
                  AND (v.expires_at IS NULL OR v.expires_at > NOW())
                  AND v.embedding IS NOT NULL
                ORDER BY v.embedding <=> CAST(:q AS vector)
                LIMIT :lim
                """
            )
            with self.engine.begin() as c:
                rows = c.execute(q, {"q": emb_json, "lim": limit}).mappings().all()
            return [
                SearchHit(
                    id=str(r["id"]),
                    role=r["role"],
                    stack=r["stack"] or [],
                    grade=r["grade"],
                    rate_min=r["rate_min"],
                    rate_max=r["rate_max"],
                    currency=r["currency"],
                    location=r["location"],
                    similarity=float(r["sim"] or 0.0),
                    source_url=r["url"],
                )
                for r in rows
            ]

        q = text(
            """
            SELECT
              v.id, v.role, v.stack, v.grade, v.rate_min, v.rate_max, v.currency, v.location,
              0.0 AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='vacancy' AND entity_id=v.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url
            FROM vacancies v
            WHERE v.status='active'
              AND (v.expires_at IS NULL OR v.expires_at > NOW())
              AND (
                LOWER(v.role) LIKE LOWER(:q)
                OR LOWER(COALESCE(v.description,'')) LIKE LOWER(:q)
                OR LOWER(v.original_text) LIKE LOWER(:q)
              )
            ORDER BY v.updated_at DESC
            LIMIT :lim
            """
        )
        with self.engine.begin() as c:
            rows = c.execute(q, {"q": f"%{query_text[:80]}%", "lim": limit}).mappings().all()
        return [
            SearchHit(
                id=str(r["id"]),
                role=r["role"],
                stack=r["stack"] or [],
                grade=r["grade"],
                rate_min=r["rate_min"],
                rate_max=r["rate_max"],
                currency=r["currency"],
                location=r["location"],
                similarity=float(r["sim"] or 0.0),
                source_url=r["url"],
            )
            for r in rows
        ]

    def upsert_matches(self, vacancy_id: str, specialist_hits: list[SearchHit]) -> None:
        q = text(
            """
            INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
            VALUES (:vid, :sid, :score, :rank)
            ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
              SET similarity_score=EXCLUDED.similarity_score,
                  rank=EXCLUDED.rank,
                  updated_at=NOW()
            """
        )
        with self.engine.begin() as c:
            for i, h in enumerate(specialist_hits, start=1):
                c.execute(q, {"vid": vacancy_id, "sid": h.id, "score": float(h.similarity), "rank": i})

    def upsert_matches_reverse(self, specialist_id: str, vacancy_hits: list[SearchHit]) -> None:
        q = text(
            """
            INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
            VALUES (:vid, :sid, :score, :rank)
            ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
              SET similarity_score=EXCLUDED.similarity_score,
                  rank=EXCLUDED.rank,
                  updated_at=NOW()
            """
        )
        with self.engine.begin() as c:
            for i, h in enumerate(vacancy_hits, start=1):
                c.execute(q, {"vid": h.id, "sid": specialist_id, "score": float(h.similarity), "rank": i})

    def find_source_by_url(self, url: str) -> Optional[dict[str, Any]]:
        q = text(
            """
            SELECT id, entity_type, entity_id, message_url
            FROM sources
            WHERE message_url = :url
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        with self.engine.begin() as c:
            row = c.execute(q, {"url": url}).mappings().first()
        return dict(row) if row else None

    def hide_entity(self, entity_type: str, entity_id: str) -> None:
        if entity_type == "vacancy":
            q = text("UPDATE vacancies SET status='hidden', updated_at=NOW() WHERE id=:id")
        else:
            q = text("UPDATE specialists SET status='hidden', updated_at=NOW() WHERE id=:id")
        with self.engine.begin() as c:
            c.execute(q, {"id": entity_id})

    def export_rows(self, table: str, only_active: bool) -> tuple[list[str], list[tuple]]:
        where = ""
        if only_active and table in ("vacancies", "specialists"):
            where = "WHERE status='active'"
        q = text(f"SELECT * FROM {table} {where} ORDER BY created_at DESC")
        with self.engine.begin() as c:
            res = c.execute(q)
            cols = list(res.keys())
            rows = res.fetchall()
        return cols, rows

    def export_sources_rows(self, only_active: bool) -> tuple[list[str], list[tuple]]:
        if not only_active:
            q = text("SELECT * FROM sources ORDER BY created_at DESC")
        else:
            q = text(
                """
                SELECT s.*
                FROM sources s
                LEFT JOIN vacancies v ON s.entity_type='vacancy' AND s.entity_id=v.id
                LEFT JOIN specialists sp ON s.entity_type='specialist' AND s.entity_id=sp.id
                WHERE (v.id IS NOT NULL AND v.status='active')
                   OR (sp.id IS NOT NULL AND sp.status='active')
                ORDER BY s.created_at DESC
                """
            )
        with self.engine.begin() as c:
            res = c.execute(q)
            cols = list(res.keys())
            rows = res.fetchall()
        return cols, rows

    def export_matches_rows(self, only_active: bool) -> tuple[list[str], list[tuple]]:
        if not only_active:
            q = text("SELECT * FROM matches ORDER BY created_at DESC")
        else:
            q = text(
                """
                SELECT m.*
                FROM matches m
                JOIN vacancies v ON m.vacancy_id=v.id
                JOIN specialists s ON m.specialist_id=s.id
                WHERE v.status='active' AND s.status='active'
                ORDER BY m.created_at DESC
                """
            )
        with self.engine.begin() as c:
            res = c.execute(q)
            cols = list(res.keys())
            rows = res.fetchall()
        return cols, rows
