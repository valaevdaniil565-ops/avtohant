from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import text

MSK = ZoneInfo("Europe/Moscow")


def fetch_digest_rows(engine, table: str, ts_column: str, window_start: datetime, window_end: datetime) -> list[dict[str, Any]]:
    entity_type = "vacancy" if table == "vacancies" else "specialist"
    extra_where = ""
    if ts_column == "updated_at":
        extra_where = "AND e.created_at < :window_start AND e.updated_at > e.created_at"
    query = text(
        f"""
        SELECT
          e.id,
          e.role,
          e.grade,
          e.stack,
          e.location,
          e.status,
          e.created_at,
          e.updated_at,
          (
            SELECT message_url FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id AND s.message_url IS NOT NULL
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS url,
          (
            SELECT COALESCE(s.source_meta ->> 'source_display', s.message_url)
            FROM sources s
            WHERE s.entity_type = :entity_type AND s.entity_id = e.id
            ORDER BY s.created_at DESC
            LIMIT 1
          ) AS source_display
        FROM {table} e
        WHERE e.status <> 'hidden'
          AND e.{ts_column} >= :window_start
          AND e.{ts_column} < :window_end
          {extra_where}
        ORDER BY e.{ts_column} DESC
        LIMIT 20
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(
            query,
            {"entity_type": entity_type, "window_start": window_start, "window_end": window_end},
        ).mappings().all()
    return [dict(row) for row in rows]


def build_daily_digest_payload(engine, *, now: datetime | None = None) -> dict[str, Any]:
    now_msk = (now or datetime.now(MSK)).astimezone(MSK)
    window_end = now_msk
    window_start = window_end - timedelta(days=1)
    return {
        "window_start": window_start,
        "window_end": window_end,
        "new_vacancies": fetch_digest_rows(engine, "vacancies", "created_at", window_start, window_end),
        "updated_vacancies": fetch_digest_rows(engine, "vacancies", "updated_at", window_start, window_end),
        "new_specialists": fetch_digest_rows(engine, "specialists", "created_at", window_start, window_end),
        "updated_specialists": fetch_digest_rows(engine, "specialists", "updated_at", window_start, window_end),
    }
