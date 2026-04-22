from __future__ import annotations

from fastapi import APIRouter
from sqlalchemy import text

from backend.app.db.session import get_engine

router = APIRouter()


@router.get("")
def list_logs(limit: int = 100) -> dict[str, object]:
    engine = get_engine()
    query = text(
        """
        SELECT
          COALESCE(a.event_type, 'event') AS status,
          COALESCE(a.actor, 'system') AS model,
          COALESCE(a.entity_id, CAST(a.id AS varchar)) AS message_id,
          COALESCE(
            a.payload ->> 'duration_ms',
            a.payload ->> 'duration',
            a.payload ->> 'latency_ms',
            '—'
          ) AS duration_ms,
          a.created_at AS date
        FROM audit_log a
        ORDER BY a.created_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"limit": max(1, min(limit, 500))}).mappings().all()
    return {"items": [dict(row) for row in rows]}
