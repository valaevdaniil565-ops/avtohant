from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text


def ensure_audit_log_table(engine) -> None:
    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          event_type VARCHAR(128) NOT NULL,
          actor VARCHAR(128) NOT NULL DEFAULT 'system',
          entity_type VARCHAR(64),
          entity_id VARCHAR(128),
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    with engine.begin() as connection:
        connection.execute(ddl)


def write_audit_event(
    engine,
    *,
    event_type: str,
    actor: str = "system",
    entity_type: str | None = None,
    entity_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    ensure_audit_log_table(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit_log(event_type, actor, entity_type, entity_id, payload, created_at)
                VALUES (:event_type, :actor, :entity_type, :entity_id, CAST(:payload AS jsonb), NOW())
                """
            ),
            {
                "event_type": event_type,
                "actor": actor,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "payload": json.dumps(payload or {}, ensure_ascii=False),
            },
        )


def list_audit_events(
    engine,
    *,
    event_type: str | None = None,
    entity_type: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_audit_log_table(engine)
    query = text(
        """
        SELECT id, event_type, actor, entity_type, entity_id, payload, created_at
        FROM audit_log
        WHERE (:event_type IS NULL OR event_type = :event_type)
          AND (:entity_type IS NULL OR entity_type = :entity_type)
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(
            query,
            {
                "event_type": event_type,
                "entity_type": entity_type,
                "limit": limit,
            },
        ).mappings().all()
    return [dict(row) for row in rows]
