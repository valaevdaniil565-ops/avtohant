from __future__ import annotations

import json
import re
from typing import Any, Optional

from sqlalchemy import text


def normalize_source_index(locator: Optional[str], fallback: Optional[int] = None) -> str:
    text_value = (locator or "").strip()
    match = re.search(r"(\d+)$", text_value)
    if match:
        return match.group(1)
    if fallback is not None:
        return str(int(fallback))
    return "-"


def compose_source_display(
    *,
    manager_name: str,
    canonical_url: Optional[str],
    external_url: Optional[str],
    external_locator: Optional[str],
    source_kind: str,
    entity_index: Optional[int] = None,
    sheet_name: Optional[str] = None,
    table_index: Optional[int] = None,
) -> str:
    if external_url:
        parts = [
            f"Менеджер: {manager_name}",
            f"Ссылка на файл: {external_url}",
        ]
        if sheet_name:
            parts.append(f"Лист: {sheet_name}")
        if table_index:
            parts.append(f"Таблица: {int(table_index)}")
        parts.append(f"Индекс: {normalize_source_index(external_locator, entity_index)}")
        return "; ".join(parts)
    if source_kind == "archive_post" and canonical_url:
        return (
            f"Менеджер: {manager_name}; "
            f"Ссылка на архив-пост: {canonical_url}; "
            f"Индекс: {normalize_source_index(None, entity_index)}"
        )
    if canonical_url:
        return f"Менеджер: {manager_name}; Ссылка на сообщение: {canonical_url}"
    return f"Менеджер: {manager_name}"


def build_source_meta(
    *,
    base_meta: Optional[dict[str, Any]],
    manager_name: str,
    canonical_url: Optional[str],
    external_url: Optional[str],
    external_locator: Optional[str],
    source_kind: str,
    entity_index: Optional[int] = None,
    source_sender_name: Optional[str] = None,
) -> dict[str, Any]:
    trace_meta = dict(base_meta or {})
    sheet_name = str(trace_meta.get("sheet_name") or trace_meta.get("table_name") or "").strip() or None
    table_index = trace_meta.get("table_index")
    source_display = compose_source_display(
        manager_name=manager_name,
        canonical_url=canonical_url,
        external_url=external_url,
        external_locator=external_locator,
        source_kind=source_kind,
        entity_index=entity_index,
        sheet_name=sheet_name,
        table_index=int(table_index) if table_index not in (None, "") else None,
    )
    out = dict(trace_meta)
    out["manager_name"] = manager_name
    out["source_kind"] = source_kind
    out["source_display"] = source_display
    if external_locator:
        out["source_index"] = normalize_source_index(external_locator, entity_index)
    elif entity_index is not None and source_kind == "archive_post":
        out["source_index"] = str(int(entity_index))
    if source_sender_name:
        out["source_sender_name"] = source_sender_name
    return out


def ensure_sources_extra_columns(engine) -> None:
    ddl = [
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS source_meta JSONB",
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS external_url VARCHAR(1024)",
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS external_kind VARCHAR(64)",
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS external_locator VARCHAR(128)",
        "ALTER TABLE specialists ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT FALSE",
        "ALTER TABLE channels ADD COLUMN IF NOT EXISTS source_kind VARCHAR(20) DEFAULT 'chat'",
    ]
    with engine.begin() as connection:
        for query in ddl:
            connection.execute(text(query))


def insert_source(
    engine,
    *,
    entity_type: str,
    entity_id: str,
    channel_id: int,
    message_id: int,
    chat_title: Optional[str],
    sender_id: Optional[int],
    sender_name: Optional[str],
    message_url: Optional[str],
    raw_text: str,
    idx: int,
    source_type: Optional[str] = None,
    external_url: Optional[str] = None,
    external_kind: Optional[str] = None,
    external_locator: Optional[str] = None,
    source_meta: Optional[dict[str, Any]] = None,
) -> None:
    source_mid = int(message_id) * 1000 + int(idx)
    query = text(
        """
        INSERT INTO sources(
          entity_type, entity_id,
          channel_id, message_id,
          chat_title, sender_id, sender_name,
          message_url, source_type, raw_text,
          external_url, external_kind, external_locator, source_meta
        )
        VALUES (
          :etype, :eid,
          :cid, :mid,
          :ctitle, :sid, :sname,
          :url, :stype, :raw,
          :external_url, :external_kind, :external_locator, CAST(:source_meta AS jsonb)
        )
        ON CONFLICT(channel_id, message_id) DO NOTHING
        """
    )
    with engine.begin() as connection:
        connection.execute(
            query,
            {
                "etype": entity_type,
                "eid": entity_id,
                "cid": int(channel_id),
                "mid": source_mid,
                "ctitle": chat_title,
                "sid": sender_id,
                "sname": sender_name,
                "url": message_url,
                "stype": source_type or "manual",
                "raw": raw_text,
                "external_url": external_url,
                "external_kind": external_kind,
                "external_locator": external_locator,
                "source_meta": json.dumps(source_meta or {}, ensure_ascii=False),
            },
        )


def find_source_entities_by_message_url(engine, url: str) -> list[dict[str, Any]]:
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT id, entity_type, entity_id, message_url, source_type, external_url, external_kind, created_at
                FROM sources
                WHERE message_url = :url
                ORDER BY created_at DESC
                LIMIT 50
                """
            ),
            {"url": url},
        ).mappings().all()
    return [dict(row) for row in rows]


def soft_hide_entities(engine, items: list[dict[str, Any]]) -> int:
    updated = 0
    with engine.begin() as connection:
        for item in items:
            entity_type = item.get("entity_type")
            entity_id = item.get("entity_id")
            if not entity_type or not entity_id:
                continue
            if entity_type == "vacancy":
                connection.execute(text("UPDATE vacancies SET status='hidden', updated_at=NOW() WHERE id=:id"), {"id": entity_id})
                updated += 1
            elif entity_type == "specialist":
                connection.execute(text("UPDATE specialists SET status='hidden', updated_at=NOW() WHERE id=:id"), {"id": entity_id})
                updated += 1
    return updated


def list_recent_sources(engine, limit: int = 20) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
          id,
          entity_type,
          entity_id,
          message_url,
          source_type,
          external_url,
          external_kind,
          external_locator,
          source_meta,
          created_at
        FROM sources
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"limit": limit}).mappings().all()
    return [dict(row) for row in rows]


def list_recent_imports(engine, limit: int = 20) -> list[dict[str, Any]]:
    query = text(
        """
        SELECT
          source_type,
          external_kind,
          external_url,
          COUNT(*) AS items_count,
          MAX(created_at) AS last_seen_at
        FROM sources
        WHERE external_url IS NOT NULL OR source_type IN ('telegram_attachment', 'external_link', 'manual_line_split', 'forward_chat_archive')
        GROUP BY source_type, external_kind, external_url
        ORDER BY MAX(created_at) DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"limit": limit}).mappings().all()
    return [dict(row) for row in rows]
