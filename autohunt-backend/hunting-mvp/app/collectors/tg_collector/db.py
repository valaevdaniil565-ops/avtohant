from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

log = logging.getLogger("tg_collector.db")


@dataclass
class ChannelRow:
    telegram_id: int
    title: str
    username: Optional[str]
    last_message_id: Optional[int]
    source_kind: str = "chat"


RAW_TABLE_DDL = """
-- требуется pgcrypto для gen_random_uuid(); если уже есть — ок
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS tg_messages_raw (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    chat_title TEXT,
    chat_username TEXT,
    sender_id BIGINT,
    sender_name TEXT,
    sender_username TEXT,
    message_date TIMESTAMPTZ,
    edited_at TIMESTAMPTZ,
    raw_text TEXT,
    raw_entities JSONB,
    message_url VARCHAR(512),
    has_attachment BOOLEAN NOT NULL DEFAULT FALSE,
    attachment_type VARCHAR(50),
    attachment_path VARCHAR(512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(channel_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_tg_messages_raw_channel_id ON tg_messages_raw(channel_id);
CREATE INDEX IF NOT EXISTS idx_tg_messages_raw_created_at ON tg_messages_raw(created_at DESC);

DO $$
BEGIN
    IF to_regclass('public.channels') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE channels ADD COLUMN IF NOT EXISTS source_kind VARCHAR(20) DEFAULT ''chat''';
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS tg_archive_map (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_chat_id BIGINT NOT NULL,
    original_message_id BIGINT NOT NULL,
    origin_type VARCHAR(32) NOT NULL,
    classification VARCHAR(20),
    archive_chat_id BIGINT,
    archive_message_id BIGINT,
    canonical_message_url VARCHAR(512),
    archive_post_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    archive_posted_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(original_chat_id, original_message_id),
    UNIQUE(archive_chat_id, archive_message_id)
);

CREATE INDEX IF NOT EXISTS idx_tg_archive_map_status ON tg_archive_map(archive_post_status);
"""


def make_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


async def ensure_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        try:
            await conn.execute(text(RAW_TABLE_DDL))
            log.info("Schema ensured: tg_messages_raw")
        except Exception:
            # если нет прав на CREATE EXTENSION — всё равно попробуем создать таблицу без него
            log.exception("ensure_schema failed (extensions/ddl). Проверь права пользователя БД.")
            raise


async def fetch_active_channels(engine: AsyncEngine) -> List[ChannelRow]:
    # channels: telegram_id, title, username, is_active, last_message_id :contentReference[oaicite:2]{index=2}
    q = text(
        """
        SELECT telegram_id, title, username, last_message_id, COALESCE(source_kind, 'chat') AS source_kind
        FROM channels
        WHERE is_active = TRUE
        ORDER BY added_at ASC
        """
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(q)).mappings().all()

    out: List[ChannelRow] = []
    for r in rows:
        out.append(
            ChannelRow(
                telegram_id=int(r["telegram_id"]),
                title=str(r["title"]),
                username=(str(r["username"]).lstrip("@") if r["username"] else None),
                last_message_id=(int(r["last_message_id"]) if r["last_message_id"] is not None else None),
                source_kind=str(r["source_kind"] or "chat"),
            )
        )
    return out


async def upsert_channels(engine: AsyncEngine, channels: List[ChannelRow]) -> None:
    if not channels:
        return

    q = text(
        """
        INSERT INTO channels (
            telegram_id,
            title,
            username,
            source_kind,
            is_active,
            updated_at
        )
        VALUES (
            :telegram_id,
            :title,
            :username,
            :source_kind,
            TRUE,
            NOW()
        )
        ON CONFLICT (telegram_id) DO UPDATE SET
            title = EXCLUDED.title,
            username = EXCLUDED.username,
            source_kind = EXCLUDED.source_kind,
            is_active = TRUE,
            updated_at = NOW()
        """
    )

    payload = [
        {
            "telegram_id": ch.telegram_id,
            "title": ch.title,
            "username": ch.username,
            "source_kind": ch.source_kind,
        }
        for ch in channels
    ]

    async with engine.begin() as conn:
        await conn.execute(q, payload)


async def get_existing_attachment_path(engine: AsyncEngine, channel_id: int, message_id: int) -> Optional[str]:
    q = text(
        """
        SELECT attachment_path
        FROM tg_messages_raw
        WHERE channel_id = :cid AND message_id = :mid
        """
    )
    async with engine.begin() as conn:
        row = (await conn.execute(q, {"cid": channel_id, "mid": message_id})).mappings().first()
        if not row:
            return None
        return row["attachment_path"]


async def upsert_raw_message(engine: AsyncEngine, payload: Dict[str, Any]) -> None:
    """
    payload keys:
      channel_id, message_id, chat_title, chat_username,
      sender_id, sender_name, sender_username,
      message_date, edited_at, raw_text, raw_entities (python obj),
      message_url, has_attachment, attachment_type, attachment_path

    NOTE: channel progress is updated by caller to avoid side effects for
    archive/manual rows that should not touch channels.last_message_id.
    """
    q = text(
        """
        INSERT INTO tg_messages_raw(
            channel_id, message_id, chat_title, chat_username,
            sender_id, sender_name, sender_username,
            message_date, edited_at, raw_text, raw_entities,
            message_url,
            has_attachment, attachment_type, attachment_path
        )
        VALUES (
            :channel_id, :message_id, :chat_title, :chat_username,
            :sender_id, :sender_name, :sender_username,
            :message_date, :edited_at, :raw_text, CAST(:raw_entities AS jsonb),
            :message_url,
            :has_attachment, :attachment_type, :attachment_path
        )
        ON CONFLICT (channel_id, message_id) DO UPDATE SET
            chat_title = EXCLUDED.chat_title,
            chat_username = EXCLUDED.chat_username,
            sender_id = COALESCE(EXCLUDED.sender_id, tg_messages_raw.sender_id),
            sender_name = COALESCE(EXCLUDED.sender_name, tg_messages_raw.sender_name),
            sender_username = COALESCE(EXCLUDED.sender_username, tg_messages_raw.sender_username),
            edited_at = COALESCE(EXCLUDED.edited_at, tg_messages_raw.edited_at),
            raw_text = COALESCE(EXCLUDED.raw_text, tg_messages_raw.raw_text),
            raw_entities = COALESCE(EXCLUDED.raw_entities, tg_messages_raw.raw_entities),
            message_url = COALESCE(EXCLUDED.message_url, tg_messages_raw.message_url),
            has_attachment = (tg_messages_raw.has_attachment OR EXCLUDED.has_attachment),
            attachment_type = COALESCE(tg_messages_raw.attachment_type, EXCLUDED.attachment_type),
            attachment_path = COALESCE(tg_messages_raw.attachment_path, EXCLUDED.attachment_path)
        """
    )

    payload2 = dict(payload)
    payload2["raw_entities"] = json.dumps(payload.get("raw_entities")) if payload.get("raw_entities") is not None else None

    async with engine.begin() as conn:
        await conn.execute(q, payload2)


async def bump_channel_progress(engine: AsyncEngine, channel_id: int, message_id: int) -> None:
    q = text(
        """
        UPDATE channels
        SET last_message_id = GREATEST(COALESCE(last_message_id, 0), :mid),
            updated_at = NOW()
        WHERE telegram_id = :cid
        """
    )
    async with engine.begin() as conn:
        await conn.execute(q, {"cid": channel_id, "mid": message_id})


async def get_archive_mapping(engine: AsyncEngine, original_chat_id: int, original_message_id: int) -> Optional[Dict[str, Any]]:
    q = text(
        """
        SELECT *
        FROM tg_archive_map
        WHERE original_chat_id = :cid AND original_message_id = :mid
        LIMIT 1
        """
    )
    async with engine.begin() as conn:
        row = (await conn.execute(q, {"cid": original_chat_id, "mid": original_message_id})).mappings().first()
    return dict(row) if row else None


async def upsert_archive_mapping(engine: AsyncEngine, payload: Dict[str, Any]) -> None:
    q = text(
        """
        INSERT INTO tg_archive_map(
            original_chat_id,
            original_message_id,
            origin_type,
            classification,
            archive_chat_id,
            archive_message_id,
            canonical_message_url,
            archive_post_status,
            archive_posted_at,
            last_error,
            updated_at
        )
        VALUES (
            :original_chat_id,
            :original_message_id,
            :origin_type,
            :classification,
            :archive_chat_id,
            :archive_message_id,
            :canonical_message_url,
            :archive_post_status,
            :archive_posted_at,
            :last_error,
            NOW()
        )
        ON CONFLICT (original_chat_id, original_message_id) DO UPDATE SET
            origin_type = EXCLUDED.origin_type,
            classification = COALESCE(EXCLUDED.classification, tg_archive_map.classification),
            archive_chat_id = COALESCE(EXCLUDED.archive_chat_id, tg_archive_map.archive_chat_id),
            archive_message_id = COALESCE(EXCLUDED.archive_message_id, tg_archive_map.archive_message_id),
            canonical_message_url = COALESCE(EXCLUDED.canonical_message_url, tg_archive_map.canonical_message_url),
            archive_post_status = EXCLUDED.archive_post_status,
            archive_posted_at = COALESCE(EXCLUDED.archive_posted_at, tg_archive_map.archive_posted_at),
            last_error = EXCLUDED.last_error,
            updated_at = NOW()
        """
    )
    async with engine.begin() as conn:
        await conn.execute(q, payload)
