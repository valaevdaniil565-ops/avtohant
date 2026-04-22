from __future__ import annotations

import re

from fastapi import APIRouter
from sqlalchemy import text

from backend.app.db.session import get_engine

router = APIRouter()

_OCTAL_ESCAPE_RE = re.compile(r"\\([0-7]{3})")


def _decode_escaped_preview(value: object) -> str:
    raw = str(value or "")
    if not raw:
        return ""

    parts: list[int] = []
    index = 0
    while index < len(raw):
        match = _OCTAL_ESCAPE_RE.match(raw, index)
        if match:
            parts.append(int(match.group(1), 8))
            index = match.end()
            continue
        parts.append(ord(raw[index]))
        index += 1

    return bytes(parts).decode("utf-8", "replace")


@router.get("")
def list_inbox(limit: int = 100) -> dict[str, object]:
    engine = get_engine()
    query = text(
        """
        SELECT
          'incoming' AS type,
          COALESCE(raw.chat_title, raw.chat_username, CAST(raw.channel_id AS varchar), '-') AS source,
          COALESCE(raw.sender_name, raw.sender_username, CAST(raw.sender_id AS varchar), '-') AS author,
          LEFT(COALESCE(encode(convert_to(raw.raw_text, 'UTF8'), 'escape'), ''), 500) AS preview,
          CASE
            WHEN raw.raw_text IS NULL OR raw.raw_text = '' THEN 'empty'
            ELSE 'received'
          END AS status,
          COALESCE(raw.message_date, raw.created_at) AS date,
          raw.channel_id,
          raw.message_id,
          raw.message_url
        FROM tg_messages_raw raw
        ORDER BY COALESCE(raw.message_date, raw.created_at) DESC, raw.created_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"limit": max(1, min(limit, 500))}).mappings().all()
    items = []
    for row in rows:
        item = dict(row)
        item["type"] = "Входящее"
        item["preview"] = _decode_escaped_preview(item.get("preview"))
        items.append(item)
    return {"items": items}
