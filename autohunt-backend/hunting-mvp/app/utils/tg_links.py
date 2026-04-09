# app/utils/tg_links.py
from __future__ import annotations

import re
from typing import Optional, Tuple

from telegram import Message

# PTB v21+: forward_origin types
try:
    from telegram import MessageOriginChannel
except Exception:  # pragma: no cover
    MessageOriginChannel = None  # type: ignore


_TME_RE = re.compile(
    r"(?:https?://)?t\.me/"
    r"(?:(?P<uname>[A-Za-z0-9_]{5,})|c/(?P<cid>\d+))"
    r"(?:/(?P<th>\d+))?"
    r"/(?P<mid>\d+)"
)


def _clean_channel_id_for_link(chat_id: int) -> str:
    """
    Для приватных супергрупп/каналов Telegram использует /c/<id_without_-100>/...
    """
    s = str(chat_id)
    if s.startswith("-100"):
        return s[4:]
    if s.startswith("-"):
        return s[1:]
    return s


def build_message_url(
    *,
    chat_id: int,
    message_id: int,
    username: Optional[str] = None,
    thread_id: Optional[int] = None,
) -> Optional[str]:
    """
    public:  https://t.me/<username>/<message_id>
    private: https://t.me/c/<id_without_-100>/<message_id>
    topics:  https://t.me/<...>/<thread_id>/<message_id>
    """
    if username:
        u = username.lstrip("@")
        if thread_id:
            return f"https://t.me/{u}/{thread_id}/{message_id}"
        return f"https://t.me/{u}/{message_id}"

    # Только супер-группы/каналы имеют рабочий /c/... формат
    s = str(chat_id)
    if s.startswith("-100") or s.startswith("-"):
        clean = _clean_channel_id_for_link(chat_id)
        if thread_id:
            return f"https://t.me/c/{clean}/{thread_id}/{message_id}"
        return f"https://t.me/c/{clean}/{message_id}"

    return None


def extract_forwarded_message_url(msg: Message) -> Optional[str]:
    """
    Достаём ссылку на ОРИГИНАЛ из пересланного сообщения.
    Надёжно работает для forwarded-постов из каналов.
    """
    if not msg:
        return None

    # 1) Новый путь (PTB v21+)
    fo = getattr(msg, "forward_origin", None)
    if fo and MessageOriginChannel and isinstance(fo, MessageOriginChannel):
        ch = fo.chat
        return build_message_url(
            chat_id=ch.id,
            message_id=fo.message_id,
            username=getattr(ch, "username", None),
            thread_id=getattr(msg, "message_thread_id", None),
        )

    # 2) Legacy поля (на случай старых апдейтов)
    fchat = getattr(msg, "forward_from_chat", None)
    fmid = getattr(msg, "forward_from_message_id", None)
    if fchat and fmid:
        return build_message_url(
            chat_id=fchat.id,
            message_id=int(fmid),
            username=getattr(fchat, "username", None),
            thread_id=getattr(msg, "message_thread_id", None),
        )

    return None


def parse_message_url(text: str) -> Optional[Tuple[str, int]]:
    """
    Если менеджер прислал "Copy link" (t.me/...), вытаскиваем нормализованную ссылку и msg_id.
    """
    m = _TME_RE.search(text or "")
    if not m:
        return None

    uname = m.group("uname")
    cid = m.group("cid")
    th = m.group("th")
    mid = int(m.group("mid"))

    if uname:
        return (f"https://t.me/{uname}/" + (f"{th}/" if th else "") + f"{mid}", mid)
    if cid:
        return (f"https://t.me/c/{cid}/" + (f"{th}/" if th else "") + f"{mid}", mid)
    return None
