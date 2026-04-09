from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.tl.custom import Message
from telethon.tl.types import DocumentAttributeFilename

from .db import (
    bump_channel_progress,
    ensure_schema,
    fetch_active_channels,
    get_archive_mapping,
    upsert_channels,
    upsert_archive_mapping,
    upsert_raw_message,
)
from .settings import Settings
from app.bots import views
from app.db.engine import make_engine
from app.llm.pre_classifier import normalize_short_bench_line, pre_classify_bench_line, split_line_wise_bench_items
from app.use_cases.jobs import enqueue_telegram_ingest

log = logging.getLogger("tg_collector")

_VACANCY_HINT_RE = re.compile(
    r"(?i)\b(ваканси\w*|ищем|нужен|нужна|нужно|требуется|позици\w*|обязанност\w*|требовани\w*|project|remote)\b"
)
_BENCH_HINT_RE = re.compile(
    r"(?i)\b(bench|available|свобод\w*|ищу проект|ищу работу|освобод\w*|на бенче|доступ\w*)\b"
)


@dataclass
class ChannelRow:
    telegram_id: int
    title: str
    username: Optional[str]
    last_message_id: int
    source_kind: str = "chat"


class RateLimiter:
    def __init__(self, min_interval_sec: float, jitter_sec: float = 0.0):
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self.jitter_sec = max(0.0, float(jitter_sec))
        self._lock = asyncio.Lock()
        self._next_allowed = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                await asyncio.sleep(self._next_allowed - now)
            jitter = random.uniform(0.0, self.jitter_sec) if self.jitter_sec > 0 else 0.0
            self._next_allowed = time.monotonic() + self.min_interval_sec + jitter


class BotForwarder:
    """Форвардит сообщение в бота и (опционально) ждёт первый ответ бота."""

    def __init__(self, client: TelegramClient, bot_entity: Any, reply_timeout_sec: int):
        self.client = client
        self.bot_entity = bot_entity
        self.reply_timeout_sec = reply_timeout_sec

        self._lock = asyncio.Lock()
        self._reply_future: Optional[asyncio.Future[Message]] = None
        self._expect_reply_after_ts: float = 0.0

        @self.client.on(events.NewMessage(chats=[self.bot_entity]))
        async def _on_bot_message(ev: events.NewMessage.Event) -> None:
            if self._reply_future is None or self._reply_future.done():
                return
            msg: Message = ev.message
            msg_dt = msg.date
            msg_dt = msg_dt.replace(tzinfo=timezone.utc) if msg_dt.tzinfo is None else msg_dt
            if msg_dt.timestamp() >= self._expect_reply_after_ts:
                self._reply_future.set_result(msg)

    async def forward_and_wait(self, original_msg: Message) -> Optional[Message]:
        async with self._lock:
            loop = asyncio.get_running_loop()
            self._reply_future = loop.create_future()

            await self.client.forward_messages(self.bot_entity, original_msg)
            self._expect_reply_after_ts = datetime.now(timezone.utc).timestamp()

            try:
                return await asyncio.wait_for(self._reply_future, timeout=self.reply_timeout_sec)
            except asyncio.TimeoutError:
                return None
            finally:
                self._reply_future = None


def _normalize_username(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    return u.strip().lstrip("@").lower()


def _botapi_channel_id_from_telethon_entity_id(entity_id: int) -> int:
    # telethon entity id для каналов/супергрупп положительный,
    # bot api chat id выглядит как -100<entity_id>
    return int(f"-100{int(entity_id)}")


class TelethonCollector:
    def __init__(self, settings: Settings):
        self.s = settings
        self.engine: AsyncEngine = create_async_engine(self.s.database_url, pool_pre_ping=True)
        self.sync_engine: Engine = make_engine(self.s.database_url)
        self.client = TelegramClient(self.s.tg_session_name, self.s.tg_api_id, self.s.tg_api_hash)

        self._channels: list[ChannelRow] = []

        # forward-to-bot
        self._forwarder: Optional[BotForwarder] = None
        self._bot_username: Optional[str] = _normalize_username(self.s.forward_bot_username)
        self._bot_peer_id: Optional[int] = None  # telethon entity id (positive)

        # archive relay
        self._archive_entity: Any | None = None
        self._archive_username: Optional[str] = _normalize_username(self.s.archive_chat_username)
        self._archive_entity_id: Optional[int] = None  # telethon entity id (positive)
        self._archive_chat_id_botapi: Optional[int] = None  # bot api style (-100...)

        # rate limits
        self._rl_backfill = RateLimiter(self.s.rl_backfill_min_sec, self.s.rl_jitter_sec)
        self._rl_forward = RateLimiter(self.s.rl_forward_min_sec, self.s.rl_jitter_sec)
        self._rl_download = RateLimiter(self.s.rl_download_min_sec, self.s.rl_jitter_sec)

    async def close(self) -> None:
        try:
            await self.client.disconnect()
        except Exception:
            log.exception("Error while disconnecting Telethon client")
        try:
            await self.engine.dispose()
        except Exception:
            log.exception("Error while disposing DB engine")
        try:
            self.sync_engine.dispose()
        except Exception:
            log.exception("Error while disposing sync DB engine")

    def _is_bot_source(self, telegram_id: int, username: Optional[str]) -> bool:
        # best-effort: защититься от случайного добавления бота как источника
        if self._bot_peer_id is not None:
            if telegram_id == self._bot_peer_id or telegram_id == -self._bot_peer_id:
                return True
        u = _normalize_username(username)
        return bool(u and self._bot_username and u == self._bot_username)

    def _is_archive_source(self, telegram_id: int, username: Optional[str]) -> bool:
        if self._archive_chat_id_botapi is not None and telegram_id == self._archive_chat_id_botapi:
            return True
        u = _normalize_username(username)
        return bool(u and self._archive_username and u == self._archive_username)

    async def _init_forwarder_if_needed(self) -> None:
        if not self.s.forward_to_bot:
            return

        if self.s.forward_bot_id is not None:
            bot_entity = await self.client.get_entity(self.s.forward_bot_id)
        else:
            bot_entity = await self.client.get_entity(self.s.forward_bot_username)  # type: ignore[arg-type]

        try:
            self._bot_peer_id = int(getattr(bot_entity, "id"))
        except Exception:
            self._bot_peer_id = None

        self._forwarder = BotForwarder(self.client, bot_entity, self.s.forward_reply_timeout_sec)

        log.info(
            "Forward-to-bot enabled. bot_username=%s bot_peer_id=%s timeout=%ss",
            self.s.forward_bot_username,
            self._bot_peer_id,
            self.s.forward_reply_timeout_sec,
        )

    async def _init_archive_if_needed(self) -> None:
        if not self.s.archive_enabled:
            return

        # ARCHIVE_CHAT_ID обычно у вас bot-api style (-100...), Telethon умеет резолвить.
        if self.s.archive_chat_id is not None:
            archive_entity = await self.client.get_entity(self.s.archive_chat_id)
        else:
            archive_entity = await self.client.get_entity(self.s.archive_chat_username)  # type: ignore[arg-type]

        self._archive_entity = archive_entity
        try:
            self._archive_entity_id = int(getattr(archive_entity, "id"))
            self._archive_chat_id_botapi = _botapi_channel_id_from_telethon_entity_id(self._archive_entity_id)
        except Exception:
            self._archive_entity_id = None
            self._archive_chat_id_botapi = None

        log.info(
            "Archive enabled. archive_chat=%s archive_entity_id=%s archive_chat_id_botapi=%s",
            self.s.archive_chat_id or self.s.archive_chat_username,
            self._archive_entity_id,
            self._archive_chat_id_botapi,
        )

    async def _load_all_channel_dialogs(self) -> tuple[list[ChannelRow], int]:
        existing_rows = await fetch_active_channels(self.engine)
        last_message_by_chat = {row.telegram_id: row.last_message_id or 0 for row in existing_rows}

        channels: list[ChannelRow] = []
        seen_ids: set[int] = set()
        dropped = 0

        async for dialog in self.client.iter_dialogs():
            if not getattr(dialog, "is_channel", False):
                continue

            telegram_id = int(getattr(dialog, "id", 0) or 0)
            if not telegram_id or telegram_id in seen_ids:
                continue

            entity = getattr(dialog, "entity", None)
            username = getattr(entity, "username", None)
            username = str(username) if username else None
            title = (
                getattr(dialog, "title", None)
                or getattr(entity, "title", None)
                or username
                or f"Источник {telegram_id}"
            )
            title = str(title)

            if self._is_bot_source(telegram_id, username):
                dropped += 1
                continue

            if self.s.archive_enabled and self._is_archive_source(telegram_id, username):
                dropped += 1
                continue

            seen_ids.add(telegram_id)
            channels.append(
                ChannelRow(
                    telegram_id=telegram_id,
                    title=title,
                    username=username,
                    last_message_id=last_message_by_chat.get(telegram_id, 0),
                    source_kind=self._detect_source_kind(entity),
                )
            )

        await upsert_channels(self.engine, channels)
        return channels, dropped

    async def start(self) -> None:
        await ensure_schema(self.engine)
        await self.client.start()
        await self._init_forwarder_if_needed()
        await self._init_archive_if_needed()

        self._channels, dropped = await self._load_all_channel_dialogs()

        if not self._channels and not self.s.archive_enabled:
            log.warning("Не найдено ни одного доступного Telegram-канала для мониторинга. Collector завершает работу.")
            return

        log.info("Loaded Telegram channels: %d (dropped=%d)", len(self._channels), dropped)

        # Backfill: сохраняем raw, но НЕ форвардим ни в архив ни в бота (иначе будет шквал)
        if self.s.backfill_enabled and self._channels:
            log.info("Backfill enabled. channels=%d", len(self._channels))
            for ch in self._channels:
                await self._backfill_channel(ch)

        source_chat_ids = [c.telegram_id for c in self._channels]
        if source_chat_ids:
            @self.client.on(events.NewMessage(chats=source_chat_ids, incoming=True))
            async def _on_new_message(ev: events.NewMessage.Event) -> None:
                await self._handle_message(ev.message, edited=False, source="source")

            @self.client.on(events.MessageEdited(chats=source_chat_ids, incoming=True))
            async def _on_edited(ev: events.MessageEdited.Event) -> None:
                await self._handle_message(ev.message, edited=True, source="source_edit")

        # Archive watcher: ТОЛЬКО ручные посты в архиве (incoming=True).
        # Наши форварды в архив — обычно outgoing, и их мы будем обрабатывать напрямую в source->archive блоке.
        if self.s.archive_enabled and self._archive_entity is not None:
            @self.client.on(events.NewMessage(chats=[self._archive_entity], incoming=True))
            async def _on_archive_new_manual(ev: events.NewMessage.Event) -> None:
                await self._handle_message(ev.message, edited=False, source="archive_manual")

            @self.client.on(events.MessageEdited(chats=[self._archive_entity], incoming=True))
            async def _on_archive_edit_manual(ev: events.MessageEdited.Event) -> None:
                await self._handle_message(ev.message, edited=True, source="archive_manual_edit")

        log.info(
            "Collector started. Monitoring sources=%d (archive=%s).",
            len(source_chat_ids),
            self.s.archive_enabled,
        )

        try:
            await self.client.run_until_disconnected()
        except asyncio.CancelledError:
            log.info("Collector cancelled (shutdown requested).")
        finally:
            await self.close()

    async def _backfill_channel(self, ch: ChannelRow) -> None:
        log.info(
            "Backfill channel_id=%s title=%s last_message_id=%s limit=%s days=%s",
            ch.telegram_id,
            ch.title,
            ch.last_message_id,
            self.s.backfill_limit,
            self.s.backfill_days,
        )

        min_date = None
        if self.s.backfill_days and self.s.backfill_days > 0:
            min_date = datetime.now(timezone.utc) - timedelta(days=self.s.backfill_days)

        saved = 0
        async for msg in self.client.iter_messages(
            entity=ch.telegram_id,
            limit=self.s.backfill_limit,
            min_id=ch.last_message_id or 0,
        ):
            if not isinstance(msg, Message) or msg.id is None:
                continue

            if min_date is not None:
                msg_dt = msg.date
                msg_dt = msg_dt.replace(tzinfo=timezone.utc) if msg_dt.tzinfo is None else msg_dt
                if msg_dt < min_date:
                    break

            await self._rl_backfill.wait()
            await self._handle_message(msg, edited=False, source="backfill")
            saved += 1

        log.info("Backfill done channel_id=%s saved=%s", ch.telegram_id, saved)

    async def _handle_message(self, msg: Message, edited: bool, source: str) -> None:
        """
        source:
          - source: realtime из channels table
          - source_edit: edit в источниках
          - backfill: история
          - archive_manual: ручной пост в архиве (incoming=True)
          - archive_manual_edit: edit ручного поста (incoming=True)
        """
        try:
            # 1) всегда сохраняем raw (для audit/debug)
            skip_download = source.startswith("archive_")
            raw = await self._build_raw_record(msg, edited=edited, skip_download=skip_download)
            await upsert_raw_message(self.engine, raw)

            # last_message_id обновляем только для источников (channels table)
            if source in ("source", "source_edit", "backfill"):
                await bump_channel_progress(self.engine, raw["channel_id"], raw["message_id"])

            log.info(
                "Saved RAW src=%s channel=%s msg=%s edit=%s out=%s url=%s",
                source,
                raw["channel_id"],
                raw["message_id"],
                edited,
                bool(getattr(msg, "out", False)),
                raw["message_url"],
            )

            # 2) backfill/edit: никаких форвардов
            if source in ("backfill", "source_edit", "archive_manual_edit") or edited:
                return

            if self.s.ingest_via_db_jobs and source in ("source", "archive_manual"):
                await self._enqueue_db_ingest(raw, source=source)
                return

            # 3) ARCHIVE pipeline: sources -> archive (bot reads archive directly)
            if self.s.archive_enabled and self._archive_entity is not None:
                if source == "source":
                    source_kind = self._detect_source_kind(await msg.get_chat())
                    if source_kind == "channel":
                        log.info(
                            "Source message kept original route: chat=%s msg=%s kind=%s",
                            raw["channel_id"],
                            raw["message_id"],
                            source_kind,
                        )
                        return

                    classification = self._classify_relevant_kind(raw.get("raw_text"))
                    if classification is None:
                        await upsert_archive_mapping(
                            self.engine,
                            {
                                "original_chat_id": raw["channel_id"],
                                "original_message_id": raw["message_id"],
                                "origin_type": "chat_archived",
                                "classification": "irrelevant",
                                "archive_chat_id": None,
                                "archive_message_id": None,
                                "canonical_message_url": None,
                                "archive_post_status": "skipped",
                                "archive_posted_at": None,
                                "last_error": None,
                            },
                        )
                        log.info(
                            "Skipped archive for irrelevant chat message: chat=%s msg=%s",
                            raw["channel_id"],
                            raw["message_id"],
                        )
                        return

                    existing_map = await get_archive_mapping(self.engine, raw["channel_id"], raw["message_id"])
                    if existing_map and existing_map.get("archive_post_status") == "posted":
                        log.info(
                            "Archive post already exists: src_chat=%s src_msg=%s archive_msg=%s",
                            raw["channel_id"],
                            raw["message_id"],
                            existing_map.get("archive_message_id"),
                        )
                        return

                    await upsert_archive_mapping(
                        self.engine,
                        {
                            "original_chat_id": raw["channel_id"],
                            "original_message_id": raw["message_id"],
                            "origin_type": "chat_archived",
                            "classification": classification,
                            "archive_chat_id": None,
                            "archive_message_id": None,
                            "canonical_message_url": None,
                            "archive_post_status": "pending",
                            "archive_posted_at": None,
                            "last_error": None,
                        },
                    )

                    await self._rl_forward.wait()
                    try:
                        archive_text = self._build_archive_post_text(raw, classification=classification)
                        arch_msg = await self.client.send_message(self._archive_entity, archive_text, parse_mode="html")
                        arch_raw = await self._build_raw_record(arch_msg, edited=False, skip_download=True)
                        await upsert_raw_message(self.engine, arch_raw)

                        await upsert_archive_mapping(
                            self.engine,
                            {
                                "original_chat_id": raw["channel_id"],
                                "original_message_id": raw["message_id"],
                                "origin_type": "chat_archived",
                                "classification": classification,
                                "archive_chat_id": arch_raw["channel_id"],
                                "archive_message_id": arch_raw["message_id"],
                                "canonical_message_url": arch_raw["message_url"],
                                "archive_post_status": "posted",
                                "archive_posted_at": datetime.now(timezone.utc),
                                "last_error": None,
                            },
                        )
                        log.info(
                            "Posted chat source to archive: src_chat=%s src_msg=%s archive_msg=%s type=%s",
                            raw["channel_id"],
                            raw["message_id"],
                            arch_raw["message_id"],
                            classification,
                        )
                        return
                    except Exception as e:
                        await upsert_archive_mapping(
                            self.engine,
                            {
                                "original_chat_id": raw["channel_id"],
                                "original_message_id": raw["message_id"],
                                "origin_type": "chat_archived",
                                "classification": classification,
                                "archive_chat_id": None,
                                "archive_message_id": None,
                                "canonical_message_url": None,
                                "archive_post_status": "failed",
                                "archive_posted_at": None,
                                "last_error": f"{type(e).__name__}: {e}",
                            },
                        )
                        log.exception(
                            "Failed to post chat source to archive (src_chat=%s src_msg=%s)",
                            raw["channel_id"],
                            raw["message_id"],
                        )
                        if self.s.archive_fail_open and self._forwarder is not None:
                            reply = await self._forwarder.forward_and_wait(msg)
                            if reply is not None:
                                preview = (reply.message or "").replace("\n", " ")[:160]
                                log.warning("Archive fail-open sent to bot. reply=%s", preview)
                        return

                # 3.3) ручные посты в архиве: просто сохраняем raw, бот читает архив напрямую
                if source == "archive_manual":
                    log.info("Archive manual message saved; no forwarding to bot.")
                    return

                return

            # 4) если архива нет — старый режим (source -> bot)
            if self._forwarder is not None and source == "source":
                await self._rl_forward.wait()
                reply = await self._forwarder.forward_and_wait(msg)
                if reply is not None:
                    preview = (reply.message or "").replace("\n", " ")[:160]
                    log.info("Bot replied msg_id=%s text=%s", reply.id, preview)
                else:
                    log.warning("Bot reply timeout after %ss. Continue.", self.s.forward_reply_timeout_sec)

        except FloodWaitError as e:
            log.warning("FloodWait %ss. Sleeping...", e.seconds)
            await asyncio.sleep(e.seconds + 1)
        except Exception:
            log.exception(
                "Failed to handle message channel=%s msg=%s",
                getattr(msg, "chat_id", None),
                getattr(msg, "id", None),
            )

    async def _build_raw_record(self, msg: Message, edited: bool, skip_download: bool = False) -> Dict[str, Any]:
        chat = await msg.get_chat()
        sender = await msg.get_sender()

        channel_id = int(getattr(msg, "chat_id", 0) or 0)
        message_id = int(getattr(msg, "id", 0) or 0)

        chat_title = getattr(chat, "title", None) or getattr(chat, "first_name", None) or "Unknown"
        chat_username = getattr(chat, "username", None)

        sender_id = int(getattr(sender, "id", 0) or 0) if sender else None
        sender_name = None
        sender_username = None
        if sender:
            fn = getattr(sender, "first_name", "") or ""
            ln = getattr(sender, "last_name", "") or ""
            sender_name = (fn + " " + ln).strip() or None
            sender_username = getattr(sender, "username", None)

        message_date = msg.date
        message_date = message_date.replace(tzinfo=timezone.utc) if message_date.tzinfo is None else message_date

        edited_at = datetime.now(timezone.utc) if edited else None
        raw_text = msg.message

        raw_entities = None
        try:
            if msg.entities:
                raw_entities = [e.to_dict() for e in msg.entities]
        except Exception:
            raw_entities = None

        has_attachment = bool(msg.media)
        attachment_type = None
        attachment_path = None
        if has_attachment:
            attachment_type = self._detect_attachment_type(msg)
            if attachment_type in ("pdf", "doc", "docx", "xlsx", "csv", "txt") and not skip_download:
                attachment_path = await self._download_attachment_if_needed(msg, channel_id, message_id)

        message_url = self._make_deeplink(chat_username, channel_id, message_id)

        return {
            "channel_id": channel_id,
            "message_id": message_id,
            "chat_title": chat_title,
            "chat_username": chat_username,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "sender_username": sender_username,
            "message_date": message_date,
            "edited_at": edited_at,
            "raw_text": raw_text,
            "raw_entities": raw_entities,
            "message_url": message_url,
            "has_attachment": has_attachment,
            "attachment_type": attachment_type,
            "attachment_path": attachment_path,
        }

    def _make_deeplink(self, username: Optional[str], channel_id: int, message_id: int) -> Optional[str]:
        """Ссылка на сообщение.
        - public: https://t.me/<username>/<mid>
        - private supergroup/channel: https://t.me/c/<id_without_-100>/<mid>
        - DM / basic group: нет стабильной ссылки -> None
        """
        if username:
            return f"https://t.me/{username}/{message_id}"
        s = str(channel_id)
        if s.startswith("-100") and len(s) > 4:
            return f"https://t.me/c/{s[4:]}/{message_id}"
        return None

    def _detect_attachment_type(self, msg: Message) -> Optional[str]:
        doc = getattr(msg, "document", None)
        if not doc:
            return None

        mime = getattr(doc, "mime_type", None) or ""
        if mime == "application/pdf":
            return "pdf"
        if mime == "application/msword":
            return "doc"
        if mime == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            return "docx"
        if mime == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            return "xlsx"
        if mime in {"text/csv", "application/csv"}:
            return "csv"
        if mime.startswith("text/plain"):
            return "txt"

        try:
            for a in doc.attributes:
                if isinstance(a, DocumentAttributeFilename):
                    name = (a.file_name or "").lower()
                    for ext in ("pdf", "docx", "doc", "xlsx", "csv", "txt"):
                        if name.endswith("." + ext):
                            return ext
        except Exception:
            pass

        return None

    async def _download_attachment_if_needed(self, msg: Message, channel_id: int, message_id: int) -> Optional[str]:
        base = Path(self.s.download_dir) / "telegram" / str(channel_id) / str(message_id)
        base.mkdir(parents=True, exist_ok=True)

        existing = list(base.glob("*"))
        if existing:
            return str(existing[0])

        doc = getattr(msg, "document", None)
        if doc and getattr(doc, "size", None):
            size_mb = doc.size / (1024 * 1024)
            if size_mb > self.s.max_file_mb:
                log.warning("Skip download: file too large %.1fMB > %dMB", size_mb, self.s.max_file_mb)
                return None

        try:
            await self._rl_download.wait()
            path = await self.client.download_media(msg, file=str(base))
            return str(path) if path else None
        except FloodWaitError as e:
            log.warning("FloodWait on download %ss", e.seconds)
            await asyncio.sleep(e.seconds + 1)
            return None
        except Exception:
            log.exception("Failed to download attachment channel=%s msg=%s", channel_id, message_id)
            return None

    def _build_archive_post_text(self, raw: Dict[str, Any], *, classification: str) -> str:
        """
        Формирует архивный пост для chat-sources.
        """
        chat_title = str(raw.get("chat_title") or "Unknown")
        chat_id = raw.get("channel_id")
        chat_username = raw.get("chat_username")
        sender_name = str(raw.get("sender_name") or "").strip()
        sender_username = str(raw.get("sender_username") or "").strip()
        sender_id = raw.get("sender_id")
        message_url = raw.get("message_url")
        message_date = raw.get("message_date")
        raw_text = (raw.get("raw_text") or "").strip()

        source_parts = [chat_title]
        if chat_username:
            source_parts.append(f"@{chat_username}")
        if chat_id:
            source_parts.append(str(chat_id))
        source_line = " / ".join(source_parts)

        sender_line = "unknown"
        if sender_username:
            sender_line = f"@{sender_username}"
            if sender_name:
                sender_line += f" ({sender_name})"
        elif sender_name:
            sender_line = sender_name
        if sender_id:
            sender_line += f" / {sender_id}"

        return views.render_archive_post(
            source_name=source_line,
            classification=classification,
            original_date=message_date if isinstance(message_date, datetime) else None,
            sender_display=sender_line,
            original_url=message_url,
            raw_text=raw_text or "[без текста]",
            max_total=3900,
        )

    def _detect_source_kind(self, entity: Any) -> str:
        if bool(getattr(entity, "broadcast", False)):
            return "channel"
        return "chat"

    def _classify_relevant_kind(self, text_in: Optional[str]) -> Optional[str]:
        text = (text_in or "").strip()
        if not text:
            return None

        normalized = normalize_short_bench_line(text)
        if split_line_wise_bench_items(normalized):
            return "bench"

        pre = pre_classify_bench_line(normalized)
        if pre.is_confident or (pre.confidence >= 0.55 and not pre.signals.get("vacancy_hint", False)):
            return "bench"

        low = normalized.lower()
        if _BENCH_HINT_RE.search(low):
            return "bench"
        if _VACANCY_HINT_RE.search(low):
            return "vacancy"
        return None

    async def _enqueue_db_ingest(self, raw: Dict[str, Any], *, source: str) -> None:
        attachment_path = str(raw.get("attachment_path") or "").strip() or None
        attachment_name = Path(attachment_path).name if attachment_path else None
        attachment_type = str(raw.get("attachment_type") or "").strip() or None
        payload = {
            "channel_id": int(raw.get("channel_id") or 0),
            "message_id": int(raw.get("message_id") or 0),
            "chat_title": raw.get("chat_title"),
            "sender_id": raw.get("sender_id"),
            "sender_name": raw.get("sender_name"),
            "message_url": raw.get("message_url"),
            "raw_text": raw.get("raw_text") or "",
            "attachment_path": attachment_path,
            "attachment_name": attachment_name,
            "attachment_mime_type": _attachment_mime_type(attachment_type, attachment_name),
            "source_kind": ("archive_post" if source.startswith("archive_") else "telegram_message"),
            "source_sender_name": raw.get("sender_name"),
        }
        dedupe_key = f"telegram_ingest:{payload['channel_id']}:{payload['message_id']}:{source}"
        await asyncio.to_thread(
            enqueue_telegram_ingest,
            self.sync_engine,
            payload=payload,
            dedupe_key=dedupe_key,
        )
        log.info(
            "Queued telegram ingest job channel=%s msg=%s source=%s attachment=%s",
            payload["channel_id"],
            payload["message_id"],
            source,
            bool(attachment_path),
        )


def _attachment_mime_type(attachment_type: str | None, attachment_name: str | None) -> str | None:
    ext = (attachment_type or "").strip().lower()
    name = (attachment_name or "").strip().lower()
    if ext == "pdf" or name.endswith(".pdf"):
        return "application/pdf"
    if ext == "docx" or name.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if ext == "xlsx" or name.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext == "csv" or name.endswith(".csv"):
        return "text/csv"
    if ext == "txt" or name.endswith(".txt"):
        return "text/plain"
    if ext == "doc" or name.endswith(".doc"):
        return "application/msword"
    return None
