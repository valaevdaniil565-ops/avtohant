from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient, events
from telethon.tl.custom import Message

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.collectors.tg_collector.settings import Settings
from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.services.registry_source_import import (
    RegistrySourceCandidate,
    extract_registry_source_candidates,
)

DEFAULT_REGISTRY_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1YUh5ZImOB8pW95WsK64wkZVqpecouvx8SnjnQwDtmhk/"
    "edit?gid=1517966699#gid=1517966699"
)

SUCCESS_MARKER = "Обработка завершена"
FATAL_MARKERS = (
    "Нет доступа",
    "Ссылки распознаны, но источник не удалось прочитать",
    "Тип сообщения не распознан",
    "Не удалось извлечь вакансии",
    "Не удалось извлечь специалистов",
)

log = logging.getLogger("import_registry_sources_via_bot")


@dataclass(frozen=True)
class ValidatedCandidate:
    candidate: RegistrySourceCandidate
    source_type: str
    items_count: int


class BotReplyAwaiter:
    def __init__(self, client: TelegramClient, bot_entity: Any):
        self.client = client
        self.bot_entity = bot_entity
        self._queue: asyncio.Queue[Message] = asyncio.Queue()

        @self.client.on(events.NewMessage(chats=[self.bot_entity]))
        async def _on_bot_message(ev: events.NewMessage.Event) -> None:
            await self._queue.put(ev.message)

    async def drain(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    async def wait_for_terminal(self, *, min_ts: float, timeout_sec: int) -> tuple[bool, str, list[str]]:
        messages: list[str] = []
        deadline = asyncio.get_running_loop().time() + timeout_sec

        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("bot did not send terminal reply in time")

            msg = await asyncio.wait_for(self._queue.get(), timeout=remaining)
            if _message_ts(msg) < min_ts:
                continue

            text = (msg.message or "").strip()
            if not text:
                continue
            messages.append(text)

            if SUCCESS_MARKER in text:
                return True, text, messages
            if any(marker in text for marker in FATAL_MARKERS):
                return False, text, messages


def _message_ts(msg: Message) -> float:
    dt = msg.date
    dt = dt.replace(tzinfo=timezone.utc) if dt and dt.tzinfo is None else dt
    return dt.timestamp() if dt else 0.0


def _resolve_bot_target(args: argparse.Namespace, settings: Settings) -> int | str:
    if args.bot_id is not None:
        return int(args.bot_id)
    if args.bot_username:
        return str(args.bot_username).strip().lstrip("@")
    if settings.forward_bot_id is not None:
        return int(settings.forward_bot_id)
    if settings.forward_bot_username:
        return str(settings.forward_bot_username).strip().lstrip("@")
    raise RuntimeError("Bot target is not configured. Pass --bot-id or --bot-username.")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Reads the partner registry Google Sheet, extracts file-source links from "
            "the vacancies/bench columns, validates them, and sends them to the bot "
            "one-by-one from the collector Telegram session."
        )
    )
    p.add_argument("--registry-url", default=DEFAULT_REGISTRY_URL)
    p.add_argument("--bot-id", type=int, default=None)
    p.add_argument("--bot-username", default=None)
    p.add_argument("--mcp-command", default=None)
    p.add_argument("--timeout-sec", type=int, default=1200)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    return p


async def _load_registry_candidates(
    registry_url: str,
    source_fetcher: MCPSourceFetcherClient,
) -> list[RegistrySourceCandidate]:
    result = await asyncio.to_thread(source_fetcher.fetch_url, registry_url)
    if not result.ok:
        raise RuntimeError(f"Registry sheet fetch failed: {result.error or result.source_type}")
    candidates = extract_registry_source_candidates(result.items)
    log.info("Registry rows parsed: %s, extracted links: %s", len(result.items), len(candidates))
    return candidates


async def _validate_candidates(
    candidates: list[RegistrySourceCandidate],
    source_fetcher: MCPSourceFetcherClient,
    *,
    limit: int = 0,
) -> list[ValidatedCandidate]:
    out: list[ValidatedCandidate] = []
    for candidate in candidates:
        result = await asyncio.to_thread(source_fetcher.fetch_url, candidate.source_url)
        if not result.ok or not result.items:
            log.warning(
                "Skip non-relevant source row=%s company=%s column=%s url=%s reason=%s",
                candidate.registry_row_index,
                candidate.company_name,
                candidate.column_name,
                candidate.source_url,
                result.error or "empty items",
            )
            continue
        out.append(
            ValidatedCandidate(
                candidate=candidate,
                source_type=result.source_type,
                items_count=len(result.items),
            )
        )
        log.info(
            "Accepted source row=%s company=%s column=%s type=%s items=%s url=%s",
            candidate.registry_row_index,
            candidate.company_name,
            candidate.column_name,
            result.source_type,
            len(result.items),
            candidate.source_url,
        )
        if limit > 0 and len(out) >= limit:
            break
    return out


async def _send_candidates_sequentially(
    client: TelegramClient,
    bot_target: int | str,
    candidates: list[ValidatedCandidate],
    *,
    timeout_sec: int,
) -> None:
    bot_entity = await client.get_entity(bot_target)
    waiter = BotReplyAwaiter(client, bot_entity)

    success = 0
    failed = 0

    for idx, item in enumerate(candidates, start=1):
        candidate = item.candidate
        await waiter.drain()
        send_ts = datetime.now(timezone.utc).timestamp()

        log.info(
            "[%s/%s] Sending row=%s company=%s column=%s type=%s items=%s url=%s",
            idx,
            len(candidates),
            candidate.registry_row_index,
            candidate.company_name,
            candidate.column_name,
            item.source_type,
            item.items_count,
            candidate.source_url,
        )
        await client.send_message(bot_entity, candidate.source_url)

        ok, terminal_text, messages = await waiter.wait_for_terminal(min_ts=send_ts, timeout_sec=timeout_sec)
        if ok:
            success += 1
            log.info("[%s/%s] Completed: %s", idx, len(candidates), terminal_text.replace("\n", " | "))
        else:
            failed += 1
            log.warning("[%s/%s] Failed: %s", idx, len(candidates), terminal_text.replace("\n", " | "))
            if "Нет доступа" in terminal_text:
                raise RuntimeError("Bot denied access for the collector account.")
        for bot_msg in messages:
            log.info("  bot> %s", bot_msg.replace("\n", " | "))

    log.info("Import finished. sent=%s success=%s failed=%s", len(candidates), success, failed)


async def _amain(args: argparse.Namespace) -> None:
    settings = Settings.load()
    source_fetcher = MCPSourceFetcherClient(command=args.mcp_command)

    candidates = await _load_registry_candidates(args.registry_url, source_fetcher)
    validated = await _validate_candidates(candidates, source_fetcher, limit=int(args.limit or 0))
    if not validated:
        log.warning("No relevant source links found.")
        return

    if args.dry_run:
        for item in validated:
            candidate = item.candidate
            log.info(
                "DRY RUN row=%s company=%s column=%s type=%s items=%s url=%s",
                candidate.registry_row_index,
                candidate.company_name,
                candidate.column_name,
                item.source_type,
                item.items_count,
                candidate.source_url,
            )
        return

    bot_target = _resolve_bot_target(args, settings)
    client = TelegramClient(settings.tg_session_name, settings.tg_api_id, settings.tg_api_hash)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("Collector Telegram session is not authorized.")
        await _send_candidates_sequentially(
            client,
            bot_target,
            validated,
            timeout_sec=int(args.timeout_sec),
        )
    finally:
        await client.disconnect()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = _build_arg_parser().parse_args()
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
