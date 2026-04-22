from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from app.services.app_settings import (
    OWN_BENCH_SYNC_LAST_ERROR_KEY,
    OWN_BENCH_SYNC_LAST_STATS_KEY,
    OWN_BENCH_SYNC_LAST_SUCCESS_AT_KEY,
    get_json_setting,
    get_setting,
    set_json_setting,
    set_setting,
)
from app.services.own_specialists import sync_own_specialists_registry

MSK = ZoneInfo("Europe/Moscow")
OWN_BENCH_SECTION_EMPTY_TEXT = "На нашем бенче нет подходящих специалистов."


def load_sync_state(engine) -> dict[str, Any]:
    return {
        "last_success_at": get_setting(engine, OWN_BENCH_SYNC_LAST_SUCCESS_AT_KEY),
        "last_error": get_setting(engine, OWN_BENCH_SYNC_LAST_ERROR_KEY),
        "last_stats": get_json_setting(engine, OWN_BENCH_SYNC_LAST_STATS_KEY),
    }


def parse_setting_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def format_sync_stamp(value: str | None) -> str | None:
    dt = parse_setting_datetime(value)
    if dt is None:
        return None
    try:
        return dt.astimezone(MSK).strftime("%d.%m %H:%M МСК")
    except Exception:
        return dt.strftime("%d.%m %H:%M")


def trim_sync_error(value: str | None, *, limit: int = 160) -> str | None:
    text_value = re.sub(r"\s+", " ", str(value or "").strip())
    if not text_value:
        return None
    return text_value if len(text_value) <= limit else text_value[: limit - 1].rstrip() + "…"


def compose_empty_text(engine) -> str:
    state = load_sync_state(engine)
    last_error = trim_sync_error(state.get("last_error"))
    last_success = format_sync_stamp(state.get("last_success_at"))
    if last_error and last_success:
        return f"Наш бенч сейчас не синхронизирован. Последняя успешная версия: {last_success}. Последняя ошибка: {last_error}"
    if last_error:
        return f"Наш бенч ещё не синхронизирован. Последняя ошибка: {last_error}"
    return OWN_BENCH_SECTION_EMPTY_TEXT


def get_sync_status(engine) -> dict[str, Any]:
    state = load_sync_state(engine)
    stats = state.get("last_stats") or {}
    return {
        "last_success_at": state.get("last_success_at"),
        "last_success_label": format_sync_stamp(state.get("last_success_at")),
        "last_error": trim_sync_error(state.get("last_error")),
        "stats": stats,
        "active_rows": stats.get("active_rows"),
        "empty_text": compose_empty_text(engine),
    }


async def run_sync(engine, ollama, source_fetcher, *, source_url: str, reason: str = "manual") -> dict[str, Any]:
    try:
        stats = await sync_own_specialists_registry(engine, ollama, source_fetcher, source_url=source_url)
    except Exception as exc:
        set_setting(engine, OWN_BENCH_SYNC_LAST_ERROR_KEY, f"{type(exc).__name__}: {exc}".strip()[:1000])
        raise
    set_setting(engine, OWN_BENCH_SYNC_LAST_SUCCESS_AT_KEY, datetime.now(timezone.utc).isoformat())
    set_setting(engine, OWN_BENCH_SYNC_LAST_ERROR_KEY, "")
    set_json_setting(engine, OWN_BENCH_SYNC_LAST_STATS_KEY, {**(stats or {}), "reason": reason})
    return stats
