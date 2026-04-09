# app/bots/manager_bot.py
from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta, timezone
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from telegram import (
    Message,
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from app.pipeline import preprocess_for_llm, build_fallback_vacancy_item, build_fallback_specialist_item
from app.db.exporter import export_xlsx_bytes
from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.llm.pre_classifier import (
    decide_hybrid_classification,
    normalize_short_bench_line,
    pre_classify_bench_line,
    split_line_wise_bench_items,
)
from app.services.source_ingestion import IngestionUnit, build_external_ingestion_units
from app.integrations.mcp_source_fetcher.normalizers import (
    csv_bytes_to_items,
    csv_bytes_to_items_with_summary,
    docx_bytes_to_text,
    html_to_text,
    pdf_bytes_to_text,
    xlsx_bytes_to_items,
    xlsx_bytes_to_items_with_summary,
)
from app.integrations.mcp_source_fetcher.schemas import NormalizedItem
from app.services.availability import resolve_specialist_is_available
from app.services.app_settings import (
    OWN_BENCH_SOURCE_URL_KEY,
    OWN_BENCH_SYNC_LAST_ERROR_KEY,
    OWN_BENCH_SYNC_LAST_STATS_KEY,
    OWN_BENCH_SYNC_LAST_SUCCESS_AT_KEY,
    ensure_app_settings_table,
    get_json_setting,
    get_setting,
    get_or_init_setting,
    set_json_setting,
    set_setting,
)
from app.services.partner_companies import (
    detect_partner_company_mention,
    ensure_partner_companies_table,
    extract_partner_company_counts_from_sheet,
    load_partner_company_names,
    upsert_partner_company_mentions,
)
from app.services.own_specialists import (
    deactivate_registry_source,
    ensure_own_specialists_registry_table,
    sync_own_specialists_registry,
)
from app.use_cases import digest as digest_use_cases
from app.use_cases import exporting as exporting_use_cases
from app.use_cases import extraction as extraction_use_cases
from app.use_cases import matching as matching_use_cases
from app.use_cases import own_bench as own_bench_use_cases
from app.use_cases import source_trace as source_trace_use_cases
from app.llm.prompts import (
    CLASSIFICATION_SYSTEM_PROMPT_V2,
    VACANCY_EXTRACTION_PROMPT_V2,
    SPECIALIST_EXTRACTION_PROMPT_V2,
)
from app.bots import views

log = logging.getLogger(__name__)

BTN_VAC = "🔍 Вакансия → ТОП кандидатов"
BTN_BENCH = "👤 Кандидат/Бенч → ТОП вакансий"
BTN_LOAD_BENCH = "📥 Загрузить бенч"
BTN_LOAD_VAC = "📥 Загрузить вакансии"
BTN_OWN_BENCH = "📁 Наш бенч"
BTN_OWN_BENCH_CHANGE = "✏️ Изменить ссылку на наш бенч"
BTN_OWN_BENCH_REFRESH = "🔄 Обновить наш бенч"
BTN_BACK = "⬅️ Назад"
BTN_EXPORT_ACTIVE = "📤 Export (active)"
BTN_EXPORT_ALL = "📤 Export (all)"
BTN_DELETE = "🗑 Delete по ссылке t.me/..."
BTN_HELP = "ℹ️ Help"

MODE_NONE = "none"
MODE_FORCE_VAC = "force_vacancy"
MODE_FORCE_BENCH = "force_bench"
MODE_LOAD_VAC = "load_vacancy"
MODE_LOAD_BENCH = "load_bench"
MODE_WAIT_DELETE_LINK = "wait_delete_link"
MODE_WAIT_OWN_BENCH_URL = "wait_own_bench_url"

VECTOR_DIM = 768  # ожидаемая размерность (nomic-embed-text обычно 768)
MSK = ZoneInfo("Europe/Moscow")
MATCH_THRESHOLD = 0.50
SPECIALISTS_EMPTY_TEXT = "На данный момент в Базе нет подходящих специалистов."
VACANCIES_EMPTY_TEXT = "На данный момент в Базе нет подходящих вакансий."
OWN_SPECIALISTS_EMPTY_TEXT = (
    "Собственные подходящие специалисты на текущий момент отсутствуют. Ниже показаны внешние специалисты."
)
OWN_BENCH_SECTION_EMPTY_TEXT = "На нашем бенче нет подходящих специалистов."
OWN_BENCH_URL = "https://disk.360.yandex.ru/i/Az1YZ4V0D1jx2g"
ARCHIVE_REFERENCE_ONLY_MARKER = "Режим: reference-only"

_MATCH_PRIMARY_EXACT_ALIASES = {
    "reactnative": "reactnative",
    "flutter": "flutter",
    "ios": "ios",
    "android": "android",
    "swift": "swift",
    "kotlin": "kotlin",
    "dotnet": "dotnet",
    "csharp": "dotnet",
    "vbnet": "dotnet",
    "aspnet": "dotnet",
    "cpp": "cplusplus",
    "cplusplus": "cplusplus",
    "c": "c",
    "java": "java",
    "python": "python",
    "php": "php",
    "go": "go",
    "golang": "go",
    "ruby": "ruby",
    "scala": "scala",
    "javascript": "javascript",
    "js": "javascript",
    "typescript": "typescript",
    "ts": "typescript",
    "nodejs": "nodejs",
    "react": "react",
    "reactjs": "react",
    "angular": "angular",
    "angularjs": "angular",
    "vue": "vue",
    "vuejs": "vue",
    "qa": "qa",
    "aqa": "qa",
    "devops": "devops",
    "sre": "devops",
    "systemanalyst": "systemanalyst",
    "системныйаналитик": "systemanalyst",
    "sa": "systemanalyst",
    "ca": "systemanalyst",
    "businessanalyst": "businessanalyst",
    "бизнесаналитик": "businessanalyst",
    "ba": "businessanalyst",
    "dataanalyst": "dataanalyst",
    "designer": "designer",
    "uxui": "designer",
    "uidesigner": "designer",
    "uxdesigner": "designer",
    "projectmanager": "projectmanager",
    "pm": "projectmanager",
    "productowner": "productowner",
    "po": "productowner",
    "productmanager": "productmanager",
    "salesforce": "salesforce",
    "1c": "1c",
    "sap": "sap",
}
_MATCH_PRIMARY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("reactnative", re.compile(r"(?i)\breact\s*native\b")),
    ("systemanalyst", re.compile(r"(?i)\b(system\s*analyst|системн\w*\s+аналит\w*)\b")),
    ("businessanalyst", re.compile(r"(?i)\b(business\s*analyst|бизнес\w*[- ]?аналит\w*)\b")),
    ("dataanalyst", re.compile(r"(?i)\b(data\s*analyst|аналит\w*\s+данн\w*)\b")),
    ("productowner", re.compile(r"(?i)\b(product\s*owner)\b")),
    ("productmanager", re.compile(r"(?i)\b(product\s*manager)\b")),
    ("projectmanager", re.compile(r"(?i)\b(project\s*manager|delivery\s*manager|scrum\s*master)\b")),
    ("designer", re.compile(r"(?i)\b(designer|ux/ui|ui/ux|ux\s*designer|ui\s*designer|дизайнер)\b")),
    ("devops", re.compile(r"(?i)\b(devops|platform\s*engineer)\b")),
    ("qa", re.compile(r"(?i)\b(aqa|qa|quality\s*assurance|tester|тестиров\w*)\b")),
    ("flutter", re.compile(r"(?i)\bflutter\b")),
    ("ios", re.compile(r"(?i)\b(ios|objective[- ]?c)\b")),
    ("android", re.compile(r"(?i)\bandroid\b")),
    ("swift", re.compile(r"(?i)\bswift\b")),
    ("kotlin", re.compile(r"(?i)\bkotlin\b")),
    ("dotnet", re.compile(r"(?i)(?:^|[^a-z0-9])(?:\.net|dotnet|c#|csharp|vb\.?net|asp\.?net)(?:[^a-z0-9]|$)")),
    ("cplusplus", re.compile(r"(?i)\b(c\+\+|cplusplus)\b")),
    ("c", re.compile(r"(?i)(?:^|[^a-z0-9])c(?:[^a-z0-9]|$)")),
    ("java", re.compile(r"(?i)\bjava\b(?!\s*script)")),
    ("python", re.compile(r"(?i)\bpython\b")),
    ("php", re.compile(r"(?i)\bphp\b")),
    ("go", re.compile(r"(?i)\bgo(lang)?\b")),
    ("ruby", re.compile(r"(?i)\bruby\b")),
    ("scala", re.compile(r"(?i)\bscala\b")),
    ("nodejs", re.compile(r"(?i)\bnode\.?\s*js\b")),
    ("react", re.compile(r"(?i)\breact(?:\.js)?\b")),
    ("angular", re.compile(r"(?i)\bangular(?:\.js)?\b")),
    ("vue", re.compile(r"(?i)\b(vue(?:\.js)?|nuxt(?:\.js)?)\b")),
    ("typescript", re.compile(r"(?i)\btypescript\b")),
    ("javascript", re.compile(r"(?i)\bjavascript\b")),
    ("salesforce", re.compile(r"(?i)\bsalesforce\b")),
    ("1c", re.compile(r"(?i)(?:^|[^a-z0-9])(1c|1с)(?:[^a-z0-9]|$)")),
    ("sap", re.compile(r"(?i)\bsap\b")),
)
_MATCH_TOOLING_EXACT_ALIASES = {
    "docker": "docker",
    "git": "git",
    "jira": "jira",
    "confluence": "confluence",
    "figma": "figma",
    "postman": "postman",
    "jenkins": "jenkins",
    "nginx": "nginx",
    "webpack": "webpack",
    "vite": "vite",
    "storybook": "storybook",
    "pvsstudio": "pvsstudio",
    "html": "html",
    "css": "css",
    "scss": "scss",
    "sass": "sass",
    "styledcomponents": "styledcomponents",
    "mui": "mui",
    "antd": "antd",
    "recharts": "recharts",
    "axios": "axios",
    "ajax": "ajax",
    "websocket": "websocket",
    "protobuf": "protobuf",
}
_MATCH_TOOLING_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("pvsstudio", re.compile(r"(?i)\bpvs[- ]?studio\b")),
    ("docker", re.compile(r"(?i)\bdocker\b")),
    ("git", re.compile(r"(?i)\bgit\b")),
    ("jira", re.compile(r"(?i)\bjira\b")),
    ("confluence", re.compile(r"(?i)\bconfluence\b")),
    ("figma", re.compile(r"(?i)\bfigma\b")),
    ("postman", re.compile(r"(?i)\bpostman\b")),
    ("jenkins", re.compile(r"(?i)\bjenkins\b")),
    ("nginx", re.compile(r"(?i)\bnginx\b")),
    ("webpack", re.compile(r"(?i)\bwebpack\b")),
    ("vite", re.compile(r"(?i)\bvite\b")),
    ("storybook", re.compile(r"(?i)\bstorybook\b")),
    ("styledcomponents", re.compile(r"(?i)\bstyled[- ]?components\b")),
)
_MATCH_SECONDARY_STOPWORDS = {
    "unknown",
    "developer",
    "engineer",
    "analyst",
    "designer",
    "manager",
    "architect",
    "разработчик",
    "инженер",
    "аналитик",
    "дизайнер",
    "менеджер",
    "архитектор",
    "backend",
    "frontend",
    "fullstack",
    "fullstackdeveloper",
    "middle",
    "middleplus",
    "middleminus",
    "senior",
    "junior",
    "lead",
    "head",
    "staff",
    "principal",
    "remote",
    "office",
    "hybrid",
    "fulltime",
    "parttime",
}
_GRADE_LEVELS = {
    "intern": 0,
    "junior": 1,
    "middle-": 2,
    "middle": 3,
    "middle+": 4,
    "senior": 5,
    "lead": 6,
    "architect": 7,
    "staff": 7,
    "principal": 8,
    "head": 8,
}
_RUSSIA_LOCATION_TOKENS = {"рф", "россия", "russia"}
_REMOTE_LOCATION_TOKENS = {"remote", "удаленка", "удаленно", "удалённо", "anywhere"}


# ------------------------
# Helpers: env + access
# ------------------------
def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def parse_ids(s: str) -> set[int]:
    return {int(x) for x in re.findall(r"-?\d+", s or "")}


def access_allowed(update: Update, allowed_ids: set[int]) -> bool:
    """
    Поддерживаем и user_id, и chat_id:
    - если менеджер пишет в личку: chat_id == user_id (обычно положительный)
    - если менеджеры работают из группы: chat_id отрицательный -100...
    """
    uid = update.effective_user.id if update.effective_user else None
    cid = update.effective_chat.id if update.effective_chat else None
    return bool((uid and uid in allowed_ids) or (cid and cid in allowed_ids))


def can_ingest(update: Update, allowed_ids: set[int], ingest_chat_ids: set[int]) -> bool:
    if access_allowed(update, allowed_ids):
        return True
    cid = update.effective_chat.id if update.effective_chat else None
    return bool(cid and cid in ingest_chat_ids)


def main_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(BTN_VAC), KeyboardButton(BTN_BENCH)],
        [KeyboardButton(BTN_LOAD_VAC), KeyboardButton(BTN_LOAD_BENCH)],
        [KeyboardButton(BTN_OWN_BENCH)],
        [KeyboardButton(BTN_EXPORT_ACTIVE), KeyboardButton(BTN_EXPORT_ALL)],
        [KeyboardButton(BTN_HELP)],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, selective=True)


def own_bench_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(BTN_OWN_BENCH_CHANGE), KeyboardButton(BTN_OWN_BENCH_REFRESH)],
        [KeyboardButton(BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True, selective=True)


def extract_text(update: Update) -> str:
    msg = update.effective_message
    if not msg:
        return ""
    return (msg.text or msg.caption or "").strip()


def _has_supported_document(update: Update) -> bool:
    msg = update.effective_message
    return bool(msg and getattr(msg, "document", None))


def split_telegram(text_out: str, max_len: int = 3800) -> list[str]:
    if len(text_out) <= max_len:
        return [text_out]
    chunks = []
    cur = []
    cur_len = 0
    for line in text_out.splitlines(True):
        if cur_len + len(line) > max_len and cur:
            chunks.append("".join(cur))
            cur, cur_len = [], 0
        cur.append(line)
        cur_len += len(line)
    if cur:
        chunks.append("".join(cur))
    return chunks


def split_telegram_html(text_out: str, max_len: int = 3500) -> list[str]:
    return split_telegram(text_out, max_len=max_len)


async def safe_reply_text(msg, text_out: str, *, disable_preview: bool = True, reply_markup=None) -> None:
    for chunk in split_telegram(text_out):
        await msg.reply_text(chunk, disable_web_page_preview=disable_preview, reply_markup=reply_markup)


async def safe_reply_html(msg, html_out: str, *, disable_preview: bool = True, reply_markup=None, plain_fallback: Optional[str] = None) -> None:
    chunks = split_telegram_html(html_out)
    try:
        for chunk in chunks:
            await msg.reply_text(
                chunk,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=disable_preview,
                reply_markup=reply_markup,
            )
    except Exception:
        log.exception("Failed to send HTML reply; falling back to plain text")
        await safe_reply_text(
            msg,
            plain_fallback or views.html_to_plain(html_out),
            disable_preview=disable_preview,
            reply_markup=reply_markup,
        )


async def safe_send_html(bot, *, chat_id: int, html_out: str, disable_preview: bool = True, reply_markup=None, plain_fallback: Optional[str] = None) -> None:
    chunks = split_telegram_html(html_out)
    try:
        for chunk in chunks:
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=disable_preview,
                reply_markup=reply_markup,
            )
    except Exception:
        log.exception("Failed to send HTML message; falling back to plain text")
        for chunk in split_telegram(plain_fallback or views.html_to_plain(html_out)):
            await bot.send_message(
                chat_id=chat_id,
                text=chunk,
                disable_web_page_preview=disable_preview,
                reply_markup=reply_markup,
            )


async def safe_edit_html(q, html_out: str, *, disable_preview: bool = True, reply_markup=None, plain_fallback: Optional[str] = None) -> None:
    try:
        await q.edit_message_text(
            html_out,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=disable_preview,
            reply_markup=reply_markup,
        )
    except Exception:
        log.exception("Failed to edit HTML message; falling back to plain text")
        await q.edit_message_text(
            plain_fallback or views.html_to_plain(html_out),
            disable_web_page_preview=disable_preview,
            reply_markup=reply_markup,
        )


def _telegram_chat_to_c_id(chat_id: int) -> Optional[str]:
    """
    Для приватных супергрупп/каналов Telegram использует ссылки вида:
    https://t.me/c/<internal_id>/<message_id>
    где internal_id = chat_id без префикса -100.
    """
    s = str(chat_id)
    if s.startswith("-100") and len(s) > 4:
        return s[4:]
    return None


def extract_tme_url_from_text(text_in: str) -> Optional[str]:
    if not text_in:
        return None
    m = re.search(r"(https?://t\.me/[A-Za-z0-9_]+/\d+)", text_in)
    if m:
        return m.group(1).split("?")[0].split("#")[0]
    m = re.search(r"(https?://t\.me/c/\d+/\d+)", text_in)
    if m:
        return m.group(1).split("?")[0].split("#")[0]
    m = re.search(r"\bt\.me/([A-Za-z0-9_]+|\bc/\d+)/\d+\b", text_in)
    if m:
        u = m.group(0)
        if u.startswith("t.me/"):
            u = "https://" + u
        return u.split("?")[0].split("#")[0]
    return None


def extract_current_chat_message_url(update: Update) -> Optional[str]:
    msg = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return None
    username = getattr(chat, "username", None)
    if username:
        return f"https://t.me/{username}/{int(msg.message_id)}"
    cid = _telegram_chat_to_c_id(int(chat.id))
    if cid:
        return f"https://t.me/c/{cid}/{int(msg.message_id)}"
    return None


def extract_source_message_url(update: Update, text_in: str) -> Optional[str]:
    """
    1) Если в тексте уже есть t.me ссылка — используем её.
    2) Если сообщение форвард — строим ссылку на оригинал (публичный username или /c/...).
    3) Иначе пытаемся построить ссылку на текущий чат (если это канал/группа с username или /c/...).
    """
    url = extract_tme_url_from_text(text_in)
    if url:
        return url

    msg = update.effective_message
    if not msg:
        return None

    # Forward origin (python-telegram-bot 21.x): msg.forward_origin
    fo = getattr(msg, "forward_origin", None)
    if fo:
        o_chat = getattr(fo, "chat", None)
        o_mid = getattr(fo, "message_id", None)
        if o_chat is not None and o_mid is not None:
            username = getattr(o_chat, "username", None)
            if username:
                return f"https://t.me/{username}/{int(o_mid)}"
            cid = _telegram_chat_to_c_id(int(getattr(o_chat, "id", 0)))
            if cid:
                return f"https://t.me/c/{cid}/{int(o_mid)}"

    return extract_current_chat_message_url(update)


def _should_create_manual_reference_post(
    *,
    manual_input: bool,
    archive_ingest_mode: bool,
    forward_kind: Optional[str],
    original_text: str,
    external_urls: Optional[list[str]] = None,
) -> bool:
    if not manual_input or archive_ingest_mode:
        return False
    if (forward_kind or "").strip():
        return False
    if extract_tme_url_from_text(original_text):
        return False
    if external_urls:
        return False
    return bool((original_text or "").strip())


def extract_archive_payload_text(text_in: str) -> str:
    """
    Для сообщений из архив-канала collector формирует обертку:
      Источник...
      Дата...
      [Ссылка...]

      Текст сообщения:
      <исходный текст>
    В ingestion режиме классифицируем именно полезную часть после маркера.
    """
    if not text_in:
        return ""
    for marker in ("Копия исходного сообщения", "-- копия исходного сообщения --", "Текст сообщения:"):
        if marker in text_in:
            tail = text_in.split(marker, 1)[1].strip()
            return tail or text_in.strip()
    return text_in.strip()


def extract_archive_declared_type(text_in: str) -> Optional[str]:
    if not text_in:
        return None
    m = re.search(r"(?im)^Тип:\s*(bench|vacancy)\s*$", text_in)
    if not m:
        return None
    return "BENCH" if m.group(1).lower() == "bench" else "VACANCY"


def _display_actor_name(username: Optional[str], fallback_name: Optional[str]) -> str:
    if username:
        return f"@{str(username).lstrip('@')}"
    if fallback_name:
        return str(fallback_name).strip() or "-"
    return "-"


def _manager_display_name(update: Update, *, manual_input: bool) -> str:
    if not manual_input:
        return "-"
    user = update.effective_user
    if not user:
        return "-"
    return _display_actor_name(getattr(user, "username", None), getattr(user, "full_name", None))


def _chat_message_url(chat_id: int, message_id: int, username: Optional[str] = None) -> Optional[str]:
    if username:
        return f"https://t.me/{str(username).lstrip('@')}/{int(message_id)}"
    cid = _telegram_chat_to_c_id(int(chat_id))
    if cid:
        return f"https://t.me/c/{cid}/{int(message_id)}"
    return None


def _normalize_source_index(locator: Optional[str], fallback: Optional[int] = None) -> str:
    text_value = (locator or "").strip()
    m = re.search(r"(\d+)$", text_value)
    if m:
        return m.group(1)
    if fallback is not None:
        return str(int(fallback))
    return "-"


def _compose_source_display(
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
        parts.append(f"Индекс: {_normalize_source_index(external_locator, entity_index)}")
        return "; ".join(parts)
    if source_kind == "archive_post" and canonical_url:
        return (
            f"Менеджер: {manager_name}; "
            f"Ссылка на архив-пост: {canonical_url}; "
            f"Индекс: {_normalize_source_index(None, entity_index)}"
        )
    if canonical_url:
        return f"Менеджер: {manager_name}; Ссылка на сообщение: {canonical_url}"
    return f"Менеджер: {manager_name}"


def _build_source_meta(
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
    sheet_name = (
        str(trace_meta.get("sheet_name") or trace_meta.get("table_name") or "").strip()
        or None
    )
    table_index = trace_meta.get("table_index")
    source_display = _compose_source_display(
        manager_name=manager_name,
        canonical_url=canonical_url,
        external_url=external_url,
        external_locator=external_locator,
        source_kind=source_kind,
        entity_index=entity_index,
        sheet_name=sheet_name,
        table_index=int(table_index) if table_index not in (None, "") else None,
    )
    out = trace_meta
    out["manager_name"] = manager_name
    out["source_kind"] = source_kind
    out["source_display"] = source_display
    if external_locator:
        out["source_index"] = _normalize_source_index(external_locator, entity_index)
    elif entity_index is not None and source_kind == "archive_post":
        out["source_index"] = str(int(entity_index))
    if source_sender_name:
        out["source_sender_name"] = source_sender_name
    return out


def _format_entity_summary(item: dict[str, Any]) -> str:
    role = item.get("role") or "Unknown"
    grade = item.get("grade") or "—"
    stack = ", ".join((item.get("stack") or [])[:6]) or "—"
    return f"{role} | {grade} | {stack}"


def _extract_forward_source_context(msg: Message) -> dict[str, Any]:
    out: dict[str, Any] = {
        "kind": None,
        "message_url": None,
        "source_name": "-",
        "message_date": getattr(msg, "forward_date", None) or getattr(msg, "date", None),
    }

    fo = getattr(msg, "forward_origin", None)
    if fo:
        out["message_date"] = getattr(fo, "date", None) or out["message_date"]

        sender_user = getattr(fo, "sender_user", None)
        if sender_user is not None:
            out["kind"] = "chat"
            out["source_name"] = _display_actor_name(
                getattr(sender_user, "username", None),
                getattr(sender_user, "full_name", None),
            )
            return out

        sender_user_name = getattr(fo, "sender_user_name", None)
        if sender_user_name:
            out["kind"] = "chat"
            out["source_name"] = str(sender_user_name).strip() or "-"
            return out

        origin_chat = getattr(fo, "chat", None) or getattr(fo, "sender_chat", None)
        if origin_chat is not None:
            chat_type = getattr(getattr(origin_chat, "type", None), "value", getattr(origin_chat, "type", None))
            out["kind"] = "channel" if str(chat_type).lower() == "channel" else "chat"
            out["source_name"] = _display_actor_name(
                getattr(origin_chat, "username", None),
                getattr(origin_chat, "title", None),
            )
            o_mid = getattr(fo, "message_id", None)
            if o_mid is not None:
                out["message_url"] = _chat_message_url(
                    int(getattr(origin_chat, "id", 0)),
                    int(o_mid),
                    getattr(origin_chat, "username", None),
                )
            return out

    fchat = getattr(msg, "forward_from_chat", None)
    if fchat is not None:
        chat_type = getattr(getattr(fchat, "type", None), "value", getattr(fchat, "type", None))
        out["kind"] = "channel" if str(chat_type).lower() == "channel" else "chat"
        out["source_name"] = _display_actor_name(getattr(fchat, "username", None), getattr(fchat, "title", None))
        fmid = getattr(msg, "forward_from_message_id", None)
        if fmid is not None:
            out["message_url"] = _chat_message_url(int(getattr(fchat, "id", 0)), int(fmid), getattr(fchat, "username", None))
        return out

    return out


def _build_reference_archive_text(
    *,
    original_text: str,
    original_date: datetime | None,
    source_name: str,
    manager_name: str,
    items: list[str],
) -> str:
    return views.render_reference_archive_post(
        original_text=original_text,
        original_date=original_date,
        source_name=source_name,
        manager_name=manager_name,
        items=items,
        mode_marker="reference-only",
        max_total=3900,
    )


async def _create_reference_archive_post(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    original_text: str,
    original_date: datetime | None,
    source_name: str,
    manager_name: str,
    items: list[str],
) -> Optional[str]:
    target = context.application.bot_data.get("archive_post_target")
    if not target:
        return None
    sent = await context.bot.send_message(
        chat_id=target,
        text=_build_reference_archive_text(
            original_text=original_text,
            original_date=original_date,
            source_name=source_name,
            manager_name=manager_name,
            items=items,
        ),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    return _chat_message_url(int(sent.chat_id), int(sent.message_id), getattr(sent.chat, "username", None))


# ------------------------
# Ollama client (под твой .env)
# ------------------------
class OllamaClient:
    def __init__(self, host: str, llm_model: str, embed_model: Optional[str]):
        self.host = host.rstrip("/")
        self.llm_model = llm_model
        self.embed_model = embed_model

    async def _post(self, path: str, payload: dict) -> tuple[int, str]:
        url = f"{self.host}{path}"
        timeout_sec = int(os.getenv("OLLAMA_HTTP_TIMEOUT_SEC", "1200"))
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.post(url, json=payload) as r:
                return r.status, await r.text()

    async def chat(self, system_prompt: str, user_text: str, num_predict: int = 1200) -> str:
        # 1) /api/chat
        st, body = await self._post(
            "/api/chat",
            {
                "model": self.llm_model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_text},
                ],
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": num_predict},
            },
        )
        if st == 200:
            data = json.loads(body)
            return ((data.get("message") or {}).get("content") or "").strip()

        # 2) /api/generate fallback
        st2, body2 = await self._post(
            "/api/generate",
            {
                "model": self.llm_model,
                "prompt": f"{system_prompt}\n\nUSER_MESSAGE:\n{user_text}",
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": num_predict},
            },
        )
        if st2 == 200:
            data = json.loads(body2)
            return (data.get("response") or "").strip()

        raise RuntimeError(
            f"Ollama endpoints not found. /api/chat HTTP {st}; /api/generate HTTP {st2}. host={self.host}"
        )

    async def embed(self, text_in: str) -> Optional[list[float]]:
        if not self.embed_model:
            return None

        # /api/embeddings (часто так)
        st, body = await self._post(
            "/api/embeddings",
            {"model": self.embed_model, "prompt": text_in},
        )
        if st == 200:
            data = json.loads(body)
            emb = data.get("embedding")
            return emb if isinstance(emb, list) else None

        # /api/embed (альтернатива)
        st2, body2 = await self._post(
            "/api/embed",
            {"model": self.embed_model, "input": text_in},
        )
        if st2 == 200:
            data = json.loads(body2)
            embs = data.get("embeddings")
            if isinstance(embs, list) and embs and isinstance(embs[0], list):
                return embs[0]

        return None

    async def diag(self) -> dict:
        out = {"host": self.host, "llm_model": self.llm_model, "embed_model": self.embed_model, "checks": {}}
        for path, payload in [
            ("/api/chat", {"model": self.llm_model, "messages": [{"role": "user", "content": "ping"}], "stream": False}),
            ("/api/generate", {"model": self.llm_model, "prompt": "ping", "stream": False}),
            ("/api/embeddings", {"model": self.embed_model or self.llm_model, "prompt": "ping"}),
            ("/api/embed", {"model": self.embed_model or self.llm_model, "input": "ping"}),
        ]:
            try:
                st, _ = await self._post(path, payload)
                out["checks"][path] = f"HTTP {st}"
            except Exception as e:
                out["checks"][path] = f"FAIL {type(e).__name__}"
        return out


def safe_json_loads(model_text: str) -> dict:
    """
    Делает best-effort парсинг JSON из ответа LLM.
    Никогда не кидает исключение наружу — возвращает {} при ошибке.
    """
    try:
        t = (model_text or "").strip()
        if t.startswith("```"):
            t = t.strip("`")
            t = t.replace("json", "", 1).strip()
        start = t.find("{")
        end = t.rfind("}")
        if start != -1 and end != -1 and end > start:
            t = t[start : end + 1]
        return json.loads(t)
    except Exception:
        log.warning("safe_json_loads: failed to parse model response (len=%s)", len(model_text or ""))
        return {}


# ------------------------
# DB helpers (4 таблицы)
# ------------------------
def generate_synthetic_id(role: str, stack: list[str], grade: Optional[str], rate_hint: Optional[int]) -> str:
    role_n = (role or "").strip().lower()
    grade_n = (grade or "").strip().lower()
    stack_n = sorted([s.strip().lower() for s in (stack or []) if s and s.strip()])
    rate_bucket = (rate_hint or 0) // 10000
    normalized = f"{role_n}|{stack_n}|{grade_n}|{rate_bucket}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def build_search_text(entity: dict) -> str:
    parts = []
    if entity.get("role"):
        parts.append(f"role: {entity['role']}")
    if entity.get("grade"):
        parts.append(f"grade: {entity['grade']}")
    st = entity.get("stack") or []
    if st:
        parts.append("stack: " + ", ".join(st[:25]))
    if entity.get("location"):
        parts.append(f"location: {entity['location']}")
    if entity.get("description"):
        parts.append("desc: " + str(entity["description"])[:800])
    return " | ".join(parts)


def _vector_str(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in v) + "]"


def upsert_vacancy(engine, data: dict, original_text: str, embedding: Optional[list[float]], status: str) -> str:
    stack = _sanitize_stack_values(data.get("stack")) or _sanitize_stack_values(data.get("role"))
    role = _coerce_entity_role(data.get("role"), stack=stack, raw_unit_text=original_text, kind="VACANCY")
    stack = stack or ([role] if role != "Unknown" else [])
    grade = _truncate_text(data.get("grade"), 50)

    exp = data.get("experience_years_min")
    if data.get("experience_years_min") and data.get("experience_years_min") == data.get("experience_years_max"):
        exp = data.get("experience_years_min")

    rate_hint = data.get("rate_min") or data.get("rate_max")
    syn = generate_synthetic_id(role, stack, grade, rate_hint)

    emb = None
    if embedding and len(embedding) == VECTOR_DIM:
        emb = _vector_str(embedding)

    closed_at = datetime.now(timezone.utc) if status == "closed" else None

    q = text(
        """
        INSERT INTO vacancies(
          synthetic_id, role, stack, grade, experience_years,
          rate_min, rate_max, currency, company, location,
          description, original_text, embedding, status, expires_at, close_reason, closed_at
        )
        VALUES (
          :syn, :role, CAST(:stack AS jsonb), :grade, :exp,
          :rmin, :rmax, :cur, :company, :loc,
          :desc, :orig,
          CAST(:emb AS vector),
          CAST(:status AS varchar(20)),
          NOW() + interval '30 days',
          :close_reason,
          :closed_at
        )
        ON CONFLICT(synthetic_id) DO UPDATE SET updated_at=NOW()
        RETURNING id
        """
    )

    with engine.begin() as c:
        vid = c.execute(
            q,
            {
                "syn": syn,
                "role": role,
                "stack": json.dumps(stack),
                "grade": grade,
                "exp": exp,
                "rmin": data.get("rate_min"),
                "rmax": data.get("rate_max"),
                "cur": _truncate_text(data.get("currency"), 10) or "RUB",
                "company": _truncate_text(data.get("company"), 255),
                "loc": _truncate_text(data.get("location"), 255),
                "desc": data.get("description") or None,
                "orig": original_text,
                "emb": emb,
                "status": status,
                "close_reason": data.get("close_reason"),
                "closed_at": closed_at,
            },
        ).scalar_one()

    return str(vid)


def upsert_specialist(
    engine,
    data: dict,
    original_text: str,
    embedding: Optional[list[float]],
    status: str,
    *,
    is_internal: bool = False,
) -> str:
    stack = _sanitize_stack_values(data.get("stack")) or _sanitize_stack_values(data.get("role"))
    role = _coerce_entity_role(data.get("role"), stack=stack, raw_unit_text=original_text, kind="BENCH")
    stack = stack or ([role] if role != "Unknown" else [])
    grade = _truncate_text(data.get("grade"), 50)

    exp = data.get("experience_years_min")
    if data.get("experience_years_min") and data.get("experience_years_min") == data.get("experience_years_max"):
        exp = data.get("experience_years_min")

    rate_hint = data.get("rate_min") or data.get("rate_max")
    syn = generate_synthetic_id(role, stack, grade, rate_hint)

    emb = None
    if embedding and len(embedding) == VECTOR_DIM:
        emb = _vector_str(embedding)

    hired_at = datetime.now(timezone.utc) if status == "hired" else None

    q = text(
        """
        INSERT INTO specialists(
          synthetic_id, role, stack, grade, experience_years,
          rate_min, rate_max, currency, location,
          description, original_text, embedding, status, expires_at, hired_at, is_internal
        )
        VALUES (
          :syn, :role, CAST(:stack AS jsonb), :grade, :exp,
          :rmin, :rmax, :cur, :loc,
          :desc, :orig,
          CAST(:emb AS vector),
          CAST(:status AS varchar(20)),
          NOW() + interval '30 days',
          :hired_at,
          :is_internal
        )
        ON CONFLICT(synthetic_id) DO UPDATE SET
          updated_at=NOW(),
          is_internal = (specialists.is_internal OR EXCLUDED.is_internal)
        RETURNING id
        """
    )

    with engine.begin() as c:
        sid = c.execute(
            q,
            {
                "syn": syn,
                "role": role,
                "stack": json.dumps(stack),
                "grade": grade,
                "exp": exp,
                "rmin": data.get("rate_min"),
                "rmax": data.get("rate_max"),
                "cur": _truncate_text(data.get("currency"), 10) or "RUB",
                "loc": _truncate_text(data.get("location"), 255),
                "desc": data.get("description") or None,
                "orig": original_text,
                "emb": emb,
                "status": status,
                "hired_at": hired_at,
                "is_internal": bool(is_internal),
            },
        ).scalar_one()

    return str(sid)


def ensure_sources_extra_columns(engine) -> None:
    ddl = [
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS source_meta JSONB",
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS external_url VARCHAR(1024)",
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS external_kind VARCHAR(64)",
        "ALTER TABLE sources ADD COLUMN IF NOT EXISTS external_locator VARCHAR(128)",
        "ALTER TABLE specialists ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT FALSE",
        "ALTER TABLE channels ADD COLUMN IF NOT EXISTS source_kind VARCHAR(20) DEFAULT 'chat'",
    ]
    with engine.begin() as c:
        for q in ddl:
            c.execute(text(q))


def insert_source(
    engine,
    entity_type: str,
    entity_id: str,
    update: Update,
    message_url: Optional[str],
    raw_text: str,
    idx: int,
    *,
    source_type: Optional[str] = None,
    external_url: Optional[str] = None,
    external_kind: Optional[str] = None,
    external_locator: Optional[str] = None,
    source_meta: Optional[dict[str, Any]] = None,
) -> None:
    """
    Schema sources имеет UNIQUE(channel_id, message_id).
    Чтобы поддержать список (несколько сущностей в одном сообщении),
    делаем message_id "уникальным" как msg_id*1000 + idx.
    """
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not (msg and chat):
        return
    source_trace_use_cases.insert_source(
        engine,
        entity_type=entity_type,
        entity_id=entity_id,
        channel_id=int(chat.id),
        message_id=int(msg.message_id),
        chat_title=getattr(chat, "title", None),
        sender_id=int(user.id) if user else None,
        sender_name=user.full_name if user else None,
        message_url=message_url,
        raw_text=raw_text,
        idx=idx,
        source_type=source_type or ("forward" if getattr(msg, "forward_origin", None) else "manual"),
        external_url=external_url,
        external_kind=external_kind,
        external_locator=external_locator,
        source_meta=source_meta,
    )


def _merge_specialist_hits(*groups: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for group in groups:
        for hit in group or []:
            sid = str(hit.get("id") or "").strip()
            if not sid:
                continue
            existing = merged.get(sid)
            if existing is None:
                merged[sid] = dict(hit)
                continue
            candidate = dict(existing)
            existing_sim = float(existing.get("sim") or 0.0)
            hit_sim = float(hit.get("sim") or 0.0)
            if hit_sim > existing_sim:
                candidate.update(hit)
            candidate["sim"] = max(existing_sim, hit_sim)
            candidate["is_internal"] = bool(existing.get("is_internal")) or bool(hit.get("is_internal"))
            candidate["is_own_bench_source"] = bool(existing.get("is_own_bench_source")) or bool(hit.get("is_own_bench_source"))
            if not candidate.get("url"):
                candidate["url"] = hit.get("url") or existing.get("url")
            if not candidate.get("source_display"):
                candidate["source_display"] = hit.get("source_display") or existing.get("source_display")
            merged[sid] = candidate
    return list(merged.values())


def search_specialists(engine, query_emb: Optional[list[float]], query_text: str, k: int, *, own_bench_url: Optional[str] = None) -> list[dict]:
    own_url = (own_bench_url or "").strip()
    def _search_active_own_bench_specialists(limit: int) -> list[dict]:
        if not own_url:
            return []
        sql = text(
            """
            SELECT
              s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location, s.is_internal,
              0.0 AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url,
              (
                SELECT COALESCE(source_meta ->> 'source_display', message_url)
                FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id
                ORDER BY created_at DESC
                LIMIT 1
              ) AS source_display,
              TRUE AS is_own_bench_source
            FROM specialists s
            WHERE s.status='active'
              AND (s.expires_at IS NULL OR s.expires_at > NOW())
              AND EXISTS(
                SELECT 1 FROM own_specialists_registry reg
                WHERE reg.specialist_id = s.id
                  AND reg.is_active = TRUE
                  AND COALESCE(reg.source_url, '') = :own_url
              )
            ORDER BY s.updated_at DESC
            LIMIT :k
            """
        )
        with engine.begin() as c:
            rows = c.execute(sql, {"own_url": own_url, "k": limit}).mappings().all()
        return [dict(r) for r in rows]

    if query_emb and len(query_emb) == VECTOR_DIM:
        qv = _vector_str(query_emb)
        sql = text(
            """
            SELECT
              s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location, s.is_internal,
              GREATEST(0.0, LEAST(1.0, 1 - (s.embedding <=> CAST(:q AS vector)))) AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url,
              (
                SELECT COALESCE(source_meta ->> 'source_display', message_url)
                FROM sources
                WHERE entity_type='specialist' AND entity_id=s.id
                ORDER BY created_at DESC
                LIMIT 1
              ) AS source_display,
              CASE
                WHEN :own_url = '' THEN FALSE
                ELSE EXISTS(
                  SELECT 1 FROM own_specialists_registry reg
                  WHERE reg.specialist_id = s.id
                    AND reg.is_active = TRUE
                    AND COALESCE(reg.source_url, '') = :own_url
                )
              END AS is_own_bench_source
            FROM specialists s
            WHERE s.status='active'
              AND (s.expires_at IS NULL OR s.expires_at > NOW())
              AND s.embedding IS NOT NULL
            ORDER BY s.embedding <=> CAST(:q AS vector)
            LIMIT :k
            """
        )
        with engine.begin() as c:
            rows = c.execute(sql, {"q": qv, "k": k, "own_url": own_url}).mappings().all()
        vector_hits = [dict(r) for r in rows]
        own_hits = _search_active_own_bench_specialists(max(k, 100))
        return _merge_specialist_hits(vector_hits, own_hits)

    sql = text(
        """
        SELECT
          s.id, s.role, s.stack, s.grade, s.rate_min, s.rate_max, s.currency, s.location, s.is_internal,
          0.0 AS sim,
          (
            SELECT message_url FROM sources
            WHERE entity_type='specialist' AND entity_id=s.id AND message_url IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
          ) AS url,
          (
            SELECT COALESCE(source_meta ->> 'source_display', message_url)
            FROM sources
            WHERE entity_type='specialist' AND entity_id=s.id
            ORDER BY created_at DESC
            LIMIT 1
          ) AS source_display,
          CASE
            WHEN :own_url = '' THEN FALSE
            ELSE EXISTS(
              SELECT 1 FROM own_specialists_registry reg
              WHERE reg.specialist_id = s.id
                AND reg.is_active = TRUE
                AND COALESCE(reg.source_url, '') = :own_url
            )
          END AS is_own_bench_source
        FROM specialists s
        WHERE s.status='active'
          AND (s.expires_at IS NULL OR s.expires_at > NOW())
          AND (
            LOWER(s.role) LIKE LOWER(:q)
            OR LOWER(COALESCE(s.description,'')) LIKE LOWER(:q)
            OR LOWER(s.original_text) LIKE LOWER(:q)
          )
        ORDER BY s.updated_at DESC
        LIMIT :k
        """
    )
    with engine.begin() as c:
        rows = c.execute(sql, {"q": f"%{query_text[:80]}%", "k": k, "own_url": own_url}).mappings().all()
    lexical_hits = [dict(r) for r in rows]
    own_hits = _search_active_own_bench_specialists(max(k, 100))
    return _merge_specialist_hits(lexical_hits, own_hits)


def search_vacancies(engine, query_emb: Optional[list[float]], query_text: str, k: int) -> list[dict]:
    if query_emb and len(query_emb) == VECTOR_DIM:
        qv = _vector_str(query_emb)
        sql = text(
            """
            SELECT
              v.id, v.role, v.stack, v.grade, v.rate_min, v.rate_max, v.currency, v.location,
              GREATEST(0.0, LEAST(1.0, 1 - (v.embedding <=> CAST(:q AS vector)))) AS sim,
              (
                SELECT message_url FROM sources
                WHERE entity_type='vacancy' AND entity_id=v.id AND message_url IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
              ) AS url,
              (
                SELECT COALESCE(source_meta ->> 'source_display', message_url)
                FROM sources
                WHERE entity_type='vacancy' AND entity_id=v.id
                ORDER BY created_at DESC
                LIMIT 1
              ) AS source_display
            FROM vacancies v
            WHERE v.status='active'
              AND (v.expires_at IS NULL OR v.expires_at > NOW())
              AND v.embedding IS NOT NULL
            ORDER BY v.embedding <=> CAST(:q AS vector)
            LIMIT :k
            """
        )
        with engine.begin() as c:
            rows = c.execute(sql, {"q": qv, "k": k}).mappings().all()
        return [dict(r) for r in rows]

    sql = text(
        """
        SELECT
          v.id, v.role, v.stack, v.grade, v.rate_min, v.rate_max, v.currency, v.location,
          0.0 AS sim,
          (
            SELECT message_url FROM sources
            WHERE entity_type='vacancy' AND entity_id=v.id AND message_url IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
          ) AS url,
          (
            SELECT COALESCE(source_meta ->> 'source_display', message_url)
            FROM sources
            WHERE entity_type='vacancy' AND entity_id=v.id
            ORDER BY created_at DESC
            LIMIT 1
          ) AS source_display
        FROM vacancies v
        WHERE v.status='active'
          AND (v.expires_at IS NULL OR v.expires_at > NOW())
          AND (
            LOWER(v.role) LIKE LOWER(:q)
            OR LOWER(COALESCE(v.description,'')) LIKE LOWER(:q)
            OR LOWER(v.original_text) LIKE LOWER(:q)
          )
        ORDER BY v.updated_at DESC
        LIMIT :k
        """
    )
    with engine.begin() as c:
        rows = c.execute(sql, {"q": f"%{query_text[:80]}%", "k": k}).mappings().all()
    return [dict(r) for r in rows]


def upsert_matches(engine, vacancy_id: str, specialist_hits: list[dict]) -> None:
    sql = text(
        """
        INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
        VALUES (:vid, :sid, :score, :rank)
        ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
          SET similarity_score=EXCLUDED.similarity_score,
              rank=EXCLUDED.rank,
              updated_at=NOW()
        """
    )
    with engine.begin() as c:
        for i, h in enumerate(specialist_hits, start=1):
            c.execute(sql, {"vid": vacancy_id, "sid": h["id"], "score": float(h["sim"] or 0.0), "rank": i})


def upsert_matches_reverse(engine, specialist_id: str, vacancy_hits: list[dict]) -> None:
    sql = text(
        """
        INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
        VALUES (:vid, :sid, :score, :rank)
        ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
          SET similarity_score=EXCLUDED.similarity_score,
              rank=EXCLUDED.rank,
              updated_at=NOW()
        """
    )
    with engine.begin() as c:
        for i, h in enumerate(vacancy_hits, start=1):
            c.execute(sql, {"vid": h["id"], "sid": specialist_id, "score": float(h["sim"] or 0.0), "rank": i})


# ------------------------
# Delete helpers
# ------------------------
def canonicalize_tme_url(url: str) -> Optional[str]:
    u = (url or "").strip()
    if not u:
        return None
    if u.startswith("t.me/"):
        u = "https://" + u
    u = u.split("?")[0].split("#")[0]
    if not re.match(r"^https://t\.me/([A-Za-z0-9_]+|c/\d+)/\d+$", u):
        return None
    return u


def extract_delete_target_url(update: Update, text_in: str) -> Optional[str]:
    direct = canonicalize_tme_url(text_in)
    if direct:
        return direct
    derived = extract_source_message_url(update, text_in or "")
    return canonicalize_tme_url(derived or "")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed = context.application.bot_data["allowed_ids"]
    if not access_allowed(update, allowed):
        await safe_reply_html(update.effective_message, views.render_status_message("Нет доступа.", warning=True))
        return

    context.user_data["mode"] = MODE_NONE
    await safe_reply_html(update.effective_message, views.render_start(), reply_markup=main_keyboard())


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed = context.application.bot_data["allowed_ids"]
    if not access_allowed(update, allowed):
        await safe_reply_html(update.effective_message, views.render_status_message("Нет доступа.", warning=True))
        return
    await safe_reply_html(update.effective_message, views.render_help(), reply_markup=main_keyboard())


def fmt_money(rmin: Optional[int], rmax: Optional[int], cur: Optional[str]) -> str:
    if rmin is None and rmax is None:
        return "—"
    cur = cur or ""
    if rmin is not None and rmax is not None and rmin != rmax:
        return f"{rmin:,}-{rmax:,} {cur}".replace(",", " ")
    v = rmin if rmin is not None else rmax
    return f"{v:,} {cur}".replace(",", " ")


def _structured_rate(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _truncate_text(value: Any, limit: int) -> Optional[str]:
    if value is None:
        return None
    text_value = re.sub(r"\s+", " ", str(value)).strip()
    if not text_value:
        return None
    return text_value[:limit]


def _looks_like_stack_blob(value: str) -> bool:
    candidate = (value or "").strip()
    if not candidate:
        return False
    if len(candidate) > 120:
        return True
    if re.search(r"(?i)\b(langs?|databases?|devops|stack|tools?|frameworks?)\s*:", candidate):
        return True
    if candidate.count(",") >= 5 or candidate.count("\n") >= 2:
        return True
    return False


def _looks_like_urlish(value: str) -> bool:
    candidate = (value or "").strip().lower()
    return bool(candidate and ("http://" in candidate or "https://" in candidate or "docs.google.com/" in candidate or "t.me/" in candidate))


_ROLE_LIKE_NAME_RE = re.compile(
    r"(?i)\b("
    r"developer|engineer|designer|manager|architect|analyst|consultant|specialist|candidate|"
    r"разработчик|дизайнер|менеджер|архитектор|аналитик|специалист|кандидат|"
    r"frontend|backend|fullstack|qa|devops|python|java|react|flutter|android|ios|mobile|middle|senior|junior|lead"
    r")\b"
)


def _coerce_person_name(fields: dict[str, Any], raw_unit_text: str, *, role: str | None = None) -> Optional[str]:
    def _clean_name(value: Any) -> Optional[str]:
        candidate = _truncate_text(value, 120)
        if not candidate:
            return None
        if _looks_like_urlish(candidate):
            return None
        if role and candidate.casefold() == str(role).strip().casefold():
            return None
        if any(ch.isdigit() for ch in candidate):
            return None
        if _ROLE_LIKE_NAME_RE.search(candidate):
            return None
        return candidate

    direct = _clean_name(fields.get("name"))
    if direct:
        return direct

    for line in (raw_unit_text or "").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        norm_key = re.sub(r"[^a-zа-яё0-9]+", " ", key.strip().lower()).strip()
        if any(token in norm_key for token in ("имя", "фио", "кандидат", "специалист", "name", "candidate", "specialist")):
            named = _clean_name(value)
            if named:
                return named

    for line in (raw_unit_text or "").splitlines()[:12]:
        candidate = line.strip()
        if not candidate or ":" in candidate or _looks_like_urlish(candidate):
            continue
        if len(candidate.split()) > 4:
            continue
        if _ROLE_LIKE_NAME_RE.search(candidate):
            continue
        if re.match(r"^[A-ZА-ЯЁ][a-zа-яё]+(?:\s+[A-ZА-ЯЁ][a-zа-яё.]+){0,2}$", candidate):
            return candidate
    return None


def _sanitize_stack_values(value: Any) -> list[str]:
    raw_values: list[str] = []
    if isinstance(value, list):
        raw_values = [str(v) for v in value if str(v).strip()]
    elif isinstance(value, str) and value.strip():
        raw_values = [value]

    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        parts = re.split(r"[\n,;]|(?:\s{2,})", raw)
        for part in parts:
            cleaned = re.sub(r"(?i)^(langs?|databases?|devops|stack|tools?|frameworks?)\s*:\s*", "", part).strip()
            if not cleaned:
                continue
            if ":" in cleaned and len(cleaned) > 40:
                continue
            if len(cleaned) > 80:
                continue
            key = _normalize_stack_token(cleaned)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(cleaned)
    return out[:20]


def _coerce_entity_role(role: Any, *, stack: list[str], raw_unit_text: str, kind: str) -> str:
    candidate = str(role or "").strip()
    if candidate and not _looks_like_stack_blob(candidate) and not _looks_like_urlish(candidate):
        return (_truncate_text(candidate, 255) or "Unknown")

    pre = preprocess_for_llm(raw_unit_text, kind=kind)
    hinted = str((pre.hints.get("role") or "")).strip()
    if hinted and not _looks_like_stack_blob(hinted) and not _looks_like_urlish(hinted):
        return (_truncate_text(hinted, 255) or "Unknown")

    for token in stack:
        if re.search(r"(?i)\b(developer|engineer|analyst|designer|manager|architect|разработ|аналит|дизайн|менедж|архитект)\w*\b", token):
            return (_truncate_text(token, 255) or "Unknown")

    for token in stack:
        if not _looks_like_urlish(token):
            return (_truncate_text(token, 255) or "Unknown")

    return "Unknown"


def _structured_stack(value: Any) -> list[str]:
    return _sanitize_stack_values(value)


def _structured_description(fields: dict[str, Any], raw_unit_text: str) -> str:
    parts: list[str] = []
    for key in ("description", "requirements_text", "responsibilities_text", "english", "availability", "resume_url"):
        value = fields.get(key)
        if value:
            parts.append(str(value))
    parts.append(raw_unit_text.strip())
    return "\n".join(p for p in parts if p).strip()[:2000]


def _build_structured_specialist_item(fields: dict[str, Any], raw_unit_text: str) -> dict:
    stack = _structured_stack(fields.get("stack"))
    role = _coerce_entity_role(fields.get("role"), stack=stack, raw_unit_text=raw_unit_text, kind="BENCH")
    stack = stack or ([role] if role != "Unknown" else [])
    description = _structured_description(fields, raw_unit_text)
    availability = str(fields.get("availability") or "").lower()
    is_available = not bool(re.search(r"(?i)\b(not available|занят|недоступ|hired|off)\b", availability))
    return {
        "name": _coerce_person_name(fields, raw_unit_text, role=role),
        "role": role,
        "stack": stack,
        "grade": _truncate_text(fields.get("grade"), 50),
        "experience_years_min": None,
        "experience_years_max": None,
        "rate_min": _structured_rate(fields.get("rate_min")),
        "rate_max": None,
        "currency": _truncate_text(fields.get("currency"), 10),
        "rate_period": None,
        "rate_is_net": None,
        "location": _truncate_text(fields.get("location"), 255),
        "work_format": _truncate_text(fields.get("work_format"), 120),
        "timezone": None,
        "availability_weeks": 0 if availability in {"asap", "now", "сразу"} else None,
        "is_available": is_available,
        "contacts": [],
        "source_urls": [str(fields["resume_url"])] if fields.get("resume_url") else [],
        "languages": [str(fields["english"])] if fields.get("english") else [],
        "relocation": None,
        "description": description or None,
    }


def _build_structured_vacancy_item(fields: dict[str, Any], raw_unit_text: str) -> dict:
    stack = _structured_stack(fields.get("stack"))
    role = _coerce_entity_role(fields.get("role"), stack=stack, raw_unit_text=raw_unit_text, kind="VACANCY")
    stack = stack or ([role] if role != "Unknown" else [])
    description = _structured_description(fields, raw_unit_text)
    return {
        "role": role,
        "stack": stack,
        "grade": _truncate_text(fields.get("grade"), 50),
        "experience_years_min": None,
        "experience_years_max": None,
        "rate_min": _structured_rate(fields.get("rate_min")),
        "rate_max": None,
        "currency": _truncate_text(fields.get("currency"), 10),
        "rate_period": None,
        "rate_is_net": None,
        "company": _truncate_text(fields.get("company"), 255),
        "client": _truncate_text(fields.get("client"), 255),
        "location": _truncate_text(fields.get("location"), 255),
        "work_format": _truncate_text(fields.get("work_format"), 120),
        "timezone": None,
        "employment_type": _truncate_text(fields.get("employment_type"), 120),
        "start_date": None,
        "duration_months": None,
        "responsibilities": [str(fields["responsibilities_text"])] if fields.get("responsibilities_text") else [],
        "requirements": [str(fields["requirements_text"])] if fields.get("requirements_text") else [],
        "nice_to_have": [],
        "benefits": [],
        "contacts": [],
        "source_urls": [],
        "is_closed": False,
        "close_reason": None,
        "description": description,
    }


async def _build_attachment_ingestion_units(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    source_url: Optional[str],
) -> tuple[list[IngestionUnit], list[dict[str, Any]]]:
    msg = update.effective_message
    document = getattr(msg, "document", None) if msg else None
    if not msg or not document:
        return [], []

    file_name = str(getattr(document, "file_name", None) or "attachment").strip()
    mime_type = str(getattr(document, "mime_type", None) or "").lower()

    tg_file = await context.bot.get_file(document.file_id)
    data = bytes(await tg_file.download_as_bytearray())
    source_ref = source_url or file_name
    items, import_summary = _normalize_attachment_items(data, file_name=file_name, mime_type=mime_type, source_ref=source_ref)

    out: list[IngestionUnit] = []
    for item in items:
        if item.row_index is not None:
            expanded_text = (
                f"External source kind: telegram_attachment\n"
                f"External source URL: {source_ref}\n"
                f"Row: {item.row_index}\n\n"
                f"{item.text.strip()}"
            ).strip()
        else:
            expanded_text = (
                f"External source kind: telegram_attachment\n"
                f"External source URL: {source_ref}\n\n"
                f"{item.text.strip()}"
            ).strip()
        locator = f"row:{item.row_index}" if item.row_index is not None else None
        out.append(
            IngestionUnit(
                text=expanded_text,
                source_type="telegram_attachment",
                external_url=source_url,
                external_kind="telegram_attachment",
                external_locator=locator,
                source_meta={
                    "external_url": source_url,
                    "external_type": "telegram_attachment",
                    "external_row_index": item.row_index,
                    "external_metadata": item.metadata,
                    "entity_hint": (item.metadata or {}).get("entity_hint"),
                    "structured_fields": (item.metadata or {}).get("structured_fields"),
                    "table_name": (item.metadata or {}).get("table_name"),
                    "sheet_name": (item.metadata or {}).get("sheet_name") or (item.metadata or {}).get("table_name"),
                    "sheet_index": (item.metadata or {}).get("sheet_index"),
                    "table_index": (item.metadata or {}).get("table_index"),
                    "header_row_index": (item.metadata or {}).get("header_row_index"),
                    "row_map": (item.metadata or {}).get("row_map"),
                    "confidence": (item.metadata or {}).get("confidence"),
                    "confidence_reason": (item.metadata or {}).get("confidence_reason"),
                    "attachment_filename": file_name,
                    "attachment_mime_type": mime_type,
                },
            )
        )
    summaries = []
    if import_summary:
        summaries.append(
            {
                **import_summary,
                "source_url": source_ref,
                "source_type": "telegram_attachment",
                "source_label": file_name,
            }
        )
    return out, summaries


def _normalize_attachment_items(data: bytes, *, file_name: str, mime_type: str, source_ref: str):
    name = file_name.lower()
    if mime_type.startswith("text/csv") or name.endswith(".csv"):
        return csv_bytes_to_items_with_summary(data, source_ref)
    if "spreadsheetml.sheet" in mime_type or name.endswith(".xlsx"):
        return xlsx_bytes_to_items_with_summary(data, source_ref)
    if "wordprocessingml.document" in mime_type or name.endswith(".docx"):
        txt = docx_bytes_to_text(data)
        items = [NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []
        return items, None
    if "application/pdf" in mime_type or name.endswith(".pdf"):
        txt = pdf_bytes_to_text(data)
        items = [NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []
        return items, None
    if mime_type.startswith("text/plain") or name.endswith(".txt"):
        txt = data.decode("utf-8", errors="replace").strip()
        items = [NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []
        return items, None
    if mime_type.startswith("text/html") or name.endswith(".html") or name.endswith(".htm"):
        txt = html_to_text(data.decode("utf-8", errors="replace"))
        items = [NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []
        return items, None
    return [], None

def _normalize_stack_token(value: str) -> str:
    s = (value or "").strip().lower()
    s = s.replace(".net", "dotnet").replace("c#", "csharp")
    s = re.sub(r"[^a-z0-9а-я]+", "", s)
    aliases = {
        "са": "systemanalyst",
        "ca": "systemanalyst",
        "системныйаналитик": "systemanalyst",
        "systemanalyst": "systemanalyst",
        "ба": "businessanalyst",
        "бизнесаналитик": "businessanalyst",
        "businessanalyst": "businessanalyst",
        "qaengineer": "qa",
    }
    s = aliases.get(s, s)
    return s


def _normalize_match_token(value: str) -> str:
    s = (value or "").strip().lower()
    s = s.replace("1с", "1c")
    s = s.replace(".net", " dotnet ")
    s = s.replace("c#", " csharp ")
    s = s.replace("c++", " cpp ")
    s = s.replace("react native", " reactnative ")
    s = s.replace("node.js", " nodejs ")
    s = s.replace("react.js", " reactjs ")
    s = s.replace("vue.js", " vuejs ")
    s = s.replace("angular.js", " angularjs ")
    s = re.sub(r"[^a-z0-9а-я]+", "", s)
    return s


def _unique_keep_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = _normalize_match_token(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _iter_match_texts(value: Any) -> list[str]:
    if not value:
        return []
    raw_values = value if isinstance(value, list) else [value]
    texts: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        text = str(raw or "").strip()
        if not text or _looks_like_urlish(text):
            continue
        for candidate in [text, *re.split(r"[\n,;/|]|(?:\s{2,})", text)]:
            cleaned = str(candidate or "").strip()
            if not cleaned or _looks_like_urlish(cleaned):
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            texts.append(cleaned)
    return texts


def _primary_labels_for_text(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate:
        return []
    norm = _normalize_match_token(candidate)
    exact = _MATCH_PRIMARY_EXACT_ALIASES.get(norm)
    if exact:
        return [exact]

    labels: list[str] = []
    for label, pattern in _MATCH_PRIMARY_PATTERNS:
        if pattern.search(candidate):
            labels.append(label)
    return _unique_keep_order(labels)


def _tooling_labels_for_text(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate:
        return []
    norm = _normalize_match_token(candidate)
    exact = _MATCH_TOOLING_EXACT_ALIASES.get(norm)
    if exact:
        return [exact]

    labels: list[str] = []
    for label, pattern in _MATCH_TOOLING_PATTERNS:
        if pattern.search(candidate):
            labels.append(label)
    return _unique_keep_order(labels)


def _secondary_tokens_for_text(text: str) -> list[str]:
    candidate = str(text or "").strip()
    if not candidate or _looks_like_urlish(candidate):
        return []
    norm = _normalize_match_token(candidate)
    if not norm or norm.isdigit() or len(norm) < 2 or norm in _MATCH_SECONDARY_STOPWORDS:
        return []
    return [norm]


def _coerce_match_entity(entity_or_stack: Any) -> dict[str, Any]:
    if isinstance(entity_or_stack, dict):
        return entity_or_stack
    return {"stack": entity_or_stack}


def _build_stack_profile(entity_or_stack: Any, *, entity_kind: str = "GENERIC") -> dict[str, list[str]]:
    entity = _coerce_match_entity(entity_or_stack)
    role_text = str(entity.get("role") or "").strip()
    stack_texts = _iter_match_texts(entity.get("stack"))
    role_texts = _iter_match_texts(role_text)

    primary_from_stack: list[str] = []
    secondary: list[str] = []
    tooling: list[str] = []

    for text in stack_texts:
        primary = _primary_labels_for_text(text)
        if primary:
            primary_from_stack.extend(primary)
            continue
        tool = _tooling_labels_for_text(text)
        if tool:
            tooling.extend(tool)
            continue
        secondary.extend(_secondary_tokens_for_text(text))

    primary_from_role: list[str] = []
    for text in role_texts:
        primary_from_role.extend(_primary_labels_for_text(text))

    if entity_kind == "VACANCY":
        primary = _unique_keep_order(primary_from_stack)[:1]
        if not primary:
            primary = _unique_keep_order(primary_from_role)[:1]
    else:
        primary = _unique_keep_order(primary_from_stack + primary_from_role)

    return {
        "primary": primary,
        "secondary": _unique_keep_order(secondary),
        "tooling": _unique_keep_order(tooling),
    }


def _stack_match_details(
    required_entity: Any,
    candidate_entity: Any,
    *,
    required_kind: str = "GENERIC",
    candidate_kind: str = "GENERIC",
) -> dict[str, Any]:
    required_profile = _build_stack_profile(required_entity, entity_kind=required_kind)
    candidate_profile = _build_stack_profile(candidate_entity, entity_kind=candidate_kind)
    required_primary = set(required_profile["primary"])
    candidate_primary = set(candidate_profile["primary"])
    overlap = required_primary & candidate_primary
    return {
        "passes": bool(required_primary and candidate_primary and overlap),
        "required_profile": required_profile,
        "candidate_profile": candidate_profile,
        "overlap": sorted(overlap),
    }


def _stack_set(value: Any) -> set[str]:
    if not value:
        return set()
    if isinstance(value, list):
        return {_normalize_stack_token(str(v)) for v in value if _normalize_stack_token(str(v))}
    return {_normalize_stack_token(str(value))}


def _stack_gate_passes(required_stack: Any, candidate_stack: Any) -> bool:
    return bool(_stack_match_details(required_stack, candidate_stack)["passes"])


def _normalize_grade(value: Any) -> Optional[str]:
    candidate = str(value or "").strip().lower()
    if not candidate:
        return None
    if "middle+" in candidate or "middle +" in candidate or "mid+" in candidate or "мидл+" in candidate:
        return "middle+"
    if "middle-" in candidate or "middle -" in candidate or "mid-" in candidate or "мидл-" in candidate:
        return "middle-"
    if "middle" in candidate or "mid" in candidate or "мидл" in candidate:
        return "middle"
    if "senior" in candidate or "sen" in candidate or "сеньор" in candidate:
        return "senior"
    if "junior" in candidate or "jun" in candidate or "джун" in candidate:
        return "junior"
    if "lead" in candidate or "лид" in candidate:
        return "lead"
    if "architect" in candidate or "архитект" in candidate:
        return "architect"
    if "head" in candidate:
        return "head"
    if "staff" in candidate:
        return "staff"
    if "principal" in candidate:
        return "principal"
    if "intern" in candidate or "стаж" in candidate:
        return "intern"
    return None


def _grade_match_score(left_grade: Any, right_grade: Any) -> float:
    left = _normalize_grade(left_grade)
    right = _normalize_grade(right_grade)
    if not left or not right:
        return 0.0
    diff = abs(_GRADE_LEVELS[left] - _GRADE_LEVELS[right])
    if diff == 0:
        return 1.0
    if diff == 1:
        return 0.6
    if diff == 2:
        return 0.25
    return 0.0


def _rate_anchor(entity_or_hit: Any) -> Optional[float]:
    entity = _coerce_match_entity(entity_or_hit)
    for key in ("rate_max", "rate_min"):
        value = entity.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        if isinstance(value, str):
            digits = re.sub(r"[^\d.]+", "", value)
            if digits:
                try:
                    parsed = float(digits)
                except ValueError:
                    continue
                if parsed > 0:
                    return parsed
    return None


def _budget_alignment_score(budget: Optional[float], ask: Optional[float]) -> float:
    if not budget or not ask:
        return 0.0
    if ask <= budget:
        return 1.0
    overflow = (ask - budget) / max(budget, 1.0)
    if overflow <= 0.10:
        return 0.6
    if overflow <= 0.25:
        return 0.25
    return 0.0


def _ascending_rate_tiebreak(value: Any) -> float:
    rate = _rate_anchor(value)
    if not rate:
        return float("-inf")
    return -rate


def _location_tokens(value: Any) -> set[str]:
    text = str(value or "").strip().lower()
    if not text:
        return set()
    tokens: set[str] = set()
    for part in re.split(r"[\s,;/|]+", text):
        norm = _normalize_match_token(part)
        if norm:
            tokens.add(norm)
    return tokens


def _location_match_score(left_value: Any, right_value: Any) -> float:
    left_tokens = _location_tokens(left_value)
    right_tokens = _location_tokens(right_value)
    if not left_tokens or not right_tokens:
        return 0.0
    if left_tokens & right_tokens:
        return 1.0
    if (_REMOTE_LOCATION_TOKENS & left_tokens) and (_REMOTE_LOCATION_TOKENS & right_tokens):
        return 1.0
    if (_RUSSIA_LOCATION_TOKENS & left_tokens) and (_RUSSIA_LOCATION_TOKENS & right_tokens):
        return 0.5
    return 0.0


def _secondary_overlap_score(required_profile: dict[str, list[str]], candidate_profile: dict[str, list[str]]) -> float:
    required = set(required_profile.get("secondary") or [])
    candidate = set(candidate_profile.get("secondary") or [])
    if not required or not candidate:
        return 0.0
    overlap = required & candidate
    if not overlap:
        return 0.0
    return min(1.0, len(overlap) / max(1, min(len(required), 3)))


def _semantic_similarity(hit: dict[str, Any]) -> float:
    value = hit.get("semantic_sim", hit.get("sim"))
    try:
        sim = float(value or 0.0)
    except (TypeError, ValueError):
        sim = 0.0
    return max(0.0, min(1.0, sim))


def _final_business_score(
    *,
    secondary_score: float,
    grade_score: float,
    rate_score: float,
    location_score: float,
    semantic_score: float,
) -> float:
    score = 0.50
    score += 0.15 * max(0.0, min(1.0, secondary_score))
    score += 0.15 * max(0.0, min(1.0, grade_score))
    score += 0.10 * max(0.0, min(1.0, rate_score))
    score += 0.05 * max(0.0, min(1.0, location_score))
    score += 0.05 * max(0.0, min(1.0, semantic_score))
    return round(max(0.0, min(1.0, score)), 4)


def _rank_specialist_hit(vacancy: Any, hit: dict[str, Any]) -> Optional[dict[str, Any]]:
    details = _stack_match_details(vacancy, hit, required_kind="VACANCY", candidate_kind="BENCH")
    if not details["passes"]:
        return None

    semantic_score = _semantic_similarity(hit)
    secondary_score = _secondary_overlap_score(details["required_profile"], details["candidate_profile"])
    grade_score = _grade_match_score(_coerce_match_entity(vacancy).get("grade"), hit.get("grade"))
    rate_score = _budget_alignment_score(_rate_anchor(vacancy), _rate_anchor(hit))
    location_score = _location_match_score(_coerce_match_entity(vacancy).get("location"), hit.get("location"))
    final_score = _final_business_score(
        secondary_score=secondary_score,
        grade_score=grade_score,
        rate_score=rate_score,
        location_score=location_score,
        semantic_score=semantic_score,
    )

    ranked_hit = dict(hit)
    ranked_hit["semantic_sim"] = semantic_score
    ranked_hit["sim"] = final_score
    ranked_hit["_match_sort_key"] = (
        final_score,
        _ascending_rate_tiebreak(hit),
        grade_score,
        rate_score,
        location_score,
        secondary_score,
        semantic_score,
        1 if bool(hit.get("is_internal")) else 0,
    )
    return ranked_hit


def _rank_specialist_hits(vacancy: Any, hits: list[dict]) -> tuple[list[dict], bool]:
    eligible = [ranked for ranked in (_rank_specialist_hit(vacancy, h) for h in hits) if ranked]
    out = sorted(
        eligible,
        key=lambda x: tuple(x.get("_match_sort_key") or (0, 0, 0, 0, 0, 0, 0)),
        reverse=True,
    )
    for hit in out:
        hit.pop("_match_sort_key", None)
    return out, any(bool(h.get("is_internal")) for h in out)


def _extract_own_bench_hits(hits: list[dict]) -> list[dict]:
    return [h for h in hits if bool(h.get("is_own_bench_source"))]


def format_own_bench_block(hits: list[dict], *, empty_text: str = OWN_BENCH_SECTION_EMPTY_TEXT) -> str:
    own_hits = (hits or [])[:10]
    return views.render_own_bench_block(own_hits, empty_text=empty_text)


def _parse_setting_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _format_sync_stamp(value: str | None) -> str | None:
    dt = _parse_setting_datetime(value)
    if dt is None:
        return None
    try:
        return dt.astimezone(MSK).strftime("%d.%m %H:%M МСК")
    except Exception:
        return dt.strftime("%d.%m %H:%M")


def _trim_sync_error(value: str | None, *, limit: int = 160) -> str | None:
    text_value = re.sub(r"\s+", " ", str(value or "").strip())
    if not text_value:
        return None
    if len(text_value) <= limit:
        return text_value
    return text_value[: limit - 1].rstrip() + "…"


def _load_own_bench_sync_state(engine) -> dict[str, Any]:
    return {
        "last_success_at": get_setting(engine, OWN_BENCH_SYNC_LAST_SUCCESS_AT_KEY),
        "last_error": get_setting(engine, OWN_BENCH_SYNC_LAST_ERROR_KEY),
        "last_stats": get_json_setting(engine, OWN_BENCH_SYNC_LAST_STATS_KEY),
    }


def _compose_own_bench_empty_text(engine) -> str:
    state = _load_own_bench_sync_state(engine)
    last_error = _trim_sync_error(state.get("last_error"))
    last_success = _format_sync_stamp(state.get("last_success_at"))
    if last_error and last_success:
        return (
            "Наш бенч сейчас не синхронизирован. "
            f"Последняя успешная версия: {last_success}. "
            f"Последняя ошибка: {last_error}"
        )
    if last_error:
        return f"Наш бенч ещё не синхронизирован. Последняя ошибка: {last_error}"
    return OWN_BENCH_SECTION_EMPTY_TEXT


def _render_own_bench_sync_status_html(engine) -> str | None:
    state = _load_own_bench_sync_state(engine)
    stats = state.get("last_stats") or {}
    lines: list[str] = []
    last_success = _format_sync_stamp(state.get("last_success_at"))
    if last_success:
        lines.append(f"Последний успешный sync: {views.code(last_success)}")
    active_rows = stats.get("active_rows")
    if active_rows not in (None, ""):
        lines.append(f"Активных специалистов: {views.code(active_rows)}")
    last_error = _trim_sync_error(state.get("last_error"))
    if last_error:
        lines.append(f"Последняя ошибка: {views.code(last_error)}")
    return views.join_nonempty(lines)


def _rank_vacancy_hit(bench: Any, hit: dict[str, Any]) -> Optional[dict[str, Any]]:
    details = _stack_match_details(bench, hit, required_kind="BENCH", candidate_kind="VACANCY")
    if not details["passes"]:
        return None

    semantic_score = _semantic_similarity(hit)
    secondary_score = _secondary_overlap_score(details["required_profile"], details["candidate_profile"])
    grade_score = _grade_match_score(_coerce_match_entity(bench).get("grade"), hit.get("grade"))
    rate_score = _budget_alignment_score(_rate_anchor(hit), _rate_anchor(bench))
    location_score = _location_match_score(_coerce_match_entity(bench).get("location"), hit.get("location"))
    final_score = _final_business_score(
        secondary_score=secondary_score,
        grade_score=grade_score,
        rate_score=rate_score,
        location_score=location_score,
        semantic_score=semantic_score,
    )

    ranked_hit = dict(hit)
    ranked_hit["semantic_sim"] = semantic_score
    ranked_hit["sim"] = final_score
    ranked_hit["_match_sort_key"] = (
        final_score,
        grade_score,
        rate_score,
        location_score,
        secondary_score,
        semantic_score,
    )
    return ranked_hit


def _rank_vacancy_hits(bench: Any, hits: list[dict]) -> list[dict]:
    eligible = [ranked for ranked in (_rank_vacancy_hit(bench, h) for h in hits) if ranked]
    out = sorted(
        eligible,
        key=lambda x: tuple(x.get("_match_sort_key") or (0, 0, 0, 0, 0, 0)),
        reverse=True,
    )
    for hit in out:
        hit.pop("_match_sort_key", None)
    return out


def format_top10(hits: list[dict]) -> str:
    return views.render_hits_block(hits, start_rank=1)


def format_hits_page(hits: list[dict], *, start_rank: int = 1) -> str:
    return views.render_hits_block(hits, start_rank=start_rank)


def _build_page_kb(token: str, page: int, total_pages: int) -> Optional[InlineKeyboardMarkup]:
    buttons: list[InlineKeyboardButton] = []
    if page > 1:
        buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"top:{token}:{page - 1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"top:{token}:{page + 1}"))
    if not buttons:
        return None
    return InlineKeyboardMarkup([buttons])


def _render_manual_top_page(state: dict[str, Any], page: int) -> tuple[str, Optional[InlineKeyboardMarkup]]:
    hits: list[dict] = state.get("hits") or []
    page_size = max(1, int(state.get("page_size") or 10))
    total_hits = len(hits)
    total_pages = max(1, (total_hits + page_size - 1) // page_size)
    page = max(1, min(int(page), total_pages))

    start = (page - 1) * page_size
    chunk = hits[start : start + page_size]
    title_value = str(state["title"]) if "title" in state and state.get("title") is not None else "TOP"
    header_value = str(state["header"]) if "header" in state and state.get("header") is not None else "Сущность"
    text_out = views.render_top_page(
        title=title_value,
        entity_label=header_value,
        summary=(str(state.get("summary")) if state.get("summary") else None),
        hits=chunk,
        source_display=(str(state.get("source_display")) if state.get("source_display") else None),
        source_url=(
            str(state.get("source_link_url"))
            if state.get("source_link_url")
            else (str(state.get("source_url")) if state.get("source_url") else None)
        ),
        page=page,
        total_pages=total_pages,
        total_hits=total_hits,
        own_bench_block=(str(state.get("intro_text")) if state.get("intro_text") else None),
        warning_text=(str(state.get("warning_text")) if state.get("warning_text") else None),
        results_label=(str(state.get("results_label")) if state.get("results_label") else None),
        start_rank=start + 1,
        entity_fields=(state.get("entity_fields") if isinstance(state.get("entity_fields"), dict) else None),
        show_hits_block=bool(state.get("show_hits_block", True)),
    )

    token = str(state.get("token") or "")
    return text_out, _build_page_kb(token, page, total_pages)


def _save_manual_top_state(context: ContextTypes.DEFAULT_TYPE, state: dict[str, Any]) -> str:
    store: dict[str, dict[str, Any]] = context.user_data.setdefault("manual_top_pages", {})
    token = str(state["token"])
    store[token] = state
    if len(store) > 5:
        oldest = sorted(store.items(), key=lambda kv: float(kv[1].get("ts", 0.0)))
        for k, _ in oldest[: len(store) - 5]:
            store.pop(k, None)
    return token


async def send_manual_top_paginated(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    title: str,
    header: str,
    summary: str,
    hits: list[dict],
    source_display: Optional[str],
    source_link_url: Optional[str],
    intro_text: Optional[str] = None,
    warning_text: Optional[str] = None,
    results_label: Optional[str] = None,
    entity_fields: Optional[dict[str, Any]] = None,
    show_hits_block: bool = True,
) -> None:
    msg = update.effective_message
    if not msg:
        return

    max_items = max(10, int(context.application.bot_data.get("manual_top_k") or 100))
    page_size = max(1, int(context.application.bot_data.get("manual_page_size") or 10))
    capped_hits = (hits or [])[:max_items]

    token_src = f"{datetime.now(timezone.utc).isoformat()}|{title}|{header}|{len(capped_hits)}"
    token = hashlib.sha1(token_src.encode("utf-8")).hexdigest()[:12]
    state = {
        "token": token,
        "title": title,
        "header": header,
        "summary": summary,
        "source_display": source_display,
        "source_link_url": source_link_url,
        "intro_text": intro_text,
        "warning_text": warning_text,
        "results_label": results_label,
        "entity_fields": entity_fields or {},
        "show_hits_block": bool(show_hits_block),
        "hits": capped_hits,
        "page_size": page_size,
        "ts": datetime.now(timezone.utc).timestamp(),
    }
    _save_manual_top_state(context, state)

    text_out, kb = _render_manual_top_page(state, 1)
    await safe_reply_html(msg, text_out, disable_preview=True, reply_markup=kb)


async def on_top_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    m = re.match(r"^top:([0-9a-f]{12}):(\d+)$", q.data or "")
    if not m:
        return
    token = m.group(1)
    page = int(m.group(2))

    store: dict[str, dict[str, Any]] = context.user_data.get("manual_top_pages", {})
    state = store.get(token)
    if not state:
        await q.answer("Сессия выдачи устарела. Запроси TOP заново.", show_alert=True)
        return

    text_out, kb = _render_manual_top_page(state, page)
    try:
        await safe_edit_html(q, text_out, disable_preview=True, reply_markup=kb)
    except Exception:
        # Сообщение могло быть удалено/изменено, не считаем критичной ошибкой.
        await q.answer("Не удалось обновить страницу.", show_alert=True)


async def do_export(update: Update, context: ContextTypes.DEFAULT_TYPE, only_active: bool) -> None:
    engine = context.application.bot_data["engine"]
    label = "active" if only_active else "all"
    await safe_reply_html(
        update.effective_message,
        views.render_status_message("Готовлю Excel выгрузку", body_html=f"Режим: {views.code(label)}"),
    )
    data = exporting_use_cases.export_database(engine, only_active=only_active)
    await update.effective_message.reply_document(document=data, filename=f"hunting_export_{label}.xlsx")


async def diag_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed = context.application.bot_data["allowed_ids"]
    if not access_allowed(update, allowed):
        await safe_reply_html(update.effective_message, views.render_status_message("Нет доступа.", warning=True))
        return
    ollama: OllamaClient = context.application.bot_data["ollama"]
    d = await ollama.diag()
    await safe_reply_html(
        update.effective_message,
        views.join_nonempty(
            [
                views.b("Ollama diag"),
                f"host={views.code(d['host'])}",
                f"llm_model={views.code(d['llm_model'])}",
                f"embed_model={views.code(d['embed_model'])}",
                f"checks={views.code(d['checks'])}",
            ]
        ),
    )


async def do_delete_by_link(update: Update, context: ContextTypes.DEFAULT_TYPE, url_text: str) -> None:
    engine = context.application.bot_data["engine"]
    url = extract_delete_target_url(update, url_text)
    if not url:
        await safe_reply_html(
            update.effective_message,
            views.render_status_message(
                "Не удалось распознать ссылку",
                body_html=f"Пример: {views.code('https://t.me/c/123456/789')}",
                warning=True,
            ),
        )
        return

    rows = source_trace_use_cases.find_source_entities_by_message_url(engine, url)

    if not rows:
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("По этой ссылке ничего не найдено", body="В sources нет связанной записи.", warning=True),
        )
        return

    context.user_data["pending_delete"] = {"url": url, "items": [dict(r) for r in rows]}
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Подтвердить скрытие", callback_data="del_confirm")],
            [InlineKeyboardButton("❌ Отмена", callback_data="del_cancel")],
        ]
    )
    await safe_reply_html(
        update.effective_message,
        views.join_nonempty(
            [
                views.b("Найдены связанные сущности"),
                f"Количество: {views.code(len(rows))}",
                f"Ссылка: {views.link('открыть', url)}",
                "",
                "Скрыть все найденные сущности?",
            ]
        ),
        reply_markup=kb,
    )


async def on_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = context.user_data.get("pending_delete")
    if not data:
        await safe_edit_html(q, views.render_status_message("Нет активной операции удаления.", warning=True))
        return

    if q.data == "del_cancel":
        context.user_data.pop("pending_delete", None)
        await safe_edit_html(q, views.render_status_message("Операция отменена."))
        return

    engine = context.application.bot_data["engine"]
    items = data["items"]
    updated = source_trace_use_cases.soft_hide_entities(engine, items)

    context.user_data.pop("pending_delete", None)
    await safe_edit_html(
        q,
        views.render_status_message("Готово", body_html=f"Скрыто записей: {views.code(updated)}"),
    )


# ------------------------
# Notify helpers
# ------------------------
def _sender_chat_id(update: Update) -> Optional[int]:
    chat = update.effective_chat
    return int(chat.id) if chat else None


async def notify_managers_top10(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    title: str,
    header: str,
    summary: str,
    top10_text: str,
    source_display: Optional[str],
    source_link_url: Optional[str],
    intro_text: Optional[str] = None,
    warning_text: Optional[str] = None,
    entity_fields: Optional[dict[str, Any]] = None,
) -> None:
    """
    Условная рассылка:
    - только если сообщение пришло из ingest trigger chat(s)
    - рассылаем всем MANAGER_CHAT_IDS кроме отправителя
    """
    trigger_chat_ids: set[int] = context.application.bot_data.get("notify_trigger_chat_ids", set())
    legacy_trigger_chat_id: int = int(context.application.bot_data.get("notify_trigger_chat_id") or 0)
    if not trigger_chat_ids and legacy_trigger_chat_id:
        trigger_chat_ids = {legacy_trigger_chat_id}

    if not trigger_chat_ids:
        log.info("notify_managers_top10: skip, no trigger chat ids configured")
        return

    sender_cid = _sender_chat_id(update)
    if sender_cid is None:
        log.info("notify_managers_top10: skip, sender chat id is None")
        return
    if sender_cid not in trigger_chat_ids:
        log.info(
            "notify_managers_top10: skip, sender chat %s not in triggers %s",
            sender_cid,
            sorted(trigger_chat_ids),
        )
        return

    allowed_ids: set[int] = context.application.bot_data["allowed_ids"]
    # исключаем отправителя (он и так получил ТОП-10)
    targets = [cid for cid in allowed_ids if cid != sender_cid]
    if not targets:
        log.info("notify_managers_top10: skip, no targets after filtering")
        return

    # Сообщение (plain text)
    payload = views.join_nonempty(
        [
            views.b(title),
            views.render_entity_summary_block(header, summary, fields=entity_fields) if summary else (views.b(header) if header else None),
            views.h(warning_text) if warning_text else None,
            intro_text,
            views.render_source(source_display, source_link_url),
            "",
            top10_text or "Нет мэтчей.",
        ]
    )

    bot = context.bot
    sent = 0
    for cid in targets:
        try:
            await safe_send_html(bot, chat_id=int(cid), html_out=payload, disable_preview=True)
            sent += 1
        except Exception as e:
            log.warning("notify_managers_top10: failed to send to %s: %s", cid, type(e).__name__)
    log.info(
        "notify_managers_top10: sender_chat=%s targets=%s sent=%s",
        sender_cid,
        len(targets),
        sent,
    )


# ------------------------
# Main processing
# ------------------------
def _heuristic_is_bench_blast(text_in: str) -> bool:
    t = (text_in or "").lower()
    if "доступны" in t and "специалист" in t:
        return True
    if "подробные анкеты" in t or "в л/с" in t or "в лс" in t:
        return True
    return False


async def process_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text_in: str,
    forced_type: Optional[str],
    *,
    reply_enabled: bool = True,
    archive_ingest_mode: bool = False,
    broadcast_to_managers: bool = False,
    enable_matching: bool = True,
) -> None:
    engine = context.application.bot_data["engine"]
    ollama: OllamaClient = context.application.bot_data["ollama"]
    top_k = context.application.bot_data["top_k"]
    manual_top_k = context.application.bot_data.get("manual_top_k", 100)
    partner_names: list[str] = context.application.bot_data.get("partner_company_names", [])
    msg = update.effective_message
    external_enabled = bool(context.application.bot_data.get("external_link_ingest_enabled"))
    source_fetcher: MCPSourceFetcherClient | None = context.application.bot_data.get("source_fetcher")
    own_bench_source_url = str(context.application.bot_data.get("own_specialists_source_url") or OWN_BENCH_URL).strip()

    if not msg:
        return
    if archive_ingest_mode and ARCHIVE_REFERENCE_ONLY_MARKER in text_in:
        log.info("Skipping reference-only archive post from ingestion.")
        return

    async def _reply(text: str, *, disable_preview: bool = True, reply_markup=None) -> None:
        if not reply_enabled:
            return
        await safe_reply_html(msg, text, disable_preview=disable_preview, reply_markup=reply_markup)

    manual_paged_mode = bool(reply_enabled and not archive_ingest_mode)
    search_limit = int(manual_top_k if manual_paged_mode else top_k)
    archive_forced_type = extract_archive_declared_type(text_in) if archive_ingest_mode else None
    effective_forced_type = forced_type or archive_forced_type
    manual_input = bool(reply_enabled and not archive_ingest_mode)
    manager_name = _manager_display_name(update, manual_input=manual_input)
    forward_ctx = _extract_forward_source_context(msg)
    manual_forward_chat = bool(manual_input and forward_ctx.get("kind") == "chat")

    telegram_source_url = (
        extract_current_chat_message_url(update)
        if archive_ingest_mode
        else extract_source_message_url(update, text_in)
    )
    effective_text_in = extract_archive_payload_text(text_in) if archive_ingest_mode else text_in
    attachment_units: list[IngestionUnit] = []
    attachment_summaries: list[dict[str, Any]] = []
    if not archive_ingest_mode and _has_supported_document(update):
        attachment_units, attachment_summaries = await _build_attachment_ingestion_units(
            update,
            context,
            source_url=extract_current_chat_message_url(update),
        )

    ext = None
    if not manual_forward_chat and not attachment_units:
        ext = await build_external_ingestion_units(
            effective_text_in,
            enabled=external_enabled,
            client=source_fetcher,
        )
    manual_plain_text_reference = _should_create_manual_reference_post(
        manual_input=manual_input,
        archive_ingest_mode=archive_ingest_mode,
        forward_kind=str(forward_ctx.get("kind") or ""),
        original_text=text_in,
        external_urls=(ext.urls if ext else None),
    )
    requires_reference_archive = bool(manual_forward_chat or manual_plain_text_reference)

    units: list[IngestionUnit]
    if manual_forward_chat:
        units = [IngestionUnit(text=effective_text_in, source_type="forward_chat_archive")]
        await _reply(
            views.render_status_message(
                "Обнаружено пересланное сообщение из чата",
                body="Создам ссылку через архив-пост.",
            ),
            reply_markup=main_keyboard(),
        )
    elif attachment_units:
        units = attachment_units
        if attachment_summaries:
            await _reply(
                views.render_import_summaries(attachment_summaries),
                reply_markup=main_keyboard(),
            )
        await _reply(
            views.render_status_message(
                "Обрабатываю внешний источник",
                body_html=f"Найдено блоков: {views.code(len(units))}. Готовлю разбор.",
            ),
            reply_markup=main_keyboard(),
        )
    elif ext and ext.units:
        units = ext.units
        status_lines = [f"• {views.h(line)}" for line in ext.statuses]
        if ext.errors:
            status_lines.append("")
            status_lines.append(views.b("Ошибки"))
            status_lines.extend(f"• {views.h(e)}" for e in ext.errors[:8])
        await _reply(
            views.render_status_message("Обработаны внешние ссылки", body_html="\n".join(status_lines)),
            reply_markup=main_keyboard(),
        )
        if ext.summaries:
            await _reply(
                views.render_import_summaries(ext.summaries),
                reply_markup=main_keyboard(),
            )
        await _reply(
            views.render_status_message(
                "Обрабатываю внешний источник",
                body_html=f"Найдено блоков: {views.code(len(units))}. Готовлю разбор.",
            ),
            reply_markup=main_keyboard(),
        )
    else:
        split_lines = split_line_wise_bench_items(effective_text_in)
        if split_lines:
            units = [
                IngestionUnit(
                    text=ln,
                    source_type="manual_line_split",
                    source_meta={"parsing_mode": "line_wise_bench_list", "line_index": line_idx},
                )
                for line_idx, ln in split_lines
            ]
        else:
            units = [IngestionUnit(text=effective_text_in, source_type="manual")]
        if ext and ext.urls and ext.errors:
            await _reply(
                views.render_status_message(
                    "Ссылки распознаны, но источник не удалось прочитать",
                    body_html="\n".join(f"• {views.h(e)}" for e in ext.errors[:8]),
                    warning=True,
                ),
                reply_markup=main_keyboard(),
            )
        else:
            if split_lines:
                note = "Для каждого релевантного блока создам ссылку через архив-пост." if manual_plain_text_reference else None
                await _reply(
                    views.render_status_message(
                        "Обнаружен список бенчей",
                        body_html=f"Строк: {views.code(len(split_lines))}. Начинаю разбор по блокам.",
                        note=note,
                    ),
                    reply_markup=main_keyboard(),
                )
            else:
                await _reply(
                    views.render_status_message(
                        "Обрабатываю сообщение",
                        body="Извлекаю сущности и готовлю TOP-выдачу.",
                        note=("Создам ссылку через архив-пост." if manual_plain_text_reference else None),
                    ),
                    reply_markup=main_keyboard(),
                )

    saved_stats = {"vacancy": 0, "specialist": 0}

    async def _process_unit(unit: IngestionUnit, unit_idx: int, total_units: int) -> None:
        raw_unit_text = unit.text
        normalized_unit_text = normalize_short_bench_line(raw_unit_text)
        pre_rule = pre_classify_bench_line(normalized_unit_text)
        partner_hit = detect_partner_company_mention(normalized_unit_text, partner_names)
        unit_entity_hint = str(unit.source_meta.get("entity_hint") or "").strip().upper() or None
        structured_fields = unit.source_meta.get("structured_fields") if isinstance(unit.source_meta.get("structured_fields"), dict) else None

        local_forced = effective_forced_type or unit_entity_hint
        if local_forced == "VACANCY" and _heuristic_is_bench_blast(normalized_unit_text):
            local_forced = "BENCH"

        pre = preprocess_for_llm(normalized_unit_text, kind=local_forced)
        clean_text = pre.text

        classifier_source = "llm"
        llm_result = None
        decision = decide_hybrid_classification(pre_rule, forced_type=local_forced)
        if decision.needs_llm:
            raw = await ollama.chat(CLASSIFICATION_SYSTEM_PROMPT_V2, clean_text, num_predict=32)
            llm_result = (raw or "").strip().upper()
            decision = decide_hybrid_classification(pre_rule, llm_label=llm_result)
            # Partner-company mention is a conservative push towards vacancy
            # only for ambiguous LLM OTHER results.
            if partner_hit and decision.kind == "OTHER" and not pre_rule.is_confident:
                decision.kind = "VACANCY"
                decision.source = "partner_bias"
        mtype = decision.kind
        classifier_source = decision.source

        log.info(
            "classification unit=%s/%s source=%s partner_hit=%s pre_conf=%.2f pre_reason=%s llm=%s final=%s raw=%r normalized=%r",
            unit_idx,
            total_units,
            classifier_source,
            partner_hit,
            pre_rule.confidence,
            pre_rule.reason,
            llm_result,
            mtype,
            raw_unit_text[:240],
            normalized_unit_text[:240],
        )

        if mtype == "OTHER":
            await _reply(
                views.render_status_message(
                    "Сообщение пропущено",
                    body="Классификатор не увидел релевантную вакансию или бенч.",
                    warning=True,
                ),
                reply_markup=main_keyboard(),
            )
            return

        source_url_for_entity = unit.external_url or telegram_source_url
        source_kind_for_entity = "file" if unit.external_url else ("archive_post" if archive_ingest_mode else ("telegram_message" if source_url_for_entity else "manager_text"))

        if mtype in ("VACANCY", "VACANCY_LIST"):
            if structured_fields and unit_entity_hint == "VACANCY":
                items = [_build_structured_vacancy_item(structured_fields, raw_unit_text)]
                classifier_source = "structured_table"
            else:
                raw = await ollama.chat(VACANCY_EXTRACTION_PROMPT_V2, clean_text, num_predict=1600)
                data = safe_json_loads(raw)
                items = (data or {}).get("items", [])
            if not items:
                if local_forced == "VACANCY":
                    items = (
                        [_build_structured_vacancy_item(structured_fields, raw_unit_text)]
                        if structured_fields
                        else [build_fallback_vacancy_item(pre)]
                    )
                else:
                    await _reply(
                        views.render_status_message("Не удалось извлечь вакансии", warning=True),
                        reply_markup=main_keyboard(),
                    )
                    return

            if requires_reference_archive:
                reference_original_text = effective_text_in if manual_forward_chat else raw_unit_text
                reference_original_date = forward_ctx.get("message_date") if manual_forward_chat else getattr(msg, "date", None)
                reference_source_name = str(forward_ctx.get("source_name") or "-") if manual_forward_chat else manager_name
                archive_post_url = await _create_reference_archive_post(
                    context,
                    original_text=reference_original_text,
                    original_date=reference_original_date,
                    source_name=reference_source_name,
                    manager_name=manager_name,
                    items=[_format_entity_summary(v) for v in items],
                )
                if archive_post_url:
                    source_url_for_entity = archive_post_url
                    source_kind_for_entity = "archive_post"
                else:
                    source_url_for_entity = None
                    source_kind_for_entity = "manager_text"
                    await _reply(
                        views.render_status_message(
                            "Не удалось создать архив-пост",
                            body="Сохраню источник без ссылки на архив.",
                            warning=True,
                        ),
                        reply_markup=main_keyboard(),
                    )

            for idx, v in enumerate(items, start=1):
                is_closed = bool(v.get("is_closed"))
                status = "closed" if is_closed else "active"
                search_text = build_search_text(v)
                q_emb = await ollama.embed(search_text)

                vacancy_id = upsert_vacancy(engine, v, raw_unit_text, q_emb, status)
                company_from_item = (v.get("company") or "").strip()
                if company_from_item:
                    upsert_partner_company_mentions(
                        engine,
                        {company_from_item: 1},
                        source_url=source_url_for_entity or unit.external_url,
                    )
                insert_source(
                    engine,
                    "vacancy",
                    vacancy_id,
                    update,
                    source_url_for_entity,
                    raw_unit_text,
                    idx=(unit_idx * 1000 + idx),
                    source_type=unit.source_type,
                    external_url=unit.external_url,
                    external_kind=unit.external_kind,
                    external_locator=unit.external_locator,
                    source_meta={
                        **_build_source_meta(
                            base_meta=unit.source_meta,
                            manager_name=manager_name,
                            canonical_url=source_url_for_entity,
                            external_url=unit.external_url,
                            external_locator=unit.external_locator,
                            source_kind=source_kind_for_entity,
                            entity_index=(idx if source_kind_for_entity == "archive_post" else None),
                            source_sender_name=(reference_source_name if requires_reference_archive else None),
                        ),
                        "classifier_source": classifier_source,
                        "pre_classifier_confidence": pre_rule.confidence,
                        "normalized_text": normalized_unit_text[:2000],
                    },
                )
                saved_stats["vacancy"] += 1

                source_display_for_entity = _compose_source_display(
                    manager_name=manager_name,
                    canonical_url=source_url_for_entity,
                    external_url=unit.external_url,
                    external_locator=unit.external_locator,
                    source_kind=source_kind_for_entity,
                    entity_index=(idx if source_kind_for_entity == "archive_post" else None),
                    sheet_name=str(unit.source_meta.get("sheet_name") or unit.source_meta.get("table_name") or "").strip() or None,
                    table_index=(int(unit.source_meta["table_index"]) if unit.source_meta.get("table_index") not in (None, "") else None),
                )

                header = f"Вакансия {idx}/{len(items)}" if len(items) > 1 else "Вакансия"
                summary = f"{v.get('role') or 'Unknown'} | {v.get('grade') or '—'} | {', '.join((v.get('stack') or [])[:8]) or '—'}"

                if is_closed:
                    await _reply(
                        views.render_status_message(
                            "Вакансия закрыта",
                            body_html=views.render_entity_summary_block(header, summary, fields=v),
                            note="Сохранил как closed.",
                            warning=True,
                        )
                    )
                    continue

                if enable_matching:
                    hits = search_specialists(engine, q_emb, search_text, search_limit, own_bench_url=own_bench_source_url)
                    ranked_hits, has_own = _rank_specialist_hits(v, hits)
                    upsert_matches(engine, vacancy_id, ranked_hits)
                    own_bench_hits = _extract_own_bench_hits(ranked_hits)
                    own_bench_block = format_own_bench_block(
                        own_bench_hits,
                        empty_text=_compose_own_bench_empty_text(engine),
                    )
                    top_hits = ranked_hits[:10]
                    top10_text = format_top10(top_hits) if top_hits else SPECIALISTS_EMPTY_TEXT
                    warning_text = None
                    if top_hits and not has_own:
                        warning_text = OWN_SPECIALISTS_EMPTY_TEXT
                else:
                    own_bench_block = None
                    top_hits = []
                    top10_text = SPECIALISTS_EMPTY_TEXT
                    warning_text = None
                if manual_paged_mode:
                    await send_manual_top_paginated(
                        update,
                        context,
                        title="" if not enable_matching else "ТОП-10 кандидатов для вакансии",
                        header=header,
                        summary=summary,
                        hits=top_hits,
                        source_display=source_display_for_entity,
                        source_link_url=source_url_for_entity,
                        intro_text=own_bench_block,
                        warning_text=warning_text,
                        results_label=("БЕНЧИ" if enable_matching else None),
                        entity_fields=v,
                        show_hits_block=enable_matching,
                    )
                else:
                    await _reply(
                        views.render_top_page(
                            title="" if not enable_matching else "TOP-10 кандидатов для вакансии",
                            entity_label=header,
                            summary=summary,
                            hits=top_hits,
                            source_display=source_display_for_entity,
                            source_url=source_url_for_entity,
                            own_bench_block=own_bench_block,
                            warning_text=warning_text,
                            results_label=("БЕНЧИ" if enable_matching else None),
                            entity_fields=v,
                            show_hits_block=enable_matching,
                        ),
                        disable_preview=True,
                    )

                if enable_matching and broadcast_to_managers:
                    await notify_managers_top10(
                        update,
                        context,
                        title="ТОП-10 для новой вакансии",
                        header=header,
                        summary=summary,
                        top10_text=top10_text,
                        source_display=source_display_for_entity,
                        source_link_url=source_url_for_entity,
                        intro_text=own_bench_block,
                        warning_text=warning_text,
                        entity_fields=v,
                    )
            return

        if mtype in ("BENCH", "BENCH_LIST"):
            if structured_fields and unit_entity_hint == "BENCH":
                items = [_build_structured_specialist_item(structured_fields, raw_unit_text)]
                classifier_source = "structured_table"
            else:
                raw = await ollama.chat(SPECIALIST_EXTRACTION_PROMPT_V2, clean_text, num_predict=1600)
                data = safe_json_loads(raw)
                items = (data or {}).get("items", [])
            if not items:
                if local_forced == "BENCH":
                    items = (
                        [_build_structured_specialist_item(structured_fields, raw_unit_text)]
                        if structured_fields
                        else [build_fallback_specialist_item(pre)]
                    )
                else:
                    await _reply(
                        views.render_status_message("Не удалось извлечь специалистов", warning=True),
                        reply_markup=main_keyboard(),
                    )
                    return

            if requires_reference_archive:
                reference_original_text = effective_text_in if manual_forward_chat else raw_unit_text
                reference_original_date = forward_ctx.get("message_date") if manual_forward_chat else getattr(msg, "date", None)
                reference_source_name = str(forward_ctx.get("source_name") or "-") if manual_forward_chat else manager_name
                archive_post_url = await _create_reference_archive_post(
                    context,
                    original_text=reference_original_text,
                    original_date=reference_original_date,
                    source_name=reference_source_name,
                    manager_name=manager_name,
                    items=[_format_entity_summary(s) for s in items],
                )
                if archive_post_url:
                    source_url_for_entity = archive_post_url
                    source_kind_for_entity = "archive_post"
                else:
                    source_url_for_entity = None
                    source_kind_for_entity = "manager_text"
                    await _reply(
                        views.render_status_message(
                            "Не удалось создать архив-пост",
                            body="Сохраню источник без ссылки на архив.",
                            warning=True,
                        ),
                        reply_markup=main_keyboard(),
                    )

            for idx, s in enumerate(items, start=1):
                is_available = resolve_specialist_is_available(s, raw_unit_text)
                s["is_available"] = is_available
                status = "active" if is_available else "hired"
                search_text = build_search_text(s)
                q_emb = await ollama.embed(search_text)

                specialist_id = upsert_specialist(engine, s, raw_unit_text, q_emb, status)
                insert_source(
                    engine,
                    "specialist",
                    specialist_id,
                    update,
                    source_url_for_entity,
                    raw_unit_text,
                    idx=(unit_idx * 1000 + idx),
                    source_type=unit.source_type,
                    external_url=unit.external_url,
                    external_kind=unit.external_kind,
                    external_locator=unit.external_locator,
                    source_meta={
                        **_build_source_meta(
                            base_meta=unit.source_meta,
                            manager_name=manager_name,
                            canonical_url=source_url_for_entity,
                            external_url=unit.external_url,
                            external_locator=unit.external_locator,
                            source_kind=source_kind_for_entity,
                            entity_index=(idx if source_kind_for_entity == "archive_post" else None),
                            source_sender_name=(reference_source_name if requires_reference_archive else None),
                        ),
                        "classifier_source": classifier_source,
                        "pre_classifier_confidence": pre_rule.confidence,
                        "normalized_text": normalized_unit_text[:2000],
                    },
                )
                saved_stats["specialist"] += 1

                source_display_for_entity = _compose_source_display(
                    manager_name=manager_name,
                    canonical_url=source_url_for_entity,
                    external_url=unit.external_url,
                    external_locator=unit.external_locator,
                    source_kind=source_kind_for_entity,
                    entity_index=(idx if source_kind_for_entity == "archive_post" else None),
                    sheet_name=str(unit.source_meta.get("sheet_name") or unit.source_meta.get("table_name") or "").strip() or None,
                    table_index=(int(unit.source_meta["table_index"]) if unit.source_meta.get("table_index") not in (None, "") else None),
                )

                header = f"Кандидат {idx}/{len(items)}" if len(items) > 1 else "Кандидат"
                summary = f"{s.get('role') or 'Unknown'} | {s.get('grade') or '—'} | {', '.join((s.get('stack') or [])[:8]) or '—'}"

                if not is_available:
                    await _reply(
                        views.render_status_message(
                            "Специалист сейчас недоступен",
                            body_html=views.render_entity_summary_block(header, summary, fields=s),
                            note="Сохранил как hired.",
                            warning=True,
                        )
                    )
                    continue

                if enable_matching:
                    hits = search_vacancies(engine, q_emb, search_text, search_limit)
                    ranked_hits = _rank_vacancy_hits(s, hits)
                    upsert_matches_reverse(engine, specialist_id, ranked_hits)
                    top_hits = ranked_hits[:10]
                    top10_text = format_top10(top_hits) if top_hits else VACANCIES_EMPTY_TEXT
                else:
                    top_hits = []
                    top10_text = VACANCIES_EMPTY_TEXT
                if manual_paged_mode:
                    await send_manual_top_paginated(
                        update,
                        context,
                        title="" if not enable_matching else "ТОП-10 вакансий для кандидата",
                        header=header,
                        summary=summary,
                        hits=top_hits,
                        source_display=source_display_for_entity,
                        source_link_url=source_url_for_entity,
                        results_label=("ВАКАНСИИ" if enable_matching else None),
                        entity_fields=s,
                        show_hits_block=enable_matching,
                    )
                else:
                    await _reply(
                        views.render_top_page(
                            title="" if not enable_matching else "TOP-10 вакансий для кандидата",
                            entity_label=header,
                            summary=summary,
                            hits=top_hits,
                            source_display=source_display_for_entity,
                            source_url=source_url_for_entity,
                            results_label=("ВАКАНСИИ" if enable_matching else None),
                            entity_fields=s,
                            show_hits_block=enable_matching,
                        ),
                        disable_preview=True,
                    )
                if enable_matching and broadcast_to_managers:
                    await notify_managers_top10(
                        update,
                        context,
                        title="ТОП-10 для нового кандидата",
                        header=header,
                        summary=summary,
                        top10_text=top10_text,
                        source_display=source_display_for_entity,
                        source_link_url=source_url_for_entity,
                        entity_fields=s,
                    )
            return

        await _reply(
            views.render_status_message(
                "Тип сообщения не распознан",
                body="Попробуйте прислать текст вакансии или кандидата в более явном виде.",
                warning=True,
            ),
            reply_markup=main_keyboard(),
        )

    for i, unit in enumerate(units, start=1):
        await _process_unit(unit, i, len(units))

    if ext and ext.urls:
        await _reply(
            views.render_status_message(
                "Обработка завершена",
                body_html=(
                    f"Вакансий: {views.code(saved_stats['vacancy'])} · "
                    f"Кандидатов: {views.code(saved_stats['specialist'])} · "
                    f"Ошибок ссылок: {views.code(len(ext.errors))}"
                ),
            ),
            reply_markup=main_keyboard(),
        )


async def handle_buttons_and_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    allowed = context.application.bot_data["allowed_ids"]
    ingest_chat_ids: set[int] = context.application.bot_data.get("ingest_chat_ids", set())
    archive_chat_ids: set[int] = context.application.bot_data.get("archive_chat_ids", set())
    if not can_ingest(update, allowed, ingest_chat_ids):
        return

    chat_id = update.effective_chat.id if update.effective_chat else None
    is_ingest_chat = bool(chat_id and chat_id in ingest_chat_ids and chat_id not in allowed)
    log.info(
        "incoming update chat_id=%s user_id=%s is_ingest_chat=%s has_text=%s",
        chat_id,
        update.effective_user.id if update.effective_user else None,
        is_ingest_chat,
        bool(extract_text(update)),
    )

    text_in = extract_text(update)
    has_document = _has_supported_document(update)
    if not text_in and not has_document:
        if not is_ingest_chat:
            await safe_reply_html(
                update.effective_message,
                views.render_status_message("Нужно сообщение с текстом", body="Пришлите текст вакансии или кандидата."),
                reply_markup=main_keyboard(),
            )
        return

    # Ingest mode for archive channel: no command/button workflow, parse message directly.
    if is_ingest_chat:
        archive_ingest_mode = bool(chat_id and chat_id in archive_chat_ids)
        await process_message(
            update,
            context,
            text_in,
            forced_type=None,
            reply_enabled=False,
            archive_ingest_mode=archive_ingest_mode,
            broadcast_to_managers=True,
        )
        return

    if text_in == BTN_HELP:
        await help_cmd(update, context)
        return

    if text_in == BTN_OWN_BENCH:
        await show_own_bench_menu(update, context)
        return

    if text_in == BTN_OWN_BENCH_CHANGE:
        context.user_data["mode"] = MODE_WAIT_OWN_BENCH_URL
        await safe_reply_html(
            update.effective_message,
            views.render_status_message(
                "Изменение ссылки на наш бенч",
                body="Пришлите новую ссылку.",
                note="Нажмите «Назад», если передумали.",
            ),
            reply_markup=own_bench_keyboard(),
        )
        return

    if text_in == BTN_OWN_BENCH_REFRESH:
        context.user_data["mode"] = MODE_NONE
        await refresh_own_bench(update, context)
        return

    if text_in == BTN_BACK:
        context.user_data["mode"] = MODE_NONE
        await safe_reply_html(update.effective_message, views.render_status_message("Возвращаюсь в главное меню."), reply_markup=main_keyboard())
        return

    if text_in == BTN_VAC:
        context.user_data["mode"] = MODE_FORCE_VAC
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Режим вакансии", body="Пришлите текст вакансии или пересланное сообщение."),
            reply_markup=main_keyboard(),
        )
        return

    if text_in == BTN_BENCH:
        context.user_data["mode"] = MODE_FORCE_BENCH
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Режим кандидата/бенча", body="Пришлите текст кандидата/бенча или пересланное сообщение."),
            reply_markup=main_keyboard(),
        )
        return

    if text_in == BTN_LOAD_VAC:
        context.user_data["mode"] = MODE_LOAD_VAC
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Загрузка вакансий", body="Пришлите файл, ссылку или текст вакансий. Сохраню их в БД без мэтчинга."),
            reply_markup=main_keyboard(),
        )
        return

    if text_in == BTN_LOAD_BENCH:
        context.user_data["mode"] = MODE_LOAD_BENCH
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Загрузка бенча", body="Пришлите файл, ссылку или текст бенча. Сохраню их в БД без мэтчинга."),
            reply_markup=main_keyboard(),
        )
        return

    if text_in == BTN_EXPORT_ACTIVE:
        await do_export(update, context, only_active=True)
        return

    if text_in == BTN_EXPORT_ALL:
        await do_export(update, context, only_active=False)
        return

    if text_in == BTN_DELETE:
        context.user_data["mode"] = MODE_WAIT_DELETE_LINK
        await safe_reply_html(
            update.effective_message,
            views.render_status_message(
                "Скрытие по ссылке",
                body_html=f"Пришлите ссылку вида {views.code('https://t.me/.../123')} или {views.code('https://t.me/c/.../123')}",
            ),
            reply_markup=main_keyboard(),
        )
        return

    mode = context.user_data.get("mode", MODE_NONE)
    if mode == MODE_WAIT_DELETE_LINK:
        await do_delete_by_link(update, context, text_in)
        context.user_data["mode"] = MODE_NONE
        return
    if mode == MODE_WAIT_OWN_BENCH_URL:
        await replace_own_bench_link(update, context, text_in)
        return

    force = None
    enable_matching = True
    if mode == MODE_FORCE_VAC:
        force = "VACANCY"
        context.user_data["mode"] = MODE_NONE
    elif mode == MODE_FORCE_BENCH:
        force = "BENCH"
        context.user_data["mode"] = MODE_NONE
    elif mode == MODE_LOAD_VAC:
        force = "VACANCY"
        enable_matching = False
        context.user_data["mode"] = MODE_NONE
    elif mode == MODE_LOAD_BENCH:
        force = "BENCH"
        enable_matching = False
        context.user_data["mode"] = MODE_NONE

    await process_message(
        update,
        context,
        text_in,
        forced_type=force,
        reply_enabled=True,
        archive_ingest_mode=False,
        broadcast_to_managers=False,
        enable_matching=enable_matching,
    )


async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await do_export(update, context, only_active=True)


async def export_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await do_export(update, context, only_active=False)


async def delete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    parts = (extract_text(update) or "").split()
    if len(parts) < 2:
        context.user_data["mode"] = MODE_WAIT_DELETE_LINK
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Пришлите t.me ссылку", body="Её можно отправить следующим сообщением."),
            reply_markup=main_keyboard(),
        )
        return
    await do_delete_by_link(update, context, parts[1])


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Unhandled error: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await safe_reply_html(
                update.effective_message,
                views.render_status_message("Ошибка", body_html=f"Причина: {views.code(context.error)}", warning=True),
            )
    except Exception:
        pass


def load_active_source_channel_chat_ids(engine) -> set[int]:
    q = text(
        """
        SELECT telegram_id
        FROM channels
        WHERE is_active = TRUE
          AND COALESCE(source_kind, 'chat') = 'channel'
        """
    )
    with engine.begin() as c:
        return {int(x) for x in c.execute(q).scalars().all()}


def _fetch_digest_rows(engine, table: str, ts_column: str, window_start: datetime, window_end: datetime) -> list[dict]:
    return digest_use_cases.fetch_digest_rows(engine, table, ts_column, window_start, window_end)


def _format_digest_items(title: str, rows: list[dict], *, ts_field: str) -> str:
    del title, rows, ts_field
    return ""


def build_daily_digest_text(engine, *, now: datetime | None = None) -> str:
    now_msk = (now or datetime.now(MSK)).astimezone(MSK)
    window_end = now_msk
    window_start = window_end - timedelta(days=1)
    payload = {
        "window_start": window_start,
        "window_end": window_end,
        "new_vacancies": _fetch_digest_rows(engine, "vacancies", "created_at", window_start, window_end),
        "updated_vacancies": _fetch_digest_rows(engine, "vacancies", "updated_at", window_start, window_end),
        "new_specialists": _fetch_digest_rows(engine, "specialists", "created_at", window_start, window_end),
        "updated_specialists": _fetch_digest_rows(engine, "specialists", "updated_at", window_start, window_end),
    }
    return views.render_digest(
        window_start=payload["window_start"],
        window_end=payload["window_end"],
        new_vacancies=payload["new_vacancies"],
        updated_vacancies=payload["updated_vacancies"],
        new_specialists=payload["new_specialists"],
        updated_specialists=payload["updated_specialists"],
    )


def get_current_own_bench_url(app: Application) -> str:
    return str(app.bot_data.get("own_specialists_source_url") or OWN_BENCH_URL).strip()


async def show_own_bench_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    engine = context.application.bot_data["engine"]
    current_url = get_current_own_bench_url(context.application)
    context.user_data["mode"] = MODE_NONE
    await safe_reply_html(
        update.effective_message,
        views.join_nonempty(
            [
                views.b("Наш бенч"),
                views.link(current_url, current_url),
                _render_own_bench_sync_status_html(engine),
            ]
        ),
        disable_preview=False,
        reply_markup=own_bench_keyboard(),
    )


async def refresh_own_bench(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    engine = app.bot_data["engine"]
    current_url = get_current_own_bench_url(app)
    if not current_url:
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Ссылка на наш бенч не настроена.", warning=True),
            reply_markup=main_keyboard(),
        )
        return
    await safe_reply_html(
        update.effective_message,
        views.render_status_message("Обновляю наш бенч", body_html=f"Текущий источник: {views.link(current_url, current_url)}"),
        disable_preview=False,
        reply_markup=own_bench_keyboard(),
    )
    try:
        await run_own_registry_sync(app, reason="manual_refresh")
    except Exception as e:
        await safe_reply_html(
            update.effective_message,
            views.render_status_message(
                "Не удалось обновить наш бенч",
                body_html=f"Причина: {views.code(type(e).__name__)}",
                warning=True,
            ),
            reply_markup=own_bench_keyboard(),
        )
        return
    await safe_reply_html(
        update.effective_message,
        views.render_status_message(
            "Наш бенч обновлён",
            body_html=views.join_nonempty(
                [
                    f"Текущая ссылка: {views.link(current_url, current_url)}",
                    _render_own_bench_sync_status_html(engine),
                ]
            ),
        ),
        disable_preview=False,
        reply_markup=own_bench_keyboard(),
    )


async def replace_own_bench_link(update: Update, context: ContextTypes.DEFAULT_TYPE, new_url: str) -> None:
    app = context.application
    engine = app.bot_data["engine"]
    candidate = (new_url or "").strip()
    if not re.match(r"^https?://", candidate):
        await safe_reply_html(
            update.effective_message,
            views.render_status_message(
                "Нужна полная ссылка",
                body_html=f"Формат: {views.code('https://...')}",
                warning=True,
            ),
            reply_markup=own_bench_keyboard(),
        )
        return

    current_url = get_current_own_bench_url(app)
    if candidate == current_url:
        context.user_data["mode"] = MODE_NONE
        await safe_reply_html(
            update.effective_message,
            views.render_status_message("Ссылка не изменилась", body_html=f"Наш бенч: {views.link(current_url, current_url)}"),
            disable_preview=False,
            reply_markup=own_bench_keyboard(),
        )
        return

    await safe_reply_html(
        update.effective_message,
        views.render_status_message("Меняю ссылку на наш бенч", body="Мягко деактивирую старые записи и запускаю новый sync."),
        reply_markup=own_bench_keyboard(),
    )
    deactivated = deactivate_registry_source(engine, current_url) if current_url else 0
    set_setting(engine, OWN_BENCH_SOURCE_URL_KEY, candidate)
    app.bot_data["own_specialists_source_url"] = candidate

    try:
        await run_own_registry_sync(app, reason="manual_replace")
    except Exception as e:
        # Возвращаем старую ссылку, если новый импорт не поднялся.
        if current_url:
            set_setting(engine, OWN_BENCH_SOURCE_URL_KEY, current_url)
            app.bot_data["own_specialists_source_url"] = current_url
            try:
                await run_own_registry_sync(app, reason="manual_replace_rollback")
            except Exception:
                log.exception("Failed to restore previous own bench after replace failure")
        await safe_reply_html(
            update.effective_message,
            views.render_status_message(
                "Новая ссылка не загрузилась",
                body_html=f"Причина: {views.code(type(e).__name__)}. Вернул прежнюю ссылку.",
                warning=True,
            ),
            reply_markup=own_bench_keyboard(),
        )
        return

    context.user_data["mode"] = MODE_NONE
    await safe_reply_html(
        update.effective_message,
        views.join_nonempty(
            [
                views.b("Ссылка на наш бенч обновлена"),
                f"Старая ссылка деактивирована: {views.code(deactivated)}",
                f"Новая ссылка: {views.link(candidate, candidate)}",
                _render_own_bench_sync_status_html(engine),
            ]
        ),
        disable_preview=False,
        reply_markup=own_bench_keyboard(),
    )


async def run_own_registry_sync(app: Application, *, reason: str) -> None:
    source_url = str(app.bot_data.get("own_specialists_source_url") or "").strip()
    source_fetcher: MCPSourceFetcherClient | None = app.bot_data.get("source_fetcher")
    ollama: OllamaClient | None = app.bot_data.get("ollama")
    engine = app.bot_data.get("engine")
    if not source_url or source_fetcher is None or ollama is None or engine is None:
        return
    stats = await own_bench_use_cases.run_sync(
        engine,
        ollama,
        source_fetcher,
        source_url=source_url,
        reason=reason,
    )
    log.info("Own specialists sync finished reason=%s stats=%s", reason, stats)


async def _run_own_registry_sync_safe(app: Application, *, reason: str) -> None:
    try:
        await run_own_registry_sync(app, reason=reason)
    except Exception:
        log.exception("Own specialists sync failed reason=%s", reason)


async def send_daily_digest(app: Application, *, reason: str) -> None:
    await run_own_registry_sync(app, reason=reason)

    engine = app.bot_data["engine"]
    digest_chat_ids: set[int] = app.bot_data.get("digest_chat_ids", set())
    text_out = build_daily_digest_text(engine)
    for chat_id in sorted(digest_chat_ids):
        try:
            await safe_send_html(app.bot, chat_id=chat_id, html_out=text_out, disable_preview=True)
        except Exception as e:
            log.warning("Daily digest send failed chat_id=%s reason=%s error=%s", chat_id, reason, type(e).__name__)


async def send_daily_digest_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    await send_daily_digest(context.application, reason="job_queue")


def _next_digest_run_at(now: datetime | None = None) -> datetime:
    now_msk = (now or datetime.now(MSK)).astimezone(MSK)
    candidate = now_msk.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_msk >= candidate:
        candidate += timedelta(days=1)
    return candidate


async def _daily_digest_scheduler(app: Application) -> None:
    while True:
        next_run = _next_digest_run_at()
        sleep_for = max(0.0, (next_run - datetime.now(MSK)).total_seconds())
        log.info("Daily digest scheduler armed. next_run=%s", next_run.isoformat())
        try:
            await asyncio.sleep(sleep_for)
        except asyncio.CancelledError:
            raise

        try:
            await send_daily_digest(app, reason="builtin_scheduler")
        except Exception:
            log.exception("Built-in daily digest scheduler failed")


async def on_startup(app: Application) -> None:
    app.bot_data["own_registry_sync_task"] = asyncio.create_task(
        _run_own_registry_sync_safe(app, reason="startup"),
        name="own_registry_startup_sync",
    )
    if app.job_queue is None:
        task = asyncio.create_task(_daily_digest_scheduler(app), name="daily_digest_builtin_scheduler")
        app.bot_data["daily_digest_task"] = task
        log.warning("JobQueue is unavailable; using built-in daily digest scheduler.")
        return
    app.job_queue.run_daily(
        send_daily_digest_job,
        time=dt_time(hour=16, minute=0, tzinfo=MSK),
        name="daily_digest_msk",
    )


async def on_shutdown(app: Application) -> None:
    for key in ("daily_digest_task", "own_registry_sync_task"):
        task = app.bot_data.pop(key, None)
        if task is None:
            continue
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


safe_json_loads = extraction_use_cases.safe_json_loads
_build_structured_specialist_item = extraction_use_cases.build_structured_specialist_item
_build_structured_vacancy_item = extraction_use_cases.build_structured_vacancy_item
ensure_sources_extra_columns = source_trace_use_cases.ensure_sources_extra_columns
_compose_source_display = source_trace_use_cases.compose_source_display
_build_source_meta = source_trace_use_cases.build_source_meta
_load_own_bench_sync_state = own_bench_use_cases.load_sync_state
_compose_own_bench_empty_text = own_bench_use_cases.compose_empty_text
search_specialists = lambda engine, query_emb, query_text, k, *, own_bench_url=None: matching_use_cases.search_specialists(
    engine,
    query_emb,
    query_text,
    k,
    own_bench_url=own_bench_url,
    vector_dim=VECTOR_DIM,
    vector_str_fn=_vector_str,
)
search_vacancies = lambda engine, query_emb, query_text, k: matching_use_cases.search_vacancies(
    engine,
    query_emb,
    query_text,
    k,
    vector_dim=VECTOR_DIM,
    vector_str_fn=_vector_str,
)
upsert_matches = matching_use_cases.upsert_matches
upsert_matches_reverse = matching_use_cases.upsert_matches_reverse
_rank_specialist_hits = matching_use_cases.rank_specialist_hits
_rank_vacancy_hits = matching_use_cases.rank_vacancy_hits
_extract_own_bench_hits = matching_use_cases.extract_own_bench_hits


def build_app() -> Application:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")
    setup_logging()

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    allowed_ids = parse_ids(os.getenv("MANAGER_CHAT_IDS", ""))
    if not allowed_ids:
        raise RuntimeError("MANAGER_CHAT_IDS is empty")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is required")

    ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    llm_model = os.getenv("LLM_MODEL", "llama3:8b")
    embed_model = os.getenv("EMBED_MODEL")
    top_k = int(os.getenv("TOP_K", "10"))
    manual_top_k = int(os.getenv("MANUAL_TOP_K", "100"))
    manual_page_size = int(os.getenv("MANUAL_PAGE_SIZE", "10"))
    external_link_ingest_enabled = os.getenv("ENABLE_EXTERNAL_LINK_INGEST", "true").strip().lower() in ("1", "true", "yes", "on")
    mcp_transport = os.getenv("MCP_SOURCE_FETCHER_TRANSPORT", "stdio").strip().lower()
    mcp_command = os.getenv("MCP_SOURCE_FETCHER_COMMAND", "python -m app.integrations.mcp_source_fetcher.server").strip()
    partner_sheet_url = os.getenv("PARTNER_COMPANIES_SOURCE_URL", "").strip()
    default_own_bench_url = os.getenv("OWN_SPECIALISTS_SOURCE_URL", OWN_BENCH_URL).strip()

    ingest_ids_raw = os.getenv("BOT_INGEST_CHAT_IDS", "").strip()
    archive_chat_raw = os.getenv("ARCHIVE_CHAT_ID", "").strip()
    archive_chat_username = os.getenv("ARCHIVE_CHAT_USERNAME", "").strip().lstrip("@")
    archive_chat_ids = parse_ids(archive_chat_raw)
    archive_post_target: int | str | None = sorted(archive_chat_ids)[0] if archive_chat_ids else (archive_chat_username or None)

    engine = create_engine(db_url, pool_pre_ping=True)
    ensure_app_settings_table(engine)
    ensure_sources_extra_columns(engine)
    ensure_partner_companies_table(engine)
    ensure_own_specialists_registry_table(engine)
    own_specialists_source_url = get_or_init_setting(engine, OWN_BENCH_SOURCE_URL_KEY, default_own_bench_url)
    oll = OllamaClient(ollama_host, llm_model, embed_model)
    source_fetcher = MCPSourceFetcherClient(command=mcp_command) if mcp_transport == "stdio" else None
    if partner_sheet_url and source_fetcher is not None:
        try:
            counts = extract_partner_company_counts_from_sheet(partner_sheet_url, source_fetcher)
            upsert_partner_company_mentions(engine, counts, source_url=partner_sheet_url)
            log.info("Partner companies sync: source=%s items=%s", partner_sheet_url, len(counts))
        except Exception as e:
            log.warning("Partner companies sync failed: %s", type(e).__name__)
    partner_company_names = load_partner_company_names(engine)
    source_channel_ingest_ids = load_active_source_channel_chat_ids(engine)
    ingest_chat_ids = parse_ids(ingest_ids_raw) | archive_chat_ids | source_channel_ingest_ids
    if ingest_chat_ids:
        notify_trigger_chat_id = sorted(ingest_chat_ids)[0]
    else:
        notify_trigger_chat_id = int(os.getenv("NOTIFY_TRIGGER_CHAT_ID", "2033799185"))
    notify_trigger_chat_ids = set(ingest_chat_ids)
    if not notify_trigger_chat_ids and notify_trigger_chat_id:
        notify_trigger_chat_ids = {notify_trigger_chat_id}
    digest_chat_ids = parse_ids(os.getenv("DIGEST_CHAT_IDS", "")) or set(allowed_ids)

    app = ApplicationBuilder().token(token).post_init(on_startup).post_shutdown(on_shutdown).build()
    app.bot_data["allowed_ids"] = allowed_ids
    app.bot_data["engine"] = engine
    app.bot_data["ollama"] = oll
    app.bot_data["top_k"] = top_k
    app.bot_data["manual_top_k"] = manual_top_k
    app.bot_data["manual_page_size"] = manual_page_size
    app.bot_data["notify_trigger_chat_id"] = notify_trigger_chat_id
    app.bot_data["notify_trigger_chat_ids"] = notify_trigger_chat_ids
    app.bot_data["external_link_ingest_enabled"] = external_link_ingest_enabled
    app.bot_data["source_fetcher"] = source_fetcher
    app.bot_data["ingest_chat_ids"] = ingest_chat_ids
    app.bot_data["archive_chat_ids"] = archive_chat_ids
    app.bot_data["archive_post_target"] = archive_post_target
    app.bot_data["archive_chat_username"] = archive_chat_username or None
    app.bot_data["digest_chat_ids"] = digest_chat_ids
    app.bot_data["partner_company_names"] = partner_company_names
    app.bot_data["own_specialists_source_url"] = own_specialists_source_url

    log.info(
        "Bot config: managers=%s ingest_chats=%s archive_chats=%s notify_triggers=%s digest_chats=%s external_ingest=%s top_k=%s manual_top_k=%s manual_page_size=%s partner_companies=%s own_source=%s",
        sorted(allowed_ids),
        sorted(ingest_chat_ids),
        sorted(archive_chat_ids),
        sorted(notify_trigger_chat_ids),
        sorted(digest_chat_ids),
        external_link_ingest_enabled,
        top_k,
        manual_top_k,
        manual_page_size,
        len(partner_company_names),
        bool(own_specialists_source_url),
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("export_all", export_all_cmd))
    app.add_handler(CommandHandler("delete", delete_cmd))
    app.add_handler(CallbackQueryHandler(on_delete_callback, pattern=r"^del_(confirm|cancel)$"))
    app.add_handler(CallbackQueryHandler(on_top_page_callback, pattern=r"^top:[0-9a-f]{12}:\d+$"))

    # IMPORTANT: PTB 21.x uses filters.CAPTION (uppercase)
    app.add_handler(MessageHandler(filters.TEXT | filters.CAPTION, handle_buttons_and_text))

    app.add_error_handler(on_error)
    return app


def main():
    app = build_app()
    log.info("Manager bot started")
    # Важно фиксировать список update types, иначе Telegram может сохранить старый allowlist
    # и не присылать channel_post обновления.
    app.run_polling(
        close_loop=False,
        allowed_updates=[
            "message",
            "edited_message",
            "channel_post",
            "edited_channel_post",
            "callback_query",
        ],
    )


if __name__ == "__main__":
    main()
