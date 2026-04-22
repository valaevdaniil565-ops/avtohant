from __future__ import annotations

import html
import re
from datetime import datetime
from typing import Any, Iterable, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")


def h(text: Any) -> str:
    return html.escape("" if text is None else str(text), quote=False)


def b(text: Any) -> str:
    return f"<b>{h(text)}</b>"


def i(text: Any) -> str:
    return f"<i>{h(text)}</i>"


def code(text: Any) -> str:
    return f"<code>{h(text)}</code>"


def link(label: Any, url: Optional[str]) -> str:
    candidate = (url or "").strip()
    if not _is_safe_url(candidate):
        return h(label)
    return f'<a href="{html.escape(candidate, quote=True)}">{h(label)}</a>'


def join_nonempty(parts: Iterable[Optional[str]], sep: str = "\n") -> str:
    return sep.join([p for p in parts if p is not None])


def html_to_plain(text_in: str) -> str:
    text_out = text_in or ""
    text_out = re.sub(
        r'<a\s+href="([^"]+)">(.+?)</a>',
        lambda m: f"{html.unescape(m.group(2))} ({html.unescape(m.group(1))})",
        text_out,
        flags=re.IGNORECASE | re.DOTALL,
    )
    text_out = re.sub(r"</?(b|i|code)>", "", text_out, flags=re.IGNORECASE)
    text_out = re.sub(r"<[^>]+>", "", text_out)
    return html.unescape(text_out)


def render_start() -> str:
    return join_nonempty(
        [
            b("Менеджерский бот мэтчинга"),
            "Пришлите вакансию или кандидата/бенч — верну релевантный TOP.",
            "",
            b("Что умею"),
            "• Вакансия → TOP кандидатов",
            "• Кандидат/бенч → TOP вакансий",
            f"• {code('/export')} / {code('/export_all')} — выгрузка Excel",
            f"• {code('/delete')} — скрытие сущности по ссылке",
        ]
    )


def render_help() -> str:
    return join_nonempty(
        [
            b("Команды и сценарии"),
            f"• {code('/start')} — главное меню",
            f"• {code('/help')} — подсказка",
            f"• {code('/export')} — выгрузка active",
            f"• {code('/export_all')} — выгрузка all",
            f"• {code('/delete')} — скрытие по t.me ссылке",
            "",
            i("Также можно использовать кнопки меню."),
        ]
    )


def render_status_message(
    title: str,
    body: Optional[str] = None,
    *,
    note: Optional[str] = None,
    warning: bool = False,
    body_html: Optional[str] = None,
) -> str:
    title_text = f"⚠️ {title}" if warning else title
    return join_nonempty(
        [
            b(title_text),
            body_html if body_html is not None else (h(body) if body else None),
            i(note) if note else None,
        ]
    )


def render_source(source_display: Optional[str], url: Optional[str] = None) -> str:
    meta = _parse_source_display(source_display, url)
    segments: list[str] = []

    person_name = meta.get("person_name")
    manager_name = meta.get("manager_name")
    source_url = meta.get("url")
    source_kind = meta.get("kind")
    sheet_name = meta.get("sheet_name")
    table_index = meta.get("table_index")
    index = meta.get("index")

    if person_name:
        segments.append(f"Специалист: {h(person_name)}")
    elif manager_name:
        segments.append(f"Менеджер: {h(manager_name)}")

    if source_kind == "archive_post":
        segments.append(link("архив-пост", source_url))
    elif source_kind == "file":
        label = "реестр" if person_name else "файл"
        segments.append(link(label, source_url))
    elif source_kind == "message":
        segments.append(link("сообщение", source_url))
    elif source_url:
        segments.append(link("источник", source_url))

    if sheet_name:
        segments.append(f"лист {h(sheet_name)}")
    if table_index:
        segments.append(f"таблица {h(table_index)}")
    if index:
        segments.append(f"индекс {h(index)}")

    if not segments:
        return ""
    return "🔗 " + " · ".join(segments)


def render_import_summaries(summaries: list[dict[str, Any]]) -> str:
    blocks = [render_import_summary(summary) for summary in summaries if summary]
    return "\n\n".join(blocks)


def render_import_summary(summary: dict[str, Any]) -> str:
    source_url = str(summary.get("source_url") or "").strip() or None
    source_label = str(summary.get("source_label") or summary.get("source_type") or "источник").strip()
    lines = [
        b("Импорт источника"),
        f"Источник: {link(source_label, source_url) if source_url else h(source_label)}",
        (
            f"Листов: {code(summary.get('sheets_total', 0))} · "
            f"обработано: {code(summary.get('sheets_processed', 0))} · "
            f"пропущено: {code(summary.get('sheets_skipped', 0))} · "
            f"строк: {code(summary.get('items_count', 0))}"
        ),
    ]

    confidence = summary.get("confidence") or {}
    total_conf = sum(int(confidence.get(k) or 0) for k in ("high", "medium", "low"))
    if total_conf:
        lines.append(
            "Уверенность: "
            + " · ".join(
                [
                    f"high {code(confidence.get('high', 0))}",
                    f"medium {code(confidence.get('medium', 0))}",
                    f"low {code(confidence.get('low', 0))}",
                ]
            )
        )

    processed = list(summary.get("processed_sheets") or [])
    if processed:
        lines.append("")
        lines.append(b("Обработанные листы"))
        for sheet in processed[:6]:
            sheet_name = str(sheet.get("sheet_name") or f"Sheet {sheet.get('sheet_index') or '?'}")
            entity_hint = str(sheet.get("sheet_entity_hint") or "UNKNOWN")
            lines.append(
                f"• {h(sheet_name)} — {code(entity_hint)} · "
                f"таблиц: {code(sheet.get('tables_processed', 0))} · "
                f"строк: {code(sheet.get('rows_imported', 0))}"
            )
            reasons = _render_skip_reasons(sheet.get("skip_reasons") or {})
            if reasons:
                lines.append(f"skip: {reasons}")

    skipped = list(summary.get("skipped_sheets") or [])
    if skipped:
        lines.append("")
        lines.append(b("Пропущенные листы"))
        for sheet in skipped[:6]:
            sheet_name = str(sheet.get("sheet_name") or f"Sheet {sheet.get('sheet_index') or '?'}")
            reason = _humanize_skip_reason(str(sheet.get("skip_reason") or "unknown"))
            lines.append(f"• {h(sheet_name)} — {h(reason)}")

    return join_nonempty(lines)


def render_hit(hit: dict[str, Any], rank: int) -> str:
    pct = int(round(max(0.0, min(1.0, float(hit.get("sim") or 0.0))) * 100))
    role = hit.get("role") or "Unknown"
    grade = hit.get("grade") or "—"
    stack = "/".join((hit.get("stack") or [])[:6]) or "—"
    rate = _fmt_money(hit.get("rate_min"), hit.get("rate_max"), hit.get("currency"))
    location = hit.get("location") or "—"

    detail_lines: list[str] = []
    if bool(hit.get("is_internal")):
        detail_lines.append(f"✅ {b('Наш специалист')}")

    detail_lines.extend(
        [
            _render_detail_line("Грейд", grade),
            _render_detail_line("Стек", stack),
            _render_detail_line("Ставка", rate),
            _render_detail_line("Локация", location),
        ]
    )

    source_line = render_source(hit.get("source_display"), hit.get("url"))

    return join_nonempty(
        [
            f"{b(f'{rank:02d}.')} {b(f'{pct}%')} · {h(role)}",
            *detail_lines,
            source_line,
        ]
    )


def render_entity_summary_block(entity_label: str, summary: Optional[str], *, fields: Optional[dict[str, Any]] = None) -> str:
    label = (entity_label or "").strip()
    role, grade, stack = _split_summary(summary)
    raw_fields = fields or {}
    name = str(raw_fields.get("name") or "").strip()
    role = str(raw_fields.get("role") or role or "").strip()
    grade = str(raw_fields.get("grade") or grade or "").strip()
    if raw_fields.get("stack"):
        if isinstance(raw_fields.get("stack"), list):
            stack = ", ".join(str(v).strip() for v in raw_fields.get("stack") or [] if str(v).strip())
        else:
            stack = str(raw_fields.get("stack") or "").strip()
    lines: list[str] = [b(label)] if label else []
    if name:
        lines.append(_render_detail_line("Имя", name))
    if role:
        lines.append(_render_detail_line("Роль", role))
    if grade:
        lines.append(_render_detail_line("Грейд", grade))
    if stack:
        lines.append(_render_detail_line("Стек", stack))
    return join_nonempty(lines)


def _render_detail_line(label: str, value: Any) -> str:
    return f"{b(f'· {label}:')} {h(value)}"


def render_hits_block(hits: list[dict[str, Any]], *, start_rank: int = 1) -> str:
    if not hits:
        return "Нет мэтчей."
    blocks = [render_hit(hit, rank) for rank, hit in enumerate(hits, start=start_rank)]
    return "\n\n".join(blocks)


def render_own_bench_block(
    hits: list[dict[str, Any]],
    *,
    empty_text: str = "На нашем бенче нет подходящих специалистов.",
) -> str:
    if not hits:
        return join_nonempty([b("НАШ БЕНЧ"), h(empty_text)])
    return join_nonempty([b("НАШ БЕНЧ"), "", render_hits_block((hits or [])[:10])])


def render_top_page(
    *,
    title: str,
    entity_label: str,
    summary: Optional[str],
    hits: list[dict[str, Any]],
    source_display: Optional[str],
    source_url: Optional[str],
    page: Optional[int] = None,
    total_pages: Optional[int] = None,
    total_hits: Optional[int] = None,
    own_bench_block: Optional[str] = None,
    warning_text: Optional[str] = None,
    results_label: Optional[str] = None,
    start_rank: int = 1,
    entity_fields: Optional[dict[str, Any]] = None,
    show_hits_block: bool = True,
) -> str:
    parts = [b(title)] if title else []
    if summary:
        parts.append(render_entity_summary_block(entity_label, summary, fields=entity_fields))
    if warning_text:
        parts.append(h(warning_text))
    if own_bench_block:
        parts.append(own_bench_block)
    if page is not None and total_pages is not None and total_hits is not None:
        parts.append(i(f"Страница {page}/{total_pages} · всего {total_hits}"))
    source_line = render_source(source_display, source_url)
    if source_line:
        parts.append(source_line)
    if show_hits_block:
        parts.append("")
        if results_label:
            parts.append(b(results_label))
            parts.append("")
        parts.append(render_hits_block(hits, start_rank=start_rank))
    return join_nonempty(parts)


def render_digest(
    *,
    window_start: datetime,
    window_end: datetime,
    new_vacancies: list[dict[str, Any]],
    updated_vacancies: list[dict[str, Any]],
    new_specialists: list[dict[str, Any]],
    updated_specialists: list[dict[str, Any]],
) -> str:
    if not any((new_vacancies, updated_vacancies, new_specialists, updated_specialists)):
        return join_nonempty(
            [
                b("Дайджест за последние 24 часа"),
                i(
                    f"Окно: {window_start.astimezone(MSK).strftime('%d.%m %H:%M')} — "
                    f"{window_end.astimezone(MSK).strftime('%d.%m %H:%M')} МСК"
                ),
                "",
                "Новых или обновлённых вакансий и bench нет.",
            ]
        )

    sections = [
        _render_digest_section("Новые вакансии", new_vacancies, ts_field="created_at"),
        _render_digest_section("Обновлённые вакансии", updated_vacancies, ts_field="updated_at"),
        _render_digest_section("Новые bench", new_specialists, ts_field="created_at"),
        _render_digest_section("Обновлённые bench", updated_specialists, ts_field="updated_at"),
    ]
    return join_nonempty(
        [
            b("Дайджест за последние 24 часа"),
            i(
                f"Окно: {window_start.astimezone(MSK).strftime('%d.%m %H:%M')} — "
                f"{window_end.astimezone(MSK).strftime('%d.%m %H:%M')} МСК"
            ),
            "",
            *sections,
        ]
    )


def render_archive_post(
    *,
    source_name: str,
    classification: str,
    original_date: datetime | None,
    sender_display: str,
    original_url: Optional[str],
    raw_text: str,
    max_total: int = 3900,
) -> str:
    body = (raw_text or "").strip() or "[без текста]"
    lines = [
        b("Архив источника"),
        f"Источник: {h(source_name or '-')}",
        f"Тип: {code(classification)}",
        f"Дата: {h(_format_utc_datetime(original_date, fallback='unknown'))}",
        f"Отправитель: {h(sender_display or 'unknown')}",
    ]
    if original_url:
        lines.append(f"Ссылка: {link('оригинал', original_url)}")
    prefix = join_nonempty(lines + ["", b("Копия исходного сообщения"), ""])
    body = _truncate_body(prefix, body, max_total)
    return prefix + ("\n" if prefix else "") + h(body)


def render_reference_archive_post(
    *,
    original_text: str,
    original_date: datetime | None,
    source_name: str,
    manager_name: str,
    items: list[str],
    mode_marker: str,
    max_total: int = 3900,
) -> str:
    body = (original_text or "").strip() or "[без текста]"
    item_lines = [f"{idx}. {h(_humanize_summary(summary))}" for idx, summary in enumerate(items, start=1)] or ["1. [не определено]"]
    prefix = join_nonempty(
        [
            b("Reference-only источник"),
            f"Дата: {h(_format_utc_datetime(original_date, fallback='-'))}",
            f"Источник: {h(source_name or '-')}",
            f"Менеджер: {h(manager_name or '-')}",
            f"Режим: {code(mode_marker)}",
            "",
            b("Копия исходного сообщения"),
            "",
        ]
    )
    body = _truncate_body(prefix, body, max_total, reserve_for_suffix=len("\n\n<b>Индексы сущностей</b>\n") + sum(len(v) + 1 for v in item_lines))
    suffix = join_nonempty(["", b("Индексы сущностей"), *item_lines])
    return prefix + h(body) + suffix


def _parse_source_display(source_display: Optional[str], fallback_url: Optional[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    text_value = (source_display or "").strip()
    if text_value:
        for part in text_value.split(";"):
            if ":" not in part:
                continue
            key, value = part.split(":", 1)
            out[_normalize_key(key)] = value.strip()

    if "менеджер" in out:
        out["manager_name"] = out["менеджер"]
    if "специалист" in out:
        out["person_name"] = out["специалист"]
    if "индекс" in out:
        out["index"] = out["индекс"]
    if "лист" in out:
        out["sheet_name"] = out["лист"]
    if "таблица" in out:
        out["table_index"] = out["таблица"]

    if "ссылка на архив пост" in out or "ссылка на архив-пост" in out:
        out["kind"] = "archive_post"
        out["url"] = out.get("ссылка на архив пост") or out.get("ссылка на архив-пост") or (fallback_url or "")
    elif "ссылка на файл" in out:
        out["kind"] = "file"
        out["url"] = out.get("ссылка на файл") or (fallback_url or "")
    elif "ссылка на сообщение" in out:
        out["kind"] = "message"
        out["url"] = out.get("ссылка на сообщение") or (fallback_url or "")
    elif fallback_url:
        out["kind"] = "message"
        out["url"] = fallback_url

    if fallback_url and "url" not in out:
        out["url"] = fallback_url
    return out


def _normalize_key(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower()).replace("ё", "е")


def _humanize_summary(summary: str) -> str:
    return re.sub(r"\s*\|\s*", " · ", (summary or "").strip())


def _split_summary(summary: Optional[str]) -> tuple[str, str, str]:
    value = (summary or "").strip()
    if not value:
        return "", "", ""
    parts = [part.strip() for part in re.split(r"\s*\|\s*", value)]
    while len(parts) < 3:
        parts.append("")
    return parts[0], parts[1], parts[2]


def _fmt_money(rmin: Optional[int], rmax: Optional[int], currency: Optional[str]) -> str:
    if rmin is None and rmax is None:
        return "—"
    cur = (currency or "").strip()
    if rmin is not None and rmax is not None and rmin != rmax:
        return f"{rmin:,}-{rmax:,} {cur}".replace(",", " ").strip()
    val = rmin if rmin is not None else rmax
    return f"{val:,} {cur}".replace(",", " ").strip()


def _render_skip_reasons(reasons: dict[str, Any]) -> str:
    parts = []
    for key, value in reasons.items():
        count = int(value or 0)
        if count <= 0:
            continue
        parts.append(f"{h(_humanize_skip_reason(str(key)))}={code(count)}")
    return " · ".join(parts)


def _humanize_skip_reason(value: str) -> str:
    mapping = {
        "empty_sheet": "пустой лист",
        "service_sheet_name": "служебный лист",
        "no_relevant_structure": "нет релевантной структуры",
        "no_detectable_tables": "не найдены таблицы",
        "no_confident_rows": "нет уверенных строк",
        "empty_row": "пустые строки",
        "totals_row": "строки итогов",
        "section_row": "строки секций",
        "low_confidence_row": "низкая уверенность",
        "no_text_lines": "пустые записи",
        "no_data_rows": "нет строк данных",
    }
    return mapping.get((value or "").strip(), (value or "").strip())


def _render_digest_section(title: str, rows: list[dict[str, Any]], *, ts_field: str) -> str:
    if not rows:
        return join_nonempty([b(title), "Нет изменений."])
    items = [b(title)]
    for idx, row in enumerate(rows, start=1):
        stack = "/".join((row.get("stack") or [])[:5]) or "—"
        stamp = row.get(ts_field)
        stamp_local = stamp.astimezone(MSK).strftime("%d.%m %H:%M") if isinstance(stamp, datetime) else "—"
        items.append(
            join_nonempty(
                [
                    f"{idx}. {h(row.get('role') or 'Unknown')}",
                    _render_detail_line("Грейд", row.get("grade") or "—"),
                    _render_detail_line("Стек", stack),
                    _render_detail_line("Обновлено", stamp_local),
                    render_source(row.get("source_display"), row.get("url")),
                ]
            )
        )
    return "\n\n".join(items)


def _format_utc_datetime(value: datetime | None, *, fallback: str) -> str:
    if not isinstance(value, datetime):
        return fallback
    dt = value if value.tzinfo else value.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S UTC")


def _truncate_body(prefix: str, body: str, max_total: int, *, reserve_for_suffix: int = 0) -> str:
    room = max(0, int(max_total) - len(prefix) - int(reserve_for_suffix))
    if len(body) <= room:
        return body
    return body[: max(0, room - 1)] + "…"


def _is_safe_url(value: str) -> bool:
    if not value:
        return False
    try:
        parsed = urlparse(value)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
