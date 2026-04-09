# app/pipeline.py
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Optional, List, Dict

# --- public API -------------------------------------------------------------

@dataclass
class PreprocessResult:
    raw: str
    text: str
    contacts: List[str]
    urls: List[str]
    hints: Dict[str, object]  # role/grade/stack/kind guesses etc.


def preprocess_for_llm(text: str, kind: Optional[str] = None, max_chars: int = 7000) -> PreprocessResult:
    """
    kind: "VACANCY" | "BENCH" | None
    """
    raw = text or ""
    t = raw

    t = _normalize_unicode(t)
    t = _strip_forward_headers(t)
    t = _strip_markdown_noise(t)
    t = _normalize_bullets(t)
    t = _strip_emojis_soft(t)
    t = _collapse_whitespace(t)

    contacts = _extract_contacts(t)
    urls = _extract_urls(t)

    # --- IMPORTANT: do not "compress" batch messages (several benches/vacancies) ---
    is_batch = _looks_like_batch(t)

    # Если очень длинно — делаем rule-based "сжатие", НО только если это не пачка
    if (not is_batch) and len(t) > max_chars:
        t = _keep_relevant_blocks(t, max_lines=180)

    # Даже если не длинно — слегка “подчистим” и оставим важное, НО только если это не пачка
    if not is_batch:
        t2 = _keep_relevant_blocks(t, max_lines=220)
        # если вдруг получилось слишком коротко — оставим исходное нормализованное
        if len(t2) >= 250:
            t = t2

    hints = {
        "role": _guess_role(t),
        "grade": _guess_grade(t),
        "stack": _guess_stack(t),
        "contacts": contacts,
        "urls": urls,
        "kind": kind,
        "is_batch": is_batch,
    }
    return PreprocessResult(raw=raw, text=t, contacts=contacts, urls=urls, hints=hints)


def build_fallback_vacancy_item(pre: PreprocessResult) -> dict:
    """
    Минимальная вакансия, если LLM вернул items=[] (и мы форсили VACANCY).
    Ничего не выдумываем: роль/grade/stack по эвристикам, description — кусок текста.
    """
    role = (pre.hints.get("role") or "Unknown") if isinstance(pre.hints.get("role"), str) else "Unknown"
    grade = pre.hints.get("grade") if isinstance(pre.hints.get("grade"), str) else None
    stack = pre.hints.get("stack") if isinstance(pre.hints.get("stack"), list) else []
    return {
        "role": role,
        "stack": stack,
        "grade": grade,
        "experience_years_min": None,
        "experience_years_max": None,
        "rate_min": None,
        "rate_max": None,
        "currency": None,
        "rate_period": None,
        "rate_is_net": None,
        "company": None,
        "client": None,
        "location": None,
        "work_format": None,
        "employment_type": None,
        "timezone": None,
        "start_date": None,
        "duration_months": None,
        "responsibilities": [],
        "requirements": [],
        "nice_to_have": [],
        "benefits": [],
        "contacts": pre.contacts,
        "source_urls": pre.urls,
        "is_closed": False,
        "close_reason": None,
        "description": (pre.text[:1500] if pre.text else "").strip(),
    }


def build_fallback_specialist_item(pre: PreprocessResult) -> dict:
    role = (pre.hints.get("role") or "Unknown") if isinstance(pre.hints.get("role"), str) else "Unknown"
    grade = pre.hints.get("grade") if isinstance(pre.hints.get("grade"), str) else None
    stack = pre.hints.get("stack") if isinstance(pre.hints.get("stack"), list) else []
    return {
        "role": role,
        "stack": stack,
        "grade": grade,
        "experience_years_min": None,
        "experience_years_max": None,
        "rate_min": None,
        "rate_max": None,
        "currency": None,
        "rate_period": None,
        "rate_is_net": None,
        "location": None,
        "work_format": None,
        "timezone": None,
        "availability_weeks": None,
        "is_available": True,
        "contacts": pre.contacts,
        "source_urls": pre.urls,
        "languages": [],
        "relocation": None,
        "description": (pre.text[:1000] if pre.text else "").strip() or None,
    }


# --- internals --------------------------------------------------------------

def _normalize_unicode(t: str) -> str:
    t = t.replace("\u00A0", " ")  # NBSP
    t = t.replace("\u200b", "")   # zero-width space
    return unicodedata.normalize("NFKC", t)


_FORWARD_HDR_RE = re.compile(r"^.{1,80},\s*\[[^\]]{6,60}\]\s*:\s*$", re.IGNORECASE)

def _strip_forward_headers(t: str) -> str:
    # убираем строки вида "Имя, [Feb 17, 2026 at 11:39 PM]:"
    lines = t.splitlines()
    out = []
    for ln in lines:
        if _FORWARD_HDR_RE.match(ln.strip()):
            continue
        out.append(ln)
    return "\n".join(out)


def _strip_markdown_noise(t: str) -> str:
    # зачёркивание / жирный / код / лишние символы форматирования
    t = t.replace("~~", "")
    t = t.replace("**", "")
    t = t.replace("__", "")
    t = t.replace("`", "")
    return t


_BULLET_PREFIX_RE = re.compile(r"^\s*([•●▪▫‣∙–—\-]+)\s+")

def _normalize_bullets(t: str) -> str:
    lines = t.splitlines()
    out = []
    for ln in lines:
        ln2 = ln
        m = _BULLET_PREFIX_RE.match(ln2)
        if m:
            ln2 = _BULLET_PREFIX_RE.sub("- ", ln2)
        # частый случай: "●текст" без пробела
        ln2 = re.sub(r"^\s*[•●▪▫]\s*", "- ", ln2)
        out.append(ln2)
    return "\n".join(out)


def _strip_emojis_soft(t: str) -> str:
    # мягкое удаление emoji/символов категории So/Sk, не трогаем буквы/цифры/пунктуацию
    out_chars = []
    for ch in t:
        cat = unicodedata.category(ch)
        # эмодзи обычно So, иногда Sk; но оставим валюты и базовую пунктуацию
        if cat in ("So", "Sk") and ch not in ("$", "€", "£", "₽"):
            continue
        out_chars.append(ch)
    return "".join(out_chars)


def _collapse_whitespace(t: str) -> str:
    # нормализуем пробелы/пустые строки
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _extract_contacts(t: str) -> List[str]:
    contacts = set()

    for u in re.findall(r"@[A-Za-z0-9_]{4,64}", t):
        contacts.add(u)

    for em in re.findall(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", t):
        contacts.add(em)

    for ph in re.findall(r"(?:(?:\+7|7|8)\s*)?(?:\(?\d{3}\)?\s*)?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}", t):
        # грубо, но для MVP ок
        p = re.sub(r"\s+", "", ph)
        if len(re.sub(r"\D", "", p)) >= 10:
            contacts.add(ph.strip())

    return sorted(contacts)


def _extract_urls(t: str) -> List[str]:
    urls = set()
    for u in re.findall(r"(https?://[^\s)]+)", t):
        urls.add(u.strip().rstrip(".,;"))
    for u in re.findall(r"\bt\.me/[^\s)]+", t):
        urls.add(("https://" + u.strip()).rstrip(".,;"))
    return sorted(urls)


def _looks_like_batch(t: str) -> bool:
    """
    Эвристика: сообщение похоже на "пачку" (несколько бенчей или несколько вакансий).
    Важно: для пачек НЕ делаем keep_relevant_blocks(), чтобы не ломать границы карточек.
    """
    low = (t or "").lower()
    if not low:
        return False

    # 1) много явных разделителей карточек
    # строки, начинающиеся с = или ~
    sep_lines = re.findall(r"(?m)^\s*[=~]\s*\S", t)
    if len(sep_lines) >= 2:
        return True

    # 2) повторы "якорей" (как минимум 2 вхождения ключевых заголовков)
    bench_hits = 0
    vac_hits = 0

    bench_hits += len(re.findall(r"(?i)\bуровень\b\s*[:\-]", t))
    bench_hits += len(re.findall(r"(?i)\bопыт\b\s*[:\-]", t))
    bench_hits += len(re.findall(r"(?i)\bстек\b\s*[:\-]", t))
    bench_hits += len(re.findall(r"(?i)\bлокац(?:ия|ион)\b\s*[:\-]", t))
    bench_hits += len(re.findall(r"(?i)\bготовност(?:ь|и)\b\s*[:\-]", t))
    bench_hits += len(re.findall(r"(?i)\b(рейт|ставка)\b\s*[:\-]", t))

    vac_hits += len(re.findall(r"(?i)\bтребован(?:ия|ий)\b\s*[:\-]", t))
    vac_hits += len(re.findall(r"(?i)\bобязанност(?:и|ей)\b\s*[:\-]", t))
    vac_hits += len(re.findall(r"(?i)\bуслов(?:ия|ий)\b\s*[:\-]", t))
    vac_hits += len(re.findall(r"(?i)\bзадач(?:и|)\b\s*[:\-]", t))

    if bench_hits >= 2 or vac_hits >= 2:
        return True

    # 3) fallback: несколько "уровень/опыт/требования" без двоеточия
    # (часто в реальных текстах)
    if low.count("уровень") >= 2 or low.count("опыт") >= 2 or low.count("требован") >= 2:
        return True

    return False


# “умное” сжатие: оставляем шапку + релевантные линии
_KEEP_KEYWORDS = (
    # вакансии/проекты
    "требован", "обязан", "задач", "ответствен", "услов", "ставк", "оплат", "срок", "дедлайн", "подач",
    "локац", "формат", "удал", "remote", "гибрид", "офис", "заказчик", "клиент", "компан",
    "контакт", "рекрутер", "telegram", "tg", "присылать", "прислать",
    # анкеты/бенчи
    "уровень", "опыт", "стек", "готовн", "рейт", "кандид", "специал",
    # инструменты
    "jira", "confluence", "atlassian", "bitbucket", "bamboo", "crowd",
    # налоги/платёжные
    "ндс", "vat", "net", "gross",
    # прочее
    "резюме", "cv",
)

def _keep_relevant_blocks(t: str, max_lines: int = 200) -> str:
    lines = [ln.strip() for ln in t.splitlines()]
    lines = [ln for ln in lines if ln]  # убираем пустые

    if not lines:
        return ""

    header = lines[:12]  # первые строки почти всегда важны

    def is_relevant(ln: str) -> bool:
        low = ln.lower()
        if "@" in ln or "t.me/" in low or "http" in low:
            return True
        if re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", ln):  # даты
            return True
        if re.search(r"\b\d{3,6}\b", ln) and any(x in low for x in ("руб", "rub", "usd", "eur", "₽", "$", "€", "ндс", "vat", "day", "дней", "дня")):
            return True
        return any(k in low for k in _KEEP_KEYWORDS)

    body = []
    for ln in lines[12:]:
        if is_relevant(ln):
            body.append(ln)

    # Если “релевантных” очень мало — добавим чуть больше (буллеты после заголовков)
    if len(body) < 12:
        body = lines[12: min(len(lines), 80)]

    out = header + [""] + body
    out = out[:max_lines]
    return "\n".join(out).strip()


def _guess_role(t: str) -> Optional[str]:
    # берём первую “сильную” строку
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    if not lines:
        return None

    for ln in lines[:6]:
        first = ln.strip()

        # убрать лидирующие маркеры карточек: "= ", "~ ", "- ", "— ", "• ", "* "
        first = re.sub(r"^[=\~\-\—\•\*]+\s*", "", first)

        # частые префиксы вакансий
        first = re.sub(
            r"^(требуется|нужен|нужна|ищем|вакансия)\s*[:\-]?\s*",
            "",
            first,
            flags=re.IGNORECASE,
        )

        first = first[:120].strip()
        if len(first) >= 3:
            return first

    # fallback: всё же вернем первую строку, если совсем не нашли
    first = re.sub(r"^[=\~\-\—\•\*]+\s*", "", lines[0]).strip()
    first = first[:120].strip()
    return first or None


def _guess_grade(t: str) -> Optional[str]:
    m = re.search(r"(уровень|грейд)\s*[:\-]\s*([A-Za-zА-Яа-я+/ ]{3,30})", t, flags=re.IGNORECASE)
    if m:
        g = m.group(2).strip()
        return _normalize_grade(g)

    # иногда пишут "Middle/Middle+"
    m2 = re.search(r"\b(Junior|Middle\+?|Senior|Lead|Architect)\b", t, flags=re.IGNORECASE)
    if m2:
        return _normalize_grade(m2.group(1))
    return None


def _normalize_grade(g: str) -> Optional[str]:
    g = g.strip().lower().replace(" ", "")
    if "junior" in g:
        return "Junior"
    if "middle+" in g or "middleplus" in g:
        return "Middle+"
    if "middle" in g:
        return "Middle"
    if "senior" in g:
        return "Senior"
    if "lead" in g:
        return "Lead"
    if "architect" in g or "архит" in g:
        return "Architect"
    return None


_TECH_WORDS = (
    "jira", "confluence", "atlassian", "bitbucket", "bamboo", "crowd",
    "ms office", "excel", "powerpoint", "visio", "bpmn", "uml",
    "sql", "postgres", "api", "rest", "soap",
)

def _guess_stack(t: str) -> List[str]:
    low = t.lower()
    found = []
    for w in _TECH_WORDS:
        if w in low:
            # нормализуем вид
            norm = w.title() if w not in ("sql", "api", "rest", "soap") else w.upper()
            if w == "jira":
                norm = "Jira"
            if w == "confluence":
                norm = "Confluence"
            if w == "atlassian":
                norm = "Atlassian"
            if w == "ms office":
                norm = "MS Office"
            found.append(norm)

    # уникализируем, сохраняем порядок
    uniq = []
    for x in found:
        if x not in uniq:
            uniq.append(x)
    return uniq[:25]
