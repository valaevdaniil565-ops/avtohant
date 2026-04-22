from __future__ import annotations

import re
from collections import Counter
from typing import Iterable

from sqlalchemy import text

from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient


_COMPANY_FIELD_RE = re.compile(
    r"(?im)(?:компан(?:ия|ии)|заказчик|клиент|customer|client)\s*[:\-]\s*([^\n|,;]{2,120})"
)
_REGISTRY_PARTNER_LINE_RE = re.compile(r"(?m)^([^:\n]{2,80})\s*:\s*([^\n]{2,200})$")
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

_ALIAS_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "Сбер": (
        re.compile(r"(?i)\bсбер\w*\b"),
        re.compile(r"(?i)\bsber(?:bank|business|cib|\w*)?\b"),
    ),
    "Лемана Про": (
        re.compile(r"(?i)\bлемана\s*про\b"),
        re.compile(r"(?i)\blemana\b"),
    ),
}

_REGISTRY_SKIP_KEYS = {
    "entity type hint",
    "роль",
    "стек",
    "резюме",
    "канала нет",
    "усн",
    "россия, снг",
    "350+",
    "аутсорсинг, аутстаффинг, продуктовая разработка",
    "работаем по процессам аутстаффа преимущественно. стеков много, основного нет",
    "java, kotlin, net, python, go, node.js, php",
    "react, angular, vue, typescript, javascript, html / css",
    "ios (swift / objective-c), android (kotlin / java), flutter, react native",
    "postgresql, mysql / mariadb, oracle, ms sql, mongodb, redis, clickhouse",
    "docker, kubernetes, helm, gitlab ci / github actions / jenkins, aws, gcp, azure, on-prem",
    "банки, финтех, ритейл, промышленность",
    "bi, ml",
    "да",
}
_INVALID_COMPANY_VALUES = {
    "банки",
    "финтех",
    "ритейл",
    "промышленность",
    "гос",
    "россия",
    "снг",
    "европа",
    "нет",
    "нету",
}


def ensure_partner_companies_table(engine) -> None:
    q = text(
        """
        CREATE TABLE IF NOT EXISTS partner_companies (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          canonical_name VARCHAR(255) NOT NULL UNIQUE,
          mentions_count INTEGER NOT NULL DEFAULT 0,
          source_url VARCHAR(1024),
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    with engine.begin() as c:
        c.execute(q)


def load_partner_company_names(engine) -> list[str]:
    with engine.begin() as c:
        rows = c.execute(
            text(
                """
                SELECT canonical_name
                FROM partner_companies
                WHERE mentions_count > 0
                ORDER BY mentions_count DESC, canonical_name ASC
                """
            )
        ).scalars().all()
    return [str(x).strip() for x in rows if str(x).strip()]


def upsert_partner_company_mentions(
    engine,
    counts: dict[str, int],
    *,
    source_url: str | None = None,
) -> None:
    if not counts:
        return
    q = text(
        """
        INSERT INTO partner_companies(canonical_name, mentions_count, source_url, updated_at)
        VALUES (:name, :cnt, :src, NOW())
        ON CONFLICT(canonical_name) DO UPDATE
          SET mentions_count = partner_companies.mentions_count + EXCLUDED.mentions_count,
              source_url = COALESCE(EXCLUDED.source_url, partner_companies.source_url),
              updated_at = NOW()
        """
    )
    with engine.begin() as c:
        for name, cnt in counts.items():
            if not name or cnt <= 0:
                continue
            c.execute(q, {"name": name, "cnt": int(cnt), "src": source_url})


def detect_partner_company_mention(text_in: str, partner_names: Iterable[str]) -> str | None:
    t = (text_in or "").lower()
    if not t:
        return None
    for raw_name in partner_names:
        name = (raw_name or "").strip()
        if not name:
            continue
        alias = _normalize_company_name(name).lower()
        if alias and alias in t:
            return name
    return None


def extract_partner_company_counts_from_sheet(
    sheet_url: str,
    client: MCPSourceFetcherClient,
) -> dict[str, int]:
    res = client.fetch_url(sheet_url)
    if not res.ok:
        return {}
    counter: Counter[str] = Counter()
    for item in res.items:
        companies = _extract_company_candidates(item.text)
        for c in companies:
            counter[c] += 1
    return dict(counter)


def extract_partner_company_sources_from_sheet(
    sheet_url: str,
    client: MCPSourceFetcherClient,
) -> list[dict[str, str]]:
    res = client.fetch_url(sheet_url)
    if not res.ok:
        return []

    entries: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in res.items:
        partner_name = _extract_registry_partner_name(item.text)
        if not partner_name:
            continue
        partner_url = _extract_registry_partner_source_url(item.text)
        if not partner_url:
            continue
        key = (partner_name, partner_url)
        if key in seen:
            continue
        seen.add(key)
        entries.append(
            {
                "name": partner_name,
                "source_url": partner_url,
                "registry_url": sheet_url,
            }
        )
    return entries


def _extract_company_candidates(text_in: str) -> set[str]:
    out: set[str] = set()
    t = text_in or ""
    if not t:
        return out

    # Known aliases (one hit per row).
    for canonical, pats in _ALIAS_PATTERNS.items():
        if any(p.search(t) for p in pats):
            out.add(canonical)

    # Explicit company fields, if present.
    for m in _COMPANY_FIELD_RE.finditer(t):
        v = _normalize_company_name(m.group(1))
        if v:
            out.add(v)
    registry_name = _extract_registry_partner_name(t)
    if registry_name:
        out.add(registry_name)
    return out


def _extract_registry_partner_name(text_in: str) -> str:
    for match in _REGISTRY_PARTNER_LINE_RE.finditer(text_in or ""):
        left = str(match.group(1) or "").strip()
        right = str(match.group(2) or "").strip()
        left_low = left.lower()
        if not left or not right:
            continue
        if left_low in _REGISTRY_SKIP_KEYS:
            continue
        if left_low.startswith("http") or "2026-" in left_low:
            continue
        if right.lower().startswith(("http://", "https://")):
            continue
        value = _normalize_company_name(right)
        if value:
            return value
    return ""


def _extract_registry_partner_source_url(text_in: str) -> str:
    lines = [line.strip() for line in str(text_in or "").splitlines() if line.strip()]
    preferred_labels = ("резюме", "https://firecode.ru/cv")
    fallback_urls: list[str] = []

    for line in lines:
        if ":" not in line:
            urls = _URL_RE.findall(line)
            fallback_urls.extend(urls)
            continue

        left, right = line.split(":", 1)
        label = left.strip().lower()
        value = right.strip()
        urls = _URL_RE.findall(value)
        if not urls:
            continue
        if any(marker in label for marker in preferred_labels):
            return urls[0]
        fallback_urls.extend(urls)

    return fallback_urls[0] if fallback_urls else ""


def _normalize_company_name(v: str) -> str:
    s = (v or "").strip()
    s = re.sub(r"\s{2,}", " ", s)
    s = s.strip(" .,:;|")
    low = s.lower()
    if not s:
        return ""
    if low in _INVALID_COMPANY_VALUES:
        return ""
    if "руб/час" in low or "₽" in low:
        return ""
    if "сбер" in low or "sber" in low:
        return "Сбер"
    if "лемана" in low:
        return "Лемана Про"
    if len(s) < 2:
        return ""
    return s
