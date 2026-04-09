from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.integrations.mcp_source_fetcher.schemas import NormalizedItem
from app.services.link_extraction import extract_external_urls

REGISTRY_VACANCIES_COLUMN = "Ссылка на канал с вакансиями"
REGISTRY_BENCH_COLUMN = "Ссылка на бенч Вашей компании"
REGISTRY_COMPANY_COLUMN = "Какую компанию Вы представляете?"

TARGET_COLUMNS = (
    REGISTRY_VACANCIES_COLUMN,
    REGISTRY_BENCH_COLUMN,
)


@dataclass(frozen=True)
class RegistrySourceCandidate:
    registry_row_index: int | None
    company_name: str
    column_name: str
    source_url: str


def parse_normalized_registry_row(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key:
            out[key] = value
    return out


def extract_registry_source_candidates(
    items: Iterable[NormalizedItem],
    *,
    target_columns: tuple[str, ...] = TARGET_COLUMNS,
) -> list[RegistrySourceCandidate]:
    out: list[RegistrySourceCandidate] = []
    seen_urls: set[str] = set()

    for item in items:
        row = parse_normalized_registry_row(getattr(item, "text", "") or "")
        if not row:
            continue
        company_name = (row.get(REGISTRY_COMPANY_COLUMN) or "-").strip() or "-"
        row_index = getattr(item, "row_index", None)

        for column_name in target_columns:
            raw_cell = (row.get(column_name) or "").strip()
            if not raw_cell:
                continue
            for url in extract_external_urls(raw_cell):
                normalized_url = url.strip()
                if not normalized_url or normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                out.append(
                    RegistrySourceCandidate(
                        registry_row_index=row_index,
                        company_name=company_name,
                        column_name=column_name,
                        source_url=normalized_url,
                    )
                )

    return out
