from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.integrations.mcp_source_fetcher.schemas import SourceFetchResult
from app.llm.pre_classifier import split_line_wise_bench_items
from app.services.link_extraction import extract_external_urls


@dataclass
class IngestionUnit:
    text: str
    source_type: str
    external_url: Optional[str] = None
    external_kind: Optional[str] = None
    external_locator: Optional[str] = None
    source_meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExternalIngestionOutcome:
    urls: list[str] = field(default_factory=list)
    units: list[IngestionUnit] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    statuses: list[str] = field(default_factory=list)
    summaries: list[dict[str, Any]] = field(default_factory=list)


async def build_external_ingestion_units(
    message_text: str,
    *,
    enabled: bool,
    client: MCPSourceFetcherClient | None,
) -> ExternalIngestionOutcome:
    out = ExternalIngestionOutcome()
    urls = extract_external_urls(message_text)
    out.urls = urls
    if not enabled or not client or not urls:
        return out

    max_units = int(os.getenv("SOURCE_FETCHER_MAX_ITEMS", "200"))
    for url in urls:
        res: SourceFetchResult = await asyncio.to_thread(client.fetch_url, url)
        if not res.ok:
            out.errors.append(f"{url}: {res.error or 'unknown error'}")
            out.statuses.append(f"• {url} -> error: {res.error or 'unknown'}")
            continue

        import_summary = dict(res.metadata.get("import_summary") or {})
        if import_summary:
            import_summary["source_url"] = res.source_url
            import_summary["source_type"] = res.source_type
            out.summaries.append(import_summary)

        built = 0
        for i, item in enumerate(res.items, start=1):
            expanded = _expand_item_text(res.source_type, item.text, item.row_index)
            if not expanded:
                expanded = [(item.text, item.row_index)]

            for expanded_text, expanded_idx in expanded:
                if len(out.units) >= max_units:
                    out.errors.append(f"items limit reached ({max_units}), tail skipped")
                    break

                combined_text = _compose_text(message_text, expanded_text, res.source_url, res.source_type, expanded_idx)
                locator = f"row:{expanded_idx}" if expanded_idx is not None else None
                out.units.append(
                    IngestionUnit(
                        text=combined_text,
                        source_type="external_link",
                        external_url=res.source_url,
                        external_kind=res.source_type,
                        external_locator=locator,
                        source_meta={
                            "external_url": res.source_url,
                            "external_type": res.source_type,
                            "external_row_index": expanded_idx,
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
                        },
                    )
                )
                built += 1
            if len(out.units) >= max_units:
                break

        out.statuses.append(f"• {url} -> {res.source_type}, items={built}")

    return out


def _compose_text(base_text: str, external_text: str, url: str, kind: str, row_index: int | None) -> str:
    row_line = f"Row: {row_index}\n" if row_index is not None else ""
    ext_part = (
        f"External source kind: {kind}\n"
        f"External source URL: {url}\n"
        f"{row_line}\n"
        f"{external_text.strip()}"
    ).strip()
    if not (base_text or "").strip():
        return ext_part
    return f"{base_text.strip()}\n\n---\n{ext_part}"


def _expand_item_text(source_type: str, text: str, row_index: int | None) -> list[tuple[str, int | None]]:
    """
    Если документ содержит много отдельных бенч-строк, разбиваем на отдельные ingest units.
    Для таблиц (sheets/csv/xlsx) не применяется: они уже row-wise.
    """
    if source_type in ("google_sheet", "direct_csv", "direct_xlsx"):
        return []

    candidate_lines = split_line_wise_bench_items(text)
    if not candidate_lines:
        return []

    out: list[tuple[str, int | None]] = []
    for local_i, v in candidate_lines:
        idx = (row_index + local_i - 1) if row_index is not None else local_i
        out.append((v, idx))
    return out
