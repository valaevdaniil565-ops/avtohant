from __future__ import annotations

import hashlib
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Literal

from app.db.repo import Repo, build_search_text
from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.integrations.mcp_source_fetcher.normalizers import (
    csv_bytes_to_items_with_summary,
    docx_bytes_to_text,
    html_to_text,
    pdf_bytes_to_text,
    xlsx_bytes_to_items_with_summary,
)
from app.integrations.mcp_source_fetcher.schemas import NormalizedItem
from app.llm.ollama_client import OllamaClient
from app.llm.pre_classifier import decide_hybrid_classification, normalize_short_bench_line, pre_classify_bench_line, split_line_wise_bench_items
from app.llm.prompts import CLASSIFICATION_SYSTEM_PROMPT_V2, SPECIALIST_EXTRACTION_PROMPT_V2, VACANCY_EXTRACTION_PROMPT_V2
from app.pipeline import build_fallback_specialist_item, build_fallback_vacancy_item, preprocess_for_llm
from app.services.availability import resolve_specialist_is_available
from app.services.link_extraction import extract_external_urls
from app.services.partner_companies import detect_partner_company_mention, load_partner_company_names, upsert_partner_company_mentions
from app.use_cases import extraction as extraction_use_cases
from app.use_cases import matching as matching_use_cases
from app.use_cases import source_trace as source_trace_use_cases
from app.use_cases.entities import _resolve_own_bench_url


ImportKind = Literal["text", "url", "file", "telegram"]
ForcedType = Literal["VACANCY", "BENCH"] | None
BenchOrigin = Literal["own", "partner"] | None


@dataclass
class ImportSummary:
    vacancies: int = 0
    specialists: int = 0
    skipped: int = 0
    hidden: int = 0
    errors: list[str] = field(default_factory=list)
    entity_refs: list[dict[str, str]] = field(default_factory=list)


@dataclass
class TraceContext:
    channel_id: int
    message_id: int
    chat_title: str | None
    sender_id: int | None
    sender_name: str | None
    message_url: str | None
    manager_name: str
    source_kind: str
    source_sender_name: str | None = None


def _synthetic_channel_id(job_id: str) -> int:
    return -1 * int(hashlib.sha256(f"web-import:{job_id}".encode("utf-8")).hexdigest()[:12], 16)


def _units_from_text(text_in: str) -> list[dict[str, Any]]:
    split_lines = split_line_wise_bench_items(text_in)
    if split_lines:
        return [
            {
                "text": text,
                "source_type": "web_text_split",
                "external_url": None,
                "external_kind": None,
                "external_locator": f"line:{line_idx}",
                "source_meta": {"line_index": line_idx, "parsing_mode": "line_wise_bench_list"},
            }
            for line_idx, text in split_lines
        ]
    return [
        {
            "text": text_in,
            "source_type": "web_text",
            "external_url": None,
            "external_kind": None,
            "external_locator": None,
            "source_meta": {},
        }
    ]


def _expand_item_text(source_type: str, text: str, row_index: int | None) -> list[tuple[str, int | None]]:
    if source_type in ("google_sheet", "direct_csv", "direct_xlsx"):
        return []
    candidate_lines = split_line_wise_bench_items(text)
    if not candidate_lines:
        return []
    out: list[tuple[str, int | None]] = []
    for local_i, value in candidate_lines:
        idx = (row_index + local_i - 1) if row_index is not None else local_i
        out.append((value, idx))
    return out


def _compose_external_text(base_text: str, external_text: str, url: str, kind: str, row_index: int | None) -> str:
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


def _units_from_url(url: str, client: MCPSourceFetcherClient) -> tuple[list[dict[str, Any]], list[str]]:
    res = client.fetch_url(url)
    if not res.ok:
        raise RuntimeError(res.error or "URL import failed")
    units: list[dict[str, Any]] = []
    statuses: list[str] = []
    built = 0
    for item in res.items:
        expanded = _expand_item_text(res.source_type, item.text, item.row_index)
        if not expanded:
            expanded = [(item.text, item.row_index)]
        for expanded_text, expanded_idx in expanded:
            built += 1
            units.append(
                {
                    "text": _compose_external_text("", expanded_text, res.source_url, res.source_type, expanded_idx),
                    "source_type": "web_url",
                    "external_url": res.source_url,
                    "external_kind": res.source_type,
                    "external_locator": f"row:{expanded_idx}" if expanded_idx is not None else None,
                    "source_meta": {
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
                }
            )
    statuses.append(f"{url} -> {res.source_type}, items={built}")
    return units, statuses


def _normalize_file_items(data: bytes, *, file_name: str, mime_type: str, source_ref: str) -> tuple[list[NormalizedItem], dict[str, Any] | None]:
    name = file_name.lower()
    if mime_type.startswith("text/csv") or name.endswith(".csv"):
        return csv_bytes_to_items_with_summary(data, source_ref)
    if "spreadsheetml.sheet" in mime_type or name.endswith(".xlsx"):
        return xlsx_bytes_to_items_with_summary(data, source_ref)
    if "wordprocessingml.document" in mime_type or name.endswith(".docx"):
        txt = docx_bytes_to_text(data)
        return ([NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []), None
    if "application/pdf" in mime_type or name.endswith(".pdf"):
        txt = pdf_bytes_to_text(data)
        return ([NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []), None
    if mime_type.startswith("text/plain") or name.endswith(".txt"):
        txt = data.decode("utf-8", errors="replace").strip()
        return ([NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []), None
    if mime_type.startswith("text/html") or name.endswith(".html") or name.endswith(".htm"):
        txt = html_to_text(data.decode("utf-8", errors="replace"))
        return ([NormalizedItem(text=txt, metadata={"source_url": source_ref})] if txt else []), None
    raise RuntimeError(f"Unsupported file type: {file_name}")


def _units_from_file(data: bytes, *, file_name: str, mime_type: str) -> list[dict[str, Any]]:
    source_ref = file_name
    items, _ = _normalize_file_items(data, file_name=file_name, mime_type=mime_type, source_ref=source_ref)
    units: list[dict[str, Any]] = []
    for item in items:
        locator = f"row:{item.row_index}" if item.row_index is not None else None
        units.append(
            {
                "text": item.text,
                "source_type": "web_file",
                "external_url": None,
                "external_kind": "uploaded_file",
                "external_locator": locator,
                "source_meta": {
                    "entity_hint": (item.metadata or {}).get("entity_hint"),
                    "structured_fields": (item.metadata or {}).get("structured_fields"),
                    "sheet_name": (item.metadata or {}).get("sheet_name") or (item.metadata or {}).get("table_name"),
                    "table_name": (item.metadata or {}).get("table_name"),
                    "table_index": (item.metadata or {}).get("table_index"),
                    "upload_filename": file_name,
                    "upload_mime_type": mime_type,
                },
            }
        )
    return units or _units_from_text(data.decode("utf-8", errors="replace"))


def _build_telegram_units(
    *,
    raw_text: str,
    attachment_path: str | None,
    attachment_name: str | None,
    attachment_mime_type: str | None,
    source_fetcher: MCPSourceFetcherClient | None,
) -> tuple[list[dict[str, Any]], list[str]]:
    statuses: list[str] = []
    external_units: list[dict[str, Any]] = []

    for url in extract_external_urls(raw_text):
        try:
            units, unit_statuses = _units_from_url(url, source_fetcher) if source_fetcher is not None else ([], [f"{url} -> source fetcher unavailable"])
        except Exception as exc:
            units, unit_statuses = [], [f"{url} -> {type(exc).__name__}: {exc}"]
        for unit in units:
            unit["source_type"] = "external_link"
        external_units.extend(units)
        statuses.extend(unit_statuses)

    if external_units:
        return external_units, statuses

    path = str(attachment_path or "").strip()
    if path:
        data = Path(path).read_bytes()
        units = _units_from_file(
            data,
            file_name=(attachment_name or Path(path).name),
            mime_type=(attachment_mime_type or "application/octet-stream"),
        )
        for unit in units:
            unit["source_type"] = "telegram_attachment"
            unit["external_kind"] = str(unit.get("external_kind") or "telegram_attachment")
        return units, statuses

    split_lines = split_line_wise_bench_items(raw_text)
    if split_lines:
        return (
            [
                {
                    "text": text,
                    "source_type": "telegram_line_split",
                    "external_url": None,
                    "external_kind": None,
                    "external_locator": f"line:{line_idx}",
                    "source_meta": {"line_index": line_idx, "parsing_mode": "line_wise_bench_list"},
                }
                for line_idx, text in split_lines
            ],
            statuses,
        )

    return (
        [
            {
                "text": raw_text,
                "source_type": "telegram_message",
                "external_url": None,
                "external_kind": None,
                "external_locator": None,
                "source_meta": {},
            }
        ],
        statuses,
    )


def _process_units(
    job_id: str,
    kind: ImportKind,
    *,
    engine,
    repo: Repo,
    ollama: OllamaClient,
    units: list[dict[str, Any]],
    forced_type: ForcedType = None,
    bench_origin: BenchOrigin = None,
    trace_context: TraceContext | None = None,
) -> ImportSummary:
    summary = ImportSummary()
    partner_names = load_partner_company_names(engine)
    own_bench_url = _resolve_own_bench_url(engine)
    synthetic_channel = trace_context.channel_id if trace_context else _synthetic_channel_id(job_id)
    base_message_id = trace_context.message_id if trace_context else 1
    chat_title = trace_context.chat_title if trace_context else "web_api"
    sender_id = trace_context.sender_id if trace_context else None
    sender_name = trace_context.sender_name if trace_context else "web_api"
    canonical_url = trace_context.message_url if trace_context else None
    manager_name = trace_context.manager_name if trace_context else "web_api"
    trace_source_kind = trace_context.source_kind if trace_context else None
    trace_source_sender_name = trace_context.source_sender_name if trace_context else None
    for unit_idx, unit in enumerate(units, start=1):
        raw_unit_text = str(unit["text"])
        normalized_unit_text = normalize_short_bench_line(raw_unit_text)
        pre_rule = pre_classify_bench_line(normalized_unit_text)
        partner_hit = detect_partner_company_mention(normalized_unit_text, partner_names)
        unit_entity_hint = str((unit.get("source_meta") or {}).get("entity_hint") or "").strip().upper() or None
        structured_fields = (unit.get("source_meta") or {}).get("structured_fields")
        if not isinstance(structured_fields, dict):
            structured_fields = None

        local_forced = forced_type or unit_entity_hint
        pre = preprocess_for_llm(normalized_unit_text, kind=local_forced)
        clean_text = pre.text
        decision = decide_hybrid_classification(pre_rule, forced_type=local_forced)
        llm_result = None
        classifier_source = decision.source
        if decision.needs_llm:
            llm_result = (ollama.generate(CLASSIFICATION_SYSTEM_PROMPT_V2, clean_text, temperature=0.0, max_tokens=32) or "").strip().upper()
            decision = decide_hybrid_classification(pre_rule, llm_label=llm_result)
            if partner_hit and decision.kind == "OTHER" and not pre_rule.is_confident:
                decision.kind = "VACANCY"
                decision.source = "partner_bias"
        mtype = decision.kind
        classifier_source = decision.source
        if mtype == "OTHER":
            summary.skipped += 1
            continue

        source_kind = trace_source_kind or ("file" if unit.get("external_kind") == "uploaded_file" else ("file" if unit.get("external_url") else "web_api"))
        common_source_meta = {
            **source_trace_use_cases.build_source_meta(
                base_meta=unit.get("source_meta"),
                manager_name=manager_name,
                canonical_url=canonical_url,
                external_url=unit.get("external_url"),
                external_locator=unit.get("external_locator"),
                source_kind=source_kind,
                source_sender_name=trace_source_sender_name,
            ),
            "classifier_source": classifier_source,
            "pre_classifier_confidence": pre_rule.confidence,
            "normalized_text": normalized_unit_text[:2000],
            "job_id": job_id,
            "import_kind": kind,
        }

        if mtype in ("VACANCY", "VACANCY_LIST"):
            if structured_fields and unit_entity_hint == "VACANCY":
                items = [extraction_use_cases.build_structured_vacancy_item(structured_fields, raw_unit_text)]
            else:
                data = extraction_use_cases.safe_json_loads(
                    ollama.generate(VACANCY_EXTRACTION_PROMPT_V2, clean_text, temperature=0.0, max_tokens=1600)
                )
                items = (data or {}).get("items", [])
            if not items:
                items = [build_fallback_vacancy_item(pre)]
            for idx, item in enumerate(items, start=1):
                is_closed = bool(item.get("is_closed"))
                status = "closed" if is_closed else "active"
                search_text = build_search_text(item)
                embedding = ollama.embed(search_text)
                vacancy_id = repo.upsert_vacancy(item, raw_unit_text, embedding, status)
                company = str(item.get("company") or "").strip()
                if company:
                    upsert_partner_company_mentions(engine, {company: 1}, source_url=unit.get("external_url"))
                source_trace_use_cases.insert_source(
                    engine,
                    entity_type="vacancy",
                    entity_id=vacancy_id,
                    channel_id=synthetic_channel,
                    message_id=base_message_id,
                    chat_title=chat_title,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    message_url=canonical_url,
                    raw_text=raw_unit_text,
                    idx=(unit_idx * 1000 + idx),
                    source_type=str(unit.get("source_type") or "web_api"),
                    external_url=unit.get("external_url"),
                    external_kind=unit.get("external_kind"),
                    external_locator=unit.get("external_locator"),
                    source_meta=common_source_meta,
                )
                if not is_closed:
                    hits = matching_use_cases.search_specialists(
                        engine,
                        embedding,
                        search_text,
                        20,
                        own_bench_url=own_bench_url,
                        vector_dim=768,
                        vector_str_fn=lambda values: "[" + ",".join(f"{float(v):.8f}" for v in values) + "]",
                    )
                    ranked_hits, _ = matching_use_cases.rank_specialist_hits(item, hits)
                    matching_use_cases.upsert_matches(engine, vacancy_id, ranked_hits)
                summary.vacancies += 1
                summary.entity_refs.append({"entity_type": "vacancy", "entity_id": vacancy_id})

        if mtype in ("BENCH", "BENCH_LIST"):
            if structured_fields and unit_entity_hint == "BENCH":
                items = [extraction_use_cases.build_structured_specialist_item(structured_fields, raw_unit_text)]
            else:
                data = extraction_use_cases.safe_json_loads(
                    ollama.generate(SPECIALIST_EXTRACTION_PROMPT_V2, clean_text, temperature=0.0, max_tokens=1600)
                )
                items = (data or {}).get("items", [])
            if not items:
                items = [build_fallback_specialist_item(pre)]
            for idx, item in enumerate(items, start=1):
                is_available = resolve_specialist_is_available(item, raw_unit_text)
                item["is_available"] = is_available
                if bench_origin == "own":
                    item["is_internal"] = True
                elif bench_origin == "partner":
                    item["is_internal"] = False
                status = "active" if is_available else "hired"
                search_text = build_search_text(item)
                embedding = ollama.embed(search_text)
                specialist_id = repo.upsert_specialist(item, raw_unit_text, embedding, status)
                source_trace_use_cases.insert_source(
                    engine,
                    entity_type="specialist",
                    entity_id=specialist_id,
                    channel_id=synthetic_channel,
                    message_id=base_message_id,
                    chat_title=chat_title,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    message_url=canonical_url,
                    raw_text=raw_unit_text,
                    idx=(unit_idx * 1000 + idx),
                    source_type=str(unit.get("source_type") or "web_api"),
                    external_url=unit.get("external_url"),
                    external_kind=unit.get("external_kind"),
                    external_locator=unit.get("external_locator"),
                    source_meta=common_source_meta,
                )
                if is_available:
                    hits = matching_use_cases.search_vacancies(
                        engine,
                        embedding,
                        search_text,
                        20,
                        vector_dim=768,
                        vector_str_fn=lambda values: "[" + ",".join(f"{float(v):.8f}" for v in values) + "]",
                    )
                    ranked_hits = matching_use_cases.rank_vacancy_hits(item, hits)
                    matching_use_cases.upsert_matches_reverse(engine, specialist_id, ranked_hits)
                summary.specialists += 1
                summary.entity_refs.append({"entity_type": "specialist", "entity_id": specialist_id})
    return summary


def process_text_import(
    job_id: str,
    *,
    engine,
    repo: Repo,
    ollama: OllamaClient,
    text: str,
    forced_type: ForcedType = None,
    bench_origin: BenchOrigin = None,
) -> ImportSummary:
    return _process_units(
        job_id,
        "text",
        engine=engine,
        repo=repo,
        ollama=ollama,
        units=_units_from_text(text),
        forced_type=forced_type,
        bench_origin=bench_origin,
    )


def process_url_import(
    job_id: str,
    *,
    engine,
    repo: Repo,
    ollama: OllamaClient,
    source_fetcher: MCPSourceFetcherClient,
    url: str,
    forced_type: ForcedType = None,
    bench_origin: BenchOrigin = None,
) -> ImportSummary:
    units, statuses = _units_from_url(url, source_fetcher)
    summary = _process_units(
        job_id,
        "url",
        engine=engine,
        repo=repo,
        ollama=ollama,
        units=units,
        forced_type=forced_type,
        bench_origin=bench_origin,
    )
    summary.errors.extend(statuses)
    return summary


def process_file_import(
    job_id: str,
    *,
    engine,
    repo: Repo,
    ollama: OllamaClient,
    file_name: str,
    mime_type: str,
    data: bytes,
    forced_type: ForcedType = None,
    bench_origin: BenchOrigin = None,
) -> ImportSummary:
    return _process_units(
        job_id,
        "file",
        engine=engine,
        repo=repo,
        ollama=ollama,
        units=_units_from_file(data, file_name=file_name, mime_type=mime_type),
        forced_type=forced_type,
        bench_origin=bench_origin,
    )


def process_telegram_import(
    job_id: str,
    *,
    engine,
    repo: Repo,
    ollama: OllamaClient,
    source_fetcher: MCPSourceFetcherClient | None,
    payload: dict[str, Any],
) -> ImportSummary:
    raw_text = str(payload.get("raw_text") or "").strip()
    attachment_path = str(payload.get("attachment_path") or "").strip() or None
    if not raw_text and not attachment_path:
        summary = ImportSummary()
        summary.skipped += 1
        summary.errors.append("Empty telegram payload: no raw_text and no attachment")
        return summary

    units, statuses = _build_telegram_units(
        raw_text=raw_text,
        attachment_path=attachment_path,
        attachment_name=(str(payload.get("attachment_name") or "").strip() or None),
        attachment_mime_type=(str(payload.get("attachment_mime_type") or "").strip() or None),
        source_fetcher=source_fetcher,
    )
    trace_context = TraceContext(
        channel_id=int(payload.get("channel_id") or 0),
        message_id=int(payload.get("message_id") or 0),
        chat_title=(str(payload.get("chat_title") or "").strip() or None),
        sender_id=(int(payload["sender_id"]) if payload.get("sender_id") not in (None, "") else None),
        sender_name=(str(payload.get("sender_name") or "").strip() or None),
        message_url=(str(payload.get("message_url") or "").strip() or None),
        manager_name=(str(payload.get("chat_title") or payload.get("sender_name") or "telegram").strip() or "telegram"),
        source_kind=(str(payload.get("source_kind") or "telegram_message").strip() or "telegram_message"),
        source_sender_name=(str(payload.get("source_sender_name") or "").strip() or None),
    )
    summary = _process_units(
        job_id,
        "telegram",
        engine=engine,
        repo=repo,
        ollama=ollama,
        units=units,
        forced_type=(str(payload.get("forced_type")) if payload.get("forced_type") else None),
        trace_context=trace_context,
    )
    summary.errors.extend(statuses)
    return summary
