from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from app.db.repo import build_search_text
from app.services.app_settings import OWN_BENCH_SOURCE_URL_KEY, PARTNER_COMPANIES_SOURCE_URL_KEY, get_effective_setting, get_setting
from app.services.audit_log import write_audit_event
from app.use_cases import imports as import_use_cases
from app.use_cases import matching as matching_use_cases
from app.use_cases.dev_seed import seed_demo_data
from app.use_cases.jobs import enqueue_own_bench_sync, enqueue_partner_companies_sync, get_job_counts
from app.use_cases.entities import list_vacancies
from app.use_cases.own_bench import get_sync_status
from app.use_cases.source_trace import list_recent_imports, list_recent_sources
from backend.app.api.schemas import (
    AdminOverviewResponse,
    ImportEntityRef,
    ImportJobAcceptedResponse,
    MatchingRebuildResponse,
    TelegramChannelItem,
    TelegramChannelListResponse,
    TelegramChannelUpsertRequest,
    TelegramImportResponse,
)
from backend.app.api.deps import get_ollama_client, get_repo, get_source_fetcher
from backend.app.db.session import get_engine, ping_db

router = APIRouter()


def _parse_embedding_text(value: object) -> list[float]:
    raw = str(value or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [float(item) for item in parsed]
    except Exception:
        return []
    return []


def _load_counts(engine) -> dict[str, int]:
    query = text(
        """
        SELECT
          (SELECT COUNT(*) FROM vacancies) AS vacancies,
          (SELECT COUNT(*) FROM specialists) AS specialists,
          (SELECT COUNT(*) FROM matches) AS matches,
          (SELECT COUNT(*) FROM sources) AS sources
        """
    )
    with engine.begin() as connection:
        row = connection.execute(query).mappings().first()
    return {
        "vacancies": int(row["vacancies"] or 0),
        "specialists": int(row["specialists"] or 0),
        "matches": int(row["matches"] or 0),
        "sources": int(row["sources"] or 0),
    }


@router.get("/overview", response_model=AdminOverviewResponse)
def get_admin_overview() -> AdminOverviewResponse:
    engine = get_engine()
    db_ok = ping_db()
    return AdminOverviewResponse(
        status="ok" if db_ok else "degraded",
        database="ok" if db_ok else "down",
        counts=_load_counts(engine),
        jobs=get_job_counts(engine),
        own_bench=get_sync_status(engine),
        recent_imports=list_recent_imports(engine, limit=10),
        recent_sources=list_recent_sources(engine, limit=10),
    )


@router.post("/jobs/own-bench-sync", response_model=ImportJobAcceptedResponse)
def run_own_bench_sync() -> ImportJobAcceptedResponse:
    engine = get_engine()
    source_url = str(get_setting(engine, OWN_BENCH_SOURCE_URL_KEY) or "").strip()
    if not source_url:
        raise HTTPException(status_code=400, detail="own_bench_source_url_not_configured")
    job = enqueue_own_bench_sync(engine, source_url=source_url, reason="manual_api")
    write_audit_event(
        engine,
        event_type="own_bench_sync.enqueued",
        entity_type="job",
        entity_id=job.id,
        payload={"source_url": source_url},
    )
    return ImportJobAcceptedResponse(job_id=job.id, status=job.status)


@router.post("/jobs/partner-sync", response_model=ImportJobAcceptedResponse)
def run_partner_sync() -> ImportJobAcceptedResponse:
    engine = get_engine()
    source_url = get_effective_setting(engine, PARTNER_COMPANIES_SOURCE_URL_KEY, "PARTNER_COMPANIES_SOURCE_URL", "")
    if not source_url:
        raise HTTPException(status_code=400, detail="partner_companies_source_url_not_configured")
    job = enqueue_partner_companies_sync(engine, source_url=source_url, reason="manual_api")
    write_audit_event(
        engine,
        event_type="partner_sync.enqueued",
        entity_type="job",
        entity_id=job.id,
        payload={"source_url": source_url},
    )
    return ImportJobAcceptedResponse(job_id=job.id, status=job.status)


@router.post("/dev/seed-demo")
def seed_demo() -> dict[str, object]:
    engine = get_engine()
    result = seed_demo_data(engine, get_repo())
    write_audit_event(
        engine,
        event_type="dev.seed_demo",
        entity_type="system",
        payload=result,
    )
    return {"status": "ok", "seeded": result}


@router.get("/telegram/channels", response_model=TelegramChannelListResponse)
def list_telegram_channels() -> TelegramChannelListResponse:
    engine = get_engine()
    query = text(
        """
        SELECT telegram_id, title, username, COALESCE(source_kind, 'chat') AS source_kind,
               is_active, COALESCE(last_message_id, 0) AS last_message_id, added_at, updated_at
        FROM channels
        ORDER BY added_at ASC, telegram_id ASC
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query).mappings().all()
    return TelegramChannelListResponse(items=[TelegramChannelItem(**dict(row)) for row in rows])


@router.post("/telegram/channels", response_model=TelegramChannelItem)
def upsert_telegram_channel(payload: TelegramChannelUpsertRequest) -> TelegramChannelItem:
    engine = get_engine()
    query = text(
        """
        INSERT INTO channels(telegram_id, title, username, source_kind, is_active, last_message_id)
        VALUES (:telegram_id, :title, :username, :source_kind, :is_active, 0)
        ON CONFLICT (telegram_id) DO UPDATE SET
          title = EXCLUDED.title,
          username = EXCLUDED.username,
          source_kind = EXCLUDED.source_kind,
          is_active = EXCLUDED.is_active,
          updated_at = NOW()
        RETURNING telegram_id, title, username, COALESCE(source_kind, 'chat') AS source_kind,
                  is_active, COALESCE(last_message_id, 0) AS last_message_id, added_at, updated_at
        """
    )
    with engine.begin() as connection:
        row = connection.execute(query, payload.model_dump()).mappings().one()
    write_audit_event(
        engine,
        event_type="telegram.channel.upsert",
        entity_type="telegram_channel",
        entity_id=str(payload.telegram_id),
        payload=payload.model_dump(),
    )
    return TelegramChannelItem(**dict(row))


@router.post("/telegram/import-vacancies", response_model=TelegramImportResponse)
def import_telegram_vacancies(limit: int = 200) -> TelegramImportResponse:
    engine = get_engine()
    repo = get_repo()
    ollama = get_ollama_client()
    source_fetcher = get_source_fetcher()
    query = text(
        """
        SELECT
          raw.channel_id,
          raw.message_id,
          raw.chat_title,
          raw.sender_id,
          raw.sender_name,
          raw.message_url,
          raw.raw_text,
          raw.attachment_path,
          raw.attachment_type,
          ch.source_kind
        FROM tg_messages_raw raw
        JOIN channels ch ON ch.telegram_id = raw.channel_id
        WHERE ch.is_active = TRUE
          AND COALESCE(ch.source_kind, 'chat') = 'vacancy'
          AND NOT EXISTS (
            SELECT 1
            FROM sources s
            WHERE s.channel_id = raw.channel_id
              AND s.message_id = raw.message_id
              AND s.entity_type = 'vacancy'
          )
        ORDER BY raw.message_date DESC NULLS LAST, raw.created_at DESC
        LIMIT :limit
        """
    )
    with engine.begin() as connection:
        rows = connection.execute(query, {"limit": max(1, min(limit, 1000))}).mappings().all()

    imported_vacancies = 0
    skipped = 0
    hidden = 0
    errors: list[str] = []
    entity_refs: list[ImportEntityRef] = []

    for row in rows:
        summary = import_use_cases.process_telegram_import(
            f"telegram_sync_{uuid.uuid4().hex[:12]}",
            engine=engine,
            repo=repo,
            ollama=ollama,
            source_fetcher=source_fetcher,
            payload={
                "channel_id": row["channel_id"],
                "message_id": row["message_id"],
                "chat_title": row["chat_title"],
                "sender_id": row["sender_id"],
                "sender_name": row["sender_name"],
                "message_url": row["message_url"],
                "raw_text": row["raw_text"],
                "attachment_path": row["attachment_path"],
                "attachment_mime_type": row["attachment_type"],
                "source_kind": row["source_kind"],
            },
        )
        imported_vacancies += summary.vacancies
        skipped += summary.skipped
        hidden += summary.hidden
        errors.extend(summary.errors)
        entity_refs.extend(ImportEntityRef(**ref) for ref in summary.entity_refs if ref.get("entity_type") == "vacancy")

    write_audit_event(
        engine,
        event_type="telegram.vacancies.import_sync",
        entity_type="system",
        payload={
            "selected_messages": len(rows),
            "imported_vacancies": imported_vacancies,
            "skipped": skipped,
            "hidden": hidden,
            "errors": errors[:20],
        },
    )
    return TelegramImportResponse(
        status="ok",
        selected_messages=len(rows),
        imported_vacancies=imported_vacancies,
        skipped=skipped,
        hidden=hidden,
        errors=errors,
        entity_refs=entity_refs,
    )


@router.post("/matching/rebuild", response_model=MatchingRebuildResponse)
def rebuild_matching(limit: int = 200) -> MatchingRebuildResponse:
    engine = get_engine()
    ollama = get_ollama_client()
    processed_vacancies = 0
    updated_matches = 0
    for vacancy in list_vacancies(engine, limit=max(1, min(limit, 1000)), offset=0):
        query_emb = vacancy.get("embedding_text")
        if not query_emb and ollama is None:
            continue
        search_text = build_search_text(vacancy)
        query_embedding = _parse_embedding_text(query_emb) if query_emb else ollama.embed(search_text)
        hits = matching_use_cases.search_specialists(
            engine,
            query_embedding,
            search_text,
            20,
            own_bench_url=str(get_setting(engine, OWN_BENCH_SOURCE_URL_KEY) or "").strip(),
            vector_dim=768,
            vector_str_fn=lambda values: "[" + ",".join(f"{float(v):.8f}" for v in values) + "]",
        )
        ranked_hits, _ = matching_use_cases.rank_specialist_hits(vacancy, hits)
        matching_use_cases.upsert_matches(engine, str(vacancy["id"]), ranked_hits)
        processed_vacancies += 1
        updated_matches += len(ranked_hits)

    write_audit_event(
        engine,
        event_type="matching.rebuild",
        entity_type="system",
        payload={"processed_vacancies": processed_vacancies, "updated_matches": updated_matches},
    )
    return MatchingRebuildResponse(status="ok", processed_vacancies=processed_vacancies, updated_matches=updated_matches)
