from __future__ import annotations

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from app.services.app_settings import OWN_BENCH_SOURCE_URL_KEY, PARTNER_COMPANIES_SOURCE_URL_KEY, get_effective_setting, get_setting
from app.services.audit_log import write_audit_event
from app.use_cases.dev_seed import seed_demo_data
from app.use_cases.jobs import enqueue_own_bench_sync, enqueue_partner_companies_sync, get_job_counts
from app.use_cases.own_bench import get_sync_status
from app.use_cases.source_trace import list_recent_imports, list_recent_sources
from backend.app.api.schemas import AdminOverviewResponse, ImportJobAcceptedResponse
from backend.app.api.deps import get_repo
from backend.app.db.session import get_engine, ping_db

router = APIRouter()


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
