from __future__ import annotations

from app.services.audit_log import write_audit_event
from app.use_cases.digest import build_daily_digest_payload
from app.use_cases.jobs import enqueue_digest
from backend.app.api.schemas import DigestPreviewResponse, DigestSectionItem, ImportJobAcceptedResponse
from backend.app.api.serializers import normalize_row
from backend.app.db.session import get_engine
from fastapi import APIRouter

router = APIRouter()


@router.get("/daily", response_model=DigestPreviewResponse)
def get_daily_digest_preview() -> DigestPreviewResponse:
    payload = build_daily_digest_payload(get_engine())
    return DigestPreviewResponse(
        window_start=payload["window_start"],
        window_end=payload["window_end"],
        new_vacancies=[DigestSectionItem(**normalize_row(row)) for row in payload["new_vacancies"]],
        updated_vacancies=[DigestSectionItem(**normalize_row(row)) for row in payload["updated_vacancies"]],
        new_specialists=[DigestSectionItem(**normalize_row(row)) for row in payload["new_specialists"]],
        updated_specialists=[DigestSectionItem(**normalize_row(row)) for row in payload["updated_specialists"]],
    )


@router.post("/enqueue", response_model=ImportJobAcceptedResponse)
def enqueue_digest_job() -> ImportJobAcceptedResponse:
    engine = get_engine()
    job = enqueue_digest(engine, reason="manual_api", deliver_to_telegram=False)
    write_audit_event(
        engine,
        event_type="digest.enqueued",
        entity_type="job",
        entity_id=job.id,
        payload={"reason": "manual_api"},
    )
    return ImportJobAcceptedResponse(job_id=job.id, status=job.status)
