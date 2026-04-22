from fastapi import APIRouter, HTTPException, Query

from app.services.audit_log import write_audit_event
from app.use_cases.jobs import get_job, list_jobs, retry_job
from backend.app.api.schemas import JobListItemResponse, JobListResponse, JobRetryResponse, JobStatusResponse
from backend.app.db.session import get_engine

router = APIRouter()


@router.get("", response_model=JobListResponse)
def list_jobs_endpoint(
    kind: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
) -> JobListResponse:
    items = list_jobs(get_engine(), kind=kind, status=status, limit=limit)
    return JobListResponse(
        items=[
            JobListItemResponse(
                job_id=job.id,
                kind=job.kind,
                status=job.status,
                submitted_at=job.created_at,
                started_at=job.started_at,
                finished_at=job.finished_at,
                error=job.error,
                result=job.result,
                attempts=job.attempts,
                max_attempts=job.max_attempts,
                available_at=job.available_at,
            )
            for job in items
        ],
        total=len(items),
        limit=limit,
        kind=kind,
        status=status,
    )


@router.get("/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str) -> JobStatusResponse:
    job = get_job(get_engine(), job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job_not_found")
    return JobStatusResponse(
        job_id=job.id,
        kind=job.kind,
        status=job.status,
        submitted_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
        result=job.result,
    )


@router.post("/{job_id}/retry", response_model=JobRetryResponse)
def retry_job_endpoint(job_id: str) -> JobRetryResponse:
    engine = get_engine()
    job = retry_job(engine, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job_not_found")
    write_audit_event(
        engine,
        event_type="job.retried",
        entity_type="job",
        entity_id=job.id,
        payload={"kind": job.kind},
    )
    return JobRetryResponse(job_id=job.id, status=job.status)
