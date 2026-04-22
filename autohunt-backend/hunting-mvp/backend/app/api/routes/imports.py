from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status

from app.use_cases import imports as import_use_cases
from app.services.audit_log import write_audit_event
from app.use_cases.jobs import enqueue_file_import, enqueue_text_import, enqueue_url_import, get_job, list_jobs
from app.use_cases.source_trace import list_recent_imports
from backend.app.api.deps import get_ollama_client, get_repo, get_source_fetcher
from backend.app.api.schemas import ImportImmediateResponse, ImportJobAcceptedResponse, ImportJobListResponse, ImportJobStatusResponse, ImportSummaryResponse, TextImportRequest, UrlImportRequest
from backend.app.db.session import get_engine

router = APIRouter()


def _summary_response(summary: import_use_cases.ImportSummary) -> ImportImmediateResponse:
    return ImportImmediateResponse(
        status="ok",
        summary=ImportSummaryResponse(
            vacancies=summary.vacancies,
            specialists=summary.specialists,
            skipped=summary.skipped,
            hidden=summary.hidden,
            errors=list(summary.errors),
            entity_refs=list(summary.entity_refs),
        ),
    )


@router.get("")
def list_imports() -> dict[str, object]:
    return {
        "status": "ok",
        "items": list_recent_imports(get_engine(), limit=20),
    }


@router.get("/recent-jobs", response_model=ImportJobListResponse)
def list_recent_import_jobs(limit: int = 10) -> ImportJobListResponse:
    effective_limit = max(1, min(limit, 50))
    jobs = [
        job
        for job in list_jobs(get_engine(), limit=max(20, effective_limit * 5))
        if job.kind in {"import_text", "import_url", "import_file"}
    ][:effective_limit]
    return ImportJobListResponse(
        items=[
            ImportJobStatusResponse(
                job_id=job.id,
                kind=job.kind,
                status=job.status,
                submitted_at=job.created_at,
                started_at=job.started_at,
                finished_at=job.finished_at,
                error=job.error,
                summary=ImportSummaryResponse(**(job.result or {})),
            )
            for job in jobs
        ]
    )


@router.get("/{job_id}", response_model=ImportJobStatusResponse)
def get_import_job(job_id: str) -> ImportJobStatusResponse:
    job = get_job(get_engine(), job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job_not_found")
    return ImportJobStatusResponse(
        job_id=job.id,
        kind=job.kind,
        status=job.status,
        submitted_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
        summary=ImportSummaryResponse(**(job.result or {})),
    )


@router.post("/text", response_model=ImportJobAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
def import_text(payload: TextImportRequest) -> ImportJobAcceptedResponse:
    engine = get_engine()
    job = enqueue_text_import(engine, text_value=payload.text, forced_type=payload.forced_type, bench_origin=payload.bench_origin)
    write_audit_event(
        engine,
        event_type="import.text.enqueued",
        entity_type="job",
        entity_id=job.id,
        payload={"forced_type": payload.forced_type, "bench_origin": payload.bench_origin},
    )
    return ImportJobAcceptedResponse(job_id=job.id, status=job.status)


@router.post("/text-sync", response_model=ImportImmediateResponse)
def import_text_sync(payload: TextImportRequest) -> ImportImmediateResponse:
    engine = get_engine()
    summary = import_use_cases.process_text_import(
        "manual_text_sync",
        engine=engine,
        repo=get_repo(),
        ollama=get_ollama_client(),
        text=payload.text,
        forced_type=payload.forced_type,
        bench_origin=payload.bench_origin,
    )
    write_audit_event(
        engine,
        event_type="import.text.sync",
        entity_type="system",
        payload={"forced_type": payload.forced_type, "bench_origin": payload.bench_origin, "summary": _summary_response(summary).summary.model_dump()},
    )
    return _summary_response(summary)


@router.post("/url", response_model=ImportJobAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
def import_url(payload: UrlImportRequest) -> ImportJobAcceptedResponse:
    engine = get_engine()
    job = enqueue_url_import(engine, url=payload.url, forced_type=payload.forced_type, bench_origin=payload.bench_origin)
    write_audit_event(
        engine,
        event_type="import.url.enqueued",
        entity_type="job",
        entity_id=job.id,
        payload={"url": payload.url, "forced_type": payload.forced_type, "bench_origin": payload.bench_origin},
    )
    return ImportJobAcceptedResponse(job_id=job.id, status=job.status)


@router.post("/url-sync", response_model=ImportImmediateResponse)
def import_url_sync(payload: UrlImportRequest) -> ImportImmediateResponse:
    engine = get_engine()
    summary = import_use_cases.process_url_import(
        "manual_url_sync",
        engine=engine,
        repo=get_repo(),
        ollama=get_ollama_client(),
        source_fetcher=get_source_fetcher(),
        url=payload.url,
        forced_type=payload.forced_type,
        bench_origin=payload.bench_origin,
    )
    write_audit_event(
        engine,
        event_type="import.url.sync",
        entity_type="system",
        payload={"url": payload.url, "forced_type": payload.forced_type, "bench_origin": payload.bench_origin, "summary": _summary_response(summary).summary.model_dump()},
    )
    return _summary_response(summary)


@router.post("/file", response_model=ImportJobAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def import_file(
    file: UploadFile = File(...),
    forced_type: str | None = Form(default=None),
    bench_origin: str | None = Form(default=None),
) -> ImportJobAcceptedResponse:
    data = await file.read()
    engine = get_engine()
    job = enqueue_file_import(
        engine,
        filename=str(file.filename or "upload.bin"),
        mime_type=str(file.content_type or "application/octet-stream"),
        data=data,
        forced_type=(forced_type if forced_type in {"VACANCY", "BENCH"} else None),
        bench_origin=(bench_origin if bench_origin in {"own", "partner"} else None),
    )
    write_audit_event(
        engine,
        event_type="import.file.enqueued",
        entity_type="job",
        entity_id=job.id,
        payload={"filename": str(file.filename or "upload.bin"), "forced_type": forced_type, "bench_origin": bench_origin},
    )
    return ImportJobAcceptedResponse(job_id=job.id, status=job.status)


@router.post("/file-sync", response_model=ImportImmediateResponse)
async def import_file_sync(
    file: UploadFile = File(...),
    forced_type: str | None = Form(default=None),
    bench_origin: str | None = Form(default=None),
) -> ImportImmediateResponse:
    data = await file.read()
    engine = get_engine()
    summary = import_use_cases.process_file_import(
        "manual_file_sync",
        engine=engine,
        repo=get_repo(),
        ollama=get_ollama_client(),
        file_name=str(file.filename or "upload.bin"),
        mime_type=str(file.content_type or "application/octet-stream"),
        data=data,
        forced_type=(forced_type if forced_type in {"VACANCY", "BENCH"} else None),
        bench_origin=(bench_origin if bench_origin in {"own", "partner"} else None),
    )
    write_audit_event(
        engine,
        event_type="import.file.sync",
        entity_type="system",
        payload={
            "filename": str(file.filename or "upload.bin"),
            "forced_type": forced_type,
            "bench_origin": bench_origin,
            "summary": _summary_response(summary).summary.model_dump(),
        },
    )
    return _summary_response(summary)
