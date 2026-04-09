from __future__ import annotations

import json
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from app.bots import views
from app.db.repo import Repo
from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.llm.ollama_client import OllamaClient
from app.services.app_settings import (
    DIGEST_DELIVERY_ENABLED_KEY,
    DIGEST_JOB_ENABLED_KEY,
    DIGEST_SCHEDULE_HOUR_KEY,
    DIGEST_SCHEDULE_MINUTE_KEY,
    OWN_BENCH_SOURCE_URL_KEY,
    PARTNER_COMPANIES_SOURCE_URL_KEY,
    get_effective_setting,
    get_setting,
)
from app.services.partner_companies import extract_partner_company_counts_from_sheet, upsert_partner_company_mentions
from app.use_cases import digest as digest_use_cases
from app.use_cases import imports as import_use_cases
from app.use_cases import own_bench as own_bench_use_cases

log = logging.getLogger(__name__)
MSK = ZoneInfo("Europe/Moscow")
_JOB_KINDS = {
    "import_text",
    "import_url",
    "import_file",
    "telegram_ingest",
    "own_bench_sync",
    "partner_companies_sync",
    "digest",
}


@dataclass
class JobRecord:
    id: str
    kind: str
    status: str
    payload: dict[str, Any]
    result: dict[str, Any]
    error: str | None
    dedupe_key: str | None
    attempts: int
    max_attempts: int
    available_at: datetime
    scheduled_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


def ensure_jobs_table(engine) -> None:
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS jobs (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          kind VARCHAR(64) NOT NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'queued',
          payload JSONB NOT NULL DEFAULT '{}'::jsonb,
          result JSONB NOT NULL DEFAULT '{}'::jsonb,
          error TEXT,
          dedupe_key VARCHAR(255),
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 3,
          available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          started_at TIMESTAMPTZ,
          finished_at TIMESTAMPTZ,
          locked_by VARCHAR(128),
          locked_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_jobs_status_available ON jobs(status, available_at)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_kind_created ON jobs(kind, created_at DESC)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedupe_key ON jobs(dedupe_key) WHERE dedupe_key IS NOT NULL",
    ]
    with engine.begin() as connection:
        for query in ddl:
            connection.execute(text(query))


def _normalize_payload(payload: dict[str, Any] | None) -> str:
    return json.dumps(payload or {}, ensure_ascii=False)


def _row_to_job(row) -> JobRecord:
    payload = row.get("payload") or {}
    result = row.get("result") or {}
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except Exception:
            result = {}
    return JobRecord(
        id=str(row["id"]),
        kind=str(row["kind"]),
        status=str(row["status"]),
        payload=dict(payload or {}),
        result=dict(result or {}),
        error=(str(row["error"]) if row.get("error") else None),
        dedupe_key=(str(row["dedupe_key"]) if row.get("dedupe_key") else None),
        attempts=int(row.get("attempts") or 0),
        max_attempts=int(row.get("max_attempts") or 0),
        available_at=row["available_at"],
        scheduled_at=row["scheduled_at"],
        started_at=row.get("started_at"),
        finished_at=row.get("finished_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def enqueue_job(
    engine,
    *,
    kind: str,
    payload: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
    max_attempts: int = 3,
    available_at: datetime | None = None,
) -> JobRecord:
    ensure_jobs_table(engine)
    if kind not in _JOB_KINDS:
        raise ValueError(f"Unsupported job kind: {kind}")
    available = available_at or datetime.now(timezone.utc)
    with engine.begin() as connection:
        if dedupe_key:
            existing = connection.execute(
                text(
                    """
                    SELECT *
                    FROM jobs
                    WHERE dedupe_key = :dedupe_key
                    LIMIT 1
                    """
                ),
                {"dedupe_key": dedupe_key},
            ).mappings().first()
            if existing:
                return _row_to_job(existing)
        row = connection.execute(
            text(
                """
                INSERT INTO jobs(kind, status, payload, result, dedupe_key, attempts, max_attempts, available_at, scheduled_at, created_at, updated_at)
                VALUES (:kind, 'queued', CAST(:payload AS jsonb), '{}'::jsonb, :dedupe_key, 0, :max_attempts, :available_at, :scheduled_at, NOW(), NOW())
                RETURNING *
                """
            ),
            {
                "kind": kind,
                "payload": _normalize_payload(payload),
                "dedupe_key": dedupe_key,
                "max_attempts": max_attempts,
                "available_at": available,
                "scheduled_at": available,
            },
        ).mappings().first()
    return _row_to_job(row)


def get_job(engine, job_id: str) -> JobRecord | None:
    ensure_jobs_table(engine)
    with engine.begin() as connection:
        row = connection.execute(text("SELECT * FROM jobs WHERE id = CAST(:job_id AS uuid)"), {"job_id": job_id}).mappings().first()
    return _row_to_job(row) if row else None


def list_jobs(engine, *, kind: str | None = None, status: str | None = None, limit: int = 50) -> list[JobRecord]:
    ensure_jobs_table(engine)
    query = text(
        """
        SELECT *
        FROM jobs
        WHERE (:kind IS NULL OR kind = :kind)
          AND (:status IS NULL OR status = :status)
        ORDER BY created_at DESC
        LIMIT :limit
        """
    )
    params = {"kind": kind, "status": status, "limit": limit}
    with engine.begin() as connection:
        rows = connection.execute(query, params).mappings().all()
    return [_row_to_job(row) for row in rows]


def retry_job(engine, job_id: str) -> JobRecord | None:
    ensure_jobs_table(engine)
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                UPDATE jobs
                SET status = 'queued',
                    error = NULL,
                    started_at = NULL,
                    finished_at = NULL,
                    locked_by = NULL,
                    locked_at = NULL,
                    available_at = NOW(),
                    updated_at = NOW()
                WHERE id = CAST(:job_id AS uuid)
                RETURNING *
                """
            ),
            {"job_id": job_id},
        ).mappings().first()
    return _row_to_job(row) if row else None


def get_job_counts(engine) -> dict[str, int]:
    ensure_jobs_table(engine)
    with engine.begin() as connection:
        rows = connection.execute(
            text(
                """
                SELECT status, COUNT(*) AS cnt
                FROM jobs
                GROUP BY status
                """
            )
        ).mappings().all()
    counts = {"queued": 0, "processing": 0, "completed": 0, "failed": 0}
    for row in rows:
        counts[str(row["status"])] = int(row["cnt"] or 0)
    return counts


def claim_next_job(engine, *, worker_id: str) -> JobRecord | None:
    ensure_jobs_table(engine)
    with engine.begin() as connection:
        row = connection.execute(
            text(
                """
                WITH next_job AS (
                  SELECT id
                  FROM jobs
                  WHERE status = 'queued'
                    AND available_at <= NOW()
                  ORDER BY available_at ASC, created_at ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE jobs j
                SET status = 'processing',
                    started_at = NOW(),
                    locked_by = :worker_id,
                    locked_at = NOW(),
                    updated_at = NOW()
                FROM next_job
                WHERE j.id = next_job.id
                RETURNING j.*
                """
            ),
            {"worker_id": worker_id},
        ).mappings().first()
    return _row_to_job(row) if row else None


def complete_job(engine, job_id: str, result: dict[str, Any]) -> None:
    ensure_jobs_table(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE jobs
                SET status = 'completed',
                    result = CAST(:result AS jsonb),
                    error = NULL,
                    finished_at = NOW(),
                    updated_at = NOW()
                WHERE id = CAST(:job_id AS uuid)
                """
            ),
            {"job_id": job_id, "result": _normalize_payload(result)},
        )


def fail_job(engine, job_id: str, *, error: str, retryable: bool = True) -> None:
    ensure_jobs_table(engine)
    with engine.begin() as connection:
        row = connection.execute(
            text("SELECT attempts, max_attempts FROM jobs WHERE id = CAST(:job_id AS uuid)"),
            {"job_id": job_id},
        ).mappings().first()
        if not row:
            return
        attempts = int(row["attempts"] or 0) + 1
        max_attempts = int(row["max_attempts"] or 0)
        should_retry = retryable and attempts < max_attempts
        backoff_seconds = min(300, int(math.pow(2, max(0, attempts - 1)) * 30))
        connection.execute(
            text(
                """
                UPDATE jobs
                SET status = :status,
                    attempts = :attempts,
                    error = :error,
                    available_at = CASE WHEN :retry = TRUE THEN NOW() + (:backoff || ' seconds')::interval ELSE available_at END,
                    finished_at = CASE WHEN :retry = TRUE THEN NULL ELSE NOW() END,
                    locked_by = NULL,
                    locked_at = NULL,
                    updated_at = NOW()
                WHERE id = CAST(:job_id AS uuid)
                """
            ),
            {
                "job_id": job_id,
                "status": "queued" if should_retry else "failed",
                "attempts": attempts,
                "error": error[:4000],
                "retry": should_retry,
                "backoff": backoff_seconds,
            },
        )


def _telegram_send_html(token: str, chat_id: int, html_text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": html_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=30,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Telegram send failed HTTP {response.status_code}: {response.text[:300]}")


def _job_result_from_summary(summary: import_use_cases.ImportSummary) -> dict[str, Any]:
    return {
        "vacancies": summary.vacancies,
        "specialists": summary.specialists,
        "skipped": summary.skipped,
        "hidden": summary.hidden,
        "errors": list(summary.errors),
        "entity_refs": list(summary.entity_refs),
    }


def _file_payload_path(job_id: str, filename: str) -> Path:
    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in filename)[:120] or "upload.bin"
    root = Path("storage") / "jobs"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{job_id}_{safe_name}"


def persist_import_upload(job_id: str, *, filename: str, data: bytes) -> str:
    path = _file_payload_path(job_id, filename)
    path.write_bytes(data)
    return str(path)


def _cleanup_import_upload(file_path: str | None) -> None:
    raw = str(file_path or "").strip()
    if not raw:
        return
    try:
        Path(raw).unlink(missing_ok=True)
    except Exception:
        log.warning("Failed to remove import upload file_path=%s", raw, exc_info=True)


def enqueue_text_import(engine, *, text_value: str, forced_type: str | None = None) -> JobRecord:
    return enqueue_job(
        engine,
        kind="import_text",
        payload={"text": text_value, "forced_type": forced_type},
        max_attempts=2,
    )


def enqueue_url_import(engine, *, url: str, forced_type: str | None = None) -> JobRecord:
    return enqueue_job(
        engine,
        kind="import_url",
        payload={"url": url, "forced_type": forced_type},
        max_attempts=2,
    )


def enqueue_file_import(engine, *, filename: str, mime_type: str, data: bytes, forced_type: str | None = None) -> JobRecord:
    job = enqueue_job(
        engine,
        kind="import_file",
        payload={"filename": filename, "mime_type": mime_type, "forced_type": forced_type},
        max_attempts=2,
    )
    file_path = persist_import_upload(job.id, filename=filename, data=data)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE jobs
                SET payload = CAST(:payload AS jsonb),
                    updated_at = NOW()
                WHERE id = CAST(:job_id AS uuid)
                """
            ),
            {
                "job_id": job.id,
                "payload": _normalize_payload(
                    {"filename": filename, "mime_type": mime_type, "forced_type": forced_type, "file_path": file_path}
                ),
            },
        )
    refreshed = get_job(engine, job.id)
    return refreshed or job


def enqueue_telegram_ingest(
    engine,
    *,
    payload: dict[str, Any],
    dedupe_key: str | None = None,
) -> JobRecord:
    return enqueue_job(
        engine,
        kind="telegram_ingest",
        payload=payload,
        dedupe_key=dedupe_key,
        max_attempts=2,
    )


def enqueue_own_bench_sync(engine, *, source_url: str, dedupe_key: str | None = None, reason: str = "scheduler") -> JobRecord:
    return enqueue_job(
        engine,
        kind="own_bench_sync",
        payload={"source_url": source_url, "reason": reason},
        dedupe_key=dedupe_key,
        max_attempts=3,
    )


def enqueue_partner_companies_sync(engine, *, source_url: str, dedupe_key: str | None = None, reason: str = "scheduler") -> JobRecord:
    return enqueue_job(
        engine,
        kind="partner_companies_sync",
        payload={"source_url": source_url, "reason": reason},
        dedupe_key=dedupe_key,
        max_attempts=3,
    )


def enqueue_digest(engine, *, dedupe_key: str | None = None, reason: str = "scheduler", deliver_to_telegram: bool = False) -> JobRecord:
    return enqueue_job(
        engine,
        kind="digest",
        payload={"reason": reason, "deliver_to_telegram": bool(deliver_to_telegram)},
        dedupe_key=dedupe_key,
        max_attempts=2,
    )


def execute_job(engine, job: JobRecord, *, repo: Repo, ollama: OllamaClient, source_fetcher: MCPSourceFetcherClient | None = None) -> dict[str, Any]:
    payload = dict(job.payload or {})
    if job.kind == "import_text":
        summary = import_use_cases.process_text_import(
            job.id,
            engine=engine,
            repo=repo,
            ollama=ollama,
            text=str(payload.get("text") or ""),
            forced_type=(str(payload.get("forced_type")) if payload.get("forced_type") else None),
        )
        return _job_result_from_summary(summary)

    if job.kind == "import_url":
        if source_fetcher is None:
            raise RuntimeError("source_fetcher is required for import_url jobs")
        summary = import_use_cases.process_url_import(
            job.id,
            engine=engine,
            repo=repo,
            ollama=ollama,
            source_fetcher=source_fetcher,
            url=str(payload.get("url") or ""),
            forced_type=(str(payload.get("forced_type")) if payload.get("forced_type") else None),
        )
        return _job_result_from_summary(summary)

    if job.kind == "import_file":
        file_path = str(payload.get("file_path") or "").strip()
        if not file_path:
            raise RuntimeError("import_file payload missing file_path")
        try:
            data = Path(file_path).read_bytes()
            summary = import_use_cases.process_file_import(
                job.id,
                engine=engine,
                repo=repo,
                ollama=ollama,
                file_name=str(payload.get("filename") or "upload.bin"),
                mime_type=str(payload.get("mime_type") or "application/octet-stream"),
                data=data,
                forced_type=(str(payload.get("forced_type")) if payload.get("forced_type") else None),
            )
        finally:
            _cleanup_import_upload(file_path)
        return _job_result_from_summary(summary)

    if job.kind == "telegram_ingest":
        summary = import_use_cases.process_telegram_import(
            job.id,
            engine=engine,
            repo=repo,
            ollama=ollama,
            source_fetcher=source_fetcher,
            payload=payload,
        )
        return _job_result_from_summary(summary)

    if job.kind == "own_bench_sync":
        if source_fetcher is None:
            raise RuntimeError("source_fetcher is required for own_bench_sync jobs")
        stats = run_own_bench_sync_sync(
            engine,
            ollama,
            source_fetcher,
            source_url=str(payload.get("source_url") or ""),
            reason=str(payload.get("reason") or "job"),
        )
        return {"stats": stats}

    if job.kind == "partner_companies_sync":
        if source_fetcher is None:
            raise RuntimeError("source_fetcher is required for partner_companies_sync jobs")
        source_url = str(payload.get("source_url") or "").strip()
        counts = extract_partner_company_counts_from_sheet(source_url, source_fetcher)
        upsert_partner_company_mentions(engine, counts, source_url=source_url)
        return {"source_url": source_url, "companies_synced": len(counts), "counts": counts}

    if job.kind == "digest":
        digest_payload = digest_use_cases.build_daily_digest_payload(engine)
        result: dict[str, Any] = {
            "window_start": digest_payload["window_start"].isoformat(),
            "window_end": digest_payload["window_end"].isoformat(),
            "new_vacancies": len(digest_payload["new_vacancies"]),
            "updated_vacancies": len(digest_payload["updated_vacancies"]),
            "new_specialists": len(digest_payload["new_specialists"]),
            "updated_specialists": len(digest_payload["updated_specialists"]),
        }
        if bool(payload.get("deliver_to_telegram")):
            token = os.getenv("BOT_TOKEN", "").strip()
            chat_ids_raw = os.getenv("DIGEST_CHAT_IDS", "").strip()
            chat_ids = [int(part.strip()) for part in chat_ids_raw.split(",") if part.strip()]
            if token and chat_ids:
                html_text = views.render_digest(**digest_payload)
                delivered = 0
                for chat_id in chat_ids:
                    _telegram_send_html(token, chat_id, html_text)
                    delivered += 1
                result["delivered_chat_ids"] = delivered
        return result

    raise RuntimeError(f"Unsupported job kind: {job.kind}")


def run_own_bench_sync_sync(engine, ollama: OllamaClient, source_fetcher: MCPSourceFetcherClient, *, source_url: str, reason: str) -> dict[str, Any]:
    import asyncio

    return asyncio.run(own_bench_use_cases.run_sync(engine, ollama, source_fetcher, source_url=source_url, reason=reason))


def schedule_interval_jobs(engine) -> list[JobRecord]:
    ensure_jobs_table(engine)
    queued: list[JobRecord] = []
    now_utc = datetime.now(timezone.utc)

    own_source_url = str(get_setting(engine, OWN_BENCH_SOURCE_URL_KEY) or "").strip()
    own_interval_min = max(5, int(os.getenv("OWN_BENCH_SYNC_INTERVAL_MIN", "240")))
    if own_source_url:
        own_bucket = int(now_utc.timestamp() // (own_interval_min * 60))
        queued.append(
            enqueue_own_bench_sync(
                engine,
                source_url=own_source_url,
                dedupe_key=f"own_bench_sync:{own_bucket}",
            )
        )

    partner_source_url = get_effective_setting(engine, PARTNER_COMPANIES_SOURCE_URL_KEY, "PARTNER_COMPANIES_SOURCE_URL", "")
    partner_interval_min = max(5, int(os.getenv("PARTNER_COMPANIES_SYNC_INTERVAL_MIN", "720")))
    if partner_source_url:
        partner_bucket = int(now_utc.timestamp() // (partner_interval_min * 60))
        queued.append(
            enqueue_partner_companies_sync(
                engine,
                source_url=partner_source_url,
                dedupe_key=f"partner_companies_sync:{partner_bucket}",
            )
        )

    digest_enabled = get_effective_setting(engine, DIGEST_JOB_ENABLED_KEY, "DIGEST_JOB_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
    if digest_enabled:
        now_msk = now_utc.astimezone(MSK)
        digest_hour = int(get_effective_setting(engine, DIGEST_SCHEDULE_HOUR_KEY, "DIGEST_SCHEDULE_HOUR", "16"))
        digest_minute = int(get_effective_setting(engine, DIGEST_SCHEDULE_MINUTE_KEY, "DIGEST_SCHEDULE_MINUTE", "0"))
        scheduled_today = now_msk.replace(hour=digest_hour, minute=digest_minute, second=0, microsecond=0)
        if now_msk >= scheduled_today:
            digest_key = f"digest:{scheduled_today.strftime('%Y%m%d%H%M')}"
            queued.append(
                enqueue_digest(
                    engine,
                    dedupe_key=digest_key,
                    deliver_to_telegram=get_effective_setting(engine, DIGEST_DELIVERY_ENABLED_KEY, "DIGEST_DELIVERY_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
                )
            )

    return queued


def worker_loop(
    engine,
    *,
    repo: Repo,
    ollama: OllamaClient,
    source_fetcher: MCPSourceFetcherClient | None = None,
    worker_id: str,
    poll_interval_sec: float = 3.0,
) -> None:
    ensure_jobs_table(engine)
    while True:
        job = claim_next_job(engine, worker_id=worker_id)
        if job is None:
            time.sleep(poll_interval_sec)
            continue
        try:
            result = execute_job(engine, job, repo=repo, ollama=ollama, source_fetcher=source_fetcher)
            complete_job(engine, job.id, result)
            log.info("Job completed id=%s kind=%s", job.id, job.kind)
        except Exception as exc:
            log.exception("Job failed id=%s kind=%s", job.id, job.kind)
            fail_job(engine, job.id, error=f"{type(exc).__name__}: {exc}", retryable=True)
