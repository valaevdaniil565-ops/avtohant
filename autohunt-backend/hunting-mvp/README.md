# Hunting MVP

Telegram-first MVP for vacancies/bench ingestion, extraction, matching and digest, now extended with a minimal web backend, background worker/scheduler, and a backend-first ingest path that still keeps the Telegram bot and Telethon collector compatible.

## What Runs Locally

- `manager bot`: [app/bots/manager_bot.py](/Users/globus/Documents/MVP/hunting-mvp/app/bots/manager_bot.py)
- `telethon collector`: [app/collectors/tg_collector/collector.py](/Users/globus/Documents/MVP/hunting-mvp/app/collectors/tg_collector/collector.py)
- `web backend`: [backend/app/main.py](/Users/globus/Documents/MVP/hunting-mvp/backend/app/main.py)
- `worker`: [backend/app/worker.py](/Users/globus/Documents/MVP/hunting-mvp/backend/app/worker.py)
- `scheduler`: [backend/app/scheduler.py](/Users/globus/Documents/MVP/hunting-mvp/backend/app/scheduler.py)

## Local Setup

1. Copy env:

```bash
cp .env.example .env
```

2. Create venv and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3. Start PostgreSQL with Docker Desktop running:

```bash
docker compose up -d db
psql postgresql://hunting:hunting@127.0.0.1:5432/hunting -f scripts/init_db.sql
```

4. Start the processes you need:

```bash
PYTHONPATH=. python -m uvicorn backend.app.main:app --reload --host 127.0.0.1 --port 8000
PYTHONPATH=. python -m backend.app.worker
PYTHONPATH=. python -m backend.app.scheduler
PYTHONPATH=. python -m app.bots.manager_bot
PYTHONPATH=. python -m app.collectors.tg_collector
ollama serve
```

## Make Targets

```bash
make install
make db-up
make db-init
make ollama
make api
make worker
make scheduler
make bot
make collector
make verify
```

`manual upload`, matching and extraction require a running local Ollama server on `OLLAMA_HOST`.

## API

Base URL: `http://127.0.0.1:8000/api`

Main endpoints:

- `GET /health`
- `GET /version`
- `GET /vacancies`
- `GET /vacancies/{id}`
- `GET /specialists`
- `GET /specialists/{id}`
- `GET /vacancies/{id}/matches`
- `GET /specialists/{id}/matches`
- `GET /sources/{entity_type}/{entity_id}`
- `POST /imports/text`
- `POST /imports/url`
- `POST /imports/file`
- `GET /imports/{job_id}`
- `GET /jobs/{job_id}`
- `POST /entities/hide-by-source`
- `GET /own-bench/status`

## Recommended Ingest Mode

Preferred local mode:

- `INGEST_VIA_DB_JOBS=true`
- `backend.app.worker` is running
- `app.collectors.tg_collector` is running

In this mode:

- Telethon collector stays the Telegram ingestion adapter.
- Collector still stores raw Telegram messages into `tg_messages_raw`.
- Collector enqueues `telegram_ingest` jobs into Postgres.
- Worker executes the shared ingest/application pipeline.
- Bot and web API continue to share the same DB and domain logic.

## External Source Support

Supported through the shared source fetcher:

- Google Sheets public links
- Google Docs public links
- Google Drive public file links
- Yandex Disk public links
- direct `.pdf`, `.docx`, `.xlsx`, `.csv`, `.txt`, `.html`
- generic allowlisted URLs

Safety rules preserved:

- allowlist enforcement via `SOURCE_FETCHER_ALLOWED_DOMAINS`
- localhost/private IP blocking
- file size, timeout and max-items limits

## Verification Performed

Verified in this workspace:

- Python compilation passed for shared use-cases, collector and backend.
- Import/bootstrap passed for backend, bot and collector in `.venv`.
- Targeted tests passed:
  - `tests/test_manager_bot_digest.py`
  - `tests/test_partner_companies.py`

Not fully verified in this workspace:

- live backend startup against PostgreSQL
- live bot startup against Telegram
- live collector startup against Telegram

Reason:

- local PostgreSQL on `127.0.0.1:5432` was not running
- Docker daemon was not running during verification

## Temporary

- `jobs` are Postgres-backed but still simple polling jobs, not a full queue system.
- file import payloads are temporarily stored in `storage/jobs/`.
- `telegram_ingest` currently uses shared import processing through jobs, but replay/admin tooling is still minimal.
- some legacy business orchestration still lives in [manager_bot.py](/Users/globus/Documents/MVP/hunting-mvp/app/bots/manager_bot.py) and is only partially extracted.
- no Alembic revision history exists yet; schema is still bootstrapped by SQL + additive runtime DDL.

## Phase 2

- add admin replay endpoints for `tg_messages_raw` and ingest jobs
- add job list/filter endpoints and UI-friendly operational status pages
- finish extracting end-to-end ingest orchestration from bot-specific code into shared use-cases
- introduce real reversible Alembic migrations
- add integration tests for `api + worker + collector` against a local Postgres
- add frontend wiring from `avtohant-main` to live API contracts
