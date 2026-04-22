from __future__ import annotations

import logging
import os
import socket
import uuid

from app.use_cases.jobs import ensure_jobs_table, worker_loop
from backend.app.api.deps import get_ollama_client, get_repo, get_source_fetcher
from backend.app.db.session import get_engine


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def main() -> None:
    setup_logging()
    engine = get_engine()
    ensure_jobs_table(engine)
    worker_id = os.getenv("JOB_WORKER_ID", f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}")
    poll_interval = float(os.getenv("JOB_WORKER_POLL_SEC", "3"))
    worker_loop(
        engine,
        repo=get_repo(),
        ollama=get_ollama_client(),
        source_fetcher=get_source_fetcher(),
        worker_id=worker_id,
        poll_interval_sec=poll_interval,
    )


if __name__ == "__main__":
    main()
