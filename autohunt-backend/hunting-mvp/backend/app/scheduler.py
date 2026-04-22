from __future__ import annotations

import logging
import os
import time

from app.use_cases.jobs import ensure_jobs_table, schedule_interval_jobs
from backend.app.db.session import get_engine


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")


def main() -> None:
    setup_logging()
    logger = logging.getLogger("backend.scheduler")
    engine = get_engine()
    ensure_jobs_table(engine)
    poll_interval = max(10.0, float(os.getenv("JOB_SCHEDULER_POLL_SEC", "30")))
    logger.info("Scheduler started poll_interval=%s", poll_interval)
    while True:
        queued = schedule_interval_jobs(engine)
        if queued:
            logger.info("Scheduler touched jobs=%s", [(job.kind, job.id) for job in queued])
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
