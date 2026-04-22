from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.db.repo import Repo
from app.use_cases.dev_seed import seed_demo_data
from backend.app.core.config import get_backend_settings
from backend.app.db.session import get_engine


def main() -> None:
    settings = get_backend_settings()
    engine = get_engine()
    repo = Repo(settings.database_url)
    result = seed_demo_data(engine, repo)
    print(result)


if __name__ == "__main__":
    main()
