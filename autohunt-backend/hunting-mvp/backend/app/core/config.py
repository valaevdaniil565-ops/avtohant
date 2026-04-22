from __future__ import annotations

import os
from functools import lru_cache

from pydantic import BaseModel, ConfigDict

from app.config import Settings as LegacySettings
from app.config import get_settings


class BackendSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    app_name: str = "Avtohunt Backend"
    app_version: str = "0.1.0"
    api_prefix: str = "/api"
    debug: bool = False
    jobs_enabled: bool = True
    jobs_poll_sec: float = 3.0
    scheduler_poll_sec: float = 30.0
    legacy: LegacySettings

    @property
    def database_url(self) -> str:
        return self.legacy.DATABASE_URL

    @property
    def ollama_host(self) -> str:
        return self.legacy.OLLAMA_HOST

    @property
    def llm_model(self) -> str:
        return self.legacy.LLM_MODEL

    @property
    def embed_model(self) -> str:
        return self.legacy.EMBED_MODEL


@lru_cache(maxsize=1)
def get_backend_settings() -> BackendSettings:
    legacy = get_settings()
    return BackendSettings(
        legacy=legacy,
        jobs_enabled=os.getenv("JOBS_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"},
        jobs_poll_sec=float(os.getenv("JOB_WORKER_POLL_SEC", "3")),
        scheduler_poll_sec=float(os.getenv("JOB_SCHEDULER_POLL_SEC", "30")),
    )
