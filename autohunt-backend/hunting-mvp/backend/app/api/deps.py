from __future__ import annotations

from functools import lru_cache

from app.db.repo import Repo
from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.llm.ollama_client import OllamaClient
from backend.app.core.config import get_backend_settings
from backend.app.db.session import get_engine


@lru_cache(maxsize=1)
def get_repo() -> Repo:
    settings = get_backend_settings()
    return Repo(settings.database_url)


@lru_cache(maxsize=1)
def get_ollama_client() -> OllamaClient:
    settings = get_backend_settings()
    return OllamaClient(host=settings.ollama_host, model=settings.llm_model, embed_model=settings.embed_model)


@lru_cache(maxsize=1)
def get_source_fetcher() -> MCPSourceFetcherClient:
    return MCPSourceFetcherClient()


def get_shared_engine():
    return get_engine()
