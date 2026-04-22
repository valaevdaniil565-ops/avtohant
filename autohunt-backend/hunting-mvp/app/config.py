import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[1]  # hunting-mvp/app -> hunting-mvp
LOCAL_ENV_PATH = BASE_DIR / ".env"
DEFAULT_SHARED_ENV_PATH = Path("C:/hunting-mvp/.env")


def _resolve_existing_env_path(raw_path: str | None) -> Path | None:
    candidate = str(raw_path or "").strip()
    if not candidate:
        return None
    path = Path(candidate).expanduser()
    return path if path.is_file() else None


def resolve_env_path() -> Path | None:
    explicit_env_path = _resolve_existing_env_path(os.getenv("HUNTING_MVP_ENV_FILE") or os.getenv("AUTOHUNT_ENV_FILE"))
    if explicit_env_path is not None:
        return explicit_env_path

    explicit_external_dir = str(os.getenv("HUNTING_MVP_EXTERNAL_DIR") or "").strip()
    if explicit_external_dir:
        external_env_path = _resolve_existing_env_path(str(Path(explicit_external_dir).expanduser() / ".env"))
        if external_env_path is not None:
            return external_env_path

    if LOCAL_ENV_PATH.is_file():
        return LOCAL_ENV_PATH

    if DEFAULT_SHARED_ENV_PATH.is_file():
        return DEFAULT_SHARED_ENV_PATH

    return None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        extra="ignore",
    )

    DATABASE_URL: str = "postgresql+psycopg://hunting:hunting@127.0.0.1:5432/hunting"

    BOT_TOKEN: str = ""
    MANAGER_CHAT_IDS: str | None = None

    OLLAMA_HOST: str = "http://localhost:11434"
    LLM_MODEL: str = "llama3:8b"
    EMBED_MODEL: str = "nomic-embed-text"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    env_path = resolve_env_path()
    if env_path is not None:
        return Settings(_env_file=str(env_path))
    return Settings()
