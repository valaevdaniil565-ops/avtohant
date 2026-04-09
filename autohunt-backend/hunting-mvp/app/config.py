from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[1]  # hunting-mvp/app -> hunting-mvp
ENV_PATH = BASE_DIR / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_PATH),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    DATABASE_URL: str

    BOT_TOKEN: str
    MANAGER_CHAT_IDS: str | None = None

    OLLAMA_HOST: str = "http://localhost:11434"
    LLM_MODEL: str = "llama3:8b"
    EMBED_MODEL: str = "nomic-embed-text"


def get_settings() -> Settings:
    return Settings()
