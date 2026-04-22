from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return int(v.strip())


def _env_int_opt(name: str) -> int | None:
    v = os.getenv(name)
    if v is None or not v.strip():
        return None
    return int(v.strip())


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or not v.strip():
        return default
    return float(v.strip())


@dataclass(frozen=True)
class Settings:
    tg_api_id: int
    tg_api_hash: str
    tg_session_name: str

    database_url: str

    download_dir: str
    max_file_mb: int

    backfill_enabled: bool
    backfill_limit: int
    backfill_days: int

    # forward-to-bot mode
    forward_to_bot: bool
    forward_bot_username: str | None
    forward_bot_id: int | None
    forward_reply_timeout_sec: int

    # archive relay mode
    archive_enabled: bool
    archive_chat_id: int | None
    archive_chat_username: str | None
    archive_fail_open: bool  # если не смогли в архив — можно (опц.) слать прямо в бота

    ingest_via_db_jobs: bool

    # anti-ban rate limits
    rl_backfill_min_sec: float
    rl_forward_min_sec: float
    rl_download_min_sec: float
    rl_jitter_sec: float

    @staticmethod
    def load() -> "Settings":
        load_dotenv()

        api_id = int(os.getenv("TG_API_ID", "").strip() or "0")
        api_hash = os.getenv("TG_API_HASH", "").strip()
        session_name = os.getenv("TG_SESSION_NAME", "storage/sessions/collector").strip()

        db_url = os.getenv("DATABASE_URL", "").strip()

        download_dir = os.getenv("DOWNLOAD_DIR", "storage").strip()  # base dir; collector adds /telegram/...
        max_file_mb = _env_int("MAX_FILE_MB", 20)

        backfill_enabled = _env_bool("BACKFILL_ENABLED", True)
        backfill_limit = _env_int("BACKFILL_LIMIT", 200)
        backfill_days = _env_int("BACKFILL_DAYS", 7)

        forward_to_bot = _env_bool("FORWARD_TO_BOT", False)
        forward_bot_username = os.getenv("FORWARD_BOT_USERNAME")
        forward_bot_username = forward_bot_username.strip().lstrip("@") if forward_bot_username else None

        fwd_id_raw = os.getenv("FORWARD_BOT_ID")
        forward_bot_id = int(fwd_id_raw.strip()) if fwd_id_raw and fwd_id_raw.strip() else None

        forward_reply_timeout_sec = _env_int("FORWARD_REPLY_TIMEOUT_SEC", 60)

        archive_enabled = _env_bool("ARCHIVE_ENABLED", False)
        archive_chat_id = _env_int_opt("ARCHIVE_CHAT_ID")  # обычно -100...
        archive_chat_username = os.getenv("ARCHIVE_CHAT_USERNAME")
        archive_chat_username = archive_chat_username.strip().lstrip("@") if archive_chat_username else None
        archive_fail_open = _env_bool("ARCHIVE_FAIL_OPEN", False)
        ingest_via_db_jobs = _env_bool("INGEST_VIA_DB_JOBS", False)

        rl_backfill_min_sec = _env_float("RL_BACKFILL_MIN_SEC", 0.30)
        rl_forward_min_sec = _env_float("RL_FORWARD_MIN_SEC", 0.80)
        rl_download_min_sec = _env_float("RL_DOWNLOAD_MIN_SEC", 0.50)
        rl_jitter_sec = _env_float("RL_JITTER_SEC", 0.40)

        if not api_id or not api_hash:
            raise RuntimeError("TG_API_ID/TG_API_HASH are required")

        if not db_url:
            raise RuntimeError("DATABASE_URL is required")

        if forward_to_bot and not (forward_bot_username or forward_bot_id):
            raise RuntimeError("FORWARD_TO_BOT=true but FORWARD_BOT_USERNAME/FORWARD_BOT_ID not set")

        if archive_enabled and not (archive_chat_id or archive_chat_username):
            raise RuntimeError("ARCHIVE_ENABLED=true but ARCHIVE_CHAT_ID/ARCHIVE_CHAT_USERNAME not set")

        return Settings(
            tg_api_id=api_id,
            tg_api_hash=api_hash,
            tg_session_name=session_name,
            database_url=db_url,
            download_dir=download_dir,
            max_file_mb=max_file_mb,
            backfill_enabled=backfill_enabled,
            backfill_limit=backfill_limit,
            backfill_days=backfill_days,
            forward_to_bot=forward_to_bot,
            forward_bot_username=forward_bot_username,
            forward_bot_id=forward_bot_id,
            forward_reply_timeout_sec=forward_reply_timeout_sec,
            archive_enabled=archive_enabled,
            archive_chat_id=archive_chat_id,
            archive_chat_username=archive_chat_username,
            archive_fail_open=archive_fail_open,
            ingest_via_db_jobs=ingest_via_db_jobs,
            rl_backfill_min_sec=rl_backfill_min_sec,
            rl_forward_min_sec=rl_forward_min_sec,
            rl_download_min_sec=rl_download_min_sec,
            rl_jitter_sec=rl_jitter_sec,
        )
