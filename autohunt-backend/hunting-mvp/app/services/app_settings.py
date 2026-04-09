from __future__ import annotations

import json
import os

from sqlalchemy import text


OWN_BENCH_SOURCE_URL_KEY = "own_bench_source_url"
OWN_BENCH_SYNC_LAST_SUCCESS_AT_KEY = "own_bench_sync_last_success_at"
OWN_BENCH_SYNC_LAST_ERROR_KEY = "own_bench_sync_last_error"
OWN_BENCH_SYNC_LAST_STATS_KEY = "own_bench_sync_last_stats"
PARTNER_COMPANIES_SOURCE_URL_KEY = "partner_companies_source_url"
DIGEST_SCHEDULE_HOUR_KEY = "digest_schedule_hour"
DIGEST_SCHEDULE_MINUTE_KEY = "digest_schedule_minute"
DIGEST_JOB_ENABLED_KEY = "digest_job_enabled"
DIGEST_DELIVERY_ENABLED_KEY = "digest_delivery_enabled"


def ensure_app_settings_table(engine) -> None:
    q = text(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
          key VARCHAR(128) PRIMARY KEY,
          value TEXT NOT NULL,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    with engine.begin() as c:
        c.execute(q)


def get_setting(engine, key: str) -> str | None:
    with engine.begin() as c:
        row = c.execute(text("SELECT value FROM app_settings WHERE key = :key"), {"key": key}).scalar_one_or_none()
    return str(row) if row is not None else None


def set_setting(engine, key: str, value: str) -> None:
    with engine.begin() as c:
        c.execute(
            text(
                """
                INSERT INTO app_settings(key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT(key) DO UPDATE
                  SET value = EXCLUDED.value,
                      updated_at = NOW()
                """
            ),
            {"key": key, "value": value},
        )


def get_json_setting(engine, key: str) -> dict:
    raw = get_setting(engine, key)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def set_json_setting(engine, key: str, value: dict) -> None:
    set_setting(engine, key, json.dumps(value or {}, ensure_ascii=False))


def get_or_init_setting(engine, key: str, default_value: str) -> str:
    ensure_app_settings_table(engine)
    current = get_setting(engine, key)
    if current is not None and str(current).strip():
        return str(current).strip()
    set_setting(engine, key, default_value)
    return default_value


def get_setting_row(engine, key: str) -> dict | None:
    ensure_app_settings_table(engine)
    with engine.begin() as c:
        row = c.execute(
            text("SELECT key, value, updated_at FROM app_settings WHERE key = :key"),
            {"key": key},
        ).mappings().first()
    return dict(row) if row else None


def list_settings(engine, keys: list[str] | None = None) -> list[dict]:
    ensure_app_settings_table(engine)
    with engine.begin() as c:
        if keys:
            rows = c.execute(
                text(
                    """
                    SELECT key, value, updated_at
                    FROM app_settings
                    WHERE key = ANY(:keys)
                    ORDER BY key ASC
                    """
                ),
                {"keys": keys},
            ).mappings().all()
        else:
            rows = c.execute(text("SELECT key, value, updated_at FROM app_settings ORDER BY key ASC")).mappings().all()
    return [dict(row) for row in rows]


def get_effective_setting(engine, key: str, env_name: str, default_value: str) -> str:
    stored = get_setting(engine, key)
    if stored is not None and str(stored).strip():
        return str(stored).strip()
    env_value = os.getenv(env_name, "").strip()
    return env_value if env_value else default_value
