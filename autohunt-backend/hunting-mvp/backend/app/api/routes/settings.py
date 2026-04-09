from __future__ import annotations

from fastapi import APIRouter

from app.services.app_settings import (
    DIGEST_DELIVERY_ENABLED_KEY,
    DIGEST_JOB_ENABLED_KEY,
    DIGEST_SCHEDULE_HOUR_KEY,
    DIGEST_SCHEDULE_MINUTE_KEY,
    OWN_BENCH_SOURCE_URL_KEY,
    PARTNER_COMPANIES_SOURCE_URL_KEY,
    get_effective_setting,
    get_setting_row,
    set_setting,
)
from app.services.audit_log import write_audit_event
from backend.app.api.schemas import SettingItem, SettingsResponse, SettingsUpdateRequest, SettingsUpdateResponse
from backend.app.db.session import get_engine

router = APIRouter()

_SETTINGS_DEFAULTS: dict[str, tuple[str, str, str]] = {
    OWN_BENCH_SOURCE_URL_KEY: ("OWN_BENCH_SOURCE_URL", "", "db"),
    PARTNER_COMPANIES_SOURCE_URL_KEY: ("PARTNER_COMPANIES_SOURCE_URL", "", "db_or_env"),
    DIGEST_SCHEDULE_HOUR_KEY: ("DIGEST_SCHEDULE_HOUR", "16", "db_or_env"),
    DIGEST_SCHEDULE_MINUTE_KEY: ("DIGEST_SCHEDULE_MINUTE", "0", "db_or_env"),
    DIGEST_JOB_ENABLED_KEY: ("DIGEST_JOB_ENABLED", "true", "db_or_env"),
    DIGEST_DELIVERY_ENABLED_KEY: ("DIGEST_DELIVERY_ENABLED", "false", "db_or_env"),
}


def _read_setting_item(engine, key: str) -> SettingItem:
    env_name, default_value, _ = _SETTINGS_DEFAULTS[key]
    row = get_setting_row(engine, key)
    if row:
        return SettingItem(key=key, value=str(row["value"]), source="db", updated_at=row.get("updated_at"))
    return SettingItem(
        key=key,
        value=get_effective_setting(engine, key, env_name, default_value),
        source="default" if not default_value else "env_or_default",
        updated_at=None,
    )


@router.get("", response_model=SettingsResponse)
def get_settings() -> SettingsResponse:
    engine = get_engine()
    return SettingsResponse(items=[_read_setting_item(engine, key) for key in _SETTINGS_DEFAULTS])


@router.put("", response_model=SettingsUpdateResponse)
def update_settings(payload: SettingsUpdateRequest) -> SettingsUpdateResponse:
    engine = get_engine()
    updated: list[SettingItem] = []
    for item in payload.items:
        if item.key not in _SETTINGS_DEFAULTS:
            continue
        set_setting(engine, item.key, item.value)
        updated.append(_read_setting_item(engine, item.key))
    if updated:
        write_audit_event(
            engine,
            event_type="settings.updated",
            entity_type="settings",
            payload={"keys": [item.key for item in updated]},
        )
    return SettingsUpdateResponse(updated=updated)
