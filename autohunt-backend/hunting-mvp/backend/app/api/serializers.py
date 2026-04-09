from __future__ import annotations

from datetime import datetime
from uuid import UUID


def to_jsonable(value):
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    return value


def normalize_row(row: dict) -> dict:
    return {key: to_jsonable(value) for key, value in dict(row).items()}
