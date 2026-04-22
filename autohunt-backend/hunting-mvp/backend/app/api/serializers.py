from __future__ import annotations

import json
from datetime import datetime
from uuid import UUID


def _parse_json_field(value, fallback):
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return fallback
    raw = value.strip()
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except Exception:
        return fallback


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
    out = {key: to_jsonable(value) for key, value in dict(row).items()}
    if "stack" in out:
        parsed_stack = _parse_json_field(out.get("stack"), [])
        out["stack"] = parsed_stack if isinstance(parsed_stack, list) else []
    if "source_meta" in out:
        parsed_meta = _parse_json_field(out.get("source_meta"), {})
        out["source_meta"] = parsed_meta if isinstance(parsed_meta, dict) else {}
    return out
