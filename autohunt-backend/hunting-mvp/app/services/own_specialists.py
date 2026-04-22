from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from app.db.repo import VECTOR_DIM, build_search_text, generate_synthetic_id
from app.integrations.mcp_source_fetcher.client import MCPSourceFetcherClient
from app.llm.pre_classifier import normalize_short_bench_line
from app.llm.prompts import SPECIALIST_EXTRACTION_PROMPT_V2
from app.pipeline import build_fallback_specialist_item, preprocess_for_llm
from app.services.availability import resolve_specialist_is_available

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RegistryBinding:
    registry_key: str
    identity_key: str | None
    specialist_id: str | None


def ensure_own_specialists_registry_table(engine) -> None:
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS own_specialists_registry (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          registry_key VARCHAR(128) NOT NULL UNIQUE,
          source_url VARCHAR(1024) NOT NULL,
          locator VARCHAR(255),
          raw_text TEXT,
          parsed_payload JSONB,
          specialist_id UUID REFERENCES specialists(id) ON DELETE SET NULL,
          parse_status VARCHAR(20) NOT NULL DEFAULT 'pending',
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          last_error TEXT,
          last_synced_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "ALTER TABLE own_specialists_registry ADD COLUMN IF NOT EXISTS identity_key VARCHAR(128)",
        "ALTER TABLE specialists ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT FALSE",
    ]
    with engine.begin() as c:
        for q in ddl:
            c.execute(text(q))


def _safe_json_loads(model_text: str) -> dict[str, Any]:
    try:
        t = (model_text or "").strip()
        if t.startswith("```"):
            t = t.strip("`")
            t = t.replace("json", "", 1).strip()
        start = t.find("{")
        end = t.rfind("}")
        if start != -1 and end != -1 and end > start:
            t = t[start : end + 1]
        parsed = json.loads(t)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _registry_locator(item: Any, idx: int) -> str:
    meta = getattr(item, "metadata", {}) or {}
    table_name = str(meta.get("table_name") or "").strip()
    row_index = getattr(item, "row_index", None)
    if table_name and row_index is not None:
        return f"{table_name}:{row_index}"
    if row_index is not None:
        return f"row:{row_index}"
    return f"idx:{idx}"


def _registry_key(source_url: str, locator: str) -> str:
    payload = f"{source_url}|{locator}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _normalize_identity_part(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _normalize_identity_stack_tokens(values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        token = _normalize_identity_part(value)
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _normalize_identity_contact(value: Any) -> str:
    token = str(value or "").strip().rstrip(".,;:")
    return _normalize_identity_part(token)


_URL_RE = re.compile(r"https?://[^\s,;]+", re.IGNORECASE)
_EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b")
_HANDLE_RE = re.compile(r"(?<!\w)@[a-zA-Z0-9_]{4,}")


def _extract_registry_contact_tokens(raw_text: str, payload: dict[str, Any]) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()

    def _push(value: Any) -> None:
        normalized = _normalize_identity_contact(value)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        tokens.append(normalized)

    payload_keys = (
        "resume_url",
        "cv_url",
        "portfolio_url",
        "telegram",
        "telegram_url",
        "email",
        "phone",
        "contact",
        "contacts",
        "linkedin_url",
        "github_url",
    )
    for key in payload_keys:
        value = (payload or {}).get(key)
        if isinstance(value, list):
            for item in value:
                _push(item)
        else:
            _push(value)

    text_in = raw_text or ""
    for match in _URL_RE.findall(text_in):
        _push(match)
    for match in _EMAIL_RE.findall(text_in):
        _push(match)
    for match in _HANDLE_RE.findall(text_in):
        _push(match)
    return tokens


def _normalize_source_index(locator: str) -> str:
    text_value = (locator or "").strip()
    for sep in (":", "#"):
        if sep in text_value:
            tail = text_value.rsplit(sep, 1)[-1].strip()
            if tail:
                return tail
    return text_value or "-"


def _normalize_header_name(value: str) -> str:
    return re.sub(r"[^a-zа-яё0-9]+", " ", (value or "").strip().lower()).strip()


def _extract_registry_specialist_name(raw_text: str) -> str:
    name_headers = (
        "имя",
        "фио",
        "ф и о",
        "фамилия имя",
        "специалист",
        "кандидат",
        "name",
        "full name",
        "candidate",
        "specialist",
        "resource name",
        "consultant",
    )
    for line in (raw_text or "").splitlines():
        if ":" not in line:
            continue
        header, value = line.split(":", 1)
        normalized_header = _normalize_header_name(header)
        if not normalized_header:
            continue
        if any(token in normalized_header for token in name_headers):
            candidate = value.strip()
            if candidate:
                return candidate
    return "-"


_INVALID_NAME_VALUES = {"-", "—", "null", "none", "unknown", "n/a"}
_ROLE_LIKE_NAME_RE = re.compile(
    r"(?i)\b("
    r"developer|engineer|designer|manager|architect|analyst|consultant|specialist|candidate|"
    r"разработчик|дизайнер|менеджер|архитектор|аналитик|специалист|кандидат|"
    r"frontend|backend|fullstack|qa|devops|python|java|react|flutter|android|ios|mobile|middle|senior|junior|lead"
    r")\b"
)


def _extract_registry_specialist_name_from_payload(payload: dict[str, Any], *, role: str | None = None) -> str:
    candidate = str((payload or {}).get("name") or "").strip()
    if not candidate:
        return "-"
    if candidate.lower() in _INVALID_NAME_VALUES:
        return "-"
    if role and candidate.casefold() == str(role).strip().casefold():
        return "-"
    if any(ch.isdigit() for ch in candidate):
        return "-"
    if _ROLE_LIKE_NAME_RE.search(candidate):
        return "-"
    return candidate


def _resolve_registry_specialist_name(raw_text: str, payload: dict[str, Any]) -> str:
    from_text = _extract_registry_specialist_name(raw_text)
    if from_text != "-":
        return from_text
    from_payload = _extract_registry_specialist_name_from_payload(payload, role=payload.get("role"))
    if from_payload != "-":
        return from_payload
    return "-"


def _build_registry_identity_key(source_url: str, raw_text: str, payload: dict[str, Any], locator: str) -> str:
    name = _normalize_identity_part(_resolve_registry_specialist_name(raw_text, payload))
    role = _normalize_identity_part((payload or {}).get("role"))
    stack_tokens = _normalize_identity_stack_tokens((payload or {}).get("stack") or [])
    contact_tokens = _extract_registry_contact_tokens(raw_text, payload)

    identity_parts = [f"source={_normalize_identity_part(source_url)}"]
    if contact_tokens:
        identity_parts.append("contacts=" + "|".join(contact_tokens[:4]))
    if name and name != "-":
        identity_parts.append(f"name={name}")
    if role:
        identity_parts.append(f"role={role}")
    if stack_tokens:
        identity_parts.append("stack=" + "|".join(stack_tokens[:4]))

    if len(identity_parts) == 1:
        normalized_raw = _normalize_identity_part(raw_text)[:400]
        if normalized_raw:
            identity_parts.append(f"raw={normalized_raw}")
        else:
            identity_parts.append(f"locator={_normalize_identity_part(locator)}")

    payload_text = "|".join(identity_parts)
    return hashlib.sha256(payload_text.encode("utf-8")).hexdigest()[:32]


def _own_registry_synthetic_id(identity_key: str | None, role: str, stack: list[str], grade: str | None, rate_hint: int | None) -> str:
    if identity_key:
        return hashlib.sha256(f"own_registry|{identity_key}".encode("utf-8")).hexdigest()[:16]
    return generate_synthetic_id(role, stack, grade, rate_hint)


def _build_registry_source_meta(source_meta: dict[str, Any], *, source_url: str, locator: str, specialist_name: str) -> dict[str, Any]:
    out = dict(source_meta or {})
    out["manager_name"] = "-"
    out["source_kind"] = "file"
    out["source_index"] = _normalize_source_index(locator)
    out["source_person_name"] = (specialist_name or "-").strip() or "-"
    out["source_display"] = (
        f"Менеджер: -; "
        f"Ссылка на файл: {source_url}; "
        f"Специалист: {out['source_person_name']}"
    )
    return out


def _external_source_ids(source_url: str, locator: str) -> tuple[int, int]:
    chat_hash = int(hashlib.sha256(source_url.encode("utf-8")).hexdigest()[:15], 16)
    msg_hash = int(hashlib.sha256(f"{source_url}|{locator}".encode("utf-8")).hexdigest()[:15], 16)
    return -chat_hash, msg_hash


def _upsert_registry_row(
    engine,
    *,
    registry_key: str,
    identity_key: str | None,
    source_url: str,
    locator: str,
    raw_text: str,
    parsed_payload: dict[str, Any] | None,
    specialist_id: str | None,
    parse_status: str,
    last_error: str | None,
) -> None:
    q = text(
        """
        INSERT INTO own_specialists_registry(
          registry_key,
          identity_key,
          source_url,
          locator,
          raw_text,
          parsed_payload,
          specialist_id,
          parse_status,
          is_active,
          last_error,
          last_synced_at,
          updated_at
        )
        VALUES (
          :registry_key,
          :identity_key,
          :source_url,
          :locator,
          :raw_text,
          CAST(:parsed_payload AS jsonb),
          :specialist_id,
          :parse_status,
          TRUE,
          :last_error,
          NOW(),
          NOW()
        )
        ON CONFLICT(registry_key) DO UPDATE SET
          identity_key = EXCLUDED.identity_key,
          raw_text = EXCLUDED.raw_text,
          parsed_payload = EXCLUDED.parsed_payload,
          specialist_id = COALESCE(EXCLUDED.specialist_id, own_specialists_registry.specialist_id),
          parse_status = EXCLUDED.parse_status,
          is_active = TRUE,
          last_error = EXCLUDED.last_error,
          last_synced_at = NOW(),
          updated_at = NOW()
        """
    )
    with engine.begin() as c:
        c.execute(
            q,
            {
                "registry_key": registry_key,
                "identity_key": identity_key,
                "source_url": source_url,
                "locator": locator,
                "raw_text": raw_text,
                "parsed_payload": json.dumps(parsed_payload or {}, ensure_ascii=False),
                "specialist_id": specialist_id,
                "parse_status": parse_status,
                "last_error": last_error,
            },
        )


def _load_registry_bindings(engine, source_url: str) -> tuple[dict[str, RegistryBinding], dict[str, RegistryBinding]]:
    q = text(
        """
        SELECT registry_key, identity_key, specialist_id
        FROM own_specialists_registry
        WHERE source_url = :source_url AND is_active = TRUE
        """
    )
    with engine.begin() as c:
        rows = c.execute(q, {"source_url": source_url}).mappings().all()
    by_registry_key: dict[str, RegistryBinding] = {}
    by_identity_key: dict[str, RegistryBinding] = {}
    for row in rows:
        binding = RegistryBinding(
            registry_key=str(row["registry_key"]),
            identity_key=(str(row["identity_key"]) if row.get("identity_key") else None),
            specialist_id=(str(row["specialist_id"]) if row.get("specialist_id") else None),
        )
        by_registry_key[binding.registry_key] = binding
        if binding.identity_key and binding.identity_key not in by_identity_key:
            by_identity_key[binding.identity_key] = binding
    return by_registry_key, by_identity_key


def _deactivate_registry_row(engine, registry_key: str, specialist_id: str | None) -> None:
    with engine.begin() as c:
        c.execute(
            text(
                """
                UPDATE own_specialists_registry
                SET is_active = FALSE, updated_at = NOW()
                WHERE registry_key = :registry_key
                """
            ),
            {"registry_key": registry_key},
        )
    _deactivate_unbound_internal_specialist(engine, specialist_id)


def _deactivate_unbound_internal_specialist(engine, specialist_id: str | None) -> None:
    if not specialist_id:
        return
    with engine.begin() as c:
        active_bindings = c.execute(
            text(
                """
                SELECT COUNT(*)
                FROM own_specialists_registry
                WHERE specialist_id = :specialist_id
                  AND is_active = TRUE
                """
            ),
            {"specialist_id": specialist_id},
        ).scalar_one()
        if int(active_bindings or 0) == 0:
            c.execute(
                text(
                    """
                    UPDATE specialists
                    SET is_internal = FALSE,
                        status = CASE WHEN status = 'active' THEN 'hidden' ELSE status END,
                        updated_at = NOW()
                    WHERE id = :specialist_id
                    """
                ),
                {"specialist_id": specialist_id},
            )


def deactivate_registry_source(engine, source_url: str) -> int:
    q = text(
        """
        SELECT registry_key, specialist_id
        FROM own_specialists_registry
        WHERE source_url = :source_url AND is_active = TRUE
        """
    )
    with engine.begin() as c:
        rows = c.execute(q, {"source_url": source_url}).mappings().all()
    for row in rows:
        _deactivate_registry_row(
            engine,
            str(row["registry_key"]),
            str(row["specialist_id"]) if row.get("specialist_id") else None,
        )
    return len(rows)


def _upsert_internal_specialist(
    engine,
    data: dict[str, Any],
    original_text: str,
    embedding: list[float] | None,
    status: str,
    *,
    identity_key: str | None = None,
) -> str:
    role = data.get("role") or "Unknown"
    stack = data.get("stack") or []
    grade = data.get("grade")
    exp = data.get("experience_years_min")
    if data.get("experience_years_min") == data.get("experience_years_max"):
        exp = data.get("experience_years_min")
    rate_hint = data.get("rate_min") or data.get("rate_max")
    syn = _own_registry_synthetic_id(identity_key, role, stack, grade, rate_hint)
    emb = None
    if embedding and len(embedding) == VECTOR_DIM:
        emb = "[" + ",".join(f"{x:.8f}" for x in embedding) + "]"
    hired_at = datetime.now(timezone.utc) if status == "hired" else None

    q = text(
        """
        INSERT INTO specialists(
          synthetic_id, role, stack, grade, experience_years,
          rate_min, rate_max, currency, location,
          description, original_text, embedding, status, expires_at, hired_at, is_internal
        )
        VALUES (
          :syn, :role, CAST(:stack AS jsonb), :grade, :exp,
          :rmin, :rmax, :cur, :loc,
          :desc, :orig,
          CAST(:emb AS vector),
          :status,
          NOW() + interval '30 days',
          :hired_at,
          TRUE
        )
        ON CONFLICT(synthetic_id) DO UPDATE SET
          role = EXCLUDED.role,
          stack = EXCLUDED.stack,
          grade = EXCLUDED.grade,
          experience_years = EXCLUDED.experience_years,
          rate_min = EXCLUDED.rate_min,
          rate_max = EXCLUDED.rate_max,
          currency = EXCLUDED.currency,
          location = EXCLUDED.location,
          description = EXCLUDED.description,
          original_text = EXCLUDED.original_text,
          embedding = COALESCE(EXCLUDED.embedding, specialists.embedding),
          status = EXCLUDED.status,
          expires_at = NOW() + interval '30 days',
          hired_at = EXCLUDED.hired_at,
          is_internal = TRUE,
          updated_at = NOW()
        RETURNING id
        """
    )
    with engine.begin() as c:
        sid = c.execute(
            q,
            {
                "syn": syn,
                "role": role,
                "stack": json.dumps(stack),
                "grade": grade,
                "exp": exp,
                "rmin": data.get("rate_min"),
                "rmax": data.get("rate_max"),
                "cur": data.get("currency") or "RUB",
                "loc": data.get("location"),
                "desc": data.get("description") or None,
                "orig": original_text,
                "emb": emb,
                "status": status,
                "hired_at": hired_at,
            },
        ).scalar_one()
    return str(sid)


def _upsert_registry_source(
    engine,
    *,
    specialist_id: str,
    source_url: str,
    locator: str,
    raw_text: str,
    source_meta: dict[str, Any],
) -> None:
    channel_id, message_id = _external_source_ids(source_url, locator)
    q = text(
        """
        INSERT INTO sources(
          entity_type, entity_id,
          channel_id, message_id,
          chat_title, sender_id, sender_name,
          message_url, source_type, raw_text,
          external_url, external_kind, external_locator, source_meta
        )
        VALUES (
          'specialist', :specialist_id,
          :channel_id, :message_id,
          'own_specialists_registry', NULL, NULL,
          :message_url, 'own_registry_sync', :raw_text,
          :external_url, 'own_registry', :external_locator, CAST(:source_meta AS jsonb)
        )
        ON CONFLICT(channel_id, message_id) DO UPDATE SET
          entity_id = EXCLUDED.entity_id,
          message_url = EXCLUDED.message_url,
          raw_text = EXCLUDED.raw_text,
          external_url = EXCLUDED.external_url,
          external_kind = EXCLUDED.external_kind,
          external_locator = EXCLUDED.external_locator,
          source_meta = EXCLUDED.source_meta
        """
    )
    with engine.begin() as c:
        c.execute(
            q,
            {
                "specialist_id": specialist_id,
                "channel_id": channel_id,
                "message_id": message_id,
                "message_url": source_url,
                "raw_text": raw_text,
                "external_url": source_url,
                "external_locator": locator,
                "source_meta": json.dumps(source_meta, ensure_ascii=False),
            },
        )


async def sync_own_specialists_registry(
    engine,
    ollama: Any,
    source_fetcher: MCPSourceFetcherClient,
    *,
    source_url: str,
) -> dict[str, int]:
    ensure_own_specialists_registry_table(engine)
    res = await asyncio.to_thread(source_fetcher.fetch_url, source_url)
    if not res.ok:
        raise RuntimeError(res.error or "registry fetch failed")

    seen_keys: set[str] = set()
    stats = {"rows": 0, "saved": 0, "failed": 0, "deactivated": 0}
    existing_by_registry_key, existing_by_identity_key = _load_registry_bindings(engine, source_url)

    for idx, item in enumerate(res.items, start=1):
        raw_text = (getattr(item, "text", "") or "").strip()
        if not raw_text:
            continue
        locator = _registry_locator(item, idx)
        registry_key = _registry_key(source_url, locator)
        seen_keys.add(registry_key)
        stats["rows"] += 1

        try:
            normalized = normalize_short_bench_line(raw_text)
            pre = preprocess_for_llm(normalized, kind="BENCH")
            raw = await ollama.chat(SPECIALIST_EXTRACTION_PROMPT_V2, pre.text, num_predict=1200)
            data = _safe_json_loads(raw)
            items = (data or {}).get("items", [])
            specialist = items[0] if items else build_fallback_specialist_item(pre)
            specialist["is_available"] = resolve_specialist_is_available(specialist, raw_text)
            identity_key = _build_registry_identity_key(source_url, raw_text, specialist, locator)
            status = "active" if bool(specialist.get("is_available", True)) else "hired"
            emb = ollama.embed(build_search_text(specialist))
            previous_binding = existing_by_registry_key.get(registry_key) or existing_by_identity_key.get(identity_key)
            specialist_id = _upsert_internal_specialist(
                engine,
                specialist,
                raw_text,
                emb,
                status,
                identity_key=identity_key,
            )
            _upsert_registry_row(
                engine,
                registry_key=registry_key,
                identity_key=identity_key,
                source_url=source_url,
                locator=locator,
                raw_text=raw_text,
                parsed_payload=specialist,
                specialist_id=specialist_id,
                parse_status="ok",
                last_error=None,
            )
            existing_binding = RegistryBinding(registry_key=registry_key, identity_key=identity_key, specialist_id=specialist_id)
            existing_by_registry_key[registry_key] = existing_binding
            existing_by_identity_key[identity_key] = existing_binding
            if previous_binding and previous_binding.registry_key != registry_key:
                _deactivate_registry_row(engine, previous_binding.registry_key, previous_binding.specialist_id)
            if previous_binding and previous_binding.registry_key == registry_key and previous_binding.specialist_id != specialist_id:
                _deactivate_unbound_internal_specialist(engine, previous_binding.specialist_id)
            _upsert_registry_source(
                engine,
                specialist_id=specialist_id,
                source_url=source_url,
                locator=locator,
                raw_text=raw_text,
                source_meta=_build_registry_source_meta(
                    {"registry_key": registry_key, "identity_key": identity_key, "locator": locator},
                    source_url=source_url,
                    locator=locator,
                    specialist_name=_resolve_registry_specialist_name(raw_text, specialist),
                ),
            )
            stats["saved"] += 1
        except Exception as e:
            log.warning("Own specialist row sync failed locator=%s error=%s", locator, type(e).__name__)
            _upsert_registry_row(
                engine,
                registry_key=registry_key,
                identity_key=None,
                source_url=source_url,
                locator=locator,
                raw_text=raw_text,
                parsed_payload=None,
                specialist_id=None,
                parse_status="failed",
                last_error=f"{type(e).__name__}: {e}",
            )
            stats["failed"] += 1

    stale_keys = set(existing_by_registry_key) - seen_keys
    for registry_key in stale_keys:
        _deactivate_registry_row(engine, registry_key, existing_by_registry_key[registry_key].specialist_id)
        stats["deactivated"] += 1

    with engine.begin() as c:
        active_rows = c.execute(
            text(
                """
                SELECT COUNT(*)
                FROM own_specialists_registry
                WHERE source_url = :source_url AND is_active = TRUE
                """
            ),
            {"source_url": source_url},
        ).scalar_one()
    stats["active_rows"] = int(active_rows or 0)

    return stats
