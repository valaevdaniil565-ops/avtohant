import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from app.config import get_settings
from app.db.engine import make_engine
from app.llm.ollama_client import OllamaClient
from app.llm.prompts import (
    CLASSIFICATION_SYSTEM_PROMPT,
    VACANCY_EXTRACTION_PROMPT,
    SPECIALIST_EXTRACTION_PROMPT,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


def safe_json_load(raw: str) -> Dict[str, Any]:
    if raw is None:
        raise ValueError("LLM returned None")
    s = raw.strip()
    if not s:
        raise ValueError("LLM returned empty response")

    # strip ```json ... ```
    if s.startswith("```"):
        parts = s.split("```")
        if len(parts) >= 3:
            s = parts[1].strip()
            if s.lower().startswith("json"):
                s = s[4:].strip()

    # try parse as-is
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    # extract {...} block (even if text around)
    start = s.find("{")
    end = s.rfind("}")
    if start != -1:
        if end == -1:
            # common truncation: missing closing brace
            candidate = s[start:] + "}"
        else:
            candidate = s[start:end + 1]

        # attempt parse
        try:
            return json.loads(candidate)
        except json.JSONDecodeError as e:
            # another common truncation: missing closing quote or bracket
            # last-resort: if ends with null/true/false/number, try close braces
            # but do not guess too much—raise with head
            raise ValueError(f"Cannot parse JSON after extraction fix. Error={e}. Head: {candidate[:200]!r}") from e

    raise ValueError(f"Cannot parse JSON. Head: {s[:200]!r}")



def build_search_text(payload: Dict[str, Any], fallback_text: str) -> str:
    parts: List[str] = []
    if payload.get("role"):
        parts.append(f"Role: {payload['role']}")
    stack = payload.get("stack")
    if isinstance(stack, list) and stack:
        parts.append("Stack: " + ", ".join([str(x) for x in stack if x]))
    if payload.get("grade"):
        parts.append(f"Grade: {payload['grade']}")
    if payload.get("experience_years") is not None:
        parts.append(f"Exp: {payload['experience_years']}y")
    if payload.get("location"):
        parts.append(f"Location: {payload['location']}")
    if payload.get("company"):
        parts.append(f"Company: {payload['company']}")
    if payload.get("description"):
        parts.append(str(payload["description"])[:600])

    if parts:
        return " | ".join(parts)[:2000]
    return (fallback_text or "")[:2000]


def vec_literal(emb: List[float]) -> str:
    if not isinstance(emb, list) or not emb:
        raise ValueError("Embedding is empty or not a list")
    return "[" + ",".join(f"{float(x):.6f}" for x in emb) + "]"


def classify(llm: OllamaClient, text_in: str) -> str:
    low = (text_in or "").lower()

    bench_hints = [
        "ищу проект", "ищу работу", "в поиске", "open to work", "готов рассмотреть",
        "рассмотрю предложения", "на бенче", "bench", "резюме", "ищу позицию",
        "доступен", "свободен", "готов к выходу"
    ]

    # маркеры вакансии — ДОЛЖНЫ быть, чтобы уверенно сказать VACANCY
    vac_hints_strong = [
        "ищем", "требуется", "вакансия", "нанимаем", "в команду", "открыта позиция",
        "обязанности", "требования", "условия", "мы предлагаем", "оформление"
    ]

    # 1) явный bench
    if any(h in low for h in bench_hints):
        return "BENCH"

    # 2) явная вакансия
    if any(h in low for h in vac_hints_strong):
        return "VACANCY"

    # 3) если это “бенч-чат” — можно принудительно BENCH (дешёвый, но супер-надежный MVP)
    # если хочешь — включим позже через channel_id, но пока оставим универсально

    # 4) fallback на LLM
    kind = llm.generate(
        system=CLASSIFICATION_SYSTEM_PROMPT,
        prompt=text_in,
        temperature=0.1,
        max_tokens=12,
    ).strip().upper()

    if "VAC" in kind:
        return "VACANCY"
    if "BEN" in kind or "SPEC" in kind or "CAND" in kind:
        return "BENCH"
    if kind in ("VACANCY", "BENCH", "OTHER"):
        return kind
    return "OTHER"




def extract_with_retry(llm: OllamaClient, prompt: str) -> Dict[str, Any]:
    extracted = llm.generate(system="", prompt=prompt, temperature=0.2, max_tokens=1600)
    log.info("LLM extracted head: %r", extracted[:300])
    try:
        return safe_json_load(extracted)
    except Exception as e:
        log.warning("JSON parse failed: %s. Retrying strict JSON-only...", e)

    strict_prompt = (
    "Ты парсер. Верни ТОЛЬКО один валидный JSON-объект.\n"
    "Никакого кода, никаких объяснений, никаких markdown-блоков.\n"
    "JSON должен начинаться с '{' и заканчиваться '}'.\n"
    "Если поле неизвестно — null, для массивов — [].\n\n"
    + prompt
    )

    extracted2 = llm.generate(system="", prompt=strict_prompt, temperature=0.0, max_tokens=1600)
    log.info("LLM extracted2 head: %r", extracted2[:300])
    return safe_json_load(extracted2)


def main() -> None:
    s = get_settings()
    engine = make_engine(s.DATABASE_URL)

    base_url = getattr(s, "OLLAMA_BASE_URL", None) or getattr(s, "OLLAMA_HOST", None) or "http://localhost:11434"
    model = getattr(s, "OLLAMA_MODEL", None) or getattr(s, "LLM_MODEL", None) or "llama3:8b"
    embed_model = getattr(s, "EMBED_MODEL", None) or "nomic-embed-text"
    llm = OllamaClient(base_url=base_url, model=model, embed_model=embed_model)

    # 1) take one unprocessed source
    with engine.begin() as c:
        row = c.execute(
            text(
                """
                SELECT id, raw_text
                FROM sources
                WHERE entity_type IS NULL
                  AND raw_text IS NOT NULL
                  AND length(raw_text) > 0
                ORDER BY created_at
                LIMIT 1
                """
            )
        ).fetchone()

        if not row:
            print("No unprocessed sources.")
            return

        source_id, raw_text = row[0], row[1]
        log.info("Processing source=%s", source_id)

    try:
        # 2) classification
        kind = classify(llm, raw_text)
        log.info("Classified as: %s", kind)

        if kind == "OTHER":
            with engine.begin() as c:
                c.execute(text("UPDATE sources SET entity_type='other' WHERE id=:id"), {"id": source_id})
            log.info("Marked as OTHER")
            return

        # 3) extraction
        now_utc = datetime.now(timezone.utc)
        expires_at = now_utc + timedelta(days=30)

        if kind == "VACANCY":
            prompt = VACANCY_EXTRACTION_PROMPT + "\n\nТЕКСТ:\n" + raw_text
            payload = extract_with_retry(llm, prompt)
            is_closed = bool(payload.get("is_closed", False))
            entity_type = "vacancy"
        else:
            prompt = SPECIALIST_EXTRACTION_PROMPT + "\n\nТЕКСТ:\n" + raw_text
            payload = extract_with_retry(llm, prompt)
            is_closed = not bool(payload.get("is_available", True))
            entity_type = "specialist"

        status = "closed" if is_closed else "active"

        # 4) embedding
        search_text = build_search_text(payload, raw_text)
        emb = llm.embed(search_text)
        emb_lit = vec_literal(emb)

        # 5) insert + link
        with engine.begin() as c:
            if entity_type == "vacancy":
                entity_id = c.execute(
                    text(
                        """
                        INSERT INTO vacancies(
                          role, stack, grade, experience_years,
                          rate_min, rate_max, currency,
                          company, location, description,
                          original_text, embedding, status, expires_at
                        )
                        VALUES (
                          :role,
                          CAST(:stack AS jsonb),
                          :grade,
                          :experience_years,
                          :rate_min,
                          :rate_max,
                          :currency,
                          :company,
                          :location,
                          :description,
                          :original_text,
                          CAST(:embedding AS vector),
                          :status,
                          :expires_at
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "role": payload.get("role") or "Unknown",
                        "stack": json.dumps(payload.get("stack") or []),
                        "grade": payload.get("grade"),
                        "experience_years": payload.get("experience_years"),
                        "rate_min": payload.get("rate_min"),
                        "rate_max": payload.get("rate_max"),
                        "currency": payload.get("currency") or "RUB",
                        "company": payload.get("company"),
                        "location": payload.get("location"),
                        "description": payload.get("description"),
                        "original_text": raw_text,
                        "embedding": emb_lit,
                        "status": status,
                        "expires_at": expires_at,
                    },
                ).fetchone()[0]
            else:
                entity_id = c.execute(
                    text(
                        """
                        INSERT INTO specialists(
                          role, stack, grade, experience_years,
                          rate_min, rate_max, currency,
                          location, description,
                          original_text, embedding, status, expires_at
                        )
                        VALUES (
                          :role,
                          CAST(:stack AS jsonb),
                          :grade,
                          :experience_years,
                          :rate_min,
                          :rate_max,
                          :currency,
                          :location,
                          :description,
                          :original_text,
                          CAST(:embedding AS vector),
                          :status,
                          :expires_at
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "role": payload.get("role") or "Unknown",
                        "stack": json.dumps(payload.get("stack") or []),
                        "grade": payload.get("grade"),
                        "experience_years": payload.get("experience_years"),
                        "rate_min": payload.get("rate_min"),
                        "rate_max": payload.get("rate_max"),
                        "currency": payload.get("currency") or "RUB",
                        "location": payload.get("location"),
                        "description": payload.get("description"),
                        "original_text": raw_text,
                        "embedding": emb_lit,
                        "status": status,
                        "expires_at": expires_at,
                    },
                ).fetchone()[0]

            c.execute(
                text(
                    """
                    UPDATE sources
                    SET entity_type=:entity_type, entity_id=:entity_id
                    WHERE id=:source_id
                    """
                ),
                {"entity_type": entity_type, "entity_id": entity_id, "source_id": source_id},
            )

        log.info("Created %s id=%s and linked source=%s", entity_type, entity_id, source_id)

    except Exception as e:
        log.exception("Failed to process source=%s: %s", source_id, e)
        with engine.begin() as c:
            c.execute(text("UPDATE sources SET entity_type='other' WHERE id=:id"), {"id": source_id})
        return


if __name__ == "__main__":
    main()
