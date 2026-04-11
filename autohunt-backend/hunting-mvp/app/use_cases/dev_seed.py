from __future__ import annotations

from sqlalchemy import text

from app.db.repo import Repo


def _insert_source(
    engine,
    *,
    entity_type: str,
    entity_id: str,
    message_id: int,
    chat_title: str,
    sender_name: str,
    message_url: str,
    raw_text: str,
) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO sources(
                  entity_type, entity_id, channel_id, message_id, chat_title, sender_id, sender_name, message_url, source_type, raw_text, source_meta
                )
                VALUES (
                  :entity_type, CAST(:entity_id AS uuid), -1, :message_id, :chat_title, 0, :sender_name, :message_url, 'manual', :raw_text,
                  '{"source_display":"demo_seed"}'::jsonb
                )
                ON CONFLICT(channel_id, message_id) DO NOTHING
                """
            ),
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "message_id": message_id,
                "chat_title": chat_title,
                "sender_name": sender_name,
                "message_url": message_url,
                "raw_text": raw_text,
            },
        )


def _upsert_match(engine, *, vacancy_id: str, specialist_id: str, score: float, rank: int) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO matches(vacancy_id, specialist_id, similarity_score, rank)
                VALUES (CAST(:vacancy_id AS uuid), CAST(:specialist_id AS uuid), :score, :rank)
                ON CONFLICT(vacancy_id, specialist_id) DO UPDATE
                  SET similarity_score = EXCLUDED.similarity_score,
                      rank = EXCLUDED.rank,
                      updated_at = NOW()
                """
            ),
            {
                "vacancy_id": vacancy_id,
                "specialist_id": specialist_id,
                "score": score,
                "rank": rank,
            },
        )


def seed_demo_data(engine, repo: Repo) -> dict[str, int]:
    vacancies = [
        {
            "role": "Senior Python Backend Engineer",
            "stack": ["Python", "FastAPI", "PostgreSQL", "Docker"],
            "grade": "Senior",
            "rate_min": 280000,
            "rate_max": 360000,
            "currency": "RUB",
            "company": "Avtohunt Demo",
            "location": "Remote",
            "description": "FastAPI backend, integrations, async services, PostgreSQL.",
            "original_text": "Ищем Senior Python Backend Engineer: FastAPI, PostgreSQL, Docker, remote, 280-360k.",
        },
        {
            "role": "Java Developer",
            "stack": ["Java", "Spring Boot", "PostgreSQL", "Kafka"],
            "grade": "Middle",
            "rate_min": 220000,
            "rate_max": 280000,
            "currency": "RUB",
            "company": "Partner Demo",
            "location": "Moscow / Hybrid",
            "description": "Spring Boot services with Kafka and PostgreSQL.",
            "original_text": "Нужен Java Developer: Spring Boot, Kafka, PostgreSQL, hybrid, 220-280k.",
        },
    ]

    specialists = [
        {
            "role": "Senior Python Developer",
            "stack": ["Python", "FastAPI", "PostgreSQL"],
            "grade": "Senior",
            "rate_min": 300000,
            "rate_max": 350000,
            "currency": "RUB",
            "location": "Remote",
            "description": "5+ years in backend development, APIs, async Python.",
            "original_text": "Senior Python Developer, FastAPI, PostgreSQL, remote, 300-350k.",
            "is_internal": True,
        },
        {
            "role": "Java Engineer",
            "stack": ["Java", "Spring Boot", "PostgreSQL"],
            "grade": "Middle",
            "rate_min": 230000,
            "rate_max": 260000,
            "currency": "RUB",
            "location": "Moscow",
            "description": "Spring Boot backend engineer with PostgreSQL.",
            "original_text": "Java Engineer, Spring Boot, PostgreSQL, Moscow, 230-260k.",
            "is_internal": False,
        },
    ]

    vacancy_ids: list[str] = []
    specialist_ids: list[str] = []

    for idx, item in enumerate(vacancies, start=1):
        vacancy_id = repo.upsert_vacancy(item, item["original_text"], None, "active")
        vacancy_ids.append(vacancy_id)
        _insert_source(
            engine,
            entity_type="vacancy",
            entity_id=vacancy_id,
            message_id=1000 + idx,
            chat_title="demo_vacancies",
            sender_name="demo_seed",
            message_url=f"https://demo.local/vacancies/{idx}",
            raw_text=item["original_text"],
        )

    for idx, item in enumerate(specialists, start=1):
        specialist_id = repo.upsert_specialist(item, item["original_text"], None, "active")
        specialist_ids.append(specialist_id)
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE specialists SET is_internal = :is_internal WHERE id = CAST(:id AS uuid)"),
                {"id": specialist_id, "is_internal": bool(item.get("is_internal"))},
            )
        _insert_source(
            engine,
            entity_type="specialist",
            entity_id=specialist_id,
            message_id=2000 + idx,
            chat_title="demo_bench",
            sender_name="demo_seed",
            message_url=f"https://demo.local/specialists/{idx}",
            raw_text=item["original_text"],
        )

    if len(vacancy_ids) >= 2 and len(specialist_ids) >= 2:
        _upsert_match(engine, vacancy_id=vacancy_ids[0], specialist_id=specialist_ids[0], score=0.94, rank=1)
        _upsert_match(engine, vacancy_id=vacancy_ids[1], specialist_id=specialist_ids[1], score=0.89, rank=1)

    return {
        "vacancies": len(vacancy_ids),
        "specialists": len(specialist_ids),
        "matches": 2 if len(vacancy_ids) >= 2 and len(specialist_ids) >= 2 else 0,
    }
