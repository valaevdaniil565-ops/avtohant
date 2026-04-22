import sys
from pathlib import Path
from time import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from app.config import get_settings
from app.db.engine import make_engine

BENCHES = [
    "Ищу проект: Java Senior, Spring Boot, PostgreSQL, Kafka, 5 лет, Москва, 320к.",
    "Java Middle разработчик, Spring, Hibernate, PostgreSQL, 3 года, удалёнка, 220к.",
    "Senior Backend: Java, Spring Boot, Microservices, PostgreSQL, 6 лет, Москва, 380к.",
    "Python Senior, FastAPI, PostgreSQL, 5 лет, удалёнка, 330к.",
]

def main():
    s = get_settings()
    engine = make_engine(s.DATABASE_URL)

    with engine.begin() as c:
        for i, raw in enumerate(BENCHES):
            msg_id = int(time()) + i
            res = c.execute(text("""
                INSERT INTO sources(entity_type, entity_id, channel_id, message_id, chat_title, sender_id, sender_name, source_type, raw_text)
                VALUES (NULL, NULL, :channel_id, :message_id, :chat_title, :sender_id, :sender_name, 'manual', :raw_text)
                RETURNING id
            """), {
                "channel_id": 2,
                "message_id": msg_id,
                "chat_title": "bench_test",
                "sender_id": 0,
                "sender_name": "seed_bench",
                "raw_text": raw
            }).fetchone()
            print("seeded bench source:", res[0], "message_id:", msg_id)

if __name__ == "__main__":
    main()
