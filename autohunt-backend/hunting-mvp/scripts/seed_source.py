import sys
from pathlib import Path
from time import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from app.config import get_settings
from app.db.engine import make_engine

RAW = """Коллеги, ищем Java Senior разработчика, Spring Boot, PostgreSQL, опыт от 4 лет, 300-350к, Москва."""

def main():
    s = get_settings()
    engine = make_engine(s.DATABASE_URL)

    # message_id делаем уникальным
    msg_id = int(time())

    with engine.begin() as c:
        res = c.execute(text("""
            INSERT INTO sources(entity_type, entity_id, channel_id, message_id, chat_title, sender_id, sender_name, source_type, raw_text)
            VALUES (NULL, NULL, :channel_id, :message_id, :chat_title, :sender_id, :sender_name, 'manual', :raw_text)
            RETURNING id
        """), {
            "channel_id": 1,
            "message_id": msg_id,
            "chat_title": "manual_test",
            "sender_id": 0,
            "sender_name": "seed",
            "raw_text": RAW
        }).fetchone()
        print("seeded source id:", res[0], "message_id:", msg_id)

if __name__ == "__main__":
    main()
