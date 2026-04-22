import asyncio
import os
import sys
from dotenv import load_dotenv

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def main():
    load_dotenv()
    db = os.getenv("DATABASE_URL", "")
    if not db:
        raise SystemExit("DATABASE_URL не задан")

    if len(sys.argv) < 3:
        raise SystemExit("Usage: python scripts/add_channel.py <peer_id> <title> [username]")

    peer_id = int(sys.argv[1])
    title = sys.argv[2]
    username = sys.argv[3].lstrip("@") if len(sys.argv) >= 4 else None

    engine = create_async_engine(db, pool_pre_ping=True)

    q = text("""
        INSERT INTO channels(telegram_id, title, username, is_active, last_message_id)
        VALUES (:tid, :title, :username, TRUE, 0)
        ON CONFLICT (telegram_id) DO UPDATE SET
            title = EXCLUDED.title,
            username = COALESCE(EXCLUDED.username, channels.username),
            is_active = TRUE,
            updated_at = NOW()
    """)

    async with engine.begin() as conn:
        await conn.execute(q, {"tid": peer_id, "title": title, "username": username})

    await engine.dispose()
    print(f"OK: added/updated channel telegram_id={peer_id} title={title!r} username={username!r}")


if __name__ == "__main__":
    asyncio.run(main())
