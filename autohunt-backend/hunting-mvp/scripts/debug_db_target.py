import asyncio
import os
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def main():
    load_dotenv()
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise SystemExit("DATABASE_URL not set")

    engine = create_async_engine(db_url, pool_pre_ping=True)

    async with engine.begin() as conn:
        db = (await conn.execute(text("SELECT current_database()"))).scalar_one()
        usr = (await conn.execute(text("SELECT current_user"))).scalar_one()
        total = (await conn.execute(text("SELECT COUNT(*) FROM channels"))).scalar_one()
        active = (await conn.execute(text("SELECT COUNT(*) FROM channels WHERE is_active = TRUE"))).scalar_one()

    await engine.dispose()

    print("DATABASE_URL =", db_url)
    print("current_database() =", db)
    print("current_user =", usr)
    print("channels.total =", total)
    print("channels.active =", active)


if __name__ == "__main__":
    asyncio.run(main())
