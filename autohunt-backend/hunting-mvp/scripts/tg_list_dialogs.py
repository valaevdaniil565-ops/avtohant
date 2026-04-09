import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon import utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def ensure_session_dir(session_name: str) -> str:
    p = Path(session_name).expanduser()
    # если указали путь с папками (storage/sessions/collector) — надо создать родительскую папку
    if p.parent and str(p.parent) not in {".", ""}:
        p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


async def main():
    load_dotenv()

    api_id = int(os.getenv("TG_API_ID", "0"))
    api_hash = os.getenv("TG_API_HASH", "").strip()
    session_name = os.getenv("TG_SESSION_NAME", "storage/sessions/collector").strip()

    if not api_id or not api_hash:
        raise SystemExit("TG_API_ID/TG_API_HASH не заданы")

    session_name = ensure_session_dir(session_name)

    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()

    print("\n=== Dialogs (take peer_id as channels.telegram_id) ===\n")
    i = 0
    async for d in client.iter_dialogs():
        i += 1
        ent = d.entity
        peer_id = utils.get_peer_id(ent)  # <-- это и нужно сохранять в БД
        title = getattr(ent, "title", None) or getattr(ent, "first_name", None) or str(peer_id)
        username = getattr(ent, "username", None)
        kind = ent.__class__.__name__
        print(f"{i:>3}. peer_id={peer_id:<15} kind={kind:<18} title={title!r} username={username!r}")
        if i >= 80:
            break

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
