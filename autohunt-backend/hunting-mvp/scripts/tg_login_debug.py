import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError


def ensure_session_dir(session_name: str) -> str:
    p = Path(session_name).expanduser()
    if p.parent and str(p.parent) not in {".", ""}:
        p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


async def main():
    load_dotenv()

    api_id = int(os.getenv("TG_API_ID", "0"))
    api_hash = os.getenv("TG_API_HASH", "").strip()
    session_name = ensure_session_dir(os.getenv("TG_SESSION_NAME", "storage/sessions/collector").strip())

    if not api_id or not api_hash:
        raise SystemExit("TG_API_ID/TG_API_HASH не заданы")

    phone = input("Phone (международный формат, например +49... или +7...): ").strip()

    client = TelegramClient(session_name, api_id, api_hash)  # без прокси
    await client.connect()

    try:
        sent = await client.send_code_request(phone)
        # Важно: тип доставки кода
        t = getattr(sent.type, "__class__", type(sent.type)).__name__
        print(f"Код отправлен. Тип доставки: {t}")
        print("Проверь: Telegram (чат с 'Telegram'), Архив, или SMS/звонок — зависит от типа.")

        code = input("Введите код: ").strip()
        try:
            await client.sign_in(phone=phone, code=code)
        except SessionPasswordNeededError:
            pwd = input("Включена 2FA. Введите пароль: ").strip()
            await client.sign_in(password=pwd)

        print("✅ Успех: сессия сохранена в", session_name + ".session")
        me = await client.get_me()
        print("Logged in as:", me.username or me.first_name)

    except FloodWaitError as e:
        print(f"⛔ FloodWait: подожди {e.seconds} секунд и повтори (не спамь запросами).")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
