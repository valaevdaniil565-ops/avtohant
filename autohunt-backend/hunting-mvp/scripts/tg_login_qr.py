import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient


def ensure_session_dir(session_name: str) -> str:
    p = Path(session_name).expanduser()
    if p.parent and str(p.parent) not in {".", ""}:
        p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


async def main():
    load_dotenv()

    api_id = int(os.getenv("TG_API_ID", "0"))
    api_hash = os.getenv("TG_API_HASH", "").strip()
    session_name = ensure_session_dir(
        os.getenv("TG_SESSION_NAME", "storage/sessions/collector").strip()
    )

    if not api_id or not api_hash:
        raise SystemExit("TG_API_ID/TG_API_HASH не заданы в .env")

    client = TelegramClient(session_name, api_id, api_hash)  # логин лучше делать без прокси
    await client.connect()

    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            print("✅ Уже авторизован:", me.username or me.first_name)
            return

        qr = await client.qr_login()

        out_dir = Path("storage/sessions").expanduser()
        out_dir.mkdir(parents=True, exist_ok=True)
        png_path = out_dir / "tg_login_qr.png"

        try:
            import qrcode
            from qrcode.image.pil import PilImage

            # PNG QR
            img = qrcode.make(qr.url, image_factory=PilImage)
            img.save(png_path)
            print(f"✅ QR сохранён: {png_path}")

            # ASCII QR (на всякий)
            qr2 = qrcode.QRCode(border=1)
            qr2.add_data(qr.url)
            qr2.make(fit=True)

            print("\n=== ASCII QR (можно сканировать прямо с экрана терминала) ===\n")
            qr2.print_ascii(invert=True)

            print(
                "\nСканируй QR в Telegram на телефоне: Settings → Devices → Link Desktop Device"
            )

        except Exception as e:
            print(f"❌ Не смог создать PNG/ASCII QR: {e}")
            print("Поставь зависимости: pip install -U pillow 'qrcode[pil]'")
            print("Вот ссылка (можно открыть на телефоне):")
            print(qr.url)

        print("\nЖду сканирования QR (до 120 сек)...")
        await qr.wait(timeout=120)

        me = await client.get_me()
        print("✅ Вход выполнен:", me.username or me.first_name)
        print("Session сохранена в:", session_name + ".session")

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
