import json
import logging
import asyncio
import re

from telegram.constants import ParseMode
from app.utils.tg_links import extract_forwarded_message_url, parse_message_url
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from sqlalchemy import text

from app.db.engine import make_engine
from app.llm.ollama_client import OllamaClient
from app.llm.prompts import (
    CLASSIFICATION_SYSTEM_PROMPT,
    VACANCY_EXTRACTION_PROMPT,
    SPECIALIST_EXTRACTION_PROMPT,
)

log = logging.getLogger(__name__)

TOP_K = 10

BULLET_RE = re.compile(r"^\s*[–\-•]\s+", re.MULTILINE)


def cmd_start():
    async def _start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Я готов.\n"
            "Пришли текст вакансии/бенча (можно пересланное сообщение).\n"
            "Команды: /id, /export"
        )

    return CommandHandler("start", _start)


def cmd_id():
    async def _id(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id if update.effective_chat else None
        user_id = update.effective_user.id if update.effective_user else None
        await update.message.reply_text(f"chat_id={chat_id}\nuser_id={user_id}")

    return CommandHandler("id", _id)


def cmd_export():
    async def _export(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Экспорт подключим следующим шагом (openpyxl).")

    return CommandHandler("export", _export)


def on_callback():
    async def _cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()

        s = context.application.bot_data.get("settings")
        if s is None:
            await q.edit_message_text("Ошибка конфигурации: settings не переданы в bot_data.")
            return

        data = (q.data or "")
        if data.startswith("hide:"):
            parts = data.split(":")
            if len(parts) != 3:
                await q.edit_message_text("Некорректная команда.")
                return

            entity_type, entity_id = parts[1], parts[2]
            table = "vacancies" if entity_type == "vacancy" else "specialists"

            engine = make_engine(s.DATABASE_URL)
            with engine.begin() as c:
                c.execute(
                    text(f"UPDATE {table} SET status='hidden' WHERE id=CAST(:id AS uuid)"),
                    {"id": entity_id},
                )

            await q.edit_message_text(f"Скрыто: {entity_type} {entity_id}")
            return

        await q.edit_message_text("Неизвестная команда.")

    return CallbackQueryHandler(_cb)


def on_error():
    async def _err(update: object, context: ContextTypes.DEFAULT_TYPE):
        log.exception("Bot error: %s", context.error)
        try:
            if isinstance(update, Update) and update.effective_chat:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"Ошибка: {context.error}",
                )
        except Exception:
            pass

    return _err


def _extract_json_from_text(s: str) -> dict:
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty LLM response")

    if s.startswith("```"):
        parts = s.split("```")
        if len(parts) >= 3:
            s = parts[1].strip()
            if s.lower().startswith("json"):
                s = s[4:].strip()

    start, end = s.find("{"), s.rfind("}")
    if start != -1 and end != -1 and end > start:
        s = s[start : end + 1]
    elif start != -1 and end == -1:
        s = s[start:] + "}"

    return json.loads(s)


def _vec_literal(emb: list[float]) -> str:
    return "[" + ",".join(f"{float(x):.6f}" for x in emb) + "]"


def _is_bulk_bench(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    bullets = len(BULLET_RE.findall(t))
    hints = ["доступны", "специалист", "анкеты", "кейсы", "ставки", "в л/с", "усилить команду", "топовые"]
    return bullets >= 5 and any(h in t.lower() for h in hints)


def _split_bench_items(text: str) -> list[str]:
    items = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(("–", "-", "•")):
            item = line.lstrip("–-•").strip()
            if len(item) < 6:
                continue
            items.append(item)
    return items


def on_message():
    async def _msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
        s = context.application.bot_data.get("settings")
        if s is None:
            await update.message.reply_text("Ошибка конфигурации: settings не переданы в bot_data.")
            return

        chat_id = update.effective_chat.id if update.effective_chat else None
        raw_acl = getattr(s, "MANAGER_CHAT_IDS", None)

        # ACL: если блок — объясняем, а не молчим
        if raw_acl:
            try:
                allowed = {int(x.strip()) for x in str(raw_acl).split(",") if x.strip()}
            except Exception as e:
                await update.message.reply_text(
                    f"ACL parse error: {e}\nMANAGER_CHAT_IDS должен быть числами через запятую."
                )
                return
            if chat_id not in allowed:
                await update.message.reply_text(f"ACL blocked. chat_id={chat_id} not in {sorted(allowed)}")
                return

        text_in = (update.message.text or update.message.caption or "").strip()
        if not text_in:
            await update.message.reply_text("Пришли текст (сообщение должно содержать текст).")
            return

        ack = await update.message.reply_text("Принял. Обрабатываю…")

        def _process_bulk_bench_sync(items: list[str]) -> int:
            engine = make_engine(s.DATABASE_URL)
            llm = OllamaClient(
                base_url=getattr(s, "OLLAMA_HOST", "http://localhost:11434"),
                model=getattr(s, "LLM_MODEL", "llama3:8b"),
                embed_model=getattr(s, "EMBED_MODEL", "nomic-embed-text"),
            )

            # 0) parent source: 1 запись на исходное сообщение (уникальная по (channel_id,message_id))
            with engine.begin() as c:
                c.execute(
                    text(
                        """
                        INSERT INTO sources(entity_type, entity_id, channel_id, message_id, chat_title,
                                            sender_id, sender_name, source_type, raw_text)
                        VALUES (NULL, NULL, :channel_id, :message_id, :chat_title,
                                :sender_id, :sender_name, 'bulk_parent', :raw_text)
                        ON CONFLICT (channel_id, message_id) DO UPDATE
                          SET raw_text = EXCLUDED.raw_text
                        """
                    ),
                    {
                        "channel_id": update.effective_chat.id,
                        "message_id": update.message.message_id,
                        "chat_title": update.effective_chat.title or "direct",
                        "sender_id": update.effective_user.id if update.effective_user else 0,
                        "sender_name": update.effective_user.full_name if update.effective_user else "unknown",
                        "raw_text": text_in,
                    },
                )

            inserted = 0
            base_mid = int(update.message.message_id)

            for i, item in enumerate(items, 1):
                synthetic_mid = base_mid * 1000 + i  # уникально для каждого пункта

                # 1) child source для пункта (уникальный message_id)
                with engine.begin() as c:
                    src_id = c.execute(
                        text(
                            """
                            INSERT INTO sources(entity_type, entity_id, channel_id, message_id, chat_title,
                                                sender_id, sender_name, source_type, raw_text)
                            VALUES (NULL, NULL, :channel_id, :message_id, :chat_title,
                                    :sender_id, :sender_name, 'bulk_bench_item', :raw_text)
                            ON CONFLICT (channel_id, message_id) DO UPDATE
                              SET raw_text = EXCLUDED.raw_text
                            RETURNING id
                            """
                        ),
                        {
                            "channel_id": update.effective_chat.id,
                            "message_id": synthetic_mid,
                            "chat_title": update.effective_chat.title or "direct",
                            "sender_id": update.effective_user.id if update.effective_user else 0,
                            "sender_name": update.effective_user.full_name if update.effective_user else "unknown",
                            "raw_text": item,
                        },
                    ).fetchone()[0]

                # 2) extract specialist (принудительно)
                strict_prompt = (
                    "Ты парсер. Верни ТОЛЬКО один валидный JSON-объект.\n"
                    "Никакого кода, никаких объяснений, никаких markdown-блоков.\n"
                    "JSON должен начинаться с '{' и заканчиваться '}'.\n"
                    "Если поле неизвестно — null, для массивов — [].\n\n"
                    + SPECIALIST_EXTRACTION_PROMPT
                    + "\n\nТЕКСТ:\n"
                    + item
                )

                extracted = llm.generate(system="", prompt=strict_prompt, temperature=0.0, max_tokens=1200)
                try:
                    data = _extract_json_from_text(extracted)
                except Exception:
                    extracted2 = llm.generate(system="", prompt=strict_prompt, temperature=0.0, max_tokens=1600)
                    data = _extract_json_from_text(extracted2)

                # 3) embedding
                emb = llm.embed(item[:2000])
                emb_lit = _vec_literal(emb)

                # 4) insert specialist + link source
                with engine.begin() as c:
                    specialist_id = c.execute(
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
                              :exp,
                              :rmin,
                              :rmax,
                              :cur,
                              :loc,
                              :desc,
                              :orig,
                              CAST(:emb AS vector),
                              'active',
                              NOW() + interval '30 days'
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "role": data.get("role") or "Unknown",
                            "stack": json.dumps(data.get("stack") or []),
                            "grade": data.get("grade"),
                            "exp": data.get("experience_years"),
                            "rmin": data.get("rate_min"),
                            "rmax": data.get("rate_max"),
                            "cur": data.get("currency") or "RUB",
                            "loc": data.get("location"),
                            "desc": data.get("description"),
                            "orig": item,
                            "emb": emb_lit,
                        },
                    ).fetchone()[0]

                    c.execute(
                        text("UPDATE sources SET entity_type='specialist', entity_id=:eid WHERE id=:sid"),
                        {"eid": specialist_id, "sid": src_id},
                    )

                inserted += 1

            return inserted

        def _process_single_sync() -> tuple[str, InlineKeyboardMarkup | None]:
            engine = make_engine(s.DATABASE_URL)
            llm = OllamaClient(
                base_url=getattr(s, "OLLAMA_HOST", "http://localhost:11434"),
                model=getattr(s, "LLM_MODEL", "llama3:8b"),
                embed_model=getattr(s, "EMBED_MODEL", "nomic-embed-text"),
            )

            # save source
            with engine.begin() as c:
                src_id = c.execute(
                    text(
                        """
                        INSERT INTO sources(entity_type, entity_id, channel_id, message_id, chat_title,
                                            sender_id, sender_name, source_type, raw_text)
                        VALUES (NULL, NULL, :channel_id, :message_id, :chat_title,
                                :sender_id, :sender_name, 'forward', :raw_text)
                        ON CONFLICT (channel_id, message_id) DO UPDATE
                          SET raw_text = EXCLUDED.raw_text
                        RETURNING id
                        """
                    ),
                    {
                        "channel_id": update.effective_chat.id,
                        "message_id": update.message.message_id,
                        "chat_title": update.effective_chat.title or "direct",
                        "sender_id": update.effective_user.id if update.effective_user else 0,
                        "sender_name": update.effective_user.full_name if update.effective_user else "unknown",
                        "raw_text": text_in,
                    },
                ).fetchone()[0]

            # classify
            kind = llm.generate(
                system=CLASSIFICATION_SYSTEM_PROMPT,
                prompt=text_in,
                temperature=0.1,
                max_tokens=12,
            ).strip().upper()

            if "VAC" in kind:
                kind = "VACANCY"
            elif "BEN" in kind or "SPEC" in kind or "CAND" in kind:
                kind = "BENCH"
            else:
                kind = "OTHER"

            if kind == "OTHER":
                with engine.begin() as c:
                    c.execute(text("UPDATE sources SET entity_type='other' WHERE id=:id"), {"id": src_id})
                return ("Не смог определить тип. Пометил как OTHER.", None)

            if kind == "VACANCY":
                base_prompt = VACANCY_EXTRACTION_PROMPT
                entity_type = "vacancy"
            else:
                base_prompt = SPECIALIST_EXTRACTION_PROMPT
                entity_type = "specialist"

            strict_prompt = (
                "Ты парсер. Верни ТОЛЬКО один валидный JSON-объект.\n"
                "Никакого кода, никаких объяснений, никаких markdown-блоков.\n"
                "JSON должен начинаться с '{' и заканчиваться '}'.\n"
                "Если поле неизвестно — null, для массивов — [].\n\n"
                + base_prompt
                + "\n\nТЕКСТ:\n"
                + text_in
            )

            extracted = llm.generate(system="", prompt=strict_prompt, temperature=0.0, max_tokens=2000)
            try:
                data = _extract_json_from_text(extracted)
            except Exception:
                extracted2 = llm.generate(system="", prompt=strict_prompt, temperature=0.0, max_tokens=2500)
                data = _extract_json_from_text(extracted2)

            emb = llm.embed(text_in[:2000])
            emb_lit = _vec_literal(emb)

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
                              :role, CAST(:stack AS jsonb), :grade, :exp,
                              :rmin, :rmax, :cur,
                              :comp, :loc, :desc,
                              :orig, CAST(:emb AS vector), 'active', NOW() + interval '30 days'
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "role": data.get("role") or "Unknown",
                            "stack": json.dumps(data.get("stack") or []),
                            "grade": data.get("grade"),
                            "exp": data.get("experience_years"),
                            "rmin": data.get("rate_min"),
                            "rmax": data.get("rate_max"),
                            "cur": data.get("currency") or "RUB",
                            "comp": data.get("company"),
                            "loc": data.get("location"),
                            "desc": data.get("description"),
                            "orig": text_in,
                            "emb": emb_lit,
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
                              :role, CAST(:stack AS jsonb), :grade, :exp,
                              :rmin, :rmax, :cur,
                              :loc, :desc,
                              :orig, CAST(:emb AS vector), 'active', NOW() + interval '30 days'
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "role": data.get("role") or "Unknown",
                            "stack": json.dumps(data.get("stack") or []),
                            "grade": data.get("grade"),
                            "exp": data.get("experience_years"),
                            "rmin": data.get("rate_min"),
                            "rmax": data.get("rate_max"),
                            "cur": data.get("currency") or "RUB",
                            "loc": data.get("location"),
                            "desc": data.get("description"),
                            "orig": text_in,
                            "emb": emb_lit,
                        },
                    ).fetchone()[0]

                c.execute(
                    text("UPDATE sources SET entity_type=:t, entity_id=:eid WHERE id=:sid"),
                    {"t": entity_type, "eid": entity_id, "sid": src_id},
                )

            # если BENCH — не матчим
            if entity_type == "specialist":
                return (f"Загружен бенч в БД: {entity_id}", None)

            # vacancy -> match
            with engine.begin() as c:
                rows = c.execute(
                    text(
                        f"""
                        SELECT id, role, location, (1 - (embedding <=> CAST(:emb AS vector))) AS sim
                        FROM specialists
                        WHERE status='active' AND embedding IS NOT NULL
                        ORDER BY embedding <=> CAST(:emb AS vector)
                        LIMIT {TOP_K}
                        """
                    ),
                    {"emb": emb_lit},
                ).fetchall()

            if not rows:
                return ("Вакансия загружена, но специалистов для сравнения пока нет.", None)

            lines = []
            for i, r in enumerate(rows, 1):
                lines.append(f"{i:02d}. {r[1]} | {r[2]} | {float(r[3]):.3f} | {r[0]}")

            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Скрыть эту запись", callback_data=f"hide:vacancy:{entity_id}")]]
            )
            return ("TOP:\n" + "\n".join(lines), kb)

        try:
            # BULK BENCH: грузим много специалистов, без TOP
            if _is_bulk_bench(text_in):
                items = _split_bench_items(text_in)
                if not items:
                    await ack.edit_text("Похоже на bulk-бенч, но не нашёл пунктов со специалистами.")
                    return

                n = await asyncio.wait_for(asyncio.to_thread(_process_bulk_bench_sync, items), timeout=240)
                await ack.edit_text(f"Загружено специалистов: {n}\n(матчинг не выполнялся)")
                return

            # SINGLE
            text_out, kb = await asyncio.wait_for(asyncio.to_thread(_process_single_sync), timeout=240)
            if kb:
                await ack.edit_text(text_out, reply_markup=kb)
            else:
                await ack.edit_text(text_out)

        except asyncio.TimeoutError:
            await ack.edit_text("Не успел обработать. Проверь Ollama/DB и попробуй ещё раз.")
        except Exception as e:
            log.exception("Message processing failed: %s", e)
            await ack.edit_text(f"Ошибка обработки: {e}")

    return MessageHandler(filters.TEXT | filters.CAPTION, _msg)
