import asyncio
import logging
import re
import sqlite3
import time
from collections import defaultdict
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from openai import AsyncOpenAI

# ================= НАСТРОЙКИ =================
BOT_TOKEN = "8734583084:AAE0KNAjDy5htEH8t3HQlIGekx-T1wAo9mA"
SUPPORT_CHAT_ID = -1004241108121  # ID группы админов (число с минусом)
DEEPSEEK_API_KEY = "sk-ВАШ_КЛЮЧ_ОТ_DEEPSEEK"

DB_FILE = "bot_data.db"

# Анти-спам: макс. число сообщений за интервал секунд
SPAM_LIMIT = 5
SPAM_WINDOW = 3.0
# =============================================

dp = Dispatcher()
user_message_history = defaultdict(list)

# Прямое подключение к DeepSeek (без прокси)
ai_client = AsyncOpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

# ================= БАЗА ДАННЫХ (SQLITE) =================
def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                forwarded_id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                user_name TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY
            )
        """)
        conn.commit()


def save_ticket(forwarded_id: int, user_id: int, user_name: str):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tickets (forwarded_id, user_id, user_name) VALUES (?, ?, ?)",
            (forwarded_id, user_id, user_name)
        )
        conn.commit()


def get_ticket(forwarded_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, user_name FROM tickets WHERE forwarded_id = ?", (forwarded_id,))
        row = cur.fetchone()
        if row:
            return {"id": row[0], "name": row[1]}
        return None


def add_banned_user(user_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)", (user_id,))
        conn.commit()


def remove_banned_user(user_id: int):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
        conn.commit()


def is_user_banned(user_id: int) -> bool:
    with sqlite3.connect(DB_FILE) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM banned_users WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None
# ========================================================


def is_spamming(user_id: int) -> bool:
    now = time.time()
    user_message_history[user_id] = [
        t for t in user_message_history[user_id] if now - t < SPAM_WINDOW
    ]
    user_message_history[user_id].append(now)
    return len(user_message_history[user_id]) > SPAM_LIMIT


def extract_url(text: str) -> str:
    if not text:
        return None
    url_match = re.search(r"https?://[^\s]+", text)
    return url_match.group(0) if url_match else None


async def summarize_link(url: str) -> str:
    """Анализ ссылки через официальный DeepSeek."""
    try:
        response = await ai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — помощник модераторов в чате администрации «Фудзи Сквад». "
                        "Пользователь прислал ссылку. На основе адреса, домена и параметров ссылки "
                        "дай максимально краткую выжимку (1-3 предложения): что это за ресурс, "
                        "какой контент там находится и несёт ли он признаки спама/фишинга. "
                        "Используй форматирование с тегами <b> и <i>."
                    ),
                },
                {"role": "user", "content": f"Что находится по этой ссылке: {url}"},
            ],
            temperature=0.3,
            max_tokens=250,
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Ошибка DeepSeek: {e}")
        return "⚠️ Не удалось получить автоматическое описание ссылки от нейросети."


def get_link_action_kb(forwarded_msg_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👁 Скрыть описание", callback_data="hide_summary")
    builder.button(text="🗑 Удалить сообщение", callback_data=f"del_msg:{forwarded_msg_id}")
    builder.adjust(2)
    return builder.as_markup()


async def process_link_in_background(bot: Bot, url: str, forwarded_msg_id: int):
    summary = await summarize_link(url)
    admin_preview_text = (
        f"🔗 <b>Анализ ссылки для администрации:</b>\n"
        f"<code>{url}</code>\n\n"
        f"{summary}"
    )
    try:
        await bot.send_message(
            chat_id=SUPPORT_CHAT_ID,
            text=admin_preview_text,
            reply_to_message_id=forwarded_msg_id,
            reply_markup=get_link_action_kb(forwarded_msg_id),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        logging.error(f"Ошибка отправки фонового превью: {e}")


@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    if is_user_banned(message.from_user.id):
        await message.answer("⛔ Доступ к боту заблокирован администрацией.")
        return

    text = (
        "👋 Привет! Ты в чате поддержки и общения <b>Фудзи Сквада</b>.\n\n"
        "Задай свой вопрос, отправь трек или ссылку, и в конце укажи тэг администратора "
        "(например: <code>Привет #Рэй</code>)."
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(F.chat.type == "private")
async def handle_user_message(message: types.Message, bot: Bot):
    user_id = message.from_user.id

    if is_user_banned(user_id):
        await message.answer("⛔ Вы заблокированы и не можете отправлять сообщения в поддержку.")
        return

    if is_spamming(user_id):
        add_banned_user(user_id)
        user_message_history.pop(user_id, None)

        await message.answer("⛔ Вы были автоматически заблокированы за спам.")
        await bot.send_message(
            chat_id=SUPPORT_CHAT_ID,
            text=(
                f"🚨 <b>Сработал автоматический анти-спам!</b>\n"
                f"Пользователь: <b>{message.from_user.full_name}</b>\n"
                f"ID: <code>{user_id}</code>\n"
                f"Действие: выдан автоматический бан."
            ),
            parse_mode="HTML",
        )
        return

    forwarded = await bot.forward_message(
        chat_id=SUPPORT_CHAT_ID,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
    )

    save_ticket(
        forwarded_id=forwarded.message_id,
        user_id=user_id,
        user_name=message.from_user.full_name
    )

    text_content = message.text or message.caption or ""
    found_url = extract_url(text_content)
    if found_url:
        asyncio.create_task(process_link_in_background(bot, found_url, forwarded.message_id))


@dp.callback_query(F.data == "hide_summary")
async def cb_hide_summary(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer("Описание скрыто")


@dp.callback_query(F.data.startswith("del_msg:"))
async def cb_delete_message(callback: types.CallbackQuery, bot: Bot):
    forwarded_id = int(callback.data.split(":")[1])
    try:
        await bot.delete_message(chat_id=callback.message.chat.id, message_id=forwarded_id)
    except Exception:
        pass

    try:
        await callback.message.delete()
    except Exception:
        pass

    await callback.answer("Сообщение удалено")


@dp.message(F.chat.id == SUPPORT_CHAT_ID, F.reply_to_message)
async def handle_admin_reply(message: types.Message, bot: Bot):
    replied_id = message.reply_to_message.message_id
    user_info = get_ticket(replied_id)

    if not user_info:
        return

    text = (message.text or "").strip().lower()

    if text.startswith("@бан") or text.startswith("/ban"):
        member = await bot.get_chat_member(chat_id=message.chat.id, user_id=message.from_user.id)
        if member.status not in ("administrator", "creator"):
            await message.reply("❌ У вас нет прав администратора для выполнения блокировки.")
            return

        target_id = user_info["id"]
        add_banned_user(target_id)

        await message.reply(
            f"🚫 Пользователь <b>{user_info['name']}</b> (ID: <code>{target_id}</code>) заблокирован в боте.",
            parse_mode="HTML",
        )
        return

    if text.startswith("@разбан") or text.startswith("/unban"):
        member = await bot.get_chat_member(chat_id=message.chat.id, user_id=message.from_user.id)
        if member.status not in ("administrator", "creator"):
            await message.reply("❌ У вас нет прав администратора для разблокировки.")
            return

        target_id = user_info["id"]
        if is_user_banned(target_id):
            remove_banned_user(target_id)
            await message.reply(
                f"✅ Пользователь <b>{user_info['name']}</b> (ID: <code>{target_id}</code>) разблокирован.",
                parse_mode="HTML",
            )
        else:
            await message.reply(f"ℹ️ Пользователь <b>{user_info['name']}</b> не находится в бане.", parse_mode="HTML")
        return

    try:
        await bot.copy_message(
            chat_id=user_info["id"],
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
        await message.reply(
            f"✅ Ответ успешно отправлен пользователю <b>{user_info['name']}</b>",
            parse_mode="HTML",
        )
    except Exception:
        await message.reply("⚠️ Не удалось доставить: пользователь заблокировал или остановил бота.")


async def main():
    logging.basicConfig(level=logging.INFO)
    init_db()

    # Прямое подключение к Telegram API без прокси (для сервера)
    session = AiohttpSession(timeout=60.0)
    bot = Bot(token=BOT_TOKEN, session=session)

    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception as e:
        logging.warning(f"Ошибка сброса вебхука: {e}")

    print("Бот Фудзи Сквада запущен (Прямое подключение, SQLite + DeepSeek)...")
    try:
        await dp.start_polling(
            bot,
            polling_timeout=30,
            allowed_updates=["message", "callback_query"],
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())