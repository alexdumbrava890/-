import asyncio
import logging
import os
import re
import signal
import sqlite3
import sys
import time
import traceback
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, Awaitable, Callable, Optional

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramAPIError, TelegramNetworkError, TelegramRetryAfter
from aiogram.filters import CommandStart
from aiogram.utils.keyboard import InlineKeyboardBuilder
from openai import AsyncOpenAI

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8734583084:AAE0KNAjDy5htEH8t3HQlIGekx-T1wAo9mA")
SUPPORT_CHAT_ID = int(os.getenv("SUPPORT_CHAT_ID", "-1004241108121"))
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-ВАШ_КЛЮЧ_ОТ_DEEPSEEK")

DB_FILE = os.getenv("DB_FILE", "bot_data.db")

SPAM_LIMIT = 5
SPAM_WINDOW = 3.0

# Ретраи
NETWORK_RETRIES = 3
NETWORK_BASE_DELAY = 1.5

# Watchdog: если polling не «жив» N секунд — перезапуск
POLLING_WATCHDOG_INTERVAL = 60
# =============================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("fudzi-bot")

dp = Dispatcher()

ai_client = AsyncOpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com",
    timeout=30.0,
    max_retries=0,  # ретраи делаем сами, чтобы контролировать
)


# ================= БЕЗОПАСНЫЕ ОБЁРТКИ =================
async def safe_call(
    coro_factory: Callable[[], Awaitable[Any]],
    *,
    name: str,
    retries: int = NETWORK_RETRIES,
    base_delay: float = NETWORK_BASE_DELAY,
    default: Any = None,
) -> Any:
    """
    Вызывает coro_factory() с ретраями. Никогда не выбрасывает наружу
    (кроме CancelledError). Возвращает default при фатальной ошибке.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            return await coro_factory()
        except asyncio.CancelledError:
            raise
        except TelegramRetryAfter as e:
            delay = float(getattr(e, "retry_after", base_delay))
            log.warning(f"[{name}] flood control, ждём {delay}s (попытка {attempt}/{retries})")
            await asyncio.sleep(delay)
            last_exc = e
        except (TelegramNetworkError, asyncio.TimeoutError, OSError) as e:
            delay = base_delay * (2 ** (attempt - 1))
            log.warning(f"[{name}] сеть/таймаут: {e!r}; ретрай через {delay:.1f}s "
                        f"(попытка {attempt}/{retries})")
            await asyncio.sleep(delay)
            last_exc = e
        except TelegramAPIError as e:
            # 4xx — ретраить бессмысленно, логируем и выходим
            log.error(f"[{name}] Telegram API error: {e!r}")
            return default
        except Exception as e:
            log.error(f"[{name}] неожиданная ошибка: {e!r}\n{traceback.format_exc()}")
            return default
    log.error(f"[{name}] исчерпаны ретраи, последняя ошибка: {last_exc!r}")
    return default


def fire_and_forget(coro: Awaitable[Any], *, name: str) -> None:
    """Запускает корутину в фоне, гарантированно не роняя event loop."""
    async def _runner():
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"[{name}] фоновая задача упала: {e!r}\n{traceback.format_exc()}")

    task = asyncio.create_task(_runner(), name=name)
    # Держим ссылку, чтобы task не собрался GC
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


_background_tasks: set[asyncio.Task] = set()


def handler_guard(name: str):
    """Декоратор: любое исключение в хендлере логируется и не роняет polling."""
    def decorator(fn: Callable[..., Awaitable[Any]]):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.error(f"[handler:{name}] упал: {e!r}\n{traceback.format_exc()}")
                # Пытаемся ответить пользователю, если есть message
                for arg in args:
                    if isinstance(arg, types.Message):
                        try:
                            await arg.answer("⚠️ Внутренняя ошибка. Администрация уже видит проблему.")
                        except Exception:
                            pass
                        break
                return None
        return wrapper
    return decorator


# ================= БАЗА ДАННЫХ =================
def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=10000;")
    return conn


def init_db():
    with _connect() as conn:
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS spam_log (
                user_id INTEGER NOT NULL,
                ts REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spam_user_ts ON spam_log(user_id, ts)")
        conn.commit()


def _db_execute(query: str, params: tuple = ()) -> Optional[list]:
    """Безопасное выполнение SQL. Никогда не бросает наружу."""
    try:
        with _connect() as conn:
            cur = conn.execute(query, params)
            rows = cur.fetchall()
            conn.commit()
            return rows
    except Exception as e:
        log.error(f"[db] ошибка запроса {query!r}: {e!r}")
        return None


def save_ticket(forwarded_id: int, user_id: int, user_name: str):
    _db_execute(
        "INSERT OR REPLACE INTO tickets (forwarded_id, user_id, user_name) VALUES (?, ?, ?)",
        (forwarded_id, user_id, user_name),
    )


def get_ticket(forwarded_id: int) -> Optional[dict]:
    rows = _db_execute(
        "SELECT user_id, user_name FROM tickets WHERE forwarded_id = ?",
        (forwarded_id,),
    )
    if rows:
        return {"id": rows[0][0], "name": rows[0][1]}
    return None


def add_banned_user(user_id: int):
    _db_execute("INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)", (user_id,))


def remove_banned_user(user_id: int):
    _db_execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))


def is_user_banned(user_id: int) -> bool:
    rows = _db_execute("SELECT 1 FROM banned_users WHERE user_id = ?", (user_id,))
    return bool(rows)


def record_and_check_spam(user_id: int) -> bool:
    now = time.time()
    _db_execute(
        "DELETE FROM spam_log WHERE user_id = ? AND ts < ?",
        (user_id, now - SPAM_WINDOW),
    )
    rows = _db_execute("SELECT COUNT(*) FROM spam_log WHERE user_id = ?", (user_id,))
    count = rows[0][0] if rows else 0
    _db_execute("INSERT INTO spam_log (user_id, ts) VALUES (?, ?)", (user_id, now))
    return count + 1 > SPAM_LIMIT


def clear_spam_log(user_id: int):
    _db_execute("DELETE FROM spam_log WHERE user_id = ?", (user_id,))


# ================= УТИЛИТЫ =================
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)


def extract_url(text: Optional[str]) -> Optional[str]:
    if not text or not isinstance(text, str):
        return None
    try:
        m = _URL_RE.search(text)
        if not m:
            return None
        url = m.group(0).rstrip(".,;:!?)]}")
        if len(url) > 2048:
            return url[:2048]
        return url
    except Exception as e:
        log.warning(f"[extract_url] {e!r}")
        return None


async def summarize_link(url: str) -> str:
    async def _call():
        return await ai_client.chat.completions.create(
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

    try:
        resp = await safe_call(_call, name="deepseek.summarize", retries=2, base_delay=2.0)
        if not resp or not getattr(resp, "choices", None):
            return "⚠️ Не удалось получить автоматическое описание ссылки от нейросети."
        content = resp.choices[0].message.content
        if not content or not isinstance(content, str):
            return "⚠️ Нейросеть вернула пустой ответ."
        return content.strip()
    except Exception as e:
        log.error(f"[summarize_link] {e!r}")
        return "⚠️ Не удалось получить автоматическое описание ссылки от нейросети."


def get_link_action_kb(forwarded_msg_id: int) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👁 Скрыть описание", callback_data="hide_summary")
    builder.button(text="🗑 Удалить сообщение", callback_data=f"del_msg:{forwarded_msg_id}")
    builder.adjust(2)
    return builder.as_markup()


async def process_link_in_background(bot: Bot, url: str, forwarded_msg_id: int):
    try:
        summary = await summarize_link(url)
        admin_preview_text = (
            f"🔗 <b>Анализ ссылки для администрации:</b>\n"
            f"<code>{url}</code>\n\n"
            f"{summary}"
        )
        await safe_call(
            lambda: bot.send_message(
                chat_id=SUPPORT_CHAT_ID,
                text=admin_preview_text,
                reply_to_message_id=forwarded_msg_id,
                reply_markup=get_link_action_kb(forwarded_msg_id),
                parse_mode="HTML",
                disable_web_page_preview=True,
            ),
            name="send_link_preview",
        )
    except Exception as e:
        log.error(f"[process_link_in_background] {e!r}")


# ================= ХЕНДЛЕРЫ =================
@dp.message(CommandStart())
@handler_guard("cmd_start")
async def cmd_start(message: types.Message):
    if not message.from_user:
        return
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
@handler_guard("handle_user_message")
async def handle_user_message(message: types.Message, bot: Bot):
    if not message.from_user:
        return
    user_id = message.from_user.id
    user_name = message.from_user.full_name or "unknown"

    if is_user_banned(user_id):
        await message.answer("⛔ Вы заблокированы и не можете отправлять сообщения в поддержку.")
        return

    if record_and_check_spam(user_id):
        add_banned_user(user_id)
        clear_spam_log(user_id)
        await message.answer("⛔ Вы были автоматически заблокированы за спам.")
        await safe_call(
            lambda: bot.send_message(
                chat_id=SUPPORT_CHAT_ID,
                text=(
                    f"🚨 <b>Сработал автоматический анти-спам!</b>\n"
                    f"Пользователь: <b>{user_name}</b>\n"
                    f"ID: <code>{user_id}</code>\n"
                    f"Действие: выдан автоматический бан."
                ),
                parse_mode="HTML",
            ),
            name="spam_alert",
        )
        return

    forwarded = await safe_call(
        lambda: bot.forward_message(
            chat_id=SUPPORT_CHAT_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        ),
        name="forward_message",
    )
    if not forwarded or not getattr(forwarded, "message_id", None):
        log.error("[handle_user_message] не удалось переслать сообщение")
        return

    save_ticket(
        forwarded_id=forwarded.message_id,
        user_id=user_id,
        user_name=user_name,
    )

    text_content = message.text or message.caption or ""
    found_url = extract_url(text_content)
    if found_url:
        fire_and_forget(
            process_link_in_background(bot, found_url, forwarded.message_id),
            name="link_preview",
        )


@dp.callback_query(F.data == "hide_summary")
@handler_guard("cb_hide_summary")
async def cb_hide_summary(callback: types.CallbackQuery):
    if callback.message:
        await safe_call(callback.message.delete, name="hide_summary.delete")
    await safe_call(callback.answer, name="hide_summary.answer", retries=1)


@dp.callback_query(F.data.startswith("del_msg:"))
@handler_guard("cb_delete_message")
async def cb_delete_message(callback: types.CallbackQuery, bot: Bot):
    try:
        forwarded_id = int((callback.data or "").split(":", 1)[1])
    except (ValueError, IndexError):
        log.warning(f"[cb_delete_message] bad callback data: {callback.data!r}")
        await safe_call(callback.answer, name="del_msg.bad", retries=1)
        return

    if callback.message:
        await safe_call(
            lambda: bot.delete_message(chat_id=callback.message.chat.id, message_id=forwarded_id),
            name="del_msg.delete_target",
        )
        await safe_call(callback.message.delete, name="del_msg.delete_self")

    await safe_call(callback.answer, name="del_msg.answer", retries=1)


@dp.message(F.chat.id == SUPPORT_CHAT_ID, F.reply_to_message)
@handler_guard("handle_admin_reply")
async def handle_admin_reply(message: types.Message, bot: Bot):
    if not message.from_user or not message.reply_to_message:
        return
    replied_id = message.reply_to_message.message_id
    user_info = get_ticket(replied_id)
    if not user_info:
        return

    text = (message.text or "").strip().lower()

    if text.startswith("@бан") or text.startswith("/ban"):
        member = await safe_call(
            lambda: bot.get_chat_member(chat_id=message.chat.id, user_id=message.from_user.id),
            name="get_chat_member.ban",
        )
        if not member or getattr(member, "status", None) not in ("administrator", "creator"):
            await safe_call(
                lambda: message.reply("❌ У вас нет прав администратора для выполнения блокировки."),
                name="ban.no_rights",
            )
            return
        target_id = user_info["id"]
        add_banned_user(target_id)
        await safe_call(
            lambda: message.reply(
                f"🚫 Пользователь <b>{user_info['name']}</b> "
                f"(ID: <code>{target_id}</code>) заблокирован в боте.",
                parse_mode="HTML",
            ),
            name="ban.reply",
        )
        return

    if text.startswith("@разбан") or text.startswith("/unban"):
        member = await safe_call(
            lambda: bot.get_chat_member(chat_id=message.chat.id, user_id=message.from_user.id),
            name="get_chat_member.unban",
        )
        if not member or getattr(member, "status", None) not in ("administrator", "creator"):
            await safe_call(
                lambda: message.reply("❌ У вас нет прав администратора для разблокировки."),
                name="unban.no_rights",
            )
            return
        target_id = user_info["id"]
        if is_user_banned(target_id):
            remove_banned_user(target_id)
            await safe_call(
                lambda: message.reply(
                    f"✅ Пользователь <b>{user_info['name']}</b> "
                    f"(ID: <code>{target_id}</code>) разблокирован.",
                    parse_mode="HTML",
                ),
                name="unban.reply.ok",
            )
        else:
            await safe_call(
                lambda: message.reply(
                    f"ℹ️ Пользователь <b>{user_info['name']}</b> не находится в бане.",
                    parse_mode="HTML",
                ),
                name="unban.reply.info",
            )
        return

    copied = await safe_call(
        lambda: bot.copy_message(
            chat_id=user_info["id"],
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        ),
        name="copy_message",
    )
    if copied:
        await safe_call(
            lambda: message.reply(
                f"✅ Ответ успешно отправлен пользователю <b>{user_info['name']}</b>",
                parse_mode="HTML",
            ),
            name="copy.reply.ok",
        )
    else:
        await safe_call(
            lambda: message.reply(
                "⚠️ Не удалось доставить: пользователь заблокировал или остановил бота."
            ),
            name="copy.reply.fail",
        )


# ================= СУПЕРВИЗОР =================
async def ensure_no_webhook(bot: Bot):
    try:
        info = await safe_call(bot.get_webhook_info, name="get_webhook_info", retries=3)
        if info:
            log.info(
                "Webhook info: url=%r, pending_updates=%s",
                getattr(info, "url", ""),
                getattr(info, "pending_update_count", "?"),
            )
        await safe_call(
            lambda: bot.delete_webhook(drop_pending_updates=True),
            name="delete_webhook",
            retries=3,
        )
    except Exception as e:
        log.error(f"[ensure_no_webhook] {e!r}")


async def polling_watchdog(bot: Bot, stop_event: asyncio.Event):
    """
    Простой watchdog: если процесс жив, но polling молчит слишком долго —
    шлём SIGTERM самому себе, чтобы супервизор (systemd/docker) перезапустил.
    Здесь — мягкий вариант: просто логируем. Жёсткий вариант — os.kill(os.getpid(), SIGTERM).
    """
    while not stop_event.is_set():
        await asyncio.sleep(POLLING_WATCHDOG_INTERVAL)
        # aiogram сам ретраит polling, поэтому вмешиваться не будем,
        # но оставим место для метрик (last_update_id и т.п.)
        log.debug("watchdog: polling alive")


async def run_bot():
    init_db()

    session = AiohttpSession(timeout=60.0)
    bot = Bot(token=BOT_TOKEN, session=session)

    await ensure_no_webhook(bot)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        log.info("Получен сигнал остановки")
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_signal)
        loop.add_signal_handler(signal.SIGINT, _handle_signal)
    except NotImplementedError:
        pass  # Windows

    print("Бот Фудзи Сквада запущен (Прямое подключение, SQLite + DeepSeek)...")
    polling_task = asyncio.create_task(
        dp.start_polling(
            bot,
            polling_timeout=30,
            allowed_updates=["message", "callback_query"],
        ),
        name="polling",
    )
    watchdog_task = asyncio.create_task(
        polling_watchdog(bot, stop_event),
        name="watchdog",
    )
    stop_task = asyncio.create_task(stop_event.wait(), name="stop")

    try:
        done, pending = await asyncio.wait(
            {polling_task, watchdog_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        # Если polling упал — покажем причину
        if polling_task in done and polling_task.exception():
            log.error(f"polling упал: {polling_task.exception()!r}")
    finally:
        for t in _background_tasks:
            t.cancel()
        await safe_call(bot.session.close, name="session.close", retries=1)
        log.info("Бот остановлен.")


async def main_with_restart():
    """
    Бесконечный супервизор: если run_bot() падает с фатальной ошибкой —
    ждём и перезапускаем внутри того же процесса.
    Это защищает от «глитчей», которые не ловятся хендлерами.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            await run_bot()
            log.info("run_bot завершился штатно, выходим.")
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            delay = min(60, 2 ** min(attempt, 6))
            log.critical(
                f"run_bot упал (попытка {attempt}): {e!r}\n"
                f"{traceback.format_exc()}\nПерезапуск через {delay}s..."
            )
            await asyncio.sleep(delay)


if __name__ == "__main__":
    try:
        asyncio.run(main_with_restart())
    except (KeyboardInterrupt, SystemExit):
        log.info("Завершение по сигналу.")
