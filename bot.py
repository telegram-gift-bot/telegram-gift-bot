

import re
import os
import csv
import logging
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from dateutil import parser
from aiogram import F
from math import ceil
import asyncssh
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiofiles
from utils import shorten_date
from dotenv import load_dotenv, find_dotenv


# ================== SCHEDULER ==================
scheduler = AsyncIOScheduler(timezone="UTC")

# ================== CONSTANTS & PATHS ==================
DB_PATH = Path("/root/richi_gift_bot/requests.db")
LOCAL_CSV = Path("/root/richi_gift_bot/remote_orders.csv")
LIMIT = 20

REMOTE = {
    "host": "38.180.6.162",
    "user": "root",
    "key": "/root/.ssh/id_rsa",
    "remote_csv": "/root/mk_tg_bot/orders.csv",
}

# ================== LOGGING ==================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot_logs.log"),
    ],
)
logger = logging.getLogger(__name__)

# ================== ENV ==================
load_dotenv(find_dotenv(), override=True)
API_TOKEN: str | None = os.getenv("TELEGRAM_BOT_API_TOKEN")

if not API_TOKEN:
    raise RuntimeError("❌ TELEGRAM_BOT_API_TOKEN is not set in .env or env vars")
else:
    logger.info("✅ TOKEN загружен успешно")

# ================== AIOGRAM CORE ==================
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# ================== FSM ==================
class LangFSM(StatesGroup):
    choosing = State()

# ================== DATABASE INIT ==================
def init_db() -> None:
    print(f"📂 Текущий путь к БД: {DB_PATH}")  # ← вот сюда

    with sqlite3.connect(DB_PATH) as con:
        con.executescript(
            '''
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shop_link TEXT NOT NULL,
                amount TEXT NOT NULL,
                note TEXT,
                reserved_by INTEGER,
                reserved_until TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                lang TEXT DEFAULT 'ru'
            );
            '''
        )
        con.commit()

# ====== АНТИСПАМ: проверка магазина ======
def is_spammy_shop(text: str) -> bool:
    text = text.strip().lower()
    if not text or len(text) < 3:
        return True
    if re.fullmatch(r"[^\w.]+", text):
        return True
    if text in {"asd", "test", "qwe", "aaa"}:
        return True
    if text.startswith("proverka"):
        return True
    return False

# ====== АНТИСПАМ: проверка комментария ======
def is_spammy_note(note: str) -> bool:
    note = (note or "").strip()

    if note == "":
        return True

    # ⬇️ Разрешаем символы "-", "—" как валидные комментарии
    if note in {"-", "—"}:
        return False

    if len(note) < 3 and note.lower() not in {"ok"}:
        return True

    if re.search(r"(\d{2,3})\1{1,}", note):
        return True

    if note.lower() in {"test", "asd", "qwe", "aaa"}:
        return True

    return False

# ====== 🔥 АНТИСПАМ: проверка суммы ======
def is_spammy_amount(amount: str) -> bool:
    amount = amount.strip().lower().replace("$", "").replace(",", "").replace(".00", "")
    
    # Не цифра или слишком большой диапазон
    if not amount or not re.fullmatch(r"\d{1,5}", amount):
        return True

    amount_int = int(amount)
    if amount_int < 10 or amount_int > 5000:
        return True

    return False
# ================== SCP DOWNLOAD ==================
async def scp_download_async() -> bool:
    try:
        async with asyncssh.connect(
            REMOTE["host"],
            username=REMOTE["user"],
            client_keys=[REMOTE["key"]],
            known_hosts=None
        ) as conn:
            await asyncssh.scp((conn, REMOTE["remote_csv"]), LOCAL_CSV)
        return True
    except Exception as e:
        logger.error("SCP download failed: %s", e)
        return False


# ================== УТИЛИТЫ ==================
def shorten_date(date_str: str, lang: str = "ru") -> str:
    try:
        dt = datetime.fromisoformat(date_str)
        if lang == "ru":
            return dt.strftime("%d.%m")
        else:
            return dt.strftime("%b %d")  # Jul 04
    except Exception:
        return "??.??"


def normalize_shop_name(raw_shop: str) -> str:
    raw_shop = raw_shop.strip()
    if raw_shop.startswith("http://") or raw_shop.startswith("https://"):
        netloc = urlparse(raw_shop).netloc
    elif "." in raw_shop and " " not in raw_shop:
        netloc = urlparse("https://" + raw_shop).netloc
    else:
        return raw_shop.capitalize()

    domain = netloc.replace("www.", "")
    parts = domain.split(".")
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return domain.capitalize()


def format_comment(raw_note: str) -> str:
    note = raw_note.strip()
    if note.lower().startswith("almost"):
        digits = ''.join(filter(str.isdigit, note))
        return f"${digits}" if digits else note

    if 'x' in note.lower():
        parts = note.lower().split('x')
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            return f"{parts[0].strip()} * ${parts[1].strip()}"

    if note.startswith("$") or note.startswith("€"):
        return note.strip()

    if note.isdigit():
        return f"${note.strip()}"

    return note


def normalize_amount(raw_amount: str) -> str:
    raw_amount = raw_amount.strip().replace(" ", "")
    raw_amount_lower = raw_amount.lower()

    if re.fullmatch(r"(\$|€)\d{2,5}", raw_amount) or re.fullmatch(r"\d{2,5}€", raw_amount):
        return raw_amount.replace("€", "€").replace("$", "$")

    match = re.fullmatch(r"(\d{1,2})[x\*](\d{1,4})", raw_amount_lower)
    if match:
        count, value = match.groups()
        return f"{count} * ${value}"

    match = re.search(r"\d{2,5}", raw_amount)
    if match:
        value = match.group(0)
        if value == "00000" or int(value) < 10:
            return ""
        return f"${value}"

    return ""

# ================== IMPORT CSV ==================

async def import_csv():
    logger.info("📥 Импорт CSV начинается")

    if not await scp_download_async():
        logger.error("❌ Не удалось скачать CSV с удалённого сервера")
        return

    if not Path(LOCAL_CSV).exists():
        logger.error("❌ Локальный CSV файл не найден")
        return

    async with aiofiles.open(LOCAL_CSV, "r", encoding="utf-8") as f:
        content = await f.read()

    fieldnames = [
        "Магазин",
        "Номиналы и сумма",
        "Комментарий",
        "Доп. инфо",
        "Телеграм",
        "Дата и время",
        "Язык"
    ]

    rows = list(csv.DictReader(content.splitlines(), fieldnames=fieldnames))
    logger.debug(f"🔍 Всего строк в файле: {len(rows)}")

    new_cnt = 0

    with sqlite3.connect(DB_PATH) as con:
        for row in rows:
            logger.debug(f"DEBUG ROW: {row}")
            try:
                raw_shop = row["Магазин"].strip()
                raw_amount = row["Номиналы и сумма"].strip()
                raw_note = (row.get("Комментарий") or "").strip()

                shop_link = normalize_shop_name(raw_shop)
                amount = normalize_amount(raw_amount)
                note = format_comment(raw_note)

                # 📅 Получение даты ДО фильтрации
                created_at_raw = (row.get("Дата и время") or "").strip()

                if not created_at_raw and None in row and len(row[None]) >= 6:
                    created_at_raw = row[None][5]

                try:
                    created_at_dt = parser.parse(created_at_raw)
                    created_at = created_at_dt.isoformat()
                except Exception:
                    logger.warning("⚠️ Не удалось разобрать дату: %s", created_at_raw)
                    created_at = datetime.utcnow().isoformat()

                # 🧼 Фильтрация (с датой в логе)
                if is_spammy_shop(shop_link) or is_spammy_amount(amount) or is_spammy_note(note):
                    logger.warning("⛔ Спам-заявка пропущена: %s | %s | %s | %s",
                                   shop_link, amount, note, shorten_date(created_at))
                    continue

                # 🔒 Пропуск забронированных
                exists_reserved = con.execute(
                    "SELECT 1 FROM requests WHERE shop_link=? AND amount=? AND note=? AND reserved_by IS NOT NULL",
                    (shop_link, amount, note)
                ).fetchone()

                if exists_reserved:
                    logger.info("🛑 Пропущена забронированная заявка: %s | %s", shop_link, amount)
                    continue

                # 🧾 Проверка на точный дубликат
                exists = con.execute(
                    "SELECT 1 FROM requests WHERE shop_link=? AND amount=? AND note=? AND created_at=?",
                    (shop_link, amount, note, created_at)
                ).fetchone()

                if not exists:
                    con.execute(
                        "INSERT INTO requests (shop_link, amount, note, created_at) VALUES (?, ?, ?, ?)",
                        (shop_link, amount, note, created_at)
                    )
                    logger.info("➕ Новая заявка: %s | %s | %s", shop_link, amount, shorten_date(created_at))
                    new_cnt += 1

            except Exception as e:
                logger.error("❌ Ошибка при обработке строки: %s — %s", row, e, exc_info=True)

        con.commit()

    if new_cnt:
        logger.info("✅ Импортировано новых заявок: %s", new_cnt)
    else:
        logger.info("ℹ️ Новых заявок не найдено")

# ================== GLOBAL STORAGE ==================
user_messages = {}

# ================== SCHEDULER ==================
def schedule_release(rid, until):
    delay = (until - datetime.now(timezone.utc)).total_seconds()

    async def release_job():
        with sqlite3.connect(DB_PATH) as con:
            con.execute(
                "UPDATE requests SET reserved_by=NULL, reserved_until=NULL WHERE id=? AND reserved_until <= ?",
                (rid, datetime.now(timezone.utc).isoformat())
            )
            con.commit()
        print(f"🔓 Auto-released RID={rid}")

    job_id = f"release_{rid}"
    scheduler.add_job(
        lambda: asyncio.create_task(release_job()),
        trigger="date",
        run_date=until,
        id=job_id,
        replace_existing=True
    )
    print(f"⏰ Scheduled auto-release in {int(delay)}s")

def schedule_reminder(rid: int, uid: int):
    remind_at = datetime.now(timezone.utc) + timedelta(hours=24)

    async def remind_job():
        lang = get_lang(uid)
        text = lang_text(
            lang,
            f"🔔 Напоминание: осталось 24 ч, чтобы завершить заявку #{rid}.\nПроверьте её в разделе 📋 Мои заявки.",
            f"🔔 Reminder: 24 h left to finish request #{rid}.\nCheck it in 📋 My Requests."
        )
        try:
            await bot.send_message(uid, text)
            print(f"🔔 Reminder sent to UID={uid} for RID={rid}")
        except Exception as e:
            print(f"⚠️ Failed to send reminder: {e}")

    job_id = f"remind_{rid}"
    scheduler.add_job(
        lambda: asyncio.create_task(remind_job()),
        trigger="date",
        run_date=remind_at,
        id=job_id,
        replace_existing=True
    )
    print(f"⏰ Scheduled reminder at {remind_at.isoformat()}")

# ================== STATE HANDLER ==================
@router.callback_query(F.data.in_({"lang_ru", "lang_en"}))
async def set_language(cb: types.CallbackQuery, state: FSMContext):
    lang = "ru" if cb.data == "lang_ru" else "en"

    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT OR REPLACE INTO users (user_id, lang) VALUES (?, ?)",
            (cb.from_user.id, lang)
        )
        con.commit()

    await state.clear()
    await show_main_menu(cb.message.chat.id, cb.from_user.id, cb.message)
    await cb.answer()


# ================== BROWSE CALLBACK ==================
@router.callback_query(F.data.startswith("browse:"))
async def cb_browse(callback: types.CallbackQuery):
    uid = callback.from_user.id
    lang = get_lang(uid)

    try:
        offset = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer(
            lang_text(lang, "Неверный формат", "Invalid format"),
            show_alert=True
        )
        return

    await delete_old_messages(bot, callback.message.chat.id, uid)
    await show_requests(callback.message.chat.id, uid, offset)
    await callback.answer()

# ──────────── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ────────────

def base_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=lang_text(lang, "📋 Мои заявки", "📋 My Requests"),
                    callback_data="my_requests"
                ),
                InlineKeyboardButton(
                    text=lang_text(lang, "🔄 Обновить", "🔄 Refresh"),
                    callback_data="refresh"
                ),
            ]
        ]
    )

def main_menu_kb(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=lang_text(lang, "📋 Мои заявки", "📋 My Requests"),
                    callback_data="my_requests"
                )
            ],
            [
                InlineKeyboardButton(
                    text=lang_text(lang, "📦 Все заявки", "📦 All Requests"),
                    callback_data="browse:0"
                )
            ]
        ]
    )

# ──────────── ЛОКАЛИЗАЦИЯ ────────────
def lang_text(lang: str, ru: str, en: str) -> str:
    """Возвращает текст на нужном языке."""
    return ru if lang == "ru" else en

def get_lang(user_id: int) -> str:
    """Получает язык пользователя из БД, по умолчанию 'ru'."""
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("SELECT lang FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row[0] if row else "ru"

# ──────────── УДАЛЕНИЕ СТАРЫХ СООБЩЕНИЙ ────────────
async def delete_old_messages(bot: Bot, chat_id: int, user_id: int):
    messages = user_messages.get(user_id, [])
    for msg_id in messages:
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass  # например, сообщение уже удалено
    user_messages[user_id] = []

# ──────────────── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ────────────────
def generate_my_request_text(shop: str, amount: str, note: str, created_at: str, reserved_until: str) -> str:
    """Формирует текст заявки для 'Моих заявок'."""
    domain = shop.replace("www.", "")
    if "." not in domain:
        domain += ".com"
    display_amount = amount if "$" in amount else f"${amount}"
    text = f"🌐 Сайт: {domain}\n💵 Сумма заказа: {display_amount}"

    if note and note.lower() not in {"-", "без комментариев", "no comments"}:
        text += f"\n🔹 Номиналы: {note}"

    if created_at:
        try:
            date_str = datetime.fromisoformat(created_at).strftime("%d.%m.%Y %H:%M")
            text += f"\n📅 Добавлено: {date_str}"
        except Exception:
            pass

    try:
        r_until = datetime.fromisoformat(reserved_until)
        left = r_until - datetime.now(timezone.utc)
        if left.total_seconds() > 0:
            hours = int(left.total_seconds() // 3600)
            minutes = int((left.total_seconds() % 3600) // 60)
            text += f"\n⏳ Осталось: {hours}ч {minutes}м"
        else:
            text += "\n⏳ Время брони истекло"
    except Exception:
        pass

    return text

# ───────── ЗАЯВКИ: клавиатура 2×N ─────────
def format_shop_title(shop_link: str) -> str:
    parts = shop_link.replace("www.", "").split(".")
    return parts[0].capitalize() if parts else shop_link.capitalize()

# ---------- МОИ ЗАЯВКИ: generate_request_buttons ----------
def generate_request_buttons(
    requests: list[dict],
    lang: str = "ru",
    offset: int = 0,
    total: int = 0,
    my: bool = False
) -> InlineKeyboardMarkup:
    buttons, row = [], []

    for req in requests:
        rid = req["id"]
        shop_title = format_shop_title(req["shop_link"])
        amount = req["amount"] if "$" in req["amount"] else f"${req['amount']}"
        created_at = req.get("created_at")
        date_str = shorten_date(created_at, lang) if created_at else "??.??"
        print(f"➡️ created_at: {created_at} → {date_str}")
        text = f"🧾 {shop_title} | {amount} | {date_str}"
        if my:
            buttons.append([
                InlineKeyboardButton(text=text, callback_data=f"my:{rid}:{offset}")
            ])
        else:
            row.append(
                InlineKeyboardButton(
                    text=text,
                    callback_data=f"view:{rid}:{offset}"
                )
            )
            if len(row) == 2:
                buttons.append(row)
                row = []

    if row:
        buttons.append(row)

    nav_row = []
    if offset >= LIMIT:
        nav_row.append(
            InlineKeyboardButton(
                text=lang_text(lang, "← Назад", "← Back"),
                callback_data=("mybrowse:" if my else "browse:") + str(offset - LIMIT)
            )
        )
    if offset + LIMIT < total:
        nav_row.append(
            InlineKeyboardButton(
                text=lang_text(lang, "Вперёд →", "Next →"),
                callback_data=("mybrowse:" if my else "browse:") + str(offset + LIMIT)
            )
        )
    if nav_row:
        buttons.append(nav_row)

    buttons.append([
        InlineKeyboardButton(
            text=lang_text(lang, "⬅️ Назад в меню", "⬅️ Back to menu"),
            callback_data="to_main_menu"
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ---------- МОЯ ЗАЯВКА ПОДРОБНО ----------
@router.callback_query(F.data.startswith("my:"))
async def cb_my_request_detail(callback: types.CallbackQuery):
    uid = callback.from_user.id
    lang = get_lang(uid)

    try:
        rid, offset = map(int, callback.data.split(":")[1:])
    except ValueError:
        await callback.answer(
            lang_text(lang, "Неверный формат", "Invalid format"),
            show_alert=True
        )
        return

    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            """
            SELECT shop_link, amount, note, reserved_until, created_at
            FROM requests
            WHERE id=? AND reserved_by=?
            """,
            (rid, uid),
        ).fetchone()

    if not row:
        await callback.answer(
            lang_text(lang, "Заявка не найдена", "Request not found"),
            show_alert=True
        )
        return

    shop, amount, note, r_until, created_at = row
    text = generate_my_request_text(shop, amount, note, created_at, r_until)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=lang_text(lang, "⏳ Продлить", "⏳ Extend"),
                callback_data=f"renew:{rid}:my"
            ),
            InlineKeyboardButton(
                text=lang_text(lang, "❌ Отменить бронь", "❌ Cancel"),
                callback_data=f"cancel:{rid}:my"
            )
        ],
        [
            InlineKeyboardButton(
                text=lang_text(lang, "✅ Завершить и отправить карту", "✅ Done & Submit"),
                callback_data=f"complete:{rid}"
            )
        ]
    ])

    await delete_old_messages(bot, callback.message.chat.id, uid)

    msg = await callback.message.answer(text, reply_markup=kb)
    user_messages.setdefault(uid, []).append(msg.message_id)


# ---------- ФУНКЦИЯ: Получение заявок по дате (новые сверху) ----------
def get_requests_page(offset: int = 0, limit: int = 20) -> list[dict]:
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            """
            SELECT id, shop_link, amount, created_at
            FROM requests
            WHERE reserved_by IS NULL
            ORDER BY datetime(created_at) DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset)
        ).fetchall()

    return [
        {"id": rid, "shop_link": shop, "amount": amt, "created_at": created_at}
        for rid, shop, amt, created_at in rows
    ]

# ================== ОБНОВЛЁННЫЙ show_requests ==================
async def show_requests(chat_id: int, user_id: int, offset: int = 0):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=14)
    lang = get_lang(user_id)

    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            """
            SELECT id, shop_link, amount, note,
                   reserved_by, reserved_until, created_at
            FROM requests
            WHERE datetime(created_at) >= ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (cutoff.isoformat(), LIMIT, offset),
        ).fetchall()

        total = con.execute(
            "SELECT COUNT(*) FROM requests WHERE datetime(created_at) >= ?",
            (cutoff.isoformat(),)
        ).fetchone()[0]

    # 👁 Отбор только незабронированных или истекших
    visible = []
    for rid, shop, amt, note, r_by, r_until, created_at in rows:
        if r_by and r_until:
            r_until_dt = datetime.fromisoformat(r_until)
            if r_until_dt.tzinfo is None:
                r_until_dt = r_until_dt.replace(tzinfo=timezone.utc)
            if r_until_dt > now:
                continue
        visible.append((rid, shop, amt, shorten_date(created_at)))

    # 📦 Генерация кнопок
    buttons, row_buf = [], []
    for rid, shop, amt, date_str in visible:
        title = format_shop_title(shop)
        amount = amt if "$" in amt else f"${amt}"
        row_buf.append(InlineKeyboardButton(
            text=f"🧾 {title} | {amount} | {date_str}",
            callback_data=f"view:{rid}:{offset}"
        ))
        if len(row_buf) == 2:
            buttons.append(row_buf)
            row_buf = []
    if row_buf:
        buttons.append(row_buf)

    # 🔁 Навигация
    nav_row = []
    if offset >= LIMIT:
        nav_row.append(InlineKeyboardButton(
            text=lang_text(lang, "← Назад", "← Back"),
            callback_data=f"browse:{offset - LIMIT}"
        ))
    if (offset + LIMIT) < total:
        nav_row.append(InlineKeyboardButton(
            text=lang_text(lang, "Вперёд →", "Next →"),
            callback_data=f"browse:{offset + LIMIT}"
        ))
    if nav_row:
        buttons.append(nav_row)

    # ↩️ Назад в меню
    buttons.append([
        InlineKeyboardButton(
            text=lang_text(lang, "⬅️ Назад в меню", "⬅️ Back to menu"),
            callback_data="to_main_menu"
        )
    ])

    # 📄 Отправка
    current_page = offset // LIMIT + 1
    total_pages = max(1, ceil(total / LIMIT))
    header = lang_text(lang, f"🗂 Страница {current_page} из {total_pages}", f"🗂 Page {current_page} of {total_pages}")

    await delete_old_messages(bot, chat_id, user_id)
    msg = await bot.send_message(
        chat_id,
        f"{header}\n\n" + lang_text(lang, "🛍 Доступные заявки:", "🛍 Available requests:"),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    user_messages.setdefault(user_id, []).append(msg.message_id)

# ---------- МОИ ЗАЯВКИ: show_my_requests ----------
async def show_my_requests(chat_id: int, user_id: int, offset: int = 0):
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=14)
    lang = get_lang(user_id)

    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            """
            SELECT id, shop_link, amount, reserved_until, created_at
            FROM requests
            WHERE reserved_by = ? AND datetime(created_at) >= ?
            ORDER BY datetime(created_at) DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, cutoff.isoformat(), LIMIT, offset),
        ).fetchall()

        total = con.execute(
            """
            SELECT COUNT(*) FROM requests
            WHERE reserved_by = ? AND datetime(created_at) >= ?
            """,
            (user_id, cutoff.isoformat()),
        ).fetchone()[0]

    # 🔁 Сбор данных
    requests = [
        {
            "id": rid,
            "shop_link": shop,
            "amount": amt,
            "reserved_until": r_until,
            "created_at": created_at
        }
        for rid, shop, amt, r_until, created_at in rows
    ]

    # 📦 Генерация кнопок
    buttons = generate_request_buttons(
        requests=requests,
        lang=lang,
        offset=offset,
        total=total,
        my=True
    )

    current_page = offset // LIMIT + 1
    total_pages = max(1, ceil(total / LIMIT))

    header = lang_text(
        lang,
        f"📋 Мои заявки ({len(requests)} из {total})\n🗂 Страница {current_page} из {total_pages}",
        f"📋 My Requests ({len(requests)} of {total})\n🗂 Page {current_page} of {total_pages}"
    )

    await delete_old_messages(bot, chat_id, user_id)

    msg = await bot.send_message(
        chat_id,
        f"{header}\n\n" + lang_text(lang, "Выберите заявку для просмотра:", "Select a request to view:"),
        reply_markup=buttons
    )

    user_messages.setdefault(user_id, []).append(msg.message_id)

# ---------- МОИ ЗАЯВКИ: BACK ----------
@router.callback_query(F.data.startswith("browse:"))
async def cb_browse(callback: types.CallbackQuery):
    try:
        offset = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer(
            lang_text(get_lang(callback.from_user.id), "Неверный сдвиг", "Invalid offset"),
            show_alert=True
        )
        return

    uid = callback.from_user.id
    await delete_old_messages(bot, callback.message.chat.id, uid)
    await show_requests(callback.message.chat.id, uid, offset)
    await callback.answer()

# ─────────────────── ОБРАБОТЧИК МОИХ ЗАЯВОК ───────────────────
@router.callback_query(F.data == "my_requests")
async def cb_my_requests(callback: types.CallbackQuery):
    uid = callback.from_user.id
    await show_my_requests(callback.message.chat.id, uid)
    await callback.answer()


@router.callback_query(F.data == "all_requests")
async def cb_all_requests(callback: types.CallbackQuery):
    await show_catalog(callback.message.chat.id, callback.from_user.id)
    await callback.answer()

# ─────────────────── ОБРАБОТЧИК отправки карты ───────────────────
@router.callback_query(F.data == "submit_card")
async def cb_submit_card(callback: types.CallbackQuery):
    lang = get_lang(callback.from_user.id)
    chat_id = callback.message.chat.id
    uid = callback.from_user.id

    await delete_old_messages(bot, chat_id, uid)

    msg = await callback.message.answer(
        lang_text(
            lang,
            "💳 Пожалуйста, отправьте данные карты сюда сообщением.",
            "💳 Please send your card details here as a message."
        ),
        reply_markup=back_to_menu_kb(lang)
    )

    user_messages.setdefault(uid, []).append(msg.message_id)

    try:
        await callback.message.delete()
    except:
        pass


@router.callback_query(F.data == "to_main_menu")
async def cb_to_main(callback: types.CallbackQuery):
    uid = callback.from_user.id
    chat_id = callback.message.chat.id

    await delete_old_messages(bot, chat_id, uid)

    lang = get_lang(uid)

    try:
        await callback.message.delete()
    except:
        pass

    msg = await bot.send_message(
        chat_id,
        lang_text(lang, "✨ Главное меню:", "✨ Main menu:"),
        reply_markup=main_menu_kb(lang)
    )
    user_messages.setdefault(uid, []).append(msg.message_id)

    await callback.answer()



# ─────────────────── ОБРАБОТЧИК просмотра страниц ─────────────────
@router.callback_query(F.data.startswith("browse:"))
async def cb_browse_requests(callback: types.CallbackQuery):
    try:
        offset = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        offset = 0
    await show_requests(callback.message.chat.id, callback.from_user.id, offset)
    await callback.answer()


@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 2:
        await callback.answer("Неверный формат", show_alert=True)
        return

    rid = int(parts[1])
    back = parts[2] if len(parts) > 2 else None
    uid = callback.from_user.id
    lang = get_lang(uid)

    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT reserved_by FROM requests WHERE id=?", (rid,)
        ).fetchone()

        if not row:
            await callback.answer(lang_text(lang, "Заявка не найдена", "Request not found"), show_alert=True)
            return

        if row[0] != uid:
            await callback.answer(lang_text(lang, "Это не ваша заявка", "This is not your request"), show_alert=True)
            return

        con.execute(
            "UPDATE requests SET reserved_by=NULL, reserved_until=NULL WHERE id=?",
            (rid,)
        )
        con.commit()

    await callback.answer(lang_text(lang, "Бронь снята", "Reservation canceled"), show_alert=True)

    if back == "my":
        await show_my_requests(callback.message.chat.id, uid)

@router.callback_query(F.data.startswith("view:"))
async def cb_view(callback: types.CallbackQuery):
    try:
        _, rid_str, offset = callback.data.split(":")
        rid = int(rid_str)
    except (ValueError, IndexError):
        await callback.answer("Invalid ID", show_alert=True)
        return

    uid  = callback.from_user.id
    lang = get_lang(uid)

    await delete_old_messages(bot, callback.message.chat.id, uid)

    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            """SELECT shop_link, amount, note,
                     reserved_by, reserved_until, created_at
               FROM requests WHERE id=?""",
            (rid,)
        ).fetchone()

    if not row:
        await callback.answer(lang_text(lang,"Заявка не найдена","Request not found"), show_alert=True)
        return

    shop, amount, note, reserved_by, reserved_until, created_at = row
    text = generate_my_request_text(shop, amount, note, created_at, reserved_until)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=lang_text(lang,"👥 Забронировать","👥 Reserve"),
            callback_data=f"reserve:{rid}:{offset}"
        )],
        [InlineKeyboardButton(
            text=lang_text(lang,"⬅️ Назад","⬅️ Back"),
            callback_data=f"browse:{offset}"
        )]
    ])

    msg = await bot.send_message(callback.message.chat.id, text, reply_markup=kb)
    user_messages.setdefault(uid, []).append(msg.message_id)

    await callback.answer()

@router.callback_query(F.data.startswith("page:"))
async def cb_page(callback: types.CallbackQuery):
    pass

# ---------- бронь ----------
# ================== CALLBACK HANDLERS ==================

@router.callback_query(F.data.startswith("reserve:"))
async def cb_reserve(callback: types.CallbackQuery):
    print(f"✅ Reserve clicked: {callback.data}")
    try:
        _, rid_str, offset = callback.data.split(":")
        rid = int(rid_str)
    except (ValueError, IndexError):
        print("❌ Failed to parse callback data")
        await callback.answer("Invalid format", show_alert=True)
        return

    uid = callback.from_user.id
    until = datetime.now(timezone.utc) + timedelta(days=2)
    print(f"ℹ️ Reserve for UID={uid}, RID={rid}, until={until}")

    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT reserved_by, reserved_until FROM requests WHERE id=?", (rid,)
        ).fetchone()
        print(f"📦 DB row: {row}")

        if row and row[0] and row[0] != uid and row[1]:
            reserved_until_dt = datetime.fromisoformat(row[1])
            if reserved_until_dt.tzinfo is None:
                reserved_until_dt = reserved_until_dt.replace(tzinfo=timezone.utc)

            if reserved_until_dt > datetime.now(timezone.utc):
                print("⛔ Already reserved by someone else")
                await callback.answer(
                    lang_text(get_lang(uid), "⛔ Уже забронирована", "⛔ Already reserved"),
                    show_alert=True
                )
                return

        print("🔁 Updating reservation in DB")
        con.execute(
            "UPDATE requests SET reserved_by=?, reserved_until=? WHERE id=?",
            (uid, until.isoformat(), rid)
        )
        con.commit()

    print("⏰ Scheduling release/reminder…")
    schedule_release(rid, until)
    schedule_reminder(rid, uid)

    await callback.message.edit_reply_markup(reply_markup=None)

    await callback.answer(
        lang_text(get_lang(uid), "✅ Забронировано на 48 ч", "✅ Reserved for 48 h"),
        show_alert=True
    )

    if offset == "my":
        await show_my_requests(callback.message.chat.id, uid)
    else:
        await show_requests(callback.message.chat.id, uid, int(offset))

    print("✅ reserve handler завершился без ошибок")

# ---------- продление ----------
@router.callback_query(F.data.startswith("renew:"))
async def cb_renew(callback: types.CallbackQuery):
    print(f"♻️ Renew clicked: {callback.data}")
    try:
        _, rid_str, offset = callback.data.split(":")
        rid = int(rid_str)
    except (ValueError, IndexError):
        print("❌ Failed to parse callback data")
        await callback.answer("Invalid format", show_alert=True)
        return

    uid = callback.from_user.id
    until = datetime.now(timezone.utc) + timedelta(days=2)
    print(f"🔁 Renewing reservation UID={uid}, RID={rid}, until={until}")

    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(
            "SELECT reserved_by FROM requests WHERE id=?", (rid,)
        ).fetchone()
        if not row or row[0] != uid:
            await callback.answer(
                lang_text(get_lang(uid), "⛔ Вы не бронировали", "⛔ You didn't reserve this"),
                show_alert=True
            )
            return

        con.execute(
            "UPDATE requests SET reserved_until=? WHERE id=?",
            (until.isoformat(), rid)
        )
        con.commit()

    schedule_release(rid, until)
    schedule_reminder(rid, uid)

    await callback.answer(
        lang_text(get_lang(uid), "✅ Бронь продлена на 48 ч", "✅ Reservation extended for 48 h"),
        show_alert=True
    )

    if offset == "my":
        await show_my_requests(callback.message.chat.id, uid)
    else:
        await show_requests(callback.message.chat.id, uid, int(offset))

    print("✅ renew handler завершился без ошибок")


# ─────────────────── ОБРАБОТЧИК “Занято” ──────────────────────────
# ───────── noop ─────────
@router.callback_query(F.data == "noop")
async def cb_noop(cb: types.CallbackQuery):
    lang = get_lang(cb.from_user.id)
    await cb.answer(
        lang_text(lang, "⛔ Занято", "⛔ Busy"),
        show_alert=True
    )


# └───────────── ФУНКЦИЯ ПОКАЗА ГЛАВНОГО МЕНЮ ┐
async def show_main_menu(chat_id: int, user_id: int, edit_message: types.Message | None = None):
    lang = get_lang(user_id)

    try:
        if edit_message:
            await edit_message.edit_text(
                lang_text(lang, "✨ Главное меню:", "✨ Main menu:"),
                reply_markup=main_menu_kb(lang)
            )
        else:
            await delete_old_messages(bot, chat_id, user_id)
            msg = await bot.send_message(
                chat_id,
                lang_text(lang, "✨ Главное меню:", "✨ Main menu:"),
                reply_markup=main_menu_kb(lang)
            )
            user_messages.setdefault(user_id, []).append(msg.message_id)
    except Exception:
        await delete_old_messages(bot, chat_id, user_id)
        msg = await bot.send_message(
            chat_id,
            lang_text(lang, "✨ Главное меню:", "✨ Main menu:"),
            reply_markup=main_menu_kb(lang)
        )
        user_messages.setdefault(user_id, []).append(msg.message_id)


# ————————— ОБРАБОТЧИК /start —————————
@router.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    uid = message.from_user.id

    # 🧹 Сброс состояния FSM
    await state.clear()

    # 📤 Показываем главное меню
    await show_main_menu(message.chat.id, uid)


# ————————— DEBUG CALLBACK (только для разработчика) —————————
DEV_IDS = {517044272}  # ← сюда впиши свой Telegram ID

@router.callback_query()
async def debug_cb(cb: types.CallbackQuery):
    if cb.from_user.id in DEV_IDS:
        print("📩 Callback data:", cb.data)
        await cb.answer("📩 Debug callback", show_alert=True)
    else:
        # Не тревожим обычных пользователей
        logger.warning("⚠️ Необработанный callback от UID=%s: %s", cb.from_user.id, cb.data)
        await cb.answer()


# ================== MAIN ==================
async def main():
    logger.info("⚙️ Запуск init_db()")
    init_db()

    scheduler.start()
    scheduler.add_job(import_csv, trigger="interval", minutes=5, id="auto_import", replace_existing=True)

    logger.info("🔧 Бот запускается...")
    await bot.delete_webhook(drop_pending_updates=True)

    me = await bot.me()
    logger.info("🤖 Бот запущен как @%s", me.username)

    await import_csv()  # вручную подгружаем CSV

    # ⬇️ ВАЖНО: запуск поллинга, чтобы бот начал слушать обновления
    await dp.start_polling(bot)

# ⬇️ Этот блок ДОЛЖЕН БЫТЬ
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
