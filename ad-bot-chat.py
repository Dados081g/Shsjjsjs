"""
Бот рекламы в Telegram-ЧАТЕ (группе/супергруппе).

Что умеет (пока всё БЕСПЛАТНО, оплата включается одной переменной):
    🔁 Автопостинг            - бот повторяет пост клиента каждые N секунд (по умолчанию 10)
    📌 Пост + автозакреп       - бот сразу кидает пост клиента в чат и закрепляет его
    🔒 Обязательная подписка   - клиент присылает свой канал/чат, а участники чата не смогут
                                писать, пока не подпишутся на него

Установка:
    pip install aiogram aiosqlite

Переменные окружения:
    BOT_TOKEN            - токен бота от @BotFather
    CHAT_ID              - id вашего чата (например -1001234567890); бот должен быть админом
                           с правами: удалять и закреплять сообщения
    THREAD_ID            - (необязательно) id темы, если в чате включены топики
    ADMIN_IDS            - id админов через запятую (получают уведомления, могут останавливать заказы)
    TZ                   - часовой пояс (по умолчанию Europe/Moscow)
    PAYMENTS_ENABLED     - "1" включит оплату Telegram Stars (по умолчанию "0" - бесплатно)
    AUTOPOST_INTERVAL    - как часто повторять пост, сек (по умолчанию 10)
    AUTOPOST_DELETE_PREV - "1": новый повтор заменяет предыдущий (чат не засоряется),
                           "0": все повторы остаются в чате (по умолчанию "1")

Для обязательной подписки бот должен быть АДМИНОМ в канале/чате клиента
(иначе Telegram не даёт проверять подписчиков).

Запуск:
    python ad-bot-chat.py
"""
import asyncio
import logging
import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ContentType
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (CallbackQuery, LabeledPrice, Message,
                           PreCheckoutQuery)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ============================ НАСТРОЙКИ ============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8831074063:AAHXzewgWmeL6YE3Vj5byyq9RTg7VeFlRSw")
CHAT_ID = int(os.getenv("CHAT_ID", "-1004463594134"))
THREAD_ID = int(os.getenv("THREAD_ID", "0")) or None
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
TZ = ZoneInfo(os.getenv("TZ", "Europe/Moscow"))
DB_PATH = "ads.db"

PAYMENTS_ENABLED = os.getenv("PAYMENTS_ENABLED", "0") == "1"
AUTOPOST_INTERVAL = int(os.getenv("AUTOPOST_INTERVAL", "10"))
AUTOPOST_DELETE_PREV = os.getenv("AUTOPOST_DELETE_PREV", "1") == "1"

SERVICES = {
    "autopost": {
        "title": "🔁 Автопостинг",
        "pin": False,
        "limit": 2,  # сколько таких заказов может идти одновременно
        "hint": f"Пришлите пост (текст или фото с подписью). Бот будет публиковать его "
                f"в чате каждые {AUTOPOST_INTERVAL} сек.",
    },
    "pin": {
        "title": "📌 Пост + автозакреп",
        "pin": True,
        "limit": 1,
        "hint": "Пришлите пост (текст или фото с подписью). Бот сразу опубликует его в чате и закрепит.",
    },
    "sub": {
        "title": "🔒 Обязательная подписка",
        "pin": False,
        "limit": 3,
        "hint": "",
    },
}

# Длительности и цены в Telegram Stars (цены нужны только при PAYMENTS_ENABLED=1; поставьте свои)
DURATIONS = {1: "1 час", 6: "6 часов", 24: "24 часа", 168: "7 дней"}
PRICES = {
    "autopost": {1: 50, 6: 200, 24: 500, 168: 2500},
    "pin": {1: 30, 6: 100, 24: 300, 168: 1500},
    "sub": {1: 100, 6: 300, 24: 600, 168: 3000},
}

MAX_TEXT_LEN = 900            # лимит подписи к фото в Telegram - 1024
PENDING_TTL = 600             # сек. на оплату (только при включённой оплате)
MAX_ORDERS_PER_USER = 1       # активных заказов на одного клиента (у админов лимита нет)
SCHEDULER_TICK = 1            # как часто планировщик проверяет заказы, сек
SUB_CACHE_TTL = 60            # сек. помнить, что человек подписан (меньше запросов к Telegram)
WARN_TTL = 30                 # сек. висит просьба подписаться, и не чаще раза в это время на человека

# Автомодерация: запрещённые слова/паттерны (дополните под себя)
STOP_PATTERNS = [
    r"казино", r"ставк[аи] на спорт", r"букмекер", r"порн", r"18\+",
    r"быстр\w+ заработ", r"гарантированн\w+ доход", r"инвестиц\w+ без риска",
    r"криптопирамид", r"займ\w* без отказа", r"наркот", r"оружие",
]
STOP_RE = re.compile("|".join(STOP_PATTERNS), re.IGNORECASE)
# Запрещены сокращатели ссылок
BAD_LINK_RE = re.compile(r"(bit\.ly|tinyurl|goo\.gl|clck\.ru|cutt\.ly)", re.IGNORECASE)
# ===================================================================

logging.basicConfig(level=logging.INFO)
router = Router()
router.message.filter(F.chat.type == "private")
router.callback_query.filter(F.message.chat.type == "private")

group_router = Router()  # обработчики внутри самого чата

flood_until = 0.0  # если Telegram попросил притормозить, автопостинг ждёт до этого времени
_sub_cache: dict[tuple[int, int], float] = {}
_exempt_cache: dict[int, float] = {}
_warned: dict[int, float] = {}
_bg_tasks: set = set()


class Order(StatesGroup):
    material = State()
    target = State()


# ============================== БД ==============================
NEW_COLUMNS = {
    "target_chat_id": "INTEGER",
    "target_title": "TEXT",
    "target_link": "TEXT",
    "last_post_ts": "INTEGER DEFAULT 0",
}


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                tariff TEXT NOT NULL,          -- ключ из SERVICES: autopost/pin/sub
                text TEXT,
                photo TEXT,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                price INTEGER NOT NULL,
                status TEXT NOT NULL,          -- pending/paid/live/done/refunded/cancelled/expired
                created_ts INTEGER NOT NULL,
                message_id INTEGER,
                charge_id TEXT,
                target_chat_id INTEGER,
                target_title TEXT,
                target_link TEXT,
                last_post_ts INTEGER DEFAULT 0
            )"""
        )
        # миграция старой базы: добавляем недостающие колонки
        cur = await db.execute("PRAGMA table_info(orders)")
        existing = {row[1] for row in await cur.fetchall()}
        for col, ddl in NEW_COLUMNS.items():
            if col not in existing:
                await db.execute(f"ALTER TABLE orders ADD COLUMN {col} {ddl}")
        await db.commit()


async def capacity(kind: str, exclude_id: int = 0):
    """Сколько заказов этого типа сейчас занято и когда освободится ближайшее место."""
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """SELECT COUNT(*), MIN(end_ts) FROM orders
               WHERE tariff=? AND id!=?
                 AND (status IN ('paid','live') OR (status='pending' AND created_ts > ?))""",
            (kind, exclude_id, now - PENDING_TTL),
        )
        return await cur.fetchone()


async def user_active_count(user_id: int) -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """SELECT COUNT(*) FROM orders
               WHERE user_id=? AND (status IN ('paid','live') OR (status='pending' AND created_ts > ?))""",
            (user_id, now - PENDING_TTL),
        )
        (cnt,) = await cur.fetchone()
    return cnt


async def get_order(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM orders WHERE id=?", (order_id,))
        return await cur.fetchone()


async def set_status(order_id: int, status: str, **extra):
    fields = ", ".join([f"{k}=?" for k in extra] + ["status=?"])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE orders SET {fields} WHERE id=?", (*extra.values(), status, order_id))
        await db.commit()


def fmt(ts: int) -> str:
    return datetime.fromtimestamp(ts, TZ).strftime("%d.%m.%Y %H:%M")


def svc_title(key: str) -> str:
    return SERVICES.get(key, {}).get("title", key)


def price_label(key: str, hours: int) -> str:
    return f"{PRICES[key][hours]} ⭐" if PAYMENTS_ENABLED else "бесплатно"


def spawn(coro):
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def delete_later(bot: Bot, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


# ========================== МОДЕРАЦИЯ ===========================
def check_material(text: str) -> str | None:
    """Возвращает причину отказа или None, если всё в порядке."""
    if not text or len(text.strip()) < 10:
        return "Текст слишком короткий (минимум 10 символов)."
    if len(text) > MAX_TEXT_LEN:
        return f"Текст слишком длинный: {len(text)} из {MAX_TEXT_LEN} символов."
    if STOP_RE.search(text):
        return "В тексте найдены запрещённые темы или формулировки."
    if BAD_LINK_RE.search(text):
        return "Ссылки-сокращатели запрещены, пришлите прямую ссылку."
    return None


# ============================ СЦЕНАРИЙ ===========================
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    kb = InlineKeyboardBuilder()
    for key, s in SERVICES.items():
        kb.button(text=s["title"], callback_data=f"svc:{key}")
    kb.adjust(1)
    note = "" if PAYMENTS_ENABLED else "\n\n🎁 Сейчас всё бесплатно!"
    await message.answer(
        "👋 Здесь можно разместить рекламу в нашем чате автоматически.\n\n"
        f"🔁 <b>Автопостинг</b> - бот повторяет ваш пост каждые {AUTOPOST_INTERVAL} сек.\n"
        "📌 <b>Пост + автозакреп</b> - бот кидает ваш пост в чат и закрепляет.\n"
        "🔒 <b>Обязательная подписка</b> - участники не смогут писать в чате, "
        f"пока не подпишутся на ваш канал или чат.{note}\n\n"
        "Выберите услугу:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML",
    )


@router.message(Command("my"))
async def my_orders(message: Message):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM orders WHERE user_id=? AND status!='expired' ORDER BY id DESC LIMIT 10",
            (message.from_user.id,),
        )
        rows = await cur.fetchall()
    if not rows:
        return await message.answer("У вас пока нет заказов. Нажмите /start.")
    names = {"pending": "ждёт оплаты", "paid": "запускается", "live": "работает",
             "done": "завершён", "refunded": "возврат", "cancelled": "остановлен"}
    kb = InlineKeyboardBuilder()
    lines = []
    for r in rows:
        lines.append(f"№{r['id']} | {svc_title(r['tariff'])} | до {fmt(r['end_ts'])} | "
                     f"{names.get(r['status'], r['status'])}")
        if r["status"] in ("paid", "live"):
            kb.button(text=f"⏹ Остановить №{r['id']}", callback_data=f"stop:{r['id']}")
    kb.adjust(1)
    await message.answer("\n".join(lines), reply_markup=kb.as_markup())


async def can_order(user_id: int, key: str) -> str | None:
    """Причина, почему заказать нельзя, или None."""
    if user_id not in ADMIN_IDS and await user_active_count(user_id) >= MAX_ORDERS_PER_USER:
        return "У вас уже есть активный заказ. Посмотреть или остановить его: /my"
    count, first_end = await capacity(key)
    if count >= SERVICES[key]["limit"]:
        when = f" Ближайшее освободится {fmt(first_end)}." if first_end else ""
        return f"Сейчас все места для этой услуги заняты.{when}"
    return None


@router.callback_query(F.data.startswith("svc:"))
async def pick_service(call: CallbackQuery, state: FSMContext):
    key = call.data.split(":")[1]
    if key not in SERVICES:
        return await call.answer("Услуга не найдена", show_alert=True)
    reason = await can_order(call.from_user.id, key)
    if reason:
        return await call.answer(reason, show_alert=True)

    await state.clear()
    await state.update_data(service=key)
    kb = InlineKeyboardBuilder()
    for hours, label in DURATIONS.items():
        kb.button(text=f"{label} - {price_label(key, hours)}", callback_data=f"dur:{hours}")
    kb.adjust(2)
    await call.message.edit_text(
        f"Вы выбрали: <b>{SERVICES[key]['title']}</b>\n\nНа какой срок?",
        reply_markup=kb.as_markup(),
        parse_mode="HTML",
    )
    await call.answer()


@router.callback_query(F.data.startswith("dur:"))
async def pick_duration(call: CallbackQuery, state: FSMContext):
    hours = int(call.data.split(":")[1])
    data = await state.get_data()
    key = data.get("service")
    if key not in SERVICES or hours not in DURATIONS:
        return await call.answer("Начните заново: /start", show_alert=True)
    await state.update_data(hours=hours)

    if key == "sub":
        await state.set_state(Order.target)
        text = (
            "Пришлите канал или чат, на который участники должны подписаться:\n"
            "• @username или ссылку вида t.me/username\n"
            "• или перешлите сюда любое сообщение из вашего канала\n\n"
            "⚠️ Сначала добавьте этого бота в АДМИНЫ вашего канала/чата, иначе я не смогу "
            "проверять подписку. Ссылки-приглашения вида t.me/+... не подойдут, "
            "для закрытого канала перешлите из него сообщение."
        )
    else:
        await state.set_state(Order.material)
        text = f"{SERVICES[key]['hint']}\nМаксимум {MAX_TEXT_LEN} символов."
    await call.message.edit_text(f"{SERVICES[key]['title']} на {DURATIONS[hours]}\n\n{text}")
    await call.answer()


@router.message(Order.material, F.text | F.photo)
async def got_material(message: Message, state: FSMContext, bot: Bot):
    text = message.caption if message.photo else message.text
    reason = check_material(text or "")
    if reason:
        return await message.answer(f"❌ {reason}\nИсправьте материал и пришлите его снова.")
    photo = message.photo[-1].file_id if message.photo else None
    await state.update_data(text=text, photo=photo)
    await finalize_order(message, state, bot)


@router.message(Order.material)
async def material_wrong(message: Message):
    await message.answer("Пришлите текст или фото с подписью.")


async def resolve_target(bot: Bot, message: Message):
    """Находит канал/чат из сообщения клиента. Возвращает (данные, None) или (None, ошибка)."""
    origin = getattr(message, "forward_origin", None)
    chat = getattr(origin, "chat", None) or getattr(message, "forward_from_chat", None)

    if chat is None:
        text = (message.text or "").strip()
        m = re.search(r"(?:t\.me/|@)([A-Za-z][A-Za-z0-9_]{3,31})", text)
        if not m:
            return None, ("Не понял. Пришлите @username, ссылку t.me/username или перешлите "
                          "сообщение из канала.")
        try:
            chat = await bot.get_chat("@" + m.group(1))
        except TelegramBadRequest:
            return None, "Не нашёл такой канал/чат. Проверьте адрес."

    if chat.type not in ("channel", "supergroup", "group"):
        return None, "Нужен канал или чат (не личный аккаунт и не бот)."
    if chat.id == CHAT_ID:
        return None, "Нельзя выбрать тот же чат, в котором стоит подписка."

    me = await bot.get_me()
    try:
        member = await bot.get_chat_member(chat.id, me.id)
    except Exception:
        member = None
    if not member or member.status not in ("administrator", "creator"):
        return None, (f"Бот не админ в «{chat.title or chat.username}». Добавьте @{me.username} "
                      "в администраторы и пришлите канал снова.")

    if chat.username:
        link = f"https://t.me/{chat.username}"
    else:
        try:
            link = await bot.export_chat_invite_link(chat.id)
        except Exception:
            return None, ("У канала нет публичного адреса, а бот не может создать ссылку-приглашение. "
                          "Дайте боту право «Приглашать пользователей» или сделайте адрес публичным.")
    return {"id": chat.id, "title": chat.title or chat.username or str(chat.id), "link": link}, None


@router.message(Order.target)
async def got_target(message: Message, state: FSMContext, bot: Bot):
    target, error = await resolve_target(bot, message)
    if error:
        return await message.answer(f"❌ {error}")
    await state.update_data(text=f"Обязательная подписка: {target['title']}", photo=None, target=target)
    await finalize_order(message, state, bot)


async def finalize_order(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    user = message.from_user
    key, hours = data["service"], data["hours"]

    count, _ = await capacity(key)
    if count >= SERVICES[key]["limit"]:
        return await message.answer("😔 Пока вы оформляли заказ, места заняли. Попробуйте позже: /start")

    now = int(time.time())
    end = now + hours * 3600
    price = PRICES[key][hours] if PAYMENTS_ENABLED else 0
    target = data.get("target") or {}
    status = "pending" if PAYMENTS_ENABLED else "paid"  # бесплатный заказ сразу считается «оплаченным»

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """INSERT INTO orders (user_id, username, tariff, text, photo, start_ts, end_ts, price,
                                   status, created_ts, target_chat_id, target_title, target_link)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (user.id, user.username, key, data["text"], data.get("photo"), now, end, price,
             status, now, target.get("id"), target.get("title"), target.get("link")),
        )
        order_id = cur.lastrowid
        await db.commit()

    if not PAYMENTS_ENABLED:
        return await confirm_order(bot, order_id)

    title = SERVICES[key]["title"]
    await message.answer(
        f"Заказ №{order_id}\n{title}, {DURATIONS[hours]}\n\n"
        f"Заказ ждёт оплаты {PENDING_TTL // 60} минут. Оплатите счёт ниже 👇"
    )
    await bot.send_invoice(
        chat_id=user.id,
        title=title,
        description=f"{title}, {DURATIONS[hours]}",
        payload=str(order_id),
        provider_token="",  # для Stars не нужен
        currency="XTR",
        prices=[LabeledPrice(label=title, amount=price)],
    )


async def confirm_order(bot: Bot, order_id: int):
    """Заказ готов к запуску: сообщаем клиенту и админам."""
    order = await get_order(order_id)
    await bot.send_message(
        order["user_id"],
        f"✅ Заказ №{order_id} принят"
        f"{'' if PAYMENTS_ENABLED else ' (бесплатно)'}.\n"
        f"{svc_title(order['tariff'])}: запуск через несколько секунд, работает до {fmt(order['end_ts'])}.\n"
        "Остановить досрочно: /my",
    )
    kb = InlineKeyboardBuilder()
    kb.button(
        text="🚫 Остановить" + (" и вернуть Stars" if PAYMENTS_ENABLED else ""),
        callback_data=f"cancel:{order_id}",
    )
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(
                admin,
                f"🆕 Заказ №{order_id} от @{order['username'] or order['user_id']}\n"
                f"{svc_title(order['tariff'])} | до {fmt(order['end_ts'])} | "
                f"{order['price'] or 'бесплатно'}{' ⭐' if order['price'] else ''}\n\n{order['text']}",
                reply_markup=kb.as_markup(),
            )
        except Exception:
            logging.exception("Не удалось уведомить админа %s", admin)


# ============================== ОПЛАТА ===========================
# Работает только при PAYMENTS_ENABLED=1
@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    order = await get_order(int(query.invoice_payload))
    if not order or order["status"] != "pending":
        return await query.answer(ok=False, error_message="Заказ не найден или уже обработан.")
    if time.time() - order["created_ts"] > PENDING_TTL:
        return await query.answer(ok=False, error_message="Время на оплату истекло, оформите заказ заново.")
    count, _ = await capacity(order["tariff"], exclude_id=order["id"])
    if count >= SERVICES[order["tariff"]]["limit"]:
        return await query.answer(ok=False, error_message="Места для этой услуги уже заняты.")
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def paid(message: Message, bot: Bot):
    sp = message.successful_payment
    order_id = int(sp.invoice_payload)
    order = await get_order(order_id)
    duration = order["end_ts"] - order["start_ts"]
    now = int(time.time())
    # срок отсчитываем с момента оплаты
    await set_status(order_id, "paid", charge_id=sp.telegram_payment_charge_id,
                     start_ts=now, end_ts=now + duration)
    await confirm_order(bot, order_id)


# ======================= ОСТАНОВКА ЗАКАЗОВ ========================
async def take_down(bot: Bot, o):
    """Убирает из чата пост заказа (открепляет и удаляет)."""
    if o["tariff"] == "sub" or not o["message_id"]:
        return
    if SERVICES.get(o["tariff"], {}).get("pin"):
        try:
            await bot.unpin_chat_message(CHAT_ID, o["message_id"])
        except Exception:
            pass
    try:
        await bot.delete_message(CHAT_ID, o["message_id"])
    except Exception:
        logging.warning("Не удалось удалить пост заказа %s", o["id"])


async def cancel_order(bot: Bot, o, reason: str, money_back: bool):
    """Останавливает заказ; при money_back и наличии оплаты возвращает Stars."""
    if o["status"] == "live":
        await take_down(bot, o)
    new_status = "cancelled"
    try:
        if money_back and o["charge_id"]:
            await bot.refund_star_payment(o["user_id"], o["charge_id"])
            new_status = "refunded"
    except Exception:
        logging.exception("Не удалось вернуть оплату по заказу %s", o["id"])
    await set_status(o["id"], new_status)
    try:
        await bot.send_message(o["user_id"], f"↩️ Заказ №{o['id']}: {reason}")
    except Exception:
        pass


@router.callback_query(F.data.startswith("cancel:"))
async def admin_cancel(call: CallbackQuery, bot: Bot):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("Нет доступа", show_alert=True)
    order = await get_order(int(call.data.split(":")[1]))
    if not order or order["status"] not in ("paid", "live"):
        return await call.answer("Заказ уже не активен", show_alert=True)
    await cancel_order(bot, order, "остановлен модератором." +
                       (" Stars возвращены." if order["charge_id"] else ""), money_back=True)
    await call.message.edit_text(call.message.text + "\n\n🚫 Остановлено.")
    await call.answer()


@router.callback_query(F.data.startswith("stop:"))
async def owner_stop(call: CallbackQuery, bot: Bot):
    order = await get_order(int(call.data.split(":")[1]))
    if not order or (order["user_id"] != call.from_user.id and call.from_user.id not in ADMIN_IDS):
        return await call.answer("Заказ не найден", show_alert=True)
    if order["status"] not in ("paid", "live"):
        return await call.answer("Заказ уже не активен", show_alert=True)
    await cancel_order(bot, order, "вы остановили заказ.", money_back=False)
    await call.message.edit_text(f"⏹ Заказ №{order['id']} остановлен.")
    await call.answer()


# ======================= ОБРАБОТЧИКИ В ЧАТЕ =======================
@group_router.message(Command("ads", "reklama"), F.chat.id == CHAT_ID)
async def group_ads(message: Message, bot: Bot):
    me = await bot.get_me()
    kb = InlineKeyboardBuilder()
    kb.button(text="📢 Разместить рекламу", url=f"https://t.me/{me.username}?start=buy")
    await message.reply("Хотите разместить рекламу в этом чате? Оформление в личке бота:",
                        reply_markup=kb.as_markup())


@group_router.message(F.pinned_message, F.chat.id == CHAT_ID)
async def clean_pin_notice(message: Message):
    """Убираем служебное сообщение «бот закрепил сообщение»."""
    try:
        await message.delete()
    except Exception:
        pass


# ---------------------- обязательная подписка ----------------------
GATED_TYPES = [
    ContentType.TEXT, ContentType.PHOTO, ContentType.VIDEO, ContentType.STICKER,
    ContentType.ANIMATION, ContentType.DOCUMENT, ContentType.VOICE,
    ContentType.VIDEO_NOTE, ContentType.AUDIO, ContentType.POLL,
]


async def active_gates():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT target_chat_id, target_title, target_link FROM orders "
            "WHERE tariff='sub' AND status='live'"
        )
        return await cur.fetchall()


async def is_exempt(bot: Bot, user_id: int) -> bool:
    """Админов чата и админов бота подписка не касается."""
    if user_id in ADMIN_IDS:
        return True
    if _exempt_cache.get(user_id, 0) > time.time():
        return True
    try:
        m = await bot.get_chat_member(CHAT_ID, user_id)
    except Exception:
        return False
    if m.status in ("administrator", "creator"):
        _exempt_cache[user_id] = time.time() + 300
        return True
    return False


async def is_subscribed(bot: Bot, chat_id: int, user_id: int) -> bool:
    key = (chat_id, user_id)
    if _sub_cache.get(key, 0) > time.time():
        return True
    try:
        m = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        # не смогли проверить (бота убрали из админов, канал удалён) - не блокируем людей
        logging.exception("Не удалось проверить подписку на %s", chat_id)
        return True
    ok = m.status in ("member", "administrator", "creator") or (
        m.status == "restricted" and getattr(m, "is_member", False))
    if ok:
        _sub_cache[key] = time.time() + SUB_CACHE_TTL
    return ok


@group_router.message(F.chat.id == CHAT_ID, F.content_type.in_(GATED_TYPES))
async def gate(message: Message, bot: Bot):
    user = message.from_user
    if not user or user.is_bot or message.sender_chat or message.is_automatic_forward:
        return
    gates = await active_gates()
    if not gates:
        return
    if await is_exempt(bot, user.id):
        return
    missing = [g for g in gates if not await is_subscribed(bot, g["target_chat_id"], user.id)]
    if not missing:
        return

    try:
        await message.delete()
    except Exception:
        logging.warning("Нет права удалять сообщения в чате")

    now = time.time()
    if now - _warned.get(user.id, 0) < WARN_TTL:
        return  # просьба уже висит, не спамим
    _warned[user.id] = now

    kb = InlineKeyboardBuilder()
    for g in missing:
        kb.button(text=f"📢 {g['target_title']}", url=g["target_link"])
    kb.button(text="✅ Я подписался", callback_data=f"chk:{user.id}")
    kb.adjust(1)
    sent = await bot.send_message(
        CHAT_ID,
        f"{user.mention_html()}, чтобы писать в этом чате, подпишитесь:",
        parse_mode="HTML",
        reply_markup=kb.as_markup(),
        message_thread_id=message.message_thread_id if message.is_topic_message else None,
    )
    spawn(delete_later(bot, CHAT_ID, sent.message_id, WARN_TTL))


@group_router.callback_query(F.data.startswith("chk:"), F.message.chat.id == CHAT_ID)
async def gate_check(call: CallbackQuery, bot: Bot):
    uid = int(call.data.split(":")[1])
    if call.from_user.id != uid:
        return await call.answer("Эта кнопка не для вас", show_alert=True)
    for g in await active_gates():
        if not await is_subscribed(bot, g["target_chat_id"], uid):
            return await call.answer("Вы ещё не подписались", show_alert=True)
    _warned.pop(uid, None)
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("Готово, можно писать!")


# ========================== ПЛАНИРОВЩИК ===========================
async def scheduler(bot: Bot):
    """Раз в секунду: запускает новые заказы, повторяет автопосты, снимает истёкшие."""
    while True:
        try:
            now = int(time.time())
            async with aiosqlite.connect(DB_PATH) as db:
                db.row_factory = aiosqlite.Row
                await db.execute(
                    "UPDATE orders SET status='expired' WHERE status='pending' AND created_ts < ?",
                    (now - PENDING_TTL,),
                )
                await db.commit()
                cur = await db.execute("SELECT * FROM orders WHERE status='paid' AND start_ts <= ?", (now,))
                to_publish = await cur.fetchall()
                cur = await db.execute("SELECT * FROM orders WHERE status='live' AND end_ts <= ?", (now,))
                to_finish = await cur.fetchall()
                cur = await db.execute(
                    "SELECT * FROM orders WHERE status='live' AND tariff='autopost' "
                    "AND end_ts > ? AND last_post_ts <= ?",
                    (now, now - AUTOPOST_INTERVAL),
                )
                to_repost = await cur.fetchall()

            for o in to_publish:
                await publish(bot, o)
            for o in to_finish:
                await finish(bot, o)
            if time.time() >= flood_until:
                for o in to_repost:
                    await repost(bot, o)
        except Exception:
            logging.exception("Ошибка планировщика")
        await asyncio.sleep(SCHEDULER_TICK)


async def send_post(bot: Bot, o, silent: bool):
    kwargs = dict(message_thread_id=THREAD_ID, disable_notification=silent)
    if o["photo"]:
        return await bot.send_photo(CHAT_ID, o["photo"], caption=o["text"], **kwargs)
    return await bot.send_message(CHAT_ID, o["text"], **kwargs)


async def publish(bot: Bot, o):
    """Первый запуск заказа."""
    key = o["tariff"]
    msg = None
    try:
        if key == "sub":
            await set_status(o["id"], "live", last_post_ts=int(time.time()))
            note = (f"🔒 Заказ №{o['id']}: обязательная подписка на «{o['target_title']}» включена "
                    f"до {fmt(o['end_ts'])}.")
        else:
            msg = await send_post(bot, o, silent=(key == "autopost"))
            if SERVICES[key]["pin"]:
                await bot.pin_chat_message(CHAT_ID, msg.message_id, disable_notification=True)
            await set_status(o["id"], "live", message_id=msg.message_id, last_post_ts=int(time.time()))
            if key == "autopost":
                note = (f"🚀 Заказ №{o['id']}: автопостинг запущен, пост выходит каждые "
                        f"{AUTOPOST_INTERVAL} сек. до {fmt(o['end_ts'])}.")
            else:
                note = f"🚀 Заказ №{o['id']}: пост опубликован и закреплён до {fmt(o['end_ts'])}."
        await bot.send_message(o["user_id"], note)
    except Exception:
        logging.exception("Запуск не удался, заказ %s", o["id"])
        if msg:  # пост ушёл, но закрепить не смогли - не оставляем его висеть
            try:
                await bot.delete_message(CHAT_ID, msg.message_id)
            except Exception:
                pass
        await cancel_order(bot, o, "не удалось запустить заказ."
                           + (" Stars возвращены автоматически." if o["charge_id"] else ""),
                           money_back=True)


async def repost(bot: Bot, o):
    """Очередной повтор автопоста."""
    global flood_until
    now = int(time.time())
    try:
        msg = await send_post(bot, o, silent=True)
    except TelegramRetryAfter as e:
        flood_until = time.time() + e.retry_after
        logging.warning("Flood control: пауза автопостинга на %s сек.", e.retry_after)
        return
    except Exception:
        logging.exception("Автопост не удался, заказ %s", o["id"])
        await set_status(o["id"], "live", last_post_ts=now)  # повторим через интервал
        return
    if AUTOPOST_DELETE_PREV and o["message_id"]:
        try:
            await bot.delete_message(CHAT_ID, o["message_id"])
        except Exception:
            pass
    await set_status(o["id"], "live", message_id=msg.message_id, last_post_ts=now)


async def finish(bot: Bot, o):
    await take_down(bot, o)
    await set_status(o["id"], "done")
    try:
        await bot.send_message(
            o["user_id"],
            f"🏁 Заказ №{o['id']} завершён. Размещение: {fmt(o['start_ts'])} - {fmt(o['end_ts'])}.\n"
            "Спасибо! Чтобы заказать снова, нажмите /start.",
        )
    except Exception:
        pass


# ============================== ЗАПУСК ============================
async def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise SystemExit("Задайте BOT_TOKEN и CHAT_ID в переменных окружения")
    await init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.include_router(group_router)

    # проверка, что бот админ в чате с нужными правами
    try:
        me = await bot.get_me()
        member = await bot.get_chat_member(CHAT_ID, me.id)
        if member.status not in ("administrator", "creator"):
            logging.warning("Бот не админ в чате %s: публикация, закреп и подписка работать не будут", CHAT_ID)
        elif member.status == "administrator" and not (member.can_delete_messages and member.can_pin_messages):
            logging.warning("Боту нужны права: удалять и закреплять сообщения")
    except Exception:
        logging.exception("Не удалось проверить права бота в чате %s", CHAT_ID)
    asyncio.create_task(scheduler(bot))
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
