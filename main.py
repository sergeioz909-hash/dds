import asyncio
import logging
import re
import random
import time
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage

# ================= НАСТРОЙКИ =================
BOT_TOKEN = "7925932638:AAFiQ8mbt0q3BZgfyrOqwHuvtjkQddWBIyw"

SOURCE_CHAT_ID = -1003448861075
SOURCE_TOPIC_IDS = [11]

TARGET_CHATS = {
    -1003866302173: [2],
    -1003630448902: [5, 3],
    -1003572624954: [4, 2],
}

LOG_FILE = "numbers_log.txt"

ADMIN_IDS = [6755723968, 987654321]  # ❗️ замените на свои ID
# ==============================================

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

numbers_queue = asyncio.Queue()
active_sessions = {}
repeat_requests = {}

# ---------- Логирование ----------
def log_number_complete(phone: str, start_time: str, end_time: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{phone} {start_time}(встал)-{end_time}(слетел)\n")

# ---------- Проверки ----------
def is_target_chat_and_topic(message: types.Message) -> bool:
    chat_id = message.chat.id
    if chat_id not in TARGET_CHATS:
        return False
    allowed_topics = TARGET_CHATS[chat_id]
    return message.message_thread_id in allowed_topics

def is_source_chat_and_topic(message: types.Message) -> bool:
    return (message.chat.id == SOURCE_CHAT_ID and
            message.message_thread_id in SOURCE_TOPIC_IDS)

# ---------- Генерация ID ----------
def generate_item_id():
    return f"{int(time.time()*1000)}_{random.randint(1000,9999)}"

# ---------- Работа с очередью ----------
async def get_queue_items(limit=20):
    items = []
    temp = []
    try:
        while not numbers_queue.empty() and len(items) < limit:
            item = numbers_queue.get_nowait()
            items.append(item)
            temp.append(item)
    except asyncio.QueueEmpty:
        pass
    for item in temp:
        await numbers_queue.put(item)
    return items

async def remove_item_by_id(item_id: str, user_id: int) -> bool:
    temp = []
    removed = False
    while not numbers_queue.empty():
        try:
            item = numbers_queue.get_nowait()
            if item.get("item_id") == item_id:
                if (item.get("added_by_user_id") == user_id) or (user_id in ADMIN_IDS):
                    removed = True
                    logging.info(f"Удалён номер {item['phone']} по запросу {user_id}")
                else:
                    temp.append(item)
            else:
                temp.append(item)
        except asyncio.QueueEmpty:
            break
    for item in temp:
        await numbers_queue.put(item)
    return removed

async def remove_from_queue(condition_func):
    temp = []
    removed = 0
    while not numbers_queue.empty():
        try:
            item = numbers_queue.get_nowait()
            if condition_func(item):
                removed += 1
            else:
                temp.append(item)
        except asyncio.QueueEmpty:
            break
    for item in temp:
        await numbers_queue.put(item)
    return removed

# ---------- Команда /stopwork (только для админов) ----------
@dp.message(Command("stopwork"))
async def stop_work(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("⛔ Эта команда только для администраторов.")
        return

    completed = 0
    now = datetime.now().strftime("%H:%M")
    sessions_to_delete = []

    for session_id, session in list(active_sessions.items()):
        if session.get("start_time"):
            # Номер успел встать, записываем слёт
            log_number_complete(session["phone"], session["start_time"], now)
            completed += 1
            sessions_to_delete.append(session_id)

    # Удаляем завершённые сессии
    for sid in sessions_to_delete:
        del active_sessions[sid]

    # Также можно уведомить в ПК-чатах о принудительном завершении
    # (опционально, чтобы операторы видели)
    for chat_id in TARGET_CHATS:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"🛑 Работа остановлена администратором. Завершено номеров: {completed}."
            )
        except:
            pass

    await message.reply(f"✅ Работа остановлена. Завершено номеров: {completed}.")

# ---------- Команда /queue ----------
@dp.message(is_source_chat_and_topic, Command("queue"))
async def show_queue(message: types.Message):
    items = await get_queue_items(15)
    if not items:
        await message.reply("📭 Очередь пуста.")
        return
    keyboard = []
    for item in items:
        phone = item["phone"]
        item_id = item.get("item_id")
        if not item_id:
            continue
        btn_text = f"❌ {phone}"
        callback_data = f"removeitem:{item_id}"
        keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=callback_data)])
    if not keyboard:
        await message.reply("⚠️ Не удалось отобразить очередь.")
        return
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    await message.reply(
        f"📋 Очередь номеров (всего: {numbers_queue.qsize()}):\nНажмите на номер, чтобы удалить его (только свои или админ).",
        reply_markup=markup
    )

# ---------- Удаление по кнопке ----------
@dp.callback_query(lambda c: c.data.startswith("removeitem:"))
async def process_remove_item(callback: types.CallbackQuery):
    item_id = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    success = await remove_item_by_id(item_id, user_id)
    if success:
        await callback.answer("✅ Номер удалён из очереди")
        items = await get_queue_items(15)
        if not items:
            await callback.message.edit_text("📭 Очередь пуста.")
        else:
            keyboard = []
            for item in items:
                phone = item["phone"]
                item_id = item.get("item_id")
                if not item_id:
                    continue
                btn_text = f"❌ {phone}"
                callback_data = f"removeitem:{item_id}"
                keyboard.append([InlineKeyboardButton(text=btn_text, callback_data=callback_data)])
            markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
            await callback.message.edit_text(
                f"📋 Очередь номеров (всего: {numbers_queue.qsize()}):\nНажмите на номер, чтобы удалить его (только свои или админ).",
                reply_markup=markup
            )
    else:
        await callback.answer("❌ Нельзя удалить этот номер (он не ваш, или уже удалён)", show_alert=True)

# ---------- Шаг 1: Сбор номеров ----------
@dp.message(is_source_chat_and_topic)
async def collect_numbers(message: types.Message):
    text = message.text or message.caption or ""
    phones = re.findall(r"\+7\d{10}|8\d{10}", text)
    if not phones:
        return
    added_numbers = []
    for phone in phones:
        if phone.startswith('8'):
            phone = '+7' + phone[1:]
        item_id = generate_item_id()
        await numbers_queue.put({
            "phone": phone,
            "source_chat_id": message.chat.id,
            "source_msg_id": message.message_id,
            "added_by_user_id": message.from_user.id,
            "item_id": item_id
        })
        added_numbers.append(phone)
        logging.info(f"📥 Добавлен номер в очередь: {phone} от {message.from_user.id}")
    queue_size = numbers_queue.qsize()
    if len(added_numbers) == 1:
        reply_text = f"📞 Номер {added_numbers[0]} добавлен в очередь.\nВсего в очереди: {queue_size}"
    else:
        nums_str = ", ".join(added_numbers)
        reply_text = f"📞 Добавлены номера: {nums_str}\nВсего в очереди: {queue_size}"
    await message.reply(reply_text)
    try:
        await message.react(emoji="👍")
    except:
        pass

# ---------- Команда /remove (старая) ----------
@dp.message(is_source_chat_and_topic, Command("remove"))
async def remove_number(message: types.Message):
    parts = message.text.strip().split(maxsplit=1)
    is_admin = message.from_user.id in ADMIN_IDS
    if message.reply_to_message:
        target_msg_id = message.reply_to_message.message_id
        def condition(item):
            if is_admin:
                return item["source_msg_id"] == target_msg_id
            else:
                return (item["source_msg_id"] == target_msg_id and
                        item["added_by_user_id"] == message.from_user.id)
        removed = await remove_from_queue(condition)
        await message.reply(f"✅ Удалено номеров из очереди: {removed}" if removed else "❌ Не найдено номеров для удаления.")
        return
    if len(parts) == 2:
        raw_phone = parts[1]
        digits = re.sub(r"\D", "", raw_phone)
        if digits.startswith('8'):
            digits = '7' + digits[1:]
        if not digits.startswith('7'):
            await message.reply("❌ Номер должен начинаться с +7 или 8")
            return
        phone = '+' + digits
        def condition(item):
            if is_admin:
                return item["phone"] == phone
            else:
                return item["phone"] == phone and item["added_by_user_id"] == message.from_user.id
        removed = await remove_from_queue(condition)
        await message.reply(f"✅ Удалено номеров из очереди: {removed}" if removed else "❌ Номер не найден в очереди или не принадлежит вам.")
        return
    await message.reply("❌ Использование:\n/remove (в ответ на сообщение с номерами)\n/remove +7XXXXXXXXXX")

# ---------- Шаг 2: Выдача номера ----------
@dp.message(is_target_chat_and_topic,
            (F.text.lower() == "номер") | (F.text == "/номер"))
async def give_number(message: types.Message):
    user_id = message.from_user.id
    topic_id = message.message_thread_id
    for sess in active_sessions.values():
        if sess.get("user_id") == user_id and sess.get("target_topic_id") == topic_id:
            await message.answer("⚠️ У вас уже есть активный номер в этом топике. Сначала завершите его.")
            return

    if numbers_queue.empty():
        await message.answer("❌ Очередь номеров пуста.")
        return

    item = await numbers_queue.get()
    phone = item["phone"]
    source_chat_id = item["source_chat_id"]
    source_msg_id = item["source_msg_id"]

    sent_msg = await message.answer(
        f"📞 Ваш номер: `{phone}`\n\n_Ожидаю фото с кодом..._",
        parse_mode="Markdown"
    )

    session_id = f"{user_id}_{sent_msg.message_id}"
    active_sessions[session_id] = {
        "phone": phone,
        "source_chat_id": source_chat_id,
        "source_msg_id": source_msg_id,
        "target_msg_id": sent_msg.message_id,
        "target_chat_id": message.chat.id,
        "target_topic_id": topic_id,
        "user_id": user_id,
        "owner_id": item["added_by_user_id"],
        "start_time": None,
    }

# ---------- Шаг 3: Получение фото с кодом ----------
@dp.message(F.photo)
async def handle_photo(message: types.Message):
    # Проверяем, не ответ ли на запрос повтора
    if message.reply_to_message:
        key = (message.chat.id, message.message_thread_id, message.reply_to_message.message_id)
        if key in repeat_requests:
            source_msg_id = repeat_requests.pop(key)
            session = None
            for sess in active_sessions.values():
                if sess["source_msg_id"] == source_msg_id:
                    session = sess
                    break
            if not session:
                await message.answer("⚠️ Сессия не найдена, номер уже обработан.")
                return
            phone = session["phone"]
            source_chat_id = session["source_chat_id"]
            photo = message.photo[-1]
            file_id = photo.file_id
            await bot.send_photo(
                chat_id=source_chat_id,
                photo=file_id,
                caption=f"🔄 Повторный код для номера {phone}",
                reply_to_message_id=source_msg_id
            )
            await message.reply("✅ Повторный код отправлен.")
            return

    if not is_target_chat_and_topic(message):
        return

    user_id = message.from_user.id
    topic_id = message.message_thread_id

    session = None
    session_id = None
    for sid, sess in active_sessions.items():
        if sess.get("user_id") == user_id and sess.get("target_topic_id") == topic_id:
            session = sess
            session_id = sid
            break

    if not session:
        await message.answer("⚠️ У вас нет активного номера в этом топике. Сначала запросите номер командой /номер")
        return

    phone = session["phone"]
    source_chat_id = session["source_chat_id"]
    source_msg_id = session["source_msg_id"]
    target_chat_id = session["target_chat_id"]
    target_topic_id = session["target_topic_id"]

    photo = message.photo[-1]
    file_id = photo.file_id

    # Отправляем фото в исходный чат с кнопкой повтора
    repeat_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Повтор", callback_data=f"repeat:{source_msg_id}")]
    ])
    await bot.send_photo(
        chat_id=source_chat_id,
        photo=file_id,
        caption=f"📸 Код для номера {phone}",
        reply_to_message_id=source_msg_id,
        reply_markup=repeat_keyboard
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Встал", callback_data=f"success:{session_id}"),
            InlineKeyboardButton(text="❌ Слетел", callback_data=f"failed:{session_id}")
        ]
    ])
    await message.answer(
        f"Код для номера {phone} отправлен в команду. Отслеживаем статус...",
        reply_markup=keyboard
    )

# ---------- Обработка кнопки "Повтор" ----------
@dp.callback_query(lambda c: c.data.startswith("repeat:"))
async def process_repeat(callback: types.CallbackQuery):
    source_msg_id = int(callback.data.split(":", 1)[1])
    session = None
    for sess in active_sessions.values():
        if sess["source_msg_id"] == source_msg_id:
            session = sess
            break
    if not session:
        await callback.answer("❌ Номер уже обработан или сессия устарела.", show_alert=True)
        return

    if callback.from_user.id != session["owner_id"] and callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Только владелец номера может запросить повтор.", show_alert=True)
        return

    phone = session["phone"]
    target_chat_id = session["target_chat_id"]
    target_topic_id = session["target_topic_id"]

    request_msg = await bot.send_message(
        chat_id=target_chat_id,
        text=f"🔄 Запрошен повтор кода для номера {phone}. Отправьте фото в ответ на это сообщение.",
        message_thread_id=target_topic_id
    )
    key = (target_chat_id, target_topic_id, request_msg.message_id)
    repeat_requests[key] = source_msg_id

    await callback.answer("✅ Запрос на повтор отправлен оператору")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except:
        pass

# ---------- Обработка кнопок "Встал" / "Слетел" ----------
@dp.callback_query(lambda c: c.data.startswith("success:") or c.data.startswith("failed:"))
async def process_status_buttons(callback: types.CallbackQuery):
    action, session_id = callback.data.split(":", 1)
    session = active_sessions.get(session_id)
    if not session:
        await callback.answer("❌ Сессия не найдена или номер уже обработан.", show_alert=True)
        return

    phone = session["phone"]
    target_chat_id = session["target_chat_id"]
    target_msg_id = session["target_msg_id"]
    target_topic_id = session["target_topic_id"]

    if action == "success":
        start_time = datetime.now().strftime("%H:%M")
        session["start_time"] = start_time

        try:
            await bot.edit_message_text(
                chat_id=target_chat_id,
                message_id=target_msg_id,
                text=f"✅ Номер {phone} встал.",
                parse_mode=None
            )
        except Exception as e:
            if "message can't be edited" in str(e).lower():
                await bot.send_message(
                    chat_id=target_chat_id,
                    text=f"✅ Номер {phone} встал.",
                    message_thread_id=target_topic_id,
                    reply_to_message_id=target_msg_id
                )

        only_fail_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Слетел", callback_data=f"failed:{session_id}")]
        ])
        try:
            await callback.message.edit_text(
                f"✅ Номер {phone} встал. Если номер слетит, нажмите кнопку ниже.",
                reply_markup=only_fail_keyboard
            )
            await callback.answer("Статус: Встал")
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                await callback.message.answer(
                    f"✅ Номер {phone} встал. Если номер слетит, нажмите кнопку ниже.",
                    reply_markup=only_fail_keyboard
                )
                await callback.answer("Статус: Встал (новое сообщение)")

    elif action == "failed":
        if session.get("start_time"):
            end_time = datetime.now().strftime("%H:%M")
            log_number_complete(phone, session["start_time"], end_time)
        else:
            end_time = datetime.now().strftime("%H:%M")
            log_number_complete(phone, "??:??", end_time)

        del active_sessions[session_id]

        try:
            await callback.message.edit_text(
                f"❌ Номер {phone} слетел.\nПопросите новый номер командой /номер",
                reply_markup=None
            )
        except:
            await callback.message.answer(
                f"❌ Номер {phone} слетел.\nПопросите новый номер командой /номер"
            )
        await bot.send_message(
            chat_id=target_chat_id,
            text=f"⚠️ Номер {phone} слетел. Попросите новый номер командой /номер",
            message_thread_id=target_topic_id
        )
        await callback.answer("Статус: Слетел")

# ---------- Запуск ----------
async def main():
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
