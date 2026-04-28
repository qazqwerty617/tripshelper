import asyncio
import logging
import io
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart
from aiogram.client.session.aiohttp import AiohttpSession
from config import BOT_TOKEN, EXCEL_PATH
import os
import llm_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_MSG_LEN = 4096

async def send_long_message(message: Message, text: str):
    """Відправляє довге повідомлення частинами якщо перевищує ліміт Telegram"""
    if len(text) <= MAX_MSG_LEN:
        await message.answer(text, disable_web_page_preview=True)
    else:
        chunks = [text[i:i+MAX_MSG_LEN] for i in range(0, len(text), MAX_MSG_LEN)]
        for chunk in chunks:
            await message.answer(chunk, disable_web_page_preview=True)

# Збільшений timeout щоб Telegram не обривав з'єднання поки LLM думає
session = AiohttpSession(timeout=300)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

ADMIN_IDS = [340517348, 8482582995]

@dp.message(CommandStart())
async def start_cmd(message: Message):
    await message.answer("Привіт! Я бот для формування красивих підбірок турів. Надішли мені текст або голосове повідомлення з деталями туру і цінами, і я все красиво оформлю.")

@dp.message(F.document)
async def handle_document(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
        
    doc = message.document
    if not doc.file_name.endswith('.xlsx'):
        await message.answer("❌ Будь ласка, надішліть файл бази готелів у форматі .xlsx")
        return
        
    msg = await message.answer("⏳ Завантажую нову базу готелів...")
    try:
        file_info = await bot.get_file(doc.file_id)
        os.makedirs(os.path.dirname(EXCEL_PATH), exist_ok=True)
        await bot.download_file(file_info.file_path, EXCEL_PATH)
        await msg.edit_text("✅ Базу готелів успішно оновлено! Нові дані вже працюють.")
    except Exception as e:
        logger.error(f"Error updating DB: {e}")
        await msg.edit_text("❌ Помилка під час оновлення файлу на сервері.")

@dp.message(F.text)
async def handle_text(message: Message):
    msg = await message.answer("✨ Формую підбірку...")
    try:
        result = await llm_service.format_tour_message(message.text)
    except Exception as e:
        logger.error(f"format_tour_message error: {e}")
        await msg.edit_text("❌ Внутрішня помилка під час генерації. Спробуй ще раз.")
        return
    try:
        if len(result) <= MAX_MSG_LEN:
            await msg.edit_text(result, disable_web_page_preview=True)
        else:
            await msg.delete()
            await send_long_message(message, result)
    except Exception as e:
        logger.error(f"edit_text error: {e}")
        await send_long_message(message, result)

@dp.message(F.voice)
async def handle_voice(message: Message):
    msg = await message.answer("🎙 Розпізнаю голосове...")
    file_id = message.voice.file_id
    file_info = await bot.get_file(file_id)
    
    buf = io.BytesIO()
    await bot.download_file(file_info.file_path, buf)
    file_bytes = buf.getvalue()
    
    text = await llm_service.transcribe_voice(file_bytes)
    if not text or text.startswith("❌"):
        await msg.edit_text("🤷 Не вдалося розпізнати текст.")
        return

    await msg.edit_text("🎙 Розпізнано. ✨ Формую підбірку...", parse_mode="HTML")
    try:
        result = await llm_service.format_tour_message(text, do_cleanup=True)
    except Exception as e:
        logger.error(f"format_tour_message voice error: {e}")
        await message.answer("❌ Внутрішня помилка під час генерації. Спробуй ще раз.")
        return
    await send_long_message(message, result)

async def main():
    if not BOT_TOKEN:
        logger.error("No BOT_TOKEN in .env")
        return
    logger.info("Bot started!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
