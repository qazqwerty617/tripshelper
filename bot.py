import asyncio
import logging
import io
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from aiogram.client.session.aiohttp import AiohttpSession
from config import BOT_TOKEN, EXCEL_PATH
import os
import llm_service
import voice_handler
import excel_parser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MAX_MSG_LEN = 4096

async def send_long_message(message: Message, text: str):
    """Відправляє довге повідомлення частинами якщо перевищує ліміт Telegram"""
    if not text:
        return
        
    if len(text) <= MAX_MSG_LEN:
        await message.answer(text, disable_web_page_preview=True)
    else:
        # Split by paragraphs if possible to avoid cutting in the middle of a line
        paragraphs = text.split('\n\n')
        current_chunk = ""
        for p in paragraphs:
            if len(current_chunk) + len(p) + 2 <= MAX_MSG_LEN:
                current_chunk += p + '\n\n'
            else:
                if current_chunk:
                    await message.answer(current_chunk.strip(), disable_web_page_preview=True)
                
                # If a single paragraph is too long, split it by characters
                if len(p) > MAX_MSG_LEN:
                    for i in range(0, len(p), MAX_MSG_LEN):
                        await message.answer(p[i:i+MAX_MSG_LEN], disable_web_page_preview=True)
                    current_chunk = ""
                else:
                    current_chunk = p + '\n\n'
        
        if current_chunk:
            await message.answer(current_chunk.strip(), disable_web_page_preview=True)

# Збільшений timeout щоб Telegram не обривав з'єднання поки LLM думає
session = AiohttpSession(timeout=300)
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()

ADMIN_IDS = [340517348, 8482582995]

@dp.message(CommandStart())
async def start_cmd(message: Message):
    await message.answer(
        "👋 Привіт! Я ваш розумний асистент для створення туристичних підбірок.\n\n"
        "📝 **Як я працюю:**\n"
        "1. Надішліть мені текст або голосове з деталями туру (готелі, ціни, дати).\n"
        "2. Я автоматично знайду готелі в базі, розрахую ціни з націнкою та податками.\n"
        "3. Ви отримаєте готове повідомлення для клієнта.\n\n"
        "⚙️ **Для адмінів:** надішліть .xlsx файл для оновлення бази.",
        parse_mode="Markdown"
    )

@dp.message(Command("clear_cache"))
async def clear_cache_cmd(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    excel_parser._db_cache["data"] = None
    await message.answer("✅ Кеш бази даних очищено. Наступний запит завантажить свіжі дані з Excel.")

@dp.message(F.document)
async def handle_document(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
        
    doc = message.document
    if not doc.file_name.lower().endswith(('.xlsx', '.xls')):
        await message.answer("❌ Будь ласка, надішліть файл бази готелів у форматі .xlsx")
        return
        
    msg = await message.answer("⏳ Завантажую та перевіряю нову базу готелів...")
    try:
        file_info = await bot.get_file(doc.file_id)
        os.makedirs(os.path.dirname(EXCEL_PATH), exist_ok=True)
        await bot.download_file(file_info.file_path, EXCEL_PATH)
        
        # Invalidate cache
        excel_parser._db_cache["data"] = None
        
        # Test loading
        db = excel_parser.get_hotel_db()
        if db:
            await msg.edit_text(f"✅ Базу успішно оновлено! Знайдено {len(db)} напрямків.")
        else:
            await msg.edit_text("⚠️ Файл завантажено, але він здається порожнім або має невірний формат.")
            
    except Exception as e:
        logger.error(f"Error updating DB: {e}")
        await msg.edit_text(f"❌ Помилка під час оновлення файлу: {str(e)}")

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
    
    # Use the new voice handler for transcription and cleanup
    text = await voice_handler.process_voice_message(file_bytes)
    if not text or text.startswith("❌"):
        await msg.edit_text("🤷 Не вдалося розпізнати текст.")
        return

    await msg.edit_text("✨ Формую підбірку...", parse_mode="HTML")
    try:
        # Pass the already cleaned text and disable internal cleanup in llm_service
        result = await llm_service.format_tour_message(text, do_cleanup=False)
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
