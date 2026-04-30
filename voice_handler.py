import asyncio
import httpx
import logging
import json
import re
import random
import itertools
from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, GROQ_API_KEY, GROQ_API_KEYS

logger = logging.getLogger(__name__)

# --- Voice Specific Settings ---
# Models for voice transcription cleanup
VOICE_CLEANUP_MODELS = ["openai/gpt-5.4-mini", "google/gemini-2.5-flash"]

# Whisper prompt for transcription
WHISPER_PROMPT = "Майорка, Тенеріфе, BLUESEA, Globales, AzuLine, HSM, BJ Playamar, Iberostar, Rixos, готель, євро, сніданки, дорослих."

# Prompt for cleaning up voice transcription
VOICE_CLEANUP_PROMPT = """Ти — коректор туристичних текстів. Твоє завдання: виправити помилки розпізнавання голосу (особливо в назвах готелів) та чітко структурувати текст. 

ПРАВИЛА:
1. ЗБЕРЕЖИ ПОРЯДОК: Готелі повинні йти РІВНО в тому порядку, в якому їх назвав менеджер. 
2. СТРУКТУРА: Обов'язково пронумеруй кожен готель (1 готель - ..., 2 готель - ...), і поруч з ним вкажи його ціну. 
3. Виправ транслітерацію брендів: "блюсія/блю сі" -> "BLUESEA", "глобаліс" -> "Globales", "іберостар" -> "Iberostar", "азулін" -> "AzuLine". 
4. КРИТИЧНО: НЕ видаляй тип харчування (сніданки, все включено тощо)! Завжди залишай його біля готелів. 
5. НЕ видаляй жодної цифри (вік дітей, кількість ночей, дати, ціни). 
"""

# --- LLM Client for voice cleanup ---
client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

# --- Key Rotation for Groq ---
def _create_key_rotator():
    keys = GROQ_API_KEYS.copy()
    if not keys and GROQ_API_KEY:
        keys = [GROQ_API_KEY]
    random.shuffle(keys)
    return itertools.cycle(keys)

_groq_key_rotator = _create_key_rotator()

async def transcribe_voice(file_bytes: bytes) -> str:
    """Transcribes voice using Groq (Whisper) with fallback to OpenRouter."""
    active_keys = GROQ_API_KEYS if GROQ_API_KEYS else ([GROQ_API_KEY] if GROQ_API_KEY else [])
    if not active_keys:
        logger.warning("No Groq API keys available for transcription.")
    else:
        for _ in range(len(active_keys)):
            key = next(_groq_key_rotator)
            url_groq = "https://api.groq.com/openai/v1/audio/transcriptions"
            headers_groq = {"Authorization": f"Bearer {key}"}
            
            files = {"file": ("voice.ogg", file_bytes, "audio/ogg")}
            data = {
                "model": "whisper-large-v3",
                "prompt": WHISPER_PROMPT,
                "response_format": "json",
                "language": "uk",
            }
            
            async with httpx.AsyncClient() as c:
                try:
                    resp = await c.post(url_groq, headers=headers_groq, files=files, data=data, timeout=20)
                    if resp.status_code == 200:
                        text = resp.json().get("text", "")
                        if text:
                            # ЖОРСТКА АВТОЗАМІНА (працює на 100% без галюцинацій)
                            fixes = {
                                "блюсія": "BLUESEA", "Блюсія": "BLUESEA",
                                "блю сі": "BLUESEA", "Блю сі": "BLUESEA",
                                "глобаліс": "Globales", "Глобаліс": "Globales",
                                "плеймар": "Playamar", "Плеймар": "Playamar",
                                "азулін": "AzuLine", "Азулін": "AzuLine",
                                "кала мілер": "Cala Millor", "Кала Мілер": "Cala Millor",
                                "kala miller": "Cala Millor", "Kala Miller": "Cala Millor"
                            }
                            for bad, good in fixes.items():
                                text = text.replace(bad, good)
                            return text
                    logger.warning(f"Groq key {key[:10]}... returned status {resp.status_code}. Trying next key.")
                except Exception as e:
                    logger.warning(f"Groq key {key[:10]}... failed: {e}. Trying next key.")
                    continue

    # Fallback to OpenRouter
    if OPENROUTER_API_KEY:
        try:
            url_or = "https://openrouter.ai/api/v1/audio/transcriptions"
            headers_or = {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
            files = {"file": ("voice.ogg", file_bytes, "audio/ogg")}
            data_or = {
                "model": "openai/whisper-large-v3",
                "prompt": WHISPER_PROMPT
            }
            
            async with httpx.AsyncClient() as c:
                resp = await c.post(url_or, headers=headers_or, files=files, data=data_or, timeout=30)
                if resp.status_code == 200:
                    text = resp.json().get("text", "")
                    if text:
                        # ЖОРСТКА АВТОЗАМІНА
                        fixes = {
                            "блюсія": "BLUESEA", "Блюсія": "BLUESEA",
                            "блю сі": "BLUESEA", "Блю сі": "BLUESEA",
                            "глобаліс": "Globales", "Глобаліс": "Globales",
                            "плеймар": "Playamar", "Плеймар": "Playamar",
                            "азулін": "AzuLine", "Азулін": "AzuLine",
                            "кала мілер": "Cala Millor", "Кала Мілер": "Cala Millor",
                            "kala miller": "Cala Millor", "Kala Miller": "Cala Millor"
                        }
                        for bad, good in fixes.items():
                            text = text.replace(bad, good)
                        return text
        except Exception as e:
            logger.error(f"OpenRouter Whisper fallback failed: {e}")

    return "❌ Помилка розпізнавання (обидва сервіси недоступні)."

async def cleanup_transcribed_text(raw_text: str) -> str:
    """Cleans up the transcription using LLM to fix errors and hallucinations."""
    if not raw_text:
        return raw_text
    
    logger.info(f"Voice transcription raw: {raw_text}")
    
    for model in VOICE_CLEANUP_MODELS:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": VOICE_CLEANUP_PROMPT},
                    {"role": "user", "content": raw_text},
                ],
                temperature=0,
                timeout=30,
            )
            cleaned = resp.choices[0].message.content.strip()
            # Remove potential markdown code blocks
            cleaned = re.sub(r'```[a-z]*\n?', '', cleaned).strip('`').strip()
            
            if cleaned and len(cleaned) > 10:
                logger.info(f"Voice transcription after cleanup ({model}): {cleaned}")
                return cleaned
        except Exception as e:
            logger.error(f"Voice cleanup error with {model}: {e}")
            
    return raw_text

async def process_voice_message(file_bytes: bytes) -> str:
    """Full pipeline: transcription -> cleanup."""
    text = await transcribe_voice(file_bytes)
    if not text or text.startswith("❌"):
        return text
    
    cleaned_text = await cleanup_transcribed_text(text)
    return cleaned_text
