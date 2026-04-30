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
WHISPER_PROMPT = (
    "Турагент диктує підбірку готелів та ціни. Напрямки: Тенерифе, Майорка, Коста-Брава, Крит, Родос, Корфу, Кіпр. "
    "Готелі: Rixos, Mitsis, Grecotel, Iberostar, Sol, Melia. Типи харчування: сніданки, вечері, напівпансіон, все включено. "
    "Ціна в євро, авіапереліт, трансфер, страховка."
)

# Prompt for cleaning up voice transcription
VOICE_CLEANUP_PROMPT = """Ти отримуєш текст після автоматичного розпізнавання голосового повідомлення турагента.
Твоє завдання: виправити помилки розпізнавання (галлюцинації Whisper, повтори, незв'язні символи), щоб текст став чистим, АЛЕ КРИТИЧНО ВАЖЛИВО ЗБЕРЕГТИ КОЖНУ НАЗВУ ГОТЕЛЮ, ЦІНУ, ДАТУ ТА МІСТО.

ПРАВИЛА:
1. Виправляй лише явну "кашу" в словах (наприклад "Тенер і Фе" -> "Тенерифе").
2. Зберігай назви готелів як є. Якщо в тексті "Бі Джей Плаямар" — залиш як є або виправ на "BJ Playamar", якщо впевнений.
3. НЕ ВИДАЛЯЙ цифри, ціни, валюту (€, євро, дол), дати та ЧАС перельоту.
4. Якщо в тексті є повторювані фрази, видали повтори.
5. ОБОВ'ЯЗКОВО збережи НАПРЯМОК (наприклад, "Майорка", "Тенерифе").
6. Поверни ТІЛЬКИ виправлений текст.
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
            }
            
            async with httpx.AsyncClient() as c:
                try:
                    resp = await c.post(url_groq, headers=headers_groq, files=files, data=data, timeout=20)
                    if resp.status_code == 200:
                        text = resp.json().get("text", "")
                        if text: return text
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
                    return resp.json().get("text", "")
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
