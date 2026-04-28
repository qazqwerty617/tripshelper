import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# Поддержка нескольких ключей Groq для ротации (через запятую в .env)
_GROQ_KEYS_RAW = os.getenv("GROQ_API_KEYS", "")
GROQ_API_KEYS = [k.strip() for k in _GROQ_KEYS_RAW.split(",") if k.strip()]
if GROQ_API_KEY and GROQ_API_KEY not in GROQ_API_KEYS:
    GROQ_API_KEYS.insert(0, GROQ_API_KEY)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
EXCEL_PATH = os.path.join(DATA_DIR, "tours.xlsx")
