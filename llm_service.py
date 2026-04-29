import asyncio
import httpx
import logging
import json
import re
import difflib
import random
import itertools
from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, GROQ_API_KEY, GROQ_API_KEYS
from excel_parser import get_hotel_db, get_tourist_tax_db, get_tax_per_person_per_night

logger = logging.getLogger(__name__)

# Round-robin generator for Groq keys
def _create_key_rotator():
    keys = GROQ_API_KEYS.copy()
    if not keys and GROQ_API_KEY:
        keys = [GROQ_API_KEY]
    random.shuffle(keys) # Initial shuffle
    return itertools.cycle(keys)

_groq_key_rotator = _create_key_rotator()

client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

_NOISE_TOKENS = {
    "hotel", "hotels", "apartments", "apartment", "resort", "spa",
    "villas", "villa", "the", "by", "and", "suites", "suite",
}

_DESTINATION_ALIASES = {
    "майорк": "майорка",
    "mallorca": "майорка",
    "палма": "майорка",
    "коста-брава": "коста-брава",
    "costa brava": "коста-брава",
    "коста-дель-соль": "коста-дель-соль",
    "тенериф": "тенеріфе",
    "tenerife": "тенеріфе",
    "гран-канар": "гран-карарія",
    "фуертевентур": "фуертевентура",
    "fuerteventura": "фуертевентура",
    "лансарот": "лансароте",
    "lanzarote": "лансароте",
    "ібіц": "ібіца",
    "ibiza": "ібіца",
    "крит": "крит",
    "crete": "крит",
    "корфу": "корфу",
    "corfu": "корфу",
    "родос": "родос",
    "rhodes": "родос",
    "кіпр": "кіпр",
    "cyprus": "кіпр",
}

_DESTINATION_PROMPT = """Ти — туристичний асистент. Тобі надіслали текст-чернетку від менеджера з описом туру.
Твоє завдання: визначити напрямок (країну/острів/регіон) з тексту і вибрати один найбільш підходящий варіант із наданого списку доступних напрямків.
Поверни ТІЛЬКИ назву напрямку зі списку. Якщо жоден не підходить, поверни "Unknown".
Без жодного іншого тексту.
"""

_EXTRACT_PROMPT = """Ти — спеціалізований AI-асистент для вилучення назв готелів. 
Твоє завдання: знайти у тексті менеджера ВСІ згадані готелі і зіставити їх з наданим списком з бази.

ПРАВИЛА:
1. Використовуй ТІЛЬКИ назви з наданого "СПИСКУ ГОТЕЛІВ НАПРЯМКУ". Якщо менеджер назвав готель, якого немає в списку — спробуй знайти максимально схожий за звучанням у цьому списку.
2. ЗАБОРОНЕНО вигадувати назви готелів, яких немає в базі, якщо є хоча б приблизний збіг.
3. ПОРЯДОК ТА КІЛЬКІСТЬ: Повертай готелі рівно в тому порядку, в якому вони йдуть у тексті. 
4. СТРОГИЙ ЛІМІТ: Якщо менеджер назвав 11 готелів — у списку має бути РІВНО 11 готелів. Не додавай зайвого.
5. ФОРМАТ: Тільки JSON {"hotels": ["Name 1", "Name 2"]}. Жодного іншого тексту.

КРИТИЧНО: Будь ласка, будь дуже уважним до назв. Ти МАЄШ вибрати готель із наданого списку, навіть якщо менеджер вимовив його з сильною помилкою.
"""

_EXTRACT_PRICES_PROMPT = """Ти — фінансовий аналітик туристичних турів. 
Твоє завдання: витягти числові дані для розрахунку.

ПРАВИЛА:
1. adults: кількість дорослих.
2. children: кількість дітей (віком від 2 до 12 років).
3. infants: кількість немовлят (до 2 років). Якщо вказано "дитина 2 роки", це зазвичай дитина (children), а не немовля (infant).
4. nights: кількість ночей.
5. check_in_month: номер місяця (1-12).
6. check_in_day: число місяця.
7. flight_per_person: ціна авіа НА ОДНУ особу. 
   - КРИТИЧНО: Якщо вказано "вартість авіа 410 євро", це майже ЗАВЖДИ ціна НА ОДНУ особу. 
   - Тільки якщо ПРЯМО вказано "за всіх" або "всього за авіа", тоді розділи на (дорослих + дітей). Не діли на немовлят.
   - Якщо ціна < 100 євро за авіа з Європи, це підозріло мало для ціни за особу, але все одно слідуй тексту.
8. hotel_prices: список ЗАГАЛЬНИХ цін за проживання для кожного готелю за весь період за ВСІХ (в тому ж порядку, що в тексті).
9. hotel_stars: зірковість кожного готелю (0, 3, 4, 5).
10. other_per_person: інші витрати на особу (трансфер, страхування тощо).

ФОРМАТ: Тільки JSON {"adults": 2, "children": 1, "infants": 0, "nights": 10, ...}.
КРИТИЧНО: Будь дуже уважним до вартості авіа. Не діли її на кількість людей, якщо менеджер не вказав, що це загальна сума.
"""

_VOICE_CLEANUP_PROMPT = """Ти отримуєш текст після автоматичного розпізнавання голосового повідомлення турагента.
Твоє завдання: виправити помилки розпізнавання, щоб текст став зрозумілим, АЛЕ ЗБЕРЕГТИ КОЖНЕ СЛОВО, що стосується готелів, цін та часу перельоту.

ПРАВИЛА:
- Виправляй лише явну "кашу" в словах (наприклад "Тенер і Фе" -> "Тенерифе", "Коста Бра во" -> "Коста-Брава").
- Зберігай назви готелів як є, навіть якщо вони звучать незвично.
- НЕ ВИДАЛЯЙ цифри, ціни та ЧАС перельоту (наприклад, "18:00 - 19:30" має залишитися повністю).
- НЕ ДОДАВАЙ нічого від себе.
- Поверни ТІЛЬКИ виправлений текст.
"""

_FORMAT_PROMPT = """Ти — професійний тревел-дизайнер. Твоє завдання: створити СТРОГО ОДНАКОВУ за структурою підбірку турів.

СТРУКТУРА ПОВІДОМЛЕННЯ (НЕ ЗМІНЮЙ):
Авіатур [на/до] [Напрямок] [Прапор]
Із [Місто] [Прапор]
🌤️ [Дати], [Ночі] ночей
Туди [Повний час або інтервал, наприклад 18:00 - 19:30]
Назад [Повний час або інтервал, наприклад 06:00 - 08:30]
🧳 ручна поклажа до 10 кг та розміром 20х40х30 см
[Додаткові послуги: трансфер/багаж/екскурсії — тільки якщо вказано]

🏠 варіанти проживання:

[Список готелів]
1) [Назва з бази] [Зірки 3★/4★/5★]
🥑 [харчування з малої літери]
[Посилання з бази]

... (всі готелі по черзі)

✔️ путівник + тур страхування
🤓 онлайн підтримка 24/7
[Рядок з цінами]

❗️Ціна актуальна на момент розрахунку подорожі

[Блок рекомендацій: ТІЛЬКИ 2-3 готелі, кожен з нового абзацу через порожній рядок]

КРИТИЧНІ ПРАВИЛА:
1. ПОРЯДОК: Готелі в списку "варіанти проживання" та ціни в рядку 💰 МАЮТЬ відповідати один одному за номером.
2. ЦІНИ: ЗАБОРОНЕНО вигадувати чи перераховувати ціни. Використовуй ТІЛЬКИ готові значення з блоку "РОЗРАХОВАНІ ЦІНИ". Якщо там написано 1)2200€, ти пишеш рівно 2200€. Жодної самодіяльності.
3. ЗІРКИ: Став зірки (3★, 4★, 5★) ЗАВЖДИ, якщо вони є в базі. Якщо в базі вказано (ЗІРКИ: 4★) — ти ОБОВ'ЯЗКОВО маєш написати 4★ поруч із назвою готелю.
4. ХАРЧУВАННЯ: Для кожного готелю обов'язково вказуй харчування після значка 🥑. 
   - Використовуй тільки: "сніданки", "сніданки + вечері", "повний пансіон", "все включено", "ультра все включено", "без харчування".
   - Якщо в блоці "ТИПИ ХАРЧУВАННЯ" вказано "напівпансіон" — ЗАМІНЮЙ на "сніданки + вечері".
5. МОВА: Весь текст УКРАЇНСЬКОЮ, назви готелів — АНГЛІЙСЬКОЮ (як у базі).
6. ОДНАКОВІСТЬ: Кожне твоє повідомлення має виглядати ідентично за структурою.

РЕКОМЕНДАЦІЇ (ВАЖЛИВО):
- Обирай 2-3 найкращих готелі. 
- Для кожного готелю пиши розгорнуту рекомендацію від першої особи (як експерт-турагент).
- ОБСЯГ: 400-600 символів на кожен готель.
- ЗМІСТ: Використовуй ТІЛЬКИ реальні факти (відгуки TripAdvisor/Booking, локація, тип пляжу — пісок/галька, захід у воду, рік реновації, якість харчування, територія).
- СТИЛЬ: Максимально людяний, чесний та "продаючий" опис. Не використовуй сухі факти, пиши емоційно, але достовірно.
- ЗАБОРОНА: У блоці рекомендацій ЗАБОРОНЕНО додавати будь-які посилання (на Booking, TripAdvisor тощо). Тільки текст.
- ПОРЯДОК: Кожна рекомендація — окремим абзацом, відділеним порожнім рядком.
"""

def calculate_tour_prices(hotel_prices: list, flight_per_person: float,
                          other_per_person: float, total_people: int,
                          has_children: bool, tourist_tax_per_person: float = 0) -> list:
    results = []
    for hotel_total in hotel_prices:
        hotel_per_person = hotel_total / total_people if total_people > 0 else hotel_total
        cost = hotel_per_person + flight_per_person + other_per_person + tourist_tax_per_person
        
        # Markup logic
        if cost < 350:
            final_per_person = cost + 150
        else:
            final_per_person = cost * 1.43
        
        # Round to nearest 5 for a cleaner look
        final_per_person = round(final_per_person / 5) * 5
        
        if has_children:
            results.append(round(final_per_person * total_people))
        else:
            results.append(round(final_per_person))
    return results

async def extract_prices_from_text(user_text: str, fast_models: list) -> dict:
    for model in fast_models:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _EXTRACT_PRICES_PROMPT},
                    {"role": "user", "content": user_text},
                ],
                temperature=0,
                timeout=20,
            )
            raw = resp.choices[0].message.content.strip()
            raw = re.sub(r'```[a-z]*\n?', '', raw).strip('`').strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                return json.loads(m.group())
        except Exception as e:
            logger.error(f"Price extraction error with {model}: {e}")
    return {}

async def cleanup_transcribed_text(raw_text: str) -> str:
    if not raw_text:
        return raw_text
    models = ["google/gemini-2.5-flash", "openai/gpt-4o-mini"]
    for model in models:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _VOICE_CLEANUP_PROMPT},
                    {"role": "user", "content": raw_text},
                ],
                temperature=0,
                timeout=30,
            )
            cleaned = resp.choices[0].message.content.strip()
            cleaned = re.sub(r'```[a-z]*\n?', '', cleaned).strip('`').strip()
            if cleaned:
                return cleaned
        except Exception as e:
            logger.error(f"Voice cleanup error with {model}: {e}")
    return raw_text

def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result

_MEAL_EXTRACT_PROMPT = """Ти — спеціаліст із туристичного харчування.
Твоє завдання: витягти тип харчування для КОЖНОГО готелю з тексту менеджера.

ПРАВИЛА:
1. Поверни список типів харчування у тому ж порядку, в якому готелі згадуються в тексті.
2. Використовуй ТІЛЬКИ ці назви:
   - "сніданки"
   - "сніданки + вечері" (якщо в тексті "напівпансіон", "HB", "напівпансін")
   - "повний пансіон" (якщо в тексті "FB", "триразове")
   - "все включено" (AI)
   - "ультра все включено" (UAI)
   - "без харчування" (RO)
3. КРИТИЧНО: Якщо в тексті "напівпансіон" або "напівпансін" — ЗАВЖДИ пиши "сніданки + вечері".
4. Якщо тип харчування не вказано для конкретного готелю, пиши "не вказано".
5. ФОРМАТ: Тільки JSON {"meals": ["тип 1", "тип 2"]}.
"""

async def _extract_meals(user_text: str, fast_models: list) -> list:
    """Extract meal types for each hotel if mentioned."""
    for model in fast_models:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _MEAL_EXTRACT_PROMPT},
                    {"role": "user", "content": user_text},
                ],
                temperature=0,
                timeout=15,
            )
            raw = resp.choices[0].message.content.strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                return json.loads(m.group()).get("meals", [])
        except Exception as e:
            logger.error(f"Meal extraction error with {model}: {e}")
    return []

def fuzzy_match_hotel(hotel_name: str, db: list) -> dict:
    def normalize_name(name: str) -> str:
        # Remove stars from name for better matching
        cleaned = re.sub(r'[3-5]\s*(?:\*|★)', '', name.lower())
        cleaned = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        tokens = [t for t in cleaned.split() if t not in _NOISE_TOKENS]
        return " ".join(tokens)

    best_match = None
    max_score = 0.0
    query = normalize_name(hotel_name)
    if not query:
        query = hotel_name.lower()
    
    for h in db:
        db_name_orig = h['hotel']
        db_name = normalize_name(db_name_orig)
        if not db_name:
            db_name = db_name_orig.lower()
            
        # 1. Exact match (after normalization)
        if query == db_name:
            return h

        # 2. SequenceMatcher score
        ratio = difflib.SequenceMatcher(None, query, db_name).ratio()
        
        # 3. Word overlap bonus (very important for voice)
        query_words = set(re.findall(r'\w+', query))
        db_words = set(re.findall(r'\w+', db_name))
        
        if not query_words: continue
        
        overlap = len(query_words & db_words)
        overlap_ratio = overlap / len(query_words)

        # Итоговый скор
        score = ratio * 0.4 + overlap_ratio * 0.6
        
        # Штраф за большую разницу в длине (защита от слишком коротких совпадений)
        len_diff = abs(len(query) - len(db_name))
        if len_diff > 15:
            score -= 0.1

        # Бонус за высокое совпадение слов
        if overlap_ratio >= 0.8:
            score += 0.5
            
        if score > max_score:
            max_score = score
            best_match = h
            
    if best_match and max_score > 0.65: # Порог срабатывания чуть выше
        return best_match
    return {"hotel": hotel_name, "link": "Посилання відсутнє ⚠️"}

def _build_hotel_candidates(user_text: str, relevant_hotels: list, limit: int = 140) -> list:
    if len(relevant_hotels) <= limit:
        return relevant_hotels
    text_norm = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', user_text.lower())
    text_norm = re.sub(r'\s+', ' ', text_norm).strip()
    text_words = set(re.findall(r'\w+', text_norm))
    scored = []
    for hotel in relevant_hotels:
        name = hotel.get("hotel", "")
        name_norm = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name.lower())
        name_norm = re.sub(r'\s+', ' ', name_norm).strip()
        hotel_words = set(re.findall(r'\w+', name_norm))
        overlap = len(hotel_words & text_words)
        ratio = difflib.SequenceMatcher(None, text_norm, name_norm).ratio()
        score = overlap * 2 + ratio
        scored.append((score, hotel))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [h for _, h in scored[:limit]]

def _extract_allowed_stars(hotel_name: str) -> str:
    """Extract stars from hotel name in DB. Support patterns like '5*', '5★', '5 *', or just ' 5 ' at the end."""
    # Pattern 1: Digit followed by * or ★ (e.g., 5*, 5 ★)
    m = re.search(r'([3-5])\s*(?:\*|★)', hotel_name)
    if m:
        return f"{m.group(1)}★"
    
    # Pattern 2: Just a digit at the very end or after a space (e.g., "Hotel Name 5")
    # We check for a digit 3-5 that is either at the end of the string or followed by a space
    m = re.search(r'\s([3-5])(?:\s|$)', hotel_name)
    if m:
        return f"{m.group(1)}★"
        
    return ""

def _inject_links(text: str, hotel_link_map: dict) -> str:
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        if '🥑' in lines[i] and i > 0:
            hotel_line = lines[i - 1].lower()
            line_words = set(re.findall(r'\w+', hotel_line))
            best_name, best_score = None, 0.0
            for h_name, link in hotel_link_map.items():
                h_words = set(re.findall(r'\w+', h_name.lower()))
                if not h_words: continue
                overlap = len(h_words & line_words) / len(h_words)
                if overlap > best_score:
                    best_score = overlap
                    best_name = h_name
            if best_name and best_score >= 0.4:
                link = hotel_link_map[best_name]
                next_i = i + 1
                if next_i < len(lines):
                    nxt = lines[next_i]
                    if 'http' in nxt or 'Посилання відсутнє' in nxt:
                        if 'http' in nxt and link and 'http' in link and link.split()[0] not in nxt:
                            lines[next_i] = link
                        i += 1
                        continue
                lines.insert(i + 1, link)
                i += 1
        i += 1
    return '\n'.join(lines)

def _count_listed_hotels(text: str) -> int:
    count = 0
    for line in text.split("\n"):
        if re.match(r'^\s*\d+\)\s+', line):
            count += 1
    return count

def _build_price_line(price_label: str, computed_prices: list) -> str:
    if not computed_prices:
        return "💰 не вказано"
    prices_str = ", ".join([f"{i+1}){p}€" for i, p in enumerate(computed_prices)])
    return f"{price_label} - {prices_str}"

def _inject_prices(text: str, price_label: str, computed_prices: list) -> str:
    enforced_price_line = _build_price_line(price_label, computed_prices)
    lines = text.split("\n")
    
    # 1. First, remove ANY line that starts with 💰 to prevent duplicates or old/wrong prices
    lines = [line for line in lines if not line.strip().startswith("💰")]
    
    # 2. Find the anchor (online support) to insert the correct price line
    anchor_idx = None
    for i, line in enumerate(lines):
        if "онлайн підтримка" in line.lower() or "путівник" in line.lower():
            anchor_idx = i
            break
            
    if anchor_idx is not None:
        # Insert after the anchor
        lines.insert(anchor_idx + 1, enforced_price_line)
    else:
        # Fallback to the end
        lines.append(enforced_price_line)
        
    return "\n".join(lines)

def _append_missing_hotels(text: str, matched_hotels: list, hotel_prices: list) -> str:
    listed = _count_listed_hotels(text)
    total = len(matched_hotels)
    if listed >= total:
        return text
    lines = text.split("\n")
    insert_idx = len(lines)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("✔️") or stripped.startswith("💰"):
            insert_idx = i
            break
    missing_block = []
    for idx in range(listed, total):
        h = matched_hotels[idx]
        stars_str = _extract_allowed_stars(h['hotel'])
        header = f"{idx + 1}) {h['hotel']}{(' ' + stars_str) if stars_str else ''}"
        meal = "🥑 не вказано"
        link_line = h['link']
        missing_block += ["", header, meal, link_line]
    for i, block_line in enumerate(missing_block):
        lines.insert(insert_idx + i, block_line)
    return "\n".join(lines)

def _pick_destination_by_keywords(user_text: str, destinations: list) -> str | None:
    text = user_text.lower()
    for key, normalized_dest in _DESTINATION_ALIASES.items():
        if key in text:
            for d in destinations:
                if normalized_dest in d.lower():
                    return d
    return None

async def format_tour_message(user_text: str, do_cleanup: bool = False) -> str:
    db = get_hotel_db()
    destinations = list(db.keys())
    
    # 0. Cleanup in parallel with initial detection if requested
    cleanup_task = None
    if do_cleanup:
        cleanup_task = asyncio.create_task(cleanup_transcribed_text(user_text))

    selected_dest = _pick_destination_by_keywords(user_text, destinations)
    
    fast_models = ["google/gemini-2.5-flash", "openai/gpt-4o-mini"]
    smart_models = ["google/gemini-2.5-flash", "openai/gpt-4o-mini"]
    
    start_time = asyncio.get_event_loop().time()

    async def _detect_destination(text):
        if selected_dest: return selected_dest
        dest_content = f"ТЕКСТ:\n{text}\n\nДОСТУПНІ НАПРЯМКИ:\n{', '.join(destinations)}"
        for mod in fast_models:
            try:
                resp = await client.chat.completions.create(
                    model=mod, messages=[{"role": "system", "content": _DESTINATION_PROMPT}, {"role": "user", "content": dest_content}],
                    temperature=0.0, timeout=15
                )
                model_name = re.sub(r'[^\w\s-]', '', resp.choices[0].message.content.strip().lower()).strip()
                for d in destinations:
                    if d.lower() in model_name or model_name in d.lower(): return d
            except Exception as e: logger.error(f"Dest model {mod} failed: {e}")
        return destinations[0] if destinations else "Unknown"

    price_task = asyncio.create_task(extract_prices_from_text(user_text, fast_models))
    dest_task = asyncio.create_task(_detect_destination(user_text))
    meal_task = asyncio.create_task(_extract_meals(user_text, fast_models))
    
    async def _extract_hotels_broadly(text):
        for model in fast_models:
            try:
                resp = await client.chat.completions.create(
                    model=model, messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": f"ТЕКСТ МЕНЕДЖЕРА:\n{text}\n\nЗнайди всі готелі."}],
                    temperature=0.0, timeout=20
                )
                raw = resp.choices[0].message.content.strip()
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m: return json.loads(m.group()).get("hotels", [])
            except Exception: pass
        return []

    broad_hotel_task = asyncio.create_task(_extract_hotels_broadly(user_text))
    
    # Wait for first-round tasks
    if cleanup_task:
        user_text = await cleanup_task
        
    selected_dest = await dest_task
    price_data = await price_task
    # broad_hotels = await broad_hotel_task # Skip broad task to rely more on targeted extraction
    extracted_meals = await meal_task
    
    logger.info(f"Step 1 parallel done in {asyncio.get_event_loop().time() - start_time:.2f}s. Dest: {selected_dest}")

    relevant_hotels = db.get(selected_dest, [])
    # Disable pre-filtering candidates to avoid missing hotels due to poor text matching
    candidate_hotels = relevant_hotels
    
    async def _do_targeted_extract():
        # Show ONLY first 300 hotels to avoid context overflow but keep it broad
        db_names = "\n".join([h['hotel'] for h in candidate_hotels[:350]])
        extraction_content = f"ТЕКСТ ВІД МЕНЕДЖЕРА:\n{user_text}\n\nОБРАНИЙ НАПРЯМОК: {selected_dest}\n\nСПИСОК ГОТЕЛІВ НАПРЯМКУ (база):\n{db_names}"
        for model in ["google/gemini-2.5-flash"]: # Use only the most capable model for this
            try:
                resp = await client.chat.completions.create(
                    model=model, messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": extraction_content}],
                    temperature=0.0, timeout=30
                )
                raw = resp.choices[0].message.content.strip()
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m: return json.loads(m.group()).get("hotels", [])
            except Exception as e:
                logger.error(f"Targeted extract error: {e}")
        return []

    extracted_hotels = await _do_targeted_extract()
    
    # HARD LIMIT: If price_data tells us exactly how many hotels there should be, use it.
    if price_data and price_data.get("hotel_prices"):
        expected_count = len(price_data["hotel_prices"])
        if len(extracted_hotels) > expected_count:
            logger.info(f"Trimming hotels from {len(extracted_hotels)} to {expected_count} based on prices.")
            extracted_hotels = extracted_hotels[:expected_count]

    matched_info = []
    hotel_link_map = {}
    all_hotels_list = [hotel for hotels in db.values() for hotel in hotels]
    matched_hotels = []
    seen_hotels = set()
    for h_name in extracted_hotels:
        match = fuzzy_match_hotel(h_name, candidate_hotels)
        if "Посилання відсутнє" in match["link"]:
            match = fuzzy_match_hotel(h_name, relevant_hotels)
        if "Посилання відсутнє" in match["link"] and all_hotels_list:
            global_match = fuzzy_match_hotel(h_name, all_hotels_list)
            if "Посилання відсутнє" not in global_match["link"]: match = global_match
        
        key = match["hotel"].strip().lower()
        if key in seen_hotels: continue
        seen_hotels.add(key)
        matched_hotels.append(match)
        stars = _extract_allowed_stars(match["hotel"])
        matched_info.append(f"- Назва: {match['hotel']}, Зірки: {stars or 'не вказувати'}, Посилання: {match['link']}")
        hotel_link_map[match['hotel'].lower()] = match['link']
        
    db_text = "\n".join(matched_info) if matched_info else "Не вдалося витягнути готелі."
    
    price_label = "💰 загальна вартість туру за особу"
    computed_prices = []
    if price_data and price_data.get("hotel_prices") and price_data.get("flight_per_person") is not None:
        adults = int(price_data.get("adults") or 2)
        children = int(price_data.get("children") or 0)
        infants = int(price_data.get("infants") or 0)
        total_people = adults + children
        has_children = (children + infants) > 0
        
        flight = float(price_data.get("flight_per_person") or 0)
        other = float(price_data.get("other_per_person") or 0)
        hotel_prices = [float(p) for p in price_data.get("hotel_prices") or []]
        nights = int(price_data.get("nights") or 7)
        month = int(price_data.get("check_in_month") or 6)
        hotel_stars_list = price_data.get("hotel_stars") or []

        max_items = len(matched_hotels) if matched_hotels else len(hotel_prices)
        hotel_prices = hotel_prices[:max_items]
        
        for idx, hotel_total in enumerate(hotel_prices):
            db_stars_str = _extract_allowed_stars(matched_hotels[idx]['hotel']) if idx < len(matched_hotels) else ""
            if db_stars_str:
                m_stars = re.search(r'\d', db_stars_str)
                stars_val = int(m_stars.group()) if m_stars else 0
            else:
                stars_val = int(hotel_stars_list[idx]) if idx < len(hotel_stars_list) else 0
            
            tax_per_night = get_tax_per_person_per_night(selected_dest or "", stars_val, month, total_people)
            tax = tax_per_night * nights
            
            # MATH LOGIC:
            # 1. Base cost per person (infants are usually not counted in hotel price division)
            hotel_per_person = hotel_total / total_people if total_people > 0 else hotel_total
            base_cost = hotel_per_person + flight + other + tax
            
            # 2. Markup (Margin)
            if base_cost < 350:
                final = base_cost + 150
            else:
                final = base_cost * 1.43
                
            # Round to nearest 5
            final = round(final / 5) * 5
            
            logger.info(f"CALC: Hotel={matched_hotels[idx]['hotel'] if idx < len(matched_hotels) else '?'}, PriceIn={hotel_total}, Flight={flight}, Tax={tax}, Base={base_cost}, Final={final}")
            
            # For children/infants, we usually show total price for everyone
            if has_children:
                total_tour_price = round(final * total_people)
                # Infants might have a small fixed price (e.g. 50-100e for flight/insurance), 
                # but we don't have this data explicitly, so we just use total_people.
                computed_prices.append(total_tour_price)
            else:
                computed_prices.append(final)
        
        price_label = "💰 загальна вартість туру" if has_children else "💰 загальна вартість туру за особу"
        prices_block = f"\n\nРОЗРАХОВАНІ ЦІНИ:\n{price_label} - {', '.join([f'{i+1}){p}€' for i, p in enumerate(computed_prices)])}"
    else:
        prices_block = "\n\nЦІНА НЕ ВКАЗАНА: у блоці 💰 напиши 'не вказано' для всіх готелів."

    # Prepare meals list (align with matched_hotels)
    # Even if extracted_meals is empty, we provide a default list to ensure LLM doesn't hallucinate
    meals_list = []
    for i in range(len(matched_hotels)):
        if extracted_meals and i < len(extracted_meals):
            meals_list.append(extracted_meals[i])
        else:
            meals_list.append("не вказано")
    
    meals_str = ", ".join([f"{i+1}) {m}" for i, m in enumerate(meals_list)])
    meals_block = f"\n\nТИПИ ХАРЧУВАННЯ (використовуй ці дані для кожного готелю СУВОРО за номером):\n{meals_str}"

    # Construct numbered hotels block with EXPLICIT star mention to help the LLM
    hotels_with_stars = []
    for i, h in enumerate(matched_hotels):
        stars = _extract_allowed_stars(h['hotel'])
        hotels_with_stars.append(f"{i+1}. {h['hotel']} (ЗІРКИ: {stars if stars else 'немає'}) | {h['link']}")
    
    numbered_hotels_block = "\n".join(hotels_with_stars)
    combined = f"ТЕКСТ ВІД МЕНЕДЖЕРА:\n{user_text}\n\nЗНАЙДЕНІ В БАЗІ ГОТЕЛІ (ВСЬОГО {len(matched_hotels)}, ВИКОРИСТОВУЙ ВСІ):\n{numbered_hotels_block}\n\nДЕТАЛЬНА ІНФОРМАЦІЯ:\n{db_text}{prices_block}{meals_block}"
    
    for model in smart_models:
        try:
            resp = await client.chat.completions.create(
                model=model, messages=[{"role": "system", "content": _FORMAT_PROMPT}, {"role": "user", "content": combined}],
                temperature=0, timeout=120
            )
            result = resp.choices[0].message.content.strip()
            result = re.sub(r'<math>.*?</math>', '', result, flags=re.DOTALL).strip()
            result = _inject_links(result, hotel_link_map)
            result = _append_missing_hotels(result, matched_hotels, computed_prices)
            result = _inject_prices(result, price_label, computed_prices)
            return result
        except Exception as e:
            logger.error(f"Format error with {model}: {e}", exc_info=True)
            if model == smart_models[-1]: # Only return error if LAST model failed
                return "❌ Помилка генерації тексту."

async def transcribe_voice(file_bytes: bytes) -> str:
    # 1. TRY GROQ with smart rotation
    active_keys = GROQ_API_KEYS if GROQ_API_KEYS else ([GROQ_API_KEY] if GROQ_API_KEY else [])
    if not active_keys:
        logger.warning("No Groq API keys available.")
    else:
        # Try all keys starting from the next one in the cycle
        for _ in range(len(active_keys)):
            key = next(_groq_key_rotator)
            url_groq = "https://api.groq.com/openai/v1/audio/transcriptions"
            headers_groq = {"Authorization": f"Bearer {key}"}
            whisper_prompt = "Турагент диктує підбірку готелів: Тенерифе, Майорка, Коста-Брава, Халкідікі, готель, зірки, харчування, сніданки, вечері, все включено, ціна в євро, виліт з міста."
            
            files = {"file": ("voice.ogg", file_bytes, "audio/ogg")}
            data = {
                "model": "whisper-large-v3",
                "prompt": whisper_prompt,
                "language": "uk"
            }
            
            async with httpx.AsyncClient() as c:
                try:
                    resp = await c.post(url_groq, headers=headers_groq, files=files, data=data, timeout=20)
                    if resp.status_code == 200:
                        text = resp.json().get("text", "")
                        if text: return text
                    
                    # If status is not 200, log and try next key
                    logger.warning(f"Groq key {key[:10]}... returned status {resp.status_code}. Trying next key.")
                except Exception as e:
                    logger.warning(f"Groq key {key[:10]}... failed: {e}. Trying next key.")
                    continue

    # 2. FALLBACK TO OPENROUTER (Paid/Stable)
    if OPENROUTER_API_KEY:
        try:
            # Using openai-style call via OpenRouter for whisper
            # OpenRouter uses different endpoint for audio, but we can use their standard completions with a whisper model if available
            # or use the direct audio/transcriptions if they support it.
            # Most reliable via OpenRouter is using their chat interface for transcription if they have a whisper provider.
            # Alternatively, use their openai-compatible audio endpoint if supported.
            url_or = "https://openrouter.ai/api/v1/audio/transcriptions"
            headers_or = {"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
            
            # Reset file pointer if needed (not needed for bytes)
            files = {"file": ("voice.ogg", file_bytes, "audio/ogg")}
            data_or = {"model": "openai/whisper-large-v3"} # Paid model on OpenRouter
            
            async with httpx.AsyncClient() as c:
                resp = await c.post(url_or, headers=headers_or, files=files, data=data_or, timeout=30)
                if resp.status_code == 200:
                    return resp.json().get("text", "")
        except Exception as e:
            logger.error(f"OpenRouter Whisper fallback failed: {e}")

    return "❌ Помилка розпізнавання (обидва сервіси недоступні)."
