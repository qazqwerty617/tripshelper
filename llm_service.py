import asyncio
import httpx
import logging
import json
import re
import difflib
from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, GROQ_API_KEY
from excel_parser import get_hotel_db, get_tourist_tax_db, get_tax_per_person_per_night

logger = logging.getLogger(__name__)

client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

_NOISE_TOKENS = {
    "hotel", "hotels", "apartments", "apartment", "resort", "spa",
    "villas", "villa", "the", "by", "and",
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
1. Використовуй ТІЛЬКИ назви з наданого "СПИСКУ ГОТЕЛІВ НАПРЯМКУ", якщо є хоча б мінімальна схожість (навіть якщо менеджер помилився у буквах або мові).
2. Якщо в базі немає схожого готелю, поверни назву, яку вжив менеджер, переклавши її на англійську.
3. ПОРЯДОК МАЄ ЗНАЧЕННЯ: Повертай готелі рівно в тому порядку, в якому вони йдуть у тексті менеджера.
4. КІЛЬКІСТЬ МАЄ ЗНАЧЕННЯ: Якщо менеджер згадав 10 готелів, у JSON має бути рівно 10 назв.
5. ФОРМАТ: Тільки JSON {"hotels": ["Name 1", "Name 2"]}. Жодного іншого тексту.

КРИТИЧНО: Менеджер може диктувати назви дуже неточно (наприклад, "Залив Ада" замість "Cala Azul"). Будь розумним, шукай за співзвучністю.
"""

_EXTRACT_PRICES_PROMPT = """Ти — фінансовий аналітик туристичних турів. 
Твоє завдання: витягти числові дані для розрахунку.

ПРАВИЛА:
1. adults: кількість дорослих.
2. children: кількість дітей.
3. nights: кількість ночей.
4. check_in_month: номер місяця (1-12).
5. check_in_day: число місяця.
6. flight_per_person: ціна авіа НА ОДНУ особу. Якщо вказано загальну суму — розділи її на всіх людей.
7. hotel_prices: список ЗАГАЛЬНИХ цін за номер для кожного готелю (в тому ж порядку, що в тексті).
8. hotel_stars: зірковість кожного готелю (0, 3, 4, 5).
9. other_per_person: інші витрати на особу.

ФОРМАТ: Тільки JSON.
КРИТИЧНО: Якщо вказано "Ціни: 500, 600, 700" — це ціни трьох готелів. Витягни їх всі.
"""

_VOICE_CLEANUP_PROMPT = """Ти отримуєш текст після автоматичного розпізнавання голосового повідомлення турагента.
Твоє завдання: акуратно виправити явні помилки розпізнавання, зберігши зміст.

ПРАВИЛА:
- Виправляй лише очевидні помилки (особливо в назвах курортів, готелів, харчування).
- Зберігай усі числа, суми, дати, часи, кількість ночей, дорослих/дітей без змін.
- Не додавай нових фактів від себе.
- Не скорочуй і не перефразовуй сильно — лише clean-up тексту.
- Поверни ТІЛЬКИ виправлений текст, без пояснень і без лапок.
"""

_FORMAT_PROMPT = """Ти — професійний тревел-дизайнер. Твоє завдання: створити СТРОГО ОДНАКОВУ за структурою підбірку турів.

СТРУКТУРА ПОВІДОМЛЕННЯ (НЕ ЗМІНЮЙ):
Авіатур [на/до] [Напрямок] [Прапор]
Із [Місто] [Прапор]
🌤️ [Дати], [Ночі] ночей
Туди [Час]
Назад [Час]
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
2. ЦІНИ: Використовуй ТІЛЬКИ готові ціни з блоку "РОЗРАХОВАНІ ЦІНИ". Формат: 💰 загальна вартість туру за особу - 1)400€, 2)450€...
3. ЗІРКИ: Став зірки (3★, 4★, 5★) ТІЛЬКИ якщо вони є в базі. Якщо в базі 1-2 зірки або порожньо — НЕ ПИШИ НІЧОГО.
4. МОВА: Весь текст УКРАЇНСЬКОЮ, назви готелів — АНГЛІЙСЬКОЮ (як у базі).
5. ОДНАКОВІСТЬ: Кожне твоє повідомлення має виглядати ідентично за структурою, незалежно від входу. Жодної самодіяльності.
"""

def calculate_tour_prices(hotel_prices: list, flight_per_person: float,
                          other_per_person: float, total_people: int,
                          has_children: bool, tourist_tax_per_person: float = 0) -> list:
    results = []
    for hotel_total in hotel_prices:
        hotel_per_person = hotel_total / total_people
        cost = hotel_per_person + flight_per_person + other_per_person + tourist_tax_per_person
        if cost < 350:
            final_per_person = cost + 150
        else:
            final_per_person = cost * 1.43
        final_per_person = round(final_per_person) + 5
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

async def _extract_meals(user_text: str, fast_models: list) -> list:
    """Extract meal types for each hotel if mentioned."""
    for model in fast_models:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "Витягни типи харчування для готелів з тексту. Поверни JSON: {\"meals\": [\"все включено\", \"сніданки\"]}. Якщо не вказано, поверни порожній список."},
                    {"role": "user", "content": user_text},
                ],
                temperature=0,
                timeout=15,
            )
            raw = resp.choices[0].message.content.strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                return json.loads(m.group()).get("meals", [])
        except Exception:
            pass
    return []

def fuzzy_match_hotel(hotel_name: str, db: list) -> dict:
    def normalize_name(name: str) -> str:
        cleaned = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name.lower())
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        tokens = [t for t in cleaned.split() if t not in _NOISE_TOKENS and len(t) > 1]
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
        score = difflib.SequenceMatcher(None, query, db_name).ratio()
        
        # 3. Word overlap bonus
        query_words = set(re.findall(r'\w+', query))
        db_words = set(re.findall(r'\w+', db_name))
        
        if query_words and query_words.issubset(db_words):
            score += 0.45  # Big bonus if all query words are in DB name
            
        overlap = len(query_words & db_words)
        if overlap >= 2:
            score += 0.2
        elif overlap == 1:
            score += 0.1
            
        if score > max_score:
            max_score = score
            best_match = h
            
    if best_match and max_score >= 0.45:
        if max_score < 0.8:
            return {"hotel": best_match["hotel"], "link": best_match["link"] + " ⚠️"}
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
    m = re.search(r'(?<!\d)([1-5])\s*(?:\*|★)?\s*$', hotel_name.strip())
    if not m:
        return ""
    stars = int(m.group(1))
    return f"{stars}★" if stars in (3, 4, 5) else ""

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
    price_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("💰"):
            price_idx = i
            break
    if price_idx is not None:
        lines[price_idx] = enforced_price_line
        return "\n".join(lines)
    anchor_idx = None
    for i, line in enumerate(lines):
        if "онлайн підтримка" in line.lower():
            anchor_idx = i
            break
    if anchor_idx is not None:
        lines.insert(anchor_idx + 1, enforced_price_line)
    else:
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
    broad_hotels = await broad_hotel_task
    extracted_meals = await meal_task
    
    logger.info(f"Step 1 parallel done in {asyncio.get_event_loop().time() - start_time:.2f}s. Dest: {selected_dest}")

    relevant_hotels = db.get(selected_dest, [])
    candidate_hotels = _build_hotel_candidates(user_text, relevant_hotels, limit=140)
    
    async def _do_targeted_extract():
        db_names = "\n".join([h['hotel'] for h in candidate_hotels])
        extraction_content = f"ТЕКСТ ВІД МЕНЕДЖЕРА:\n{user_text}\n\nОБРАНИЙ НАПРЯМОК: {selected_dest}\n\nСПИСОК ГОТЕЛІВ НАПРЯМКУ (база):\n{db_names}"
        for model in fast_models:
            try:
                resp = await client.chat.completions.create(
                    model=model, messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": extraction_content}],
                    temperature=0.0, timeout=25
                )
                raw = resp.choices[0].message.content.strip()
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m: return json.loads(m.group()).get("hotels", [])
            except Exception: pass
        return []

    targeted_hotels = await _do_targeted_extract()
    extracted_hotels = _dedupe_keep_order(broad_hotels + targeted_hotels)
    
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
        total_people = int(price_data.get("adults", 2)) + int(price_data.get("children", 0))
        has_children = int(price_data.get("children", 0)) > 0
        flight = float(price_data.get("flight_per_person", 0))
        other = float(price_data.get("other_per_person", 0))
        hotel_prices = [float(p) for p in price_data.get("hotel_prices", [])]
        nights = int(price_data.get("nights", 7))
        month = int(price_data.get("check_in_month", 6))
        hotel_stars_list = price_data.get("hotel_stars", [])

        max_items = len(matched_hotels) if matched_hotels else len(hotel_prices)
        hotel_prices = hotel_prices[:max_items]
        
        for idx, hotel_total in enumerate(hotel_prices):
            stars = int(hotel_stars_list[idx]) if idx < len(hotel_stars_list) else 0
            tax = get_tax_per_person_per_night(selected_dest or "", stars, month, total_people) * nights
            cost = (hotel_total / total_people) + flight + other + tax
            final = round(cost + 150) + 5 if cost < 350 else round(cost * 1.43) + 5
            computed_prices.append(round(final * total_people) if has_children else final)
        
        price_label = "💰 загальна вартість туру" if has_children else "💰 загальна вартість туру за особу"
        prices_block = f"\n\nРОЗРАХОВАНІ ЦІНИ:\n{price_label} - {', '.join([f'{i+1}){p}€' for i, p in enumerate(computed_prices)])}"
    else:
        prices_block = "\n\nЦІНА НЕ ВКАЗАНА: у блоці 💰 напиши 'не вказано' для всіх готелів."

    # Prepare meals list (align with matched_hotels)
    meals_block = ""
    if extracted_meals:
        # Ensure meals list matches hotels count for clarity
        meals_str = ", ".join([f"{i+1}) {extracted_meals[i] if i < len(extracted_meals) else 'не вказано'}" for i in range(len(matched_hotels))])
        meals_block = f"\n\nТИПИ ХАРЧУВАННЯ (використовуй ці дані для кожного готелю СУВОРО за номером):\n{meals_str}"

    numbered_hotels_block = "\n".join([f"{i+1}. {h['hotel']} {_extract_allowed_stars(h['hotel'])} | {h['link']}" for i, h in enumerate(matched_hotels)])
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
        except Exception as e: logger.error(f"Format error with {model}: {e}")
    return "❌ Помилка генерації тексту."

async def transcribe_voice(file_bytes: bytes) -> str:
    if not GROQ_API_KEY: return "❌ Немає GROQ_API_KEY."
    url = "https://api.groq.com/openai/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
    files = {"file": ("voice.ogg", file_bytes, "audio/ogg")}
    async with httpx.AsyncClient() as c:
        try:
            resp = await c.post(url, headers=headers, files=files, data={"model": "whisper-large-v3"}, timeout=30)
            if resp.status_code == 200: return resp.json().get("text", "")
            return "❌ Помилка розпізнавання."
        except Exception: return "❌ Мережева помилка."
