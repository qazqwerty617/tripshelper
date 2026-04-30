import asyncio
import logging
import json
import re
import difflib
from openai import AsyncOpenAI
from config import OPENROUTER_API_KEY, GROQ_API_KEY, GROQ_API_KEYS
from excel_parser import get_hotel_db, get_tourist_tax_db, get_tax_per_person_per_night
from voice_handler import cleanup_transcribed_text

logger = logging.getLogger(__name__)

def _safe_int(val, default=0):
    try:
        if val is None: return default
        s = re.sub(r'[^\d]', '', str(val))
        return int(s) if s else default
    except: return default

client = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

_NOISE_TOKENS = {
    "hotel", "hotels", "apartments", "apartment", "apartamentos", "apartamento",
    "resort", "spa", "villas", "villa", "the", "by", "and", "suites", "suite",
    "hostal", "pension", "boutique", "park", "garden", "beach", "club",
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
    "мальор": "майорка",
    "мальорк": "майорка",
}

_DESTINATION_PROMPT = """Ти — туристичний асистент. Тобі надіслали текст-чернетку від менеджера з описом туру.
Твоє завдання: визначити напрямок (країну/острів/регіон) з тексту і вибрати один найбільш підходящий варіант із наданого списку доступних напрямків.
Поверни ТІЛЬКИ назву напрямку зі списку. Якщо жоден не підходить, поверни "Unknown".
Без жодного іншого тексту.
"""

_EXTRACT_PROMPT = """Ти — спеціалізований AI-асистент для вилучення назв готелів. 
Твоє завдання: знайти у тексті менеджера ВСІ згадані готелі і зіставити їх з наданим списком з бази.

ПРАВИЛА:
1. Використовуй ТІЛЬКИ назви з наданого "СПИСКУ ГОТЕЛІВ НАПРЯМКУ".
2. НЕ ЗАМІНЮЙ ГОТЕЛІ на інші. Якщо менеджер написав "Hotel A", а в списку є "Hotel B" — не замінюй їх, якщо вони не є очевидно одним і тим самим готелем (наприклад, різна зірковість або мережа).
3. Якщо менеджер назвав готель, якого немає в списку — поверни ТУ САМУ ідентичну назву, яку надав менеджер. НЕ ПРИДУМУЙ схожі назви з бази, якщо немає 100% впевненості.
4. ПОРЯДОК ТА КІЛЬКІСТЬ: Повертай готелі рівно в тому порядку, в якому вони йдуть у тексті. 
5. СТРОГИЙ ЛІМІТ: Якщо менеджер назвав 11 готелів — у списку має бути РІВНО 11 готелів.
6. ФОРМАТ: Тільки JSON {"hotels": ["Name 1", "Name 2"]}. Жодного іншого тексту.

КРИТИЧНО: Краще повернути оригінальну назву від менеджера, ніж помилково вибрати неправильний готель з бази.
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
   - СУВОРО: Ціна авіа ЗАВЖДИ вважається за ОДНУ особу, якщо тільки менеджер ПРЯМО і ЧІТКО не написав "за всіх" або "загальна сума". 
   - Якщо написано просто "авіа 410" або "вартість авіа 410" — це ціна за ОДНУ особу. НЕ ДІЛИ її на кількість людей.
   - Тільки якщо вказано "авіа 1200 за трьох", тоді розділи (1200 / 3 = 400).
   - Якщо ціна підозріло мала (наприклад, 40 євро), все одно вважати це ціною за особу.
8. hotel_prices: список ЗАГАЛЬНИХ цін за проживання для кожного готелю за весь період за ВСІХ (в тому ж порядку, що в тексті).
9. hotel_stars: зірковість кожного готелю (0, 3, 4, 5).
10. other_per_person: інші витрати на особу (трансфер, страхування тощо).

ФОРМАТ: Тільки JSON {"adults": 2, "children": 1, "infants": 0, "nights": 10, ...}.
КРИТИЧНО: Будь дуже уважним до вартості авіа. Не діли її на кількість людей, якщо менеджер не вказав, що це загальна сума.
"""

_FORMAT_PROMPT = """Ти — професійний тревел-дизайнер та експерт із продажів. Твоє завдання: створити СТРОГО ОДНАКОВУ за структурою підбірку турів.

СТРУКТУРА ПОВІДОМЛЕННЯ (НЕ ЗМІНЮЙ):
Авіатур [на/до] [Напрямок] [Прапор]
Із [Місто] [Прапор]
🌤️ [Дати], [Ночі] ночей
Туди [Час]
Назад [Час]
🧳 ручна поклажа до 10 кг та розміром 20х40х30 см
[Додаткові послуги: трансфер/багаж/екскурсії — тільки якщо вказано]

🏠 варіанти проживання:

1) [Назва з бази] [Зірки 3★/4★/5★]
🥑 [харчування з малої літери]
[Посилання з бази]

2) [Назва з бази] [Зірки 3★/4★/5★]
🥑 [харчування з малої літери]
[Посилання з бази]

... (всі готелі по черзі. Між готелями ОДИН порожній рядок)

✔️ путівник + тур страхування
🤓 онлайн підтримка 24/7
💰 [Рядок з цінами]

❗️Ціна актуальна на момент розрахунку подорожі

[Блок рекомендацій]

КРИТИЧНІ ПРАВИЛА:
1. ЖОДНИХ ГАЛЮЦИНАЦІЙ: Використовуй ТІЛЬКИ ті готелі, які надані в блоці "ЗНАЙДЕНІ В БАЗІ ГОТЕЛІ". НЕ вигадуй назви та НЕ використовуй готелі, яких немає у списку "ДЕТАЛЬНА ІНФОРМАЦІЯ".
2. ПОРЯДОК: У списку "варіанти проживання" мають бути ТІЛЬКИ 3 рядки на готель (Назва, Харчування, Посилання). НЕ ДОДАВАЙ описи готелів у цей список.
3. ВІДСТУПИ: ОДИН порожній рядок між блоками. НЕ став по 2-3 порожніх рядки.
4. ЗІРКИ: Став зірки (3★, 4★, 5★) тільки ОДИН РАЗ поруч із назвою готелю.
5. ПОСИЛАННЯ: Пиши ТІЛЬКИ чисте посилання. НЕ додавай слова "Посилання", "Link" або дужки.

БЛОК РЕКОМЕНДАЦІЙ (ОБОВ'ЯЗКОВО):
- Оберіть 2-3 найкращих готелі з наданого списку.
- Для кожного обраного готелю напишіть переконливий опис (400-600 символів).
- Пишіть емоційно, від першої особи, підкреслюючи переваги саме для цього туру.
- Формат:
**[Назва готелю] [Зірки]**
[Ваш текст опису]
(порожній рядок між рекомендаціями)
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
    raw = await _call_llm_with_retry(
        messages=[
            {"role": "system", "content": _EXTRACT_PRICES_PROMPT},
            {"role": "user", "content": user_text},
        ],
        models=fast_models,
        timeout=20
    )
    if raw:
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except: pass
    return {}

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
2. Якщо менеджер вказав тип харчування один раз для всіх готелів (наприклад, "харчування сніданки" на початку), то поверни "сніданки" для КОЖНОГО готелю у списку.
3. Використовуй ТІЛЬКИ ці назви:
   - "сніданки"
   - "сніданки + вечері"
   - "повний пансіон"
   - "все включено"
   - "ультра все включено"
   - "без харчування"
4. ФОРМАТ: Тільки JSON {"meals": ["тип 1", "тип 2"]}.
"""

async def _call_llm_with_retry(messages, models, temperature=0, timeout=30, max_tokens=None):
    """Calls LLM with fallback models and retry logic."""
    for model in models:
        for attempt in range(2): # Try each model up to 2 times
            try:
                params = {
                    "model": model,
                    "messages": messages,
                    "temperature": temperature,
                    "timeout": timeout,
                }
                if max_tokens:
                    params["max_tokens"] = max_tokens
                    
                resp = await client.chat.completions.create(**params)
                content = resp.choices[0].message.content.strip()
                # Basic cleanup of markdown
                content = re.sub(r'```[a-z]*\n?', '', content).strip('`').strip()
                if content:
                    return content
            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "rate limit" in err_str:
                    logger.warning(f"Rate limited on {model}, attempt {attempt+1}. Switching/Retrying...")
                    if attempt == 0:
                        await asyncio.sleep(1) # Small pause before retry
                        continue
                logger.warning(f"LLM call failed for {model}: {e}")
                break # Try next model
    return None

async def _extract_meals(user_text: str, fast_models: list) -> list:
    """Extract meal types for each hotel if mentioned."""
    raw = await _call_llm_with_retry(
        messages=[
            {"role": "system", "content": _MEAL_EXTRACT_PROMPT},
            {"role": "user", "content": user_text},
        ],
        models=fast_models,
        timeout=15
    )
    if raw:
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group()).get("meals", [])
            except: pass
    return []

def fuzzy_match_hotel(hotel_name: str, db: list) -> tuple[dict, float]:
    def normalize_name(name: str) -> str:
        # Remove stars from name for better matching
        cleaned = re.sub(r'[3-5]\s*(?:\*|★)', '', name.lower())
        
        # Simple Transliteration for Ukrainian/Russian names to Latin
        trans_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
            'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
            'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
            'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
            'я': 'ya', 'і': 'i', 'ї': 'yi', 'є': 'ye'
        }
        
        # Replace common transcription errors and synonyms
        replacements = {
            "blucia": "bluesea", "blusia": "bluesea", "bluesee": "bluesea", "блю сі": "bluesea", "блюсі": "bluesea",
            "бі джей": "bj", "бі джи": "bg", "би джей": "bj", "би джи": "bg", "біджей": "bj", "плеймар": "playamar",
            "playmar": "playamar", "blaucel": "bluesea", "багамас": "bahamas",
            "іберостар": "iberostar", "ріксос": "rixos", "мітсіс": "mitsis",
            "глікотель": "grecotel", "грекотель": "grecotel", "соль": "sol", "мелія": "melia",
            "хсм": "hsm", "бг": "bg", "bg": "bj", "каста": "costa", "калла": "cala", "calla": "cala", "міллер": "millor",
            "miller": "millor", "медіадіа": "mediodia", "mediadia": "mediodia", "глобаліс": "globales",
            "globalis": "globales", "ізабель": "isabel", "азулін": "azuline"
        }
        
        for old, new in replacements.items():
            cleaned = cleaned.replace(old, new)
            
        # Try transliterating Cyrillic tokens
        tokens = cleaned.split()
        normalized_tokens = []
        for token in tokens:
            if any(ord(c) > 127 for c in token): # Has Cyrillic
                trans_token = "".join(trans_map.get(c, c) for c in token)
                normalized_tokens.append(trans_token)
            else:
                normalized_tokens.append(token)
        
        cleaned = " ".join(normalized_tokens)
            
        # Remove common separators and noise
        cleaned = re.sub(r'[^a-z0-9\s]', ' ', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        final_tokens = [t for t in cleaned.split() if t not in _NOISE_TOKENS]
        return " ".join(final_tokens)

    best_match = None
    max_score = 0.0
    query = normalize_name(hotel_name)
    if not query:
        query = hotel_name.lower()
    
    query_words = set(re.findall(r'\w+', query))
    
    for h in db:
        db_name_orig = h['hotel']
        db_name = normalize_name(db_name_orig)
        if not db_name:
            db_name = db_name_orig.lower()
            
        # 1. Exact match
        if query == db_name:
            return h, 1.0

        # 2. SequenceMatcher score
        ratio = difflib.SequenceMatcher(None, query, db_name).ratio()
        
        # 3. Word overlap bonus
        db_words = set(re.findall(r'\w+', db_name))
        if not query_words: continue
        
        overlap = len(query_words & db_words)
        overlap_ratio = overlap / len(query_words) if query_words else 0

        # Weighted score: overlap is more important for identifying the right hotel
        score = ratio * 0.2 + overlap_ratio * 0.8
        
        # Penalty for large length difference
        len_diff = abs(len(query) - len(db_name))
        if len_diff > 15: # Stricter length check
            score -= 0.2

        # Strong bonus for high word overlap
        if overlap_ratio >= 0.85:
            score += 0.5
        elif overlap_ratio >= 0.7:
            score += 0.2
                
        if score > max_score:
            max_score = score
            best_match = h
            
    if best_match and max_score > 0.75: # Increased from 0.65 to 0.75 for higher precision
        return best_match, max_score
        
    return {"hotel": hotel_name, "link": "Посилання відсутнє ⚠️"}, 0.0

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
    # Clean name from warnings for star extraction
    clean_name = hotel_name.replace('⚠️', '').strip()
    
    # Pattern 1: Digit followed by * or ★ (e.g., 5*, 5 ★)
    m = re.search(r'([1-5])\s*(?:\*|★)', clean_name)
    if m:
        return f"{m.group(1)}★"
    
    # Pattern 2: Just a digit at the very end or after a space (e.g., "Hotel Name 5")
    m = re.search(r'\s([1-5])(?:\s|$)', clean_name)
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
    # Count both "N)" patterns and "🥑" icons
    # Filter out duplicated blocks by counting unique 🥑 icons that are not too close
    meal_icons = text.count('🥑')
    numbered_matches = len(re.findall(r'^\s*\d+[\)\.]\s+', text, re.MULTILINE))
    
    # If the LLM put descriptions inside the list, we might have more text but we care about the blocks
    return max(numbered_matches, meal_icons)

def _build_price_line(price_label: str, computed_prices: list) -> str:
    if not computed_prices:
        return "💰 не вказано"
    prices_str = ", ".join([f"{i+1}){p}€" for i, p in enumerate(computed_prices)])
    return f"{price_label} - {prices_str}"

def _inject_prices(text: str, price_label: str, computed_prices: list) -> str:
    enforced_price_line = _build_price_line(price_label, computed_prices)
    lines = text.split("\n")
    
    # 1. First, remove ANY line that starts with 💰 to prevent duplicates
    lines = [line for line in lines if not line.strip().startswith("💰")]
    
    # 2. Find the anchor to insert the correct price line.
    # We want it AFTER "онлайн підтримка". If not found, then AFTER "путівник".
    anchor_idx = None
    
    # Try to find "онлайн підтримка" first
    for i, line in enumerate(lines):
        if "онлайн підтримка" in line.lower():
            anchor_idx = i
            break
            
    # If not found, try "путівник"
    if anchor_idx is None:
        for i, line in enumerate(lines):
            if "путівник" in line.lower():
                anchor_idx = i
                break
            
    if anchor_idx is not None:
        # Check if the next line is already a price line or similar (should be removed by step 1)
        lines.insert(anchor_idx + 1, enforced_price_line)
    else:
        # If no footer anchors found, the LLM might have failed to generate the footer.
        # We find the last hotel/meal line and insert after it.
        last_meal_idx = -1
        for i, line in enumerate(lines):
            if "🥑" in line:
                last_meal_idx = i
        
        if last_meal_idx != -1:
            # Insert after the last hotel block (usually hotel name + meal + link)
            # Find the end of that block
            target_idx = last_meal_idx + 1
            if target_idx < len(lines) and ("http" in lines[target_idx] or "Посилання" in lines[target_idx]):
                target_idx += 1
            
            # Add footer elements if missing
            footer = [
                "",
                "✔️ путівник + тур страхування",
                "🤓 онлайн підтримка 24/7",
                enforced_price_line,
                "",
                "❗️Ціна актуальна на момент розрахунку подорожі"
            ]
            for j, f_line in enumerate(footer):
                lines.insert(target_idx + j, f_line)
        else:
            # Total fallback
            lines.append("")
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

def _fallback_hotel_extraction(user_text: str, candidate_hotels: list) -> list:
    """Non-LLM fallback: finds hotels by simple string matching/overlap when LLM fails."""
    if not candidate_hotels:
        return []
    
    # Pre-normalize the user text for better matching
    def normalize_for_fallback(t: str) -> str:
        t = t.lower()
        replacements = {
            "blucia": "bluesea", "blusia": "bluesea", "bluesee": "bluesea", "блю сі": "bluesea", "блюсі": "bluesea",
            "бі джей": "bj", "бі джи": "bg", "би джей": "bj", "би джи": "bg", "біджей": "bj", "плеймар": "playamar",
            "playmar": "playamar", "blaucel": "bluesea", "багамас": "bahamas", "casta": "costa", "calla": "cala",
            "mediadia": "mediodia", "globalis": "globales", "ізабель": "isabel", "азулін": "azuline",
            "каста": "costa", "калла": "cala", "міллер": "millor", "медіадіа": "mediodia", "глобаліс": "globales",
            "bg": "bj", "bg ": "bj ", " bg": " bj" # Common transcription swap
        }
        for old, new in replacements.items():
            t = t.replace(old, new)
        return t

    text_norm = normalize_for_fallback(user_text)
    text_words = set(re.findall(r'\w+', text_norm))
    found_hotels = []
    
    for h in candidate_hotels:
        name = h['hotel']
        # Normalize DB name
        name_clean = re.sub(r'[3-5]\s*(?:\*|★)', '', name.lower())
        name_clean = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name_clean)
        name_norm = normalize_for_fallback(name_clean)
        name_words = [w for w in re.findall(r'\w+', name_norm) if w not in _NOISE_TOKENS]
        
        if not name_words:
            continue
            
        # 1. Check if the full normalized name is in the text
        if name_norm in text_norm:
            found_hotels.append(name)
            continue
            
        # 2. Check word overlap (allowing for small errors in names)
        matches = 0
        for nw in name_words:
            if nw in text_words:
                matches += 1
            else:
                # Fuzzy check for each word (expensive but only for name_words)
                for tw in text_words:
                    if len(tw) > 3 and nw.startswith(tw[:3]) and difflib.SequenceMatcher(None, nw, tw).ratio() > 0.8:
                        matches += 1
                        break
        
        if matches / len(name_words) >= 0.65: # Lower threshold for fuzzy word matching
            found_hotels.append(name)
            
    return _dedupe_keep_order(found_hotels)

async def format_tour_message(user_text: str, do_cleanup: bool = False) -> str:
    db = get_hotel_db()
    destinations = list(db.keys())
    
    # 0. Cleanup in parallel with initial detection if requested
    cleanup_task = None
    if do_cleanup:
        cleanup_task = asyncio.create_task(cleanup_transcribed_text(user_text))

    selected_dest = _pick_destination_by_keywords(user_text, destinations)
    
    fast_models = ["google/gemini-2.0-flash-001", "openai/gpt-4o-mini"]
    smart_models = ["openai/gpt-4o-mini", "google/gemini-2.0-flash-001"]
    
    start_time = asyncio.get_event_loop().time()

    async def _detect_destination(text):
        if selected_dest: return selected_dest
        dest_content = f"ТЕКСТ:\n{text}\n\nДОСТУПНІ НАПРЯМКИ:\n{', '.join(destinations)}"
        
        raw = await _call_llm_with_retry(
            messages=[{"role": "system", "content": _DESTINATION_PROMPT}, {"role": "user", "content": dest_content}],
            models=fast_models,
            timeout=15
        )
        if raw:
            model_name = re.sub(r'[^\w\s-]', '', raw.lower()).strip()
            for d in destinations:
                if d.lower() in model_name or model_name in d.lower(): return d
        return destinations[0] if destinations else "Unknown"

    price_task = asyncio.create_task(extract_prices_from_text(user_text, fast_models))
    dest_task = asyncio.create_task(_detect_destination(user_text))
    meal_task = asyncio.create_task(_extract_meals(user_text, fast_models))
    
    async def _extract_hotels_broadly(text):
        raw = await _call_llm_with_retry(
            messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": f"ТЕКСТ МЕНЕДЖЕРА:\n{text}\n\nЗнайди всі готелі."}],
            models=fast_models,
            timeout=20
        )
        if raw:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group()).get("hotels", [])
                except: pass
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
    # Enable smart candidate filtering for large databases (Crete, Mallorca, etc.)
    # High-quality matches will always be in the top 100.
    candidate_hotels = _build_hotel_candidates(user_text, relevant_hotels, limit=60)
    
    async def _do_targeted_extract(text_to_parse):
        # Use the smart-filtered list
        db_names = "\n".join([h['hotel'] for h in candidate_hotels])
        # IMPORTANT: We pass ONLY candidate hotels to LLM to prevent it from picking random ones
        extraction_content = f"ТЕКСТ:\n{text_to_parse}\n\nНАПРЯМОК: {selected_dest}\n\nБАЗА (ТІЛЬКИ ЦІ ГОТЕЛІ):\n{db_names}"
        
        raw = await _call_llm_with_retry(
            messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": extraction_content}],
            models=["openai/gpt-5.4-mini", "openai/gpt-4o-mini"],
            timeout=40,
            max_tokens=1000
        )
        if raw:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group()).get("hotels", [])
                except: pass
        return []

    # If it was a voice message, the raw transcription might be better for hotel name extraction
    # than the LLM-cleaned one which might "over-clean" names.
    # We'll try to extract from the user_text (which might be cleaned) and fallback if needed.
    extracted_hotels = await _do_targeted_extract(user_text)
    
    if not extracted_hotels:
        logger.info("LLM extraction failed or returned empty list. Trying fallback search...")
        extracted_hotels = _fallback_hotel_extraction(user_text, candidate_hotels)
        if not extracted_hotels:
             # Try one more time with broader candidate list
             extracted_hotels = _fallback_hotel_extraction(user_text, relevant_hotels[:200])
             
    if not extracted_hotels and do_cleanup:
        # If it was a voice message and nothing was found, try extracting from a slightly broader context
        logger.info("No hotels found in cleaned text even with fallback.")
    
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
        match, score = fuzzy_match_hotel(h_name, candidate_hotels)
        if score == 0.0 or "Посилання відсутнє" in match["link"]:
            match, score = fuzzy_match_hotel(h_name, relevant_hotels)
        if score == 0.0 and all_hotels_list:
            global_match, g_score = fuzzy_match_hotel(h_name, all_hotels_list)
            if g_score > 0:
                match, score = global_match, g_score
        
        # New Rule: If confidence is low, add warning emoji. If no match, use original name.
        display_name = match["hotel"]
        stars = _extract_allowed_stars(display_name)
        
        # CLEANUP: Remove any existing stars or symbols from display_name before adding ours
        # to avoid "3* 3★ 3★"
        display_name = re.sub(r'\s*[3-5]\s*(?:\*|★)', '', display_name).strip()

        if score == 0.0:
            display_name = f"{h_name} ⚠️"
        elif score < 0.70: # Slightly lower threshold
            display_name = f"{display_name} ⚠️"

        # Force stars from DB if they were extracted
        if stars:
            # Check if name already has this exact star string to avoid duplication
            if stars not in display_name:
                display_name = f"{display_name} {stars}"

        key = display_name.strip().lower()
        if key in seen_hotels: continue
        seen_hotels.add(key)
        
        # Update match dict for internal consistency
        match["hotel"] = display_name
        matched_hotels.append(match)
        
        stars = _extract_allowed_stars(display_name)
        matched_info.append(f"- Назва: {display_name}, Зірки: {stars or 'не вказувати'}, Посилання: {match['link']}")
        hotel_link_map[display_name.lower()] = match['link']
        
    db_text = "\n".join(matched_info) if matched_info else "Не вдалося витягнути готелі."
    
    price_label = "💰 загальна вартість туру за особу"
    computed_prices = []
    if price_data and price_data.get("hotel_prices") and price_data.get("flight_per_person") is not None:
        adults = _safe_int(price_data.get("adults"), 2)
        children = _safe_int(price_data.get("children"), 0)
        infants = _safe_int(price_data.get("infants"), 0)
        total_people = adults + children
        has_children = (children + infants) > 0
        
        flight = 0.0
        try:
            flight_raw = str(price_data.get("flight_per_person") or "0")
            flight = float(re.sub(r'[^\d.]', '', flight_raw.replace(',', '.')) or 0)
        except Exception: pass
        
        other = 0.0
        try:
            other_raw = str(price_data.get("other_per_person") or "0")
            other = float(re.sub(r'[^\d.]', '', other_raw.replace(',', '.')) or 0)
        except Exception: pass
        
        hotel_prices = []
        for p in (price_data.get("hotel_prices") or []):
            try:
                p_clean = re.sub(r'[^\d.]', '', str(p).replace(',', '.'))
                if p_clean: hotel_prices.append(float(p_clean))
            except Exception: pass
            
        nights = _safe_int(price_data.get("nights"), 7)
        month = _safe_int(price_data.get("check_in_month"), 6)
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
            # TAX RULE: Only adults pay tax (approximate logic for Mallorca/Spain)
            # We calculate total tax for adults and distribute it per person in the final display
            total_tax_for_stay = tax_per_night * nights * adults
            tax_per_person_share = total_tax_for_stay / total_people if total_people > 0 else 0
            
            # MATH LOGIC:
            # 1. Base cost per person (WITHOUT TAX)
            hotel_per_person = hotel_total / total_people if total_people > 0 else hotel_total
            base_cost_no_tax = hotel_per_person + flight + other
            
            # 2. Markup (Margin) applied ONLY to net cost
            if base_cost_no_tax < 350:
                final_no_tax = base_cost_no_tax + 150
            else:
                final_no_tax = base_cost_no_tax * 1.43
            
            # 3. Add tax AFTER markup (tax is not a subject for margin)
            final = final_no_tax + tax_per_person_share
                
            # Round to nearest 5
            final = round(final / 5) * 5
            
            logger.info(f"CALC: Hotel={matched_hotels[idx]['hotel'] if idx < len(matched_hotels) else '?'}, PriceIn={hotel_total}, Flight={flight}, TaxShare={tax_per_person_share}, Base={base_cost_no_tax}, Final={final}")
            
            # For children/infants, we usually show total price for everyone
            if has_children:
                total_tour_price = round(final * total_people)
                computed_prices.append(total_tour_price)
            else:
                computed_prices.append(final)
        
        price_label = "💰 загальна вартість туру за всіх" if has_children else "💰 загальна вартість туру за особу"
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
    
    result = await _call_llm_with_retry(
        messages=[{"role": "system", "content": _FORMAT_PROMPT}, {"role": "user", "content": combined}],
        models=smart_models,
        timeout=90,
        max_tokens=2500
    )
    
    if result:
        # 1. Basic cleanup
        result = re.sub(r'<math>.*?</math>', '', result, flags=re.DOTALL).strip()
        
        # 2. Fix spacing: reduce multiple newlines to max 2
        result = re.sub(r'\n{3,}', '\n\n', result)
        
        # 3. Final formatting cleanups
        result = re.sub(r'[\s\[(]*Посилання[\]\)]*', '', result, flags=re.IGNORECASE)
        result = re.sub(r'[`\[](https?://[^\s`\]]+)[`\]]', r'\1', result)
        result = re.sub(r'([1-5]★)\s+\1', r'\1', result)
        
        # 4. Inject links, missing hotels and correct prices
        result = _inject_links(result, hotel_link_map)
        result = _append_missing_hotels(result, matched_hotels, computed_prices)
        result = _inject_prices(result, price_label, computed_prices)
        
        # 5. Remove double footer "❗️Ціна актуальна" if LLM repeated it
        footer_phrase = "❗️Ціна актуальна на момент розрахунку подорожі"
        if result.count(footer_phrase) > 1:
            # Keep only the first occurrence
            parts = result.split(footer_phrase)
            result = footer_phrase.join(parts[:2]) + "".join(parts[2:])

        return result.strip()
    
    return "❌ Помилка генерації тексту."

    return "❌ Помилка розпізнавання (обидва сервіси недоступні)."
