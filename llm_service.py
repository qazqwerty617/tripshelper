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

# Brands for strict matching (Global for all functions)
BRANDS = {"bluesea", "hipotels", "globales", "iberostar", "rixos", "mitsis", "grecotel", "sol", "melia", "hsm", "azuline", "bj", "bg", "thb", "bahia", "palladium", "h10", "riu", "barcelo", "occidental", "allegro", "viva", "zafiro", "js", "bjs", "mar"}

_DESTINATION_PROMPT = """Ти — туристичний асистент. Тобі надіслали текст-чернетку від менеджера з описом туру.
Твоє завдання: визначити напрямок (країну/острів/регіон) з тексту і вибрати один найбільш підходящий варіант із наданого списку доступних напрямків.
Поверни ТІЛЬКИ назву напрямку зі списку. Якщо жоден не підходить, поверни "Unknown".
Без жодного іншого тексту.
"""

_EXTRACT_PROMPT = """Ти — робот-парсеp. Твоє завдання: виписати назви готелів ТАК, ЯК ЇХ НАПИСАВ МЕНЕДЖЕР. 
ПРАВИЛА: 
1. Витягни назву готелю та ціну. 
2. НЕ намагайся знайти їх у базі. 
3. НЕ виправляй назви. 
4. ПОРЯДОК: як у тексті. 
ФОРМАТ: JSON {"found_hotels": [{"raw_name": "назва", "price": 1200}]} 
"""

_EXTRACT_PRICES_PROMPT = """Ти — фінансовий парсер. Витягни числові дані з тексту: 
1. adults: кількість дорослих. 
2. children: кількість дітей (якщо є). 
3. nights: кількість ночей (за замовчуванням 7, якщо не вказано, але шукай уважно). 
4. check_in_month: номер місяця вильоту (1-12). 
5. flight_per_person: ціна авіа на 1 особу (якщо загальна - поділи на всіх людей). 
6. hotel_prices: СЛОВНИК {"Назва готелю": ціна за номер}. 

КРИТИЧНО: Будь дуже уважним до слова "дитина/діти". Якщо воно є — children має бути більше 0! 
ФОРМАТ: Тільки JSON. 
"""

_FORMAT_PROMPT = """Ти — професійний тревел-дизайнер. Твоє завдання: написати вступну частину повідомлення та блок рекомендацій.

БЛОК 1: ВСТУП (ПРИКЛАД):
Авіатур до Майорки 🇪🇸
Із Берліна 🇩🇪
🌤️ 15.06 - 25.06, 10 ночей
Туди 22:10
Назад 15:35
🧳 ручна поклажа до 10 кг та розміром 20х40х30 см

БЛОК 2: РЕКОМЕНДАЦІЇ (ОБОВ'ЯЗКОВО):
- Оберіть ТІЛЬКИ 2-3 найкращих готелі з наданого списку. НЕ БІЛЬШЕ.
- Для кожного обраного готелю напишіть переконливий опис (400-600 символів).
- Пишіть емоційно, від першої особи, підкреслюючи переваги.
- Формат:
**[Назва готелю] [Зірки]**
[Ваш текст опису]
(порожній рядок між рекомендаціями)

ПРАВИЛА:
1. Поверни ТІЛЬКИ Вступ та Рекомендації.
2. Використовуй роздільник "===END_INTRO===" між Вступом та Рекомендаціями.
3. СУВОРО ЗАБОРОНЕНО: Не пиши нумерований список готелів (1, 2, 3...) та типи харчування у вступі. Тільки заголовок та деталі перельоту.
4. НЕ ПИШИ ціни — я додам їх сам.
5. НЕ ПИШИ фразу "Ціна актуальна..." — я додам її сам.
"""

def calculate_tour_prices(hotel_prices: list, flight_per_person: float,
                          other_per_person: float, total_people: int,
                          has_children: bool, tourist_tax_per_person: float = 0) -> list:
    results = []
    for hotel_total in hotel_prices:
        hotel_per_person = hotel_total / total_people if total_people > 0 else hotel_total
        cost = hotel_per_person + flight_per_person + other_per_person + tourist_tax_per_person
        
        # Markup logic from April 28th
        if cost < 350:
            final_per_person = cost + 150
        else:
            final_per_person = cost * 1.43
        
        # Original rounding from April 28th
        final_per_person = round(final_per_person) + 5
        
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
        timeout=20,
        response_format={"type": "json_object"}
    )
    if raw:
        try:
            return json.loads(raw)
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
4. КІЛЬКІСТЬ: Якщо в тексті 7 готелів, у масиві "meals" має бути РІВНО 7 елементів.
5. ФОРМАТ: Тільки JSON {"meals": ["тип 1", "тип 2"]}.
"""

async def _call_llm_with_retry(messages, models, temperature=0, timeout=30, max_tokens=None, response_format=None):
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
                if response_format:
                    params["response_format"] = response_format
                    
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
        timeout=15,
        response_format={"type": "json_object"}
    )
    if raw:
        try:
            return json.loads(raw).get("meals", [])
        except: pass
    return []

def fuzzy_match_hotel(hotel_name: str, db: list) -> tuple[dict, float]:
    def normalize_name(name: str) -> str:
        # Remove stars from name for better matching
        cleaned = re.sub(r'[1-5]\s*(?:\*|★)', '', name.lower())
        
        # Simple Transliteration for Ukrainian/Russian names to Latin
        trans_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'ґ': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
            'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
            'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
            'ч': 'ch', 'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu',
            'я': 'ya', 'і': 'i', 'ї': 'yi', 'є': 'ye'
        }
        
        # Replace common transcription errors and synonyms
        replacements = {
            "blucia": "bluesea", "blusia": "bluesea", "bluesee": "bluesea", "блю сі": "bluesea", "блюсі": "bluesea",
            "бі джей": "bj", "би джей": "bj", "біджей": "bj", "плеймар": "playamar", "playmar": "playamar",
            "blaucel": "bluesea", "багамас": "bahamas",
            "іберостар": "iberostar", "ріксос": "rixos", "мітсіс": "mitsis",
            "глікотель": "grecotel", "грекотель": "grecotel", "соль": "sol", "мелія": "melia",
            "хсм": "hsm", "каста": "costa", "калла": "cala", "calla": "cala", "міллер": "millor",
            "miller": "millor", "медіадіа": "mediodia", "mediadia": "mediodia", "глобаліс": "globales",
            "globalis": "globales", "ізабель": "isabel", "азулін": "azuline", "гранд": "gran", "grand": "gran"
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

    query = normalize_name(hotel_name)
    query_words = set(query.split())
    
    if not query_words:
        return {"hotel": hotel_name, "link": "Посилання відсутнє ⚠️"}, 0.0

    best_match = None
    max_score = 0.0

    for h in db:
        db_name_orig = h['hotel']
        db_name = normalize_name(db_name_orig)
        db_words = set(db_name.split())
        
        # 1. Проверяем, входят ли ВСЕ слова из запроса в название из базы 
        # (Например: "Eri Beach" полностью входит в "Eri Beach & Village") 
        if query_words.issubset(db_words): 
            return h, 1.0 
        
        # 2. Считаем процент перекрытия слов 
        overlap = len(query_words & db_words) 
        score = overlap / len(query_words) 
        
        # Штраф за разные бренды (если они есть) 
        q_brands = query_words & BRANDS 
        d_brands = db_words & BRANDS 
        if q_brands and d_brands and q_brands != d_brands: 
            score -= 1.0 

        if score > max_score: 
            max_score = score 
            best_match = h 

    # ПОРОГ ТЕПЕРЬ 0.6 (позволяет находить отели, даже если менеджер сказав половину названия) 
    if best_match and max_score >= 0.6: 
        return best_match, max_score 
        
    return {"hotel": hotel_name, "link": "Посилання відсутнє ⚠️"}, 0.0

def _build_hotel_candidates(user_text: str, relevant_hotels: list, limit: int = 150) -> list:
    if len(relevant_hotels) <= limit:
        return relevant_hotels
    
    text_norm = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', user_text.lower())
    text_norm = re.sub(r'\s+', ' ', text_norm).strip()
    text_words = set(re.findall(r'\w+', text_norm))
    
    # Filter out noise from text words for scoring
    text_words_clean = text_words - _NOISE_TOKENS
    
    scored = []
    for hotel in relevant_hotels:
        name = hotel.get("hotel", "")
        name_norm = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name.lower())
        name_norm = re.sub(r'\s+', ' ', name_norm).strip()
        hotel_words = set(re.findall(r'\w+', name_norm))
        hotel_words_clean = hotel_words - _NOISE_TOKENS
        
        if not hotel_words_clean:
            scored.append((0, hotel))
            continue
            
        overlap = len(hotel_words_clean & text_words_clean)
        
        # Brands match
        brand_overlap = len(hotel_words & BRANDS & text_words)
        
        # Sequence ratio for fuzzy parts
        ratio = difflib.SequenceMatcher(None, text_norm, name_norm).ratio()
        
        # Weighted score: overlap is most important, then brands
        score = overlap * 5 + brand_overlap * 3 + ratio * 2
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
        # Remove ordinals and common prefixes
        t = re.sub(r'\b(перший|другий|третій|четвертий|п’ятий|шостий|сьомий|восьмий|дев’ятий|десятий|одинадцятий|дванадцятий)\b', '', t)
        t = re.sub(r'\b(готель|отель|номер|варіант)\b', '', t)
        
        replacements = {
            "blucia": "bluesea", "blusia": "bluesea", "bluesee": "bluesea", "блю сі": "bluesea", "блюсі": "bluesea",
            "бі джей": "bj", "бі джи": "bj", "би джей": "bj", "би джи": "bj", "біджей": "bj", "плеймар": "playamar",
            "playmar": "playamar", "blaucel": "bluesea", "багамас": "bahamas", "casta": "costa", "calla": "cala",
            "mediadia": "mediodia", "globalis": "globales", "ізабель": "isabel", "азулін": "azuline",
            "каста": "costa", "калла": "cala", "міллер": "millor", "медіадіа": "mediodia", "глобаліс": "globales",
            "гранд": "gran", "grand": "gran",
            "bg": "bj", "bg ": "bj ", " bg": " bj" # Common transcription swap
        }
        for old, new in replacements.items():
            t = t.replace(old, new)
        return t

    text_norm = normalize_for_fallback(user_text)
    text_words = set(re.findall(r'\w+', text_norm))
    found_hotels = []
    
    # Sort hotels by length descending to match longer names first (e.g. "Hotel Brand Name" before "Hotel Brand")
    sorted_candidates = sorted(candidate_hotels, key=lambda x: len(x['hotel']), reverse=True)
    
    # 1. Check if the full normalized name is in the text
    # Prioritize exact matches first
    exact_matches = []
    for h in sorted_candidates:
        name = h['hotel']
        # Clean stars and trailing digits from DB name
        name_clean = re.sub(r'\s*[1-5]\s*(?:\*|★)', ' ', name.lower())
        name_clean = re.sub(r'\s+[1-5]\s*$', ' ', name_clean)
        
        name_clean = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name_clean)
        name_norm = normalize_for_fallback(name_clean)
        
        if len(name_norm) > 5 and name_norm in text_norm:
            exact_matches.append(name)
            
    # 2. Check word overlap for the rest
    fuzzy_matches = []
    for h in sorted_candidates:
        name = h['hotel']
        if name in exact_matches: continue
        
        # Clean stars and trailing digits from DB name
        name_clean = re.sub(r'\s*[1-5]\s*(?:\*|★)', ' ', name.lower())
        name_clean = re.sub(r'\s+[1-5]\s*$', ' ', name_clean)
        
        name_clean = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name_clean)
        name_norm = normalize_for_fallback(name_clean)
        name_words = [w for w in re.findall(r'\w+', name_norm) if w not in _NOISE_TOKENS]
        
        if not name_words: continue
            
        matches = 0
        for nw in name_words:
            if nw in text_words:
                matches += 1
            else:
                for tw in text_words:
                    if len(tw) > 3 and nw.startswith(tw[:3]) and difflib.SequenceMatcher(None, nw, tw).ratio() > 0.8:
                        matches += 1
                        break
        
        if matches / len(name_words) >= 0.75: 
            fuzzy_matches.append(name)
            
    # Sort by appearance in text
    all_found = exact_matches + fuzzy_matches
    all_found.sort(key=lambda name: text_norm.find(normalize_for_fallback(re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', name.lower()))))
    
    return _dedupe_keep_order(all_found)

def _count_potential_hotels(text: str) -> int:
    """Estimates how many hotels are mentioned based on numbering patterns."""
    text = text.lower()
    # Count patterns like "1 готель", "2 вариант", "3)", "4.", etc.
    patterns = [
        r'\d+\s*[)\.]\s+', # 1) or 1.
        r'\d+\s+(?:готель|отель|варіант|вариант)', # 1 готель
        r'(?:перший|другий|третій|четвертий|п’ятий|шостий|сьомий|восьмий|дев’ятий|десятий)\s+(?:готель|отель|варіант|вариант)'
    ]
    all_matches = set()
    for p in patterns:
        for m in re.finditer(p, text):
            all_matches.add(m.start())
    
    count = len(all_matches)
    return count if count > 0 else 1

def _sort_hotels_by_appearance(hotels: list[str], text: str) -> list[str]:
    """Sorts hotel names based on their first appearance in the text."""
    text_lower = text.lower()
    # Normalize text for searching (remove punctuation but keep ordinals)
    text_norm = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', text_lower)
    text_norm = re.sub(r'\s+', ' ', text_norm).strip()

    def get_first_pos(h_name: str) -> int:
        clean_name = h_name.replace("[NOT_FOUND]", "").strip().lower()
        # Clean stars/ratings from name for search
        clean_name = re.sub(r'\s*[1-5]\s*(?:\*|★)', ' ', clean_name)
        clean_name = re.sub(r'[^a-z0-9а-яіїєґ\s]', ' ', clean_name)
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        if not clean_name: return 999999
        
        # 1. Try full name match
        pos = text_norm.find(clean_name)
        if pos != -1: return pos
        
        # 2. Try unique words match (first unique word that appears)
        words = [w for w in clean_name.split() if w not in _NOISE_TOKENS and len(w) > 3]
        if words:
            positions = []
            for w in words:
                p = text_norm.find(w)
                if p != -1: positions.append(p)
            if positions: return min(positions)
            
        return 999999

    # Sort and remove duplicates while preserving first appearance
    seen = set()
    sorted_hotels = sorted(hotels, key=get_first_pos)
    final = []
    for h in sorted_hotels:
        if h.lower() not in seen:
            final.append(h)
            seen.add(h.lower())
    return final

async def format_tour_message(user_text: str, do_cleanup: bool = False, raw_voice_text: str = None) -> str:
    db = get_hotel_db()
    destinations = list(db.keys())
    
    # Text to use for hotel name extraction (raw is better for fuzzy matching)
    hotel_search_text = raw_voice_text if raw_voice_text else user_text
    
    potential_count = _count_potential_hotels(hotel_search_text)
    logger.info(f"Potential hotels count detected: {potential_count}")
    
    # Pre-clean hotel search text from common ordinal words that might confuse extraction
    hotel_search_text_cleaned = re.sub(r'\b(перший|другий|третій|четвертий|п’ятий|шостий|сьомий|восьмий|дев’ятий|десятий)\s+готель\b', 'готель', hotel_search_text.lower())
    
    # 0. Cleanup in parallel with initial detection if requested
    cleanup_task = None
    if do_cleanup:
        cleanup_task = asyncio.create_task(cleanup_transcribed_text(user_text))

    selected_dest = _pick_destination_by_keywords(hotel_search_text_cleaned, destinations)
    
    fast_models = ["openai/gpt-5.4-mini", "google/gemini-2.5-flash"]
    smart_models = ["openai/gpt-5.4-mini", "google/gemini-2.5-flash"]
    
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

    # ЗАВЖДИ беремо сирий текст для витягування цін та дат, бо очищений текст втрачає дані!
    base_text_for_data = raw_voice_text if raw_voice_text else user_text
    
    # Pass hint about potential count to price extractor
    price_content = base_text_for_data
    if potential_count > 1:
        price_content = f"(ВАЖЛИВО: Я очікую {potential_count} готелів)\n{base_text_for_data}"

    price_task = asyncio.create_task(extract_prices_from_text(price_content, fast_models))
    dest_task = asyncio.create_task(_detect_destination(hotel_search_text))
    meal_task = asyncio.create_task(_extract_meals(user_text, fast_models))
    
    async def _extract_hotels_broadly(text):
        raw = await _call_llm_with_retry(
            messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": f"ТЕКСТ МЕНЕДЖЕРА:\n{text}\n\nЗнайди всі готелі."}],
            models=fast_models,
            timeout=20,
            response_format={"type": "json_object"}
        )
        if raw:
            try:
                return json.loads(raw).get("hotels", [])
            except: pass
        return []

    broad_hotel_task = asyncio.create_task(_extract_hotels_broadly(hotel_search_text))
    
    # Wait for first-round tasks
    if cleanup_task:
        user_text = await cleanup_task
        
    selected_dest = await dest_task
    
    # ✅ ДОДАЙ ОЧИЩЕННЯ НАЗВИ ВІД "20стр", "13стр" ТОЩО:
    if selected_dest:
        clean_dest_name = re.sub(r'\s*\d+\s*стр.*', '', selected_dest, flags=re.IGNORECASE).strip().title()
    else:
        clean_dest_name = "Unknown"
    
    price_data = await price_task
    # broad_hotels = await broad_hotel_task # Skip broad task to rely more on targeted extraction
    extracted_meals = await meal_task
    
    # NEW: If price_data has more hotels than we extracted, we need to be careful
    expected_count = len(price_data.get("hotel_prices", [])) if price_data else 0
    
    # If price extractor found fewer hotels than our heuristic, use the higher number as expected
    if potential_count > expected_count:
        logger.warning(f"Price extractor found {expected_count} but heuristic found {potential_count}. Using {potential_count} as target.")
        expected_count = potential_count
    
    logger.info(f"Step 1 parallel done in {asyncio.get_event_loop().time() - start_time:.2f}s. Dest: {clean_dest_name}")

    relevant_hotels = db.get(selected_dest, [])
    
    # -----------------------------------------
    # STRICT DIRECT MATCHING PHASE
    # -----------------------------------------
    text_clean_for_search = re.sub(r'[^a-z0-9\s]', ' ', hotel_search_text_cleaned.lower())
    text_clean_for_search = re.sub(r'\s+', ' ', text_clean_for_search).strip()
    
    direct_matched_hotels = []
    for h in relevant_hotels:
        h_name = h['hotel'].lower()
        h_name = re.sub(r'\s*[1-5]\s*(?:\*|★)', ' ', h_name)
        h_name = re.sub(r'\s+[1-5]\s*$', ' ', h_name)
        
        h_clean = re.sub(r'[^a-z0-9\s]', ' ', h_name)
        h_clean = re.sub(r'\s+', ' ', h_clean).strip()
        h_words = [w for w in h_clean.split() if w not in _NOISE_TOKENS and len(w) > 2]
        
        if h_words:
            if h_clean in text_clean_for_search:
                direct_matched_hotels.append(h['hotel'])
            else:
                unique_db_words = set(h_words) - BRANDS
                if unique_db_words and all(word in text_clean_for_search for word in unique_db_words):
                    db_brands = set(h_words) & BRANDS
                    text_words = set(text_clean_for_search.split())
                    text_brands = text_words & BRANDS
                    if not db_brands or (db_brands & text_brands):
                        direct_matched_hotels.append(h['hotel'])
    
    if "playamar" in text_clean_for_search:
        direct_matched_hotels = [name for name in direct_matched_hotels if "playamar" in name.lower() or "playamar" not in " ".join(direct_matched_hotels).lower()]
    
    logger.info(f"Direct matching found: {direct_matched_hotels}")

    async def _extract_hotels_and_prices(text):
        raw = await _call_llm_with_retry(
            messages=[{"role": "system", "content": _EXTRACT_PROMPT}, {"role": "user", "content": text}],
            models=fast_models,
            timeout=25,
            response_format={"type": "json_object"}
        )
        if raw:
            try:
                return json.loads(raw).get("found_hotels", [])
            except: pass
        return []

    # Витягуємо готелі та ціни парой
    extracted_data = await _extract_hotels_and_prices(hotel_search_text)
    extracted_hotels_raw = [item.get("raw_name") for item in extracted_data]
    extracted_prices_raw = [item.get("price") for item in extracted_data]
    
    logger.info(f"LLM extracted {len(extracted_hotels_raw)} items: {extracted_data}")

    # Matching extracted names with DB
    matched_hotels = []
    final_hotel_prices_raw = []
    seen_hotels = set()
    
    for i, h_name in enumerate(extracted_hotels_raw):
        if not h_name: continue
        
        match, score = fuzzy_match_hotel(h_name, relevant_hotels)
        display_name = match["hotel"]
        
        # Додаємо зірки, якщо їх немає в назві
        stars = _extract_allowed_stars(display_name)
        if stars and stars not in display_name:
            display_name = f"{display_name} {stars}"
            
        key = display_name.strip().lower()
        if key in seen_hotels: continue
        seen_hotels.add(key)
        
        match["hotel"] = display_name
        matched_hotels.append(match)
        
        # Беремо ціну, яку витягнула LLM парой до цього готелю
        price_val = extracted_prices_raw[i] if i < len(extracted_prices_raw) else 0
        try:
            p_clean = re.sub(r'[^\d.]', '', str(price_val).replace(',', '.'))
            final_hotel_prices_raw.append(float(p_clean) if p_clean else 0.0)
        except:
            final_hotel_prices_raw.append(0.0)

    computed_prices = []
    has_children = False
    price_label = "💰 загальна вартість туру за особу"

    if price_data and final_hotel_prices_raw:
        adults = _safe_int(price_data.get("adults"), 2)
        children = _safe_int(price_data.get("children"), 0)
        infants = _safe_int(price_data.get("infants"), 0)
        total_people = adults + children
        has_children = (children + infants) > 0
        
        # NEW: Raw flight price (might be total or per person)
        flight_raw_val = price_data.get("flight_total") or price_data.get("flight_per_person") or 0
        flight_per_person = 0.0
        try:
            f_clean = float(re.sub(r'[^\d.]', '', str(flight_raw_val).replace(',', '.')) or 0)
            if f_clean > 500 and total_people > 1 and "total" in str(price_data.keys()).lower():
                flight_per_person = f_clean / total_people
            else:
                flight_per_person = f_clean
        except: pass
        
        other = 0.0
        try:
            other_raw = str(price_data.get("other_per_person") or "0")
            other = float(re.sub(r'\D.', '', other_raw.replace(',', '.')) or 0)
        except: pass
        
        nights = _safe_int(price_data.get("nights"), 7)
        month = _safe_int(price_data.get("check_in_month"), 6)
        hotel_stars_list = price_data.get("hotel_stars") or []
        
        for idx, hotel_total in enumerate(final_hotel_prices_raw):
            stars_val = 0
            db_stars_str = _extract_allowed_stars(matched_hotels[idx]['hotel']) if idx < len(matched_hotels) else ""
            if db_stars_str:
                m_stars = re.search(r'\d', db_stars_str)
                stars_val = int(m_stars.group()) if m_stars else 0
            elif hotel_stars_list and idx < len(hotel_stars_list):
                stars_val = _safe_int(hotel_stars_list[idx])
            
            tax_per_night = get_tax_per_person_per_night(clean_dest_name or "", stars_val, month, total_people)
            total_tax_for_stay = tax_per_night * nights * adults
            tax_per_person_share = total_tax_for_stay / total_people if total_people > 0 else 0
            
            # MATH LOGIC FROM APRIL 28TH
            hotel_per_person = hotel_total / total_people if total_people > 0 else hotel_total
            base_cost_no_tax = hotel_per_person + flight_per_person + other
            
            if base_cost_no_tax < 350:
                final_no_tax = base_cost_no_tax + 150
            else:
                final_no_tax = base_cost_no_tax * 1.43
            
            final = final_no_tax + tax_per_person_share
            final = round(final) + 5
            
            if has_children:
                computed_prices.append(round(final * total_people))
            else:
                computed_prices.append(final)
        
        price_label = "💰 загальна вартість туру за всіх" if has_children else "💰 загальна вартість туру за особу"

    # Prepare data for LLM formatting
    hotels_info = []
    for i, h in enumerate(matched_hotels):
        stars = _extract_allowed_stars(h['hotel'])
        meal = extracted_meals[i] if extracted_meals and i < len(extracted_meals) else "не вказано"
        price = computed_prices[i] if i < len(computed_prices) else "не вказано"
        hotels_info.append(f"{i+1}) {h['hotel']} (ЗІРКИ: {stars if stars else 'немає'}) | Харчування: {meal} | Посилання: {h['link']} | ЦІНА: {price}€")
    
    db_text = "\n".join(hotels_info)
    
    combined_content = f"ТЕКСТ МЕНЕДЖЕРА:\n{user_text}\n\nНАПРЯМОК: {clean_dest_name}\n\n"
    combined_content += f"БАЗА ГОТЕЛІВ ТА ЦІНИ (ВИКОРИСТОВУЙ ВСЕ):\n{db_text}\n\n"
    combined_content += f"РОЗРАХОВАНІ ЦІНИ (ДЛЯ РЯДКА З ЦІНАМИ):\n{price_label} - {', '.join([f'{i+1}){p}€' for i, p in enumerate(computed_prices)])}"

    result = await _call_llm_with_retry(
        messages=[{"role": "system", "content": _FORMAT_PROMPT}, {"role": "user", "content": combined_content}],
        models=smart_models,
        timeout=90,
        max_tokens=3000
    )
    
    # Prepare meals alignment for the programmatic block
    hotel_meal_list = []
    for i in range(len(matched_hotels)):
        if extracted_meals and i < len(extracted_meals):
            hotel_meal_list.append(extracted_meals[i])
        else:
            hotel_meal_list.append("не вказано")

    if result:
        # 1. Start with the LLM-generated intro and recommendations
        # (We assume LLM followed the instruction to provide Intro and Recommendations)
        
        # 2. Build the "Options" block programmatically (100% precision)
        options_block = "\n🏠 варіанти проживання:\n\n"
        for i, hotel_data in enumerate(matched_hotels, 1):
            name = hotel_data['hotel']
            # Link mapping: use the exact name as stored in matched_hotels
            link = hotel_data['link']
            meal = hotel_meal_list[i-1] # Use the prepared list
            
            options_block += f"{i}) {name}\n"
            options_block += f"🥑 {meal}\n"
            options_block += f"{link}\n\n"
            
        # 3. Build the "Footer" block programmatically
        footer_block = "✔️ путівник + тур страхування\n"
        footer_block += "🤓 онлайн підтримка 24/7\n"
        footer_block += f"{price_label} - "
        
        price_strings = []
        for i, p in enumerate(computed_prices, 1):
            price_strings.append(f"{i}){p}€")
        footer_block += ", ".join(price_strings) + "\n\n"
        footer_block += "❗️Ціна актуальна на момент розрахунку подорожі\n\n"

        # 4. Assemble the final message
        # Split LLM result into Intro and Recommendations using the delimiter
        if "===END_INTRO===" in result:
            parts = result.split("===END_INTRO===")
            intro = parts[0].strip()
            recommendations_raw = parts[1].strip()
        else:
            # Fallback if LLM didn't use the delimiter
            lines = result.split("\n")
            intro_lines = []
            recommendation_lines = []
            is_recommendation = False
            for line in lines:
                if "**" in line and not is_recommendation:
                    is_recommendation = True
                if is_recommendation:
                    recommendation_lines.append(line)
                else:
                    intro_lines.append(line)
            intro = "\n".join(intro_lines).strip()
            recommendations_raw = "\n".join(recommendation_lines).strip()
        
        # Programmatically limit recommendations to 3
        rec_parts = re.split(r'\n(?=\*\*)', recommendations_raw)
        recommendations = "\n".join(rec_parts[:3]).strip()
        
        # Assemble: Intro -> Options -> Footer -> Recommendations
        final_message = f"{intro}\n{options_block}{footer_block}\n{recommendations}"
        
        # Final cleanup of multiple newlines and extra symbols
        final_message = re.sub(r'\n{3,}', '\n\n', final_message).strip()
        
        return final_message
    
    return "❌ Помилка генерації тексту."
