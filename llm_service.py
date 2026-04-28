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

_EXTRACT_PROMPT = """Ти — туристичний асистент. Тобі надіслали текст-чернетку від менеджера з описом туру (місто вильоту, курорт, готель, харчування та ціни).
Твоє завдання: знайти у тексті ВСІ згадані готелі.

ПРАВИЛА ВИЛУЧЕННЯ:
- Менеджер часто диктує назви готелів українською або російською транслітерацією (наприклад, "Ариас осіті", "Хедеф Біч", "Блусі Дон Джайме").
- Ти ОБОВ'ЯЗКОВО маєш перекласти ці назви на правильну АНГЛІЙСЬКУ (латинську) назву готелю (наприклад: "Ares City", "Hedef Beach", "Bluesea Don Jaime").
- Якщо вказана зірковість (наприклад "три зірки", "4 зірки"), додай її до назви (наприклад "Ares City 3*").
- КРИТИЧНО ВАЖЛИВО: Ти маєш знайти і повернути ВСІ готелі, які перелічив менеджер. Якщо їх 10, поверни 10. Якщо 15, поверни 15. НЕ ПРОПУСКАЙ ЖОДНОГО!
- Тобі також передають "СПИСОК ГОТЕЛІВ НАПРЯМКУ" (з бази). Якщо готель менеджера схожий на назву зі списку — ОБОВ'ЯЗКОВО поверни саме назву зі списку.
- Не вигадуй нові готелі, якщо в списку є очевидний відповідник.

ВАЖЛИВО:
1. Поверни результат У ФОРМАТІ СУВОРОГО JSON.
2. Не пиши ніякого тексту до чи після JSON.
3. Формат JSON: {"hotels": ["назва першого готелю англійською", "назва другого готелю англійською"]}
4. Якщо не знайдено жодного готелю, поверни: {"hotels": []}
"""

_EXTRACT_PRICES_PROMPT = """Ти — помічник для розрахунку туристичного туру.
З тексту менеджера витягни числові дані і поверни ТІЛЬКИ JSON, без жодного тексту до чи після.

ФОРМАТ:
{
  "adults": <кількість дорослих (ціле число)>,
  "children": <кількість дітей (0 якщо немає)>,
  "nights": <кількість ночей (ціле число)>,
  "check_in_month": <місяць заїзду як число: 1=Січень, 5=Травень, 7=Липень і т.д.>,
  "check_in_day": <день заїзду як число>,
  "flight_per_person": <ціна авіа на 1 особу в євро, або null якщо не вказано>,
  "hotel_prices": [<ціна готелю 1>, <ціна готелю 2>, ...],
  "hotel_stars": [<зірки готелю 1 як число (0 якщо невідомо)>, <зірки готелю 2>, ...],
  "other_per_person": <сума інших витрат (трансфер, екскурсії тощо) на особу, або 0>
}

КРИТИЧНО: Готелі зазвичай перераховані через кому у форматі "Назва - X євро". Знайди їх ВСІ.
Якщо у тексті після слова "КІЛЬКІСТЬ ГОТЕЛІВ:" вказано число N — у "hotel_prices" МАЄ БУТИ РІВНО N чисел.

ПРАВИЛО АВІА:
- "X євро квиток/білет", "авіа X€", "квитки по X€" → flight_per_person = X (ЗА ОСОБУ)
- "X євро на всіх", "загальна вартість квитків X€" → flight_per_person = X / (adults + children)

ПРАВИЛО ЦІНИ ГОТЕЛЮ:
- Ціна готелю — ЗАВЖДИ загальна вартість номера для ВСІХ людей, НЕ на особу.
- Повертай загальну ціну. Ділити НЕ ПОТРІБНО.

ПРАВИЛО ЗІРОК: "3 зірки"/"3*" → 3. "4 зірки" → 4. Невідомо → 0.
"""

_EXTRACT_FROM_CHUNK_PROMPT = """Ти отримуєш текст менеджера і НЕВЕЛИКИЙ список можливих готелів з бази.
Твоє завдання: повернути ВСІ готелі зі списку, які згадані або дуже схожі на згадані у тексті.

ПРАВИЛА:
- Обирай ТІЛЬКИ назви з наданого списку.
- Якщо в тексті згадано кілька готелів, поверни всі.
- Не вигадуй нових назв.
- Якщо в цьому шматку списку немає відповідників, поверни порожній список.
- Поверни ТІЛЬКИ строгий JSON: {"hotels": ["name 1", "name 2"]}
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

_FORMAT_PROMPT = """Ти — професійний туристичний менеджер.
Твоє завдання: перетворити вхідний текст-чернетку (голосове або текстове повідомлення з містом вильоту, курортом, готелем, харчування та цінами) у вірний шаблон.
Тобі потрібно використовувати виключно список знайдених готелів у нашій базі з правильними назвами англійською та посиланнями.

КРИТИЧНО ВАЖЛИВО: Ти маєш включити у підбірку ВСІ готелі, які перелічені у блоці "ЗНАЙДЕНІ В БАЗІ ГОТЕЛІ". Якщо там 12 готелів — у фінальному тексті має бути 12 пунктів. Не скорочуй список!

ВИМОГИ ДО ФОРМАТУВАННЯ (ЗБЕРІГАЙ ТОЧНО ТАКУ СТРУКТУРУ І ЕМОДЗІ):

Авіатур до [Напрямок] [Прапор країни]
Із [Місто вильоту] [Прапор країни]
🌤️ [Дати туди-назад (див. правило)], [Кількість ночей] ночей
Туди [Час вильоту-прильоту туди]
Назад [Час вильоту-прильоту назад]
🧳 ручна поклажа до 10 кг та розміром 20х40х30 см [це пишеться ЗАВЖДИ за замовчуванням]
   - Якщо менеджер згадав додаткову ручну поклажу/пріоріті — заміни на: 🧳 ручна поклажа до 10 кг + 1 пріоріті
   - Якщо менеджер згадав багаж — заміни на: 🧳 ручна поклажа до 10 кг + 1 багаж до [вага]кг
[Рядок про трансфер - пиши ТІЛЬКИ якщо є інформація в тексті, інакше НЕ ПИШИ НІЧОГО]
[Рядок про екскурсії - пиши ТІЛЬКИ якщо є інформація в тексті, інакше НЕ ПИШИ НІЧОГО]
🏠 варіанти проживання:

ПРАВИЛО ДЛЯ НАПРЯМКУ:
У полі [Напрямок] пиши ПОВНУ, красиву українську назву курорту в правильному відмінку. Уникай русизмів та НІКОЛИ не скорочуй назви.
ВАЖЛИВО — правильний прийменник:
- Якщо це місто або країна (Барселона, Стамбул, Єгипет, ОАЕ) — пиши "ДО": "Авіатур до Барселони 🇪🇸"
- Якщо це курорт або місце відпочинку (Крит, Майорка, Коста-Брава, Халкідікі, Анталія, Ріміні) — пиши "НА": "Авіатур на Крит 🇬🇷"


1) [Правильна назва готелю з бази англійською] [Зірковість ТІЛЬКИ з бази: 3★/4★/5★ або порожньо]
🥑 [тип харчування з маленької літери. Заміни слова: "пенсіон"→"пансіон", "напівпансіон"→"сніданки+вечері", "півпансіон"→"сніданки+вечері" (напр: "все включено", "повний пансіон", "сніданки+вечері", "тільки сніданок")]
[Посилання з бази - тільки прямий URL, без тексту і дужок. Якщо є знак ⚠️, залиш його біля посилання]

2) [Правильна назва готелю 2 англійською] [Зірковість ТІЛЬКИ з бази: 3★/4★/5★ або порожньо]
...

✔️ путівник + тур страхування
🤓 онлайн підтримка 24/7
[РЯДОК З ЦІНАМИ — дивись правило нижче]

❗️Ціна актуальна на момент розрахунку подорожі

[Блок рекомендацій по 2-3 готелям, див. правило про рекомендації]

ПРАВИЛО ЩОДО ЦІН:
Ціни вже РОЗРАХОВАНІ програмою і передані тобі у блоці "РОЗРАХОВАНІ ЦІНИ".
❌ НЕ ПЕРЕРАХОВУЙ ціни самостійно! Бери тільки ті числа що в блоці.
- Якщо в блоці написано "💰 загальна вартість туру за особу" — використовуй САМЕ ЦЕЙ заголовок.
- Якщо в блоці написано "💰 загальна вартість туру" — використовуй САМЕ ЦЕЙ заголовок.
- Якщо в блоці написано "ЦІНА НЕ ВКАЗАНА" — пиши "💰 не вказано" для всіх готелів.
ФОРМАТ рядка з цінами — ЗАВЖДИ в один рядок через кому:
💰 загальна вартість туру за особу - 1)400€, 2)450€, 3)500€, 4)641€
НЕ пиши ціни списком (з нового рядка кожну). ТІЛЬКИ в один рядок через кому.
НЕ використовуй коми, крапки або пробіли як роздільники тисяч! (наприклад: 1043 замість 1,043 або 1 043).

ПРАВИЛО ФОРМАТУВАННЯ ДАТ:
- Якщо дати в одному місяці: пиши у форматі "17-24 травня" (тільки числа та назва місяця).
- Якщо дати в різних місяцях: пиши у форматі "30.06-06.07" (тільки числа).

ДОДАТКОВО: менеджер також може вказати ціни за
- багаж / додаткову ручну поклажу / пріоріті (формат: 🧳 ручна поклажа до 10 кг + 1 багаж 20кг / 1 додаткова пріоріті)
- трансфер / шаттл-бас (формат: 🚖 Індивідуальний трансфер АБО 🚍 Шаттл-бас door-to-door)
- програму екскурсій (формат: ⭐️ Повноцінна програма туру)

ПОСИЛАННЯ ПІД ГОТЕЛЯМИ: береш з файлу ексель. На кожній сторінці окремий напрямок та таблиця "Назва - посилання". Використовуй назви та посилання виключно з файлу.

КРИТИЧНЕ ПРАВИЛО ПО ЗІРКАХ (БЕЗ ВИНЯТКІВ):
- Показуй зірки ЛИШЕ якщо в базі готелю явно вказано 3, 4 або 5 зірок → тільки як 3★ / 4★ / 5★.
- Якщо в базі 1 або 2 зірки — НЕ пиши зірки взагалі (порожньо).
- Якщо в базі не вказано зірковість — НЕ пиши зірки взагалі (порожньо).
- НІКОЛИ не вигадуй і не змінюй зірковість самостійно.

- Якщо біля готелю вказано "Посилання відсутнє..." — ОБОВ'ЯЗКОВО додай цей готель до списку проживання у підбірці (без посилання) та розрахуй для нього ціну! НІКОЛИ не видаляй готелі зі списку. Після підбірки (в кінці) додай примітку: "⚠️ Увага, менеджер: посилання для [Назва готелю] не знайдено в базі, перевірте вручну."

- Якщо є позначка ⚠️ (низька точність збігу назви готелю) — все одно вставляй його у підбірку, але після підбірки додай: "⚠️ Увага, менеджер: назва готелю [Назва] була розпізнана з низькою точністю. Будь ласка, перевірте чи правильно підібрано готель з бази."

РЕКОМЕНДАЦІЇ ПО ГОТЕЛЯМ:
Окремо після кожної підбірки обирай ТІЛЬКИ 2-3 найцікавіших готелі і надавай рекомендації лише по ним. Ці рекомендації — це ГОТОВИЙ ТЕКСТ, який менеджер може скопіювати і відправити клієнту. Пиши від першої особи, як ніби менеджер звертається до клієнта (наприклад: "Хочу звернути вашу увагу на готель...").
Опис готелів та їх рекомендації необхідно робити на основі: реальних відгуків Trip Advisor / Booking та аналогів, брати до уваги локацію, курорт, відстань до пляжу, вид пляжу (пісок / галька), роки реставрації, якість харчування, кількість басейнів, розмір території, якість обслуговування. 
НАЙГОЛОВНІШЕ: Пиши МАКСИМАЛЬНО ЖИВОЮ, ЛЮДСЬКОЮ МОВОЮ. Рекомендації мають бути дійсно крутими, цікавими і продаючими, ніби їх пише досвідчений турагент-експерт, який сам там був. Уникай сухих енциклопедичних фактів, додавай емоцій, але при цьому вся інформація має бути достовірною, не придумуй того, чого в готелі немає. РОБИ ОПИСИ УНІКАЛЬНИМИ.
ВАЖЛИВО: кожну рекомендацію (по кожному готелю) відділяй ПОРОЖНІМ РЯДКОМ від наступної. Не пиши всі рекомендації суцільним текстом — кожен готель з нового абзацу через рядок.

ВАЖЛИВО: Використовуй ПРАВИЛЬНІ назви готелів АНГЛІЙСЬКОЮ та посилання ТІЛЬКИ з наданого списку (БАЗА ГОТЕЛІВ). Основний текст пиши УКРАЇНСЬКОЮ.
"""

def calculate_tour_prices(hotel_prices: list, flight_per_person: float,
                          other_per_person: float, total_people: int,
                          has_children: bool, tourist_tax_per_person: float = 0) -> list:
    """Calculate final tour prices in Python — deterministic, no LLM math."""
    results = []
    for hotel_total in hotel_prices:
        hotel_per_person = hotel_total / total_people
        cost = hotel_per_person + flight_per_person + other_per_person + tourist_tax_per_person
        if cost < 350:
            final_per_person = cost + 150
        else:
            final_per_person = cost * 1.43
        final_per_person = round(final_per_person) + 5  # always +5€ per person
        if has_children:
            results.append(round(final_per_person * total_people))
        else:
            results.append(round(final_per_person))
    return results


async def extract_prices_from_text(user_text: str, fast_models: list) -> dict:
    """Extract numeric tour data from manager's text via LLM."""
    for model in fast_models:
        try:
            resp = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _EXTRACT_PRICES_PROMPT},
                    {"role": "user", "content": user_text},
                ],
                temperature=0,
                timeout=30,
            )
            raw = resp.choices[0].message.content.strip()
            raw = re.sub(r'```[a-z]*\n?', '', raw).strip('`').strip()
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                data = json.loads(m.group())
                return data
        except Exception as e:
            logger.error(f"Price extraction error with {model}: {e}")
    return {}


async def cleanup_transcribed_text(raw_text: str) -> str:
    """Refine speech-to-text output with LLM while preserving numbers/facts."""
    if not raw_text:
        return raw_text

    models = ["openai/gpt-4o-mini", "google/gemini-2.5-flash"]
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


async def _extract_hotels_by_chunks(user_text: str, candidate_hotels: list, models: list[str]) -> list[str]:
    """
    For large destinations, query the model on smaller hotel chunks and merge matches.
    This is slower, but much more reliable than one huge extraction call.
    """
    if not candidate_hotels:
        return []

    found = []
    chunk_size = 35
    for start in range(0, len(candidate_hotels), chunk_size):
        chunk = candidate_hotels[start:start + chunk_size]
        hotel_names = "\n".join(h["hotel"] for h in chunk)
        prompt_user = (
            f"ТЕКСТ МЕНЕДЖЕРА:\n{user_text}\n\n"
            f"МОЖЛИВІ ГОТЕЛІ З БАЗИ:\n{hotel_names}"
        )
        for model in models:
            try:
                resp = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": _EXTRACT_FROM_CHUNK_PROMPT},
                        {"role": "user", "content": prompt_user},
                    ],
                    temperature=0.0,
                    timeout=30,
                )
                raw = resp.choices[0].message.content.strip()
                raw = re.sub(r'```[a-z]*\n?', '', raw).strip('`').strip()
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m:
                    data = json.loads(m.group())
                    found.extend(data.get("hotels", []))
                    break
            except Exception as e:
                logger.error(f"Chunk extraction error with {model}: {e}")
    return _dedupe_keep_order(found)


def fuzzy_match_hotel(hotel_name: str, db: list) -> dict:
    def normalize_name(name: str) -> str:
        # Keep alnum/space only and drop decorative words to improve fuzzy matching.
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
        db_name = normalize_name(h['hotel'])
        if not db_name:
            db_name = h['hotel'].lower()

        score = difflib.SequenceMatcher(None, query, db_name).ratio()
        
        query_words = set(re.findall(r'\w+', query))
        db_words = set(re.findall(r'\w+', db_name))
        
        if query_words and query_words.issubset(db_words):
            score += 0.3

        overlap = len(query_words & db_words)
        if overlap >= 2:
            score += 0.2
        elif overlap == 1:
            score += 0.1
            
        if score > max_score:
            max_score = score
            best_match = h
            
    if best_match and max_score >= 0.5:
        if max_score < 0.7:
            return {"hotel": best_match["hotel"], "link": best_match["link"] + " ⚠️ (Низька точність збігу)"}
        return best_match
        
    return {"hotel": hotel_name, "link": "Посилання відсутнє (готель не знайдено в базі)"}


def _build_hotel_candidates(user_text: str, relevant_hotels: list, limit: int = 140) -> list:
    """
    Reduce huge destination lists (e.g. 500+ hotels) to likely candidates.
    This makes hotel extraction significantly more stable for large sheets.
    """
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
    top = [h for _, h in scored[:limit]]

    # Safety net: keep at least some part of original list diversity.
    if not top:
        return relevant_hotels[:limit]
    return top


def _pick_destination_by_keywords(user_text: str, destinations: list) -> str | None:
    text = user_text.lower()
    for key, normalized_dest in _DESTINATION_ALIASES.items():
        if key in text:
            for d in destinations:
                if normalized_dest in d.lower():
                    return d
    return None


def _extract_allowed_stars(hotel_name: str) -> str:
    """
    Return display stars strictly by business rule:
    - only 3/4/5 are shown (as 3★/4★/5★)
    - 1/2 or unknown -> empty string
    """
    m = re.search(r'(?<!\d)([1-5])\s*(?:\*|★)?\s*$', hotel_name.strip())
    if not m:
        return ""
    stars = int(m.group(1))
    return f"{stars}★" if stars in (3, 4, 5) else ""

def _inject_links(text: str, hotel_link_map: dict) -> str:
    """Post-process the LLM output to ensure correct links under each hotel.
    Uses word-overlap matching so partial hotel names still resolve correctly.
    """
    lines = text.split('\n')
    i = 0
    while i < len(lines):
        if '🥑' in lines[i] and i > 0:
            hotel_line = lines[i - 1].lower()
            line_words = set(re.findall(r'\w+', hotel_line))

            # Pick best matching hotel by word overlap ratio
            best_name, best_score = None, 0.0
            for h_name, link in hotel_link_map.items():
                h_words = set(re.findall(r'\w+', h_name.lower()))
                if not h_words:
                    continue
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
                        # Replace hallucinated / wrong link
                        if 'http' in nxt and link and 'http' in link and link.split()[0] not in nxt:
                            lines[next_i] = link
                        i += 1
                        continue
                lines.insert(i + 1, link)
                i += 1  # skip inserted line
        i += 1
    return '\n'.join(lines)


def _count_listed_hotels(text: str) -> int:
    """Count numbered hotel items in the final formatted message."""
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
    """
    Enforce the exact price line calculated in Python.
    Replaces any LLM-generated price line or inserts it near the support block.
    NOTE: We do NOT truncate computed_prices to listed_hotels because the LLM
    may have skipped hotels — the append_missing_hotels step will add them back.
    """
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
    """
    If the LLM wrote fewer hotel entries than we have matched_hotels,
    append the missing ones before the ✔️ block so the message is complete.
    """
    listed = _count_listed_hotels(text)
    total = len(matched_hotels)
    if listed >= total:
        return text

    lines = text.split("\n")
    # Find the insertion point — just before ✔️ or 💰 line
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


async def format_tour_message(user_text: str) -> str:
    db = get_hotel_db()
    destinations = list(db.keys())

    selected_dest = _pick_destination_by_keywords(user_text, destinations)
    # gemini-2.5-flash is smarter at following long lists — use it first everywhere
    fast_models = [
        "google/gemini-2.5-flash",
        "openai/gpt-4o-mini",
    ]
    smart_models = [
        "google/gemini-2.5-flash",
        "openai/gpt-4o-mini",
    ]
    
    try:
        if not selected_dest:
            dest_content = f"ТЕКСТ:\n{user_text}\n\nДОСТУПНІ НАПРЯМКИ:\n{', '.join(destinations)}"
            model_name = ""
            for mod in fast_models:
                try:
                    resp = await client.chat.completions.create(
                        model=mod,
                        messages=[
                            {"role": "system", "content": _DESTINATION_PROMPT},
                            {"role": "user", "content": dest_content},
                        ],
                        temperature=0.0,
                        timeout=30,
                    )
                    model_name = resp.choices[0].message.content.strip().lower()
                    # Remove emojis from model_name
                    model_name = re.sub(r'[^\w\s-]', '', model_name).strip()
                    if model_name:
                        break
                except Exception as e:
                    logger.error(f"Dest model {mod} failed: {e}")
                    
            for d in destinations:
                if d.lower() in model_name or model_name in d.lower():
                    selected_dest = d
                    break
                
            if not selected_dest:
                logger.info(f"Fuzzy match failed for '{model_name}', trying strict LLM pick")
                picked = ""
                for mod in fast_models:
                    try:
                        pick_resp = await client.chat.completions.create(
                            model=mod,
                            messages=[
                                {"role": "system", "content": "Ти маєш повернути тільки одну точну назву з наданого списку, яка найкраще підходить під запит. Без жодних інших слів."},
                                {"role": "user", "content": f"Запит: {model_name}\n\nСписок: {', '.join(destinations)}"}
                            ],
                            temperature=0.0,
                            timeout=30,
                        )
                        picked = pick_resp.choices[0].message.content.strip().lower()
                        if picked:
                            break
                    except Exception as e:
                        logger.error(f"Pick model {mod} failed: {e}")
                for d in destinations:
                    if d.lower() == picked or picked in d.lower() or d.lower() in picked:
                        selected_dest = d
                        break
    except Exception as e:
        logger.error(f"Destination extraction error: {e}")
        
    if not selected_dest and destinations:
        selected_dest = destinations[0]
        
    logger.info(f"Selected destination: '{selected_dest}'")
    relevant_hotels = db.get(selected_dest, [])
    candidate_hotels = _build_hotel_candidates(user_text, relevant_hotels, limit=140)
    db_names = "\n".join([h['hotel'] for h in candidate_hotels])
    
    extraction_content = (
        f"ТЕКСТ ВІД МЕНЕДЖЕРА:\n{user_text}\n\n"
        f"ОБРАНИЙ НАПРЯМОК: {selected_dest}\n\n"
        f"СПИСОК ГОТЕЛІВ НАПРЯМКУ (база):\n{db_names}"
    )
    
    # ── Run hotel-name extraction and price extraction IN PARALLEL ──────────
    async def _do_extract_hotels() -> list:
        for model in fast_models:
            try:
                resp = await client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": _EXTRACT_PROMPT},
                        {"role": "user", "content": extraction_content},
                    ],
                    temperature=0.0,
                    timeout=45,
                )
                raw = resp.choices[0].message.content.strip()
                raw = re.sub(r'```[a-z]*\n?', '', raw).strip('`').strip()
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m:
                    data = json.loads(m.group())
                    hotels = data.get("hotels", [])
                    if hotels:
                        return hotels
            except Exception as e:
                logger.error(f"Hotel extraction error with {model}: {e}")
        return []

    # Hint the price LLM how many hotels to expect so it doesn't stop early
    hotel_count_hint = ""
    rough_count = len(re.findall(r'(?:євро|euro|€)', user_text, re.IGNORECASE)) - 1
    if rough_count > 0:
        hotel_count_hint = f"\n\nКІЛЬКІСТЬ ГОТЕЛІВ: приблизно {rough_count}"
    price_text = user_text + hotel_count_hint

    extracted_hotels, price_data = await asyncio.gather(
        _do_extract_hotels(),
        extract_prices_from_text(price_text, fast_models),
    )
    logger.info(f"Extracted hotels: {len(extracted_hotels)}, price data: {price_data}")

    expected_hotels = len(price_data.get("hotel_prices", [])) if price_data else 0
    # Always run chunk extraction and merge — guarantees we don't miss any hotel
    models_to_try = fast_models
    chunk_hotels = await _extract_hotels_by_chunks(user_text, candidate_hotels, models_to_try)
    extracted_hotels = _dedupe_keep_order(extracted_hotels + chunk_hotels)
    logger.info(f"After chunk merge: {len(extracted_hotels)} hotels (expected {expected_hotels}")
        
    matched_info = []
    hotel_link_map = {}  # hotel_name -> link for post-processing
    all_hotels = [hotel for hotels in db.values() for hotel in hotels]
    matched_hotels = []
    seen_hotels = set()
    for h_name in extracted_hotels:
        match = fuzzy_match_hotel(h_name, candidate_hotels)
        if "Посилання відсутнє" in match["link"]:
            # If shortlist missed it, retry on full destination sheet.
            match = fuzzy_match_hotel(h_name, relevant_hotels)
        if "Посилання відсутнє" in match["link"] and all_hotels:
            # Fallback: destination might be misdetected, try matching globally.
            global_match = fuzzy_match_hotel(h_name, all_hotels)
            if "Посилання відсутнє" not in global_match["link"]:
                match = global_match

        dedupe_key = match["hotel"].strip().lower()
        if dedupe_key in seen_hotels:
            continue
        seen_hotels.add(dedupe_key)
        matched_hotels.append(match)

        stars_for_display = _extract_allowed_stars(match["hotel"])
        stars_text = stars_for_display if stars_for_display else "не вказувати"
        matched_info.append(
            f"- Назва: {match['hotel']}, Зірки: {stars_text}, Посилання: {match['link']}"
        )
        hotel_link_map[match['hotel'].lower()] = match['link']
        logger.info(f"Matched: '{h_name}' -> '{match['hotel']}' link={'YES' if match['link'] and 'відсутнє' not in match['link'] else 'NO'}")
        
    db_text = "\n".join(matched_info) if matched_info else "Не вдалося витягнути готелі."

    tax_info = get_tourist_tax_db()

    # --- Python price calculation ---
    
    prices_block = ""
    price_label = "💰 загальна вартість туру за особу"
    computed_prices = []
    if price_data and price_data.get("hotel_prices") and price_data.get("flight_per_person") is not None:
        adults = int(price_data.get("adults", 2))
        children = int(price_data.get("children", 0))
        total_people = adults + children
        has_children = children > 0
        flight = float(price_data.get("flight_per_person", 0))
        other = float(price_data.get("other_per_person", 0))
        hotel_prices = [float(p) for p in price_data.get("hotel_prices", [])]
        
        nights = int(price_data.get("nights", 7))
        check_in_month = int(price_data.get("check_in_month", 6))
        hotel_stars_list = price_data.get("hotel_stars", [])

        max_items = len(matched_hotels) if matched_hotels else len(hotel_prices)
        hotel_prices = hotel_prices[:max_items]
        hotel_stars_list = hotel_stars_list[:max_items]

        computed = []
        for idx, hotel_total in enumerate(hotel_prices):
            stars = int(hotel_stars_list[idx]) if idx < len(hotel_stars_list) else 0
            tax_per_night = get_tax_per_person_per_night(
                destination=selected_dest or "",
                stars=stars,
                month=check_in_month,
                num_people=total_people,
            )
            tax_per_person = tax_per_night * nights
            hotel_per_person = hotel_total / total_people
            cost = hotel_per_person + flight + other + tax_per_person
            if cost < 350:
                final_per_person = cost + 150
            else:
                final_per_person = cost * 1.43
            final_per_person = round(final_per_person) + 5
            if has_children:
                computed.append(round(final_per_person * total_people))
            else:
                computed.append(round(final_per_person))
            logger.info(f"Hotel {idx+1}: hotel/pp={hotel_per_person:.1f} flight={flight} tax/pp={tax_per_person:.2f} cost={cost:.1f} → {computed[-1]}€")
        
        prices_str = ", ".join([f"{i+1}){p}€" for i, p in enumerate(computed)])
        if has_children:
            price_label = "💰 загальна вартість туру"
        else:
            price_label = "💰 загальна вартість туру за особу"
        computed_prices = computed
        prices_block = f"\n\nРОЗРАХОВАНІ ЦІНИ (вже підраховані Python-програмою, ВИКОРИСТОВУЙ ТІЛЬКИ ЦІ ЧИСЛА, не перераховуй):\n{price_label} - {prices_str}"
    else:
        prices_block = "\n\nЦІНА НЕ ВКАЗАНА: у блоці 💰 напиши 'не вказано' для всіх готелів."

    # Build explicit numbered hotel list so LLM cannot skip any entry
    numbered_hotels_lines = []
    for idx, h in enumerate(matched_hotels):
        stars_str = _extract_allowed_stars(h['hotel'])
        stars_display = f" {stars_str}" if stars_str else ""
        numbered_hotels_lines.append(
            f"{idx + 1}. {h['hotel']}{stars_display} | {h['link']}"
        )
    numbered_hotels_block = "\n".join(numbered_hotels_lines)

    combined = (
        f"ТЕКСТ ВІД МЕНЕДЖЕРА:\n{user_text}\n\n"
        f"ЗНАЙДЕНІ В БАЗІ ГОТЕЛІ (ВСЬОГО {len(matched_hotels)} — ВКЛЮЧИ ВСІ {len(matched_hotels)} У ПІДБІРКУ, нумерація починається з 1):\n"
        f"{numbered_hotels_block}\n\n"
        f"ДЕТАЛЬНА ІНФОРМАЦІЯ ГОТЕЛІВ (назва, зірки, посилання):\n{db_text}"
        f"{prices_block}"
    )
    
    models_for_format = smart_models
    for model in models_for_format:
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": _FORMAT_PROMPT},
                    {"role": "user", "content": combined},
                ],
                temperature=0,
                timeout=120,
            )
            result_text = response.choices[0].message.content.strip()
            result_text = re.sub(r'<math>.*?</math>', '', result_text, flags=re.DOTALL).strip()
            result_text = _inject_links(result_text, hotel_link_map)
            # First append any hotels the LLM missed, then enforce prices
            result_text = _append_missing_hotels(result_text, matched_hotels, computed_prices)
            result_text = _inject_prices(result_text, price_label, computed_prices)
            return result_text
        except Exception as e:
            logger.error(f"LLM Error with {model}: {e}")
            
    return "❌ Помилка генерації тексту. Можливо, вказані моделі недоступні."

async def transcribe_voice(file_bytes: bytes) -> str:
    if not GROQ_API_KEY:
        return "❌ Немає GROQ_API_KEY для транскрипції."
    url = "https://api.groq.com/openai/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
    files = {"file": ("voice.ogg", file_bytes, "audio/ogg")}
    data = {"model": "whisper-large-v3"}
    
    async with httpx.AsyncClient() as c:
        try:
            resp = await c.post(url, headers=headers, files=files, data=data, timeout=30)
            if resp.status_code == 200:
                return resp.json().get("text", "")
            else:
                logger.error(f"Groq error: {resp.text}")
                return "❌ Помилка розпізнавання."
        except Exception as e:
            logger.error(f"Voice error: {e}")
            return "❌ Мережева помилка."
