import openpyxl
import os
import logging
import re
from typing import List, Dict
from config import EXCEL_PATH

logger = logging.getLogger(__name__)

def get_hotel_db() -> Dict[str, List[Dict[str, str]]]:
    if not os.path.exists(EXCEL_PATH):
        logger.warning(f"Excel file not found at {EXCEL_PATH}")
        return {}

    try:
        wb = openpyxl.load_workbook(EXCEL_PATH)
        db = {}

        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            normalized_sheet = sheet_name.strip().lower()
            hotels = []

            for row in ws.iter_rows():
                hotel_name = ""
                link = ""

                for cell in row:
                    if cell.value is None:
                        continue
                    val = str(cell.value).strip()

                    # Extract URL: check hyperlink first, then text
                    cell_link = ""
                    if cell.hyperlink:
                        target = cell.hyperlink.target
                        if target and (target.startswith("http") or target.startswith("www")):
                            cell_link = target
                    if not cell_link and (val.startswith("http") or val.startswith("www")):
                        cell_link = val
                    if not cell_link and val.startswith("=HYPERLINK"):
                        match = re.search(r'=HYPERLINK\("([^"]+)"', val)
                        if match:
                            cell_link = match.group(1)

                    if cell_link:
                        link = cell_link
                    elif len(val) > 2 and not val.isdigit():
                        hotel_name = val

                if hotel_name:
                    hotels.append({"hotel": hotel_name, "link": link})

            if hotels and "податок" not in normalized_sheet:
                db[normalized_sheet] = hotels

        return db

    except Exception as e:
        logger.error(f"Error reading excel: {e}")
        return {}


def format_hotel_db_for_prompt() -> str:
    db = get_hotel_db()
    if not db:
        return "База готелів порожня або файл не знайдено."

    lines = []
    for sheet, hotels in db.items():
        for h in hotels:
            lines.append(f"Назва: {h['hotel']} | Посилання: {h['link']}")
    return "\n".join(lines)


def get_tourist_tax_db() -> str:
    if not os.path.exists(EXCEL_PATH):
        return ""
    try:
        wb = openpyxl.load_workbook(EXCEL_PATH)
        tax_md = []
        for sheet_name in wb.sheetnames:
            if "ПОДАТОК" in sheet_name.upper():
                ws = wb[sheet_name]
                for row in ws.iter_rows(min_row=1, max_row=50, values_only=True):
                    if not any(row): continue

                    # Check if it's a title row or notes row
                    if row[2] and not row[3] and not row[5]:
                        tax_md.append(f"**{str(row[2]).strip()}**")
                        continue

                    # Table header or data row — data at [3..11]
                    if row[3] and row[4] and row[5] is not None:
                        resort = str(row[3]).strip()
                        period = str(row[4]).strip()
                        unit = str(row[5]).strip() if row[5] else ""
                        stars_0 = str(row[6]).strip() if row[6] is not None else ""
                        stars_1_2 = str(row[7]).strip() if len(row) > 7 and row[7] is not None else ""
                        stars_3 = str(row[8]).strip() if len(row) > 8 and row[8] is not None else ""
                        stars_4 = str(row[9]).strip() if len(row) > 9 and row[9] is not None else ""
                        stars_5 = str(row[10]).strip() if len(row) > 10 and row[10] is not None else ""
                        who_pays = str(row[11]).strip() if len(row) > 11 and row[11] else ""

                        if resort.upper() == 'КУРОРТ':
                            tax_md.append(f"| {resort} | {period} | {unit} | БЕЗ ЗІРОК | 1-2★ | 3★ | 4★ | 5★ | {who_pays} |")
                            tax_md.append(f"|---|---|---|---|---|---|---|---|---|")
                        else:
                            tax_md.append(f"| {resort} | {period} | {unit} | {stars_0} | {stars_1_2} | {stars_3} | {stars_4} | {stars_5} | {who_pays} |")
                break

        if tax_md:
            return ("ІНФОРМАЦІЯ ПРО ТУРИСТИЧНИЙ ПОДАТОК:\n" + "\n".join(tax_md) +
                    "\n\n❌ КРИТИЧНО ВАЖЛИВО: Якщо напрямку НЕМАЄ в таблицях вище (наприклад: Туреччина, Анталія, Єгипет, ОАЕ, Кіпр, Сонячний Берег, Золоті Піски, Тенеріфе, Гран-Канарія, Фуертевентура, Лансароте, Коста-дель-Соль) — туристичного податку НЕ ІСНУЄ! НЕ ВИГАДУЙ податок! Встанови його рівним 0€.")
        return ""
    except Exception as e:
        logger.error(f"Error reading tourist tax: {e}")
        return ""


# Maps destination keywords to resort name in the tax table
_RESORT_ALIASES = {
    "майорк": "Майорка", "mallorca": "Майорка",
    "ібіц": "Ібіца", "ibiza": "Ібіца",
    "коста-брав": "Коста-Брава",
    "коста-дорад": "Коста-Дорада",
    "мальт": "Мальта", "malta": "Мальта",
    "рімін": "Ріміні", "rimini": "Ріміні",
    "лідо": "Лідо-ді-Єзоло", "jesolo": "Лідо-ді-Єзоло",
    "мадейр": "Мадейра", "madeira": "Мадейра",
    "крит": "Крит", "crete": "Крит",
    "корфу": "Корфу", "corfu": "Корфу",
    "родос": "Родос", "rhodes": "Родос",
    "міконос": "Міконос", "mykonos": "Міконос",
    "халкідік": "Халкідікі", "halkidiki": "Халкідікі",
    "закінтос": "Закінтос", "zakynthos": "Закінтос",
    "тенериф": "Тенеріфе", "tenerife": "Тенеріфе",
}


def _month_in_period(period: str, month: int) -> bool:
    """Check if month (1-12) falls within a period string like '01.05–31.10' or 'цілий рік'."""
    period = period.strip().lower()
    if "цілий" in period:
        return True
    m = re.search(r'(\d{2})\.(\d{2})[–\-](\d{2})\.(\d{2})', period)
    if m:
        s, e = int(m.group(2)), int(m.group(4))
        return (s <= month <= e) if s <= e else (month >= s or month <= e)
    m2 = re.search(r'з\s+\d{2}\.(\d{2})', period)
    if m2:
        return month >= int(m2.group(1))
    m3 = re.search(r'до\s+\d{2}\.(\d{2})', period)
    if m3:
        return month <= int(m3.group(1))
    return False


def get_tax_per_person_per_night(destination: str, stars: int, month: int, num_people: int = 1) -> float:
    """
    Return tourist tax per person per night for given destination/stars/month.
    Columns in Excel: [3]=resort [4]=period [5]=unit [6]=0★ [7]=1-2★ [8]=3★ [9]=4★ [10]=5★
    Returns 0.0 if destination has no tax.
    """
    if not os.path.exists(EXCEL_PATH):
        return 0.0
    try:
        dest_lower = destination.lower()
        resort_name = None
        for key, val in _RESORT_ALIASES.items():
            if key in dest_lower:
                resort_name = val.lower()
                break
        
        # If no alias, use the destination itself as a fallback
        if not resort_name:
            resort_name = destination.strip().lower()

        wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True)
        for sheet_name in wb.sheetnames:
            if "ПОДАТОК" not in sheet_name.upper():
                continue
            ws = wb[sheet_name]
            for row in ws.iter_rows(min_row=1, max_row=100, values_only=True): # Increased max_row
                if not any(row):
                    continue
                if len(row) < 11 or not row[3] or not row[4]:
                    continue
                
                row_resort = str(row[3]).strip().lower()
                if row_resort in ("курорт", "таблиця", "ставки", "курорти", "назва"):
                    continue
                
                # Flexible match: either exact or one contains the other
                if resort_name not in row_resort and row_resort not in resort_name:
                    continue
                
                if not _month_in_period(str(row[4]), month):
                    continue
                
                unit = str(row[5]).strip().lower() if row[5] else ""
                # Col mapping: 0★→6, 1-2★→7, 3★→8, 4★→9, 5★→10
                # UPDATE: 1-2 stars now count as "no stars" (0★) as requested
                col = {0: 6, 1: 6, 2: 6, 3: 8, 4: 9, 5: 10}.get(stars, 9)
                rate_val = row[col] if len(row) > col else None
                
                try:
                    # Clean the rate value (sometimes it might have currency symbols or commas)
                    if isinstance(rate_val, str):
                        rate_val = rate_val.replace(',', '.').replace('€', '').strip()
                    rate = float(rate_val) if rate_val is not None and str(rate_val).strip() != "" else 0.0
                except (ValueError, TypeError):
                    rate = 0.0
                
                # If "за номер / ніч" — divide by people to get per-person rate
                if "номер" in unit and num_people > 0:
                    rate = rate / num_people
                
                logger.info(f"TAX FOUND: {row_resort} {stars}★ month={month} -> {rate}€/person/night")
                return rate
        
        logger.info(f"TAX NOT FOUND for {destination} (resort={resort_name}), stars={stars}, month={month}")
        return 0.0
    except Exception as e:
        logger.error(f"Tax lookup error: {e}")
        return 0.0
