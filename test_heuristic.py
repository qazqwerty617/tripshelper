
import re

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

test_text = """Майорка на двох дорослих та дитину 2 роки, харчування сніданки , з Берліну на 15.06-25.06, туди 22:10-00:55(+1), назад 15:35-18:20, вартість авіа 247 євро, 1 готель -BJ Playamar Hotel & Apartamentos - 1259 євро, 2 готель - Hotel HSM Canarios Park - 1450 євро, 3 готель - BLUESEA Costa Verde  - 1566 євро, 4 готель - BLUESEA Cala Millor - 1699 євро, 5 готель - AzuLine Hotel Bahamas & Bahamas II - 1674 євро, 6 готель - BLUESEA Mediodia - 1879 євро, 7 готель - BLUESEA Gran Playa - 1881 євро,  8 готель - Globales Isabel - 1980 євро"""

print(f"Detected count: {_count_potential_hotels(test_text)}")
