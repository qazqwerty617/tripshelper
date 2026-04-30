
import asyncio
import logging
from llm_service import fuzzy_match_hotel, BRANDS

# Mock DB entries
mock_db = [
    {"hotel": "BJ Playamar Hotel & Apartamentos 2*", "link": "http://playamar"},
    {"hotel": "BG Hotel Caballero 4*", "link": "http://caballero"},
    {"hotel": "Cala Millor Garden 4*", "link": "http://garden"},
    {"hotel": "Iberostar Waves Cala Millor 4*", "link": "http://iberostar"},
    {"hotel": "Hotel THB Maria Isabel 4*", "link": "http://maria-isabel"}
]

test_cases = [
    ("BJ Playamar Hotel & Apartamentos", "BJ Playamar Hotel & Apartamentos 2*"),
    ("BLUESEA Cala Millor", "None"), # Should NOT match Cala Millor Garden or Iberostar
    ("Globales Isabel", "None"), # Should NOT match Maria Isabel
]

print(f"Current BRANDS: {sorted(list(BRANDS))}")
print("-" * 50)

for query, expected in test_cases:
    match, score = fuzzy_match_hotel(query, mock_db)
    # The function adds emoji if score is low but > 0.82. If score < 0.82, it returns score 0.0.
    result = match['hotel'] if score > 0 else "None"
    
    # Clean result from non-ascii for printing
    printable_result = result.encode('ascii', 'ignore').decode().strip()
    
    print(f"Query: {query}")
    print(f"Match: {printable_result} (Score: {score:.2f})")
    
    if expected == "None":
        if score == 0:
            print("OK: SUCCESS (Correctly rejected)")
        else:
            print(f"FAIL: Incorrectly matched to {printable_result}")
    else:
        if printable_result.startswith(expected.encode('ascii', 'ignore').decode().strip()):
            print("OK: SUCCESS")
        else:
            print(f"FAIL: Expected {expected}")
    print("-" * 50)
