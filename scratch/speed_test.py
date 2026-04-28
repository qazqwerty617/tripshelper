import asyncio
import logging
import llm_service

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
logging.basicConfig(level=logging.INFO)

async def test_speed():
    sample_text = """
    Майорка на 7 ночей, 2 дорослих. 
    Виліт з Варшави 15 червня. 
    Готелі: Alua Sun Cala Antena - 800 євро, 
    Bahia de Pollensa - 1200 євро. 
    Авіа 200 євро за людину. 
    Трансфер 50 євро.
    """
    
    print("Starting optimization test...")
    import time
    start = time.time()
    
    result = await llm_service.format_tour_message(sample_text)
    
    end = time.time()
    print(f"\n\nRESULT:\n{result}")
    print(f"\n\nTotal time: {end - start:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(test_speed())
