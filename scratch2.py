import asyncio
import logging
logging.basicConfig(level=logging.INFO)
from llm_service import format_tour_message

text = "Летит двое взрослых из Будапешта на Майорку. Так, 300 евро билеты. Значит, летят они в отеле. Hotel Bellavista. 100 евро. HSR Gil. 200 евро. Hotel Corsa Mediterraneo. 300 евро. Hotel Don Pepe. 400. Hotel Bellevue Club. 400. mixmart 500 hotel серомар луна луна парк 495 хотел кассандра 700 хотел джесси хори хори Rizzo 800, Hotel Marble 900, Hotel Tijbidos Plays 2000, Blesa S Bolero 1100"

async def main():
    res = await format_tour_message(text)

asyncio.run(main())
