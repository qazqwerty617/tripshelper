import asyncio
from openai import AsyncOpenAI
import os
from dotenv import load_dotenv

load_dotenv()

client = AsyncOpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1",
)

async def test_model(model):
    print(f"Testing {model}...")
    try:
        resp = await client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": "hi"}],
            timeout=10
        )
        print(f"Success {model}: {resp.choices[0].message.content[:20]}...")
    except Exception as e:
        print(f"Error {model}: {e}")

async def main():
    models = [
        "google/gemini-2.5-flash",
        "google/gemini-2.0-flash-001",
        "google/gemini-flash-1.5",
        "deepseek/deepseek-chat"
    ]
    await asyncio.gather(*[test_model(m) for m in models])

if __name__ == "__main__":
    asyncio.run(main())
