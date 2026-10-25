import asyncio
import json
import redis.asyncio as redis
import os
from typing import Any


def load_enrichment() -> dict[str, dict[str, Any]]:
    with open("data/enrichment.json") as file:
        enrichment = json.load(file)
    return enrichment


async def main() -> None:
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True
    )
    try:
        enrichment = await asyncio.to_thread(load_enrichment)
        for key, value in enrichment.items():
            metadata_json = json.dumps(value)
            await client.set(key, metadata_json)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())