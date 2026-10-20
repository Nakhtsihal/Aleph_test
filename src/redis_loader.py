import asyncio
import json
import redis.asyncio as redis
import os


# async def ...
#     ├── создать client
#     ├── открыть enrichment.json
#     └── for ...
#           ├── json.dumps
#           └── await client.set
def load_enrichment():
    with open("data/enrichment.json") as file:
        enrichment = json.load(file)
    return enrichment


async def test_redis():
    client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=6379,
        decode_responses=True
    )
    enrichment = await asyncio.to_thread(load_enrichment)

    for key, value in enrichment.items():
        metadata_json = json.dumps(value) # берём строку
        await client.set(key, metadata_json)

    print("Loaded")
    await client.aclose()


asyncio.run(test_redis())