import asyncio
import json
import redis.asyncio as redis


# async def ...
#     ├── создать client
#     ├── открыть enrichment.json
#     └── for ...
#           ├── json.dumps
#           └── await client.set
async def test_redis():
    client = redis.Redis(
        host="localhost",
        port=6379,
        decode_responses=True
    )
    with open("../data/enrichment.json") as file:
        enrichment = json.load(file)

    for key, value in enrichment.items():
        metadata_json = json.dumps(value) # берём строку
        await client.set(key, metadata_json)

    print("Loaded")






asyncio.run(test_redis())