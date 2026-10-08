import json
import asyncio
import redis.asyncio as redis

class Enricher:
    def __init__(self):
        self.client = redis.Redis(
        host="localhost",
        port=6379,
        decode_responses=True
    )
        self.semaphore = asyncio.Semaphore(20)
    async def enrich_log(self, parsed_log):
        client_ip = parsed_log["client_ip"]
        async with self.semaphore: # ограничеваем до 20 задач
            metadata_json = await self.client.get(client_ip)

        if metadata_json is not None:
            metadata = json.loads(metadata_json)
        else:
            metadata = {}

        parsed_log["findings"] = {client_ip: metadata}


        return parsed_log



