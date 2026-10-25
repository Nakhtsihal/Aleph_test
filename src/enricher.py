import json
import asyncio
import redis.asyncio as redis
import os
from typing import Any


class Enricher:
    def __init__(self) -> None:
        self.client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=6379,
            decode_responses=True
        )
        self.semaphore = asyncio.Semaphore(20)

    async def enrich_log(self, parsed_log: dict[str, Any]) -> dict[str, Any]:
        client_ip = parsed_log.get("client_ip")
        result = parsed_log.copy()
        result["aleph"] = {
            "source": parsed_log.get("source"),
            "message": parsed_log.get("message"),
        }

        result["findings"] = {}
        if not client_ip:
            return result

        async with self.semaphore:
            metadata_json = await self.client.get(client_ip)

        if metadata_json is not None:
            result["findings"][client_ip] = json.loads(metadata_json)
        return result
