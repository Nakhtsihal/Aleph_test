import json
import asyncio
import redis.asyncio as redis
import os
from typing import Any


def read_enrichment_file(path: str) -> dict[str, dict[str, Any]]:
    with open(path) as file:
        enrichment = json.load(file)
    return enrichment


class Enricher:
    def __init__(
            self,
            host: str | None = None,
            port: int | None = None,
            concurrency: int | None = None) -> None:

        port = port or int(os.getenv("REDIS_PORT", "6379"))

        concurrency = concurrency or int(os.getenv("ENRICHMENT_CONCURRENCY", "20"))

        if not 1 <= port <= 65535:
            raise ValueError("Redis port must be between 1 and 65535")

        if concurrency <= 0:
            raise ValueError("Enrichment concurrency must be greater than 0")

        self._redis = redis.Redis(
            host=host or os.getenv("REDIS_HOST", "localhost"),
            port=port,
            decode_responses=True
        )

        self.semaphore = asyncio.Semaphore(concurrency)

    async def load_enrichment(self, path: str) -> None:
        enrichment = await asyncio.to_thread(read_enrichment_file, path)
        for key, value in enrichment.items():
            metadata_json = json.dumps(value)
            await self._redis.set(key, metadata_json)

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
            metadata_json = await self._redis.get(client_ip)

        if metadata_json is not None:
            result["findings"][client_ip] = json.loads(metadata_json)
        return result

    async def close_connection(self) -> None:
        await self._redis.aclose()
