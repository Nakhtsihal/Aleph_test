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
            port: int | None = None, ) -> None:

        port = port or int(os.getenv("REDIS_PORT", "6379"))

        if not 1 <= port <= 65535:
            raise ValueError("Redis port must be between 1 and 65535")


        self._redis = redis.Redis(
            host=host or os.getenv("REDIS_HOST", "localhost"),
            port=port,
            decode_responses=True
        )

    async def load_enrichment(self, path: str) -> None:
        enrichment = await asyncio.to_thread(read_enrichment_file, path)
        for key, value in enrichment.items():
            metadata_json = json.dumps(value)
            await self._redis.set(key, metadata_json)

    async def enrich_logs(self, parsed_logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        client_ips = []
        for i in parsed_logs:
            ip = i.get("client_ip")
            if isinstance(ip, str) and ip and ip not in client_ips:
                client_ips.append(ip)

        metadata_by_ip: dict[str, str | None] = {}

        if client_ips:
            # Fetch all IP data at once.
            values = await self._redis.mget(client_ips)
            metadata_by_ip = dict(zip(client_ips, values))

        enriched_logs = []

        for log in parsed_logs:
            result = log.copy()
            result["aleph"] = {
                "source": log.get("source"),
                "message": log.get("message"),
            }
            result["findings"] = {}
            ip = log.get("client_ip")

            if isinstance(ip, str):
                metadata_json = metadata_by_ip.get(ip)

                if metadata_json is not None:
                    result["findings"][ip] = json.loads(metadata_json)

            enriched_logs.append(result)

        return enriched_logs

    async def close_connection(self) -> None:
        await self._redis.aclose()
