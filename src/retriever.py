import httpx
import os
from typing import Any


class LogRetriever:
    def __init__(
            self,
            url: str | None = None,
            api_key: str | None = None) -> None:

        if url is None:
            url = os.getenv(
                "ALEPH_API_URL",
                "http://127.0.0.1:8000/aleph/get_logs",
            )
        if api_key is None:
            api_key = os.getenv(
                "ALEPH_API_KEY",
                "access_key_1")

        self._url = url
        self._client = httpx.AsyncClient(headers={"X-API-Key": api_key})

    async def fetch_logs(
            self,
            since: int,
            until: int) -> list[dict[str, Any] | str]:

        params = {
            "since": since,
            "until": until
        }

        response = await self._client.get(self._url, params=params)
        response.raise_for_status()
        logs = response.json()
        return logs

    async def close_connection(self) -> None:
        await self._client.aclose()
