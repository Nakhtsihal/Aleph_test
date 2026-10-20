import httpx
import os


async def fetch_logs(since, until):
    params = {
        "since": since,
        "until": until
    }
    headers = {
        "X-API-Key": "access_key_1"
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(
            os.getenv("ALEPH_API_URL", "http://127.0.0.1:8000/aleph/get_logs"),
            params=params,
            headers=headers
        )
        response.raise_for_status()
        logs = response.json()
    return logs
