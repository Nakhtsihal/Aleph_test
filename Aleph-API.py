from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import APIKeyHeader
from src.parser import SysLogParser
from src.logging_config import setup_logging
from datetime import datetime, timezone
from typing import Any
import json
import asyncio
import logging

setup_logging()
logger = logging.getLogger(__name__)
app = FastAPI()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
syslog_parser = SysLogParser()

access_keys = {
    "access_key_1": "user1",
    "access_key_2": "user2"
}


async def get_api_key(api_key_header: str | None = Depends(api_key_header)) -> str:
    if api_key_header is None:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return api_key_header


async def authenticate_user(api_key: str = Depends(get_api_key)) -> bool:
    if api_key in access_keys:
        return True
    else:
        raise HTTPException(status_code=401, detail="Unauthorized")


def timestamp_to_seconds(value: str | int) -> int:
    try:
        return int(value) // 1000
    except ValueError:
        parsed_time = datetime.fromisoformat(value)

        if parsed_time.tzinfo is None:
            parsed_time = parsed_time.replace(tzinfo=timezone.utc)

        return int(parsed_time.timestamp())


def load_logs() -> list[dict[str, Any] | str]:
    with open("data/ha_logs.json", "r") as file:
        logs = json.load(file)

    with open("data/ha_syslogs.json", "r") as file:
        syslogs = json.load(file)

    logs.extend(syslogs)
    return logs


@app.get("/aleph/get_logs")
async def get_logs(since: int,
                   until: int,
                   authenticated: bool = Depends(authenticate_user)) -> list[dict[str, Any] | str]:
    try:
        logs = await asyncio.to_thread(load_logs)
        filtered_logs = []

        for log in logs:
            if isinstance(log, str):
                parsed_log = syslog_parser.parse(log)
            else:
                parsed_log = log

            timestamp = timestamp_to_seconds(parsed_log["timestamp"])
            if since < timestamp <= until:
                filtered_logs.append(log)
    except Exception:
        logger.error(
            "Failed to retrieve logs",
            extra={"action": "get_logs", "status": "error"}
        )
        raise

    logger.info(
        "Returning %s logs for range (%s, %s]",
        len(filtered_logs),
        since,
        until,
        extra={"action": "get_logs", "status": "success"}
    )
    return filtered_logs
