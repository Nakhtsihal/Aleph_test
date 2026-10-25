import asyncio
import json
import nats
import time
import os
import httpx
import logging
import signal
from nats.aio.client import Client
from nats.js.client import JetStreamContext
from parser import parse_syslog, parse_regular_log
from enricher import Enricher
from retriever import fetch_logs
from nats_publisher import publish_log, setup_jetstream
from typing import Any

logger = logging.getLogger(__name__)


class LogProcessor:
    def __init__(self) -> None:
        self.enricher = Enricher()
        self.nc: Client | None = None
        self.js: JetStreamContext | None = None
        self.since = int(os.getenv("START_TIMESTAMP", str(int(time.time()))))

    async def start(self) -> None:
        self.nc = await nats.connect(os.getenv("NATS_URL", "nats://localhost:4222"))
        self.js = await setup_jetstream(self.nc)
        logger.info("Processor started")

    async def close(self) -> None:
        try:
            if self.nc is not None:
                await self.nc.drain()
        finally:
            await self.enricher.client.aclose()
        logger.info("Processor connections closed")

    async def run(self) -> None:
        if self.js is None:
            raise RuntimeError("Call start() before run()")
        while True:
            until = int(time.time())
            try:
                logs = await fetch_logs(self.since, until)
            except httpx.HTTPError as error:
                logger.warning("API request error: %s", error)
                await asyncio.sleep(30)
                continue

            tasks = []

            for log in logs:
                if isinstance(log, str):
                    parsed_log = parse_syslog(log)
                else:
                    parsed_log = parse_regular_log(log)

                enriched_log = self.enricher.enrich_log(parsed_log)
                tasks.append(enriched_log)

            enriched_logs = await asyncio.gather(*tasks)

            for log in enriched_logs:
                await publish_log(self.js, log)

            await asyncio.to_thread(save_logs, enriched_logs)
            if enriched_logs:
                logger.info("Published and saved %s logs", len(enriched_logs))
            self.since = until
            await asyncio.sleep(30)


def save_logs(logs: list[dict[str, Any]]) -> None:
    if not logs:
        return

    path = "data/output.json"
    temporary_path = path + ".tmp"

    try:
        with open(path, "r") as file:
            existing_logs = json.load(file)
    except FileNotFoundError:
        existing_logs = []

    existing_logs.extend(logs)

    with open(temporary_path, "w") as file:
        json.dump(existing_logs, file, indent=4)

    os.replace(temporary_path, path)


async def main() -> None:
    processor = LogProcessor()
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()

    if task is not None:
        loop.add_signal_handler(signal.SIGTERM, task.cancel)

    try:
        await processor.start()
        await processor.run()
    except asyncio.CancelledError:
        logger.info("Processor shutdown requested")
        raise
    finally:
        try:
            await processor.close()
        finally:
            loop.remove_signal_handler(signal.SIGTERM)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Processor stopped by user")
    except asyncio.CancelledError:
        logger.info("Processor stopped")