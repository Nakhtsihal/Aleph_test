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
from parser import RegularLogParser, SysLogParser
from enricher import Enricher
from retriever import LogRetriever
from nats_publisher import NatsPublisher, setup_jetstream
from typing import Any
from logging_config import setup_logging

logger = logging.getLogger(__name__)


class LogProcessor:
    def __init__(self) -> None:
        self.poll_interval = int(os.getenv("POLL_INTERVAL", "30"))

        if self.poll_interval <= 0:
            raise ValueError("Polling interval must be greater than 0")

        self.enricher = Enricher()
        self.nats_client: Client | None = None
        self.jetstream: JetStreamContext | None = None
        self.since = int(os.getenv("START_TIMESTAMP", str(int(time.time()))))
        self.regular_parser = RegularLogParser()
        self.syslog_parser = SysLogParser()
        self.retriever = LogRetriever()
        self.publisher: NatsPublisher | None = None

    async def start(self) -> None:
        self.nats_client = await nats.connect(os.getenv("NATS_URL", "nats://localhost:4222"))
        self.jetstream = await setup_jetstream(self.nats_client)
        self.publisher = NatsPublisher(self.jetstream)

        logger.info("Processor started",
                    extra={"action": "processor_start", "status": "success"},
        )

    async def close(self) -> None:
        try:
            if self.nats_client is not None:
                await self.nats_client.drain()
        finally:
            try:
                await self.enricher.close_connection()
            finally:
                await self.retriever.close_connection()
        logger.info("Processor connections closed",
                    extra={"action": "close_connections", "status": "success"})

    async def run(self) -> None:
        if self.publisher is None:
            raise RuntimeError("Call start() before run()")
        while True:
            until = int(time.time())
            try:
                logs = await self.retriever.fetch_logs(self.since, until)
            except httpx.HTTPError as error:
                logger.warning(
                    "API request error: %s",
                    error,
                    extra={"action": "fetch_logs", "status": "error"}
                )
                await asyncio.sleep(self.poll_interval)
                continue

            tasks = []

            for log in logs:
                if isinstance(log, str):
                    parsed_log = self.syslog_parser.parse(log)
                else:
                    parsed_log = self.regular_parser.parse(log)

                enriched_log = self.enricher.enrich_log(parsed_log)
                tasks.append(enriched_log)

            enriched_logs = await asyncio.gather(*tasks)

            for log in enriched_logs:
                await self.publisher.publish_log(log)

            await asyncio.to_thread(save_logs, enriched_logs)
            if enriched_logs:
                logger.info("Published and saved %s logs",
                    len(enriched_logs),
                     extra={"action": "process_batch", "status": "success"}
                )
            self.since = until
            await asyncio.sleep(self.poll_interval)


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
        logger.info(
            "Processor shutdown requested",
            extra={"action": "processor_shutdown", "status": "requested"}
        )
        raise
    finally:
        try:
            await processor.close()
        finally:
            loop.remove_signal_handler(signal.SIGTERM)

if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Processor stopped by user",
                    extra={"action": "processor_stop", "status": "success"})
    except asyncio.CancelledError:
        logger.info("Processor stopped",
                    extra={"action": "processor_stop", "status": "success"})
    except Exception:
        logger.exception(
            "Processor failed",
            extra={"action": "processor_run", "status": "error"}
        )
        raise SystemExit(1)
