import asyncio
import json
import nats
import time
import os
import httpx

from parser import parse_syslog, parse_regular_log
from enricher import Enricher
from nats_publisher import publish_log
from retriever import fetch_logs


def save_logs(logs):
    if not logs:
        return

    path = "data/output.json"
    temporary_path = path + ".tmp"

    try:
        with open(path, "r") as file:
            existing_logs = json.load(file)
    except FileNotFoundError:
        existing_logs = []

    existing_logs.extend(logs)                # добавляет каждый лог отдельно

    with open(temporary_path, "w") as file:
        json.dump(existing_logs, file, indent=4)

    os.replace(temporary_path, path)

async def main():

    enricher = Enricher()
    nc = await nats.connect(os.getenv("NATS_URL", "nats://localhost:4222"))
    since = int(os.getenv("START_TIMESTAMP", str(int(time.time()))))
    try:
        while True:
            until = int(time.time())
            try:
                logs = await fetch_logs(since, until)
            except httpx.HTTPError as error:
                print("API request error:", error)
                await asyncio.sleep(30)
                continue

            tasks = []

            for log in logs:
                if isinstance(log, str):
                    parsed_log = parse_syslog(log)
                else:
                    parsed_log = parse_regular_log(log)

                enriched_log = enricher.enrich_log(parsed_log)
                tasks.append(enriched_log)

            enriched_logs = await asyncio.gather(*tasks)

            for log in enriched_logs:
                await publish_log(nc, log)

            await asyncio.to_thread(save_logs, enriched_logs)
            print(len(enriched_logs))

            since = until
            await asyncio.sleep(30)

    finally:
        await nc.drain()
        await enricher.client.aclose()

asyncio.run(main())