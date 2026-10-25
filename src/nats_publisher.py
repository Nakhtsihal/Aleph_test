from nats.js.api import StorageType
from nats.js.errors import NotFoundError
from nats.aio.client import Client
from nats.js.client import JetStreamContext
from typing import Any
import json


async def publish_log(js: JetStreamContext, log: dict[str, Any]) -> None:
    message = json.dumps(log).encode("utf-8")
    await js.publish("enriched_logs", message)


async def setup_jetstream(nc: Client) -> JetStreamContext:
    js = nc.jetstream()
    try:
        await js.stream_info("ENRICHED_LOGS")
    except NotFoundError:
        await js.add_stream(
            name="ENRICHED_LOGS",
            subjects=["enriched_logs"],
            storage=StorageType.FILE
        )
    return js

