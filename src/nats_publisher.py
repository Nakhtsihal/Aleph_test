from nats.js.api import StorageType
from nats.js.errors import NotFoundError
from nats.aio.client import Client
from nats.js.client import JetStreamContext
from typing import Any
from abc import ABC, abstractmethod
from src import logger
import json



class PublisherInterface(ABC):
    @abstractmethod
    async def publish_log(self, log: dict[str, Any]) -> None:
        raise NotImplementedError()


class NatsPublisher(PublisherInterface):
    def __init__(self, jetstream: JetStreamContext, subject: str = "enriched_logs") -> None:
        self._jetstream = jetstream
        self._subject = subject

    async def publish_log(self, log: dict[str, Any]) -> None:
        message = json.dumps(log).encode("utf-8")
        await self._jetstream.publish(self._subject, message)


async def setup_jetstream(
        nats_client: Client,
        stream_name: str = "enriched_logs",
        subject: str = "enriched_logs",
        max_age: float = 0.0) -> JetStreamContext:

    if max_age < 0:
        raise ValueError("Stream max age must not be negative")

    jetstream = nats_client.jetstream()

    try:
        stream_info = await jetstream.stream_info(stream_name)
    except NotFoundError:
        await jetstream.add_stream(
            name=stream_name,
            subjects=[subject],
            storage=StorageType.FILE,
            max_age=max_age)
    else:

        if stream_info.config.subjects != [subject]:
            raise ValueError("Existing stream subjects do not match requested subject")

        if stream_info.config.max_age != max_age:
            raise ValueError("Existing stream max age does not match requested max age")

        if stream_info.config.storage != StorageType.FILE:
            raise ValueError("Existing stream must use file storage")

    logger.info(
        "JetStream stream %s is ready",
        stream_name,
        extra={"action": "setup_stream", "status": "success"},
    )

    return jetstream
