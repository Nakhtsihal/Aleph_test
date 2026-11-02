import asyncio
import nats
import os
import logging
from nats.aio.msg import Msg
from nats.aio.client import Client
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError
from logging_config import setup_logging


logger = logging.getLogger(__name__)


class NatsSubscriber:
    def __init__(
            self,
            url: str | None = None,
            subject: str = "enriched_logs",
            stream_name: str = "enriched_logs",
            durable_name: str = "log_viewer",
            ack_wait: float = 30.0,
            max_ack_pending: int = 1000) -> None:

        if url is None:
            url = os.getenv("NATS_URL", "nats://localhost:4222")

        if ack_wait <= 0:
            raise ValueError("Acknowledgement wait must be greater than 0")

        if max_ack_pending <= 0:
            raise ValueError("Maximum pending acknowledgements must be greater than 0")

        self.url = url
        self.subject = subject
        self.stream_name = stream_name
        self.durable_name = durable_name
        self.nats_client: Client | None = None
        self.ack_wait = ack_wait
        self.max_ack_pending = max_ack_pending

    async def start(self) -> None:
        self.nats_client = await nats.connect(self.url, error_cb=self.error_handler,)

    async def message_handler(self, msg: Msg) -> None:
        logger.info(
            "Received log: %s",
            msg.data.decode(),
            extra={"action": "receive_log", "status": "success"})

        await msg.ack()

    async def run(self) -> None:

        if self.nats_client is None:
            raise RuntimeError("Call the start() before run()")
        jetstream = self.nats_client.jetstream()
        try:
            consumer_info = await jetstream.consumer_info(
                self.stream_name,
                self.durable_name
            )
        except NotFoundError:
            pass

        else:
            if consumer_info.config.ack_wait != self.ack_wait:
                raise ValueError(
                    "Existing consumer ack wait does not match requested ack wait"
                )

            if consumer_info.config.max_ack_pending != self.max_ack_pending:
                raise ValueError(
                    "Existing consumer max ack pending does not match requested value"
                )

        await jetstream.subscribe(
            self.subject,
            stream=self.stream_name,
            durable=self.durable_name,
            cb=self.message_handler,
            manual_ack=True,
            config=ConsumerConfig(
                ack_wait=self.ack_wait,
                max_ack_pending=self.max_ack_pending
            )
        )
        logger.info("Subscriber started",
                    extra={"action": "subscriber_start", "status": "success"})

        await asyncio.Event().wait()

    async def close_connection(self) -> None:
        if self.nats_client is not None:
            await self.nats_client.drain()
            logger.info(
                "Subscriber connection closed",
                extra={"action": "subscriber_close", "status": "success"}
            )

    async def error_handler(self, error: Exception) -> None:
        logger.error(
            "NATS subscriber error: %s",
            error,
            exc_info=(type(error), error, error.__traceback__),
            extra={"action": "subscriber_error", "status": "error"}
        )


async def main() -> None:
    subscriber = NatsSubscriber()
    try:
        await subscriber.start()
        await subscriber.run()

    finally:
        await subscriber.close_connection()

if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info(
            "Subscriber stopped by user",
            extra={"action": "subscriber_stop", "status": "success"}
        )
    except Exception:
        logger.exception(
            "Subscriber failed",
            extra={"action": "subscriber_run", "status": "error"}
        )
        raise
