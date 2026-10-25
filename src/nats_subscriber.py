import asyncio
import nats
from nats.aio.msg import Msg


async def message_handler(msg: Msg) -> None:
    print(msg.data.decode())
    await msg.ack()


async def main() -> None:
    nc = await nats.connect("nats://localhost:4222")
    try:
        js = nc.jetstream()

        await js.subscribe(
            "enriched_logs",
            stream="ENRICHED_LOGS",
            durable="log_viewer",
            cb=message_handler,
            manual_ack=True
        )
        await asyncio.Event().wait()
    finally:
        await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())