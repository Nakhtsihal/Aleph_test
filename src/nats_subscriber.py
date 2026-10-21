import asyncio
import nats


async def message_handler(msg):
    print(msg.data.decode())         # содержит сообщение в байтах, которое преобразуется в строку
    await msg.ack()                 #


async def main():
    nc = await nats.connect("nats://localhost:4222")
    try:
        js = nc.jetstream()
        print("Connected to NATS!")

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