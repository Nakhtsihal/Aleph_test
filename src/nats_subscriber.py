import asyncio
import nats


async def message_handler(msg):
    print(msg.data.decode())          # содержит сообщение в байтах, которое преобразуется в строку

async def main():
    nc = await nats.connect("nats://localhost:4222")
    print("Connected to NATS!")
    await nc.subscribe("enriched_logs", cb=message_handler)

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())