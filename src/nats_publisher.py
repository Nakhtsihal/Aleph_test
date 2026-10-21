from nats.js.api import StorageType
from nats.js.errors import NotFoundError
import json


async def publish_log(js, log):
    message = json.dumps(log).encode("utf-8")
    await js.publish("enriched_logs", message)


async def setup_jetstream(nc):
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



# async def main():
#     nc = await nats.connect("nats://localhost:4222")
#     print("Connected to NATS!")
#
#     print("Message published!")
#     await nc.drain()
#
# log = {
#     "client_ip": "1.1.1.1",
#     "action": "login",
#     "findings": {
#         "1.1.1.1": {
#             "country": "Australia"
#         }
#     }
# }
#
#
# if __name__ == "__main__":
#     asyncio.run(main())


