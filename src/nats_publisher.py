import json


async def publish_log(nc, log):
    message = json.dumps(log).encode("utf-8")
    await nc.publish("enriched_logs", message)

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


