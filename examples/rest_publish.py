"""REST publish — send messages without a live WebSocket connection (all editions).

rest_publish() posts to the gateway over HTTP, resolves on its ack, and returns the server-assigned
stable message identity `mid` (the same `mid` subscribers see on the delivered message), so a
producer needs no open socket. Because no WebSocket is needed we deliberately do NOT connect() — we
construct the client, publish, and close() to release the HTTP session. (Contrast with
`await client.publish(...)`, which is fire-and-forget over an already-open WS connection.)

Note: on the Kafka backend the channel must match a provisioned routing rule server-side, or the
publish is rejected with a 409 (PublishNotRoutableError).

Run with:

    SUKKO_URL=wss://gateway.example.com/ws SUKKO_TOKEN=<jwt> python examples/rest_publish.py
"""

from __future__ import annotations

import asyncio
import os

from sukko import SukkoClient, SukkoError


async def main() -> None:
    url = os.environ.get("SUKKO_URL", "ws://localhost:8080/ws")
    token = os.environ.get("SUKKO_TOKEN")
    channel = os.environ.get("SUKKO_CHANNEL", "acme.trades")

    client = SukkoClient(url, token=token)
    try:
        for seq in range(5):
            mid = await client.rest_publish(channel, {"seq": seq, "price": 100 + seq})
            print(f"published seq={seq} mid={mid}")
            await asyncio.sleep(1)
    except SukkoError as err:
        print(f"publish failed: {err}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
