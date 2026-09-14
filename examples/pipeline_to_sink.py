"""Pipeline to sink — stream messages into a file as JSON lines.

The data-pipeline shape: consume the feed and write each record to a sink (here, a local JSON-lines
file). The SDK's bounded queue + overflow_policy absorb bursts, but the consume loop must still not
block on a slow sink — for a remote or slow sink, offload the write (e.g. asyncio.to_thread) so it
doesn't stall delivery. A local append-file is fast enough to write inline.

Run with:

    SUKKO_URL=... SUKKO_TOKEN=<jwt> SUKKO_SINK=feed.jsonl python examples/pipeline_to_sink.py
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
from typing import TextIO

from sukko import Message, ReplayMessage, SukkoClient


async def pump(client: SukkoClient, sink: TextIO) -> None:
    async for item in client.messages():
        if isinstance(item, (Message, ReplayMessage)):
            record = {"ts": item.ts, "channel": item.channel, "data": item.data}
            sink.write(json.dumps(record) + "\n")
            sink.flush()


async def run(sink: TextIO) -> None:
    url = os.environ.get("SUKKO_URL", "ws://localhost:8080/ws")
    token = os.environ.get("SUKKO_TOKEN")
    channel = os.environ.get("SUKKO_CHANNEL", "acme.trades")

    async with SukkoClient(url, token=token) as client:
        await client.subscribe([channel])
        print(f"piping {channel} — Ctrl-C to stop")

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        worker = asyncio.create_task(pump(client, sink))
        await stop.wait()
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker
        print("shutting down")


def main() -> None:
    # Open the sink synchronously (outside the async loop — no blocking open() inside async code).
    sink_path = os.environ.get("SUKKO_SINK", "feed.jsonl")
    with open(sink_path, "a", encoding="utf-8") as sink:
        print(f"appending to {sink_path}")
        asyncio.run(run(sink))


if __name__ == "__main__":
    main()
