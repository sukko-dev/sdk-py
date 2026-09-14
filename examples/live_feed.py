"""Live feed — subscribe to a channel and print messages until interrupted.

A long-running async consumer with graceful shutdown: the shape for a backend service or a
market-data tap. It handles SIGINT (Ctrl-C) and SIGTERM (container stop) so the client drains its
queue and closes the socket cleanly instead of being killed mid-flight.

Run with:

    SUKKO_URL=wss://gateway.example.com/ws SUKKO_TOKEN=<jwt> python examples/live_feed.py
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal

from sukko import Message, ReplayMessage, SukkoClient


async def consume(client: SukkoClient) -> None:
    async for item in client.messages():
        # Live and recovered records both arrive here; the other delivery item types (gaps,
        # overflow) are covered in recovery_and_gaps.py.
        if isinstance(item, (Message, ReplayMessage)):
            print(item.ts, item.channel, item.data)


async def main() -> None:
    url = os.environ.get("SUKKO_URL", "ws://localhost:8080/ws")
    token = os.environ.get("SUKKO_TOKEN")
    channel = os.environ.get("SUKKO_CHANNEL", "acme.trades")

    async with SukkoClient(url, token=token) as client:
        await client.subscribe([channel])
        print(f"subscribed to {channel} — Ctrl-C to stop")

        # Signal handlers set an event; main cancels the consume task and lets the `async with`
        # block close the client. (For a quick local run you could drop the signal handling and wrap
        # asyncio.run(main()) in `try/except KeyboardInterrupt` instead — SIGTERM just wouldn't be
        # caught.)
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        worker = asyncio.create_task(consume(client))
        await stop.wait()
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker
        print("shutting down")


if __name__ == "__main__":
    asyncio.run(main())
