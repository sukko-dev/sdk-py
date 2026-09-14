"""Recovery and gaps — handle every delivery item type across reconnects.

On the Kafka backend the SDK reconnects with backoff, replays from the last position, and surfaces
recovered records on the same messages() stream (they carry `pos`). Where it can't confirm recovery
(the Direct backend, or an SSE reconnect) it emits a PossibleGap per channel rather than dropping
silently; if the local queue overflows on a non-pausable transport it emits an Overflow. The SDK
owns the reconnect loop — this consumer just reacts to what it emits.

Delivery guarantee: at-least-once within the replay window, best-effort beyond.

Run with:

    SUKKO_URL=wss://gateway.example.com/ws SUKKO_TOKEN=<jwt> python examples/recovery_and_gaps.py
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal

from sukko import Gap, Message, Overflow, PossibleGap, ReplayMessage, SukkoClient


async def consume(client: SukkoClient) -> None:
    async for item in client.messages():
        match item:
            case Message():
                print("live", item.channel, item.ts, item.data)
            case ReplayMessage():
                print("recovered", item.channel, item.ts, item.data)  # replayed after a reconnect
            case Gap():
                # Confirmed loss on the Kafka backend: records in [from_seq, to_seq] were skipped.
                print("gap", item.channel, "before", item.last_pos)
            case PossibleGap():
                # Unconfirmed loss (Direct backend / SSE reconnect) — treat as at-least-a-gap.
                print("possible gap", item.channel)
            case Overflow():
                # The local delivery queue overflowed and dropped records — the consumer fell
                # behind. Tune with queue_maxsize / overflow_policy on the client.
                print("overflow — dropped", item.dropped)


async def main() -> None:
    url = os.environ.get("SUKKO_URL", "ws://localhost:8080/ws")
    token = os.environ.get("SUKKO_TOKEN")
    channel = os.environ.get("SUKKO_CHANNEL", "acme.trades")

    # A stable client_id lets the server resume this consumer's position across reconnects. Persist
    # it (e.g. to a file) to also recover across process restarts — omitted here for brevity.
    async with SukkoClient(url, token=token, client_id="example-recovery-consumer") as client:
        await client.subscribe([channel])
        print(f"subscribed to {channel} — Ctrl-C to stop")

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
