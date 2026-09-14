"""Sync consumer — a blocking stream for scripts and Jupyter notebooks.

SyncSukkoClient runs the async client on its own background event-loop thread, so you use it like an
ordinary blocking API — no asyncio boilerplate, and it works inside a running notebook loop. Ideal
for a quant script or an exploratory notebook cell.

Run with:

    SUKKO_URL=wss://gateway.example.com/ws SUKKO_TOKEN=<jwt> python examples/sync_script.py
"""

from __future__ import annotations

import os

from sukko import Message, ReplayMessage, SyncSukkoClient


def main() -> None:
    url = os.environ.get("SUKKO_URL", "ws://localhost:8080/ws")
    token = os.environ.get("SUKKO_TOKEN")
    channel = os.environ.get("SUKKO_CHANNEL", "acme.trades")

    with SyncSukkoClient(url, token=token) as client:
        client.subscribe([channel])
        print(f"subscribed to {channel} — Ctrl-C to stop")
        try:
            for item in client.stream():
                if isinstance(item, (Message, ReplayMessage)):
                    print(item.ts, item.channel, item.data)
        except KeyboardInterrupt:
            print("shutting down")


if __name__ == "__main__":
    main()
