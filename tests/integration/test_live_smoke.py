"""Live-compose smoke + wire-format — real gateway, real Kafka backend.

Covers the cleanly-live-testable success criteria: async subscribe→receive, the sync wrapper,
REST publish (all editions), and push VAPID (Pro Web Push). Reconnect-with-replay and
mid-stream token refresh
(auth refresh) need controlled disconnect / token-expiry injection the compose harness must drive —
they are stubbed skip-with-reason below rather than silently omitted.

Unlike the unit layer these use real time (short `wait_for` timeouts), which is correct for an
end-to-end wire smoke.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from sukko import Message, SukkoClient, SyncSukkoClient

pytestmark = pytest.mark.integration

# First delivery on a freshly-booted kafka topic (consumer joining from offset 0) can take
# tens of seconds, so the live timeout is generous — a warm stack returns in well under a second.
_TIMEOUT = 60.0


async def _first_on_channel(client: SukkoClient, channel: str) -> Message:
    async for item in client.messages():
        if isinstance(item, Message) and item.channel == channel:
            return item
    raise AssertionError("stream ended before a message arrived")


async def _await_subscribed(client: SukkoClient, channel: str, deadline: float = 15.0) -> None:
    """Wait until the server has granted the subscription — race-free readiness before a
    publish. The SDK exposes no subscription-ack event, so poll the public granted set."""
    end = time.monotonic() + deadline
    while channel not in client.subscriptions:
        if time.monotonic() > end:
            raise AssertionError(f"not subscribed to {channel} within {deadline}s")
        await asyncio.sleep(0.05)


async def test_async_subscribe_publish_receive_roundtrip(
    ws_url: str, token: str, channel: str
) -> None:
    """Subscribe, REST-publish to the same channel, and receive it back."""
    async with SukkoClient(ws_url, token=token) as client:
        await client.subscribe([channel])
        await _await_subscribed(client, channel)  # race-free: wait for the server grant
        await client.rest_publish(channel, {"probe": "async"})  # REST publish (all editions)
        msg = await asyncio.wait_for(_first_on_channel(client, channel), timeout=_TIMEOUT)
        assert msg.data.get("probe") == "async"
        assert msg.pos is not None  # Kafka backend attaches a position cursor


def test_sync_lifecycle_and_publish(ws_url: str, token: str, channel: str) -> None:
    """The sync wrapper connects/subscribes/publishes/closes with no asyncio boilerplate."""
    with SyncSukkoClient(ws_url, token=token) as client:
        client.subscribe([channel])
        time.sleep(0.3)
        client.rest_publish(channel, {"probe": "sync"})  # completes without raising


async def test_push_vapid_key(ws_url: str, token: str, edition: str) -> None:
    """The push VAPID key round-trips (Web Push = Pro). Skipped unless the live stack is Pro+
    — on Community the push route is absent (404), so it is not exercised here."""
    if edition not in ("pro", "enterprise"):
        pytest.skip(f"Web Push is Pro-gated — stack edition is {edition}")
    async with SukkoClient(ws_url, token=token) as client:
        key = await client.push.get_vapid_key()
        assert isinstance(key, str) and key


@pytest.mark.skip(
    reason="replay leg needs the harness to force a disconnect mid-stream (compose-driven)"
)
async def test_reconnect_with_replay() -> None:  # pragma: no cover
    """After a forced disconnect the client reconnects and replays missed messages. Requires
    the compose harness to sever the connection (e.g. kill the ws-server pod) — driven there."""


@pytest.mark.skip(reason="needs a short-lived token whose expiry the harness controls")
async def test_auto_token_refresh() -> None:  # pragma: no cover
    """A near-expiry token is refreshed without dropping subscriptions. Requires a
    controllably-short-lived token issued by the compose harness."""
