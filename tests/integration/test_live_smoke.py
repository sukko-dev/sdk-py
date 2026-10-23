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
from collections.abc import Callable, Sequence

import pytest

from sukko import (
    ConnectionState,
    Message,
    SseTransport,
    SukkoClient,
    SyncSukkoClient,
    Transport,
    WebSocketTransport,
)

pytestmark = pytest.mark.integration

# First delivery on a freshly-booted kafka topic (consumer joining from offset 0) can take
# tens of seconds, so the live timeout is generous — a warm stack returns in well under a second.
_TIMEOUT = 60.0


async def _first_on_channel(client: SukkoClient, channel: str) -> Message:
    async for item in client.messages():
        if isinstance(item, Message) and item.channel == channel:
            return item
    raise AssertionError("stream ended before a message arrived")


async def _wait_until(pred: Callable[[], bool], deadline: float, what: str) -> None:
    """Poll pred() until true or the deadline — the SDK surfaces readiness as state, not events."""
    end = time.monotonic() + deadline
    while not pred():
        if time.monotonic() > end:
            raise AssertionError(f"timed out waiting for {what} within {deadline}s")
        await asyncio.sleep(0.05)


async def _await_subscribed(client: SukkoClient, channel: str, deadline: float = 15.0) -> None:
    """Wait until the server has granted the subscription — race-free readiness before a
    publish. The SDK exposes no subscription-ack event, so poll the public granted set."""
    await _wait_until(
        lambda: channel in client.subscriptions, deadline, f"subscription to {channel}"
    )


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


async def test_api_key_auth(ws_url: str, api_key: str, token: str, channel: str) -> None:
    """An API-key-authenticated client subscribes and receives — proving the gateway accepts
    X-API-Key auth on the WebSocket path, not only JWTs. API keys are read-only (publish needs
    a JWT), so a JWT client does the publish."""
    ch = f"{channel}-apikey"
    async with (
        SukkoClient(ws_url, api_key=api_key) as subscriber,
        SukkoClient(ws_url, token=token) as publisher,
    ):
        await subscriber.subscribe([ch])
        await _await_subscribed(subscriber, ch)
        await publisher.rest_publish(ch, {"probe": "apikey"})
        msg = await asyncio.wait_for(_first_on_channel(subscriber, ch), timeout=_TIMEOUT)
        assert msg.data.get("probe") == "apikey"


def _http_base(ws_url: str) -> str:
    """The gateway's HTTP base for the SSE transport: ws(s)://host/ws → http(s)://host."""
    base = ws_url.replace("wss://", "https://").replace("ws://", "http://")
    return base.removesuffix("/ws").rstrip("/")


async def test_sse_subscribe_receive(ws_url: str, token: str, channel: str, edition: str) -> None:
    """SSE subscribe→receive: a receive-only SSE subscriber gets a message published over WS.
    SSE transport is Pro-gated (features.go SSETransport:Pro), so this skips unless the live
    stack is Pro+ — on Community the SSE route is refused."""
    if edition not in ("pro", "enterprise"):
        pytest.skip(f"SSE transport is Pro-gated — stack edition is {edition}")
    ch = f"{channel}-sse"
    http_base = _http_base(ws_url)

    def sse_factory(channels: Sequence[str], last_event_id: str | None) -> Transport:
        # SSE channels are a connect-time subscription; the factory receives the desired set.
        return SseTransport(http_base, list(channels), token=token, last_event_id=last_event_id)

    # SSE needs its channels at connect time, so record the desired set via subscribe() BEFORE
    # connect() (subscribe never auto-connects) — otherwise the factory builds SseTransport with
    # no channels and raises. This is why the SSE subscriber isn't an auto-connecting `async with`.
    subscriber = SukkoClient(ws_url, token=token, transport_factory=sse_factory)
    try:
        await subscriber.subscribe([ch])
        await subscriber.connect()  # resolves once the SSE stream is live
        await _wait_until(
            lambda: subscriber.state is ConnectionState.CONNECTED, 15.0, "SSE stream connected"
        )
        async with SukkoClient(ws_url, token=token) as publisher:
            await publisher.rest_publish(ch, {"probe": "sse"})
            msg = await asyncio.wait_for(_first_on_channel(subscriber, ch), timeout=_TIMEOUT)
        assert msg.data.get("probe") == "sse"
    finally:
        await subscriber.close()


async def test_push_vapid_key(ws_url: str, token: str, edition: str) -> None:
    """The push VAPID key round-trips (Web Push = Pro). Skipped unless the live stack is Pro+
    — on Community the push route is absent (404), so it is not exercised here."""
    if edition not in ("pro", "enterprise"):
        pytest.skip(f"Web Push is Pro-gated — stack edition is {edition}")
    async with SukkoClient(ws_url, token=token) as client:
        key = await client.push.get_vapid_key()
        assert isinstance(key, str) and key


async def test_reconnect_with_replay(ws_url: str, token: str, channel: str) -> None:
    """Kafka gap→replay (no-loss recovery): messages published while the WebSocket is down are
    replayed after reconnect. Only the WS is dropped — the gateway (hence REST) stays up. A
    gated transport factory holds the reconnect closed so the publishes are guaranteed to land
    while the client is offline, forcing the replay path."""
    recovery_channel = f"{channel}-replay"
    gate = asyncio.Event()
    gate.set()  # the initial connect proceeds; cleared to hold a reconnect's open() closed
    holder: dict[str, WebSocketTransport] = {}

    def factory(channels: Sequence[str], resume: str | None) -> WebSocketTransport:
        transport = WebSocketTransport(ws_url, token=token)
        original_open = transport.open

        async def gated_open() -> None:
            await gate.wait()
            await original_open()

        transport.open = gated_open  # type: ignore[method-assign]
        holder["t"] = transport
        return transport

    client = SukkoClient(ws_url, token=token, transport_factory=factory, reconnect_attempts=0)
    received: list[Message] = []

    async def collect() -> None:
        async for item in client.messages():
            if isinstance(item, Message) and item.channel == recovery_channel:
                received.append(item)

    await client.connect()
    collector = asyncio.create_task(collect())
    try:
        await client.subscribe([recovery_channel])
        await _await_subscribed(client, recovery_channel)

        # A live message first — establishes the kafka pos cursor the resume replays from.
        await client.rest_publish(recovery_channel, {"seq": 1})
        await _wait_until(lambda: _has_seq(received, 1), 60.0, "first live message")
        assert received[0].pos is not None, "kafka backend must attach a pos cursor"

        # Drop ONLY the WS and hold the reconnect closed; publish while offline (REST via the
        # still-up gateway). Kafka retains these for the resume.
        gate.clear()
        await holder["t"].close(1000)
        await client.rest_publish(recovery_channel, {"seq": 2})
        await client.rest_publish(recovery_channel, {"seq": 3})
        gate.set()  # release the reconnect → the client resumes and replays seq 2 + 3

        await _wait_until(
            lambda: _has_seq(received, 2) and _has_seq(received, 3), 30.0, "replay of seq 2 + 3"
        )
    finally:
        await client.close()
        await collector

    seqs = [m.data.get("seq") for m in received]
    assert 2 in seqs and 3 in seqs, f"missed messages not replayed: {seqs}"
    assert seqs.count(1) == 1, f"anchor message re-delivered (cursor not exclusive): {seqs}"


def _has_seq(messages: list[Message], seq: int) -> bool:
    return any(m.data.get("seq") == seq for m in messages)


@pytest.mark.skip(reason="needs a short-lived token whose expiry the harness controls")
async def test_auto_token_refresh() -> None:  # pragma: no cover
    """A near-expiry token is refreshed without dropping subscriptions. Requires a
    controllably-short-lived token issued by the compose harness."""
