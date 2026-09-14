"""Client integration tests — connect/subscribe/receive, publish fail-fast, grant-diff,
escalation-vs-refresh, and the cancel→await→drain lifecycle. Deterministic:
FakeTransport/FakeServer + FakeClock, no sockets, no real sleeps.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from fakes import FakeClock, FakeServer, FakeTransport
from sukko.client import SukkoClient
from sukko.constants import CLOSE_CODES, CloseDirection
from sukko.errors import NotConnectedError, RecoveryInterruptedError, SukkoError
from sukko.messages import (
    Auth,
    Gap,
    Message,
    PossibleGap,
    Publish,
    Reconnect,
    ReconnectError,
    Subscribe,
    SubscriptionAck,
)
from sukko.transport.base import ConnectionState


async def _drain(times: int = 6) -> None:
    """Yield to the event loop so the read-pump can process pending server frames."""
    for _ in range(times):
        await asyncio.sleep(0)


def _make_client(
    server: FakeServer, *, clock: FakeClock | None = None, **kwargs: object
) -> SukkoClient:
    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server)

    return SukkoClient(
        "ws://test",
        transport_factory=factory,
        clock=clock or FakeClock(),
        **kwargs,  # type: ignore[arg-type]
    )


async def test_connect_subscribe_receive() -> None:
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()
    assert client.state is ConnectionState.CONNECTED

    await client.subscribe(["acme.trades"])
    await _drain()
    assert client.subscriptions == frozenset({"acme.trades"})

    server.push(Message(seq=1, ts=10, channel="acme.trades", data={"price": 100}))
    stream = client.messages()
    item = await anext(stream)
    assert isinstance(item, Message)
    assert item.channel == "acme.trades" and item.data == {"price": 100}
    await client.close()


async def test_publish_fails_fast_when_not_connected() -> None:
    client = _make_client(FakeServer())
    with pytest.raises(NotConnectedError):
        await client.publish("acme.trades", {"x": 1})


async def test_publish_sends_frame_when_connected() -> None:
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()
    await client.publish("acme.trades", {"x": 1})
    await _drain()
    published = [m for m in server.sent_messages if isinstance(m, Publish)]
    assert any(m.data.channel == "acme.trades" for m in published)
    await client.close()


async def test_publish_ack_listener_receives_channel_and_mid() -> None:
    """The publish ack surfaces the server-assigned stable message identity ``mid`` (None when a
    pre-field server or a multi-topic fan-out publish omits it)."""
    server = FakeServer()
    server.enable_auto_ack()  # acks publishes with mid="mid-1"
    acks: list[tuple[str, str | None]] = []
    client = _make_client(server, on_publish_ack=lambda channel, mid: acks.append((channel, mid)))
    await client.connect()
    await client.publish("acme.trades", {"x": 1})
    await _drain()
    assert acks == [("acme.trades", "mid-1")]

    server.push(b'{"type":"publish_ack","channel":"acme.trades","status":"accepted"}')  # no mid
    await _drain()
    assert acks[-1] == ("acme.trades", None)
    await client.close()


async def test_not_granted_channels_are_surfaced() -> None:
    server = FakeServer()

    def responder(srv: FakeServer, msg: object) -> None:
        if isinstance(msg, Subscribe):
            requested = msg.data.channels or ([msg.data.channel] if msg.data.channel else [])
            granted = [c for c in requested if c != "acme.private"]  # deny the private one
            srv.push(SubscriptionAck(subscribed=granted, count=len(granted)))

    server.responder = responder
    seen: list[frozenset[str]] = []
    client = _make_client(server, on_not_granted=seen.append)
    await client.connect()
    await client.subscribe(["acme.public", "acme.private"])
    await _drain()
    assert client.subscriptions == frozenset({"acme.public"})
    assert seen and "acme.private" in seen[-1]
    await client.close()


async def test_escalation_resubscribes_delta_but_refresh_preserves() -> None:
    """Escalation (api-key→JWT) re-subscribes the newly-permitted delta; refresh doesn't."""
    server = FakeServer()
    grantable = {"acme.public"}

    def responder(srv: FakeServer, msg: object) -> None:
        if isinstance(msg, Subscribe):
            requested = msg.data.channels or ([msg.data.channel] if msg.data.channel else [])
            granted = [c for c in requested if c in grantable]
            srv.push(SubscriptionAck(subscribed=granted, count=len(granted)))
        elif isinstance(msg, Auth):
            grantable.add("acme.private")  # the JWT unlocks the private channel
            srv.push(b'{"type":"auth_ack","data":{"exp":0}}')

    server.responder = responder
    client = _make_client(server, api_key="key")
    await client.connect()
    await client.subscribe(["acme.public", "acme.private"])
    await _drain()
    assert client.subscriptions == frozenset({"acme.public"})  # private denied under api-key

    subscribes_before = sum(isinstance(m, Subscribe) for m in server.sent_messages)
    await client.escalate("jwt-token")  # blocks until auth_ack, then re-subscribes the delta
    await _drain()
    # escalation re-issued a subscribe (for the delta) and it is now granted
    assert sum(isinstance(m, Subscribe) for m in server.sent_messages) == subscribes_before + 1
    assert client.subscriptions == frozenset({"acme.public", "acme.private"})

    # a plain refresh must NOT re-subscribe (subs preserved)
    subscribes_after_escalation = sum(isinstance(m, Subscribe) for m in server.sent_messages)
    await client.refresh_token()
    await _drain()
    subscribes_now = sum(isinstance(m, Subscribe) for m in server.sent_messages)
    assert subscribes_now == subscribes_after_escalation
    await client.close()


def _multi_epoch_client(
    servers: list[FakeServer], clock: FakeClock, **kwargs: object
) -> SukkoClient:
    """A client whose factory hands out ``servers`` one per epoch (for reconnect tests)."""
    index = [0]

    def factory(_channels: Sequence[str]) -> FakeTransport:
        server = servers[index[0]]
        index[0] += 1
        return FakeTransport(server)

    return SukkoClient(
        "ws://test",
        transport_factory=factory,
        clock=clock,
        **kwargs,  # type: ignore[arg-type]
    )


async def test_reconnect_resumes_subscriptions_and_sends_reconnect_payload() -> None:
    """After a drop, the client reconnects, replays via reconnect{last_pos}, and resumes."""
    clock = FakeClock()
    servers = [FakeServer(), FakeServer()]
    for server in servers:
        server.enable_auto_ack()
    client = _multi_epoch_client(servers, clock)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    # a live message carries a pos → recovery remembers it for the reconnect payload
    servers[0].push(Message(seq=1, ts=1, channel="acme.a", data={}, pos="2-5"))
    await _drain()

    servers[0].close_connection(CLOSE_CODES.POLICY_VIOLATION, CloseDirection.REMOTE)  # slow-client
    await _drain()
    await clock.advance(2.0)  # elapse the backoff (full-jitter, base 1s)
    await _drain()

    reconnects = [m for m in servers[1].sent_messages if isinstance(m, Reconnect)]
    assert reconnects and reconnects[0].data.last_pos == {"acme.a": "2-5"}  # replay from last pos
    resubs = [m for m in servers[1].sent_messages if isinstance(m, Subscribe)]
    assert resubs and "acme.a" in (resubs[0].data.channels or [])  # subscriptions resumed
    await client.close()


async def test_heartbeat_pong_timeout_triggers_reconnect() -> None:
    clock = FakeClock()
    servers = [FakeServer(), FakeServer()]  # epoch 0 never sends pong → timeout; epoch 1 is healthy
    servers[1].enable_auto_ack()
    client = _multi_epoch_client(servers, clock, heartbeat_interval=30.0, heartbeat_timeout=5.0)
    await client.connect()
    await client.subscribe(["acme.a"])  # so the reconnect has something to resume
    await _drain()

    await clock.advance(30.0)  # heartbeat is sent
    await _drain()
    assert any(m.__class__.__name__ == "Heartbeat" for m in servers[0].sent_messages)
    await clock.advance(5.0)  # no pong within the timeout → local 4000 close → reconnect
    await _drain()
    await clock.advance(2.0)  # elapse the backoff
    await _drain()

    # reconnected onto the second transport and resumed the subscription
    assert any(isinstance(m, Subscribe) for m in servers[1].sent_messages)
    assert client.state is ConnectionState.CONNECTED
    await client.close()


async def test_close_ends_messages_and_leaves_no_orphan_tasks() -> None:
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()

    consumed: list[object] = []

    async def consume() -> None:
        async for item in client.messages():
            consumed.append(item)

    consumer = asyncio.ensure_future(consume())
    await _drain()

    await client.close()
    await consumer  # messages() ended via the shutdown sentinel — the async-for returned cleanly

    assert client.state is ConnectionState.DISCONNECTED
    assert client._supervisor is not None and client._supervisor.done()  # supervisor awaited
    assert not client._bg_tasks  # no orphaned fire-and-forget tasks


# --- regression tests for review fixes ---------------------------------------------------------


async def test_malformed_frame_does_not_kill_the_client() -> None:
    """C1: a non-JSON/undecodable frame must be skipped, not crash the read-pump/supervisor."""
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    server.push(b"{ this is not valid json")  # bad frame
    server.push(Message(seq=1, ts=1, channel="acme.a", data={"ok": True}))  # good frame after it
    item = await anext(client.messages())
    assert isinstance(item, Message) and item.data == {"ok": True}  # survived + still delivering
    assert client.state is ConnectionState.CONNECTED
    await client.close()


async def test_user_callback_exception_is_isolated() -> None:
    """C4: a raising user callback must not kill the supervisor."""
    server = FakeServer()

    def responder(srv: FakeServer, msg: object) -> None:
        if isinstance(msg, Subscribe):
            srv.push(SubscriptionAck(subscribed=[], count=0))  # deny → triggers on_not_granted

    server.responder = responder

    def boom(_channels: frozenset[str]) -> None:
        raise ValueError("user callback blew up")

    client = _make_client(server, on_not_granted=boom)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()
    assert client.state is ConnectionState.CONNECTED  # supervisor survived the callback raise
    await client.close()


async def test_close_does_not_raise_when_queue_is_full() -> None:
    """C3: close()'s shutdown sentinel must be infallible even on a full (back-pressured) queue."""
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server, queue_maxsize=200)  # floor is 100+100
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()
    # fill the queue past capacity without consuming messages()
    for i in range(250):
        server.push(Message(seq=i, ts=0, channel="acme.a", data={}))
    await _drain()
    await client.close()  # must not raise QueueFull
    assert client.state is ConnectionState.DISCONNECTED


async def test_terminal_4001_close_stops_reconnect() -> None:
    created: list[FakeTransport] = []

    def factory(_channels: Sequence[str]) -> FakeTransport:
        server = FakeServer()
        server.enable_auto_ack()
        transport = FakeTransport(server)
        created.append(transport)
        return transport

    client = SukkoClient("ws://test", transport_factory=factory, clock=FakeClock(), reconnect=True)
    await client.connect()
    await _drain()
    # server sends a terminal auth-failed close (4001) → must NOT reconnect
    created[0].server.close_connection(CLOSE_CODES.AUTH_FAILED, CloseDirection.REMOTE)
    await _drain()
    assert len(created) == 1  # no second epoch was opened
    assert client.state is ConnectionState.DISCONNECTED
    await client.close()


async def test_direct_degrade_emits_possible_gap_through_messages() -> None:
    """Degrade path via the client: reconnect_error:not_available → PossibleGap in messages()."""
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    server.push(ReconnectError(code="not_available", message="direct backend"))
    item = await anext(client.messages())
    assert isinstance(item, PossibleGap) and item.channel == "acme.a"
    await client.close()


async def test_direct_reconnect_probes_empty_pos_then_possible_gap() -> None:
    """End-to-end: a pure-Direct backend (messages carry no pos) is DETECTED via an
    empty-last_pos reconnect PROBE. The first connect sends no reconnect; the reconnect sends one
    with an empty last_pos so the backend can report not_available; that degrades to PossibleGap.
    Reverting build_reconnect's connected_once probe makes the reconnect frame never send → the
    probe assert below fails (the old code silently dropped instead)."""
    clock = FakeClock()
    servers = [FakeServer(), FakeServer()]
    for server in servers:
        server.enable_auto_ack()
    client = _multi_epoch_client(servers, clock)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    # Direct backend → no message ever carries a pos. The FIRST connect must not send a reconnect.
    assert not [m for m in servers[0].sent_messages if isinstance(m, Reconnect)]

    servers[0].close_connection(CLOSE_CODES.POLICY_VIOLATION, CloseDirection.REMOTE)
    await _drain()
    await clock.advance(2.0)  # elapse the backoff
    await _drain()

    # The reconnect probes with an EMPTY last_pos so the backend can answer not_available.
    reconnects = [m for m in servers[1].sent_messages if isinstance(m, Reconnect)]
    assert reconnects and reconnects[0].data.last_pos == {}

    servers[1].push(ReconnectError(code="not_available", message="direct backend"))
    item = await anext(client.messages())
    assert isinstance(item, PossibleGap) and item.channel == "acme.a"
    await client.close()


async def test_recovery_interrupted_surfaced_via_on_error() -> None:
    """A disconnect mid-replay surfaces RecoveryInterruptedError (never a bare disconnect)."""
    errors: list[SukkoError] = []
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server, on_error=errors.append)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    server.push(Gap(channel="acme.a", from_seq=1, to_seq=2, last_pos="2-9", ts=0))  # → REPLAYING
    await _drain()
    server.close_connection(CLOSE_CODES.POLICY_VIOLATION, CloseDirection.REMOTE)  # drop mid-replay
    await _drain()
    assert any(isinstance(e, RecoveryInterruptedError) for e in errors)
    await client.close()


async def test_rest_publish_and_push_through_client_without_connect() -> None:
    """rest_publish/push work through SukkoClient WITHOUT connect(); close() with no
    connect still works (the Phase-4 wiring, previously only component-tested)."""
    import httpx

    from sukko._http import HttpApi
    from sukko.push import PushClient
    from sukko.rest import RestPublisher

    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        if request.url.path == "/api/v1/push/vapid-key":
            return httpx.Response(200, json={"public_key": "vapid"})
        return httpx.Response(
            200, json={"status": "accepted", "channel": "acme.a", "mid": "9c5b1f0a-2-1235"}
        )

    client = SukkoClient("wss://h/ws", token="jwt")
    # inject a mock-backed HttpApi (test seam) into the client's REST surfaces
    client._http = HttpApi(
        "https://h",
        lambda: (client._auth.token, client._auth.api_key),
        transport=httpx.MockTransport(handler),
    )
    client._rest = RestPublisher(client._http)
    client.push = PushClient(client._http)

    mid = await client.rest_publish("acme.a", {"x": 1})  # no connect()
    assert mid == "9c5b1f0a-2-1235"  # the ack's stable message identity is surfaced
    assert seen["url"] == "https://h/api/v1/publish"
    assert await client.push.get_vapid_key() == "vapid"
    await client.close()  # REST-only close path
    assert client.state is ConnectionState.DISCONNECTED
