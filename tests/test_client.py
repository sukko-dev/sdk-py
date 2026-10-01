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
from sukko.transport.base import SSE_CAPABILITIES, ConnectionState


async def _drain(times: int = 6) -> None:
    """Yield to the event loop so the read-pump can process pending server frames."""
    for _ in range(times):
        await asyncio.sleep(0)


def _make_client(
    server: FakeServer, *, clock: FakeClock | None = None, **kwargs: object
) -> SukkoClient:
    def factory(_channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
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

    def factory(_channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
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

    def factory(_channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
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


async def test_no_replay_control_frame_emits_possible_gap_per_channel() -> None:
    """SSE ``no_replay`` control frame → one PossibleGap per channel on messages() (ADR-0006).

    sukko-py's SSE recovery is optimistic (no blanket PossibleGap on reopen), so ``no_replay`` is
    the only signal that a cursor channel went unrecovered — it must surface, not be dropped.
    """
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()
    await client.subscribe(["acme.a", "acme.b"])
    await _drain()

    # Raw bytes: no_replay is an SSE-only frame, not a ServerMessage the FakeServer can encode.
    server.push(b'{"type":"no_replay","channels":["acme.a","acme.b"]}')
    stream = client.messages()
    first = await anext(stream)
    second = await anext(stream)
    assert isinstance(first, PossibleGap) and first.channel == "acme.a"
    assert isinstance(second, PossibleGap) and second.channel == "acme.b"
    await client.close()


async def test_replay_truncated_control_frame_surfaced_via_on_error() -> None:
    """SSE replay_truncated → a channel-less RecoveryInterruptedError via on_error (ADR-0006)."""
    errors: list[SukkoError] = []
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server, on_error=errors.append)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    server.push(b'{"type":"replay_truncated","replayed":3}')
    await _drain()
    interrupted = [e for e in errors if isinstance(e, RecoveryInterruptedError)]
    assert len(interrupted) == 1
    assert interrupted[0].channel is None  # connection-level, not channel-scoped
    assert interrupted[0].reason == "replay_truncated"  # programmatic discriminator
    assert "3" in str(interrupted[0])
    await client.close()


async def test_unknown_control_frame_is_still_dropped() -> None:
    """A genuinely unknown tag still falls through the second-pass decode to log-and-skip, and the
    read-pump survives it (a following good frame is delivered)."""
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    server.push(b'{"type":"some_future_frame","x":1}')  # neither ServerMessage nor SSE control
    server.push(Message(seq=1, ts=1, channel="acme.a", data={"ok": True}))
    item = await anext(client.messages())
    assert isinstance(item, Message) and item.channel == "acme.a"
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


async def test_recovery_deadline_suspended_while_consumer_backpressured() -> None:
    """Client-level wiring: while the delivery consumer is backpressured (the blocking put stalls
    the read-pump so recovery frames stop), the recovery detection deadline must SUSPEND, not fire —
    the client signals note_backpressure into the blocking put (platform ADR-0025). Guards that
    wiring: deleting the note_backpressure(True) call makes the timer raise a spurious
    RecoveryInterrupted here."""
    errors: list[SukkoError] = []
    clock = FakeClock(start=0.0)
    server = FakeServer()
    server.enable_auto_ack()
    client = _make_client(server, clock=clock, on_error=errors.append, queue_maxsize=200)
    await client.connect()
    await client.subscribe(["acme.a"])
    await _drain()

    server.push(
        Gap(channel="acme.a", from_seq=1, to_seq=2, last_pos="2-9", ts=0)
    )  # → REPLAYING, deadline at +10s
    await _drain()
    # Fill the queue past capacity with no messages() consumer → the read-pump's put blocks
    # (back-pressure), so recovery frames stop and the client signals note_backpressure(True).
    for i in range(250):
        server.push(Message(seq=i, ts=0, channel="acme.a", data={}))
    await _drain()

    await clock.advance(11.0)  # a full detection-deadline window elapses while backpressured
    await _drain()
    assert not any(isinstance(e, RecoveryInterruptedError) for e in errors)  # suspended, not fired

    # RESUME: consume the queue so every blocked put completes — back-pressure clears and the
    # client must signal note_backpressure(False). Then a full window with no recovery frame MUST
    # interrupt; else the engine stays paused forever and no deadline fires again (a silent wedge).
    stream = client.messages()
    while True:  # real-time drain until the pump goes quiet (all blocked puts released)
        try:
            await asyncio.wait_for(stream.__anext__(), timeout=0.05)
        except TimeoutError:
            break
    # A back-pressure episode occurred during the stall, so the first resumed window re-arms the
    # deadline (episode count changed since arm); the next clean window fires.
    await clock.advance(11.0)
    await _drain()
    await clock.advance(11.0)
    await _drain()
    assert any(isinstance(e, RecoveryInterruptedError) for e in errors)  # fires after resume
    await client.close()


async def test_sse_last_event_id_threaded_across_reconnect() -> None:
    """The supervisor threads a dropped SSE transport's last_event_id into the NEXT epoch's
    factory call, so the reconnect echoes Last-Event-ID and the server replays the gap (the fix:
    Client-managed SSE reconnect was previously losing the cursor). Pins first-connect-carries-None
    and that the cursor advances to what the prior epoch reached."""
    calls: list[str | None] = []
    # Epoch 1 reaches a cursor; epoch 2 reaches NONE (a cursorless epoch — e.g. it dropped before
    # any id: arrived). The not-None guard must keep epoch 1's cursor so epoch 3 still resumes from
    # it. Removing the guard makes epoch 3 receive None → the calls[2] assertion goes red.
    epoch_ids: list[str | None] = ["evt-1", None]
    created: list[FakeTransport] = []

    def factory(_channels: Sequence[str], last_event_id: str | None = None) -> FakeTransport:
        calls.append(last_event_id)
        server = FakeServer()
        transport = FakeTransport(server, capabilities=SSE_CAPABILITIES)
        # Simulate the id: cursor this epoch's transport captured by the time it drops.
        idx = len(created)
        transport.last_event_id = epoch_ids[idx] if idx < len(epoch_ids) else None
        created.append(transport)
        return transport

    clock = FakeClock()
    client = SukkoClient("ws://test", transport_factory=factory, clock=clock, reconnect=True)
    await client.subscribe(["t.a"])  # SSE needs a channel; recorded before connect (no bounce)
    await client.connect()
    await _drain()
    assert calls[0] is None  # first connect carries no resume cursor

    # Drop epoch 1 (has cursor "evt-1") → reconnect epoch 2 (desired set non-empty, no park).
    created[0].server.close_connection(CLOSE_CODES.GOING_AWAY, CloseDirection.REMOTE)
    await _drain()
    await clock.advance(2.0)  # elapse the reconnect backoff (full-jitter, base 1s)
    await _drain()
    assert len(created) >= 2, "expected a reconnect epoch after the non-terminal close"
    assert calls[1] == "evt-1", "reconnect must echo the id the dropped epoch reached"

    # Drop epoch 2 (cursorless) → reconnect epoch 3 must STILL carry "evt-1" (guard preserved it).
    created[1].server.close_connection(CLOSE_CODES.GOING_AWAY, CloseDirection.REMOTE)
    await _drain()
    await clock.advance(2.0)
    await _drain()
    assert len(created) >= 3, "expected a second reconnect epoch"
    assert calls[2] == "evt-1", "a cursorless epoch must not clobber the held resume cursor"
    await client.close()


async def test_ws_factory_ignores_resume_cursor_arg() -> None:
    """A transport without a last_event_id (the WebSocket case) never clobbers the resume cursor:
    the default WS factory accepts and ignores the arg, and the getattr harvest yields None so the
    supervisor keeps whatever cursor it held."""
    # The default factory is WebSocket; connecting proves it accepts the 2-arg call shape.
    server = FakeServer()
    server.enable_auto_ack()

    def factory(_channels: Sequence[str], last_event_id: str | None = None) -> FakeTransport:
        assert last_event_id is None  # first (and only) connect
        return FakeTransport(server)  # WEBSOCKET_CAPABILITIES, no last_event_id attr

    client = SukkoClient("ws://test", transport_factory=factory, clock=FakeClock(), reconnect=True)
    await client.connect()
    await _drain()
    assert client.state is ConnectionState.CONNECTED
    await client.close()


async def test_connect_raises_when_supervisor_dies_before_connecting() -> None:
    """A transport_factory that raises (e.g. a stale 1-arg factory not migrated to the 2-arg
    signature, or any bad factory) must surface as a connect() failure — not hang forever with the
    error stranded on the dead supervisor task."""

    def bad_factory(_channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
        raise RuntimeError("boom: factory cannot build a transport")

    client = SukkoClient(
        "ws://test", transport_factory=bad_factory, clock=FakeClock(), reconnect=True
    )
    with pytest.raises(SukkoError):  # non-SukkoError is wrapped as ConfigurationError
        await asyncio.wait_for(client.connect(), timeout=1.0)
    await client.close()


def _sse_epoch_recorder() -> tuple[list[list[str]], object]:
    """A factory that records the channel set each SSE epoch is built with and returns an
    SSE-capability FakeTransport per epoch."""
    epochs: list[list[str]] = []

    def factory(channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
        epochs.append(list(channels))
        return FakeTransport(FakeServer(), capabilities=SSE_CAPABILITIES)

    return epochs, factory


async def test_sse_subscribe_on_live_bounces_with_union() -> None:
    """subscribe() on a live SSE stream redials with the union of channels — immediately, no
    backoff (a deliberate bounce, not a failure)."""
    epochs, factory = _sse_epoch_recorder()
    clock = FakeClock()
    client = SukkoClient("ws://t", transport_factory=factory, clock=clock, reconnect=True)
    await client.subscribe(["a"])  # recorded pre-connect (no bounce)
    await client.connect()
    await _drain()
    assert epochs[0] == ["a"]

    await client.subscribe(["b"])  # live SSE → bounce
    await _drain()  # NOTE: no clock.advance — a bounce must not wait on backoff
    assert len(epochs) >= 2, "subscribe on live SSE must redial immediately (no backoff)"
    assert sorted(epochs[1]) == ["a", "b"], f"redial must carry the union, got {epochs[1]}"
    await client.close()


async def test_sse_unsubscribe_on_live_bounces_with_reduced_set() -> None:
    epochs, factory = _sse_epoch_recorder()
    client = SukkoClient("ws://t", transport_factory=factory, clock=FakeClock(), reconnect=True)
    await client.subscribe(["a", "b"])
    await client.connect()
    await _drain()
    assert sorted(epochs[0]) == ["a", "b"]

    await client.unsubscribe(["b"])  # live SSE → bounce with the reduced set
    await _drain()
    assert len(epochs) >= 2
    assert epochs[1] == ["a"], f"redial must carry the reduced set, got {epochs[1]}"
    await client.close()


async def test_sse_unsubscribe_to_empty_parks_then_subscribe_wakes() -> None:
    """Unsubscribing the last channel parks the supervisor (no dial into an empty ?channels=);
    a later subscribe wakes it and dials the new set — ADR-0014's race case."""
    epochs, factory = _sse_epoch_recorder()
    client = SukkoClient("ws://t", transport_factory=factory, clock=FakeClock(), reconnect=True)
    await client.subscribe(["a"])
    await client.connect()
    await _drain()
    assert epochs == [["a"]]

    await client.unsubscribe(["a"])  # empties the desired set → park (no redial)
    await _drain()
    assert len(epochs) == 1, "must not redial into an empty channel set (parked)"

    await client.subscribe(["c"])  # wakes the parked supervisor
    await _drain()
    assert len(epochs) == 2, "a subscribe must wake the parked supervisor and dial"
    assert epochs[1] == ["c"]
    await client.close()


async def test_sse_first_connect_empty_raises() -> None:
    """First connect() on SSE with no channels is a caller error (ADR-0014): the SSE transport
    rejects an empty set and connect() surfaces it rather than hanging."""

    def factory(channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
        if not channels:
            raise ValueError("SSE requires at least one channel")  # mirrors SseTransport ctor
        return FakeTransport(FakeServer(), capabilities=SSE_CAPABILITIES)

    client = SukkoClient("ws://t", transport_factory=factory, clock=FakeClock(), reconnect=True)
    with pytest.raises(SukkoError):
        await asyncio.wait_for(client.connect(), timeout=1.0)  # no subscribe → empty → raises
    await client.close()


async def test_sse_park_survives_stray_wake_and_stays_revivable() -> None:
    """A stray subscribe/unsubscribe while parked (empty desired set) must NOT redial an empty
    ?channels= — with the real SSE transport that dies on empty, so a stray wake would strand the
    client. The park re-checks emptiness on each wake and stays parked until a real subscribe."""
    dialed: list[list[str]] = []

    def factory(channels: Sequence[str], _last_event_id: str | None = None) -> FakeTransport:
        if not channels:
            raise ValueError("SSE requires at least one channel")  # mirrors SseTransport ctor
        dialed.append(list(channels))
        return FakeTransport(FakeServer(), capabilities=SSE_CAPABILITIES)

    client = SukkoClient("ws://t", transport_factory=factory, clock=FakeClock(), reconnect=True)
    await client.subscribe(["a"])
    await client.connect()
    await _drain()
    await client.unsubscribe(["a"])  # empties → park
    await _drain()
    await client.unsubscribe(["never-subscribed"])  # stray wake: must not dial empty / die
    await _drain()
    await client.subscribe(["c"])  # client must still be alive → dials ["c"]
    await _drain()
    assert dialed == [["a"], ["c"]], f"a stray wake redialed empty / stranded the client: {dialed}"
    await client.close()


async def test_sse_bounce_redials_even_with_reconnect_false() -> None:
    """A deliberate bounce is not a failure, so subscribe on a live SSE stream redials even when
    reconnect=False — it must not silently disconnect the client."""
    epochs, factory = _sse_epoch_recorder()
    client = SukkoClient("ws://t", transport_factory=factory, clock=FakeClock(), reconnect=False)
    await client.subscribe(["a"])
    await client.connect()
    await _drain()
    assert epochs == [["a"]]

    await client.subscribe(["b"])  # live SSE bounce — must redial despite reconnect=False
    await _drain()
    assert len(epochs) == 2, "bounce must redial even with reconnect=False"
    assert sorted(epochs[1]) == ["a", "b"]
    assert client.state is ConnectionState.CONNECTED
    await client.close()
