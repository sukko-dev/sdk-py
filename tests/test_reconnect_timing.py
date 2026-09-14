"""Reconnect/heartbeat timing — deterministic backoff/jitter + pong-liveness via FakeClock."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from fakes import FakeClock, FakeServer, FakeTransport
from sukko._clock import full_jitter_backoff
from sukko.client import SukkoClient
from sukko.messages import Heartbeat
from sukko.transport.base import SSE_CAPABILITIES, ConnectionState


async def _drain(times: int = 6) -> None:
    for _ in range(times):
        await asyncio.sleep(0)


def test_full_jitter_backoff_formula_and_cap() -> None:
    clock = FakeClock(jitter=0.5)
    assert full_jitter_backoff(0, base=1.0, cap=30.0, clock=clock) == 0.5  # 0.5 * min(30, 1)
    assert full_jitter_backoff(1, base=1.0, cap=30.0, clock=clock) == 1.0  # 0.5 * 2
    assert full_jitter_backoff(2, base=1.0, cap=30.0, clock=clock) == 2.0  # 0.5 * 4
    assert full_jitter_backoff(10, base=1.0, cap=30.0, clock=clock) == 15.0  # 0.5 * capped 30


def test_full_jitter_backoff_rejects_negative_attempt() -> None:
    with pytest.raises(ValueError):
        full_jitter_backoff(-1, base=1.0, cap=30.0, clock=FakeClock())


def test_should_retry_zero_is_unlimited_and_n_is_exactly_n() -> None:
    # reconnect_attempts=0 = UNLIMITED (not disabled), and N = exactly N reconnects
    # (attempts 0..N-1). The old `(attempt+1) < N` disabled 0 and gave only N-1 (off-by-one vs js).
    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(FakeServer())

    unlimited = SukkoClient(
        "ws://t", transport_factory=factory, clock=FakeClock(), reconnect=True, reconnect_attempts=0
    )
    assert all(unlimited._should_retry(a) for a in range(100))  # never gives up

    capped = SukkoClient(
        "ws://t", transport_factory=factory, clock=FakeClock(), reconnect=True, reconnect_attempts=3
    )
    assert [capped._should_retry(a) for a in range(5)] == [True, True, True, False, False]

    off = SukkoClient(
        "ws://t",
        transport_factory=factory,
        clock=FakeClock(),
        reconnect=False,
        reconnect_attempts=5,
    )
    assert not off._should_retry(0)  # reconnect=False disables regardless of the count


async def test_no_heartbeat_on_receive_only_sse() -> None:
    # A client-sent heartbeat needs a send-capable transport. On receive-only SSE
    # (can_send=False) the heartbeat is gated off — no frame is sent and no pong-timeout fires.
    clock = FakeClock()
    server = FakeServer()

    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server, capabilities=SSE_CAPABILITIES)

    client = SukkoClient(
        "ws://test",
        transport_factory=factory,
        clock=clock,
        heartbeat_interval=30.0,
        heartbeat_timeout=5.0,
    )
    await client.connect()
    await _drain()
    await clock.advance(30.0)  # a WS client would send a heartbeat here...
    await _drain()
    await clock.advance(5.0)  # ...and a pong-timeout would close here
    await _drain()
    assert not any(isinstance(m, Heartbeat) for m in server.sent_messages)  # never sent one
    assert client.state is ConnectionState.CONNECTED  # and did not disconnect
    await client.close()


async def test_heartbeat_stays_alive_when_pong_received() -> None:
    clock = FakeClock()
    server = FakeServer()
    server.enable_auto_ack()  # auto-acks the heartbeat with a Pong

    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server)

    client = SukkoClient(
        "ws://test",
        transport_factory=factory,
        clock=clock,
        heartbeat_interval=30.0,
        heartbeat_timeout=5.0,
    )
    await client.connect()
    await _drain()
    await clock.advance(30.0)  # heartbeat sent; server auto-acks a Pong
    await _drain()
    await clock.advance(5.0)  # a frame (the Pong) arrived within the timeout → still alive
    await _drain()
    assert client.state is ConnectionState.CONNECTED  # did NOT reconnect
    await client.close()
