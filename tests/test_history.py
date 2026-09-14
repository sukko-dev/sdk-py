"""History flow — client.history() sends a History frame, history-flagged messages
flow through messages() terminated by history_complete, and the client-side limit guard holds."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from fakes import FakeServer, FakeTransport
from sukko.client import SukkoClient
from sukko.errors import ConfigurationError, NotConnectedError
from sukko.messages import History, HistoryComplete, Message


async def _drain(times: int = 6) -> None:
    for _ in range(times):
        await asyncio.sleep(0)


def _client(server: FakeServer, **kwargs: object) -> SukkoClient:
    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server)

    return SukkoClient("ws://test", transport_factory=factory, **kwargs)  # type: ignore[arg-type]


async def test_history_request_flows_flagged_messages_then_complete() -> None:
    server = FakeServer()

    def responder(srv: FakeServer, msg: object) -> None:
        if isinstance(msg, History):
            channel = msg.data.channel
            srv.push(Message(seq=1, ts=0, channel=channel, data={"h": 1}, history=True))
            srv.push(HistoryComplete(channel=channel, count=1, source="cache"))

    server.responder = responder
    client = _client(server)
    await client.connect()
    await client.history("acme.a", limit=10)
    await _drain()
    item = await anext(client.messages())
    assert isinstance(item, Message)
    assert item.history is True  # historical record flows through the same stream
    # the History frame was actually sent
    assert any(isinstance(m, History) and m.data.limit == 10 for m in server.sent_messages)
    await client.close()


async def test_history_rejects_limit_above_client_history_limit() -> None:
    server = FakeServer()
    server.enable_auto_ack()
    client = _client(server, history_limit=50)
    await client.connect()
    with pytest.raises(ConfigurationError):
        await client.history("acme.a", limit=999)  # exceeds the client knob
    await client.close()


async def test_history_when_not_connected_raises() -> None:
    client = _client(FakeServer())
    with pytest.raises(NotConnectedError):
        await client.history("acme.a")
