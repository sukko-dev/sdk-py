"""Phase-2 transport unit tests — the network-free logic (fakes, capabilities, WS close-direction,
auth header/query building, the hand-rolled SSE parser). Fully deterministic; no sockets, no sleeps.

The full behavioral matrix that drives transports through the client lives in Phase 5; these cover
the transport-internal logic that a live/integration test would otherwise be the only guard for.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import msgspec
import pytest
from websockets.exceptions import ConnectionClosed
from websockets.frames import Close

from fakes import FakeServer, FakeTransport
from sukko.constants import CLOSE_CODES, CloseDirection
from sukko.errors import ConnectionClosedError, TransportError
from sukko.messages import Message, ServerMessage, Subscribe, SubscribeData, SubscriptionAck
from sukko.transport.base import SSE_CAPABILITIES, WEBSOCKET_CAPABILITIES, ConnectionState
from sukko.transport.sse import SseTransport
from sukko.transport.websocket import WebSocketTransport

# --- capabilities -----------------------------------------------------------------------------


def test_capabilities_are_explicit() -> None:
    assert WEBSOCKET_CAPABILITIES.can_send
    assert WEBSOCKET_CAPABILITIES.can_publish
    assert WEBSOCKET_CAPABILITIES.can_subscribe
    assert WEBSOCKET_CAPABILITIES.can_pause_receive
    # SSE: receive-only and non-pausable — explicit, not runtime-sniffed (§XV).
    assert not SSE_CAPABILITIES.can_send
    assert not SSE_CAPABILITIES.can_publish
    assert not SSE_CAPABILITIES.can_subscribe
    assert not SSE_CAPABILITIES.can_pause_receive


# --- FakeTransport / FakeServer ---------------------------------------------------------------


async def test_fake_transport_roundtrip() -> None:
    server = FakeServer()
    transport = FakeTransport(server)
    await transport.open()
    assert transport.state is ConnectionState.CONNECTED

    server.push(Message(seq=1, ts=10, channel="acme.trades", data={"p": 1}))
    received = msgspec.json.decode(await transport.recv(), type=ServerMessage)
    assert isinstance(received, Message)
    assert received.channel == "acme.trades"

    sub = msgspec.json.encode(Subscribe(data=SubscribeData(channels=["acme.trades"])))
    await transport.send(sub)
    assert len(server.sent_messages) == 1
    assert isinstance(server.sent_messages[0], Subscribe)


async def test_fake_transport_close_signal_raises_with_direction() -> None:
    server = FakeServer()
    transport = FakeTransport(server)
    await transport.open()
    server.close_connection(CLOSE_CODES.POLICY_VIOLATION, CloseDirection.REMOTE)
    with pytest.raises(ConnectionClosedError) as exc_info:
        await transport.recv()
    assert exc_info.value.code == CLOSE_CODES.POLICY_VIOLATION
    assert exc_info.value.direction is CloseDirection.REMOTE
    assert transport.state is ConnectionState.DISCONNECTED


async def test_fake_transport_pause_blocks_recv_until_resume() -> None:
    server = FakeServer()
    transport = FakeTransport(server)
    await transport.open()
    transport.pause()
    server.push(SubscriptionAck(subscribed=["acme.a"], count=1))
    task = asyncio.ensure_future(transport.recv())
    await asyncio.sleep(0)  # 0-delay yield: let recv() reach the pause gate
    assert not task.done(), "recv must block while paused (back-pressure)"
    transport.resume()
    assert msgspec.json.decode(await task, type=ServerMessage).__class__ is SubscriptionAck


async def test_fake_server_auto_ack_subscribe() -> None:
    server = FakeServer()
    server.enable_auto_ack()
    transport = FakeTransport(server)
    await transport.open()
    sub = msgspec.json.encode(Subscribe(data=SubscribeData(channels=["a.b", "a.c"])))
    await transport.send(sub)
    ack = msgspec.json.decode(await transport.recv(), type=ServerMessage)
    assert isinstance(ack, SubscriptionAck)
    assert ack.subscribed == ["a.b", "a.c"]


# --- WebSocket close-direction (the 4000 disambiguator) ---------------------------------------


def test_ws_close_details_remote() -> None:
    exc = ConnectionClosed(rcvd=Close(4000, "force"), sent=None)
    code, _reason, direction = WebSocketTransport._close_details(exc)
    assert (code, direction) == (4000, CloseDirection.REMOTE)


def test_ws_close_details_local() -> None:
    exc = ConnectionClosed(rcvd=None, sent=Close(4000, "pong-timeout"))
    code, _reason, direction = WebSocketTransport._close_details(exc)
    assert (code, direction) == (4000, CloseDirection.LOCAL)


def test_ws_close_details_abnormal() -> None:
    exc = ConnectionClosed(rcvd=None, sent=None)
    code, _reason, direction = WebSocketTransport._close_details(exc)
    assert (code, direction) == (1006, CloseDirection.REMOTE)


# --- WebSocket auth building ------------------------------------------------------------------


def test_ws_header_auth() -> None:
    transport = WebSocketTransport("wss://h/ws", token="jwt-abc", auth_via="header")
    uri, headers = transport._build_uri_and_headers()
    assert uri == "wss://h/ws"
    assert headers["Authorization"] == "Bearer jwt-abc"


def test_ws_query_auth() -> None:
    transport = WebSocketTransport("wss://h/ws", api_key="key-xyz", auth_via="query")
    uri, headers = transport._build_uri_and_headers()
    assert "api_key=key-xyz" in uri
    assert headers == {}


# --- SSE URL + parser -------------------------------------------------------------------------


def test_sse_url_and_headers() -> None:
    transport = SseTransport(
        "https://h/", ["acme.a", "acme.b"], token="jwt", last_event_id="42", auth_via="header"
    )
    url, headers = transport._url_and_headers()
    assert url.startswith("https://h/sse?")
    assert "channels=acme.a" in url  # comma-joined (encoding may escape the comma)
    assert headers["Last-Event-ID"] == "42"
    assert headers["Authorization"] == "Bearer jwt"


async def _lines(*items: str) -> AsyncIterator[str]:
    for item in items:
        yield item


async def test_sse_parser_yields_message_and_tracks_id() -> None:
    transport = SseTransport("https://h", ["acme.a"])
    transport._lines = _lines(
        ": keepalive",
        "id: 7",
        "event: message",
        'data: {"type":"message","seq":1,"ts":9,"channel":"acme.a","data":{"x":1}}',
        "",
    )
    payload = await transport.recv()
    decoded = msgspec.json.decode(payload, type=ServerMessage)
    assert isinstance(decoded, Message)
    assert decoded.channel == "acme.a"
    assert transport.last_event_id == "7"


async def test_sse_send_is_receive_only() -> None:
    transport = SseTransport("https://h", ["acme.a"])
    with pytest.raises(TransportError):
        await transport.send(b"{}")


async def test_sse_stream_end_raises_disconnect() -> None:
    transport = SseTransport("https://h", ["acme.a"])
    transport._lines = _lines()  # immediately exhausted
    with pytest.raises(ConnectionClosedError):
        await transport.recv()
    assert transport.state is ConnectionState.DISCONNECTED
