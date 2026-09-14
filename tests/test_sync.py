"""Sync-wrapper tests — blocking client from a plain script AND from inside a
running loop (the Jupyter/IPython case). Same behavior over the same async core.

The responder pushes frames on the background loop thread (in response to the client's own send), so
the FakeServer's queue is only ever touched from that loop — never cross-thread.
"""

from __future__ import annotations

from collections.abc import Sequence

from fakes import FakeServer, FakeTransport
from sukko.messages import Message, Subscribe, SubscriptionAck
from sukko.sync import SyncSukkoClient
from sukko.transport.base import ConnectionState


def _server_that_delivers_on_subscribe() -> FakeServer:
    server = FakeServer()

    def responder(srv: FakeServer, msg: object) -> None:
        if isinstance(msg, Subscribe):
            channels = msg.data.channels or []
            srv.push(SubscriptionAck(subscribed=list(channels), count=len(channels)))
            if channels:
                srv.push(Message(seq=1, ts=0, channel=channels[0], data={"hi": 1}))

    server.responder = responder
    return server


def _sync_client(server: FakeServer) -> SyncSukkoClient:
    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server)

    return SyncSukkoClient("ws://test", transport_factory=factory)


def test_sync_client_from_a_plain_script() -> None:
    """No running loop on the calling thread — the plain-script case."""
    client = _sync_client(_server_that_delivers_on_subscribe())
    try:
        client.connect()
        assert client.state is ConnectionState.CONNECTED
        client.subscribe(["acme.a"])
        item = next(client.stream())  # blocking — the background loop delivers
        assert isinstance(item, Message)
        assert item.data == {"hi": 1}
    finally:
        client.close()
    assert client.state is ConnectionState.DISCONNECTED


async def test_sync_client_within_a_running_loop() -> None:
    """The pytest-asyncio loop on this thread simulates a Jupyter cell's running loop — the
    sync wrapper must still work (its OWN loop on a background thread; never asyncio.run)."""
    client = _sync_client(_server_that_delivers_on_subscribe())
    try:
        client.connect()  # blocks this thread briefly; completes on the background loop
        client.subscribe(["acme.a"])
        item = next(client.stream())
        assert isinstance(item, Message)
    finally:
        client.close()


def test_sync_context_manager() -> None:
    server = _server_that_delivers_on_subscribe()

    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server)

    with SyncSukkoClient("ws://test", transport_factory=factory) as client:
        assert client.state is ConnectionState.CONNECTED
        client.subscribe(["acme.a"])
        assert isinstance(next(client.stream()), Message)


def test_sync_close_idempotent_and_without_connect() -> None:
    server = _server_that_delivers_on_subscribe()

    def factory(_channels: Sequence[str]) -> FakeTransport:
        return FakeTransport(server)

    client = SyncSukkoClient("ws://test", transport_factory=factory)
    client.close()  # close without ever connecting
    client.close()  # idempotent — must not raise (P4)
    assert client.state is ConnectionState.DISCONNECTED


def test_sync_rest_publish_returns_mid() -> None:
    """Sync≡async: the sync wrapper surfaces the REST publish ack's stable ``mid``."""
    import httpx

    from sukko._http import HttpApi
    from sukko.rest import RestPublisher

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "accepted", "channel": "acme.a", "mid": "m-9"})

    client = _sync_client(FakeServer())
    try:
        # inject a mock-backed HttpApi (the same test seam as the async client tests)
        inner = client._client
        inner._http = HttpApi(
            "https://h", lambda: ("jwt", None), transport=httpx.MockTransport(handler)
        )
        inner._rest = RestPublisher(inner._http)
        assert client.rest_publish("acme.a", {"x": 1}) == "m-9"  # no connect() needed
    finally:
        client.close()
