"""SSE transport tests — the real open()/error mapping (Pro-gate, tenant-limit) and
Last-Event-ID resume via httpx.MockTransport (the SSE parser is covered in test_transport)."""

from __future__ import annotations

import httpx
import pytest

from sukko.errors import EditionRequiredError, TenantLimitExceededError
from sukko.transport.base import ConnectionState
from sukko.transport.sse import SseTransport

_STREAM_HEADERS = {"content-type": "text/event-stream"}


async def test_open_success_streams_and_delivers() -> None:
    frame = '{"type":"message","seq":1,"ts":0,"channel":"acme.a","data":{}}'
    body = f"id: 1\nevent: message\ndata: {frame}\n\n"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/sse"
        assert "channels=acme.a" in str(request.url)
        assert request.headers["Authorization"] == "Bearer jwt"
        return httpx.Response(200, text=body, headers=_STREAM_HEADERS)

    transport = SseTransport(
        "https://h", ["acme.a"], token="jwt", transport=httpx.MockTransport(handler)
    )
    await transport.open()
    assert transport.state is ConnectionState.CONNECTED
    assert b'"channel":"acme.a"' in await transport.recv()
    await transport.close()


async def test_open_pro_gate_raises_edition_required() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"code": "EDITION_LIMIT", "message": "SSE requires Pro"})

    transport = SseTransport(
        "https://h", ["acme.a"], token="jwt", transport=httpx.MockTransport(handler)
    )
    with pytest.raises(EditionRequiredError):
        await transport.open()
    assert transport.state is ConnectionState.ERROR


async def test_open_tenant_limit_raises() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"code": "TENANT_LIMIT_EXCEEDED", "message": "cap"})

    transport = SseTransport(
        "https://h", ["acme.a"], token="jwt", transport=httpx.MockTransport(handler)
    )
    with pytest.raises(TenantLimitExceededError):
        await transport.open()


async def test_last_event_id_resume_header_is_sent() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["last_event_id"] = request.headers.get("Last-Event-ID")
        return httpx.Response(200, text=": keepalive\n\n", headers=_STREAM_HEADERS)

    transport = SseTransport(
        "https://h",
        ["acme.a"],
        token="jwt",
        last_event_id="42",
        transport=httpx.MockTransport(handler),
    )
    await transport.open()
    assert seen["last_event_id"] == "42"
    await transport.close()
