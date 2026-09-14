"""REST-publish tests — happy path + gateway error map, via httpx.MockTransport."""

from __future__ import annotations

import httpx
import pytest

from sukko._http import HttpApi
from sukko.errors import (
    PayloadTooLargeError,
    ProtocolError,
    PublishError,
    PublishNotRoutableError,
    RateLimitError,
)
from sukko.rest import RestPublisher


def _publisher(handler: httpx.MockTransport) -> RestPublisher:
    http = HttpApi("https://h", lambda: ("jwt", None), transport=handler)
    return RestPublisher(http)


async def test_publish_success_sends_channel_and_data_and_returns_mid() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization")
        seen["body"] = request.content
        return httpx.Response(
            200, json={"status": "accepted", "channel": "acme.a", "mid": "9c5b1f0a-2-1235"}
        )

    publisher = _publisher(httpx.MockTransport(handler))
    mid = await publisher.publish("acme.a", {"x": 1})
    assert mid == "9c5b1f0a-2-1235"  # the stable identity subscribers see on the delivered copy
    assert seen["url"] == "https://h/api/v1/publish"
    assert seen["auth"] == "Bearer jwt"
    assert b'"channel":"acme.a"' in seen["body"]  # type: ignore[operator]


async def test_publish_mid_absent_returns_none() -> None:
    # pre-field servers and multi-topic fan-out publishes omit `mid` — not an error.
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "accepted", "channel": "acme.a"})

    publisher = _publisher(httpx.MockTransport(handler))
    assert await publisher.publish("acme.a", {"x": 1}) is None


async def test_publish_mistyped_mid_raises_protocol_error() -> None:
    # §II: a present-but-non-string `mid` is a contract violation, never silently coerced/dropped.
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "accepted", "channel": "acme.a", "mid": 42})

    publisher = _publisher(httpx.MockTransport(handler))
    with pytest.raises(ProtocolError):
        await publisher.publish("acme.a", {"x": 1})


async def test_publish_403_permission_denial_is_publish_error() -> None:
    # REST publish is not edition-gated: a 403 is a permission denial (API-key-only auth, tenant
    # mismatch, or publish rules deny), mapped to PublishError — the EDITION_LIMIT mapping is
    # covered on surfaces that are still gated (SSE in test_sse.py, push in test_push.py).
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"code": "FORBIDDEN", "message": "publish denied"})

    publisher = _publisher(httpx.MockTransport(handler))
    with pytest.raises(PublishError):
        await publisher.publish("acme.a", {})


async def test_publish_error_map() -> None:
    cases = [
        (409, "PUBLISH_NOT_ROUTABLE", PublishNotRoutableError),
        (413, "BODY_TOO_LARGE", PayloadTooLargeError),
        (429, "RATE_LIMITED", RateLimitError),
    ]
    for status, code, exc_type in cases:

        def handler(
            _request: httpx.Request, _status: int = status, _code: str = code
        ) -> httpx.Response:
            return httpx.Response(_status, json={"code": _code, "message": "x"})

        publisher = _publisher(httpx.MockTransport(handler))
        with pytest.raises(exc_type):
            await publisher.publish("acme.a", {})


def test_http_base_from_ws_edge_cases() -> None:
    from sukko._http import http_base_from_ws

    assert http_base_from_ws("wss://h/ws") == "https://h"
    assert http_base_from_ws("ws://h:8080/x") == "http://h:8080"
    assert http_base_from_ws("https://h") == "https://h"  # already-http passthrough


async def test_query_auth_puts_credentials_in_url() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={})

    http = HttpApi(
        "https://h",
        lambda: ("jwt", "key"),
        auth_via="query",
        transport=httpx.MockTransport(handler),
    )
    await http.request("GET", "/x")
    assert "token=jwt" in seen["url"] and "api_key=key" in seen["url"]  # type: ignore[operator]


async def test_non_json_error_body_is_still_typed() -> None:
    from sukko.errors import ServiceUnavailableError

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="<html>gateway down</html>")

    http = HttpApi("https://h", lambda: ("j", None), transport=httpx.MockTransport(handler))
    with pytest.raises(ServiceUnavailableError):
        await http.request("GET", "/x")
