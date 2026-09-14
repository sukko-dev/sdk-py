"""Push tests — subscribe/unsubscribe/get_vapid_key + edition/availability."""

from __future__ import annotations

import httpx
import pytest

from sukko._http import HttpApi
from sukko._redact import default_redactor
from sukko.errors import EditionRequiredError, ProtocolError, ServiceUnavailableError
from sukko.push import PushClient


def _push(handler: httpx.MockTransport) -> PushClient:
    return PushClient(HttpApi("https://h", lambda: ("jwt", None), transport=handler))


async def test_subscribe_returns_int64_device_id() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.content
        return httpx.Response(201, json={"device_id": 12345})

    push = _push(httpx.MockTransport(handler))
    device_id = await push.subscribe(platform="android", channels=["acme.a"], token="fcm-tok")
    assert device_id == 12345
    assert isinstance(device_id, int)
    assert seen["url"] == "https://h/api/v1/push/subscribe"
    assert b'"platform":"android"' in seen["body"]  # type: ignore[operator]


async def test_unsubscribe_deletes_with_device_id() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["body"] = request.content
        return httpx.Response(200, json={"success": True})

    push = _push(httpx.MockTransport(handler))
    await push.unsubscribe(12345)
    assert seen["method"] == "DELETE"
    assert b'"device_id":12345' in seen["body"]  # type: ignore[operator]


async def test_get_vapid_key_returns_public_key() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"public_key": "BNcR-vapid-key"})

    push = _push(httpx.MockTransport(lambda r: handler(r)))
    assert await push.get_vapid_key() == "BNcR-vapid-key"


async def test_push_edition_gate_and_unavailable_are_typed() -> None:
    def gate(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"code": "EDITION_LIMIT", "message": "requires Pro"})

    with pytest.raises(EditionRequiredError):
        await _push(httpx.MockTransport(gate)).get_vapid_key()

    def down(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"code": "SERVICE_UNAVAILABLE", "message": "push down"})

    with pytest.raises(ServiceUnavailableError):
        await _push(httpx.MockTransport(down)).subscribe(
            platform="ios", channels=["a.b"], token="t"
        )


async def test_subscribe_registers_secrets_for_redaction() -> None:
    # regression (P1): push subscribe must register token/p256dh_key/auth_secret so they're masked.
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"device_id": 1})

    await _push(httpx.MockTransport(handler)).subscribe(
        platform="web",
        channels=["a.b"],
        endpoint="https://push/ep",
        p256dh_key="P256REGISTEREDKEY",
        auth_secret="AUTHREGISTEREDSEC",
    )
    assert "P256REGISTEREDKEY" not in default_redactor.redact("leaked P256REGISTEREDKEY here")
    assert "AUTHREGISTEREDSEC" not in default_redactor.redact("leaked AUTHREGISTEREDSEC here")


async def test_push_403_without_code_still_edition_required() -> None:
    # regression (P8): push 403 is only ever the edition gate — map to EditionRequiredError even
    # when the gateway omits the code.
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"message": "nope"})  # no `code`

    with pytest.raises(EditionRequiredError):
        await _push(httpx.MockTransport(handler)).get_vapid_key()


async def test_web_subscribe_requires_endpoint_and_keys() -> None:
    # regression (P11): §II boundary validation, typed as ProtocolError (not a raw ValueError).
    push = _push(httpx.MockTransport(lambda _r: httpx.Response(201, json={"device_id": 1})))
    with pytest.raises(ProtocolError):
        await push.subscribe(platform="web", channels=["a.b"])  # missing endpoint/keys
    with pytest.raises(ProtocolError):
        await push.subscribe(platform="android", channels=["a.b"])  # missing token


async def test_malformed_device_id_raises_protocol_error() -> None:
    # regression (P9): a bad 2xx body is a typed ProtocolError inside the SukkoError hierarchy.
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"device_id": "not-an-int"})

    with pytest.raises(ProtocolError):
        await _push(httpx.MockTransport(handler)).subscribe(
            platform="ios", channels=["a.b"], token="t"
        )
