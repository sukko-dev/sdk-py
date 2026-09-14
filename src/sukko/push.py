"""Push subscription management — register/unregister devices, fetch the VAPID key.

**Pro-gated** REST over the gateway (``/api/v1/push/*``) for **Web Push**; the ``android``/``ios``
mobile platforms (FCM/APNs) require **Enterprise**. An insufficient edition yields a typed
:class:`~sukko.errors.EditionRequiredError` (403), an outage a ``ServiceUnavailableError`` (503).

This SDK does **NOT** *receive* push — a Python backend is neither a Web Push nor an FCM/APNs
target (an explicit non-goal). The use-case is registering a mobile app's device token or a
browser's Web Push subscription **on the user's behalf**. There is no list-subscriptions
endpoint, so the caller
persists the returned ``device_id`` (an **int64**, not a string).
"""

from __future__ import annotations

from typing import Any, Literal

from ._http import HttpApi
from ._redact import register_secret
from .errors import ProtocolError

Platform = Literal["web", "android", "ios"]

_SUBSCRIBE_PATH = "/api/v1/push/subscribe"
_VAPID_PATH = "/api/v1/push/vapid-key"


class PushClient:
    """The ``client.push`` namespace — push subscription management (Web Push = Pro; mobile
    FCM/APNs = Enterprise)."""

    def __init__(self, http: HttpApi) -> None:
        self._http = http

    async def subscribe(
        self,
        *,
        platform: Platform,
        channels: list[str],
        token: str | None = None,
        endpoint: str | None = None,
        p256dh_key: str | None = None,
        auth_secret: str | None = None,
    ) -> int:
        """Register a device for push on ``channels`` and return its ``device_id`` (int64).

        ``token`` is required for ``android``/``ios``; ``endpoint``+``p256dh_key``+``auth_secret``
        for ``web`` (the caller persists the returned id — there is no list endpoint)."""
        # §II: validate the per-platform required fields locally rather than round-trip to a 400.
        if platform == "web" and not (endpoint and p256dh_key and auth_secret):
            raise ProtocolError("web push requires endpoint, p256dh_key, and auth_secret")
        if platform in ("android", "ios") and not token:
            raise ProtocolError(f"{platform} push requires a device token")
        # §IX: register the subscriber secrets so they are masked if a failing request embeds them.
        register_secret(token, p256dh_key, auth_secret)
        body: dict[str, Any] = {"platform": platform, "channels": channels}
        if token is not None:
            body["token"] = token
        if endpoint is not None:
            body["endpoint"] = endpoint
        if p256dh_key is not None:
            body["p256dh_key"] = p256dh_key
        if auth_secret is not None:
            body["auth_secret"] = auth_secret
        result = await self._http.request("POST", _SUBSCRIBE_PATH, json=body, edition_gated=True)
        device_id = result.get("device_id")
        if not isinstance(device_id, int) or isinstance(device_id, bool):
            raise ProtocolError(f"push subscribe returned no int64 device_id: {result!r}")
        return device_id

    async def unsubscribe(self, device_id: int) -> None:
        """Remove a device's push registration (``DELETE /api/v1/push/subscribe``)."""
        await self._http.request(
            "DELETE", _SUBSCRIBE_PATH, json={"device_id": device_id}, edition_gated=True
        )

    async def get_vapid_key(self) -> str:
        """Return the tenant's VAPID public key (for a web frontend to create a Web Push sub)."""
        result = await self._http.request("GET", _VAPID_PATH, edition_gated=True)
        public_key = result.get("public_key")
        if not isinstance(public_key, str):
            raise ProtocolError(f"vapid-key returned no public_key: {result!r}")
        return public_key
