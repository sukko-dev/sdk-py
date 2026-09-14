"""Awaitable REST publish — publish without a WebSocket.

``POST /api/v1/publish`` (gateway OpenAPI). **Not edition-gated** — REST publish is available in
all editions. A 403 is a permission denial (API-key-only auth, tenant mismatch, or the tenant's
publish rules deny the channel), typed :class:`~sukko.errors.PublishError`. Note: on the Kafka
backend, publishing *into* Kafka additionally requires a routing rule provisioned server-side
whose pattern matches the channel — a missing rule surfaces as a 409
:class:`~sukko.errors.PublishNotRoutableError`, not an edition 403. Unlike WS publish (fire-and-
forget, ack via event), this returns a genuine awaitable result — the gateway's error map (400/403/
409/413/429/503) is surfaced typed by :class:`~sukko._http.HttpApi`.
"""

from __future__ import annotations

from ._http import HttpApi
from .errors import ProtocolError
from .messages import JSONValue

_PUBLISH_PATH = "/api/v1/publish"


class RestPublisher:
    """REST publish over a shared :class:`~sukko._http.HttpApi`."""

    def __init__(self, http: HttpApi) -> None:
        self._http = http

    async def publish(self, channel: str, data: JSONValue) -> str | None:
        """Publish ``data`` to ``channel`` via REST and return the server-assigned stable message
        identity ``mid`` — the same ``mid`` subscribers receive on the delivered envelope (see
        :class:`~sukko.messages.Message`). ``None`` when the publish fans out to multiple topics
        (each produced message gets its own ``mid``) or the server predates the field. Raises a
        typed error on rejection (the 400/403/409/413/429/503 map)."""
        result = await self._http.request(
            "POST", _PUBLISH_PATH, json={"channel": channel, "data": data}
        )
        mid = result.get("mid")
        if mid is not None and not isinstance(mid, str):
            # §II: a present-but-non-string mid is a contract violation, never silently dropped.
            raise ProtocolError(f"publish returned a non-string mid: {mid!r}")
        return mid
