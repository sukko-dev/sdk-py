"""SSE transport over an ``httpx`` stream (Pro-gated).

Derived from the gateway OpenAPI ``GET /sse``: channels are a **connect-time**, comma-separated,
required query param; ``Last-Event-ID`` resumes after a drop; the body is ``text/event-stream`` with
``id:`` / ``event: message`` / ``data: {json}`` records plus periodic ``: keepalive`` comments.
There is no ``retry:`` field and no subprotocol.

Its capabilities diverge from WebSocket **explicitly** (§XV), not silently: it is receive-only
(``can_send``/``can_publish``/``can_subscribe`` = ``False`` — publish via REST, (un)subscribe is
connect-time only, auth-refresh is a reconnect), and stopping the read gives no clean TCP
back-pressure (``can_pause_receive=False`` → client uses a bounded buffer + overflow policy).

Held open in httpx's **manual streaming mode** (``send(..., stream=True)``) so it spans many
:meth:`recv` calls; :meth:`close` must ``aclose`` it. The ``read`` timeout is disabled (the stream
is long-lived; ``: keepalive`` comments are consumed and ignored — an SSE idle-timeout is a v1
non-goal).
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import msgspec

from .._redact import register_secret
from ..constants import CLOSE_CODES, SUKKO_DEFAULTS, CloseDirection
from ..errors import SukkoError, TransportError, error_from_close, error_from_http_status
from .base import SSE_CAPABILITIES, ConnectionState, TransportCapabilities


class SseTransport:
    """A single SSE connection. Implements :class:`~.base.Transport` (receive-only)."""

    capabilities: TransportCapabilities = SSE_CAPABILITIES

    def __init__(
        self,
        base_url: str,
        channels: list[str],
        *,
        token: str | None = None,
        api_key: str | None = None,
        auth_via: str = "header",
        last_event_id: str | None = None,
        connect_timeout: float = SUKKO_DEFAULTS.CONNECTION_TIMEOUT,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not channels:
            raise ValueError("SseTransport requires at least one channel (connect-time subscribe)")
        self._base_url = base_url.rstrip("/")
        self._channels = channels
        self._token = token
        self._api_key = api_key
        self._auth_via = auth_via
        self._last_event_id = last_event_id
        self._connect_timeout = connect_timeout
        self._transport = transport  # test seam (httpx.MockTransport); production leaves it None
        self._client: httpx.AsyncClient | None = None
        self._response: httpx.Response | None = None
        self._lines: AsyncIterator[str] | None = None  # from response.aiter_lines()
        self._state = ConnectionState.DISCONNECTED
        register_secret(token, api_key)

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def last_event_id(self) -> str | None:
        """Most recent event ``id`` — pass to a fresh transport to resume via ``Last-Event-ID``."""
        return self._last_event_id

    def _url_and_headers(self) -> tuple[str, dict[str, str]]:
        params: dict[str, str] = {"channels": ",".join(self._channels)}
        headers: dict[str, str] = {"Accept": "text/event-stream"}
        if self._last_event_id is not None:
            headers["Last-Event-ID"] = self._last_event_id
        if self._auth_via == "query":
            if self._token:
                params["token"] = self._token
            if self._api_key:
                params["api_key"] = self._api_key
        else:
            if self._token:
                headers["Authorization"] = f"Bearer {self._token}"
            if self._api_key:
                headers["X-API-Key"] = self._api_key
        return f"{self._base_url}/sse?{httpx.QueryParams(params)}", headers

    async def open(self) -> None:
        self._state = ConnectionState.CONNECTING
        url, headers = self._url_and_headers()
        # read=None: the SSE body is long-lived; keepalive comments hold the connection open.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._connect_timeout, read=None), transport=self._transport
        )
        try:
            request = self._client.build_request("GET", url, headers=headers)
            self._response = await self._client.send(request, stream=True)
        except httpx.HTTPError as exc:
            self._state = ConnectionState.ERROR
            await self._teardown()
            raise TransportError(f"sse connect failed: {exc}") from exc

        if self._response.status_code != 200:
            body = await self._response.aread()
            status = self._response.status_code
            await self._teardown()
            self._state = ConnectionState.ERROR
            raise self._error_for_body(status, body)

        self._lines = self._response.aiter_lines()
        self._state = ConnectionState.CONNECTED

    @staticmethod
    def _error_for_body(status: int, body: bytes) -> SukkoError:
        code: str | None = None
        message: str | None = None
        try:
            decoded = msgspec.json.decode(body, type=dict[str, object])
            raw_code = decoded.get("code")
            raw_message = decoded.get("message")
            code = raw_code if isinstance(raw_code, str) else None
            message = raw_message if isinstance(raw_message, str) else None
        except (msgspec.DecodeError, ValueError):
            pass
        return error_from_http_status(status, code=code, message=message, handshake=True)

    async def send(self, data: bytes) -> None:
        raise TransportError(
            "SSE is receive-only — publish via REST (can_send=False)", retryable=False
        )

    async def recv(self) -> bytes:
        """Return the ``data`` payload of the next ``message`` event, parsing SSE framing inline."""
        lines = self._lines
        if lines is None:
            raise TransportError("recv on a closed SSE transport", retryable=False)

        event_type = "message"
        data_parts: list[str] = []
        try:
            async for line in lines:
                if line == "":  # blank line dispatches the buffered event
                    payload = "\n".join(data_parts)
                    is_message = event_type == "message"
                    event_type, data_parts = "message", []  # reset on EVERY dispatch (SSE spec)
                    if payload and is_message:
                        return payload.encode()
                    continue
                if line.startswith(":"):  # comment (": keepalive") — consumed and ignored
                    continue
                field, _, value = line.partition(":")
                if value.startswith(" "):
                    value = value[1:]
                if field == "event":
                    event_type = value
                elif field == "data":
                    data_parts.append(value)
                elif field == "id":
                    self._last_event_id = value
        except httpx.HTTPError as exc:
            self._state = ConnectionState.DISCONNECTED
            raise error_from_close(
                CLOSE_CODES.NORMAL, CloseDirection.REMOTE, reason=f"sse stream error: {exc}"
            ) from exc

        # Iterator exhausted → server closed the stream. Treat as a remote disconnect; the client
        # reconnects and resumes from last_event_id.
        self._state = ConnectionState.DISCONNECTED
        raise error_from_close(CLOSE_CODES.NORMAL, CloseDirection.REMOTE, reason="sse stream ended")

    def pause(self) -> None:  # no-op: can_pause_receive=False
        pass

    def resume(self) -> None:  # no-op: can_pause_receive=False
        pass

    async def close(self, code: int = CLOSE_CODES.NORMAL, reason: str = "") -> None:
        self._state = ConnectionState.DISCONNECTED
        await self._teardown()

    async def _teardown(self) -> None:
        self._lines = None
        if self._response is not None:
            await self._response.aclose()
            self._response = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None
