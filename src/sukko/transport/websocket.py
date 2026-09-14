"""WebSocket transport over the ``websockets`` asyncio client.

Full-duplex and pausable: ``websockets`` buffers up to ``max_queue`` frames, then stops reading the
socket — so *not calling* :meth:`recv` produces real TCP back-pressure (``can_pause_receive=True``).

Auth is **header-default** (§IX): ``Authorization: Bearer`` / ``X-API-Key`` handshake headers, so a
credential never lands in a proxy access log. ``auth_via="query"`` opts into query params; the leak
surface in ``websockets`` exception strings is closed by ``_redact``.

Close-code direction (the 4000 disambiguator) is read from the ``ConnectionClosed`` frame — a frame
in ``.rcvd`` means the *remote* closed (``force_disconnect``); only in ``.sent`` means *we* did
(heartbeat-timeout). ``.code`` alone is unreliable (it returns 1006 for a local-only close).
"""

from __future__ import annotations

import asyncio
from typing import Literal
from urllib.parse import urlencode, urlparse, urlunparse

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus

from .._redact import register_secret
from ..constants import CLOSE_CODES, SUKKO_DEFAULTS, CloseDirection
from ..errors import (
    TenantLimitExceededError,
    TransportError,
    error_from_close,
    error_from_http_status,
)
from .base import WEBSOCKET_CAPABILITIES, ConnectionState, TransportCapabilities

AuthVia = Literal["header", "query"]

#: Abnormal closure (RFC 6455) — no close frame was exchanged (network drop).
_ABNORMAL_CLOSURE = 1006


class WebSocketTransport:
    """A single WebSocket connection. Implements :class:`~sukko.transport.base.Transport`."""

    capabilities: TransportCapabilities = WEBSOCKET_CAPABILITIES

    def __init__(
        self,
        url: str,
        *,
        token: str | None = None,
        api_key: str | None = None,
        auth_via: AuthVia = "header",
        open_timeout: float = SUKKO_DEFAULTS.CONNECTION_TIMEOUT,
        max_queue: int | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._api_key = api_key
        self._auth_via = auth_via
        self._open_timeout = open_timeout
        self._max_queue = max_queue
        self._conn: ClientConnection | None = None
        self._state = ConnectionState.DISCONNECTED
        # Read gate: set == reading permitted, cleared == paused. asyncio.Event binds to the running
        # loop lazily (3.10+), so constructing it here is safe. Starts unpaused.
        self._resume_event = asyncio.Event()
        self._resume_event.set()
        # §IX: register credentials so any error/log embedding the connect URL/headers is masked.
        register_secret(token, api_key)

    @property
    def state(self) -> ConnectionState:
        return self._state

    def _build_uri_and_headers(self) -> tuple[str, dict[str, str]]:
        headers: dict[str, str] = {}
        if self._auth_via == "header":
            if self._token:
                headers["Authorization"] = f"Bearer {self._token}"
            if self._api_key:
                headers["X-API-Key"] = self._api_key
            return self._url, headers
        # query mode: append token / api_key as query params (opt-in; §IX redaction covers leaks)
        params: dict[str, str] = {}
        if self._token:
            params["token"] = self._token
        if self._api_key:
            params["api_key"] = self._api_key
        parsed = urlparse(self._url)
        merged = f"{parsed.query}&{urlencode(params)}" if parsed.query else urlencode(params)
        return urlunparse(parsed._replace(query=merged)), headers

    async def open(self) -> None:
        self._state = ConnectionState.CONNECTING
        self._resume_event.set()
        uri, headers = self._build_uri_and_headers()
        try:
            self._conn = await connect(
                uri,
                additional_headers=headers,
                open_timeout=self._open_timeout,
                max_queue=self._max_queue,
            )
        except InvalidStatus as exc:
            self._state = ConnectionState.ERROR
            status = exc.response.status_code
            if status == 429:
                raise TenantLimitExceededError() from exc
            raise error_from_http_status(status, handshake=True) from exc
        except (InvalidHandshake, OSError, TimeoutError) as exc:
            self._state = ConnectionState.ERROR
            raise TransportError(f"websocket handshake failed: {exc}") from exc
        self._state = ConnectionState.CONNECTED

    async def send(self, data: bytes) -> None:
        if self._conn is None:
            raise TransportError("send on a closed transport", retryable=False)
        try:
            await self._conn.send(data)
        except ConnectionClosed as exc:
            self._state = ConnectionState.DISCONNECTED
            code, reason, direction = self._close_details(exc)
            raise error_from_close(code, direction, reason=reason) from exc
        except OSError as exc:
            self._state = ConnectionState.DISCONNECTED
            raise TransportError(f"websocket send failed: {exc}") from exc

    async def recv(self) -> bytes:
        if self._conn is None:
            raise TransportError("recv on a closed transport", retryable=False)
        # Block while paused — the socket is NOT drained, so websockets' max_queue fills → TCP
        # back-pressure engages (the whole point of can_pause_receive).
        await self._resume_event.wait()
        try:
            message = await self._conn.recv()
        except ConnectionClosed as exc:
            self._state = ConnectionState.DISCONNECTED
            code, reason, direction = self._close_details(exc)
            raise error_from_close(code, direction, reason=reason) from exc
        return message if isinstance(message, bytes) else message.encode()

    @staticmethod
    def _close_details(exc: ConnectionClosed) -> tuple[int, str, CloseDirection]:
        """Resolve (code, reason, direction) from a close, reading ``rcvd``/``sent`` — not ``.code``
        (which is 1006 for a local-only close). When BOTH frames are present (we closed and the peer
        echoed), ``rcvd_then_sent`` disambiguates who initiated: ``False`` = we sent first (LOCAL),
        ``True`` = the peer closed and we echoed (REMOTE). Reading ``rcvd`` blindly here would
        misclassify a local 4000 heartbeat-timeout the server echoes as an operator
        force_disconnect — the exact 4000 ambiguity this resolves."""
        if exc.rcvd is not None and exc.sent is not None:
            if exc.rcvd_then_sent:  # peer closed first, we echoed
                return exc.rcvd.code, exc.rcvd.reason, CloseDirection.REMOTE
            return exc.sent.code, exc.sent.reason, CloseDirection.LOCAL  # we closed, peer echoed
        if exc.rcvd is not None:
            return exc.rcvd.code, exc.rcvd.reason, CloseDirection.REMOTE
        if exc.sent is not None:
            return exc.sent.code, exc.sent.reason, CloseDirection.LOCAL
        return _ABNORMAL_CLOSURE, "", CloseDirection.REMOTE  # network drop, no close frame

    def pause(self) -> None:
        self._resume_event.clear()

    def resume(self) -> None:
        self._resume_event.set()

    async def close(self, code: int = CLOSE_CODES.NORMAL, reason: str = "") -> None:
        conn = self._conn
        if conn is None:
            return
        self._conn = None
        self._state = ConnectionState.DISCONNECTED
        # Unblock a paused recv() so the read-pump can observe the close instead of hanging.
        self._resume_event.set()
        await conn.close(code, reason)
