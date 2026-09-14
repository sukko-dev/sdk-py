"""Transport abstraction — the seam between the client core and a concrete wire protocol.

Two implementations plug in behind this Protocol — ``WebSocketTransport`` and ``SseTransport``.
**Recovery, heartbeat, and reconnect live in the
client, not the transport** (parity with ``@sukko/sdk``) — a transport only opens, sends, receives,
and closes one connection.

**Idiomatic deviation (§XI, documented):** ``@sukko/sdk``'s transport is an ``EventEmitter`` with
``on(event, cb)``. This SDK uses a **pull model** instead — ``await transport.recv()`` returns the
next frame and *raises* ``ConnectionClosedError`` (carrying code + direction) when the connection
closes. Pull + exceptions is the idiomatic asyncio shape (it mirrors ``websockets``'
own ``recv()``), composes with ``TaskGroup``/cancellation, and lets the client drive back-pressure
by simply not calling ``recv()`` while its delivery queue is full.

**Back-pressure (§XV, capability-gated — never runtime-sniffed):** ``can_pause_receive`` declares
whether *not reading* produces real TCP back-pressure. WebSocket: yes — ``websockets`` buffers up to
``max_queue`` frames then stops reading the socket. SSE-over-httpx: no — so the client falls back to
a bounded buffer + explicit overflow policy for that transport. ``pause()``/``resume()`` gate the
read so the client can proactively stop draining; on a non-pausable transport they are no-ops.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from ..constants import CLOSE_CODES, CloseDirection


class ConnectionState(StrEnum):
    """The five connection states (mirrors ``@sukko/sdk`` ``types.ts``).

    ``RECONNECTING`` is a *client*-level state (the client owns reconnect); a transport only ever
    reports ``CONNECTING`` → ``CONNECTED`` → ``DISCONNECTED`` / ``ERROR`` for its single connection.
    """

    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class TransportCapabilities:
    """What a transport can do — declared explicitly so the client branches on capability, never on
    runtime type-sniffing (§XV)."""

    can_send: bool  # can send arbitrary client frames mid-connection (WS: yes; SSE: receive-only)
    can_publish: bool  # can publish over this transport (WS: yes; SSE: publishes via REST instead)
    can_subscribe: bool  # (un)subscribe mid-connection (WS: yes; SSE: connect-time only)
    can_pause_receive: bool  # not-reading yields real TCP back-pressure (WS: yes; SSE: no)


@dataclass(frozen=True, slots=True)
class CloseInfo:
    """Why a connection closed. ``direction`` disambiguates the overloaded 4000 code (remote
    ``force_disconnect`` vs local heartbeat-timeout)."""

    code: int
    direction: CloseDirection
    reason: str = ""


#: WebSocket: full-duplex, pausable — the reference transport.
WEBSOCKET_CAPABILITIES = TransportCapabilities(
    can_send=True, can_publish=True, can_subscribe=True, can_pause_receive=True
)

#: SSE: receive-only over a one-way HTTP stream; publish + (un)subscribe happen out-of-band (REST /
#: connect-time channels), and stopping the read does not give clean TCP back-pressure.
SSE_CAPABILITIES = TransportCapabilities(
    can_send=False, can_publish=False, can_subscribe=False, can_pause_receive=False
)


@runtime_checkable
class Transport(Protocol):
    """A single-connection wire transport. Implementations own exactly one connection lifecycle."""

    #: Static capability set — read by the client to gate back-pressure, publish, and subscribe.
    capabilities: TransportCapabilities

    @property
    def state(self) -> ConnectionState:
        """The transport's current connection state."""
        ...

    async def open(self) -> None:
        """Establish the connection (handshake + auth). Raises a typed ``SukkoError``
        (e.g. ``TenantLimitExceededError`` on a handshake 429) on failure."""
        ...

    async def send(self, data: bytes) -> None:
        """Send one already-encoded client frame. Requires ``capabilities.can_send``."""
        ...

    async def recv(self) -> bytes:
        """Return the next server frame. Raises :class:`~sukko.errors.ConnectionClosedError` (with
        code + direction) when the connection closes. Blocks while paused."""
        ...

    def pause(self) -> None:
        """Stop reading so TCP back-pressure engages. No-op when ``not can_pause_receive``."""
        ...

    def resume(self) -> None:
        """Resume reading after :meth:`pause`. No-op when ``not can_pause_receive``."""
        ...

    async def close(self, code: int = CLOSE_CODES.NORMAL, reason: str = "") -> None:
        """Close the connection (idempotent). ``code`` is the local close code the SDK initiates."""
        ...
