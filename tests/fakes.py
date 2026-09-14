"""In-process ``FakeTransport`` + ``FakeServer`` — the backbone of the unit test matrix.

Parity with the TS ``MockTransport``: no sockets, no event loop timers, fully deterministic. A test
scripts the server side (``push`` a frame, ``close_connection`` at a chosen point, or install a
``responder`` for auto-acks) and drives the client through a :class:`FakeTransport` that implements
the :class:`~sukko.transport.base.Transport` protocol.

Encoding note: production code only ever *encodes client* / *decodes server* frames. The fake does
the mirror image (encodes server frames, decodes client frames) with ``msgspec`` directly — the test
util, not the SDK, owns that reverse direction.
"""

from __future__ import annotations

import asyncio
import heapq
from collections.abc import Callable
from dataclasses import dataclass, field

import msgspec

from sukko.constants import CLOSE_CODES, CloseDirection
from sukko.errors import SukkoError, TransportError, error_from_close
from sukko.messages import (
    Auth,
    ClientMessage,
    Heartbeat,
    Pong,
    Publish,
    PublishAck,
    ServerMessage,
    Subscribe,
    SubscriptionAck,
    Unsubscribe,
    UnsubscriptionAck,
)
from sukko.transport.base import (
    WEBSOCKET_CAPABILITIES,
    ConnectionState,
    TransportCapabilities,
)


class FakeClock:
    """Virtual-time :class:`~sukko._clock.Clock` — deterministic substitute for real sleeps (§VII).

    ``sleep`` registers a sleeper keyed by its wake time and blocks; :meth:`advance` moves virtual
    time forward, waking sleepers in deadline order and letting each woken task run (so a task that
    sleeps again re-registers before the next wakes). ``random`` returns a fixed ``jitter`` so
    full-jitter backoff is reproducible.
    """

    def __init__(self, *, start: float = 0.0, jitter: float = 0.5) -> None:
        self._now = start
        self.jitter = jitter
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._seq = 0

    def monotonic(self) -> float:
        return self._now

    def now(self) -> float:
        return self._now

    def random(self) -> float:
        return self.jitter

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self._now + seconds, self._seq, future))
        self._seq += 1
        await future

    async def advance(self, seconds: float) -> None:
        """Advance virtual time by ``seconds``, firing every sleeper due at or before the target."""
        await asyncio.sleep(0)  # let just-created tasks reach their sleep() and register first
        target = self._now + seconds
        while self._sleepers and self._sleepers[0][0] <= target:
            wake, _, future = heapq.heappop(self._sleepers)
            if future.cancelled():
                continue  # a cancelled sleeper (interruptible wait lost the race) — skip
            self._now = wake
            if not future.done():
                future.set_result(None)
            await asyncio.sleep(0)  # let the woken task run (it may register a new sleeper)
        self._now = target
        await asyncio.sleep(0)

    @property
    def pending_sleepers(self) -> int:
        return len(self._sleepers)


@dataclass(slots=True)
class _CloseSignal:
    code: int
    direction: CloseDirection
    reason: str = ""


Responder = Callable[["FakeServer", ClientMessage], None]


@dataclass(slots=True)
class FakeServer:
    """Scriptable server side. Tests push server→client frames and inspect client→server sends."""

    #: Frames the client will receive, in order (bytes or a scripted close).
    _outbound: asyncio.Queue[bytes | _CloseSignal] = field(default_factory=asyncio.Queue)
    #: Raw client frames received, in order.
    inbound: list[bytes] = field(default_factory=list)
    #: Decoded client messages received, in order.
    sent_messages: list[ClientMessage] = field(default_factory=list)
    #: Optional auto-responder invoked on each decoded client message.
    responder: Responder | None = None

    def push(self, message: ServerMessage | bytes) -> None:
        """Enqueue a server→client frame (a typed model or raw bytes)."""
        data = message if isinstance(message, bytes) else msgspec.json.encode(message)
        self._outbound.put_nowait(data)

    def close_connection(
        self, code: int, direction: CloseDirection = CloseDirection.REMOTE, reason: str = ""
    ) -> None:
        """Script a connection close the client's next ``recv()`` will observe."""
        self._outbound.put_nowait(_CloseSignal(code, direction, reason))

    async def _next(self) -> bytes | _CloseSignal:
        return await self._outbound.get()

    def _receive(self, data: bytes) -> None:
        self.inbound.append(data)
        message = msgspec.json.decode(data, type=ClientMessage)
        self.sent_messages.append(message)
        if self.responder is not None:
            self.responder(self, message)

    def enable_auto_ack(self) -> None:
        """Install a responder that auto-acks subscribe/unsubscribe/publish/heartbeat — the common
        case for tests that care about behavior downstream of a successful ack."""

        def respond(server: FakeServer, message: ClientMessage) -> None:
            if isinstance(message, Subscribe):
                channels = message.data.channels or (
                    [message.data.channel] if message.data.channel else []
                )
                server.push(SubscriptionAck(subscribed=list(channels), count=len(channels)))
            elif isinstance(message, Unsubscribe):
                server.push(UnsubscriptionAck(unsubscribed=message.data.channels))
            elif isinstance(message, Publish):
                server.push(
                    PublishAck(channel=message.data.channel, status="accepted", mid="mid-1")
                )
            elif isinstance(message, Heartbeat):
                server.push(Pong(ts=0))
            elif isinstance(message, Auth):
                server.push(b'{"type":"auth_ack","data":{"exp":0}}')

        self.responder = respond


class FakeTransport:
    """A :class:`~sukko.transport.base.Transport` backed by a :class:`FakeServer`. Deterministic —
    no real I/O. ``capabilities`` are configurable so a test can exercise the SSE receive-only path.
    """

    def __init__(
        self,
        server: FakeServer,
        *,
        capabilities: TransportCapabilities = WEBSOCKET_CAPABILITIES,
        fail_open: SukkoError | None = None,
    ) -> None:
        self.server = server
        self.capabilities = capabilities
        self._fail_open = fail_open
        self._state = ConnectionState.DISCONNECTED
        self._closed = False
        self._resume_event = asyncio.Event()
        self._resume_event.set()

    @property
    def state(self) -> ConnectionState:
        return self._state

    async def open(self) -> None:
        self._state = ConnectionState.CONNECTING
        if self._fail_open is not None:
            self._state = ConnectionState.ERROR
            raise self._fail_open
        self._state = ConnectionState.CONNECTED

    async def send(self, data: bytes) -> None:
        if self._closed:
            raise TransportError("send on a closed transport", retryable=False)
        self.server._receive(data)

    async def recv(self) -> bytes:
        if self.capabilities.can_pause_receive:
            await self._resume_event.wait()
        item = await self.server._next()
        if isinstance(item, _CloseSignal):
            self._state = ConnectionState.DISCONNECTED
            raise error_from_close(item.code, item.direction, reason=item.reason)
        return item

    def pause(self) -> None:
        if self.capabilities.can_pause_receive:
            self._resume_event.clear()

    def resume(self) -> None:
        if self.capabilities.can_pause_receive:
            self._resume_event.set()

    async def close(self, code: int = CLOSE_CODES.NORMAL, reason: str = "") -> None:
        if self._closed:
            return
        self._closed = True
        self._state = ConnectionState.DISCONNECTED
        self._resume_event.set()
        # Make a pending/next recv() observe the local close (mirrors the real transport): enqueue a
        # LOCAL close so the read-pump sees a ConnectionClosedError with the right direction.
        self.server.close_connection(code, CloseDirection.LOCAL, reason)
