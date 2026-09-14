"""``SukkoClient`` — the async core that wires transports + subscriptions + back-pressure + auth +
recovery into one client.

**Per-epoch supervision (§VI):** an outer ``_run`` loop owns reconnect; each successful connection
opens a fresh ``asyncio.TaskGroup`` running the read-pump + heartbeat + recovery-timer per epoch.
A dropped connection surfaces as ``ConnectionClosedError`` from the read-pump → the group tears down
its siblings → the loop backs off (unless terminal) and reconnects. The ``DeliveryQueue`` behind
:meth:`messages` is **client-lifetime** — it survives epochs so the consumer's ``async for`` never
breaks; recovery frames flow into that same stream.

**Graceful close:** :meth:`close` cancels the supervisor, awaits it (no orphaned tasks / "Task was
destroyed"), fails any pending auth, and signals the queue so ``messages()`` ends after draining.

Parity note (browser-isms dropped): no ``autoConnect``, no module singleton, no visibility/online
reconnect triggers, no ``localStorage`` client-id — a backend/notebook has none. ``connect`` is
always explicit; the resume identity is caller-supplied or per-process (see recovery.py).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import suppress
from typing import Any, ParamSpec, Self

import msgspec

from ._clock import SYSTEM_CLOCK, Clock, full_jitter_backoff
from ._http import HttpApi, http_base_from_ws
from .auth import AuthManager, GetToken
from .backpressure import DeliveryQueue, OverflowPolicy
from .constants import CLOSE_CODES, DEFAULT_HISTORY_LIMIT, DEFAULT_QUEUE_MAXSIZE, SUKKO_DEFAULTS
from .errors import (
    REPLAY_ERROR_CODES,
    ConfigurationError,
    NotConnectedError,
    RecoveryInterruptedError,
    SukkoError,
    TransportError,
    error_from_ws_error,
)
from .messages import (
    Auth,
    AuthAck,
    AuthData,
    AuthError,
    ClientMessage,
    DeliveredItem,
    Error,
    Gap,
    Heartbeat,
    History,
    HistoryComplete,
    HistoryData,
    HistoryError,
    Message,
    PossibleGap,
    Publish,
    PublishAck,
    PublishData,
    PublishError,
    Reconnect,
    ReconnectData,
    ReconnectError,
    Replay,
    ReplayComplete,
    ReplayData,
    ReplayMessage,
    ServerMessage,
    Subscribe,
    SubscribeData,
    SubscribeError,
    SubscriptionAck,
    Unsubscribe,
    UnsubscribeData,
    UnsubscribeError,
    UnsubscriptionAck,
    decode_server_message,
    encode_client,
)
from .push import PushClient
from .recovery import (
    Action,
    EmitPossibleGap,
    RaiseRecoveryInterrupted,
    RecoveryEngine,
    SendReconnect,
    SendReplay,
)
from .rest import RestPublisher
from .subscriptions import SubscriptionState
from .transport.base import ConnectionState, Transport
from .transport.websocket import WebSocketTransport

logger = logging.getLogger("sukko.client")

_P = ParamSpec("_P")

#: Builds a fresh transport for a connection epoch, given the channels to resume (used by SSE's
#: connect-time ``channels``; the WebSocket factory ignores them and subscribes dynamically).
TransportFactory = Callable[[Sequence[str]], Transport]

ErrorListener = Callable[[SukkoError], None]
NotGrantedListener = Callable[[frozenset[str]], None]
#: Called with ``(channel, mid)`` on each WS publish ack. ``mid`` is the server-assigned stable
#: message identity subscribers see on the delivered envelope — ``None`` when the publish fans out
#: to multiple topics or the server predates the field.
PublishAckListener = Callable[[str, "str | None"], None]


class SukkoClient:
    """Asyncio-first client for the Sukko real-time platform."""

    def __init__(
        self,
        url: str,
        *,
        token: str | None = None,
        api_key: str | None = None,
        get_token: GetToken | None = None,
        transport_factory: TransportFactory | None = None,
        rest_base_url: str | None = None,
        auth_via: str = "header",
        reconnect: bool = True,
        reconnect_attempts: int = SUKKO_DEFAULTS.RECONNECT_ATTEMPTS,
        reconnect_delay_base: float = SUKKO_DEFAULTS.RECONNECT_DELAY_BASE,
        reconnect_delay_max: float = SUKKO_DEFAULTS.RECONNECT_DELAY_MAX,
        heartbeat_interval: float = SUKKO_DEFAULTS.HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = SUKKO_DEFAULTS.HEARTBEAT_TIMEOUT,
        connection_timeout: float = SUKKO_DEFAULTS.CONNECTION_TIMEOUT,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
        overflow_policy: OverflowPolicy = "drop_oldest",
        history_limit: int = DEFAULT_HISTORY_LIMIT,
        client_id: str | None = None,
        replay_floor: float = SUKKO_DEFAULTS.REPLAY_FLOOR,
        refresh_min_interval: float = SUKKO_DEFAULTS.REFRESH_MIN_INTERVAL,
        recovery_deadline: float = SUKKO_DEFAULTS.RECOVERY_DEADLINE,
        clock: Clock = SYSTEM_CLOCK,
        on_error: ErrorListener | None = None,
        on_not_granted: NotGrantedListener | None = None,
        on_publish_ack: PublishAckListener | None = None,
    ) -> None:
        if auth_via not in ("header", "query"):
            raise ConfigurationError(f"auth_via must be 'header' or 'query', got {auth_via!r}")
        if overflow_policy not in ("drop_oldest", "drop_newest"):
            raise ConfigurationError(
                f"overflow_policy must be 'drop_oldest' or 'drop_newest', got {overflow_policy!r}"
            )
        self._url = url
        self._auth_via = auth_via
        self._clock = clock
        self._reconnect = reconnect
        self._reconnect_attempts = reconnect_attempts
        self._delay_base = reconnect_delay_base
        self._delay_max = reconnect_delay_max
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._connection_timeout = connection_timeout
        self._history_limit = history_limit
        self._on_error = on_error
        self._on_not_granted = on_not_granted
        self._on_publish_ack = on_publish_ack

        self._subscriptions = SubscriptionState()
        self._auth = AuthManager(
            token=token,
            api_key=api_key,
            get_token=get_token,
            send_auth=self._send_auth_token,
            clock=clock,
            refresh_min_interval=refresh_min_interval,
        )
        self._recovery = RecoveryEngine(
            client_id=client_id,
            clock=clock,
            replay_floor=replay_floor,
            recovery_deadline=recovery_deadline,
        )
        self._queue = DeliveryQueue(
            queue_maxsize,
            can_pause_receive=True,  # revised per-epoch from the actual transport's capabilities
            history_limit=history_limit,
            overflow_policy=overflow_policy,
        )
        self._transport_factory = transport_factory or self._default_transport_factory

        # REST surfaces (publish + push) share one authed HTTP client, reading credentials fresh
        # so a rotated token is picked up. Usable without a WS connection.
        self._http = HttpApi(
            rest_base_url or http_base_from_ws(url),
            credentials=lambda: (self._auth.token, self._auth.api_key),
            auth_via=auth_via,
            timeout=connection_timeout,
        )
        self._rest = RestPublisher(self._http)
        self.push = PushClient(self._http)  #: ``client.push`` (Web Push = Pro; mobile = Enterprise)

        self._transport: Transport | None = None
        self._state = ConnectionState.DISCONNECTED
        self._supervisor: asyncio.Task[None] | None = None
        self._should_run = False
        self._closed = False
        self._connected_once = False
        self._connect_result: asyncio.Future[None] | None = None
        self._last_frame_at = 0.0
        self._recovery_wake = asyncio.Event()
        self._bg_tasks: set[asyncio.Task[None]] = set()  # tracked fire-and-forget refresh tasks

    # --- public API ---------------------------------------------------------------------------

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def subscriptions(self) -> frozenset[str]:
        """The channels currently granted on the live connection (read-only view)."""
        return self._subscriptions.granted

    async def connect(self) -> None:
        """Start the client and wait until the first connection succeeds (or fails terminally)."""
        if self._supervisor is not None:
            return
        self._should_run = True
        self._connect_result = asyncio.get_running_loop().create_future()
        self._supervisor = asyncio.ensure_future(self._run())
        await self._connect_result

    async def close(self) -> None:
        """Stop the client: cancel the supervisor, await it (no orphaned tasks), fail pending auth,
        and end ``messages()``."""
        if self._closed:
            return
        self._closed = True
        self._should_run = False
        supervisor = self._supervisor
        if supervisor is not None and not supervisor.done():
            supervisor.cancel()
            with suppress(asyncio.CancelledError):
                await supervisor
        for task in list(self._bg_tasks):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await self._auth.aclose()
        await self._http.aclose()
        self._state = ConnectionState.DISCONNECTED
        self._queue.close()

    async def subscribe(self, channels: Sequence[str]) -> None:
        """Subscribe to ``channels``. On a live WS connection the request is sent now; otherwise the
        channels are recorded and applied on the next (re)connect."""
        chans = list(channels)
        self._subscriptions.want(chans)
        transport = self._transport
        if (
            transport is not None
            and self._state is ConnectionState.CONNECTED
            and transport.capabilities.can_subscribe
        ):
            await self._send(transport, Subscribe(data=SubscribeData(channels=chans)))

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        chans = list(channels)
        transport = self._transport
        if (
            transport is not None
            and self._state is ConnectionState.CONNECTED
            and transport.capabilities.can_subscribe
        ):
            await self._send(transport, Unsubscribe(data=UnsubscribeData(channels=chans)))
        self._subscriptions.unwant(chans)

    async def publish(self, channel: str, data: object) -> None:
        """Publish over the WS connection (fire-and-forget; ack/error surface via events). Raises
        :class:`~sukko.errors.NotConnectedError` immediately if not connected (Scenario 3.2)."""
        transport = self._transport
        if transport is None or self._state is not ConnectionState.CONNECTED:
            raise NotConnectedError()
        if not transport.capabilities.can_publish:
            raise TransportError(
                "this transport cannot publish — use rest_publish", retryable=False
            )
        await self._send(transport, Publish(data=PublishData(channel=channel, data=data)))

    async def rest_publish(self, channel: str, data: object) -> str | None:
        """Publish over REST — awaitable, works **without** a WS connection and in all editions.
        Returns the server-assigned stable message identity ``mid`` (``None`` on multi-topic
        fan-out or a pre-field server) — see :class:`~sukko.messages.Message`."""
        return await self._rest.publish(channel, data)

    async def history(self, channel: str, limit: int | None = None) -> None:
        """Request up to ``limit`` historical messages for a subscribed ``channel`` (default
        ``history_limit``). They arrive as ``history: true`` messages through ``messages()``,
        terminated by a ``history_complete``. Available in all editions when the server history
        toggle ``WS_HISTORY_ENABLED`` is on (else a typed ``history_error`` surfaces). Rejects a
        ``limit`` above the client ``history_limit``."""
        transport = self._transport
        if transport is None or self._state is not ConnectionState.CONNECTED:
            raise NotConnectedError()
        effective = self._history_limit if limit is None else limit
        if effective > self._history_limit:
            raise ConfigurationError(
                f"history limit {effective} exceeds the client history_limit {self._history_limit}"
            )
        self._recovery.note_history_request(channel)
        self._recovery_wake.set()  # wake the recovery timer to arm the detection deadline
        await self._send(transport, History(data=HistoryData(channel=channel, limit=effective)))

    async def refresh_token(self) -> None:
        """Force an immediate token refresh (single-flight, floored)."""
        await self._auth.refresh()

    def update_token(self, token: str) -> None:
        """Set the credential for the next connect without sending ``auth``."""
        self._auth.update_token(token)

    async def escalate(self, jwt: str) -> None:
        """Escalate an api-key connection to JWT. On success, re-subscribes the newly-permitted
        delta (the retained not-granted set) — distinct from refresh, which preserves subs."""
        connected = self._transport is not None and self._state is ConnectionState.CONNECTED
        escalated = await self._auth.escalate(jwt, connected=connected)
        if escalated and self._transport is not None:
            delta = sorted(self._subscriptions.not_granted)
            if delta:
                await self._send(self._transport, Subscribe(data=SubscribeData(channels=delta)))

    async def messages(self) -> AsyncIterator[DeliveredItem]:
        """Async-iterate delivered messages and data-loss signals. Ends when the client closes."""
        while True:
            item = await self._queue.get()
            if item is None:  # shutdown sentinel
                return
            yield item

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # --- supervisor ---------------------------------------------------------------------------

    def _running(self) -> bool:
        # A method (not a bare attribute read) so mypy doesn't narrow it away — close() can flip
        # _should_run concurrently while the supervisor is awaiting open()/a task group.
        return self._should_run

    async def _run(self) -> None:
        attempt = 0
        while self._running():
            transport = self._transport_factory(self._subscriptions.resume_channels())
            try:
                await transport.open()
            except SukkoError as exc:
                if not self._running():
                    return
                if self._should_retry(attempt):
                    self._state = ConnectionState.RECONNECTING
                    await self._backoff(attempt)
                    attempt += 1
                    continue
                self._fail_connect(exc)
                self._state = ConnectionState.ERROR
                return

            self._transport = transport
            self._last_frame_at = self._clock.monotonic()
            self._state = ConnectionState.CONNECTED
            # Resolve connect() as soon as we are CONNECTED (before _on_connected sends) so a
            # resume-send failure can never leave connect() awaiting forever.
            if not self._connected_once:
                self._connected_once = True
                self._resolve_connect()
            attempt = 0

            terminal = False
            try:
                try:
                    await self._on_connected(transport)  # reconnect-replay + resume subscriptions
                except SukkoError as exc:
                    # A resume/reconnect send failed (connection dropped again) — skip the pumps and
                    # fall through to reconnect rather than killing the supervisor.
                    logger.warning("resume send failed", extra={"error": str(exc)})
                else:
                    async with asyncio.TaskGroup() as group:
                        group.create_task(self._read_pump(transport))
                        # Heartbeat is WS-only: a client-sent ping needs a send-capable transport.
                        # On receive-only SSE (can_send=False) it would churn every interval (and
                        # can't send), so gate it off — SSE liveness comes from server activity.
                        if transport.capabilities.can_send:
                            group.create_task(self._heartbeat(transport))
                        group.create_task(self._recovery_timer(transport))
            except* SukkoError as eg:
                terminal = any(getattr(e, "terminal", False) for e in eg.exceptions)
            finally:
                self._transport = None
                await self._on_disconnected(transport)
                with suppress(SukkoError):
                    await transport.close()

            if not self._running() or not self._reconnect or terminal:
                self._state = ConnectionState.DISCONNECTED
                return
            self._state = ConnectionState.RECONNECTING
            await self._backoff(attempt)
            attempt += 1
        self._state = ConnectionState.DISCONNECTED

    def _should_retry(self, attempt: int) -> bool:
        # 0 = unlimited; else retry attempts 0..N-1 (N reconnects total — parity with sukko-js's
        # `attempt >= N` stop; the old `(attempt + 1) < N` gave only N-1). Disabling reconnect is
        # the separate `reconnect=False` flag.
        return self._reconnect and (
            self._reconnect_attempts == 0 or attempt < self._reconnect_attempts
        )

    async def _backoff(self, attempt: int) -> None:
        delay = full_jitter_backoff(
            attempt, base=self._delay_base, cap=self._delay_max, clock=self._clock
        )
        await self._clock.sleep(delay)

    def _resolve_connect(self) -> None:
        if self._connect_result is not None and not self._connect_result.done():
            self._connect_result.set_result(None)

    def _fail_connect(self, exc: SukkoError) -> None:
        if self._connect_result is not None and not self._connect_result.done():
            self._connect_result.set_exception(exc)

    # --- connection lifecycle -----------------------------------------------------------------

    async def _on_connected(self, transport: Transport) -> None:
        # Reconnect-with-replay first (Kafka), then resume subscriptions. On the first connect both
        # are empty (the caller subscribes after connect()).
        if not transport.capabilities.can_subscribe:
            return  # SSE: channels are connect-time (baked into the transport by the factory)
        # build_reconnect() reads connected_once BEFORE mark_connected() flips it: a first connect
        # has nothing to resume/probe; the second connect onward probes with an empty last_pos so a
        # pure-Direct backend is detected via not_available rather than silently dropping.
        reconnect = self._recovery.build_reconnect()
        self._recovery.mark_connected()
        if reconnect is not None:
            await self._send(
                transport,
                Reconnect(
                    data=ReconnectData(client_id=reconnect.client_id, last_pos=reconnect.last_pos)
                ),
            )
        resume = self._subscriptions.resume_channels()
        if resume:
            await self._send(transport, Subscribe(data=SubscribeData(channels=resume)))

    async def _on_disconnected(self, transport: Transport) -> None:
        self._subscriptions.on_disconnect()
        for action in self._recovery.handle_disconnect():
            if isinstance(action, RaiseRecoveryInterrupted):
                self._emit_error(RecoveryInterruptedError(action.reason, channel=action.channel))
        await self._auth.aclose()
        self._recovery_wake.set()

    # --- read-pump + dispatch -----------------------------------------------------------------

    async def _read_pump(self, transport: Transport) -> None:
        while True:
            data = await transport.recv()  # raises ConnectionClosedError on close
            self._last_frame_at = self._clock.monotonic()  # any frame proves liveness
            try:
                message = decode_server_message(data)
            except msgspec.DecodeError:
                # Covers both malformed JSON and schema/unknown-tag ValidationError (a subclass).
                # A single bad frame must not kill the read-pump / supervisor — log and skip.
                logger.warning("dropping undecodable/unknown server frame")
                continue
            await self._dispatch(transport, message)

    async def _dispatch(self, transport: Transport, message: ServerMessage) -> None:
        if isinstance(message, (Message, ReplayMessage)):
            self._recovery.note_pos(message.channel, message.pos)
            # Reset the per-channel idle deadline: a recovery frame proves the server is still
            # streaming, so a slow consumer draining a large replay/history is not mistaken for a
            # stuck stream (fix #3 — the deadline measures server silence, not consumer speed).
            if isinstance(message, ReplayMessage):
                self._recovery.note_replay_message(message.channel)
            elif message.history:
                self._recovery.note_history_message(message.channel)
            await self._queue.put(message)
        elif isinstance(message, Gap):
            await self._queue.put(message)  # surface the loss signal to the consumer too
            await self._run_actions(
                transport, self._recovery.handle_gap(message.channel, message.last_pos)
            )
            self._recovery_wake.set()
        elif isinstance(message, ReplayComplete):
            await self._run_actions(
                transport, self._recovery.handle_replay_complete(message.channel)
            )
        elif isinstance(message, HistoryComplete):
            self._recovery.handle_history_complete(message.channel)
        elif isinstance(message, SubscriptionAck):
            not_granted = self._subscriptions.on_subscription_ack(message.subscribed)
            if not_granted and self._on_not_granted is not None:
                self._safe_notify(self._on_not_granted, not_granted)
        elif isinstance(message, UnsubscriptionAck):
            self._subscriptions.on_unsubscription_ack(message.unsubscribed, forced=message.forced)
        elif isinstance(message, AuthAck):
            self._auth.on_auth_ack(message.data.exp)
        elif isinstance(message, AuthError):
            if self._auth.on_auth_error(message.data.code, message.data.message):
                self._spawn(self._auth.reactive_refresh())
        elif isinstance(message, PublishAck):
            if self._on_publish_ack is not None:
                self._safe_notify(self._on_publish_ack, message.channel, message.mid)
        elif isinstance(message, PublishError):
            self._emit_error(error_from_ws_error("publish_error", message.code, message.message))
        elif isinstance(message, ReconnectError):
            if message.code == "not_available":
                actions = self._recovery.handle_not_available(self._subscriptions.resume_channels())
                await self._run_actions(transport, actions)
            else:
                self._emit_error(
                    error_from_ws_error("reconnect_error", message.code, message.message)
                )
        elif isinstance(message, HistoryError):
            self._recovery.handle_history_complete(message.channel)
            self._emit_error(
                error_from_ws_error(
                    "history_error", message.code, message.message, channel=message.channel
                )
            )
        elif isinstance(message, Error):
            if message.channel is not None and message.code in REPLAY_ERROR_CODES:
                # A replay/recovery error mid-flight — reset the channel FSM and surface a single
                # RecoveryInterruptedError (not a ReplayError now + a deadline-fired one later).
                actions = self._recovery.handle_recovery_failure(message.channel, message.code)
                await self._run_actions(transport, actions)
            else:
                self._emit_error(
                    error_from_ws_error(
                        "error", message.code, message.message, channel=message.channel
                    )
                )
        elif isinstance(message, (SubscribeError, UnsubscribeError)):
            self._emit_error(
                error_from_ws_error(_wire_type(message), message.code, message.message)
            )
        # ReconnectAck / Pong: liveness already recorded; nothing more to do.

    async def _run_actions(self, transport: Transport, actions: list[Action]) -> None:
        for action in actions:
            if isinstance(action, SendReplay):
                await self._send(
                    transport,
                    Replay(data=ReplayData(channel=action.channel, from_pos=action.from_pos)),
                )
            elif isinstance(action, SendReconnect):
                await self._send(
                    transport,
                    Reconnect(
                        data=ReconnectData(client_id=action.client_id, last_pos=action.last_pos)
                    ),
                )
            elif isinstance(action, EmitPossibleGap):
                await self._queue.put(PossibleGap(channel=action.channel))
            elif isinstance(action, RaiseRecoveryInterrupted):
                self._emit_error(RecoveryInterruptedError(action.reason, channel=action.channel))

    # --- heartbeat + recovery timer -----------------------------------------------------------

    async def _heartbeat(self, transport: Transport) -> None:
        while True:
            await self._clock.sleep(self._heartbeat_interval)
            sent_at = self._clock.monotonic()
            await self._send(transport, Heartbeat())
            await self._clock.sleep(self._heartbeat_timeout)
            if self._last_frame_at < sent_at:
                await transport.close(CLOSE_CODES.HEARTBEAT_TIMEOUT, "heartbeat pong timeout")
                return

    async def _recovery_timer(self, transport: Transport) -> None:
        while True:
            deadline = self._recovery.next_deadline()
            now = self._clock.monotonic()
            if deadline is not None and deadline <= now:
                await self._run_actions(transport, self._recovery.due())
                continue
            await self._interruptible_wait(None if deadline is None else max(0.0, deadline - now))

    async def _interruptible_wait(self, delay: float | None) -> None:
        self._recovery_wake.clear()
        waiters: set[asyncio.Future[object]] = {asyncio.ensure_future(self._recovery_wake.wait())}
        if delay is not None:
            waiters.add(asyncio.ensure_future(self._clock.sleep(delay)))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()

    # --- helpers ------------------------------------------------------------------------------

    async def _send(self, transport: Transport, message: ClientMessage) -> None:
        await transport.send(encode_client(message))

    async def _send_auth_token(self, token: str) -> None:
        transport = self._transport
        if transport is None:
            raise NotConnectedError("cannot send auth while disconnected")
        await self._send(transport, Auth(data=AuthData(token=token)))

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> None:
        """Launch a tracked fire-and-forget task (kept referenced so it is not GC'd, and cancelled
        on close so it is never orphaned — §VI)."""
        task = asyncio.ensure_future(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)

    @staticmethod
    def _safe_notify(callback: Callable[_P, None], *args: _P.args, **kwargs: _P.kwargs) -> None:
        """Invoke a user callback, isolating any exception so it can't escape into the read-pump /
        TaskGroup and kill the supervisor (§VI)."""
        try:
            callback(*args, **kwargs)
        except Exception:
            logger.exception("user callback raised")

    def _emit_error(self, error: SukkoError) -> None:
        if self._on_error is None:
            logger.warning("unhandled sukko error", extra={"error": str(error)})
            return
        try:
            self._on_error(error)
        except Exception:
            # A user callback must never escape into the read-pump / TaskGroup and kill the
            # supervisor (§VI). Log and continue.
            logger.exception("on_error callback raised")

    def _default_transport_factory(self, channels: Sequence[str]) -> Transport:
        return WebSocketTransport(
            self._url,
            token=self._auth.token,
            api_key=self._auth.api_key,
            auth_via="query" if self._auth_via == "query" else "header",
            open_timeout=self._connection_timeout,
        )


def _wire_type(message: SubscribeError | UnsubscribeError) -> str:
    return "subscribe_error" if isinstance(message, SubscribeError) else "unsubscribe_error"
