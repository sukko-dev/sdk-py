"""Typed protocol models for the full AsyncAPI v1.4.0 message set.

Hand-written ``msgspec.Struct`` **tagged unions** discriminated on the wire ``type`` field
(msgspec's default ``tag_field`` is ``"type"``, which matches the contract exactly). msgspec gives
fast, zero-overhead typed decode/encode on the market-data hot path (§XI performant) and rejects
malformed frames at the boundary (§II defense-in-depth) — e.g. a ``gap`` missing its required
``last_pos``.

Contract fidelity (§I):
- Wire ``type`` strings, field names, and required/optional-ness mirror the AsyncAPI exactly.
- Two server messages nest their fields under ``data`` (``auth_ack``, ``auth_error``); the other 16
  are flat. Client→server messages all use the ``{type, data}`` envelope.
- ``message``/``replay_message`` ``data`` is an **opaque, server-owned** JSON payload the SDK passes
  through untouched. ``pos`` is opaque (never parsed; stored and echoed back on reconnect).
  ``mid`` is the opaque **stable message identity** — identical on every delivered copy of the same
  message (live / replay / history); an identity for dedup, never a replay cursor.
- ``history``/``forced``/``truncated`` are omitempty on the wire → **absent means ``False``**.

``PossibleGap`` is an **SDK-internal** event (not a wire type, not in :data:`ServerMessage`) — the
Direct-backend data-loss signal, kept distinct from the wire ``gap`` per §XV (see recovery.py).
"""

from __future__ import annotations

from typing import Any, TypeAlias

import msgspec

#: An arbitrary, server-owned JSON payload. The SDK never inspects it — it decodes to native Python
#: (dict/list/scalar) and hands it to the consumer verbatim, so the publishing service owns its
#: schema (contract: ``data`` is "type: object", but we stay permissive rather than reject).
JSONValue = Any


# =============================================================================================
# Server → client (the full AsyncAPI clientReceive set — 18 types)
# =============================================================================================


class Message(msgspec.Struct, tag="message"):
    """A live or historical broadcast message on a subscribed channel.

    ``seq`` is connection-scoped (starts at 1, **resets every reconnect** — not per-channel).
    ``pos`` is the durable, opaque replay cursor (absent on Direct-backend messages). ``history`` is
    ``True`` only for records replayed via history / subscribe-with-history.

    ``mid`` is the server-assigned **stable message identity**: the same message carries the same
    ``mid`` on every delivered copy — live, gap-replay (:class:`ReplayMessage`), and history —
    unlike ``seq`` (per-connection) and ``pos`` (a replay cursor, not an identity). Use it for
    client-side deduplication (e.g. dropping the overlap between a reconnect replay and messages
    already received), idempotent processing, and cross-delivery correlation. Opaque, at most 64
    characters; never parse it and never send it as a replay cursor (``from_pos``/``last_pos``
    accept ``pos`` values only). ``None`` when the server predates the field.
    """

    seq: int
    ts: int
    channel: str
    data: JSONValue
    history: bool = False  # absent/false on live messages; true for historical replay
    pos: str | None = None  # opaque Kafka cursor; absent on Direct backend
    mid: str | None = None  # stable message identity; identical on every copy of the same message


class _AuthAckData(msgspec.Struct):
    exp: int  # new token expiry in Unix seconds; 0 == no-expiry (never schedule a refresh)


class AuthAck(msgspec.Struct, tag="auth_ack"):
    """Acknowledges a successful auth refresh/escalation. ``data.exp`` drives the refresh timer."""

    data: _AuthAckData


class _AuthErrorData(msgspec.Struct):
    code: str  # invalid_token | token_expired | tenant_mismatch | rate_limited | not_available
    message: str


class AuthError(msgspec.Struct, tag="auth_error"):
    """A failed auth refresh/escalation. Drives a reactive (single-flight, floored) refresh."""

    data: _AuthErrorData


class SubscriptionAck(msgspec.Struct, tag="subscription_ack"):
    """Confirms which channels were subscribed. Channels filtered by permission are **absent** from
    ``subscribed`` — the SDK diffs requested-vs-``subscribed`` and surfaces the not-granted set."""

    subscribed: list[str]
    count: int


class UnsubscriptionAck(msgspec.Struct, tag="unsubscription_ack"):
    """Confirms unsubscribed channels. ``forced=True`` marks a server-forced unsubscribe (auth
    downgrade / permission change); ``count`` is typically absent on forced unsubscribes."""

    unsubscribed: list[str]
    count: int | None = None
    forced: bool = False


class PublishAck(msgspec.Struct, tag="publish_ack"):
    """Confirms a WS publish was accepted (``status`` is always ``"accepted"``).

    ``mid`` is the stable identity assigned to the published message — the same ``mid``
    subscribers see on the delivered envelope (:class:`Message`). ``None`` when the publish fans
    out to multiple topics (each produced message gets its own ``mid``) or the server predates
    the field.
    """

    channel: str
    status: str  # always "accepted" — required by the contract
    mid: str | None = None  # stable identity of the published message; absent on fan-out


class PublishError(msgspec.Struct, tag="publish_error"):
    """A failed WS publish. ``code`` is one of the 11 AsyncAPI ``publish_error`` codes."""

    code: str
    message: str


class ReconnectAck(msgspec.Struct, tag="reconnect_ack"):
    """Acknowledges a reconnect-with-replay. ``messages_replayed`` counts records replayed."""

    status: str
    messages_replayed: int
    message: str


class ReconnectError(msgspec.Struct, tag="reconnect_error"):
    """A failed reconnect-replay. ``not_available`` is a Direct-backend *capability* signal —
    surfaced by recovery as such (degrade to naive resubscribe), not a retryable error."""

    code: str  # invalid_request | not_available | replay_failed
    message: str


class Pong(msgspec.Struct, tag="pong"):
    """Heartbeat response. Any received message (not only ``pong``) clears the pong timer."""

    ts: int


class Error(msgspec.Struct, tag="error"):
    """Generic error: an unparseable client frame or a rejected live replay. ``channel`` is present
    for replay-related errors."""

    code: str
    message: str
    channel: str | None = None


class SubscribeError(msgspec.Struct, tag="subscribe_error"):
    """The subscribe request itself was malformed (``code=invalid_request``)."""

    code: str
    message: str


class UnsubscribeError(msgspec.Struct, tag="unsubscribe_error"):
    """The unsubscribe request itself was malformed (``code=invalid_request``)."""

    code: str
    message: str


class HistoryComplete(msgspec.Struct, tag="history_complete"):
    """Terminates a history delivery for a channel. ``truncated`` absent → treat as ``False``."""

    channel: str
    count: int
    source: str  # cache | kafka | mixed — required by the contract
    truncated: bool = False


class HistoryError(msgspec.Struct, tag="history_error"):
    """A failed history / subscribe-with-history request — includes ``history_disabled`` (server
    toggle off). ``channel`` is always present."""

    code: str
    channel: str
    message: str


class Gap(msgspec.Struct, tag="gap"):
    """Advisory notice that message(s) were dropped (send buffer full). ``last_pos`` is **required**
    — the conservative Kafka cursor to anchor a ``replay``. Only emitted by the Kafka backend."""

    channel: str
    from_seq: int
    to_seq: int
    last_pos: str
    ts: int


class ReplayMessage(msgspec.Struct, tag="replay_message"):
    """A single message delivered during live ``gap``→``replay`` recovery.

    ``mid`` is the same stable identity the message carried (or would have carried) on its live
    and history deliveries — use it to deduplicate replayed messages against ones already
    received (see :class:`Message`). ``None`` when the server predates the field.
    """

    seq: int
    channel: str
    ts: int
    data: JSONValue
    pos: str | None = None
    mid: str | None = None  # stable message identity; identical on every copy of the same message


class ReplayComplete(msgspec.Struct, tag="replay_complete"):
    """Terminates a live replay for a channel. ``truncated`` absent → treat as ``False``."""

    channel: str
    messages_replayed: int
    truncated: bool = False


#: Tagged union of every server→client message. Decode with :func:`decode_server_message`.
ServerMessage: TypeAlias = (
    Message
    | AuthAck
    | AuthError
    | SubscriptionAck
    | UnsubscriptionAck
    | PublishAck
    | PublishError
    | ReconnectAck
    | ReconnectError
    | Pong
    | Error
    | SubscribeError
    | UnsubscribeError
    | HistoryComplete
    | HistoryError
    | Gap
    | ReplayMessage
    | ReplayComplete
)


# =============================================================================================
# Client → server (the full AsyncAPI clientSend set — 8 types, all `{type, data}` envelopes)
# =============================================================================================


class HistoryMode(msgspec.Struct, omit_defaults=True):
    """The optional ``history`` block of a subscribe-with-history request (available in all
    editions when the server history toggle ``WS_HISTORY_ENABLED`` is on)."""

    limit: int


class SubscribeData(msgspec.Struct, omit_defaults=True):
    """Two mutually exclusive modes: multi-channel (``channels``) OR single-channel-with-history
    (``channel`` + ``history``). Unset fields are omitted on encode (§XV: explicit modes)."""

    channels: list[str] | None = None
    channel: str | None = None
    history: HistoryMode | None = None


class Subscribe(msgspec.Struct, tag="subscribe"):
    data: SubscribeData


class UnsubscribeData(msgspec.Struct):
    channels: list[str]


class Unsubscribe(msgspec.Struct, tag="unsubscribe"):
    data: UnsubscribeData


class PublishData(msgspec.Struct):
    channel: str
    data: JSONValue


class Publish(msgspec.Struct, tag="publish"):
    data: PublishData


class ReconnectData(msgspec.Struct):
    client_id: str
    #: Map of tenant-prefixed channel → last opaque ``pos`` seen on it (research.md D4a). Keyed by
    #: the FULL channel, not the bare suffix; values are echoed verbatim, never constructed.
    last_pos: dict[str, str]


class Reconnect(msgspec.Struct, tag="reconnect"):
    data: ReconnectData


class Heartbeat(msgspec.Struct, tag="heartbeat"):
    """App-level heartbeat (NOT a WS ping frame). ``data`` is optional and omitted."""


class AuthData(msgspec.Struct):
    token: str  # JWT for refresh, or for API-key→JWT escalation


class Auth(msgspec.Struct, tag="auth"):
    data: AuthData


class HistoryData(msgspec.Struct):
    channel: str
    limit: int


class History(msgspec.Struct, tag="history"):
    data: HistoryData


class ReplayData(msgspec.Struct):
    channel: str
    from_pos: str  # opaque; typically the last_pos from a gap


class Replay(msgspec.Struct, tag="replay"):
    data: ReplayData


#: Tagged union of every client→server message. Encode with :func:`encode_client`.
ClientMessage: TypeAlias = (
    Subscribe | Unsubscribe | Publish | Reconnect | Heartbeat | Auth | History | Replay
)


# =============================================================================================
# SDK-internal events (NOT wire types) + the delivery-stream item type
# =============================================================================================


class PossibleGap(msgspec.Struct):
    """The Direct-backend data-loss signal — emitted per resubscribed channel on a Direct reconnect,
    where real ``gap``s are impossible (no Kafka cursor). Deliberately a **distinct type** from the
    wire :class:`Gap` (§XV: it carries no ``last_pos``/``pos``/``seq`` and MUST NOT enter the
    coalescing/replay engine). Lets one caller code path branch on ``channel`` across both backends.
    """

    channel: str


class Overflow(msgspec.Struct):
    """The delivery queue overflowed on a **non-pausable** transport (SSE) and dropped ``dropped``
    messages. An **in-band** data-loss signal (§III: never silent) — positional, like
    :class:`PossibleGap`, so the caller sees where in the stream loss occurred."""

    dropped: int


#: What flows through ``client.messages()``: delivered data (live/history ``Message``, recovered
#: ``ReplayMessage``) plus positional data-loss signals (``Gap``/``PossibleGap``/``Overflow``).
#: Acks/pong/errors are handled out-of-band and never enter this stream.
DeliveredItem: TypeAlias = Message | ReplayMessage | Gap | PossibleGap | Overflow


# =============================================================================================
# Codec
# =============================================================================================

_decoder: msgspec.json.Decoder[ServerMessage] = msgspec.json.Decoder(ServerMessage)
_encoder = msgspec.json.Encoder()


def decode_server_message(data: bytes | str) -> ServerMessage:
    """Decode a server frame into its typed model, dispatching on the ``type`` tag.

    Raises :class:`msgspec.ValidationError` on a malformed frame or unknown ``type`` — the caller
    (read-pump) decides whether to surface it as a :class:`~sukko.errors.ProtocolError` or log-and-
    skip an unknown future type (forward-compat).
    """
    return _decoder.decode(data.encode() if isinstance(data, str) else data)


def encode_client(message: ClientMessage) -> bytes:
    """Encode a client→server message to JSON bytes for transport."""
    return _encoder.encode(message)
