"""Typed exception hierarchy mapping every contract error / close / HTTP code.

No raw transport traceback ever reaches a caller: transport failures are wrapped, and **every**
exception message passes through :func:`sukko._redact.redact` in :class:`SukkoError.__init__`, so a
credential embedded in a ``httpx``/``websockets`` error string (e.g. a ``?token=`` query param) is
masked in the message, ``str``, and ``repr`` (§IX).

Three mapping entry points keep the code→exception logic in one place (§X shared consolidation):
:func:`error_from_close`, :func:`error_from_http_status`, and :func:`error_from_ws_error`.
"""

from __future__ import annotations

from ._redact import redact
from .constants import CloseDirection, is_force_disconnect, is_heartbeat_timeout


class SukkoError(Exception):
    """Base for every SDK error. Redacts its message so no credential leaks via ``str``/``repr``."""

    def __init__(self, message: str) -> None:
        self.message = redact(message)
        super().__init__(self.message)


# --- Connection / transport -------------------------------------------------------------------


class NotConnectedError(SukkoError):
    """Raised when an operation requiring a live socket (e.g. WS ``publish``) is attempted while
    not connected — fail-fast, never a silent no-op (Scenario 3.2)."""

    def __init__(self, message: str = "client is not connected") -> None:
        super().__init__(message)


class TransportError(SukkoError):
    """Wraps a raw transport failure (``websockets``/``httpx``) so no unredacted traceback leaks.

    ``retryable`` lets the reconnect policy decide whether to back off and retry or give up.
    """

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


class ConnectionClosedError(SukkoError):
    """A WebSocket close was received or initiated. Carries the numeric ``code`` and the
    ``direction`` needed to disambiguate 4000 (operator ``force_disconnect`` vs local pong-timeout).
    ``terminal`` marks closes that MUST NOT drive a tight reconnect loop (e.g. auth failed)."""

    def __init__(
        self,
        code: int,
        direction: CloseDirection,
        *,
        reason: str = "",
        terminal: bool = False,
    ) -> None:
        self.code = code
        self.direction = direction
        self.terminal = terminal
        detail = reason or _describe_close(code, direction)
        super().__init__(f"connection closed ({code}, {direction.value}): {detail}")


# --- Auth -------------------------------------------------------------------------------------


class AuthError(SukkoError):
    """Authentication/authorization failure. ``code`` is the AsyncAPI ``auth_error`` code
    (``invalid_token``, ``token_expired``, ``tenant_mismatch``, ``rate_limited``, ``not_available``)
    or an HTTP-derived code."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class EditionRequiredError(SukkoError):
    """A gated capability (SSE / Web Push = Pro, mobile FCM/APNs push = Enterprise) was used on an
    insufficient edition — HTTP 403 ``EDITION_LIMIT``, surfaced typed rather than a
    raw 403."""

    def __init__(
        self,
        message: str = "this capability requires a higher edition",
        *,
        required: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = "EDITION_LIMIT"
        self.required_edition = required


# --- Rate limiting ----------------------------------------------------------------------------


class RateLimitError(SukkoError):
    """HTTP 429 / WS ``rate_limited``. ``retry_after`` is honored when present (the gateway does not
    currently send ``Retry-After`` — upstream filing #5 — so it is usually ``None``)."""

    def __init__(self, message: str = "rate limited", *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class TenantLimitExceededError(RateLimitError):
    """Handshake 429 ``TENANT_LIMIT_EXCEEDED`` — the tenant connection cap was hit at upgrade.
    Distinct in the reconnect policy: back off, do not hammer."""

    def __init__(
        self, message: str = "tenant connection limit exceeded", *, retry_after: float | None = None
    ) -> None:
        super().__init__(message, retry_after=retry_after)
        self.code = "TENANT_LIMIT_EXCEEDED"


# --- Publish ----------------------------------------------------------------------------------


class PublishError(SukkoError):
    """Publish rejected — WS ``publish_error`` or a REST publish HTTP error. ``code`` is the
    machine-readable reason, ``channel``/``status`` present when known."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        channel: str | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.channel = channel
        self.status = status


class PublishNotRoutableError(PublishError):
    """HTTP 409 ``PUBLISH_NOT_ROUTABLE`` — no routing rule matched (Kafka backend)."""


class PayloadTooLargeError(PublishError):
    """HTTP 413 ``BODY_TOO_LARGE`` / WS ``message_too_large`` — payload over the transport limit."""


# --- Subscription -----------------------------------------------------------------------------


class SubscribeError(SukkoError):
    """AsyncAPI ``subscribe_error`` — the subscribe request itself was malformed."""

    def __init__(
        self, message: str, *, code: str | None = None, channel: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.channel = channel


class UnsubscribeError(SukkoError):
    """AsyncAPI ``unsubscribe_error`` — the unsubscribe request itself was malformed."""

    def __init__(
        self, message: str, *, code: str | None = None, channel: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.channel = channel


# --- Recovery / replay / history --------------------------------------------------------------


class ReplayError(SukkoError):
    """A live ``replay`` or reconnect-replay was rejected — the AsyncAPI ``error`` replay codes
    (``offset_out_of_range``, ``replay_failed``, ``replay_rate_limited``, ``replay_in_progress``,
    ``not_subscribed``). Caller may fall back to ``history``."""

    def __init__(
        self, message: str, *, code: str | None = None, channel: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.channel = channel


class RecoveryInterruptedError(SukkoError):
    """A recovery (reconnect-replay / live replay / history) was cut short — a missing
    ``replay_complete``/``history_complete`` past the client-side detection deadline, or a
    1008/``replay_failed`` mid-recovery. Surfaced typed, never a bare disconnect (§III)."""

    def __init__(
        self, message: str, *, channel: str | None = None, reason: str | None = None
    ) -> None:
        super().__init__(message)
        self.channel = channel
        self.reason = reason


class HistoryError(SukkoError):
    """AsyncAPI ``history_error`` — includes ``history_disabled`` (server toggle off). ``code`` is
    the machine-readable reason, ``channel`` is always present."""

    def __init__(
        self, message: str, *, code: str | None = None, channel: str | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.channel = channel


class ReconnectError(SukkoError):
    """AsyncAPI ``reconnect_error``. ``not_available`` is surfaced by recovery as a *capability
    signal* (Direct backend), not as a retryable error — see recovery.py."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


# --- Generic / service ------------------------------------------------------------------------


class ServiceUnavailableError(SukkoError):
    """HTTP 503 / WS ``service_unavailable`` — a transient backend outage; retryable."""

    def __init__(self, message: str = "service temporarily unavailable") -> None:
        super().__init__(message)
        self.retryable = True


class ProtocolError(SukkoError):
    """A malformed or unparseable server message (``invalid_json``) or a contract violation the SDK
    detected while decoding (§II defense-in-depth)."""


class ConfigurationError(SukkoError):
    """Invalid client construction config (e.g. ``queue_maxsize`` below the recovery floor). Raised
    at construction so the system never runs with wrong state (§I constructors, §XV)."""


# --- Mapping helpers (single source of truth) -------------------------------------------------

# HTTP status → the codes the gateway attaches, per the REST publish error map.
_HTTP_403_EDITION = "EDITION_LIMIT"

# WS error codes that mean "a replay/recovery attempt was rejected".
REPLAY_ERROR_CODES = frozenset(
    {
        "replay_in_progress",
        "replay_rate_limited",
        "offset_out_of_range",
        "replay_failed",
        "not_subscribed",
    }
)


def _describe_close(code: int, direction: CloseDirection) -> str:
    """Human-readable close reason, resolving the 4000 overlap by ``direction``."""
    if is_force_disconnect(code, direction):
        return "operator force-disconnect"
    if is_heartbeat_timeout(code, direction):
        return "heartbeat pong timeout (client-initiated)"
    return {
        1000: "normal closure",
        1001: "server going away",
        1002: "protocol error",
        1008: "policy violation (slow client)",
        1011: "server internal error",
        4001: "authentication failed",
        4002: "subscription failed",
    }.get(code, "unspecified")


def error_from_close(
    code: int, direction: CloseDirection, *, reason: str = ""
) -> ConnectionClosedError:
    """Build a :class:`ConnectionClosedError`, marking terminal (no-retry) closes."""
    terminal = code == 4001  # AUTH_FAILED — do not enter a tight reconnect loop
    return ConnectionClosedError(code, direction, reason=reason, terminal=terminal)


def error_from_http_status(
    status: int,
    *,
    code: str | None = None,
    message: str | None = None,
    retry_after: float | None = None,
    handshake: bool = False,
) -> SukkoError:
    """Map an HTTP status (+ optional gateway ``code``) to a typed error (REST error map)."""
    msg = message or f"HTTP {status}"
    if status == 400:
        return PublishError(msg, code=code or "INVALID_REQUEST", status=400)
    if status == 401:
        return AuthError(msg, code=code or "UNAUTHORIZED")
    if status == 403:
        if code == _HTTP_403_EDITION:
            return EditionRequiredError(msg)
        return PublishError(msg, code=code or "FORBIDDEN", status=403)
    if status == 409:
        return PublishNotRoutableError(msg, code=code or "PUBLISH_NOT_ROUTABLE", status=409)
    if status == 413:
        return PayloadTooLargeError(msg, code=code or "BODY_TOO_LARGE", status=413)
    if status == 429:
        if handshake or code == "TENANT_LIMIT_EXCEEDED":
            return TenantLimitExceededError(msg, retry_after=retry_after)
        return RateLimitError(msg, retry_after=retry_after)
    if status == 503:
        return ServiceUnavailableError(msg)
    return SukkoError(msg)


def error_from_ws_error(
    msg_type: str,
    code: str | None,
    message: str,
    *,
    channel: str | None = None,
) -> SukkoError:
    """Map a decoded server error envelope (``publish_error``/``auth_error``/``error``/… ``type`` +
    ``code``) to a typed exception. Takes primitives to avoid a dependency on the message models."""
    if msg_type == "auth_error":
        return AuthError(message, code=code)
    if msg_type == "publish_error":
        if code == "message_too_large":
            return PayloadTooLargeError(message, code=code, channel=channel)
        if code in ("no_routing_rules", "no_matching_route"):
            return PublishNotRoutableError(message, code=code, channel=channel)
        if code == "rate_limited":
            return RateLimitError(message)
        if code in ("service_unavailable", "not_available"):
            return ServiceUnavailableError(message)
        return PublishError(message, code=code, channel=channel)
    if msg_type == "subscribe_error":
        return SubscribeError(message, code=code, channel=channel)
    if msg_type == "unsubscribe_error":
        return UnsubscribeError(message, code=code, channel=channel)
    if msg_type == "history_error":
        return HistoryError(message, code=code, channel=channel)
    if msg_type == "reconnect_error":
        return ReconnectError(message, code=code)
    if msg_type == "error":
        if code == "invalid_json":
            return ProtocolError(message)
        if code == "rate_limited" or code == "replay_rate_limited":
            return RateLimitError(message)
        if code in REPLAY_ERROR_CODES:
            return ReplayError(message, code=code, channel=channel)
        return SukkoError(message)
    return SukkoError(message)
