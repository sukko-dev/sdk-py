"""Sukko — asyncio-first Python client SDK for the Sukko real-time platform.

Built to the authoritative AsyncAPI v1.4.0 + gateway OpenAPI contracts. The full protocol model
set lives in ``sukko.messages`` and the full error hierarchy in ``sukko.errors``; the names below
are the curated public surface.

    import sukko
    async with sukko.SukkoClient("wss://host/ws", token=jwt) as client:
        await client.subscribe(["acme.trades"])
        async for msg in client.messages():
            ...
"""

from __future__ import annotations

import logging

from ._redact import RedactingFilter
from .backpressure import OverflowPolicy
from .channels import ParsedChannel, build_channel, parse_channel
from .client import SukkoClient
from .constants import CLOSE_CODES, MAX_REPLAY_MESSAGES, CloseDirection
from .errors import (
    AuthError,
    ConfigurationError,
    ConnectionClosedError,
    EditionRequiredError,
    HistoryError,
    NotConnectedError,
    PayloadTooLargeError,
    PublishError,
    PublishNotRoutableError,
    RateLimitError,
    RecoveryInterruptedError,
    ReplayError,
    ServiceUnavailableError,
    SukkoError,
    TenantLimitExceededError,
    TransportError,
)
from .messages import (
    DeliveredItem,
    Gap,
    Message,
    Overflow,
    PossibleGap,
    ReplayMessage,
)
from .push import Platform, PushClient
from .sync import SyncSukkoClient
from .transport.base import (
    ConnectionState,
    Transport,
    TransportCapabilities,
)
from .transport.sse import SseTransport
from .transport.websocket import WebSocketTransport

__version__ = "0.1.0"

# §V/§IX: the SDK is library-quiet (a NullHandler on the root `sukko` logger so nothing is emitted
# unless the application configures logging) AND credential-safe. A logger's filters run only for
# records IT creates (not propagated child records), so the RedactingFilter is attached to EVERY SDK
# logger — the root and each child — so no record (message or `extra=` field) can carry a token.
_SDK_LOGGERS = ("sukko", "sukko.client", "sukko.auth")
_redacting_filter = RedactingFilter()
for _name in _SDK_LOGGERS:
    logging.getLogger(_name).addFilter(_redacting_filter)
logging.getLogger("sukko").addHandler(logging.NullHandler())

__all__ = [
    "__version__",
    # clients
    "SukkoClient",
    "SyncSukkoClient",
    # channels
    "build_channel",
    "parse_channel",
    "ParsedChannel",
    # connection
    "ConnectionState",
    "CLOSE_CODES",
    "CloseDirection",
    # transports
    "Transport",
    "TransportCapabilities",
    "WebSocketTransport",
    "SseTransport",
    # delivery stream items
    "DeliveredItem",
    "Message",
    "ReplayMessage",
    "Gap",
    "PossibleGap",
    "Overflow",
    # push
    "PushClient",
    "Platform",
    # config
    "OverflowPolicy",
    "MAX_REPLAY_MESSAGES",
    # errors
    "SukkoError",
    "NotConnectedError",
    "ConnectionClosedError",
    "TransportError",
    "AuthError",
    "EditionRequiredError",
    "RateLimitError",
    "TenantLimitExceededError",
    "PublishError",
    "PublishNotRoutableError",
    "PayloadTooLargeError",
    "ReplayError",
    "RecoveryInterruptedError",
    "HistoryError",
    "ServiceUnavailableError",
    "ConfigurationError",
]
