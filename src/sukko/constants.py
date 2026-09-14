"""Protocol constants: WebSocket close codes, client-option defaults, and recovery bounds.

Numeric values are grounded in the contracts and the server ``envDefault``s:
- Close codes: AsyncAPI ``client-ws.asyncapi.yaml`` "WebSocket Close Codes" table.
- ``MAX_REPLAY_MESSAGES`` / ``DEFAULT_HISTORY_LIMIT``: server ``WS_MAX_REPLAY_MESSAGES`` (100) and
  ``WS_HISTORY_MAX_LIMIT`` (100).
- Timing defaults: the idiomatic-asyncio conversion of ``@sukko/sdk``'s millisecond
  ``SUKKO_DEFAULTS`` to **seconds as floats**. The unit divergence is deliberate (parity appendix).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final


class CloseDirection(StrEnum):
    """Which side initiated a WebSocket close — the disambiguator for close code 4000.

    ``@sukko/sdk`` uses **4000** for a *client-initiated* heartbeat-timeout close, while the
    AsyncAPI contract uses **4000** for an *operator-initiated* ``force_disconnect``. The number is
    identical, so the SDK MUST track direction to tell them apart (upstream drift filing #1).
    """

    LOCAL = "local"  # the SDK closed the socket
    REMOTE = "remote"  # the server (or an operator via the Connections API) closed it


@dataclass(frozen=True, slots=True)
class _CloseCodes:
    """WebSocket close codes used by the Sukko protocol (mirrors ``@sukko/sdk`` ``CLOSE_CODES``)."""

    NORMAL: int = 1000  # Normal closure (client or server initiated)
    GOING_AWAY: int = 1001  # Server graceful shutdown
    PROTOCOL_ERROR: int = 1002  # Protocol error
    POLICY_VIOLATION: int = 1008  # Slow client disconnected by server (REMOTE)
    INTERNAL_ERROR: int = 1011  # Server internal error
    # 4000 is direction-dependent — see CloseDirection:
    FORCE_DISCONNECT: int = 4000  # REMOTE: operator force-disconnect (AsyncAPI force_disconnect)
    HEARTBEAT_TIMEOUT: int = 4000  # LOCAL: client-initiated pong-timeout close
    AUTH_FAILED: int = 4001  # LOCAL: authentication failed
    SUBSCRIPTION_FAILED: int = 4002  # LOCAL: subscription failed


#: Singleton close-code table.
CLOSE_CODES: Final = _CloseCodes()

#: Terminal close codes that MUST NOT trigger a tight reconnect loop (auth failed → stop retrying).
TERMINAL_CLOSE_CODES: Final[frozenset[int]] = frozenset({CLOSE_CODES.AUTH_FAILED})


def is_force_disconnect(code: int, direction: CloseDirection) -> bool:
    """True when a 4000 close is the *remote* operator ``force_disconnect`` (not our timeout)."""
    return code == CLOSE_CODES.FORCE_DISCONNECT and direction is CloseDirection.REMOTE


def is_heartbeat_timeout(code: int, direction: CloseDirection) -> bool:
    """True when a 4000 close is our own *local* heartbeat-timeout close (not a server close)."""
    return code == CLOSE_CODES.HEARTBEAT_TIMEOUT and direction is CloseDirection.LOCAL


@dataclass(frozen=True, slots=True)
class _Defaults:
    """Client-option defaults. Durations are **seconds (float)** — the asyncio-idiomatic conversion
    of ``@sukko/sdk``'s millisecond ``SUKKO_DEFAULTS``."""

    RECONNECT_ATTEMPTS: int = 5  # max reconnect attempts before giving up (0 = unlimited)
    RECONNECT_DELAY_BASE: float = 1.0  # backoff base (was 1000 ms)
    RECONNECT_DELAY_MAX: float = 30.0  # backoff cap (was 30000 ms)
    HEARTBEAT_INTERVAL: float = 30.0  # between heartbeat sends (was 30000 ms)
    HEARTBEAT_TIMEOUT: float = 5.0  # pong wait before local close (was 5000 ms)
    CONNECTION_TIMEOUT: float = 10.0  # handshake/connect timeout (was 10000 ms)
    # min interval between auto replays per channel (WS_REPLAY_RATE_LIMIT_INTERVAL):
    REPLAY_FLOOR: float = 10.0
    REFRESH_MIN_INTERVAL: float = 30.0  # min interval between token refreshes (single-flight floor)
    REFRESH_LEAD: float = 30.0  # fire proactive refresh this many seconds before auth_ack.exp
    RECOVERY_DEADLINE: float = 10.0  # "no recovery frame" timer (2x server WS_REPLAY_TIMEOUT=5s)


#: Singleton defaults table.
SUKKO_DEFAULTS: Final = _Defaults()

#: Contract bound: max messages returned per replay request (server ``WS_MAX_REPLAY_MESSAGES``).
MAX_REPLAY_MESSAGES: Final = 100

#: Default ``history_limit`` client knob — tracks the server ``WS_HISTORY_MAX_LIMIT`` envDefault.
#: The SDK never *requests* more than ``history_limit`` and rejects a larger request client-side.
DEFAULT_HISTORY_LIMIT: Final = 100

#: Default bounded delivery-queue size. MUST satisfy the construction floor
#: ``queue_maxsize >= history_limit + MAX_REPLAY_MESSAGES`` (256 >= 100 + 100) so a concurrent
#: history+replay burst cannot alone trip back-pressure.
DEFAULT_QUEUE_MAXSIZE: Final = 256
