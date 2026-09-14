"""Close-code → action table — the 4000 local-vs-remote disambiguation + terminal codes."""

from __future__ import annotations

from sukko.constants import (
    CLOSE_CODES,
    TERMINAL_CLOSE_CODES,
    CloseDirection,
    is_force_disconnect,
    is_heartbeat_timeout,
)
from sukko.errors import error_from_close


def test_4000_disambiguation_by_direction() -> None:
    # Same numeric code, opposite meaning by who initiated (the whole reason direction is tracked).
    assert is_force_disconnect(4000, CloseDirection.REMOTE) is True  # operator force_disconnect
    assert is_force_disconnect(4000, CloseDirection.LOCAL) is False
    assert is_heartbeat_timeout(4000, CloseDirection.LOCAL) is True  # our pong-timeout close
    assert is_heartbeat_timeout(4000, CloseDirection.REMOTE) is False


def test_terminal_vs_reconnectable_closes() -> None:
    assert CLOSE_CODES.AUTH_FAILED in TERMINAL_CLOSE_CODES
    assert error_from_close(CLOSE_CODES.AUTH_FAILED, CloseDirection.REMOTE).terminal is True
    # slow-client (1008) and force-disconnect (4000) are reconnectable — not terminal
    assert error_from_close(CLOSE_CODES.POLICY_VIOLATION, CloseDirection.REMOTE).terminal is False
    assert error_from_close(4000, CloseDirection.REMOTE).terminal is False


def test_close_error_carries_code_and_direction() -> None:
    err = error_from_close(1008, CloseDirection.REMOTE, reason="slow client")
    assert err.code == 1008
    assert err.direction is CloseDirection.REMOTE
    assert "slow client" in str(err)
