"""Auth tests — single-flight refresh, floor, proactive timer,
escalation vs refresh, offline-defer, and teardown. Deterministic via FakeClock; no real sleeps.
"""

from __future__ import annotations

import asyncio

import pytest

from fakes import FakeClock
from sukko.auth import AuthManager
from sukko.errors import AuthError, NotConnectedError


def _manager(clock: FakeClock, tokens: list[str] | None = None) -> tuple[AuthManager, list[str]]:
    sent: list[str] = []
    supply = iter(tokens or ["fresh"])

    async def send_auth(token: str) -> None:
        sent.append(token)

    async def get_token() -> str:
        return next(supply, "fresh")

    manager = AuthManager(
        token="old",
        get_token=get_token,
        send_auth=send_auth,
        clock=clock,
        refresh_min_interval=30.0,
        refresh_lead=30.0,
    )
    return manager, sent


async def test_refresh_sends_auth_and_resolves_on_ack() -> None:
    clock = FakeClock()
    manager, sent = _manager(clock, ["rotated"])
    task = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    assert sent == ["rotated"]  # fetched a fresh token and sent auth
    manager.on_auth_ack(exp=0)  # server acks
    await task
    assert manager.token == "rotated"  # rotated without error


async def test_single_flight_coalesces_concurrent_triggers() -> None:
    clock = FakeClock()
    manager, sent = _manager(clock, ["a", "b"])
    t1 = asyncio.ensure_future(manager.refresh())
    t2 = asyncio.ensure_future(manager.refresh())  # concurrent → must coalesce
    await asyncio.sleep(0)
    assert len(sent) == 1, "single-flight: only one auth in flight"
    manager.on_auth_ack(exp=0)
    await asyncio.gather(t1, t2)


async def test_refresh_honors_30s_floor() -> None:
    clock = FakeClock()
    manager, sent = _manager(clock, ["one", "two"])
    t1 = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    manager.on_auth_ack(exp=0)
    await t1
    assert sent == ["one"]

    # a second refresh right away must wait out the 30s floor before sending
    t2 = asyncio.ensure_future(manager.refresh())
    await clock.advance(29.0)
    assert sent == ["one"], "must not refresh again before the floor elapses"
    await clock.advance(1.0)
    assert sent == ["one", "two"]
    manager.on_auth_ack(exp=0)
    await t2


async def test_exp_zero_arms_no_proactive_timer() -> None:
    clock = FakeClock()
    manager, _sent = _manager(clock)
    manager.on_auth_ack(exp=0)  # no-expiry
    assert clock.pending_sleepers == 0  # nothing scheduled


async def test_proactive_timer_fires_at_exp_minus_lead() -> None:
    clock = FakeClock(start=1000.0)
    manager, sent = _manager(clock, ["proactive"])
    manager.on_auth_ack(exp=1100)  # exp in 100s; lead 30 → fire at +70s
    await clock.advance(69.0)
    assert sent == [], "must not refresh before exp - lead"
    await clock.advance(1.0)
    assert sent == ["proactive"]
    manager.on_auth_ack(exp=0)


async def test_escalation_sends_when_connected() -> None:
    clock = FakeClock()
    manager, sent = _manager(clock)
    task = asyncio.ensure_future(manager.escalate("jwt-esc", connected=True))
    await asyncio.sleep(0)
    assert sent == ["jwt-esc"]
    manager.on_auth_ack(exp=0)
    assert await task is True  # caller re-subscribes the delta on True


async def test_offline_escalation_defers_to_update_token() -> None:
    clock = FakeClock()
    manager, sent = _manager(clock)
    result = await manager.escalate("jwt-offline", connected=False)
    assert result is False  # deferred
    assert sent == []  # no auth sent while offline
    assert manager.token == "jwt-offline"  # but credential updated for the next connect


async def test_escalation_waits_for_inflight_refresh_then_sends_own_frame() -> None:
    # An escalation MUST NOT coalesce onto an in-flight refresh — it waits for the
    # refresh to settle, then sends its OWN jwt frame. Old code coalesced → no jwt frame → fails.
    clock = FakeClock()
    manager, sent = _manager(clock, ["refreshed"])
    refresh = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    assert sent == ["refreshed"]  # refresh sent, awaiting its ack (in flight)

    esc = asyncio.ensure_future(manager.escalate("jwt-esc", connected=True))
    await asyncio.sleep(0)
    assert sent == ["refreshed"], "escalation must WAIT for the in-flight refresh, not coalesce"

    manager.on_auth_ack(exp=0)  # the refresh acks → it completes, freeing the slot
    assert await refresh is None
    for _ in range(3):  # let the escalation's wait-loop observe the freed slot and send
        await asyncio.sleep(0)
    assert sent == ["refreshed", "jwt-esc"]  # escalation sent its OWN frame
    manager.on_auth_ack(exp=0)  # escalation's own ack
    assert await esc is True


async def test_escalation_proceeds_after_failed_refresh() -> None:
    # A rejected in-flight refresh must NOT abort the escalation — it still sends its own frame.
    clock = FakeClock()
    manager, sent = _manager(clock, ["refreshed"])
    refresh = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    esc = asyncio.ensure_future(manager.escalate("jwt-esc", connected=True))
    await asyncio.sleep(0)

    manager.on_auth_error("invalid_token", "rejected")  # the refresh fails
    with pytest.raises(AuthError):
        await refresh
    for _ in range(3):
        await asyncio.sleep(0)
    assert sent == ["refreshed", "jwt-esc"]  # escalation proceeded despite the failed refresh
    manager.on_auth_ack(exp=0)
    assert await esc is True


async def test_escalation_defers_if_inflight_refresh_dropped_by_close() -> None:
    # If the in-flight refresh is dropped by a disconnect (NotConnectedError), escalation can't send
    # now → defer (store the JWT, return False), never hang or send into a dead connection.
    clock = FakeClock()
    manager, sent = _manager(clock, ["refreshed"])
    refresh = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    esc = asyncio.ensure_future(manager.escalate("jwt-esc", connected=True))
    await asyncio.sleep(0)

    await manager.aclose()  # drops the in-flight refresh with NotConnectedError
    with pytest.raises(NotConnectedError):
        await refresh
    assert await esc is False  # escalation deferred
    assert manager.token == "jwt-esc"  # credential stored for the next connect
    assert sent == ["refreshed"]  # no jwt frame sent into the dead connection


async def test_in_flight_auth_error_fails_refresh_without_looping() -> None:
    clock = FakeClock()
    manager, _sent = _manager(clock)
    task = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    reactive_needed = manager.on_auth_error("invalid_token", "bad")
    assert reactive_needed is False  # resolves our own in-flight refresh; do NOT trigger a loop
    with pytest.raises(AuthError):
        await task


async def test_unsolicited_auth_error_requests_reactive_refresh() -> None:
    clock = FakeClock()
    manager, _sent = _manager(clock)
    assert manager.on_auth_error("token_expired", "expired") is True  # no refresh in flight


async def test_aclose_fails_pending_refresh() -> None:
    clock = FakeClock()
    manager, _sent = _manager(clock)
    task = asyncio.ensure_future(manager.refresh())
    await asyncio.sleep(0)
    await manager.aclose()
    with pytest.raises(NotConnectedError):
        await task  # pending failed on teardown — never hangs across a disconnect
