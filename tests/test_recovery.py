"""Recovery tests — reconnect-replay, gap coalescing + the
10s/channel floor, mid-replay follow-up, Direct degrade, and RecoveryInterrupted on both triggers.
Deterministic via FakeClock; the engine never compares opaque pos values.
"""

from __future__ import annotations

from fakes import FakeClock
from sukko.recovery import (
    EmitPossibleGap,
    RaiseRecoveryInterrupted,
    RecoveryEngine,
    SendReconnect,
    SendReplay,
)


def test_reconnect_payload_carries_last_pos_per_channel() -> None:
    eng = RecoveryEngine(client_id="c1", clock=FakeClock())
    eng.note_pos("acme.BTC.trade", "2-100")
    eng.note_pos("acme.ETH.trade", "5-200")
    reconnect = eng.build_reconnect()
    assert reconnect == SendReconnect(
        client_id="c1", last_pos={"acme.BTC.trade": "2-100", "acme.ETH.trade": "5-200"}
    )


def test_first_connect_no_pos_means_no_reconnect() -> None:
    eng = RecoveryEngine(clock=FakeClock())
    eng.note_pos("acme.a", None)  # Direct message → nothing to anchor
    assert eng.build_reconnect() is None  # first connect: nothing to resume, nothing to probe


def test_reconnect_probes_empty_pos_after_connected_once() -> None:
    # Once connected, a reconnect with no stored pos MUST send an empty last_pos
    # to probe the backend — else a pure-Direct session never elicits not_available and
    # silently drops. Reverting build_reconnect's connected_once branch makes this None → fails.
    eng = RecoveryEngine(client_id="c1", clock=FakeClock())
    eng.mark_connected()
    assert eng.build_reconnect() == SendReconnect(client_id="c1", last_pos={})


def test_direct_never_probes_even_after_connected_once() -> None:
    eng = RecoveryEngine(client_id="c1", clock=FakeClock())
    eng.mark_connected()
    eng.handle_not_available(["acme.a"])  # backend reported Direct
    assert eng.build_reconnect() is None  # known Direct → never probes again


async def test_replay_floor_and_gap_coalescing() -> None:
    clock = FakeClock(start=0.0)
    eng = RecoveryEngine(clock=clock)
    assert eng.handle_gap("acme.a", "p1") == [SendReplay("acme.a", "p1")]  # immediate (floor open)
    assert eng.handle_replay_complete("acme.a") == []  # back to idle

    # a new gap now is inside the 10s floor (last replay at t=0) → wait, don't replay yet
    assert eng.handle_gap("acme.a", "p2") == []
    assert eng.handle_gap("acme.a", "p3") == []  # coalesces — first un-recovered anchor (p2) wins
    assert eng.next_deadline() == 10.0
    await clock.advance(9.0)
    assert eng.due() == []  # floor not yet elapsed
    await clock.advance(1.0)
    assert eng.due() == [SendReplay("acme.a", "p2")]  # conservative anchor = p2, never p3


async def test_gap_mid_replay_drives_followup_after_complete() -> None:
    clock = FakeClock(start=100.0)
    eng = RecoveryEngine(clock=clock)
    assert eng.handle_gap("acme.a", "p1") == [SendReplay("acme.a", "p1")]  # REPLAYING
    assert eng.handle_gap("acme.a", "p2") == []  # arrives mid-replay → retained as follow-up anchor
    assert eng.handle_replay_complete("acme.a") == []  # floor not elapsed → follow-up waits
    await clock.advance(10.0)
    assert eng.due() == [SendReplay("acme.a", "p2")]  # follow-up replay from the mid-replay gap


def test_direct_degrade_emits_possible_gap_no_retry() -> None:
    eng = RecoveryEngine(clock=FakeClock())
    assert eng.build_reconnect() is None
    actions = eng.handle_not_available(["acme.a", "acme.b"])
    assert actions == [EmitPossibleGap("acme.a"), EmitPossibleGap("acme.b")]
    assert eng.is_direct is True
    assert eng.build_reconnect() is None  # Direct backend → never sends reconnect (no retry loop)


async def test_recovery_interrupted_when_replay_complete_missing_past_deadline() -> None:
    clock = FakeClock(start=0.0)
    eng = RecoveryEngine(clock=clock)
    eng.handle_gap("acme.a", "p1")  # REPLAYING, detection deadline at t=10
    await clock.advance(10.0)  # no replay_complete arrives
    actions = eng.due()
    assert actions == [
        RaiseRecoveryInterrupted("acme.a", "no replay_complete before detection deadline")
    ]


async def test_replay_idle_deadline_resets_on_each_frame() -> None:
    # The detection deadline is a per-frame-reset IDLE timer (server silence), NOT an
    # absolute cap. A steady replay stream far exceeding the deadline must NOT interrupt — only
    # server silence past the deadline does. Under the old absolute deadline the []-assert fails.
    clock = FakeClock(start=0.0)
    eng = RecoveryEngine(clock=clock)
    eng.handle_gap("acme.a", "p1")  # REPLAYING, deadline at t=10
    for _ in range(5):  # total 45s ≫ the 10s deadline, but each frame resets the idle timer
        await clock.advance(9.0)
        assert eng.due() == []  # a frame keeps arriving before the idle deadline → not stuck
        eng.note_replay_message("acme.a")
    await clock.advance(10.0)  # now the server goes silent past the deadline
    assert eng.due() == [
        RaiseRecoveryInterrupted("acme.a", "no replay_complete before detection deadline")
    ]


async def test_history_idle_deadline_resets_on_each_frame() -> None:
    clock = FakeClock(start=0.0)
    eng = RecoveryEngine(clock=clock)
    eng.note_history_request("acme.a")  # history deadline at t=10
    for _ in range(5):
        await clock.advance(9.0)
        assert eng.due() == []
        eng.note_history_message("acme.a")
    await clock.advance(10.0)
    assert eng.due() == [
        RaiseRecoveryInterrupted("acme.a", "no history_complete before detection deadline")
    ]


def test_recovery_interrupted_on_disconnect_mid_replay() -> None:
    eng = RecoveryEngine(clock=FakeClock())
    eng.handle_gap("acme.a", "p1")  # REPLAYING
    actions = eng.handle_disconnect()
    assert any(isinstance(a, RaiseRecoveryInterrupted) and a.channel == "acme.a" for a in actions)


def test_disconnect_preserves_pos_for_next_reconnect() -> None:
    eng = RecoveryEngine(client_id="c1", clock=FakeClock())
    eng.note_pos("acme.a", "2-9")
    eng.handle_disconnect()
    assert eng.build_reconnect() == SendReconnect(client_id="c1", last_pos={"acme.a": "2-9"})
