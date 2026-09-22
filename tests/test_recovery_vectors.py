"""Parity-vector binding for the ``recovery`` machine.

Loads the vendored, checksum-pinned corpus (platform ADR-0023 / sdk-py ADR-0004) and replays each
recovery scenario through the real :class:`~sukko.recovery.RecoveryEngine` via a thin adapter,
proving the language-neutral schema binds to this SDK's pure FSM and that its canonical actions
match the contract's expected effects.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sukko.recovery import (
    Action,
    EmitPossibleGap,
    RaiseRecoveryInterrupted,
    RecoveryEngine,
    SendReconnect,
    SendReplay,
)

VECTORS_DIR = Path(__file__).parent / "vectors"


class _VectorClock:
    """Minimal synchronous virtual clock — the recovery FSM only reads ``monotonic()``."""

    def __init__(self) -> None:
        self._now = 0.0

    def monotonic(self) -> float:
        return self._now

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:  # pragma: no cover - unused by the pure FSM
        return None

    def advance_ms(self, ms: float) -> None:
        self._now += ms / 1000.0  # vectors advance in ms; py durations are seconds


def _canonical(action: Action) -> dict[str, Any]:
    """Render a recovery Action as the language-neutral canonical form (snake_case tag + keys)."""
    if isinstance(action, SendReplay):
        return {"action": "send_replay", "channel": action.channel, "from_pos": action.from_pos}
    if isinstance(action, SendReconnect):
        return {
            "action": "send_reconnect",
            "client_id": action.client_id,
            "last_pos": action.last_pos,
        }
    if isinstance(action, EmitPossibleGap):
        return {"action": "emit_possible_gap", "channel": action.channel}
    if isinstance(action, RaiseRecoveryInterrupted):
        return {
            "action": "raise_recovery_interrupted",
            "channel": action.channel,
            "reason": action.reason,
        }
    raise AssertionError(f"unhandled recovery action: {action!r}")


def _run(scenario: dict[str, Any]) -> list[dict[str, Any]]:
    clock = _VectorClock()
    engine = RecoveryEngine(client_id="c1", clock=clock)  # type: ignore[arg-type]
    engine.mark_connected()
    out: list[dict[str, Any]] = []
    for step in scenario["inputs"]:
        if "advance" in step:
            clock.advance_ms(step["advance"])
            actions = engine.due()
        else:
            event = step["event"]
            if event == "gap":
                actions = engine.handle_gap(step["channel"], step["last_pos"])
            elif event == "replay_message":
                engine.note_replay_message(step["channel"])
                actions = []
            elif event == "replay_complete":
                actions = engine.handle_replay_complete(step["channel"])
            elif event == "disconnect":
                actions = engine.handle_disconnect()
            else:
                raise AssertionError(f"recovery vector: unhandled input event {step!r}")
        out.extend(_canonical(a) for a in actions)
    return out


def test_recovery_gap_replay_basic() -> None:
    scenario = json.loads((VECTORS_DIR / "recovery" / "gap-replay-basic.json").read_text())
    assert scenario["machine"] == "recovery"
    assert _run(scenario) == scenario["expect"]
