"""Reconnect / gap-recovery engine — the pos-anchored recovery state machine.

Kafka-backend-only: pos-recovery (reconnect-replay, live ``gap``→``replay``, ``history``) needs the
Kafka backend. On the Direct backend ``pos`` is absent and ``reconnect_error: not_available`` is
returned — the engine degrades to naive resubscribe and emits a per-channel :class:`PossibleGap`
data-loss signal (no retry loop).

**A pure action machine**: it never performs I/O. ``handle_*`` methods and the
clock-driven :meth:`due` return a list of :class:`Action`\\s the client executes (send a ``replay``,
send a ``reconnect``, emit a ``PossibleGap``, or surface a ``RecoveryInterruptedError``). The clock
is injected, so gap-coalescing, the 10s/channel replay floor, and the detection deadline are all
deterministic-testable.

**Never compare ``pos`` values** — they are opaque by contract. Gaps for a channel arrive in
connection order, so the **first un-recovered gap's ``last_pos`` is the anchor** for the replay
cycle; later gaps within the cycle just mean "more loss" (replaying from the earlier anchor covers
them — the contract's conservative-anchor guarantee: may re-deliver, never misses). A gap arriving
mid-replay becomes the anchor for the follow-up cycle after ``replay_complete``. Per channel:
``idle → floor-wait → replaying`` (with a pending follow-up anchor).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto

from ._clock import SYSTEM_CLOCK, Clock
from .constants import SUKKO_DEFAULTS


@dataclass(frozen=True, slots=True)
class SendReplay:
    """Send ``replay{channel, from_pos}`` to recover a live gap."""

    channel: str
    from_pos: str


@dataclass(frozen=True, slots=True)
class SendReconnect:
    """Send ``reconnect{client_id, last_pos}`` to replay everything missed across the disconnect."""

    client_id: str
    last_pos: dict[str, str]


@dataclass(frozen=True, slots=True)
class EmitPossibleGap:
    """Surface a Direct-backend :class:`~sukko.messages.PossibleGap` for ``channel`` — no
    ``last_pos`` to anchor a replay (the disconnect is an unrecoverable gap on Direct)."""

    channel: str


@dataclass(frozen=True, slots=True)
class RaiseRecoveryInterrupted:
    """Surface a :class:`~sukko.errors.RecoveryInterruptedError` — a truncated recovery (missing
    completion past the deadline, or a failure mid-recovery), never a bare disconnect (§III)."""

    channel: str
    reason: str


Action = SendReplay | SendReconnect | EmitPossibleGap | RaiseRecoveryInterrupted


class _Phase(Enum):
    IDLE = auto()
    FLOOR_WAIT = auto()  # a gap is pending but the per-channel replay floor has not elapsed
    REPLAYING = auto()  # a replay is in flight, awaiting replay_complete


@dataclass(slots=True)
class _Channel:
    phase: _Phase = _Phase.IDLE
    anchor: str | None = None  # from_pos for the pending/active replay (first un-recovered gap)
    followup_anchor: str | None = None  # first gap seen while REPLAYING → next cycle's anchor
    last_replay_at: float = float("-inf")  # for the 1/floor-per-channel rate limit
    floor_wake: float | None = None  # absolute time FLOOR_WAIT may fire
    deadline: float | None = None  # absolute "no replay_complete by now" detection deadline
    arm_pause_episodes: int = (
        0  # _pause_episodes captured when `deadline` was armed (silence baseline)
    )


class RecoveryEngine:
    """Owns ``client_id`` + per-channel ``pos`` and the per-channel recovery FSM."""

    def __init__(
        self,
        *,
        client_id: str | None = None,
        clock: Clock = SYSTEM_CLOCK,
        replay_floor: float = SUKKO_DEFAULTS.REPLAY_FLOOR,
        recovery_deadline: float = SUKKO_DEFAULTS.RECOVERY_DEADLINE,
    ) -> None:
        #: Caller-supplied or per-process id. A fresh per-process id forfeits cross-restart replay
        #: unless the caller persists it (Scenario 4.4).
        self.client_id = client_id or uuid.uuid4().hex
        self._clock = clock
        self._floor = replay_floor
        self._deadline = recovery_deadline
        self._pos: dict[str, str] = {}
        self._channels: dict[str, _Channel] = {}
        self._direct = False  # flipped to True once the backend reports not_available
        self._connected_once = False  # set on first connect → later reconnects probe for Direct
        self._history_deadline: dict[
            str, tuple[float, int]
        ] = {}  # channel -> (deadline, arm_pause_episodes)
        self._paused = False
        self._pause_episodes = (
            0  # monotonic; a False->True transition opens a back-pressure episode
        )

    # --- pos tracking -------------------------------------------------------------------------

    def note_pos(self, channel: str, pos: str | None) -> None:
        """Record the last-seen opaque ``pos`` for ``channel`` (from a live or replay message). A
        missing ``pos`` (Direct backend) is ignored — there is nothing to anchor."""
        if pos is not None:
            self._pos[channel] = pos

    @property
    def is_direct(self) -> bool:
        return self._direct

    def _channel(self, channel: str) -> _Channel:
        rec = self._channels.get(channel)
        if rec is None:
            rec = _Channel()
            self._channels[channel] = rec
        return rec

    # --- live gap → replay --------------------------------------------------------------------

    def handle_gap(self, channel: str, last_pos: str) -> list[Action]:
        """A wire ``gap`` arrived. Coalesce per the conservative-anchor rule and, if the floor is
        open, emit a replay; otherwise wait out the floor (:meth:`due` fires it)."""
        now = self._clock.monotonic()
        rec = self._channel(channel)
        if rec.phase is _Phase.IDLE:
            rec.anchor = last_pos
            earliest = rec.last_replay_at + self._floor
            if now >= earliest:
                return [self._begin_replay(rec, channel, last_pos, now)]
            rec.phase = _Phase.FLOOR_WAIT
            rec.floor_wake = earliest
            return []
        if rec.phase is _Phase.FLOOR_WAIT:
            return []  # coalesce: keep the earlier anchor (replay from it covers this gap too)
        # REPLAYING: retain the first mid-replay gap as the follow-up anchor
        if rec.followup_anchor is None:
            rec.followup_anchor = last_pos
        return []

    def _begin_replay(self, rec: _Channel, channel: str, from_pos: str, now: float) -> SendReplay:
        rec.phase = _Phase.REPLAYING
        rec.anchor = None
        rec.last_replay_at = now
        rec.floor_wake = None
        rec.deadline = now + self._deadline
        rec.arm_pause_episodes = self._pause_episodes
        return SendReplay(channel=channel, from_pos=from_pos)

    def note_backpressure(self, paused: bool) -> None:
        """The delivery consumer stalled (``True``) or resumed (``False``). While stalled,
        recovery frames stop arriving, so a detection deadline **suspends** rather than fires:
        the silence is the consumer's, not the server's (platform ADR-0025; the Py half of Go's
        park-suspension). A ``False``->``True`` transition opens a back-pressure **episode**, so a
        stall that opens and closes entirely within one deadline window still suspends that window
        (point-sampling ``_paused`` alone would miss it)."""
        if paused and not self._paused:
            self._pause_episodes += 1
        self._paused = paused

    def _backpressure_suspends(self, arm_pause_episodes: int) -> bool:
        """True when the deadline's silence is the consumer's: currently backpressured, or a
        back-pressure episode opened since the deadline was armed (mirrors Go's parked-now ||
        episodes-changed suspension)."""
        return self._paused or self._pause_episodes != arm_pause_episodes

    def handle_replay_complete(self, channel: str) -> list[Action]:
        """A ``replay_complete`` arrived. If a gap landed mid-replay, start the follow-up cycle
        (floor-gated); otherwise the channel returns to idle."""
        rec = self._channel(channel)
        rec.deadline = None
        if rec.phase is not _Phase.REPLAYING:
            return []
        followup = rec.followup_anchor
        rec.followup_anchor = None
        if followup is None:
            rec.phase = _Phase.IDLE
            return []
        now = self._clock.monotonic()
        earliest = rec.last_replay_at + self._floor
        if now >= earliest:
            return [self._begin_replay(rec, channel, followup, now)]
        rec.phase = _Phase.FLOOR_WAIT
        rec.anchor = followup
        rec.floor_wake = earliest
        return []

    def note_replay_message(self, channel: str) -> None:
        """A ``replay_message`` arrived — reset the per-channel idle detection deadline. It measures
        **server silence**, not consumer speed, so a slow consumer draining a large replay is not
        mistaken for a stuck stream. No-op unless the channel is mid-replay."""
        rec = self._channels.get(channel)
        if rec is not None and rec.phase is _Phase.REPLAYING:
            rec.deadline = self._clock.monotonic() + self._deadline
            rec.arm_pause_episodes = self._pause_episodes

    # --- clock-driven timers ------------------------------------------------------------------

    def due(self) -> list[Action]:
        """Fire any floor-waits whose floor has elapsed, and surface a
        :class:`RaiseRecoveryInterrupted` for any replay/history past its detection deadline."""
        now = self._clock.monotonic()
        actions: list[Action] = []
        for channel, rec in self._channels.items():
            if (
                rec.phase is _Phase.FLOOR_WAIT
                and rec.floor_wake is not None
                and now >= rec.floor_wake
                and rec.anchor is not None
            ):
                actions.append(self._begin_replay(rec, channel, rec.anchor, now))
            elif rec.phase is _Phase.REPLAYING and rec.deadline is not None and now >= rec.deadline:
                if self._backpressure_suspends(rec.arm_pause_episodes):
                    # Consumer stall, not server silence — re-arm, don't fire (platform ADR-0025).
                    rec.deadline = now + self._deadline
                    rec.arm_pause_episodes = self._pause_episodes
                else:
                    rec.phase = _Phase.IDLE
                    rec.deadline = None
                    rec.followup_anchor = None  # reset fully, matching the other failure paths
                    actions.append(
                        RaiseRecoveryInterrupted(
                            channel, "no replay_complete before detection deadline"
                        )
                    )
        for channel in list(self._history_deadline):
            deadline, arm = self._history_deadline[channel]
            if now >= deadline:
                if self._backpressure_suspends(arm):
                    self._history_deadline[channel] = (now + self._deadline, self._pause_episodes)
                else:
                    del self._history_deadline[channel]
                    actions.append(
                        RaiseRecoveryInterrupted(
                            channel, "no history_complete before detection deadline"
                        )
                    )
        return actions

    def next_deadline(self) -> float | None:
        """Earliest absolute time :meth:`due` needs to run, or ``None`` if nothing is pending."""
        times = [
            t
            for rec in self._channels.values()
            for t in (rec.floor_wake if rec.phase is _Phase.FLOOR_WAIT else None, rec.deadline)
            if t is not None
        ]
        times.extend(deadline for deadline, _arm in self._history_deadline.values())
        return min(times) if times else None

    # --- history ------------------------------------------------------------------------------

    def note_history_request(self, channel: str) -> None:
        """Arm the detection deadline for an in-flight history request on ``channel``."""
        self._history_deadline[channel] = (
            self._clock.monotonic() + self._deadline,
            self._pause_episodes,
        )

    def note_history_message(self, channel: str) -> None:
        """A history ``message`` arrived — reset that channel's idle history deadline (server
        silence, not consumer speed). No-op unless a history request is in flight."""
        if channel in self._history_deadline:
            self._history_deadline[channel] = (
                self._clock.monotonic() + self._deadline,
                self._pause_episodes,
            )

    def handle_history_complete(self, channel: str) -> None:
        self._history_deadline.pop(channel, None)

    # --- reconnect / failure ------------------------------------------------------------------

    def build_reconnect(self) -> SendReconnect | None:
        """The reconnect payload. With stored ``pos``, resume from it. Otherwise — **once connected
        at least once** — probe with an empty ``last_pos`` map (legal: the contract requires the
        key's presence, not a non-empty map) so a pure-Direct session elicits ``not_available`` and
        degrades instead of silently dropping. Returns ``None`` only on a genuine first connection
        (nothing to resume or probe) or after a known Direct degrade. Call :meth:`mark_connected`
        *after* this."""
        if self._direct:
            return None
        if self._pos:
            return SendReconnect(client_id=self.client_id, last_pos=dict(self._pos))
        if self._connected_once:
            return SendReconnect(client_id=self.client_id, last_pos={})
        return None

    def mark_connected(self) -> None:
        """Record that a connection has been established, so the *next* reconnect probes with an
        empty ``last_pos`` (Direct detection) even before any ``pos`` is seen. Read by
        :meth:`build_reconnect`; the client calls this right after ``build_reconnect()``."""
        self._connected_once = True

    def handle_not_available(self, channels: Iterable[str]) -> list[Action]:
        """``reconnect_error: not_available`` — the backend is Direct. Degrade to naive resubscribe
        (no retry loop) and emit one :class:`PossibleGap` per resubscribed channel."""
        self._direct = True
        self._pos.clear()
        self._channels.clear()
        return [EmitPossibleGap(channel) for channel in channels]

    def handle_recovery_failure(self, channel: str, reason: str) -> list[Action]:
        """A ``replay_failed``/replay error mid-recovery — surface a RecoveryInterruptedError and
        reset the channel."""
        rec = self._channels.get(channel)
        if rec is not None:
            rec.phase = _Phase.IDLE
            rec.deadline = None
            rec.followup_anchor = None
        return [RaiseRecoveryInterrupted(channel, reason)]

    def handle_disconnect(self) -> list[Action]:
        """A disconnect happened. Any channel mid-recovery (floor-wait or replaying) is a truncated
        recovery → RecoveryInterruptedError (the 1008-mid-replay trigger). Per-channel state resets;
        ``pos`` and ``client_id`` persist for the next reconnect-replay."""
        actions: list[Action] = []
        for channel, rec in self._channels.items():
            if rec.phase in (_Phase.FLOOR_WAIT, _Phase.REPLAYING):
                actions.append(RaiseRecoveryInterrupted(channel, "disconnected mid-recovery"))
        self._channels.clear()
        for channel in list(self._history_deadline):
            actions.append(RaiseRecoveryInterrupted(channel, "disconnected mid-history"))
        self._history_deadline.clear()
        return actions
