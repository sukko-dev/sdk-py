"""Injectable clock / sleep / RNG seam — the determinism substitute for the race detector.

Python has no ``-race``. Deterministic time is how the SDK's timing-sensitive logic
(backoff, jitter, heartbeat interval, pong timeout, replay floor, recovery deadline) is made
testable: every such path takes a :class:`Clock` instead of calling :mod:`time`/:mod:`asyncio`/
:mod:`random` directly. Production code uses :class:`SystemClock`; tests inject a fake clock that
advances virtual time with no real sleeping.

See docs/engineering-principles.md §VII (Determinism).
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Time, sleep, and jitter — the three sources of nondeterminism, behind one seam.

    ``monotonic`` drives deadlines and rate floors (never wall-clock, which can jump).
    ``now`` is wall-clock Unix seconds, used only to compare against a JWT ``exp`` claim.
    ``sleep`` is the awaitable delay used by backoff. ``random`` returns a float in ``[0.0, 1.0)``
    for jitter.
    """

    def monotonic(self) -> float:
        """Monotonic clock in seconds — for durations, deadlines, and rate floors."""
        ...

    def now(self) -> float:
        """Wall-clock time in Unix seconds — only for comparing against a token ``exp``."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Suspend the current task for ``seconds`` (may be 0)."""
        ...

    def random(self) -> float:
        """A pseudo-random float in ``[0.0, 1.0)`` — the sole jitter source."""
        ...


class SystemClock:
    """Production :class:`Clock`: real monotonic/wall time, :func:`asyncio.sleep`, real RNG.

    A private :class:`random.Random` instance is used so that seeding it (or swapping it in a
    test) never perturbs the global :mod:`random` state, and vice-versa.
    """

    __slots__ = ("_rng",)

    def __init__(self, *, seed: int | None = None) -> None:
        self._rng = random.Random(seed)

    def monotonic(self) -> float:
        return time.monotonic()

    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)

    def random(self) -> float:
        return self._rng.random()


#: The default clock used when a caller injects none. A module-level singleton is safe: it is
#: stateless apart from its private RNG, which callers may reseed via ``SystemClock(seed=...)``.
SYSTEM_CLOCK: SystemClock = SystemClock()


def full_jitter_backoff(
    attempt: int,
    *,
    base: float,
    cap: float,
    clock: Clock,
) -> float:
    """Exponential backoff with *full jitter*, capped (§IV).

    Returns a delay in ``[0, min(cap, base * 2**attempt))`` — the AWS "full jitter" strategy,
    which decorrelates reconnect storms better than equal-jitter. ``attempt`` is 0-based
    (attempt 0 → up to ``base``). The RNG is drawn from ``clock`` so timing tests stay
    deterministic.
    """
    if attempt < 0:
        raise ValueError("attempt must be >= 0")
    ceiling = min(cap, base * (2.0**attempt))
    return clock.random() * ceiling
