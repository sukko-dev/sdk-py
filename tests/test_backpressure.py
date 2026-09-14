"""Back-pressure tests. Deterministic; no real sleeps.

Back-pressure is asserted by the *mechanism* — a full queue blocks ``put()`` (so the read-pump stops
draining) — not by observing a ``pause()`` call, which is deliberately not on the hot path.
"""

from __future__ import annotations

import asyncio

import pytest

from sukko.backpressure import DeliveryQueue
from sukko.errors import ConfigurationError
from sukko.messages import Message, Overflow


def _msg(seq: int) -> Message:
    return Message(seq=seq, ts=0, channel="acme.a", data={})


def test_constructor_rejects_below_floor() -> None:
    with pytest.raises(ConfigurationError):
        DeliveryQueue(maxsize=2, can_pause_receive=True, history_limit=1, max_replay_messages=2)
    # exactly the floor is allowed
    DeliveryQueue(maxsize=3, can_pause_receive=True, history_limit=1, max_replay_messages=2)


async def test_pausable_put_blocks_when_full() -> None:
    queue = DeliveryQueue(maxsize=3, can_pause_receive=True, history_limit=1, max_replay_messages=2)
    for i in range(3):
        await queue.put(_msg(i))  # fills to maxsize without blocking
    blocked = asyncio.ensure_future(queue.put(_msg(99)))
    await asyncio.sleep(0)
    assert not blocked.done(), "put must block when full — this IS the back-pressure signal"
    got = await queue.get()  # free one slot
    assert isinstance(got, Message)
    await blocked  # now the blocked put completes
    assert queue.qsize() == 3


async def test_co_fill_burst_fits_at_floor_without_backpressure() -> None:
    # A concurrent history(=history_limit) + replay(=max_replay) burst must fit at the
    # floor without any put blocking (else recovery would self-sabotage into 1008).
    history_limit, max_replay = 4, 6
    queue = DeliveryQueue(
        maxsize=history_limit + max_replay,
        can_pause_receive=True,
        history_limit=history_limit,
        max_replay_messages=max_replay,
    )
    for i in range(history_limit + max_replay):
        put = asyncio.ensure_future(queue.put(_msg(i)))
        await asyncio.sleep(0)
        assert put.done(), f"burst item {i} must not block below the floor"
        await put


async def test_nonpausable_overflow_drops_oldest_and_signals() -> None:
    queue = DeliveryQueue(
        maxsize=3,
        can_pause_receive=False,
        history_limit=1,
        max_replay_messages=2,
        overflow_policy="drop_oldest",
    )
    for i in range(5):  # 5 into a size-3 queue → 2 dropped (the oldest: seq 0, 1)
        await queue.put(_msg(i))
    first = await queue.get()
    assert isinstance(first, Overflow)
    assert first.dropped == 2  # loss is signalled, never silent
    remaining = [await queue.get() for _ in range(3)]
    assert [m.seq for m in remaining if isinstance(m, Message)] == [2, 3, 4]  # newest kept


async def test_nonpausable_drop_newest_keeps_oldest() -> None:
    queue = DeliveryQueue(
        maxsize=2,
        can_pause_receive=False,
        history_limit=1,
        max_replay_messages=1,
        overflow_policy="drop_newest",
    )
    for i in range(4):
        await queue.put(_msg(i))
    overflow = await queue.get()
    assert isinstance(overflow, Overflow) and overflow.dropped == 2
    kept = [await queue.get() for _ in range(2)]
    assert [m.seq for m in kept if isinstance(m, Message)] == [0, 1]  # oldest kept, newest dropped
