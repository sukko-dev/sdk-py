"""Smoke tests for the virtual-time FakeClock — the foundation every timing test relies on."""

from __future__ import annotations

import asyncio

from fakes import FakeClock


async def test_sleep_wakes_only_after_advancing_past_deadline() -> None:
    clock = FakeClock()
    woke = False

    async def sleeper() -> None:
        nonlocal woke
        await clock.sleep(5.0)
        woke = True

    task = asyncio.ensure_future(sleeper())
    await clock.advance(4.0)
    assert not woke, "must not wake before the deadline"
    await clock.advance(1.0)
    await task
    assert woke
    assert clock.monotonic() == 5.0


async def test_sleepers_wake_in_deadline_order() -> None:
    clock = FakeClock()
    order: list[str] = []

    async def sleeper(name: str, delay: float) -> None:
        await clock.sleep(delay)
        order.append(name)

    tasks = [
        asyncio.ensure_future(sleeper("late", 5.0)),
        asyncio.ensure_future(sleeper("early", 3.0)),
    ]
    await clock.advance(3.0)
    assert order == ["early"]
    await clock.advance(2.0)
    await asyncio.gather(*tasks)
    assert order == ["early", "late"]
