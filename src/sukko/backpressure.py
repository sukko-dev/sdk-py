"""Bounded delivery queue with capability-gated back-pressure.

One queue backs ``client.messages()`` for the whole client lifetime (it survives reconnect epochs so
the consumer's ``async for`` never breaks). How a full queue behaves is **gated on the transport
capability, in ``put()`` — never runtime-sniffed** (§XV):

- **Pausable transport (WebSocket):** ``put()`` simply *blocks*. In the pull-model read-pump, a
  blocked ``put()`` means ``recv()`` is not being called → ``websockets`` buffers to ``max_queue`` →
  TCP back-pressure → the platform's own slow-client path. Blocking-put **is** the pause; no
  separate high/low-water ``pause()`` call (that would be two mechanisms for one behavior).
- **Non-pausable transport (SSE):** stopping an httpx read does not give clean back-pressure, so the
  queue keeps a **bounded buffer** and applies an explicit overflow policy, surfacing an in-band
  :class:`~sukko.messages.Overflow` marker — **never a silent drop** (§III).

**Construction floor:** ``maxsize >= history_limit + max_replay_messages`` — a concurrent
caller ``history`` (≤ ``history_limit``) and an automatic ``replay`` (≤ ``max_replay_messages``)
must both fit, so a recovery burst cannot alone trip back-pressure → 1008 → unrecoverable on Direct.
Violating it raises :class:`~sukko.errors.ConfigurationError` at construction.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Literal

from .constants import MAX_REPLAY_MESSAGES
from .errors import ConfigurationError
from .messages import DeliveredItem, Overflow

#: For a real-time feed the newest data is the most valuable, so the default evicts the **oldest**
#: queued item to admit the newest. ``drop_newest`` discards the incoming item instead.
OverflowPolicy = Literal["drop_oldest", "drop_newest"]


class DeliveryQueue:
    """A bounded queue of :class:`~sukko.messages.DeliveredItem`. Pausable transports get real
    back-pressure via a blocking :meth:`put`; non-pausable transports get bounded-buffer + overflow
    signalling."""

    def __init__(
        self,
        maxsize: int,
        *,
        can_pause_receive: bool,
        history_limit: int,
        max_replay_messages: int = MAX_REPLAY_MESSAGES,
        overflow_policy: OverflowPolicy = "drop_oldest",
    ) -> None:
        floor = history_limit + max_replay_messages
        if maxsize < floor:
            raise ConfigurationError(
                f"queue_maxsize ({maxsize}) must be >= history_limit + max_replay_messages "
                f"({history_limit} + {max_replay_messages} = {floor}) so a history+replay burst "
                f"cannot alone trip back-pressure"
            )
        # A ``None`` item is the shutdown sentinel enqueued by :meth:`close` to end ``messages()``.
        self._queue: asyncio.Queue[DeliveredItem | None] = asyncio.Queue(maxsize)
        self._can_pause = can_pause_receive
        self._policy = overflow_policy
        self._dropped = 0  # pending, un-signalled overflow count (non-pausable path only)

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    def qsize(self) -> int:
        return self._queue.qsize()

    async def put(self, item: DeliveredItem) -> None:
        """Enqueue ``item``. On a pausable transport this **blocks when full** (the back-pressure
        signal). On a non-pausable transport it applies the overflow policy and records the loss."""
        if self._can_pause:
            await self._queue.put(item)
            return
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            if self._policy == "drop_oldest":
                self._queue.get_nowait()  # evict oldest, admit newest (freshness wins)
                self._queue.put_nowait(item)
            # drop_newest: `item` is discarded
            self._dropped += 1

    async def get(self) -> DeliveredItem | None:
        """Return the next item, or ``None`` once :meth:`close` has been called and the buffer is
        drained (the shutdown sentinel that ends ``messages()``). If drops occurred on the
        non-pausable path, an ``Overflow`` marker is surfaced first (coalescing pending drops)."""
        if self._dropped:
            dropped = self._dropped
            self._dropped = 0
            return Overflow(dropped=dropped)
        return await self._queue.get()

    def close(self) -> None:
        """Enqueue the shutdown sentinel so a blocked/looping :meth:`get` returns ``None`` and the
        consumer's ``async for`` ends after draining what remains. Infallible: on a full (back-
        pressured) queue, evict one item to make room so the sentinel is always delivered."""
        try:
            self._queue.put_nowait(None)
        except asyncio.QueueFull:
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()  # evict oldest to guarantee the sentinel lands
            self._queue.put_nowait(None)

    def drain_nowait(self) -> list[DeliveredItem]:
        """Non-blocking drain of everything currently buffered — used by ``close()`` to flush within
        a bounded timeout before releasing resources."""
        items: list[DeliveredItem] = []
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is not None:
                items.append(item)
        return items
