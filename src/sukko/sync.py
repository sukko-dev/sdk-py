"""Synchronous wrapper — one code path over the async core, Jupyter-safe.

``SyncSukkoClient`` owns a **private event loop on a background daemon thread** and submits every
operation to it via :func:`asyncio.run_coroutine_threadsafe`, blocking the caller for the result. It
**never** calls :func:`asyncio.run` — which raises inside IPython/Jupyter (whose main thread already
runs a loop). Because the loop lives on its own thread, the wrapper works identically from a plain
script and from a notebook cell (a call blocks only the *calling* thread while it is outstanding).

Behavior is not re-implemented: every method delegates to the same
:class:`~sukko.client.SukkoClient` (no divergent behavior). ``stream()`` is the blocking
analog of ``async for`` over ``messages()``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Coroutine, Iterator, Sequence
from contextlib import suppress
from typing import Any, Self, TypeVar, cast

from .client import SukkoClient
from .messages import DeliveredItem
from .transport.base import ConnectionState

_T = TypeVar("_T")

#: How long ``close()`` waits for the async client to close before stopping the loop anyway, so
#: teardown always makes progress even if a coroutine on the loop is wedged.
_CLOSE_TIMEOUT = 30.0


class SyncSukkoClient:
    """Blocking client backed by a background asyncio loop. Constructor args mirror
    :class:`~sukko.client.SukkoClient`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._closed = False
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name="sukko-sync-loop", daemon=True
        )
        self._thread.start()
        try:
            # Build the async client ON the loop thread so its asyncio primitives bind to that loop.
            self._client: SukkoClient = self._run(self._build(args, kwargs))
        except BaseException:
            self._shutdown_loop()  # never leak the thread/loop if construction fails
            raise

    @staticmethod
    async def _build(args: tuple[Any, ...], kwargs: dict[str, Any]) -> SukkoClient:
        return SukkoClient(*args, **kwargs)

    def _run(self, coro: Coroutine[Any, Any, _T], *, timeout: float | None = None) -> _T:
        """Submit ``coro`` to the background loop and block for its result."""
        if threading.current_thread() is self._thread:
            # A user callback runs on the loop thread; blocking on a coroutine that can only run
            # when this thread yields would deadlock. Fail loudly instead.
            coro.close()  # avoid a "coroutine was never awaited" warning
            raise RuntimeError(
                "SyncSukkoClient methods must not be called from within a callback running on the "
                "client's own event loop (that would deadlock)"
            )
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def _shutdown_loop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    # --- lifecycle ----------------------------------------------------------------------------

    def connect(self) -> None:
        self._run(self._client.connect())

    def close(self) -> None:
        """Close the client and stop the background loop/thread. Idempotent; safe to call twice."""
        if self._closed:
            return
        self._closed = True
        # §IV: cleanup continues on failure — never let a close error block loop teardown.
        with suppress(Exception):
            if hasattr(self, "_client"):
                self._run(self._client.close(), timeout=_CLOSE_TIMEOUT)
        self._shutdown_loop()

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # --- operations ---------------------------------------------------------------------------

    def subscribe(self, channels: Sequence[str]) -> None:
        self._run(self._client.subscribe(channels))

    def unsubscribe(self, channels: Sequence[str]) -> None:
        self._run(self._client.unsubscribe(channels))

    def publish(self, channel: str, data: object) -> None:
        self._run(self._client.publish(channel, data))

    def rest_publish(self, channel: str, data: object) -> str | None:
        return self._run(self._client.rest_publish(channel, data))

    def refresh_token(self) -> None:
        self._run(self._client.refresh_token())

    def escalate(self, jwt: str) -> None:
        self._run(self._client.escalate(jwt))

    def update_token(self, token: str) -> None:
        # Synchronous: the token store is an atomic reference swap and the redactor is lock-guarded,
        # so this is safe to run on the calling thread without hopping to the loop.
        self._client.update_token(token)

    @property
    def state(self) -> ConnectionState:
        return self._client.state

    @property
    def subscriptions(self) -> frozenset[str]:
        return self._client.subscriptions

    def stream(self) -> Iterator[DeliveredItem]:
        """Blocking iterator over delivered messages — the sync analog of ``async for msg in
        client.messages()``. Ends when the client closes."""
        messages = self._client.messages()
        try:
            while True:
                try:
                    item = self._run(self._anext(messages))
                except StopAsyncIteration:
                    return
                yield item
        finally:
            # Deterministically close the async generator on the loop (skip if the client is gone).
            if not self._closed:
                gen = cast("AsyncGenerator[DeliveredItem, None]", messages)
                with suppress(Exception):
                    self._run(gen.aclose())

    @staticmethod
    async def _anext(messages: AsyncIterator[DeliveredItem]) -> DeliveredItem:
        return await anext(messages)
