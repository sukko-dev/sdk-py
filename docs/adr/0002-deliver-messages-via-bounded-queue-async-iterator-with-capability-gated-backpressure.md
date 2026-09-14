# ADR-0002: Deliver messages via a bounded-queue async iterator with capability-gated back-pressure

**Status**: Accepted
**Date**: 2026-08-21
**Ticket**: feat/python-sdk

## Context

The SDK's audience is long-running backend subscribers to a real-time feed, where a fast producer meets a slower consumer. The TS sibling delivers synchronously via `emit` with no buffer — a fast feed either drops or grows unbounded. The core also had to choose a concurrency substrate (Python 3.11+ makes `asyncio.TaskGroup`/`except*`/`Self` available) and a story for non-async callers, including Jupyter, whose already-running loop makes `asyncio.run()` throw. A cross-cutting constraint (§IX) is "explicit modes, no runtime detection."

## Decision

The async core is asyncio-first. Messages are delivered through one path: `async for msg in client.messages()` backed by a bounded `asyncio.Queue`; callbacks are optional sugar over the same stream. When the queue fills and the transport advertises `can_pause_receive`, the SDK stops reading the socket → TCP back-pressure → the platform's slow-client path (close 1008 → reconnect+recover); otherwise it applies a bounded buffer with an explicit overflow signal. Back-pressure is **capability-gated on the transport, never runtime-sniffed** (§IX) — WS is stream-capable (`can_pause_receive=True`), SSE is not. Connection lifecycle uses a **per-epoch TaskGroup**: an outer supervisor owns reconnect, and each connection opens a fresh connection-scoped TaskGroup (read-pump + heartbeat) torn down and recreated per reconnect epoch. Synchronous callers use `SyncSukkoClient`, which owns a private event loop on a background thread and submits coroutines via `run_coroutine_threadsafe` — never `asyncio.run()` — so one code path serves scripts and notebooks alike.

## Consequences

- Easier: "no silent drop" becomes testable; recovery (`replay`, `history`) reuses the same queue and iterator, so recovered records reach callers with zero handler-code change.
- Harder / coupling: because recovery shares the delivery queue, a recovery burst could fill it and pause its own socket, so the constructor enforces a hard floor `queue_maxsize ≥ history_limit + max_replay_messages` (sum, not max — a caller `history` and an automatic `replay` can be in flight together); this ties three config knobs together and is validated fail-fast. A single flat TaskGroup can't restart a member, forcing the per-epoch shape. The sync wrapper's background-loop lifecycle (esp. in Jupyter) is a known footgun and carries a mandatory running-loop equivalence test.
- The capability-gating decision is shared across SDKs: the same delivery *contract* is enforced per-transport (`ws`-backed Node transport / Python `websockets` = real back-pressure; WHATWG/browser = bounded-buffer+signal).

## Alternatives rejected

- **Callback-only delivery (the `@sukko/sdk` model)** — no back-pressure; a fast feed silently drops or OOMs.
- **Unbounded queue** — OOM on a fast feed behind a slow handler.
- **Runtime-sniffing back-pressure capability** — violates §IX explicit-modes; capability is declared on the transport instead.
- **`asyncio.run()` in the sync wrapper** — raises inside Jupyter's running loop; background-loop thread is the only Jupyter-safe option.
- **A separate sync implementation** — two code paths diverge; the wrapper reuses the async core.
