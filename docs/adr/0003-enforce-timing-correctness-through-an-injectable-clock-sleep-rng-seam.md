# ADR-0003: Enforce timing correctness through an injectable clock/sleep/RNG seam

**Status**: Accepted
**Date**: 2026-08-21
**Ticket**: feat/python-sdk

## Context

The SDK's correctness is dominated by timing: exponential backoff + jitter, heartbeat interval, pong timeout, replay rate floor (1/10s per channel), the refresh floor (30s), and the recovery detection deadline. Go's server side leans on `-race` to catch concurrency bugs; Python has no race detector. Tests that use real sleeps for these paths are slow and flaky, and — more importantly — non-deterministic timing hides ordering bugs the way an unguarded data race does on the server.

## Decision

Deterministic time is the enforcement mechanism — the explicit Python substitute for the race detector (§VII), treated as a design constraint, not merely a test convention. Every timing-sensitive path takes an injectable `Clock` that bundles the three sources of nondeterminism behind one seam: `monotonic` (deadlines/floors — never wall-clock, which can jump), `now` (wall-clock, only for comparing a JWT `exp`), an awaitable `sleep`, and `random` for jitter (`src/sukko/_clock.py`). Production uses `SystemClock`; unit tests inject a fake clock that advances virtual time with no real sleeping. No timing path calls `time`, `asyncio.sleep`, or `random` directly. The test matrix mandates deterministic coverage of backoff/jitter, heartbeat-timeout, replay floor + gap coalescing, the recovery deadline (`RecoveryInterruptedError`), and refresh floor, plus one assertion per contract message/error/close code, all against an in-process `FakeTransport`/`FakeServer`.

## Consequences

- Easier: timing tests are fast and reproducible; edge cases (a heartbeat missing its pong by exactly the timeout, backoff jitter bounds, a coalesced replay watermark) become assertable rather than probabilistic.
- Harder / coupling: the `Clock` must be threaded through every constructor that schedules anything (client, recovery, auth, backpressure, heartbeat), so it is a pervasive dependency; adding a new timing behavior means routing it through the seam rather than reaching for `time`/`random`. Ruff/mypy don't enforce "no direct `time` call," so the discipline is guarded by review and by the fake-clock tests failing loudly if a real sleep slips in.
- The seam also carries the RNG, so jitter is reproducible in tests without special-casing.

## Alternatives rejected

- **Real sleeps in tests** — slow and flaky; can't assert exact backoff/timeout boundaries.
- **Monkeypatching `time`/`asyncio.sleep`/`random` per test** — brittle, global, and leaks across tests; an explicit injected seam is localized and typed.
- **`freezegun`-style time freezing** — an added dependency that patches globally and doesn't cover `asyncio.sleep` cleanly; conflicts with the minimal-deps stance.
- **Wall-clock for deadlines** — can jump (NTP/suspend); `monotonic` is used for all floors and deadlines.
