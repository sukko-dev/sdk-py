# ADR-0006: Translate the SSE recovery control frames to existing surfaces; defer precise per-channel recovery

**Status**: Accepted
**Date**: 2026-10-01

## Context

The platform added two SSE reconnect-recovery control frames (platform slice 3b,
`gateway.openapi` 1.0.3), delivered on the SSE stream as `event: message` with the type in
`data.type`, like the existing `gap` notification:

- `{"type":"no_replay","channels":[...]}` — cursor channels the server could not replay on
  reconnect (unauthorized, no Kafka mapping on a direct backend, or the replay errored).
- `{"type":"replay_truncated","replayed":N}` — the reconnect replay was cut short at the server's
  `WS_MAX_REPLAY_MESSAGES` cap; `N` records were delivered and a gap remains.

These are **SSE-only**: the server emits them only on the gRPC `Subscribe` (SSE) path, so they live
in `gateway.openapi`, not the WS `client-ws.asyncapi.yaml` this SDK vendors. Today both frames fail
the `ServerMessage` union decode (unknown tag) and are dropped by `_read_pump`'s log-and-skip.

**sukko-py's SSE recovery is optimistic.** Unlike sukko-js — which pessimistically emits a synthetic
`PossibleGap` for *every* desired channel on *every* SSE reopen because a receive-only transport
cannot confirm live replay — sukko-py trusts the server's `Last-Event-ID` replay (the opaque resume
cursor threaded across epochs in `_run`) and emits `PossibleGap` only on the WS
`reconnect_error: not_available` Direct-degrade. (sukko-go is likewise optimistic — its
coalesced-`PossibleGap` snapshot is populated only by the WS `subscription_ack` path, so it emits
none on SSE either.) So before this change, a channel the server could not replay on an SSE
reconnect was **silently lost** in sukko-py: no replay, no signal.

Prior art (§XII): Centrifugo returns a `recovered` boolean per subscription (complete or failed,
never partial; `false` → use the history API); Ably sets `resumed=false` on reattach for the same
purpose. Both collapse the outcome into "fully recovered vs. may-have-gap" at the channel/connection
level. `no_replay` is exactly that per-channel negative signal; `PossibleGap` is this SDK's existing
expression of it. Sukko deliberately *delivers the recovered prefix and flags the remainder* where
Centrifugo discards partials — so `replay_truncated` maps to this SDK's existing truncated-recovery
advisory rather than to a `PossibleGap`.

## Decision

Decode both frames in a second pass inside `_read_pump`'s existing `except msgspec.DecodeError`
branch — a small non-exported tagged union (`SSEControlFrame = NoReplay | ReplayTruncated`) with its
own decoder. On a hit, translate to **existing** surfaces; on a miss, log-and-skip exactly as today.
The frames are **not** `ServerMessage` members, so the contract-coverage count (18 server tags) and
the phantom-model check are untouched, and no public type is exported (the application sees only the
existing `PossibleGap` / `RecoveryInterruptedError`).

- `no_replay` → one `PossibleGap(channel=…)` per channel, onto `messages()`. Because sukko-py has no
  blanket, this is the **only** signal for those channels — it closes the pre-slice-3b silent-loss
  window. (This differs from sukko-js, where the blanket already covers `no_replay` so it is
  recognized-but-not-re-signaled there; sukko-go matches sukko-py — see Consequences.)
- `replay_truncated` → a channel-less `RecoveryInterruptedError` via `on_error` ("reconnect replay
  truncated at the server cap; N delivered, a gap remains"). Connection-level, not channel-scoped.

The **precise/complete** recovery model is **not** claimed and is deferred to a platform-first arc
(a future ADR will supersede this one). `no_replay` *narrows* sukko-py's silent-loss window; it does
not close it, because of two platform protocol gaps:

1. **No recovery-complete sentinel** on the SSE reconnect path — a client cannot await the absence
   of `no_replay` to conclude a channel was fully recovered.
2. **The quiet-channel hole.** `no_replay` is derived from the cursor (`lastPos`); a channel
   subscribed but with no pos-bearing message before the drop has no cursor entry, so it gets neither
   replay nor `no_replay`. sukko-py still trusts it silently. The cursor is opaque to the client, so
   the client cannot compute requested-minus-cursor. (The gateway's cursor map is also not seeded
   from the inbound `Last-Event-ID`, so quiet channels erode out of the cursor across reconnects —
   this compounds the hole and is design input for the deferred arc, not fixed here.)

## Consequences

- Minimal, additive: a mini-union + one `_dispatch_sse_control` method; no new public type, no
  change to `ServerMessage`, the coverage count, or the vendored AsyncAPI.
- sukko-py SSE clients now receive a `PossibleGap` for every server-reported unreplayable channel
  (previously a silent loss) and a `RecoveryInterrupted` when the replay is truncated.
- **§XVIII cross-SDK divergence (pre-existing, documented).** sukko-js is pessimistic (a blanket
  `PossibleGap` on every SSE reopen) and so recognizes-but-does-not-re-signal `no_replay`; sukko-py
  and sukko-go are optimistic (no blanket) and translate `no_replay` to a per-channel `PossibleGap`.
  The surfaces differ because each SDK's recovery model dictates what is redundant; the shared
  invariant is **no silent recovery loss**. The deferred precision arc converges sukko-js onto the
  optimistic model (dropping its blanket) and is the right place to re-unify the behavior.

## Alternatives rejected

- **Add `no_replay`/`replay_truncated` as `ServerMessage` members**: would force them into the WS
  AsyncAPI this SDK vendors (documenting WS frames that never flow over WS) and break the
  exact-count coverage test. They are SSE (gateway.openapi) frames; the SDK translates them.
- **Recognize `no_replay` but don't re-signal (the sukko-js choice)**: wrong for sukko-py — with no
  blanket, that would re-introduce the silent-loss window this change closes.
- **Claim precise/complete recovery now**: blocked on the two protocol gaps above; deferred to a
  platform-first arc.
