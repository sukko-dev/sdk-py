# ADR-0005: subscribe/unsubscribe on a live SSE stream bounces the connection

**Status**: Accepted
**Date**: 2026-09-30

## Context

SSE is receive-only and connect-time-subscribed: the channel set lives in the
`GET /sse?channels=…` URL, there is no live `subscribe` frame, and the gateway rejects an
empty set. The transport already captures the opaque `Last-Event-ID` cursor and echoes it
on reconnect, and the `Client` supervisor threads that cursor across epochs (the resume-
cursor work). What remained: a `subscribe`/`unsubscribe` on a *live* SSE stream previously
only recorded the desired set and applied it on the next natural reconnect — a silent mode
change (§XV) that also diverged from sdk-js, which bounces the stream immediately.

## Decision

On a live SSE connection (`capabilities.can_subscribe` is false), `subscribe`/`unsubscribe`
mutate the desired set synchronously and then **bounce** the connection: the epoch is closed
and the supervisor redials with the new channel set, resuming lost messages via
`Last-Event-ID`. The bounce is marked deliberate — the reconnect is immediate, with no
backoff and without burning a retry attempt (it is not a failure). Unsubscribing the last
channel **parks** the supervisor (it does not dial an empty `?channels=`) until a later
`subscribe` repopulates the set and wakes it. This matches sdk-js and the sdk-go contract
(ADR-0015 / ADR-0014).

**subscribe never auto-connects**: a `subscribe` while not connected only records the desired
set; the caller drives `connect()` explicitly. And the first `connect()` with an empty SSE
set is a caller error surfaced as a typed connect failure — **not** a benign no-op. sdk-js
makes both a no-op because its `connect()` is invoked fire-and-forget from browser
online/visibility listeners where a throw escapes uncaught; this SDK dropped those
browser-isms (see the client module's parity note), so an explicit, awaited `connect()`
returns the error. Same behavioral contract (never dial SSE with an empty set), idiomatic
surface per language.

## Consequences

- **Easier**: SSE subscribe/unsubscribe take effect immediately and losslessly; parity with
  sdk-js; no silent deferral.
- **Harder**: the supervisor gains a deliberate-bounce path (skip backoff, don't count the
  attempt) and a park-until-`_desired_changed` wait on the empty-SSE-reconnect leg. WebSocket
  is untouched (it subscribes live; its dial takes no channels, so it never parks).

## Alternatives rejected

- **Silent-defer** (record now, apply on the next natural reconnect): the §XV mode change and
  the sdk-js divergence this ADR closes.
- **Auto-connect on subscribe / no-op on empty connect** (the sdk-js surface): browser-isms
  not appropriate for an explicit-lifecycle backend client.

## Cross-references

- sdk-go ADR-0015 (SSE Last-Event-ID recovery + subscribe-bounce) and ADR-0014 (empty-desired-
  set parks on reconnect) — the shared cross-SDK contract this implements in Python.
