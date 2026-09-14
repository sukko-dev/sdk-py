# Sukko Python SDK Engineering Principles

These are the engineering principles that govern this SDK's codebase — the SDK
adaptation of the [Sukko platform engineering principles](https://github.com/sukko-dev/sukko/blob/main/docs/engineering-principles.md).
Code comments cite them by section: a comment like `// per §VII` refers to a
section of one of the two documents. Each heading below carries a parenthetical
cross-reference (e.g. `(§VIII)`) naming the platform section it adapts — use it
to map between the two numberings; a cited section that does not exist below
(e.g. §XV–§XVIII) always refers to the platform document. Architecture
decisions are recorded separately in [`docs/adr/`](adr/).

This is the published form of the project's internal engineering rules; the
two are kept in sync on every amendment.

> **Shared across all Sukko SDKs.** Principles **I** (contracts), **XI** (language quality bar),
> and **XII** (prior-art research) are the *shared Sukko SDK constitution* — they hold identically
> for `sdk-py`, `sdk-js` (`@sukko/sdk`), `sdk-go`, and any future SDK. Each SDK keeps the same
> **behavioral contract** while being **idiomatic to its own language**; a new SDK adopts these
> same principles, adapted to its ecosystem. The remaining principles here are this repo's
> Python-specific adaptations of the same platform values.

## I. Contracts are the single source of truth (§XII/§XVI/§XVII)

Every public type and every runtime behavior MUST derive from Sukko's **authoritative API
contracts** — never from `ws/internal/shared/protocol` (server-internal), and never copied from a
sibling SDK's current behavior (which may lag). The two contracts, referenced explicitly:

- **AsyncAPI** — [`ws/docs/asyncapi/client-ws.asyncapi.yaml`](https://github.com/sukko-dev/sukko/blob/main/ws/docs/asyncapi/client-ws.asyncapi.yaml) in the platform repo — the WebSocket client protocol
  (message `type`s, payload schemas, error/close codes, auth bindings, channel format).
- **OpenAPI** — [`ws/docs/openapi/gateway.openapi.yaml`](https://github.com/sukko-dev/sukko/blob/main/ws/docs/openapi/gateway.openapi.yaml) in the platform repo — the REST surface (publish, auth,
  SSE, push), status/error codes, and payload limits.

Wire `type` consts, field names, error codes, close codes, and limits MUST match the contracts
**exactly** (`message` not `data`; `auth` not `auth_refresh`). A **contract-coverage test** asserts
every AsyncAPI message type has a typed model. When the contract and an existing SDK's
behavior disagree, **the contract wins** and the drift is filed upstream (never silently matched).
Deliberate divergences from `@sukko/sdk` (header-default auth, back-pressure, gap recovery) MUST be
documented.

## II. Defense in depth (§II)

Validate at every boundary. Decode every server message through `msgspec` (reject malformed —
e.g. a `gap` missing its required `last_pos`). Diff subscribe grants and surface not-granted
channels. Validate constructor config (e.g. `queue_maxsize ≥ history_limit + max_replay_messages`)
and fail fast with a clear error — never silently default to wrong state.

## III. Error handling — no silent failures (§III)

Every contract error/close/HTTP code maps to a **typed exception** (`SukkoError` hierarchy).
`publish()` while not connected raises `NotConnectedError` immediately — never a silent no-op.
A truncated recovery raises `RecoveryInterruptedError`, never a bare disconnect. `not_available`
(Direct backend) is a **typed capability signal**, not a retryable error. Wrap with context; no
raw transport tracebacks leak to callers.

## IV. Graceful degradation (§IV)

Reconnect uses **exponential backoff + jitter, capped**. On the Direct backend, pos-recovery is
absent → degrade to naive resubscribe (no retry loop) and emit a `PossibleGap` data-loss signal.
Handshake 429 (`TENANT_LIMIT_EXCEEDED`) backs off — never hammers. Optional features (SSE, push)
are edition-gated with typed errors, never half-initialized.

## V. Structured logging — no secrets (§V, §IX)

Use the stdlib `logging` module with **structured `extra`** fields, not f-strings-into-messages:
`logger.info("connected", extra={"transport": "ws", "tenant": tenant})`. Use appropriate levels
(debug/info/warning/error). The SDK's logger is `logging.getLogger("sukko")` and is **library-quiet
by default** (a `NullHandler` is attached; the SDK never configures the root logger or adds
handlers the application didn't ask for). **Credentials MUST never reach a log record, error
message, or `repr`** — every such string passes through `_redact` (value-based masking of the
registered token/api-key/push-key + pattern masking of `token=`/`api_key=`/`Authorization`/
`X-API-Key`). Redaction is asserted by test, not assumed "by construction."

## VI. Concurrency safety (§VII)

The asyncio analog of the platform's goroutine rules:
- **Per-epoch `TaskGroup`**: an outer supervisor owns reconnect; each connection opens a fresh
  connection-scoped `TaskGroup` (read-pump + heartbeat) torn down and recreated per reconnect
  epoch. A single flat `TaskGroup` cannot restart a member — wrong shape for a per-epoch read-pump.
- **Graceful `close()`**: cancel tasks → await them → drain the queue within a bounded timeout →
  close the transport. No orphaned tasks, no "Task was destroyed" warnings.
- **`CancelledError` is always re-raised** — never swallowed.
- Back-pressure is **capability-gated** (transport `can_pause_receive`), never runtime-sniffed.
- The delivery pipeline (decode → queue → `messages()`) is the hot path: feature work (auth
  refresh, subscription tracking, metrics) MUST NOT block it.

## VII. Determinism = the race-detector substitute (§VIII)

Python has no `-race`. **Deterministic time is the enforcement mechanism.** All timing paths
(backoff, jitter, heartbeat interval, pong timeout, replay floor, recovery deadline) take an
**injectable clock + `sleep` + RNG** (`_clock.py`). Timing tests use a fake clock — **never real
sleeps**. Unit tests run against an in-process `FakeTransport`/`FakeServer`. Test coverage is
mandatory: one assertion per client→server message, per server→client event/error, per close code,
plus heartbeat-timeout, cancellation/close-drain, back-pressure overflow, recovery, and
**sync≡async equivalence** (incl. the Jupyter running-loop case).

## VIII. Security (§IX)

Credentials sent via **request headers by default** (`Authorization: Bearer` / `X-API-Key`);
query-param auth is opt-in and its leak surface is closed by redaction. JWT refresh issues a new
token (never extends). Auth refresh is single-flight with a configurable floor (default 30s) to
avoid an `auth_error`→refresh loop. Edition gates (SSE/publish = Pro; push = Enterprise) surface
`EditionRequiredError`, never a raw 403.

## IX. Simplicity — explicit modes (§XV)

No implicit mode detection, no dual-purpose values, no silent fallback. The `auth` message is
dual-purpose (refresh vs escalation) — the mode-switch is explicit off the SDK's **owned
credential state**, not runtime-detected. Backend capability (Kafka vs Direct) is driven by the
explicit `reconnect_error: not_available` signal, not inferred from absent `pos`. The wire `gap`
(`last_pos` required) and the synthetic `PossibleGap` (channel-only) are **distinct types** — one
value never means two things.

## X. Cross-repo awareness (§XVI)

Sibling repos: [`sukko`](https://github.com/sukko-dev/sukko) (platform + contracts),
[`sdk-js`](https://github.com/sukko-dev/sdk-js) (TS SDK), [`sdk-go`](https://github.com/sukko-dev/sdk-go)
(Go SDK), [`cli`](https://github.com/sukko-dev/cli) (operator CLI — NOT this SDK's domain),
[`docs`](https://github.com/sukko-dev/docs) (docs site — its SDK reference pages are CI-generated
from source). Contract drift found while building is filed upstream, not worked around here.

## XI. Language quality bar — idiomatic, robust, performant, secure (SHARED)

**Applies to every Sukko SDK, adapted to its language.** The implementation MUST be **idiomatic to
its own language and ecosystem** — not a transliteration of a sibling SDK. Beyond idiom, every
change MUST clear five bars:

- **Idiomatic** — Python: PEP 8, asyncio idioms (`async with`, `TaskGroup`, `async for`), full type
  annotations + shipped `py.typed`, passes `ruff` and `mypy --strict`; prefer the stdlib and small
  focused deps over frameworks. (TS sibling: `strict`, `isolatedDeclarations`, no `any`, ESM+CJS.)
- **Robust** — typed errors and no silent failures (§III), graceful degradation (§IV), a clean
  async/task lifecycle with no orphaned tasks (§VI), input validated at every boundary (§II). Edge
  cases (empty, nil, max, error paths, cancellation) are covered by tests (§VII).
- **Performant** — the message-delivery pipeline is the hot path: no needless allocation or
  re-serialization, back-pressure over unbounded buffering, feature work never blocks delivery. Use
  the fast path the language offers (here: `msgspec` decode, lock-free where possible).
- **Secure** — credentials never appear in logs, error messages, or `repr` (§V/§VIII); header-default
  auth; TLS for external endpoints; validate untrusted server input before acting on it.
- **No dead code** — every code path, capability branch, exported symbol, and message/struct field
  MUST be reachable and exercised. A discovered dead or unreachable path MUST be **removed** (or made
  reachable), never merely guarded around; a degenerate never-taken branch is a bug, not defensive
  coding. No stub/no-op implementations ship — unbuilt work stays out of scope, not empty
  scaffolding. Enforced as part of definition-of-done by `ruff` (`F401`/`F841`) and `mypy --strict`.

The two implementations share the same **behavioral contract**; they need not share code shape.
Correctness over pattern: if a sibling SDK does it wrong, fix it there too — don't copy the defect.

## XII. Prior-art & industry research — mandatory, every change (SHARED)

**Before designing any feature, fixing any bug, or making any improvement**, research how the
problem is already solved — **on the internet, not from memory** (docs and training data go stale).
This mirrors the platform constitution's §XI. Research and briefly document:

1. **The common industry pattern** — how established real-time clients solve it: Pusher, Ably,
   Socket.IO, Phoenix Channels, Centrifugo, PubNub, and the platform's own prior art.
2. **Failure modes & edge cases** mature implementations handle (reconnect storms, token races,
   back-pressure, partial recovery, ordering, idempotency).
3. **Language-ecosystem norms** — for Python: asyncio patterns, `websockets`/`httpx` best practice,
   packaging/typing conventions; for the TS sibling: `ws`/WHATWG streams, ESM/CJS, `d.ts` shape.
4. **Where and why this SDK deviates** from the common pattern.

"Not invented here" solutions to already-solved problems are forbidden. A change without this
research is incomplete — cite the sources (PR description or a code comment).

## XIII. Decision Records

Durable engineering decisions are recorded as Architecture Decision Records in
[`docs/adr/`](adr/) — any choice that is likely to be challenged, expensive to reverse, or
needed by a future contributor MUST be recorded at the moment it is made. Accepted ADRs are
never edited — they are superseded by new ones. ADRs capture the decision, its context and
consequences, and the rejected alternatives.

Planning artifacts are ephemeral — there are no per-feature specification or plan documents
in the repository; the durable outputs of design work are ADRs and committed documentation.

## Governance

When platform-side behavior and this document disagree, the **contracts** win. The numbered
principles above are amended, not overridden. The platform-wide rules live in the
[platform engineering principles](https://github.com/sukko-dev/sukko/blob/main/docs/engineering-principles.md).
