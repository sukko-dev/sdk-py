# ADR-0001: Derive the SDK from the authoritative AsyncAPI/OpenAPI contracts, not from the TypeScript sibling

**Status**: Accepted
**Date**: 2026-08-21
**Ticket**: feat/python-sdk

## Context

Sukko's first SDK is TypeScript (`@sukko/sdk`, `../sukko-js`), and the obvious move for a second SDK is to port its behavior. But review found the TS SDK *lags* the contract: no back-pressure, resubscribe-only recovery, and no `gap`/`replay`/`history` surface (a founding constraint). Two authoritative contracts already exist — the WebSocket AsyncAPI v1.4.0 (`../sukko/ws/docs/asyncapi/client-ws.asyncapi.yaml`) and the gateway OpenAPI (`../sukko/ws/docs/openapi/gateway.openapi.yaml`) — and the server-internal `ws/internal/shared/protocol` must never be exposed or copied. A mechanical choice also had to be made: generate models from the specs, or hand-write them.

## Decision

Every public type and runtime behavior derives from the two authoritative contracts, never from a sibling SDK's current behavior and never from server-internal protocol types. Wire `type` consts, field names, and error/close codes match the contracts exactly (`message` not `data`; `auth` not `auth_refresh`). Message models are **hand-written `msgspec.Struct` tagged unions** (not codegen), guarded by a contract-coverage test that asserts every AsyncAPI message type has a typed model. When the contract and any SDK disagree, the contract wins and the divergence is filed upstream — never silently matched. Neither SDK is the behavioral reference: both converge on the contract (the reference-parity fixes show correction flowing from sukko-js → sukko-py through the contract, while sukko-py's contract-faithful features — back-pressure, gap recovery — are scheduled into sukko-js).

## Consequences

- Easier: a single objective arbiter for "correct"; deliberate divergences from `@sukko/sdk` (header-default auth, back-pressure, full recovery) are documentable rather than accidental.
- Harder: hand-written models must track contract versions by hand (the coverage test catches *missing* types, not silent field drift); five contract ambiguities surfaced during build (close-code 4000 overlap, `GATEWAY_MAX_PUBLISH_SIZE` 64KB-vs-1MB, missing list-push-subscriptions endpoint, `last_pos` key format, no `Retry-After` on 429) had to be filed upstream and resolved against server source, not worked around here.
- Coupling: the SDK is coupled to contract text and, for pinned ambiguities, to server behavior (e.g. `last_pos` keyed by the full tenant-prefixed channel, pinned against server source).

## Alternatives rejected

- **Port `@sukko/sdk` behavior** — it lags the contract; parity would inherit its defects (no back-pressure, silent gaps).
- **AsyncAPI→Python codegen** — no mature generator for AsyncAPI 3.0.0; fragile, and covers only part of the surface.
- **OpenAPI codegen (`datamodel-code-generator`)** — covers only the REST schemas, not the WS envelope.
- **pydantic v2 models** — heavier and slower decode on the market-data hot path; kept only as a documented fallback.
- **Expose `internal/shared/protocol`** — server-internal, unstable, explicitly forbidden (§I).
