# ADR-0004: Vendor a checksum-pinned copy of the contract (and parity corpus) for hermetic CI

**Status**: Accepted
**Date**: 2026-09-22
**Refines**: ADR-0001 (derive from the authoritative contracts) — this fixes the *mechanism*, not the principle

## Context

ADR-0001 already puts this SDK on the correct footing: derive every type and behavior from the
authoritative AsyncAPI/OpenAPI contracts, never from a sibling SDK. But the *mechanism* differs
from the other SDKs. The contract-coverage test reads the contract from a live sibling checkout
via `SUKKO_ASYNCAPI_PATH` and skips-with-reason when that path is absent. So on a bare CI
checkout the strongest conformance check does not run, and the SDK is not pinned to a specific
contract *version* the way sukko-go (ADR-0003) and sukko-js (ADR-0002) are — both vendor a
checksum-pinned copy of the contract into their own test data. The platform has now also made the
behavioral-parity vector corpus a contract artifact housed with the AsyncAPI (platform ADR-0023),
to be vendored the same way.

## Decision

This SDK vendors a checksum-pinned copy of the authoritative contract documents (the client-ws
AsyncAPI at its pinned version, e.g. v1.4.1) and the parity-vector corpus into its own test data,
refreshed by a documented copy-record-verify procedure, and runs the contract-coverage and
parity-vector tests against the vendored copies — hermetically, with no cross-repo checkout. The
`SUKKO_ASYNCAPI_PATH` override may remain for local cross-repo development, but CI depends only on
the vendored, checksum-verified copies. Neither the AsyncAPI copy nor the corpus is derived from a
sibling SDK; both are contract artifacts from the platform repo. This does not change ADR-0001's
principle — it makes the verification match it on a bare checkout, and aligns the mechanism with
sukko-go and sukko-js.

## Consequences

- **Easier**: the coverage + parity tests run on every CI checkout (no skip); a contract version
  bump is a deliberate, reviewable re-pin; the three SDKs share one verification mechanism.
- **Harder**: the vendored copies must be re-pinned when the platform bumps a contract version
  (the copy-record-verify procedure), rather than tracking a live path.
- **Coupling**: this SDK is pinned to a specific contract version, not to whatever the sibling
  checkout currently holds.

## Alternatives rejected

- **Keep reading the live `SUKKO_ASYNCAPI_PATH` only** — the strongest check silently skips on
  bare CI, and the SDK is not pinned to a contract version.
- **Vendor from sukko-js or sukko-go** — forbidden by ADR-0001; the copies come from the platform
  contract, which all SDKs vendor independently.
