# Changelog

All notable changes to `sukko` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2] - 2026-10-23

### Fixed
- WebSocket transport now sends client frames as **TEXT**, not binary. `encode_client`
  returns bytes and `websockets` sends bytes as a binary frame, which the gateway ignores —
  so `subscribe`, WebSocket `publish`, and auth refresh silently had no effect against a real
  gateway (REST publish was unaffected). Encoded frames are decoded to a `str` so they are
  sent as TEXT frames.

## [0.1.1] - 2026-10-21

### Added
- SSE reconnect-recovery control frames surfaced to clients (ADR-0006): `Gap`
  (the wire gap event carrying the recovered range), `PossibleGap` (an
  SDK-internal signal that a reconnect may have missed messages on a channel),
  and `RecoveryInterruptedError`. These let callers detect and handle
  message-loss windows across an SSE reconnect.

## [0.1.0] - 2026-10-04

### Added
- Stable message identity `mid` on delivered envelopes: `Message.mid`,
  `ReplayMessage.mid`, and `PublishAck.mid` (optional; `None` on servers that
  predate the field). Identical on every copy of the same message — live,
  gap-replay, and history — for client-side deduplication and idempotent
  processing. An identity, never a replay cursor.
- `rest_publish()` (and `SyncSukkoClient.rest_publish()`) now returns the
  server-assigned `mid` (`None` on multi-topic fan-out publishes).
- Initial asyncio-first Python client SDK for the Sukko real-time platform,
  built to the AsyncAPI v1.4.0 + gateway OpenAPI contracts.

### Changed
- `on_publish_ack` listeners now receive `(channel, mid)` instead of
  `(channel,)`.
- Edition documentation updated to the platform's edition remap: REST publish
  and message history are available in all editions (history still requires the
  server `WS_HISTORY_ENABLED` toggle); Web Push subscription management is Pro;
  mobile FCM/APNs push is Enterprise; SSE remains Pro.
