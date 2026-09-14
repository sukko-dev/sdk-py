# Changelog

All notable changes to `sukko` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
