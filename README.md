# sukko

Asyncio-first Python client SDK for the [Sukko](https://github.com/sukko-dev/sukko) real-time
platform — real-time market-data / event subscription for backend services, data pipelines, and
notebooks.

- **Async-iterator delivery** with capability-gated back-pressure — no unbounded buffering, no
  silent drops.
- **Full gap recovery** — reconnect-with-replay, advisory `gap` → live `replay`, and `history`.
- **Automatic single-flight auth** — proactive + reactive token refresh, API-key → JWT escalation.
- **WebSocket + SSE** transports, awaitable **REST publish**, **push** subscription management.
- **Jupyter-safe sync wrapper** (`SyncSukkoClient`) — no `asyncio` boilerplate.
- Fully typed (`py.typed`), built to the AsyncAPI v1.4.0 + gateway OpenAPI contracts.

```bash
pip install sukko
```

Requires Python 3.11+.

## Quickstart (async)

```python
import asyncio
from sukko import SukkoClient

async def main():
    async with SukkoClient("wss://gateway.example.com/ws", token="<jwt>") as client:
        await client.subscribe(["acme.trades"])
        async for msg in client.messages():
            print(msg.channel, msg.data)          # live + recovered messages arrive here

asyncio.run(main())
```

## Quickstart (sync — scripts & notebooks)

```python
from sukko import SyncSukkoClient

with SyncSukkoClient("wss://gateway.example.com/ws", token="<jwt>") as client:
    client.subscribe(["acme.trades"])
    for msg in client.stream():                    # blocking; Jupyter-safe (own background loop)
        print(msg.channel, msg.data)
```

## Publishing

```python
await client.publish("acme.trades", {"price": 100})              # over the WS connection (fire-and-forget)
mid = await client.rest_publish("acme.trades", {"price": 100})   # awaitable REST — no WS needed (all editions)
```

`rest_publish` returns the server-assigned stable message identity `mid` — the same `mid`
subscribers see on the delivered message (`None` on multi-topic fan-out). Delivered messages carry
`msg.mid` on every copy (live, gap-replay, and history), so it can be used to deduplicate — e.g.
dropping the overlap between a reconnect replay and messages already received.

## Auth

Credentials are sent via **request headers by default** (`Authorization: Bearer` / `X-API-Key`);
pass `auth_via="query"` to opt into query-param auth. Long-lived tokens are refreshed automatically
(supply a `get_token` async callback). An API-key connection can **escalate** to a JWT mid-session:

```python
client = SukkoClient(url, api_key="<key>", get_token=fetch_fresh_jwt)
...
await client.escalate("<jwt>")   # gains JWT permissions; re-subscribes newly-permitted channels
```

## Channels

Channels are `"{tenant}.{suffix}"` (the tenant is the segment before the first dot; the suffix is an
opaque dotted remainder). Helpers:

```python
from sukko import build_channel, parse_channel
build_channel("acme", "trades.btc")   # -> "acme.trades.btc"
parse_channel("acme.trades.btc")      # -> ParsedChannel(tenant="acme", suffix="trades.btc")
```

## Recovery

On a Kafka backend the SDK recovers missed messages automatically: it reconnects with exponential
backoff + jitter and replays from the last position, and turns advisory `gap` notices into live
`replay`s — all surfaced through the same `messages()` stream (recovered records carry `history` /
`pos`). The delivery guarantee is **at-least-once within the replay window, best-effort beyond**.
On the Direct backend (no positions), a disconnect surfaces a `PossibleGap` signal per channel so
data loss is never silent.

You can also fetch history explicitly (all editions, when the server has history enabled):

```python
await client.history("acme.trades", limit=50)   # arrives as history-flagged messages
```

## Push subscription management (Web Push = Pro; mobile FCM/APNs = Enterprise)

Register a mobile/browser device for push on a user's behalf (this SDK does not *receive* push):

```python
device_id = await client.push.subscribe(platform="android", channels=["acme.alerts"], token="<fcm>")
await client.push.unsubscribe(device_id)
key = await client.push.get_vapid_key()
```

## Errors

Every failure is a typed subclass of `sukko.SukkoError` — e.g. `NotConnectedError`,
`EditionRequiredError`, `RateLimitError`, `RecoveryInterruptedError`, `PublishError`. Credentials are
never present in an error message, `repr`, or log record.

## Engineering

- [Engineering principles](docs/engineering-principles.md) — the rules this SDK
  is built and reviewed against. Code comments cite them by section: `# per §VI`
  refers to section VI of that document (or of the
  [platform principles](https://github.com/sukko-dev/sukko/blob/main/docs/engineering-principles.md)
  it adapts — each heading carries the cross-reference).
- [Architecture decision records](docs/adr/) — durable decisions with context
  and rejected alternatives; comments cite them as `ADR-NNNN`.

## License

MIT — see [LICENSE](LICENSE).
