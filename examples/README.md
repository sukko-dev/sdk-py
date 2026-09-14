# Examples

Runnable, single-file examples for the `sukko` Python SDK, aimed at backend / data-pipeline use.
Each script is self-contained — run it directly:

```bash
python examples/live_feed.py
```

All scripts read their configuration from the environment (with sensible local defaults):

| Variable       | Default                  | Purpose                                  |
| -------------- | ------------------------ | ---------------------------------------- |
| `SUKKO_URL`    | `ws://localhost:8080/ws` | Gateway WebSocket URL                    |
| `SUKKO_TOKEN`  | *(none)*                 | JWT for authentication                   |
| `SUKKO_CHANNEL`| `acme.trades`            | Channel to subscribe/publish             |
| `SUKKO_SINK`   | `feed.jsonl`             | Output file (`pipeline_to_sink.py` only) |

```bash
SUKKO_URL=wss://gateway.example.com/ws SUKKO_TOKEN=<jwt> python examples/live_feed.py
```

| Script                  | Shows                                                                    |
| ----------------------- | ----------------------------------------------------------------------- |
| `live_feed.py`          | Long-running async consumer with graceful SIGINT/SIGTERM shutdown       |
| `sync_script.py`        | Blocking client for plain scripts and Jupyter notebooks                 |
| `recovery_and_gaps.py`  | Handling live, recovered, gap, possible-gap, and overflow delivery items|
| `rest_publish.py`       | Publishing over REST with no live WebSocket (all editions)              |
| `pipeline_to_sink.py`   | Consuming a feed into a JSON-lines file sink                            |
