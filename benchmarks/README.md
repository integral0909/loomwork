# Benchmarks

`bench.py` measures the costs that matter when operating Loomwork:

| Benchmark    | What it measures                                                         |
| ------------ | ------------------------------------------------------------------------ |
| `throughput` | Workflows per second for a two-step workflow, started and awaited in bulk |
| `fanout`     | Step attempts per second at step concurrency 1, 4, 16 and 64              |
| `replay`     | Time to replay a history from scratch, by history length                  |
| `tick`       | Tick latency with and without the worker's replay cache                   |

```bash
uv run python -m benchmarks.bench                                  # memory store
uv run python -m benchmarks.bench --store sqlite
uv run python -m benchmarks.bench --store postgres --dsn "$DSN"    # uses and truncates the database
uv run python -m benchmarks.bench --only replay --json replay.json
```

## Reading the results

- **Replay cost is linear in history length.** With the replay cache a worker only feeds
  new events to a live instance, so the per-tick cost stays flat; without it every tick
  replays the whole history. Compare `tick cache=on` and `cache=off` on a long chain.
- **Durable stores are bound by statements per operation.** A two-step workflow issues
  about 42 SQL statements over its lifetime (claims, commits and results). The worker
  dispatchers poll once per poll interval per kind of work, not per concurrency slot,
  so idle workers do not compete with busy ones for connections.
- **Numbers are only comparable on the same machine**, with nothing else running.

## Sample (Apple M-series laptop, local PostgreSQL 16, nothing else running)

```
throughput  workflows=200     ~90-100 workflows/s  (postgres)
throughput  workflows=500     ~600 workflows/s     (memory)
replay      steps=1000        ~33 ms for 3003 events (~11 us/event)
tick        steps=100         ~1.3 ms/tick with cache, ~3.1 ms without (memory)
```
