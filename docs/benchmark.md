## Benchmark (indicative: one machine, client-side, HTTP over loopback)

- Measured 2026-11-10 05:43 UTC against http://127.0.0.1:8000 (run `056fff85`, `python -m scripts.bench`).
- Machine: Windows-11-10.0.26200-SP0, 12 logical CPUs, Python 3.12.16 for the client.
- Latency: sequential deliveries of new events, after a warm-up. Throughput: concurrent clients; end to end = until no event of the run is pending.

| Ingestion latency | Samples | p50 | p95 | p99 | max | NFR-2 target (p95) |
|---|---|---|---|---|---|---|
| Redis up | 500 | 8.6 ms | 12.9 ms | 17.0 ms | 26.6 ms | < 100 ms: met |
| Redis down | 500 | 8.2 ms | 10.9 ms | 15.9 ms | 28.0 ms | < 150 ms: met |

| Throughput (NFR-3) | Events | Clients | Accepted/s | End to end/s |
|---|---|---|---|---|
| New events | 2000 | 8 | 187 | 139 |
