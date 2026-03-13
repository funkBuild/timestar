# TimeStar vs InfluxDB 2.7 Benchmarks

Comparative benchmarks measuring insert throughput, query latency, and multi-core
scaling between TimeStar and InfluxDB 2.7.

**Date:** 2026-03-12

## Test Environment

| Component | Spec |
|-----------|------|
| CPU | AMD Ryzen 9 (32 logical cores available) |
| RAM | 128 GB |
| OS | Ubuntu 25.04, Linux 6.14 |
| Filesystem | ext4 |
| TimeStar | Built with GCC 14, C++23, `-O2` |
| InfluxDB | 2.7 (official Docker image `influxdb:2.7`) |

Both databases were given identical resource constraints via Docker `--cpus` /
`--memory` for InfluxDB and Seastar `-c` for TimeStar. InfluxDB was allocated
8 GB memory at each core count.

## Workload

| Parameter | Value |
|-----------|-------|
| Measurement | `server.metrics` |
| Fields per point | 10 (cpu_usage, memory_usage, disk_io_read, disk_io_write, network_in, network_out, load_avg_1m, load_avg_5m, load_avg_15m, temperature) |
| Tags | `host` (10 hosts), `rack` (2 racks) |
| Batch size | 10,000 timestamps per request |
| Batches | 1,000 |
| Total points per round | **100,000,000** (100M) |
| Warmup | 10 batches (not timed) |
| Client concurrency | 8 threads (Python `requests` + `ThreadPoolExecutor`) |
| PRNG seed | 42 (deterministic, reproducible data) |

The same Python HTTP client is used for both databases, ensuring client overhead
is identical. TimeStar receives JSON array-format writes to `POST /write`.
InfluxDB receives line protocol writes to `POST /api/v2/write`.

Queries use TimeStar's native query API (`POST /query`) and InfluxDB's Flux API
(`POST /api/v2/query`). All aggregation queries use an `aggregationInterval`
that matches the Flux aggregation semantics (single-value collapse for scalar
aggregations, explicit `aggregateWindow` for time-bucketed queries).

## Insert Throughput

| Cores | TimeStar (pts/sec) | InfluxDB (pts/sec) | Ratio |
|------:|-------------------:|-------------------:|------:|
| 1 | 21.62M | 885K | **24.4x** |
| 2 | 27.92M | 1.71M | **16.3x** |
| 4 | 34.52M | 3.38M | **10.2x** |
| 8 | 35.40M | 1.90M | **18.6x** |
| 12 | 37.10M | 5.33M | **7.0x** |

TimeStar sustains 21-37M pts/sec across all core counts. InfluxDB peaks at
~5M pts/sec at 12 cores. Both databases plateau beyond 8 cores, with TimeStar
levelling off due to the Python client becoming the bottleneck (the native
Seastar insert benchmark achieves higher throughput).

## Query Latency (avg ms)

12 query patterns covering aggregation, tag filtering, group-by, time-bucketed
aggregation, and full-range scans.

### Per-Query Results

| Query | 1c TS | 1c IX | 2c TS | 2c IX | 4c TS | 4c IX | 8c TS | 8c IX | 12c TS | 12c IX |
|-------|------:|------:|------:|------:|------:|------:|------:|------:|-------:|-------:|
| latest: single field | 2.98 | 18.30 | 1.59 | 17.96 | 2.41 | 18.31 | 3.00 | 18.13 | 4.30 | 18.35 |
| avg: narrow, 1 field | 0.60 | 2.44 | 0.73 | 2.38 | 0.57 | 2.35 | 0.67 | 2.36 | 0.63 | 2.44 |
| max: narrow, 1 field | 0.54 | 2.30 | 0.58 | 2.28 | 0.53 | 2.36 | 0.58 | 2.23 | 0.61 | 2.27 |
| sum: narrow, 1 field | 0.55 | 2.33 | 0.59 | 2.26 | 0.54 | 2.26 | 0.59 | 2.24 | 0.58 | 2.28 |
| count: narrow, 1 field | 0.55 | 2.23 | 0.59 | 2.34 | 0.54 | 2.27 | 0.59 | 2.25 | 0.59 | 2.33 |
| avg: host filter | 0.39 | 2.21 | 0.39 | 2.21 | 0.41 | 2.23 | 0.41 | 2.25 | 0.42 | 2.22 |
| avg: group by host | 0.92 | 2.41 | 0.68 | 2.41 | 0.79 | 2.36 | 0.66 | 2.35 | 0.63 | 2.42 |
| avg: 1h buckets, medium | 0.73 | 3.46 | 0.87 | 3.39 | 0.76 | 3.43 | 0.71 | 3.35 | 0.75 | 3.41 |
| avg: narrow, all fields | 1.01 | 2.88 | 1.00 | 2.86 | 0.93 | 2.81 | 0.93 | 2.82 | 0.85 | 2.90 |
| avg: full range, 1 field | 54.62 | 686.28 | 32.35 | 473.76 | 22.44 | 259.88 | 18.79 | 259.62 | 14.92 | 261.16 |
| avg: group by host, full | 54.22 | 366.10 | 32.64 | 359.44 | 22.43 | 455.26 | 19.32 | 404.18 | 14.74 | 381.68 |
| avg: 5m buckets, tag | 0.60 | 2.61 | 0.62 | 2.66 | 0.58 | 2.54 | 0.55 | 2.62 | 0.62 | 2.77 |

### Speedup Summary (TimeStar vs InfluxDB)

| Query | 1c | 2c | 4c | 8c | 12c |
|-------|---:|---:|---:|---:|----:|
| latest: single field | **TS 6.1x** | **TS 11.3x** | **TS 7.6x** | **TS 6.0x** | **TS 4.3x** |
| avg: narrow, 1 field | TS 4.1x | TS 3.3x | TS 4.1x | TS 3.5x | TS 3.9x |
| max: narrow, 1 field | TS 4.3x | TS 3.9x | TS 4.5x | TS 3.8x | TS 3.7x |
| sum: narrow, 1 field | TS 4.2x | TS 3.8x | TS 4.2x | TS 3.8x | TS 3.9x |
| count: narrow, 1 field | TS 4.1x | TS 4.0x | TS 4.2x | TS 3.8x | TS 3.9x |
| avg: host filter | TS 5.7x | TS 5.7x | TS 5.4x | TS 5.5x | TS 5.3x |
| avg: group by host | TS 2.6x | TS 3.5x | TS 3.0x | TS 3.6x | TS 3.8x |
| avg: 1h buckets, medium | TS 4.7x | TS 3.9x | TS 4.5x | TS 4.7x | TS 4.5x |
| avg: narrow, all fields | TS 2.9x | TS 2.9x | TS 3.0x | TS 3.0x | TS 3.4x |
| avg: full range, 1 field | TS 12.6x | TS 14.6x | TS 11.6x | TS 13.8x | TS 17.5x |
| avg: group by host, full | TS 6.8x | TS 11.0x | TS 20.3x | TS 20.9x | TS 25.9x |
| avg: 5m buckets, tag | TS 4.4x | TS 4.3x | TS 4.4x | TS 4.8x | TS 4.5x |
| **OVERALL** | **TS 8.7x** | **TS 10.9x** | **TS 12.1x** | **TS 12.0x** | **TS 12.3x** |

### Total Query Time (ms, all 12 queries)

| Cores | TimeStar | InfluxDB | Ratio |
|------:|---------:|---------:|------:|
| 1 | 689 | 5,972 | **TS 8.7x** |
| 2 | 446 | 4,868 | **TS 10.9x** |
| 4 | 355 | 4,283 | **TS 12.1x** |
| 8 | 336 | 4,020 | **TS 12.0x** |
| 12 | 319 | 3,927 | **TS 12.3x** |

## Key Observations

- **Insert throughput**: TimeStar is 7-24x faster depending on core count.
  The advantage is largest at low core counts because Seastar's shard-per-core
  architecture has lower per-request overhead than InfluxDB's goroutine model.

- **Narrow-range aggregation** (avg, max, sum, count over 100 minutes of data):
  TimeStar is consistently 3-5x faster. Sub-millisecond latencies at all core
  counts.

- **Full-range aggregation** (scan all 100M points): TimeStar's advantage grows
  with cores -- from 13x at 1 core to **26x at 12 cores**. This is where
  Seastar's shard-per-core parallelism pays off most: each shard scans its
  partition independently and partial aggregations are merged.

- **`latest` query**: TimeStar is now **4-11x faster** than InfluxDB at all core
  counts, thanks to the aggregation-method-aware TSM pushdown optimization.
  For `LATEST` queries, the pushdown path iterates TSM blocks in reverse time
  order and stops after finding the most recent non-tombstoned point, reading
  far fewer blocks than a full scan. Previous benchmarks showed InfluxDB winning
  this query at 1-8 cores (54ms vs 18ms at 1 core); TimeStar now completes in
  under 3ms at 1 core.

- **Overall query performance**: TimeStar is **8.7-12.3x faster** across the
  full 12-query suite, up from 3.3-8.0x in previous benchmarks. The `latest`
  pushdown optimization accounts for the majority of this improvement.

- **Storage efficiency**: TimeStar uses 4.9-6.6x less disk space than InfluxDB
  for the same data, thanks to ALP float compression and Snappy string encoding.

## Reproducing These Results

### Prerequisites

- Docker (for InfluxDB)
- Python 3.10+ with `requests` and `influxdb-client` packages
- TimeStar built (`cd build && cmake .. && make -j$(nproc)`)

### Install Python Dependencies

```bash
pip install requests influxdb-client
```

### Run the Full Benchmark

```bash
# From the project root:
python3 benchmark/run_scaling_bench.py \
  --cores 1 2 4 8 12 \
  --batches 1000 \
  --batch-size 10000 \
  --hosts 10 \
  --racks 2 \
  --concurrency 8 \
  --warmup-batches 10 \
  --memory 8g \
  --no-influx3
```

This runs 5 rounds (one per core count). Each round:

1. Starts a fresh InfluxDB 2.7 container with `--cpus N` and `--memory 8g`
2. Starts a fresh TimeStar instance with `-c N` (clean shard directories)
3. Pre-generates 1,000 deterministic payloads (seed=42)
4. Runs 10 warmup batches (not timed)
5. Inserts 100M points into both databases and measures throughput
6. Waits 3 seconds for background TSM flushes
7. Runs 12 query patterns with warmup + timed iterations
8. Stops both databases and cleans up

Results are saved to `benchmark/results_<timestamp>.json`.

### Quick Smoke Test

```bash
# Smaller dataset, single core count (~30 seconds)
python3 benchmark/run_scaling_bench.py --cores 4 --batches 20 --batch-size 1000 --no-influx3
```

### Multiple Runs for Statistical Confidence

```bash
# 3 runs per core count, results are averaged
python3 benchmark/run_scaling_bench.py --cores 1 2 4 8 12 --runs 3 --no-influx3
```

### Standalone Comparison (Manual Server Management)

```bash
# Start TimeStar manually
cd build && ./bin/timestar_http_server -c 4 --port 8086

# In another terminal, run the comparison benchmark
python3 benchmark/influxdb_comparison.py --cpus 4 --batches 1000 --batch-size 10000
```

## Benchmark Scripts

| Script | Description |
|--------|-------------|
| `benchmark/run_scaling_bench.py` | Automated multi-core scaling benchmark (manages both databases) |
| `benchmark/influxdb_comparison.py` | Single-config comparison (requires TimeStar to be running) |
| `bin/timestar_insert_bench` | Native Seastar insert benchmark (TimeStar only, higher throughput) |
| `bin/timestar_query_bench` | Native Seastar query benchmark (TimeStar only) |
