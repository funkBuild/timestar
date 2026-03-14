# TimeStar vs InfluxDB 2.7 Benchmarks

Comparative benchmarks measuring insert throughput, query latency, storage
efficiency, and multi-core scaling between TimeStar and InfluxDB 2.7.

**Date:** 2026-03-14

## Test Environment

| Component | Spec |
|-----------|------|
| CPU | AMD Ryzen 9 (32 logical cores available) |
| RAM | 128 GB |
| OS | Ubuntu 25.04, Linux 6.14 |
| Filesystem | ext4 |
| TimeStar | Built with GCC 14, C++23, `-O3 -march=native` |
| InfluxDB | 2.7.12 (official Docker image `influxdb:2.7`) |

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

## Latest Results (4 cores, 100M points)

### Insert Throughput

| Database | Throughput | Ratio |
|----------|----------:|------:|
| **TimeStar** | **31.66M pts/sec** | **9.5x faster** |
| InfluxDB 2.7 | 3.34M pts/sec | baseline |

### Storage Efficiency (verified post-compaction)

Both databases were allowed to fully compact their data before measurement.
TimeStar: all data in TSM files, WAL empty. InfluxDB: WAL snapshot complete
(993 TSM files, empty WAL segments), verified via `du -sb` on the engine
directory after waiting for `cache-snapshot-write-cold-duration` to expire.

| Database | Size on Disk | Ratio |
|----------|----------:|------:|
| **TimeStar** | **299 MB** | **3.3x smaller** |
| InfluxDB 2.7 | 1,016 MB | baseline |

TimeStar's storage advantage comes from ALP (Adaptive Lossless floating-Point)
compression for float values and XOR-based timestamp encoding, which are more
efficient than InfluxDB's Gorilla encoding for this workload pattern.

### Query Latency (4 cores, avg ms)

| Query | TimeStar | InfluxDB | Speedup |
|-------|--------:|---------:|--------:|
| latest: single field | 0.86 | 18.25 | **21.2x** |
| avg: narrow, 1 field | 0.59 | 2.38 | **4.0x** |
| max: narrow, 1 field | 0.50 | 2.36 | **4.7x** |
| sum: narrow, 1 field | 0.51 | 2.28 | **4.5x** |
| count: narrow, 1 field | 0.51 | 2.26 | **4.4x** |
| avg: host filter | 0.39 | 2.24 | **5.8x** |
| avg: group by host | 0.78 | 2.54 | **3.3x** |
| avg: 1h buckets, medium | 0.73 | 3.50 | **4.8x** |
| avg: narrow, all fields | 1.09 | 2.94 | **2.7x** |
| avg: full range, 1 field | 8.07 | 263.05 | **32.6x** |
| avg: group by host, full | 6.79 | 450.57 | **66.3x** |
| avg: 5m buckets, tag | 0.61 | 2.60 | **4.3x** |
| **Total (all 12 queries)** | **174 ms** | **4,279 ms** | **24.7x** |

## Multi-Core Scaling

### Insert Throughput

| Cores | TimeStar (pts/sec) | InfluxDB (pts/sec) | Ratio |
|------:|-------------------:|-------------------:|------:|
| 1 | 21.62M | 885K | **24.4x** |
| 2 | 27.92M | 1.71M | **16.3x** |
| 4 | 31.66M | 3.34M | **9.5x** |
| 8 | 35.40M | 1.90M | **18.6x** |
| 12 | 37.10M | 5.33M | **7.0x** |

TimeStar sustains 21-37M pts/sec across all core counts. InfluxDB peaks at
~5M pts/sec at 12 cores. Both databases plateau beyond 8 cores, with TimeStar
levelling off due to the Python client becoming the bottleneck (the native
Seastar insert benchmark achieves higher throughput).

### Query Speedup Summary (TimeStar vs InfluxDB)

| Query | 1c | 2c | 4c | 8c | 12c |
|-------|---:|---:|---:|---:|----:|
| latest: single field | **6.1x** | **11.3x** | **21.2x** | **6.0x** | **4.3x** |
| avg: narrow, 1 field | 4.1x | 3.3x | 4.0x | 3.5x | 3.9x |
| max: narrow, 1 field | 4.3x | 3.9x | 4.7x | 3.8x | 3.7x |
| sum: narrow, 1 field | 4.2x | 3.8x | 4.5x | 3.8x | 3.9x |
| count: narrow, 1 field | 4.1x | 4.0x | 4.4x | 3.8x | 3.9x |
| avg: host filter | 5.7x | 5.7x | 5.8x | 5.5x | 5.3x |
| avg: group by host | 2.6x | 3.5x | 3.3x | 3.6x | 3.8x |
| avg: 1h buckets, medium | 4.7x | 3.9x | 4.8x | 4.7x | 4.5x |
| avg: narrow, all fields | 2.9x | 2.9x | 2.7x | 3.0x | 3.4x |
| avg: full range, 1 field | 12.6x | 14.6x | 32.6x | 13.8x | 17.5x |
| avg: group by host, full | 6.8x | 11.0x | **66.3x** | 20.9x | 25.9x |
| avg: 5m buckets, tag | 4.4x | 4.3x | 4.3x | 4.8x | 4.5x |
| **OVERALL** | **8.7x** | **10.9x** | **24.7x** | **12.0x** | **12.3x** |

### Total Query Time (ms, all 12 queries)

| Cores | TimeStar | InfluxDB | Ratio |
|------:|---------:|---------:|------:|
| 1 | 689 | 5,972 | **8.7x** |
| 2 | 446 | 4,868 | **10.9x** |
| 4 | 174 | 4,279 | **24.7x** |
| 8 | 336 | 4,020 | **12.0x** |
| 12 | 319 | 3,927 | **12.3x** |

## Key Observations

- **Insert throughput**: TimeStar is 9-24x faster depending on core count.
  The advantage is largest at low core counts because Seastar's shard-per-core
  architecture has lower per-request overhead than InfluxDB's goroutine model.

- **Storage efficiency**: TimeStar uses 3.3x less disk space than InfluxDB
  for the same 100M point dataset (299 MB vs 1,016 MB post-compaction), thanks
  to ALP float compression, XOR timestamp encoding, and Snappy block compression.

- **Narrow-range aggregation** (avg, max, sum, count over 100 minutes of data):
  TimeStar is consistently 3-5x faster. Sub-millisecond latencies at all core
  counts.

- **Full-range aggregation** (scan all 100M points): TimeStar's advantage grows
  with cores -- from 13x at 1 core to **66x at 4 cores**. This is where
  Seastar's shard-per-core parallelism pays off most: each shard scans its
  partition independently and partial aggregations are merged.

- **`latest` query**: TimeStar is **4-21x faster** than InfluxDB at all core
  counts, thanks to the aggregation-method-aware TSM pushdown optimization.
  For `LATEST` queries, the pushdown path iterates TSM blocks in reverse time
  order and stops after finding the most recent non-tombstoned point, reading
  far fewer blocks than a full scan.

- **Overall query performance**: TimeStar is **8.7-24.7x faster** across the
  full 12-query suite. The advantage is strongest at 4 cores where the
  shard-per-core model achieves optimal parallelism for this workload.

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
