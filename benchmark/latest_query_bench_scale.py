#!/usr/bin/env python3
"""
Scaled LATEST query benchmark — tests with larger cardinality to stress
the scatter-gather discovery path.

Usage:
  cd build && ./bin/timestar_http_server -c 4
  python3 benchmark/latest_query_bench_scale.py
"""

import argparse
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import requests

MEASUREMENT = "scale.metrics"
BASE_TS = 1_000_000_000_000_000_000
MINUTE_NS = 60_000_000_000

# Scale parameters: 200 hosts × 10 fields = 2000 series
HOST_COUNT = 200
RACK_COUNT = 10
FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature",
]
POINTS_PER_SERIES = 2000  # enough for multiple TSM files per series


@dataclass
class BenchResult:
    name: str
    iterations: int
    client_ms: list = field(default_factory=list)
    server_ms: list = field(default_factory=list)

    def summary(self) -> dict:
        c = self.client_ms
        s = self.server_ms
        return {
            "name": self.name,
            "iterations": self.iterations,
            "client_p50_ms": round(statistics.median(c), 3),
            "client_p99_ms": round(sorted(c)[int(len(c) * 0.99)], 3) if len(c) > 1 else round(c[0], 3),
            "client_min_ms": round(min(c), 3),
            "client_max_ms": round(max(c), 3),
            "client_mean_ms": round(statistics.mean(c), 3),
            "server_p50_ms": round(statistics.median(s), 3),
            "server_p99_ms": round(sorted(s)[int(len(s) * 0.99)], 3) if len(s) > 1 else round(s[0], 3),
            "server_min_ms": round(min(s), 3),
            "server_mean_ms": round(statistics.mean(s), 3),
            "overhead_ms": round(statistics.mean(c) - statistics.mean(s), 3),
        }


def insert_data(base_url: str, threads: int = 4):
    """Insert test data using multiple threads."""
    total_series = HOST_COUNT * len(FIELD_NAMES)
    total_points = HOST_COUNT * len(FIELD_NAMES) * POINTS_PER_SERIES
    print(f"Inserting data: {HOST_COUNT} hosts × {len(FIELD_NAMES)} fields × {POINTS_PER_SERIES} pts")
    print(f"  = {total_series:,} series, {total_points:,} total points")

    session = requests.Session()
    batch_size = 500  # points per HTTP request
    start_time = time.perf_counter()

    def insert_host(host_idx):
        host = f"host-{host_idx:03d}"
        rack = f"rack-{host_idx % RACK_COUNT}"
        dc = f"dc-{host_idx % 2}"
        count = 0

        for pt_start in range(0, POINTS_PER_SERIES, batch_size):
            batch = []
            for i in range(pt_start, min(pt_start + batch_size, POINTS_PER_SERIES)):
                ts = BASE_TS + i * MINUTE_NS
                fields = {}
                for fi, fname in enumerate(FIELD_NAMES):
                    fields[fname] = 50.0 + (host_idx * 0.1) + (i * 0.001) + (fi * 5.0)
                batch.append({
                    "measurement": MEASUREMENT,
                    "tags": {"host": host, "rack": rack, "dc": dc},
                    "fields": fields,
                    "timestamp": ts,
                })
            resp = session.post(f"{base_url}/write", json={"writes": batch}, timeout=60)
            if resp.status_code != 200:
                print(f"  Write failed for {host}: {resp.status_code}")
                return 0
            count += len(batch) * len(FIELD_NAMES)
        return count

    with ThreadPoolExecutor(max_workers=threads) as pool:
        futs = [pool.submit(insert_host, i) for i in range(HOST_COUNT)]
        written = sum(f.result() for f in futs)

    elapsed = time.perf_counter() - start_time
    rate = written / elapsed if elapsed > 0 else 0
    print(f"  Inserted {written:,} points in {elapsed:.1f}s ({rate:,.0f} pts/sec)")
    print("  Waiting 5s for TSM flush ...")
    time.sleep(5)


def run_query(session, base_url: str, payload: dict) -> tuple:
    start = time.perf_counter()
    resp = session.post(f"{base_url}/query", json=payload, timeout=30)
    client_ms = (time.perf_counter() - start) * 1000
    if resp.status_code != 200:
        raise RuntimeError(f"Query failed: {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    server_ms = data.get("statistics", {}).get("execution_time_ms", 0)
    return client_ms, server_ms, data


def bench_query(session, base_url: str, name: str, payload: dict,
                iterations: int = 200, warmup_n: int = 20) -> BenchResult:
    for _ in range(warmup_n):
        run_query(session, base_url, payload)

    result = BenchResult(name=name, iterations=iterations)
    for _ in range(iterations):
        client_ms, server_ms, _ = run_query(session, base_url, payload)
        result.client_ms.append(client_ms)
        result.server_ms.append(server_ms)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8086)
    parser.add_argument("--skip-insert", action="store_true")
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    session = requests.Session()

    try:
        session.get(f"{base_url}/health", timeout=5)
    except requests.ConnectionError:
        print(f"ERROR: Cannot connect to {base_url}")
        sys.exit(1)

    if not args.skip_insert:
        insert_data(base_url)

    end_ts = BASE_TS + POINTS_PER_SERIES * MINUTE_NS
    full_interval = end_ts - BASE_TS
    iters = args.iterations

    # Verify
    _, _, check = run_query(session, base_url, {
        "query": f"latest:{MEASUREMENT}(cpu_usage){{host:host-000,rack:rack-0,dc:dc-0}}",
        "startTime": BASE_TS, "endTime": end_ts,
        "aggregationInterval": full_interval,
    })
    if check.get("status") != "success" or not check.get("series"):
        print("ERROR: No data found. Run without --skip-insert.")
        sys.exit(1)

    series_count = 0
    _, _, all_check = run_query(session, base_url, {
        "query": f"latest:{MEASUREMENT}(cpu_usage)",
        "startTime": BASE_TS, "endTime": end_ts,
        "aggregationInterval": full_interval,
    })
    series_count = all_check.get("statistics", {}).get("series_count", "?")
    print(f"Data verified. {series_count} series for cpu_usage. Running {iters} iterations.\n")

    results = []

    # 1. Single series, fully-qualified tags (direct routing candidate)
    results.append(bench_query(session, base_url,
        "1 series, exact tags, 1 field",
        {"query": f"latest:{MEASUREMENT}(cpu_usage){{host:host-000,rack:rack-0,dc:dc-0}}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # 2. Single series, no interval (sparse index fast path)
    results.append(bench_query(session, base_url,
        "1 series, exact tags, no interval",
        {"query": f"latest:{MEASUREMENT}(cpu_usage){{host:host-000,rack:rack-0,dc:dc-0}}",
         "startTime": BASE_TS, "endTime": end_ts}, iters))

    # 3. Single host, all fields (4 series on same shard)
    results.append(bench_query(session, base_url,
        "1 host all fields (10 series, mixed shards)",
        {"query": f"latest:{MEASUREMENT}(){{host:host-000,rack:rack-0,dc:dc-0}}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # 4. One rack, 1 field (20 hosts × 1 field = 20 series)
    results.append(bench_query(session, base_url,
        "1 rack, 1 field (20 series)",
        {"query": f"latest:{MEASUREMENT}(cpu_usage){{rack:rack-0}}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # 5. One rack, all fields (20 hosts × 10 fields = 200 series)
    results.append(bench_query(session, base_url,
        "1 rack, all fields (200 series)",
        {"query": f"latest:{MEASUREMENT}(){{rack:rack-0}}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # 6. All hosts, 1 field (200 series)
    results.append(bench_query(session, base_url,
        "no scopes, 1 field (200 series)",
        {"query": f"latest:{MEASUREMENT}(cpu_usage)",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # 7. All hosts, all fields (2000 series)
    results.append(bench_query(session, base_url,
        "no scopes, all fields (2000 series)",
        {"query": f"latest:{MEASUREMENT}()",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # 8. AVG baseline for comparison
    results.append(bench_query(session, base_url,
        "AVG: 1 series, exact tags (baseline)",
        {"query": f"avg:{MEASUREMENT}(cpu_usage){{host:host-000,rack:rack-0,dc:dc-0}}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval}, iters))

    # Print
    summaries = [r.summary() for r in results]

    if args.json:
        print(json.dumps(summaries, indent=2))
    else:
        print(f"{'Benchmark':<48} {'Client p50':>11} {'Client p99':>11} {'Server p50':>11} {'Overhead':>9}")
        print("─" * 91)
        for s in summaries:
            print(f"{s['name']:<48} {s['client_p50_ms']:>8.2f} ms {s['client_p99_ms']:>8.2f} ms "
                  f"{s['server_p50_ms']:>8.2f} ms {s['overhead_ms']:>6.2f} ms")

    ts = time.strftime("%Y%m%d_%H%M%S")
    outpath = f"benchmark/latest_scale_{ts}.json"
    with open(outpath, "w") as f:
        json.dump(summaries, f, indent=2)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
