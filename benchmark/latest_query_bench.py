#!/usr/bin/env python3
"""
Focused benchmark for LATEST query latency.

Measures end-to-end HTTP round-trip and server-side execution time for
LATEST queries under different scenarios to identify optimization targets.

Usage:
  # Start TimeStar first:
  #   cd build && ./bin/timestar_http_server -c 4
  # Then:
  #   python3 benchmark/latest_query_bench.py
  #   python3 benchmark/latest_query_bench.py --skip-insert   # reuse existing data
"""

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

# ── Schema ─────────────────────────────────────────────────────────────

MEASUREMENT = "bench.latest"
BASE_TS = 1_000_000_000_000_000_000  # ~2001-09-09 in nanos
MINUTE_NS = 60_000_000_000

HOST_COUNT = 20
RACK_COUNT = 4
FIELD_NAMES = ["cpu_usage", "memory_usage", "disk_io", "network_in"]
POINTS_PER_SERIES = 500  # enough to create multiple TSM blocks


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


def insert_data(base_url: str):
    """Insert deterministic test data."""
    print(f"Inserting data: {HOST_COUNT} hosts × {len(FIELD_NAMES)} fields × {POINTS_PER_SERIES} points ...")

    total_points = 0
    batch_size = 200

    for host_idx in range(HOST_COUNT):
        host = f"host-{host_idx:02d}"
        rack = f"rack-{host_idx % RACK_COUNT}"

        for field_start in range(0, POINTS_PER_SERIES, batch_size):
            batch = []
            for i in range(field_start, min(field_start + batch_size, POINTS_PER_SERIES)):
                ts = BASE_TS + i * MINUTE_NS
                fields = {}
                for fi, fname in enumerate(FIELD_NAMES):
                    fields[fname] = 50.0 + (host_idx * 1.1) + (i * 0.01) + (fi * 5.0)
                batch.append({
                    "measurement": MEASUREMENT,
                    "tags": {"host": host, "rack": rack},
                    "fields": fields,
                    "timestamp": ts,
                })

            resp = requests.post(f"{base_url}/write", json={"writes": batch}, timeout=30)
            if resp.status_code != 200:
                print(f"  Write failed: {resp.status_code} {resp.text[:200]}")
                sys.exit(1)
            total_points += len(batch) * len(FIELD_NAMES)

    print(f"  Inserted {total_points:,} points ({HOST_COUNT * len(FIELD_NAMES)} series)")

    # Wait for memstore rollover to TSM
    print("  Waiting 3s for TSM flush ...")
    time.sleep(3)


def run_query(base_url: str, payload: dict) -> tuple:
    """Run a single query, return (client_ms, server_ms, response_json)."""
    start = time.perf_counter()
    resp = requests.post(f"{base_url}/query", json=payload, timeout=30)
    client_ms = (time.perf_counter() - start) * 1000

    if resp.status_code != 200:
        raise RuntimeError(f"Query failed: {resp.status_code} {resp.text[:200]}")

    data = resp.json()
    server_ms = data.get("statistics", {}).get("execution_time_ms", 0)
    return client_ms, server_ms, data


def warmup(base_url: str, payload: dict, n: int = 5):
    """Warm up caches."""
    for _ in range(n):
        run_query(base_url, payload)


def bench_query(base_url: str, name: str, payload: dict,
                iterations: int = 100, warmup_n: int = 10) -> BenchResult:
    """Benchmark a query with warmup."""
    warmup(base_url, payload, warmup_n)

    result = BenchResult(name=name, iterations=iterations)
    for _ in range(iterations):
        client_ms, server_ms, _ = run_query(base_url, payload)
        result.client_ms.append(client_ms)
        result.server_ms.append(server_ms)

    return result


def main():
    parser = argparse.ArgumentParser(description="LATEST query latency benchmark")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8086)
    parser.add_argument("--skip-insert", action="store_true",
                        help="Skip data insertion (reuse existing data)")
    parser.add_argument("--iterations", type=int, default=200,
                        help="Query iterations per benchmark")
    parser.add_argument("--json", action="store_true",
                        help="Output results as JSON")
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"

    # Check server is up
    try:
        requests.get(f"{base_url}/health", timeout=5)
    except requests.ConnectionError:
        print(f"ERROR: Cannot connect to {base_url}. Start TimeStar first.")
        sys.exit(1)

    if not args.skip_insert:
        insert_data(base_url)

    end_ts = BASE_TS + POINTS_PER_SERIES * MINUTE_NS
    full_interval = end_ts - BASE_TS
    iters = args.iterations

    # Verify data exists
    _, _, check = run_query(base_url, {
        "query": f"latest:{MEASUREMENT}(cpu_usage){{host:host-00}}",
        "startTime": BASE_TS, "endTime": end_ts,
        "aggregationInterval": full_interval,
    })
    if check.get("status") != "success" or not check.get("series"):
        print("ERROR: No data found. Run without --skip-insert first.")
        sys.exit(1)
    print(f"Data verified. Running {iters} iterations per benchmark.\n")

    results = []

    # ── Benchmark 1: Single series, exact tags, single field ──────────
    # This is the ideal case for direct routing optimization.
    # Should be routable to exactly 1 shard.
    results.append(bench_query(base_url,
        "LATEST: 1 series, exact tags, 1 field",
        {
            "query": f"latest:{MEASUREMENT}(cpu_usage){{host:host-00,rack:rack-0}}",
            "startTime": BASE_TS, "endTime": end_ts,
            "aggregationInterval": full_interval,
        }, iters))

    # ── Benchmark 2: Single series, exact tags, all fields ────────────
    results.append(bench_query(base_url,
        "LATEST: 1 series, exact tags, all fields",
        {
            "query": f"latest:{MEASUREMENT}(){{host:host-00,rack:rack-0}}",
            "startTime": BASE_TS, "endTime": end_ts,
            "aggregationInterval": full_interval,
        }, iters))

    # ── Benchmark 3: Wildcard tags (all hosts in rack-0) ──────────────
    # 5 hosts per rack with 20 hosts / 4 racks
    results.append(bench_query(base_url,
        "LATEST: wildcard (rack-0, all hosts), 1 field",
        {
            "query": f"latest:{MEASUREMENT}(cpu_usage){{rack:rack-0}}",
            "startTime": BASE_TS, "endTime": end_ts,
            "aggregationInterval": full_interval,
        }, iters))

    # ── Benchmark 4: No scopes (all 80 series for 1 field) ───────────
    results.append(bench_query(base_url,
        "LATEST: no scopes, 1 field (20 series)",
        {
            "query": f"latest:{MEASUREMENT}(cpu_usage)",
            "startTime": BASE_TS, "endTime": end_ts,
            "aggregationInterval": full_interval,
        }, iters))

    # ── Benchmark 5: No scopes, all fields (80 series) ───────────────
    results.append(bench_query(base_url,
        "LATEST: no scopes, all fields (80 series)",
        {
            "query": f"latest:{MEASUREMENT}()",
            "startTime": BASE_TS, "endTime": end_ts,
            "aggregationInterval": full_interval,
        }, iters))

    # ── Benchmark 6: LATEST without aggregation interval ──────────────
    # Tests the non-bucketed fast path (sparse index zero-I/O)
    results.append(bench_query(base_url,
        "LATEST: 1 series, exact tags, no interval",
        {
            "query": f"latest:{MEASUREMENT}(cpu_usage){{host:host-00,rack:rack-0}}",
            "startTime": BASE_TS, "endTime": end_ts,
        }, iters))

    # ── Benchmark 7: Compare with AVG (non-LATEST) ───────────────────
    # Baseline to show how much faster LATEST should be
    results.append(bench_query(base_url,
        "AVG: 1 series, exact tags, 1 field (baseline)",
        {
            "query": f"avg:{MEASUREMENT}(cpu_usage){{host:host-00,rack:rack-0}}",
            "startTime": BASE_TS, "endTime": end_ts,
            "aggregationInterval": full_interval,
        }, iters))

    # ── Print results ─────────────────────────────────────────────────
    summaries = [r.summary() for r in results]

    if args.json:
        print(json.dumps(summaries, indent=2))
    else:
        print(f"{'Benchmark':<52} {'Client p50':>11} {'Client p99':>11} {'Server p50':>11} {'Overhead':>9}")
        print("─" * 95)
        for s in summaries:
            print(f"{s['name']:<52} {s['client_p50_ms']:>8.2f} ms {s['client_p99_ms']:>8.2f} ms "
                  f"{s['server_p50_ms']:>8.2f} ms {s['overhead_ms']:>6.2f} ms")

        print()
        print("Legend:")
        print("  Client p50/p99 = end-to-end HTTP round-trip (includes network + JSON)")
        print("  Server p50     = server-side execution time (from response statistics)")
        print("  Overhead       = mean(client) - mean(server) = network + parse + format")

    # Save results
    ts = time.strftime("%Y%m%d_%H%M%S")
    outpath = f"benchmark/latest_bench_{ts}.json"
    with open(outpath, "w") as f:
        json.dump(summaries, f, indent=2)
    print(f"\nResults saved to {outpath}")


if __name__ == "__main__":
    main()
