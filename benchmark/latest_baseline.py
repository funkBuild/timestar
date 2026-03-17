#!/usr/bin/env python3
"""
Baseline benchmark for the exact LATEST query used in influxdb_comparison.py.

Schema: server.metrics with 10 hosts, 2 racks, 10 fields.
Query:  latest:server.metrics(cpu_usage) — no scopes, all series.

Usage:
  cd build && ./bin/timestar_http_server -c 4
  python3 benchmark/latest_baseline.py
  python3 benchmark/latest_baseline.py --skip-insert
"""

import argparse
import json
import math
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# ── Schema (matches influxdb_comparison.py / timestar_insert_bench.cpp) ──

MEASUREMENT = "server.metrics"
FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature",
]
MINUTE_NS = 60_000_000_000
BASE_TS = 1_000_000_000_000_000_000

NUM_HOSTS = 10
NUM_RACKS = 2
BATCH_SIZE = 10000   # timestamps per batch
NUM_BATCHES = 100    # enough for multiple TSM rollovers
SEED = 42


def generate_batch(seed, hid, rid, start_ts, batch_size):
    """Generate a batch matching influxdb_comparison.py format."""
    rng = random.Random(seed)
    writes = []
    for i in range(batch_size):
        ts = start_ts + i * MINUTE_NS
        fields = {}
        for fname in FIELD_NAMES:
            fields[fname] = rng.uniform(0, 100)
        writes.append({
            "measurement": MEASUREMENT,
            "tags": {"host": f"server-{hid:02d}", "rack": f"rack-{rid}"},
            "fields": fields,
            "timestamp": ts,
        })
    return writes


def insert_data(base_url: str, num_batches: int, batch_size: int):
    """Insert data matching influxdb_comparison.py schema."""
    rng = random.Random(SEED)
    total_points = num_batches * batch_size * len(FIELD_NAMES)
    print(f"Inserting {num_batches} batches × {batch_size} timestamps × {len(FIELD_NAMES)} fields")
    print(f"  = {total_points:,} total points")

    session = requests.Session()
    start = time.perf_counter()

    for b in range(num_batches):
        hid = rng.randint(1, NUM_HOSTS)
        rid = rng.randint(1, NUM_RACKS)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        batch = generate_batch(SEED + b, hid, rid, start_ts, batch_size)

        # Split into smaller HTTP batches to avoid timeouts
        chunk_size = 500
        for ci in range(0, len(batch), chunk_size):
            chunk = batch[ci:ci + chunk_size]
            resp = session.post(f"{base_url}/write", json={"writes": chunk}, timeout=60)
            if resp.status_code != 200:
                print(f"  Write failed batch {b}: {resp.status_code} {resp.text[:200]}")
                sys.exit(1)

        if (b + 1) % 20 == 0:
            elapsed = time.perf_counter() - start
            rate = (b + 1) * batch_size * len(FIELD_NAMES) / elapsed
            print(f"  {b+1}/{num_batches} batches, {rate:,.0f} pts/sec")

    elapsed = time.perf_counter() - start
    rate = total_points / elapsed
    print(f"  Done: {total_points:,} points in {elapsed:.1f}s ({rate:,.0f} pts/sec)")
    print("  Waiting for TSM flush ...")
    time.sleep(8)


def run_query(session, base_url, payload):
    start = time.perf_counter()
    resp = session.post(f"{base_url}/query", json=payload, timeout=30)
    client_ms = (time.perf_counter() - start) * 1000
    if resp.status_code != 200:
        raise RuntimeError(f"Query failed: {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    server_ms = data.get("statistics", {}).get("execution_time_ms", 0)
    series_count = data.get("statistics", {}).get("series_count", 0)
    point_count = data.get("statistics", {}).get("point_count", 0)
    return client_ms, server_ms, series_count, point_count


def bench(session, base_url, name, payload, iterations=200, warmup=20):
    # Warmup
    for _ in range(warmup):
        run_query(session, base_url, payload)

    client_times = []
    server_times = []
    series_count = 0
    point_count = 0

    for _ in range(iterations):
        c, s, sc, pc = run_query(session, base_url, payload)
        client_times.append(c)
        server_times.append(s)
        series_count = sc
        point_count = pc

    c = client_times
    s = server_times
    return {
        "name": name,
        "iterations": iterations,
        "series_count": series_count,
        "point_count": point_count,
        "client_p50_ms": round(statistics.median(c), 4),
        "client_p99_ms": round(sorted(c)[int(len(c) * 0.99)], 4),
        "client_min_ms": round(min(c), 4),
        "client_mean_ms": round(statistics.mean(c), 4),
        "server_p50_ms": round(statistics.median(s), 4),
        "server_p99_ms": round(sorted(s)[int(len(s) * 0.99)], 4),
        "server_min_ms": round(min(s), 4),
        "server_mean_ms": round(statistics.mean(s), 4),
        "overhead_ms": round(statistics.mean(c) - statistics.mean(s), 4),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8086)
    parser.add_argument("--skip-insert", action="store_true")
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--batches", type=int, default=100)
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.port}"
    session = requests.Session()

    try:
        session.get(f"{base_url}/health", timeout=5)
    except requests.ConnectionError:
        print(f"ERROR: Cannot connect to {base_url}")
        sys.exit(1)

    if not args.skip_insert:
        insert_data(base_url, args.batches, BATCH_SIZE)

    end_ts = BASE_TS + args.batches * BATCH_SIZE * MINUTE_NS
    full_span_ns = end_ts - BASE_TS
    hours = math.ceil(full_span_ns / (3600 * 1_000_000_000))
    full_interval = f"{hours}h"

    results = []

    # ── Exact influxdb_comparison LATEST query ────────────────────────
    r = bench(session, base_url,
        "latest:server.metrics(cpu_usage) [influxdb_comparison]",
        {"query": "latest:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        args.iterations)
    results.append(r)

    # ── Single series with exact tags ─────────────────────────────────
    r = bench(session, base_url,
        "latest:server.metrics(cpu_usage){host:server-01,rack:rack-1}",
        {"query": "latest:server.metrics(cpu_usage){host:server-01,rack:rack-1}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        args.iterations)
    results.append(r)

    # ── All fields, no scopes ─────────────────────────────────────────
    r = bench(session, base_url,
        "latest:server.metrics() [all fields, all series]",
        {"query": "latest:server.metrics()",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        args.iterations)
    results.append(r)

    # ── Print ─────────────────────────────────────────────────────────
    print()
    print(f"{'Benchmark':<58} {'Series':>6} {'Srv p50':>9} {'Srv p99':>9} {'Client p50':>11}")
    print("─" * 100)
    for r in results:
        print(f"{r['name']:<58} {r['series_count']:>6} "
              f"{r['server_p50_ms']:>6.3f}ms {r['server_p99_ms']:>6.3f}ms "
              f"{r['client_p50_ms']:>8.3f}ms")

    ts = time.strftime("%Y%m%d_%H%M%S")
    outpath = f"benchmark/latest_baseline_{ts}.json"
    with open(outpath, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {outpath}")


if __name__ == "__main__":
    main()
