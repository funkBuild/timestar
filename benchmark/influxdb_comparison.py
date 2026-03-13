#!/usr/bin/env python3
"""
InfluxDB vs TimeStar — apples-to-apples benchmark.

Generates the SAME data as timestar_insert_bench (deterministic, same schema)
and runs equivalent insert + query workloads against both databases.

Usage:
  # 1. Start InfluxDB (the script does this automatically via Docker)
  # 2. Start TimeStar  (you must do this manually first)
  #      cd build && ./bin/timestar_http_server -c 4
  # 3. Run the benchmark
  #      python3 benchmark/influxdb_comparison.py

The script uses Python's requests library for both databases so the client
overhead is identical — the comparison measures pure server-side throughput.
"""

import argparse
import json
import math
import os
import random
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

import requests

# ── Data schema (matches timestar_insert_bench.cpp) ────────────────────

MEASUREMENT = "server.metrics"
FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature",
]
MINUTE_NS = 60_000_000_000
BASE_TS = 1_000_000_000_000_000_000  # ~2001-09-09 in nanos

# ── InfluxDB Docker management ─────────────────────────────────────────

INFLUX_CONTAINER = "timestar-bench-influxdb"
INFLUX_PORT = 8087  # avoid collision with TimeStar on 8086
INFLUX_ORG = "bench"
INFLUX_BUCKET = "bench"
INFLUX_TOKEN = "benchtoken123456"  # noqa: S105 – test-only token
INFLUX_IMAGE = "influxdb:2.7"


def docker_influx_running() -> bool:
    r = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", INFLUX_CONTAINER],
        capture_output=True, text=True,
    )
    return r.returncode == 0 and "true" in r.stdout


def start_influxdb(cpus: int, memory: str):
    """Start InfluxDB 2.7 in Docker with resource limits matching TimeStar."""
    if docker_influx_running():
        print(f"  InfluxDB container '{INFLUX_CONTAINER}' already running.")
        return

    # Remove stale container if exists
    subprocess.run(["docker", "rm", "-f", INFLUX_CONTAINER],
                   capture_output=True)

    print(f"  Pulling {INFLUX_IMAGE} …")
    subprocess.run(["docker", "pull", INFLUX_IMAGE],
                   capture_output=True, check=True)

    print(f"  Starting InfluxDB (cpus={cpus}, memory={memory}) …")
    subprocess.run([
        "docker", "run", "-d",
        "--name", INFLUX_CONTAINER,
        "--cpus", str(cpus),
        "--memory", memory,
        "-p", f"{INFLUX_PORT}:8086",
        "-e", f"DOCKER_INFLUXDB_INIT_MODE=setup",
        "-e", f"DOCKER_INFLUXDB_INIT_USERNAME=admin",
        "-e", f"DOCKER_INFLUXDB_INIT_PASSWORD=adminadmin",
        "-e", f"DOCKER_INFLUXDB_INIT_ORG={INFLUX_ORG}",
        "-e", f"DOCKER_INFLUXDB_INIT_BUCKET={INFLUX_BUCKET}",
        "-e", f"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN={INFLUX_TOKEN}",
        INFLUX_IMAGE,
    ], check=True, capture_output=True)

    # Wait for ready
    print("  Waiting for InfluxDB to be ready …")
    for i in range(30):
        try:
            r = requests.get(f"http://127.0.0.1:{INFLUX_PORT}/health", timeout=2)
            if r.status_code == 200:
                print("  InfluxDB ready.")
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("InfluxDB did not become ready in 30s")


def stop_influxdb():
    subprocess.run(["docker", "rm", "-f", INFLUX_CONTAINER], capture_output=True)


# ── Deterministic data generation ──────────────────────────────────────

def generate_batch(seed: int, host_id: int, rack_id: int,
                   start_ts: int, count: int) -> tuple:
    """
    Returns (line_protocol_str, timestar_json_str) for the same data.
    Uses the same PRNG seeding as the C++ bench for reproducibility.
    """
    rng = random.Random(seed ^ (host_id << 32) ^ start_ts)

    host_tag = f"host-{host_id:02d}"
    rack_tag = f"rack-{rack_id}"

    # Build both formats simultaneously
    lp_lines = []
    # TimeStar array-format JSON
    timestamps = []
    field_arrays = {f: [] for f in FIELD_NAMES}

    for i in range(count):
        ts = start_ts + i * MINUTE_NS
        timestamps.append(ts)

        vals = {}
        for fname in FIELD_NAMES:
            v = rng.uniform(0.0, 100.0)
            vals[fname] = v
            field_arrays[fname].append(round(v, 6))

        # Line protocol: measurement,tag=val,tag=val field=val,field=val timestamp
        fields_str = ",".join(f"{k}={v}" for k, v in vals.items())
        lp_lines.append(
            f"{MEASUREMENT},host={host_tag},rack={rack_tag} {fields_str} {ts}"
        )

    lp = "\n".join(lp_lines)

    ts_json = json.dumps({
        "measurement": MEASUREMENT,
        "tags": {"host": host_tag, "rack": rack_tag},
        "timestamps": timestamps,
        "fields": {k: v for k, v in field_arrays.items()},
    })

    return lp, ts_json


# ── Benchmark runner ───────────────────────────────────────────────────

@dataclass
class InsertResult:
    total_points: int = 0
    wall_seconds: float = 0.0
    ok: int = 0
    fail: int = 0
    first_error: str = ""
    latencies_ms: list = field(default_factory=list)

    @property
    def throughput(self) -> float:
        return self.total_points / self.wall_seconds if self.wall_seconds > 0 else 0

    @property
    def avg_latency_ms(self) -> float:
        return sum(self.latencies_ms) / len(self.latencies_ms) if self.latencies_ms else 0

    @property
    def p50_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[len(s) // 2]

    @property
    def p99_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.99)]


@dataclass
class QueryBenchResult:
    name: str = ""
    iterations: int = 0
    successes: int = 0
    total_ms: float = 0.0
    min_ms: float = float("inf")
    max_ms: float = 0.0
    latencies_ms: list = field(default_factory=list)
    response_bytes: int = 0
    error: str = ""

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.successes if self.successes > 0 else 0

    @property
    def p50_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[len(s) // 2]

    @property
    def p95_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[int(len(s) * 0.95)]


def run_insert_bench(target: str, batches: int, batch_size: int,
                     num_hosts: int, num_racks: int, seed: int,
                     concurrency: int,
                     ts_host: str, ts_port: int,
                     influx_port: int) -> InsertResult:
    """Run insert benchmark against the specified target."""
    result = InsertResult()
    pts_per_batch = batch_size * len(FIELD_NAMES)

    # Pre-generate all payloads
    payloads = []
    rng = random.Random(seed)
    for b in range(batches):
        hid = rng.randint(1, num_hosts)
        rid = rng.randint(1, num_racks)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        lp, ts_json = generate_batch(seed, hid, rid, start_ts, batch_size)
        payloads.append((lp, ts_json))

    def send_one(idx):
        lp, ts_json = payloads[idx]
        t0 = time.perf_counter()
        try:
            if target == "influxdb":
                r = requests.post(
                    f"http://127.0.0.1:{influx_port}/api/v2/write"
                    f"?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns",
                    data=lp,
                    headers={
                        "Authorization": f"Token {INFLUX_TOKEN}",
                        "Content-Type": "text/plain; charset=utf-8",
                    },
                    timeout=60,
                )
                ok = r.status_code == 204
                err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
            else:  # timestar
                r = requests.post(
                    f"http://{ts_host}:{ts_port}/write",
                    data=ts_json,
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
                ok = 200 <= r.status_code < 300
                err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            ok = False
            err = str(e)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        return ok, err, elapsed_ms

    wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(send_one, i): i for i in range(batches)}
        for fut in as_completed(futures):
            ok, err, lat = fut.result()
            if ok:
                result.ok += 1
                result.total_points += pts_per_batch
            else:
                result.fail += 1
                if not result.first_error:
                    result.first_error = err
            result.latencies_ms.append(lat)

    result.wall_seconds = time.perf_counter() - wall_start
    return result


# ── Query benchmark ────────────────────────────────────────────────────

# Time ranges matching the insert benchmark data
NARROW_END = BASE_TS + 100 * MINUTE_NS
MED_END = BASE_TS + 10000 * MINUTE_NS


def build_query_suite(total_batches: int, batch_size: int):
    """
    Returns list of (name, timestar_request, influxdb_flux_query, iterations).

    Both databases must do equivalent work per query.  Flux aggregations like
    mean(), last(), sum() etc. WITHOUT aggregateWindow() collapse all points
    in the range into a single scalar per series.  TimeStar only does this
    when aggregationInterval > 0, so we set the interval to cover the full
    query span for those cases (one giant bucket == one scalar output).
    """
    end_ts = BASE_TS + total_batches * batch_size * MINUTE_NS

    # Interval strings that cover the entire query range so TimeStar
    # collapses to a single aggregate per series, matching Flux mean()/last()/etc.
    full_span_ns = end_ts - BASE_TS
    narrow_span_ns = NARROW_END - BASE_TS
    med_span_ns = MED_END - BASE_TS

    def span_to_interval(ns):
        """Convert nanosecond span to a TimeStar interval string."""
        # Use hours (rounded up) to get a single bucket
        hours = math.ceil(ns / (3600 * 1_000_000_000))
        return f"{hours}h"

    full_interval = span_to_interval(full_span_ns)
    narrow_interval = span_to_interval(narrow_span_ns)
    med_interval = span_to_interval(med_span_ns)

    # Convert nanos to RFC3339 for Flux
    def ns_to_rfc(ns):
        secs = ns // 1_000_000_000
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(secs))

    narrow_start_rfc = ns_to_rfc(BASE_TS)
    narrow_end_rfc = ns_to_rfc(NARROW_END)
    med_end_rfc = ns_to_rfc(MED_END)
    full_end_rfc = ns_to_rfc(end_ts)

    suite = []

    # ── 1. Latest value (full range → single value per series) ──
    suite.append((
        "latest: single field",
        {"query": "latest:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {full_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> last()',
        20,
    ))

    # ── 2. Avg narrow range (→ single value per series) ─────────
    suite.append((
        "avg: narrow range, single field",
        {"query": "avg:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> mean()',
        20,
    ))

    # ── 3. Max narrow range ─────────────────────────────────────
    suite.append((
        "max: narrow range, single field",
        {"query": "max:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> max()',
        20,
    ))

    # ── 4. Sum narrow range ─────────────────────────────────────
    suite.append((
        "sum: narrow range, single field",
        {"query": "sum:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> sum()',
        20,
    ))

    # ── 5. Count narrow range ───────────────────────────────────
    suite.append((
        "count: narrow range, single field",
        {"query": "count:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> count()',
        20,
    ))

    # ── 6. Avg with tag filter ──────────────────────────────────
    suite.append((
        "avg: single host filter",
        {"query": "avg:server.metrics(cpu_usage){host:host-01}",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage" and r.host == "host-01") '
        f'|> mean()',
        20,
    ))

    # ── 7. Group by host (narrow range, one value per host) ─────
    suite.append((
        "avg: group by host",
        {"query": "avg:server.metrics(cpu_usage) by {host}",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> group(columns: ["host"]) '
        f'|> mean()',
        10,
    ))

    # ── 8. Time-bucketed aggregation (1h buckets, medium range) ─
    # Both use explicit time buckets — already equivalent.
    suite.append((
        "avg: 1h buckets, medium range",
        {"query": "avg:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": MED_END,
         "aggregationInterval": "1h"},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {med_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> aggregateWindow(every: 1h, fn: mean)',
        10,
    ))

    # ── 9. All fields, narrow range ─────────────────────────────
    suite.append((
        "avg: narrow, all 10 fields",
        {"query": "avg:server.metrics()",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}") '
        f'|> mean()',
        10,
    ))

    # ── 10. Full range, single field (→ single value) ───────────
    suite.append((
        "avg: full range, single field",
        {"query": "avg:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {full_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> mean()',
        5,
    ))

    # ── 11. Group by host, full range ───────────────────────────
    suite.append((
        "avg: group by host, full range",
        {"query": "avg:server.metrics(cpu_usage) by {host}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {full_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> group(columns: ["host"]) '
        f'|> mean()',
        5,
    ))

    # ── 12. 5-min buckets, tag filter, medium range ─────────────
    # Both use explicit time buckets — already equivalent.
    suite.append((
        "avg: 5m buckets, tag filter",
        {"query": "avg:server.metrics(cpu_usage){host:host-05}",
         "startTime": BASE_TS, "endTime": MED_END,
         "aggregationInterval": "5m"},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {med_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage" and r.host == "host-05") '
        f'|> aggregateWindow(every: 5m, fn: mean)',
        10,
    ))

    return suite


def run_query_bench_timestar(host: str, port: int, name: str,
                             ts_req: dict, iterations: int) -> QueryBenchResult:
    result = QueryBenchResult(name=name, iterations=iterations)
    url = f"http://{host}:{port}/query"
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            r = requests.post(url, json=ts_req, timeout=30)
            ok = 200 <= r.status_code < 300
        except Exception as e:
            result.error = str(e)
            continue
        elapsed = (time.perf_counter() - t0) * 1000
        if ok:
            result.successes += 1
            result.total_ms += elapsed
            result.min_ms = min(result.min_ms, elapsed)
            result.max_ms = max(result.max_ms, elapsed)
            result.latencies_ms.append(elapsed)
            if result.response_bytes == 0:
                result.response_bytes = len(r.content)
        else:
            if not result.error:
                result.error = f"HTTP {r.status_code}: {r.text[:200]}"
    if result.successes == 0:
        result.min_ms = 0
    return result


def run_query_bench_influxdb(port: int, name: str,
                              flux_query: str, iterations: int) -> QueryBenchResult:
    result = QueryBenchResult(name=name, iterations=iterations)
    url = f"http://127.0.0.1:{port}/api/v2/query?org={INFLUX_ORG}"
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "application/vnd.flux",
        "Accept": "application/csv",
    }
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            r = requests.post(url, data=flux_query, headers=headers, timeout=30)
            ok = 200 <= r.status_code < 300
        except Exception as e:
            result.error = str(e)
            continue
        elapsed = (time.perf_counter() - t0) * 1000
        if ok:
            result.successes += 1
            result.total_ms += elapsed
            result.min_ms = min(result.min_ms, elapsed)
            result.max_ms = max(result.max_ms, elapsed)
            result.latencies_ms.append(elapsed)
            if result.response_bytes == 0:
                result.response_bytes = len(r.content)
        else:
            if not result.error:
                result.error = f"HTTP {r.status_code}: {r.text[:200]}"
    if result.successes == 0:
        result.min_ms = 0
    return result


# ── Formatting helpers ─────────────────────────────────────────────────

def fmt_pts(n: float) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


def fmt_ms(ms: float) -> str:
    if ms >= 1000:
        return f"{ms/1000:.2f}s"
    return f"{ms:.2f}ms"


def print_separator(char="=", width=90):
    print(char * width)


def print_comparison_row(label, ts_val, influx_val, unit="", better_lower=True):
    """Print a row comparing two values with winner indicator."""
    if ts_val == 0 and influx_val == 0:
        ratio = "N/A"
    elif influx_val == 0:
        ratio = "inf"
    else:
        r = ts_val / influx_val
        if better_lower:
            # Lower is better (latency) — ratio < 1 means TimeStar wins
            if r < 1:
                ratio = f"TimeStar {1/r:.1f}x faster"
            else:
                ratio = f"InfluxDB {r:.1f}x faster"
        else:
            # Higher is better (throughput) — ratio > 1 means TimeStar wins
            if r > 1:
                ratio = f"TimeStar {r:.1f}x faster"
            else:
                ratio = f"InfluxDB {1/r:.1f}x faster"

    print(f"  {label:<36s}  {ts_val:>12.2f}{unit}  {influx_val:>12.2f}{unit}  {ratio}")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="InfluxDB vs TimeStar benchmark")
    parser.add_argument("--cpus", type=int, default=4,
                        help="CPU cores for InfluxDB container (match TimeStar -c)")
    parser.add_argument("--memory", default="8g",
                        help="Memory limit for InfluxDB container")
    parser.add_argument("--batches", type=int, default=100,
                        help="Number of insert batches")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Timestamps per batch")
    parser.add_argument("--hosts", type=int, default=10,
                        help="Number of simulated hosts")
    parser.add_argument("--racks", type=int, default=2,
                        help="Number of simulated racks")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="Concurrent HTTP connections")
    parser.add_argument("--seed", type=int, default=42,
                        help="PRNG seed")
    parser.add_argument("--ts-host", default="127.0.0.1",
                        help="TimeStar host")
    parser.add_argument("--ts-port", type=int, default=8086,
                        help="TimeStar port")
    parser.add_argument("--skip-insert", action="store_true",
                        help="Skip insert bench (data already loaded)")
    parser.add_argument("--skip-query", action="store_true",
                        help="Skip query bench")
    parser.add_argument("--no-docker", action="store_true",
                        help="Don't manage Docker (InfluxDB already running)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Remove InfluxDB container after benchmark")
    args = parser.parse_args()

    total_points = args.batches * args.batch_size * len(FIELD_NAMES)

    print_separator()
    print(" TimeStar vs InfluxDB 2.7 — Benchmark")
    print_separator()
    print(f"  CPUs:           {args.cpus}")
    print(f"  Memory:         {args.memory}")
    print(f"  Batches:        {args.batches}")
    print(f"  Batch size:     {args.batch_size} timestamps x {len(FIELD_NAMES)} fields "
          f"= {args.batch_size * len(FIELD_NAMES):,} pts/batch")
    print(f"  Total points:   {total_points:,}")
    print(f"  Hosts/Racks:    {args.hosts}/{args.racks}")
    print(f"  Concurrency:    {args.concurrency} threads")
    print(f"  Seed:           {args.seed}")
    print_separator()
    print()

    # ── Start InfluxDB ──────────────────────────────────────────
    if not args.no_docker:
        print("[1/4] Setting up InfluxDB …")
        start_influxdb(args.cpus, args.memory)
        print()

    # ── Verify both targets are reachable ───────────────────────
    print("[2/4] Health checks …")
    try:
        r = requests.get(f"http://{args.ts_host}:{args.ts_port}/health", timeout=5)
        assert r.status_code == 200
        print(f"  TimeStar  @ {args.ts_host}:{args.ts_port}: OK")
    except Exception as e:
        print(f"  TimeStar  @ {args.ts_host}:{args.ts_port}: FAILED ({e})")
        print("  Start TimeStar first:  cd build && ./bin/timestar_http_server -c 4")
        sys.exit(1)

    try:
        r = requests.get(f"http://127.0.0.1:{INFLUX_PORT}/health", timeout=5)
        assert r.status_code == 200
        print(f"  InfluxDB  @ 127.0.0.1:{INFLUX_PORT}: OK")
    except Exception as e:
        print(f"  InfluxDB  @ 127.0.0.1:{INFLUX_PORT}: FAILED ({e})")
        sys.exit(1)
    print()

    # ── Insert benchmark ────────────────────────────────────────
    ts_insert = None
    influx_insert = None

    if not args.skip_insert:
        print("[3/4] Insert benchmark …")
        print(f"  Generating {args.batches} batches ({fmt_pts(total_points)} points) …")

        # Warmup: 5 batches each
        warmup_batches = min(5, args.batches // 5) if args.batches >= 10 else 1
        print(f"  Warming up ({warmup_batches} batches each) …")
        run_insert_bench("timestar", warmup_batches, args.batch_size,
                         args.hosts, args.racks, args.seed + 999,
                         args.concurrency, args.ts_host, args.ts_port,
                         INFLUX_PORT)
        run_insert_bench("influxdb", warmup_batches, args.batch_size,
                         args.hosts, args.racks, args.seed + 999,
                         args.concurrency, args.ts_host, args.ts_port,
                         INFLUX_PORT)

        print()
        print("  Running TimeStar insert …")
        ts_insert = run_insert_bench(
            "timestar", args.batches, args.batch_size,
            args.hosts, args.racks, args.seed,
            args.concurrency, args.ts_host, args.ts_port, INFLUX_PORT)
        print(f"    {ts_insert.ok}/{args.batches} OK, "
              f"{fmt_pts(ts_insert.throughput)} pts/sec, "
              f"wall={ts_insert.wall_seconds:.2f}s")
        if ts_insert.first_error:
            print(f"    first error: {ts_insert.first_error[:120]}")

        # Wait briefly for TimeStar background flushes
        print("  Waiting 2s for background flushes …")
        time.sleep(2)

        print("  Running InfluxDB insert …")
        influx_insert = run_insert_bench(
            "influxdb", args.batches, args.batch_size,
            args.hosts, args.racks, args.seed,
            args.concurrency, args.ts_host, args.ts_port, INFLUX_PORT)
        print(f"    {influx_insert.ok}/{args.batches} OK, "
              f"{fmt_pts(influx_insert.throughput)} pts/sec, "
              f"wall={influx_insert.wall_seconds:.2f}s")
        if influx_insert.first_error:
            print(f"    first error: {influx_insert.first_error[:120]}")

        print()

        # ── Insert comparison table ─────────────────────────────
        print_separator("-")
        print(" INSERT RESULTS")
        print_separator("-")
        print(f"  {'Metric':<36s}  {'TimeStar':>14s}  {'InfluxDB':>14s}  Winner")
        print(f"  {'-'*36}  {'-'*14}  {'-'*14}  {'-'*24}")
        print_comparison_row("Throughput (pts/sec)",
                             ts_insert.throughput, influx_insert.throughput,
                             better_lower=False)
        print_comparison_row("Wall time (s)",
                             ts_insert.wall_seconds, influx_insert.wall_seconds,
                             "s", better_lower=True)
        print_comparison_row("Avg batch latency (ms)",
                             ts_insert.avg_latency_ms, influx_insert.avg_latency_ms,
                             "ms", better_lower=True)
        print_comparison_row("P50 batch latency (ms)",
                             ts_insert.p50_ms, influx_insert.p50_ms,
                             "ms", better_lower=True)
        print_comparison_row("P99 batch latency (ms)",
                             ts_insert.p99_ms, influx_insert.p99_ms,
                             "ms", better_lower=True)
        print()

    # ── Query benchmark ─────────────────────────────────────────
    if not args.skip_query:
        print("[4/4] Query benchmark …")

        suite = build_query_suite(args.batches, args.batch_size)

        # Warmup queries
        print("  Warming up (3 iterations each) …")
        for name, ts_req, flux_q, iters in suite:
            run_query_bench_timestar(args.ts_host, args.ts_port, name, ts_req, 3)
            run_query_bench_influxdb(INFLUX_PORT, name, flux_q, 3)

        print()
        print_separator("-")
        print(" QUERY RESULTS")
        print_separator("-")
        print(f"  {'Query':<36s}  {'TimeStar':>10s}  {'InfluxDB':>10s}  {'Winner':<28s}  {'Detail'}")
        print(f"  {'-'*36}  {'-'*10}  {'-'*10}  {'-'*28}  {'-'*20}")

        ts_total_ms = 0
        influx_total_ms = 0

        for name, ts_req, flux_q, iters in suite:
            ts_res = run_query_bench_timestar(
                args.ts_host, args.ts_port, name, ts_req, iters)
            influx_res = run_query_bench_influxdb(
                INFLUX_PORT, name, flux_q, iters)

            ts_avg = ts_res.avg_ms
            influx_avg = influx_res.avg_ms
            ts_total_ms += ts_res.total_ms
            influx_total_ms += influx_res.total_ms

            # Winner
            if ts_res.successes == 0 and influx_res.successes == 0:
                winner = "BOTH FAILED"
            elif ts_res.successes == 0:
                winner = "InfluxDB (TimeStar err)"
            elif influx_res.successes == 0:
                winner = "TimeStar (InfluxDB err)"
            elif ts_avg < influx_avg:
                ratio = influx_avg / ts_avg if ts_avg > 0 else float("inf")
                winner = f"TimeStar {ratio:.1f}x faster"
            else:
                ratio = ts_avg / influx_avg if influx_avg > 0 else float("inf")
                winner = f"InfluxDB {ratio:.1f}x faster"

            detail = ""
            if ts_res.error:
                detail = f"TS err: {ts_res.error[:40]}"
            elif influx_res.error:
                detail = f"IX err: {influx_res.error[:40]}"
            else:
                detail = f"resp: {ts_res.response_bytes}B / {influx_res.response_bytes}B"

            print(f"  {name:<36s}  {fmt_ms(ts_avg):>10s}  {fmt_ms(influx_avg):>10s}  {winner:<28s}  {detail}")

        print()
        print(f"  {'TOTAL query time':<36s}  {fmt_ms(ts_total_ms):>10s}  {fmt_ms(influx_total_ms):>10s}")
        print()

    # ── Final summary ───────────────────────────────────────────
    print_separator("=")
    print(" SUMMARY")
    print_separator("=")
    if ts_insert and influx_insert:
        ts_tput = ts_insert.throughput
        ix_tput = influx_insert.throughput
        if ix_tput > 0:
            ratio = ts_tput / ix_tput
            if ratio > 1:
                print(f"  Insert throughput:  TimeStar is {ratio:.1f}x faster "
                      f"({fmt_pts(ts_tput)} vs {fmt_pts(ix_tput)} pts/sec)")
            else:
                print(f"  Insert throughput:  InfluxDB is {1/ratio:.1f}x faster "
                      f"({fmt_pts(ix_tput)} vs {fmt_pts(ts_tput)} pts/sec)")
    if not args.skip_query and ts_total_ms > 0 and influx_total_ms > 0:
        if ts_total_ms < influx_total_ms:
            ratio = influx_total_ms / ts_total_ms
            print(f"  Query total time:   TimeStar is {ratio:.1f}x faster "
                  f"({fmt_ms(ts_total_ms)} vs {fmt_ms(influx_total_ms)})")
        else:
            ratio = ts_total_ms / influx_total_ms
            print(f"  Query total time:   InfluxDB is {ratio:.1f}x faster "
                  f"({fmt_ms(influx_total_ms)} vs {fmt_ms(ts_total_ms)})")

    print_separator("=")
    print()

    # ── Cleanup ─────────────────────────────────────────────────
    if args.cleanup and not args.no_docker:
        print("Cleaning up InfluxDB container …")
        stop_influxdb()


if __name__ == "__main__":
    main()
