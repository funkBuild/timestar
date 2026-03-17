#!/usr/bin/env python3
"""
TDengine vs TimeStar — apples-to-apples benchmark.

Generates the SAME data as the InfluxDB comparison benchmark (deterministic,
same schema) and runs equivalent insert + query workloads against both databases.

Usage:
  # 1. Start TimeStar (you must do this manually first)
  #      cd build && ./bin/timestar_http_server -c 4
  # 2. Run the benchmark (TDengine started automatically via Docker)
  #      python3 benchmark/tdengine_comparison.py
  # 3. With options:
  #      python3 benchmark/tdengine_comparison.py --cpus 4 --batches 200 --batch-size 5000

TDengine uses a REST API on port 6041 with SQL syntax.
The data schema matches timestar_insert_bench.cpp and influxdb_comparison.py
for cross-benchmark consistency.
"""

import argparse
import json
import math
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
BASE_TS = 1_704_067_200_000_000_000  # 2024-01-01 00:00:00 UTC in nanos

# ── TDengine Docker management ──────────────────────────────────────

TDENGINE_CONTAINER = "timestar-bench-tdengine"
TDENGINE_PORT = 6041
TDENGINE_IMAGE = "tdengine/tdengine:latest"
TDENGINE_DB = "bench"
TDENGINE_USER = "root"
TDENGINE_PASS = "taosdata"  # noqa: S105 — default TDengine password


def docker_tdengine_running() -> bool:
    r = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", TDENGINE_CONTAINER],
        capture_output=True, text=True,
    )
    return r.returncode == 0 and "true" in r.stdout


def start_tdengine(cpus: int, memory: str):
    """Start TDengine in Docker with resource limits matching TimeStar."""
    if docker_tdengine_running():
        print(f"  TDengine container '{TDENGINE_CONTAINER}' already running.")
        return

    subprocess.run(["docker", "rm", "-f", TDENGINE_CONTAINER], capture_output=True)

    print(f"  Pulling {TDENGINE_IMAGE} …")
    subprocess.run(["docker", "pull", TDENGINE_IMAGE], capture_output=True, check=True)

    print(f"  Starting TDengine (cpus={cpus}, memory={memory}) …")
    subprocess.run([
        "docker", "run", "-d",
        "--name", TDENGINE_CONTAINER,
        "--cpus", str(cpus),
        "--memory", memory,
        "-p", f"{TDENGINE_PORT}:6041",
        TDENGINE_IMAGE,
    ], check=True, capture_output=True)

    # Wait for ready
    print("  Waiting for TDengine to be ready …")
    for i in range(30):
        try:
            r = tdengine_sql("SELECT SERVER_VERSION()")
            if r is not None:
                print("  TDengine ready.")
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("TDengine did not become ready in 30s")

    # Create database and super table
    print("  Creating database and schema …")
    tdengine_sql(f"CREATE DATABASE IF NOT EXISTS {TDENGINE_DB} PRECISION 'ns'")
    tdengine_sql(f"USE {TDENGINE_DB}")

    # Create super table with tags (host, rack) and value columns.
    # TDengine REST API: use database-scoped endpoint, BINARY for tags.
    # Note: "host" is a reserved word in TDengine — must be backtick-quoted.
    cols = ", ".join(f"`{f}` DOUBLE" for f in FIELD_NAMES)
    tdengine_sql_raw(
        f"CREATE STABLE server_metrics "
        f"(ts TIMESTAMP, {cols}) "
        f"TAGS (`host` BINARY(20), `rack` BINARY(10))",
        TDENGINE_DB,
    )
    print("  Schema created.")


def stop_tdengine():
    subprocess.run(["docker", "rm", "-f", TDENGINE_CONTAINER], capture_output=True)


def tdengine_sql_raw(sql: str, db: str = None) -> Optional[dict]:
    """Execute SQL via the database-scoped REST endpoint (no db prefix in SQL)."""
    url = f"http://127.0.0.1:{TDENGINE_PORT}/rest/sql"
    if db:
        url += f"/{db}"
    try:
        r = requests.post(url, data=sql, auth=(TDENGINE_USER, TDENGINE_PASS),
                          headers={"Content-Type": "text/plain"}, timeout=30)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def tdengine_sql(sql: str, db: str = None) -> Optional[dict]:
    """Execute a SQL statement against TDengine REST API."""
    url = f"http://127.0.0.1:{TDENGINE_PORT}/rest/sql"
    if db:
        url += f"/{db}"
    try:
        r = requests.post(
            url,
            data=sql,
            auth=(TDENGINE_USER, TDENGINE_PASS),
            headers={"Content-Type": "text/plain"},
            timeout=30,
        )
        if r.status_code == 200:
            body = r.json()
            if body.get("code") == 0 or body.get("status") == "succ":
                return body
            return body
        return None
    except Exception:
        return None


# ── Data generation ──────────────────────────────────────────────────

def generate_batch(seed: int, host_id: int, rack_id: int,
                   start_ts: int, batch_size: int):
    """Generate a batch of data points. Returns (tdengine_sql, timestar_json)."""
    host_tag = f"host-{host_id:02d}"
    rack_tag = f"rack-{rack_id}"
    data_rng = random.Random(seed ^ (host_id << 32) ^ start_ts)

    timestamps = []
    field_arrays = {f: [] for f in FIELD_NAMES}

    for i in range(batch_size):
        ts = start_ts + i * MINUTE_NS
        timestamps.append(ts)
        for f in FIELD_NAMES:
            field_arrays[f].append(round(data_rng.uniform(0.0, 100.0), 6))

    # Build TDengine INSERT SQL using auto-create subtable
    # TDengine supports batch INSERT with multiple VALUES clauses
    subtable = f"t_{host_tag.replace('-', '_')}_{rack_tag.replace('-', '_')}"
    values_parts = []
    for i in range(batch_size):
        vals = ", ".join(str(field_arrays[f][i]) for f in FIELD_NAMES)
        values_parts.append(f"({timestamps[i]}, {vals})")

    # TDengine auto-creates subtable from supertable with USING clause.
    # No database prefix needed — the REST endpoint is scoped to the database.
    td_sql = (
        f"INSERT INTO {subtable} "
        f"USING server_metrics TAGS ('{host_tag}', '{rack_tag}') "
        f"VALUES {' '.join(values_parts)}"
    )

    # TimeStar JSON format
    ts_json = json.dumps({
        "measurement": MEASUREMENT,
        "tags": {"host": host_tag, "rack": rack_tag},
        "timestamps": timestamps,
        "fields": {k: v for k, v in field_arrays.items()},
    })

    return td_sql, ts_json


# ── Result types ─────────────────────────────────────────────────────

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
        s = sorted(self.latencies_ms) if self.latencies_ms else [0]
        return s[len(s) // 2]

    @property
    def p99_ms(self) -> float:
        s = sorted(self.latencies_ms) if self.latencies_ms else [0]
        return s[int(len(s) * 0.99)]


@dataclass
class QueryResult:
    name: str = ""
    iterations: int = 0
    successes: int = 0
    total_ms: float = 0.0
    min_ms: float = float("inf")
    max_ms: float = 0.0
    latencies_ms: list = field(default_factory=list)
    error: str = ""

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.successes if self.successes > 0 else 0

    @property
    def p50_ms(self) -> float:
        s = sorted(self.latencies_ms) if self.latencies_ms else [0]
        return s[len(s) // 2]

    @property
    def p95_ms(self) -> float:
        s = sorted(self.latencies_ms) if self.latencies_ms else [0]
        return s[int(len(s) * 0.95)]


# ── Insert benchmarks ────────────────────────────────────────────────

def run_insert_timestar(batches, batch_size, hosts, racks, seed, concurrency,
                        ts_host, ts_port) -> InsertResult:
    result = InsertResult()
    pts_per_batch = batch_size * len(FIELD_NAMES)

    rng = random.Random(seed)
    payloads = []
    for b in range(batches):
        hid = rng.randint(1, hosts)
        rid = rng.randint(1, racks)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        _, ts_json = generate_batch(seed, hid, rid, start_ts, batch_size)
        payloads.append(ts_json)

    def send_one(idx):
        t0 = time.perf_counter()
        try:
            r = requests.post(
                f"http://{ts_host}:{ts_port}/write",
                data=payloads[idx],
                headers={"Content-Type": "application/json"},
                timeout=60,
            )
            ok = 200 <= r.status_code < 300
            err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            ok, err = False, str(e)[:200]
        return ok, err, (time.perf_counter() - t0) * 1000

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


def run_insert_tdengine(batches, batch_size, hosts, racks, seed,
                        concurrency) -> InsertResult:
    result = InsertResult()
    pts_per_batch = batch_size * len(FIELD_NAMES)

    rng = random.Random(seed)
    payloads = []
    for b in range(batches):
        hid = rng.randint(1, hosts)
        rid = rng.randint(1, racks)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        td_sql, _ = generate_batch(seed, hid, rid, start_ts, batch_size)
        payloads.append(td_sql)

    # Ensure USE database before inserts
    tdengine_sql(f"USE {TDENGINE_DB}")

    def send_one(idx):
        t0 = time.perf_counter()
        try:
            r = requests.post(
                f"http://127.0.0.1:{TDENGINE_PORT}/rest/sql/{TDENGINE_DB}",
                data=payloads[idx],
                auth=(TDENGINE_USER, TDENGINE_PASS),
                headers={"Content-Type": "text/plain"},
                timeout=120,
            )
            ok = r.status_code == 200
            if ok:
                body = r.json()
                ok = body.get("code") == 0 or body.get("status") == "succ"
                err = "" if ok else body.get("desc", str(body))[:200]
            else:
                err = f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            ok, err = False, str(e)[:200]
        return ok, err, (time.perf_counter() - t0) * 1000

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


# ── Query benchmarks ─────────────────────────────────────────────────

def run_query_timestar(host, port, name, req, iterations) -> QueryResult:
    result = QueryResult(name=name, iterations=iterations)
    url = f"http://{host}:{port}/query"
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            r = requests.post(url, json=req, timeout=30)
            ok = 200 <= r.status_code < 300
        except Exception as e:
            result.error = str(e)[:200]
            ok = False
        elapsed = (time.perf_counter() - t0) * 1000
        if ok:
            result.successes += 1
            result.total_ms += elapsed
            result.min_ms = min(result.min_ms, elapsed)
            result.max_ms = max(result.max_ms, elapsed)
            result.latencies_ms.append(elapsed)
    if result.successes == 0:
        result.min_ms = 0
    return result


def run_query_tdengine(name, sql, iterations) -> QueryResult:
    result = QueryResult(name=name, iterations=iterations)
    url = f"http://127.0.0.1:{TDENGINE_PORT}/rest/sql/{TDENGINE_DB}"
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            r = requests.post(
                url,
                data=sql,
                auth=(TDENGINE_USER, TDENGINE_PASS),
                headers={"Content-Type": "text/plain"},
                timeout=60,
            )
            ok = r.status_code == 200
            if ok:
                body = r.json()
                ok = body.get("code") == 0 or body.get("status") == "succ"
                if not ok and not result.error:
                    result.error = body.get("desc", str(body))[:200]
        except Exception as e:
            ok = False
            if not result.error:
                result.error = str(e)[:200]
        elapsed = (time.perf_counter() - t0) * 1000
        if ok:
            result.successes += 1
            result.total_ms += elapsed
            result.min_ms = min(result.min_ms, elapsed)
            result.max_ms = max(result.max_ms, elapsed)
            result.latencies_ms.append(elapsed)
    if result.successes == 0:
        result.min_ms = 0
    return result


# ── Query suite ──────────────────────────────────────────────────────

def build_query_suite(total_batches, batch_size):
    """Build equivalent queries for TimeStar and TDengine."""
    end_ts = BASE_TS + total_batches * batch_size * MINUTE_NS
    NARROW_END = BASE_TS + 100 * MINUTE_NS
    MED_END = BASE_TS + 10000 * MINUTE_NS

    full_span_ns = end_ts - BASE_TS
    narrow_span_ns = NARROW_END - BASE_TS

    def span_to_interval(ns):
        hours = math.ceil(ns / (3600 * 1_000_000_000))
        return f"{hours}h"

    full_interval = span_to_interval(full_span_ns)
    narrow_interval = span_to_interval(narrow_span_ns)

    # REST endpoint is scoped to database — no prefix needed in SQL
    T = "server_metrics"

    def td_range(start, end):
        return f"ts >= {start} AND ts < {end}"

    NR = td_range(BASE_TS, NARROW_END)
    MR = td_range(BASE_TS, MED_END)
    FR = td_range(BASE_TS, end_ts)

    # TDengine tag filters use tags directly in WHERE (like columns)
    suite = [
        (
            "latest: single field",
            {"query": "latest:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": end_ts,
             "aggregationInterval": full_interval},
            f"SELECT LAST(cpu_usage), ts FROM {T} WHERE {FR}",
            20,
        ),
        (
            "avg: narrow, 1 field",
            {"query": "avg:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT AVG(cpu_usage) FROM {T} WHERE {NR}",
            20,
        ),
        (
            "max: narrow, 1 field",
            {"query": "max:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT MAX(cpu_usage) FROM {T} WHERE {NR}",
            20,
        ),
        (
            "sum: narrow, 1 field",
            {"query": "sum:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT SUM(cpu_usage) FROM {T} WHERE {NR}",
            20,
        ),
        (
            "count: narrow, 1 field",
            {"query": "count:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT COUNT(cpu_usage) FROM {T} WHERE {NR}",
            20,
        ),
        (
            "avg: host filter",
            {"query": "avg:server.metrics(cpu_usage){host:host-01}",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT AVG(cpu_usage) FROM {T} WHERE {NR} AND `host` ='host-01'",
            20,
        ),
        (
            "avg: group by host",
            {"query": "avg:server.metrics(cpu_usage) by {{host}}",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT `host`, AVG(cpu_usage) FROM {T} WHERE {NR} PARTITION BY `host`",
            10,
        ),
        (
            "avg: 1h buckets, medium",
            {"query": "avg:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": MED_END,
             "aggregationInterval": "1h"},
            f"SELECT _wstart, AVG(cpu_usage) FROM {T} WHERE {MR} INTERVAL(1h)",
            10,
        ),
        (
            "avg: narrow, all fields",
            {"query": "avg:server.metrics()",
             "startTime": BASE_TS, "endTime": NARROW_END,
             "aggregationInterval": narrow_interval},
            f"SELECT {', '.join(f'AVG({f})' for f in FIELD_NAMES)} FROM {T} WHERE {NR}",
            10,
        ),
        (
            "avg: full range, 1 field",
            {"query": "avg:server.metrics(cpu_usage)",
             "startTime": BASE_TS, "endTime": end_ts,
             "aggregationInterval": full_interval},
            f"SELECT AVG(cpu_usage) FROM {T} WHERE {FR}",
            5,
        ),
        (
            "avg: group by host, full",
            {"query": "avg:server.metrics(cpu_usage) by {{host}}",
             "startTime": BASE_TS, "endTime": end_ts,
             "aggregationInterval": full_interval},
            f"SELECT `host`, AVG(cpu_usage) FROM {T} WHERE {FR} PARTITION BY `host`",
            5,
        ),
        (
            "avg: 5m buckets, tag",
            {"query": "avg:server.metrics(cpu_usage){host:host-05}",
             "startTime": BASE_TS, "endTime": MED_END,
             "aggregationInterval": "5m"},
            f"SELECT _wstart, AVG(cpu_usage) FROM {T} WHERE {MR} AND `host` ='host-05' INTERVAL(5m)",
            10,
        ),
    ]
    return suite


# ── Formatting helpers ───────────────────────────────────────────────

def fmt_pts(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(int(n))


def fmt_ms(ms):
    if ms >= 1000:
        return f"{ms/1000:.2f}s"
    return f"{ms:.1f}ms"


def print_separator(ch="="):
    print(ch * 70)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TimeStar vs TDengine benchmark")
    parser.add_argument("--cpus", type=int, default=4,
                        help="CPU cores for TDengine container (match TimeStar -c)")
    parser.add_argument("--memory", default="8g",
                        help="Memory limit for TDengine container")
    parser.add_argument("--batches", type=int, default=100,
                        help="Number of insert batches")
    parser.add_argument("--batch-size", type=int, default=5000,
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
                        help="Don't manage Docker (TDengine already running)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Remove TDengine container after benchmark")
    args = parser.parse_args()

    total_points = args.batches * args.batch_size * len(FIELD_NAMES)

    print_separator()
    print(" TimeStar vs TDengine — Benchmark")
    print_separator()
    print(f"  CPUs:           {args.cpus}")
    print(f"  Memory:         {args.memory}")
    print(f"  Batches:        {args.batches}")
    print(f"  Batch size:     {args.batch_size} timestamps x {len(FIELD_NAMES)} fields "
          f"= {args.batch_size * len(FIELD_NAMES):,} pts/batch")
    print(f"  Total points:   {total_points:,}")
    print(f"  Hosts/Racks:    {args.hosts}/{args.racks}")
    print(f"  Concurrency:    {args.concurrency} threads")
    print_separator()
    print()

    # ── Start TDengine ───────────────────────────────────────────
    if not args.no_docker:
        print("[1/4] Setting up TDengine …")
        start_tdengine(args.cpus, args.memory)
        print()

    # ── Health checks ────────────────────────────────────────────
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
        body = tdengine_sql("SELECT SERVER_VERSION()")
        assert body is not None
        print(f"  TDengine  @ 127.0.0.1:{TDENGINE_PORT}: OK")
    except Exception as e:
        print(f"  TDengine  @ 127.0.0.1:{TDENGINE_PORT}: FAILED ({e})")
        sys.exit(1)
    print()

    # ── Insert benchmark ─────────────────────────────────────────
    ts_insert = None
    td_insert = None

    if not args.skip_insert:
        print("[3/4] Insert benchmark …")
        print(f"  Generating {args.batches} batches ({fmt_pts(total_points)} points) …")

        # Warmup
        warmup_batches = min(5, max(1, args.batches // 10))
        print(f"  Warming up ({warmup_batches} batches each) …")
        run_insert_timestar(warmup_batches, args.batch_size, args.hosts, args.racks,
                            args.seed + 999, args.concurrency, args.ts_host, args.ts_port)
        run_insert_tdengine(warmup_batches, args.batch_size, args.hosts, args.racks,
                            args.seed + 999, args.concurrency)
        print()

        print("  Running TimeStar insert …")
        ts_insert = run_insert_timestar(
            args.batches, args.batch_size, args.hosts, args.racks,
            args.seed, args.concurrency, args.ts_host, args.ts_port)
        print(f"    {ts_insert.ok}/{args.batches} OK, "
              f"{fmt_pts(ts_insert.throughput)} pts/sec, "
              f"wall={ts_insert.wall_seconds:.2f}s, "
              f"p50={ts_insert.p50_ms:.1f}ms, p99={ts_insert.p99_ms:.1f}ms")
        if ts_insert.first_error:
            print(f"    first error: {ts_insert.first_error[:120]}")

        time.sleep(2)  # let background flushes complete

        print("  Running TDengine insert …")
        td_insert = run_insert_tdengine(
            args.batches, args.batch_size, args.hosts, args.racks,
            args.seed, args.concurrency)
        print(f"    {td_insert.ok}/{args.batches} OK, "
              f"{fmt_pts(td_insert.throughput)} pts/sec, "
              f"wall={td_insert.wall_seconds:.2f}s, "
              f"p50={td_insert.p50_ms:.1f}ms, p99={td_insert.p99_ms:.1f}ms")
        if td_insert.first_error:
            print(f"    first error: {td_insert.first_error[:120]}")
        print()

        # Insert comparison table
        print_separator("-")
        print(" INSERT RESULTS")
        print_separator("-")
        print(f"  {'Metric':<30s}  {'TimeStar':>14s}  {'TDengine':>14s}  {'Ratio':>10s}")
        print(f"  {'-'*30}  {'-'*14}  {'-'*14}  {'-'*10}")

        def ratio_str(ts_v, td_v, higher_better=True):
            if td_v <= 0 or ts_v <= 0:
                return "N/A"
            r = ts_v / td_v if higher_better else td_v / ts_v
            if r > 1:
                return f"TS {r:.1f}x"
            return f"TD {1/r:.1f}x"

        rows = [
            ("Throughput (pts/sec)", ts_insert.throughput, td_insert.throughput, True),
            ("Wall time (s)", ts_insert.wall_seconds, td_insert.wall_seconds, False),
            ("Avg batch latency (ms)", ts_insert.avg_latency_ms, td_insert.avg_latency_ms, False),
            ("p50 latency (ms)", ts_insert.p50_ms, td_insert.p50_ms, False),
            ("p99 latency (ms)", ts_insert.p99_ms, td_insert.p99_ms, False),
        ]
        for label, ts_v, td_v, higher in rows:
            print(f"  {label:<30s}  {ts_v:>14.2f}  {td_v:>14.2f}  {ratio_str(ts_v, td_v, higher):>10s}")
        print()

    # ── Query benchmark ──────────────────────────────────────────
    if not args.skip_query:
        print("[4/4] Query benchmark …")
        suite = build_query_suite(args.batches, args.batch_size)

        # Warmup
        print("  Warming up (3 iterations each) …")
        for name, ts_req, td_sql, iters in suite:
            run_query_timestar(args.ts_host, args.ts_port, name, ts_req, 3)
            run_query_tdengine(name, td_sql, 3)
        print()

        print_separator("-")
        print(" QUERY RESULTS (avg latency per query)")
        print_separator("-")
        print(f"  {'Query':<32s}  {'TimeStar':>10s}  {'TDengine':>10s}  {'Winner':>10s}")
        print(f"  {'-'*32}  {'-'*10}  {'-'*10}  {'-'*10}")

        ts_total_ms = 0
        td_total_ms = 0

        for name, ts_req, td_sql, iters in suite:
            ts_res = run_query_timestar(args.ts_host, args.ts_port, name, ts_req, iters)
            td_res = run_query_tdengine(name, td_sql, iters)

            ts_avg = ts_res.avg_ms
            td_avg = td_res.avg_ms
            ts_total_ms += ts_res.total_ms
            td_total_ms += td_res.total_ms

            # Determine winner
            if ts_res.successes > 0 and td_res.successes > 0 and ts_avg > 0 and td_avg > 0:
                if ts_avg <= td_avg:
                    winner = f"TS {td_avg/ts_avg:.1f}x"
                else:
                    winner = f"TD {ts_avg/td_avg:.1f}x"
            elif ts_res.successes > 0:
                winner = "TS (only)"
            elif td_res.successes > 0:
                winner = "TD (only)"
            else:
                winner = "BOTH FAIL"

            err_suffix = ""
            if td_res.successes == 0 and td_res.error:
                err_suffix = f"  TD:{td_res.error[:40]}"

            print(f"  {name:<32s}  {fmt_ms(ts_avg):>10s}  {fmt_ms(td_avg):>10s}  {winner:>10s}{err_suffix}")

        print()
        print(f"  {'TOTAL':>32s}  {fmt_ms(ts_total_ms):>10s}  {fmt_ms(td_total_ms):>10s}")
        print()

    # ── Summary ──────────────────────────────────────────────────
    print_separator("=")
    print(" SUMMARY")
    print_separator("=")

    if ts_insert and td_insert:
        ts_t, td_t = ts_insert.throughput, td_insert.throughput
        if td_t > 0 and ts_t > 0:
            r = ts_t / td_t
            if r > 1:
                print(f"  Insert:  TimeStar {r:.1f}x faster ({fmt_pts(ts_t)} vs {fmt_pts(td_t)} pts/sec)")
            else:
                print(f"  Insert:  TDengine {1/r:.1f}x faster ({fmt_pts(td_t)} vs {fmt_pts(ts_t)} pts/sec)")

    if not args.skip_query and ts_total_ms > 0 and td_total_ms > 0:
        r = td_total_ms / ts_total_ms
        if r > 1:
            print(f"  Query:   TimeStar {r:.1f}x faster ({fmt_ms(ts_total_ms)} vs {fmt_ms(td_total_ms)})")
        else:
            print(f"  Query:   TDengine {1/r:.1f}x faster ({fmt_ms(td_total_ms)} vs {fmt_ms(ts_total_ms)})")

    print_separator("=")
    print()

    # ── Cleanup ──────────────────────────────────────────────────
    if args.cleanup and not args.no_docker:
        print("Cleaning up TDengine container …")
        stop_tdengine()


if __name__ == "__main__":
    main()
