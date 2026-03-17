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

# ── TimescaleDB Docker management ────────────────────────────────────

TSDB_CONTAINER = "timestar-bench-timescaledb"
TSDB_PORT = 5433  # avoid collision with local postgres
TSDB_USER = "bench"
TSDB_PASS = "benchpass"  # noqa: S105
TSDB_DB = "bench"
TSDB_IMAGE = "timescale/timescaledb:latest-pg17"

# ── QuestDB Docker management ────────────────────────────────────────

QUEST_CONTAINER = "timestar-bench-questdb"
QUEST_HTTP_PORT = 9000
QUEST_ILP_PORT = 9009
QUEST_IMAGE = "questdb/questdb:latest"


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


def docker_tsdb_running() -> bool:
    r = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", TSDB_CONTAINER],
        capture_output=True, text=True,
    )
    return r.returncode == 0 and "true" in r.stdout


def start_timescaledb(cpus: int, memory: str):
    """Start TimescaleDB in Docker with resource limits matching TimeStar."""
    if docker_tsdb_running():
        print(f"  TimescaleDB container '{TSDB_CONTAINER}' already running.")
        return

    subprocess.run(["docker", "rm", "-f", TSDB_CONTAINER], capture_output=True)

    print(f"  Pulling {TSDB_IMAGE} …")
    subprocess.run(["docker", "pull", TSDB_IMAGE], capture_output=True, check=True)

    print(f"  Starting TimescaleDB (cpus={cpus}, memory={memory}) …")
    subprocess.run([
        "docker", "run", "-d",
        "--name", TSDB_CONTAINER,
        "--cpus", str(cpus),
        "--memory", memory,
        "-p", f"{TSDB_PORT}:5432",
        "-e", f"POSTGRES_USER={TSDB_USER}",
        "-e", f"POSTGRES_PASSWORD={TSDB_PASS}",
        "-e", f"POSTGRES_DB={TSDB_DB}",
        TSDB_IMAGE,
    ], check=True, capture_output=True)

    import psycopg2

    print("  Waiting for TimescaleDB to be ready …")
    for i in range(30):
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=TSDB_PORT,
                user=TSDB_USER, password=TSDB_PASS, dbname=TSDB_DB,
                connect_timeout=2,
            )
            conn.close()
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("TimescaleDB did not become ready in 30s")

    # Create hypertable
    conn = psycopg2.connect(
        host="127.0.0.1", port=TSDB_PORT,
        user=TSDB_USER, password=TSDB_PASS, dbname=TSDB_DB,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS server_metrics (
            time  TIMESTAMPTZ NOT NULL,
            host  TEXT NOT NULL,
            rack  TEXT NOT NULL,
            cpu_usage        DOUBLE PRECISION,
            memory_usage     DOUBLE PRECISION,
            disk_io_read     DOUBLE PRECISION,
            disk_io_write    DOUBLE PRECISION,
            network_in       DOUBLE PRECISION,
            network_out      DOUBLE PRECISION,
            load_avg_1m      DOUBLE PRECISION,
            load_avg_5m      DOUBLE PRECISION,
            load_avg_15m     DOUBLE PRECISION,
            temperature      DOUBLE PRECISION
        );
    """)
    try:
        cur.execute(
            "SELECT create_hypertable('server_metrics', 'time', "
            "chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);"
        )
    except Exception:
        pass  # already a hypertable
    cur.close()
    conn.close()
    print("  TimescaleDB ready (hypertable created).")


def stop_timescaledb():
    subprocess.run(["docker", "rm", "-f", TSDB_CONTAINER], capture_output=True)


def docker_quest_running() -> bool:
    r = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", QUEST_CONTAINER],
        capture_output=True, text=True,
    )
    return r.returncode == 0 and "true" in r.stdout


def start_questdb(cpus: int, memory: str):
    """Start QuestDB in Docker with resource limits matching TimeStar."""
    if docker_quest_running():
        print(f"  QuestDB container '{QUEST_CONTAINER}' already running.")
        return

    subprocess.run(["docker", "rm", "-f", QUEST_CONTAINER], capture_output=True)

    print(f"  Pulling {QUEST_IMAGE} …")
    subprocess.run(["docker", "pull", QUEST_IMAGE], capture_output=True, check=True)

    print(f"  Starting QuestDB (cpus={cpus}, memory={memory}) …")
    subprocess.run([
        "docker", "run", "-d",
        "--name", QUEST_CONTAINER,
        "--cpus", str(cpus),
        "--memory", memory,
        "-p", f"{QUEST_HTTP_PORT}:9000",
        "-p", f"{QUEST_ILP_PORT}:9009",
        "-e", "QDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL=1000",
        QUEST_IMAGE,
    ], check=True, capture_output=True)

    print("  Waiting for QuestDB to be ready …")
    for i in range(30):
        try:
            r = requests.get(
                f"http://127.0.0.1:{QUEST_HTTP_PORT}/exec",
                params={"query": "SELECT 1;"},
                timeout=2,
            )
            if r.status_code == 200:
                print("  QuestDB ready.")
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("QuestDB did not become ready in 30s")


def stop_questdb():
    subprocess.run(["docker", "rm", "-f", QUEST_CONTAINER], capture_output=True)


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


def run_insert_bench_timescaledb(batches: int, batch_size: int,
                                  num_hosts: int, num_racks: int,
                                  seed: int, concurrency: int) -> InsertResult:
    """Run insert benchmark against TimescaleDB using COPY for maximum throughput."""
    import psycopg2
    from io import StringIO

    result = InsertResult()
    pts_per_batch = batch_size * len(FIELD_NAMES)

    # Pre-generate all payloads as TSV for COPY
    rng = random.Random(seed)
    payloads = []
    for b in range(batches):
        hid = rng.randint(1, num_hosts)
        rid = rng.randint(1, num_racks)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        data_rng = random.Random(seed ^ (hid << 32) ^ start_ts)
        host_tag = f"host-{hid:02d}"
        rack_tag = f"rack-{rid}"
        buf = StringIO()
        for i in range(batch_size):
            ts_ns = start_ts + i * MINUTE_NS
            ts_s = ts_ns / 1_000_000_000
            # Convert to ISO timestamp for TimescaleDB
            ts_str = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime(ts_s))
            vals = [round(data_rng.uniform(0.0, 100.0), 6) for _ in FIELD_NAMES]
            buf.write(f"{ts_str}\t{host_tag}\t{rack_tag}\t")
            buf.write("\t".join(str(v) for v in vals))
            buf.write("\n")
        payloads.append(buf.getvalue())

    def send_batch(idx):
        t0 = time.perf_counter()
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=TSDB_PORT,
                user=TSDB_USER, password=TSDB_PASS, dbname=TSDB_DB,
            )
            cur = conn.cursor()
            buf = StringIO(payloads[idx])
            cur.copy_from(buf, "server_metrics", columns=[
                "time", "host", "rack",
                "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
                "network_in", "network_out", "load_avg_1m", "load_avg_5m",
                "load_avg_15m", "temperature",
            ])
            conn.commit()
            cur.close()
            conn.close()
            ok = True
            err = ""
        except Exception as e:
            ok = False
            err = str(e)[:200]
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return ok, err, elapsed_ms

    wall_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(send_batch, i): i for i in range(batches)}
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


def run_query_bench_timescaledb(name: str, sql: str,
                                 iterations: int) -> QueryBenchResult:
    """Run a query benchmark against TimescaleDB."""
    import psycopg2
    result = QueryBenchResult(name=name, iterations=iterations)
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=TSDB_PORT,
                user=TSDB_USER, password=TSDB_PASS, dbname=TSDB_DB,
            )
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            ok = True
        except Exception as e:
            result.error = str(e)[:200]
            ok = False
            rows = []
        elapsed = (time.perf_counter() - t0) * 1000
        if ok:
            result.successes += 1
            result.total_ms += elapsed
            result.min_ms = min(result.min_ms, elapsed)
            result.max_ms = max(result.max_ms, elapsed)
            result.latencies_ms.append(elapsed)
            if result.response_bytes == 0:
                result.response_bytes = len(str(rows))
    if result.successes == 0:
        result.min_ms = 0
    return result


def run_insert_bench_questdb(batches: int, batch_size: int,
                             num_hosts: int, num_racks: int,
                             seed: int, concurrency: int) -> InsertResult:
    """Run insert benchmark against QuestDB using InfluxDB Line Protocol over HTTP."""
    result = InsertResult()
    pts_per_batch = batch_size * len(FIELD_NAMES)

    # Pre-generate all payloads as Line Protocol (reuse same generator as InfluxDB)
    rng = random.Random(seed)
    payloads = []
    for b in range(batches):
        hid = rng.randint(1, num_hosts)
        rid = rng.randint(1, num_racks)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        lp, _ = generate_batch(seed, hid, rid, start_ts, batch_size)
        payloads.append(lp)

    def send_one(idx):
        t0 = time.perf_counter()
        try:
            r = requests.post(
                f"http://127.0.0.1:{QUEST_HTTP_PORT}/write",
                data=payloads[idx],
                headers={"Content-Type": "text/plain; charset=utf-8"},
                timeout=120,
            )
            ok = r.status_code in (200, 204)
            err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            ok = False
            err = str(e)[:200]
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

    # QuestDB needs time to commit WAL data before queries work.
    # Wait until row count stabilizes (WAL fully applied).
    print("    Waiting for QuestDB WAL commit …")
    for _ in range(60):
        try:
            r = requests.get(
                f"http://127.0.0.1:{QUEST_HTTP_PORT}/exec",
                params={"query": f'SELECT count() FROM "server.metrics"'},
                timeout=5,
            )
            if r.status_code == 200:
                cnt = r.json().get("dataset", [[0]])[0][0]
                if cnt >= result.total_points // len(FIELD_NAMES):
                    print(f"    QuestDB committed {cnt:,} rows.")
                    break
        except Exception:
            pass
        time.sleep(2)
    else:
        print("    Warning: QuestDB WAL may not be fully committed.")
    return result


def run_query_bench_questdb(name: str, sql: str,
                             iterations: int) -> QueryBenchResult:
    """Run a query benchmark against QuestDB via HTTP /exec endpoint."""
    result = QueryBenchResult(name=name, iterations=iterations)
    url = f"http://127.0.0.1:{QUEST_HTTP_PORT}/exec"
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            r = requests.get(url, params={"query": sql}, timeout=60)
            ok = 200 <= r.status_code < 300
            if ok:
                body = r.json()
                if "error" in body:
                    ok = False
                    if not result.error:
                        result.error = body["error"][:200]
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
            if result.response_bytes == 0:
                result.response_bytes = len(r.content)
        else:
            if not result.error:
                result.error = f"HTTP {r.status_code}: {r.text[:200]}"
    if result.successes == 0:
        result.min_ms = 0
    return result


# ── Query benchmark ────────────────────────────────────────────────────

# Time ranges matching the insert benchmark data
NARROW_END = BASE_TS + 100 * MINUTE_NS
MED_END = BASE_TS + 10000 * MINUTE_NS


def build_query_suite(total_batches: int, batch_size: int):
    """
    Returns list of (name, timestar_request, influxdb_flux_query,
                      timescaledb_sql, questdb_sql, iterations).

    All databases must do equivalent work per query.
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

    # SQL timestamps for TimescaleDB (same format as RFC3339 but with quotes)
    def ns_to_sql(ns):
        secs = ns // 1_000_000_000
        return time.strftime("'%Y-%m-%d %H:%M:%S+00'", time.gmtime(secs))

    sql_start = ns_to_sql(BASE_TS)
    sql_narrow_end = ns_to_sql(NARROW_END)
    sql_med_end = ns_to_sql(MED_END)
    sql_full_end = ns_to_sql(end_ts)
    T = "server_metrics"  # table name shorthand for TimescaleDB
    Q = '"server.metrics"'  # QuestDB table (auto-created from ILP, needs quoting)

    suite = []

    # Helper: range clause for TimescaleDB SQL
    def sql_range(start, end):
        return f"time >= {start}::timestamptz AND time < {end}::timestamptz"

    NR = sql_range(sql_start, sql_narrow_end)   # narrow
    MR = sql_range(sql_start, sql_med_end)       # medium
    FR = sql_range(sql_start, sql_full_end)      # full

    # Helper: range clause for QuestDB SQL (uses ISO strings directly)
    def q_range(start_ns, end_ns):
        def ns_to_iso(ns):
            secs = ns // 1_000_000_000
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(secs))
        return f"timestamp >= '{ns_to_iso(start_ns)}' AND timestamp < '{ns_to_iso(end_ns)}'"

    QNR = q_range(BASE_TS, NARROW_END)
    QMR = q_range(BASE_TS, MED_END)
    QFR = q_range(BASE_TS, end_ts)

    # ── 1. Latest value ──────────────────────────────────────────
    suite.append((
        "latest: single field",
        {"query": "latest:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {full_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> last()',
        f"SELECT cpu_usage, time FROM {T} WHERE {FR} ORDER BY time DESC LIMIT 1",
        f"SELECT cpu_usage, timestamp FROM {Q} WHERE {QFR} ORDER BY timestamp DESC LIMIT 1",
        20,
    ))

    # ── 2. Avg narrow range ──────────────────────────────────────
    suite.append((
        "avg: narrow, 1 field",
        {"query": "avg:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> mean()',
        f"SELECT AVG(cpu_usage) FROM {T} WHERE {NR}",
        f"SELECT avg(cpu_usage) FROM {Q} WHERE {QNR}",
        20,
    ))

    # ── 3. Max narrow range ──────────────────────────────────────
    suite.append((
        "max: narrow, 1 field",
        {"query": "max:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> max()',
        f"SELECT MAX(cpu_usage) FROM {T} WHERE {NR}",
        f"SELECT max(cpu_usage) FROM {Q} WHERE {QNR}",
        20,
    ))

    # ── 4. Sum narrow range ──────────────────────────────────────
    suite.append((
        "sum: narrow, 1 field",
        {"query": "sum:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> sum()',
        f"SELECT SUM(cpu_usage) FROM {T} WHERE {NR}",
        f"SELECT sum(cpu_usage) FROM {Q} WHERE {QNR}",
        20,
    ))

    # ── 5. Count narrow range ────────────────────────────────────
    suite.append((
        "count: narrow, 1 field",
        {"query": "count:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> count()',
        f"SELECT COUNT(cpu_usage) FROM {T} WHERE {NR}",
        f"SELECT count() FROM {Q} WHERE {QNR}",
        20,
    ))

    # ── 6. Avg with tag filter ───────────────────────────────────
    suite.append((
        "avg: host filter",
        {"query": "avg:server.metrics(cpu_usage){host:host-01}",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage" and r.host == "host-01") '
        f'|> mean()',
        f"SELECT AVG(cpu_usage) FROM {T} WHERE {NR} AND host = 'host-01'",
        f"SELECT avg(cpu_usage) FROM {Q} WHERE {QNR} AND host = 'host-01'",
        20,
    ))

    # ── 7. Group by host ─────────────────────────────────────────
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
        f"SELECT host, AVG(cpu_usage) FROM {T} WHERE {NR} GROUP BY host",
        f"SELECT host, avg(cpu_usage) FROM {Q} WHERE {QNR} GROUP BY host",
        10,
    ))

    # ── 8. Time-bucketed aggregation (1h buckets, medium range) ──
    suite.append((
        "avg: 1h buckets, medium",
        {"query": "avg:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": MED_END,
         "aggregationInterval": "1h"},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {med_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> aggregateWindow(every: 1h, fn: mean)',
        f"SELECT time_bucket('1 hour', time) AS bucket, AVG(cpu_usage) FROM {T} WHERE {MR} GROUP BY bucket ORDER BY bucket",
        f"SELECT timestamp, avg(cpu_usage) FROM {Q} WHERE {QMR} SAMPLE BY 1h ALIGN TO CALENDAR",
        10,
    ))

    # ── 9. All fields, narrow range ──────────────────────────────
    suite.append((
        "avg: narrow, all fields",
        {"query": "avg:server.metrics()",
         "startTime": BASE_TS, "endTime": NARROW_END,
         "aggregationInterval": narrow_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {narrow_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}") '
        f'|> mean()',
        f"SELECT AVG(cpu_usage), AVG(memory_usage), AVG(disk_io_read), AVG(disk_io_write), "
        f"AVG(network_in), AVG(network_out), AVG(load_avg_1m), AVG(load_avg_5m), "
        f"AVG(load_avg_15m), AVG(temperature) FROM {T} WHERE {NR}",
        f"SELECT avg(cpu_usage), avg(memory_usage), avg(disk_io_read), avg(disk_io_write), "
        f"avg(network_in), avg(network_out), avg(load_avg_1m), avg(load_avg_5m), "
        f"avg(load_avg_15m), avg(temperature) FROM {Q} WHERE {QNR}",
        10,
    ))

    # ── 10. Full range, single field ─────────────────────────────
    suite.append((
        "avg: full range, 1 field",
        {"query": "avg:server.metrics(cpu_usage)",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {full_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> mean()',
        f"SELECT AVG(cpu_usage) FROM {T} WHERE {FR}",
        f"SELECT avg(cpu_usage) FROM {Q} WHERE {QFR}",
        5,
    ))

    # ── 11. Group by host, full range ────────────────────────────
    suite.append((
        "avg: group by host, full",
        {"query": "avg:server.metrics(cpu_usage) by {host}",
         "startTime": BASE_TS, "endTime": end_ts,
         "aggregationInterval": full_interval},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {full_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage") '
        f'|> group(columns: ["host"]) '
        f'|> mean()',
        f"SELECT host, AVG(cpu_usage) FROM {T} WHERE {FR} GROUP BY host",
        f"SELECT host, avg(cpu_usage) FROM {Q} WHERE {QFR} GROUP BY host",
        5,
    ))

    # ── 12. 5-min buckets, tag filter ────────────────────────────
    suite.append((
        "avg: 5m buckets, tag",
        {"query": "avg:server.metrics(cpu_usage){host:host-05}",
         "startTime": BASE_TS, "endTime": MED_END,
         "aggregationInterval": "5m"},
        f'from(bucket:"{INFLUX_BUCKET}") '
        f'|> range(start: {narrow_start_rfc}, stop: {med_end_rfc}) '
        f'|> filter(fn:(r) => r._measurement == "{MEASUREMENT}" and r._field == "cpu_usage" and r.host == "host-05") '
        f'|> aggregateWindow(every: 5m, fn: mean)',
        f"SELECT time_bucket('5 minutes', time) AS bucket, AVG(cpu_usage) FROM {T} WHERE {MR} AND host = 'host-05' GROUP BY bucket ORDER BY bucket",
        f"SELECT timestamp, avg(cpu_usage) FROM {Q} WHERE {QMR} AND host = 'host-05' SAMPLE BY 5m ALIGN TO CALENDAR",
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
                        help="Don't manage Docker (InfluxDB/TimescaleDB already running)")
    parser.add_argument("--cleanup", action="store_true",
                        help="Remove containers after benchmark")
    parser.add_argument("--skip-timescale", action="store_true",
                        help="Skip TimescaleDB benchmark")
    parser.add_argument("--skip-influx", action="store_true",
                        help="Skip InfluxDB benchmark")
    parser.add_argument("--skip-quest", action="store_true",
                        help="Skip QuestDB benchmark")
    args = parser.parse_args()

    total_points = args.batches * args.batch_size * len(FIELD_NAMES)

    print_separator()
    print(" TimeStar vs InfluxDB 2.7 vs TimescaleDB — Benchmark")
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

    # ── Start databases ──────────────────────────────────────────
    if not args.no_docker:
        print("[1/5] Setting up databases …")
        if not args.skip_influx:
            start_influxdb(args.cpus, args.memory)
        if not args.skip_timescale:
            start_timescaledb(args.cpus, args.memory)
        if not args.skip_quest:
            start_questdb(args.cpus, args.memory)
        print()

    # ── Verify targets are reachable ─────────────────────────────
    print("[2/5] Health checks …")
    try:
        r = requests.get(f"http://{args.ts_host}:{args.ts_port}/health", timeout=5)
        assert r.status_code == 200
        print(f"  TimeStar    @ {args.ts_host}:{args.ts_port}: OK")
    except Exception as e:
        print(f"  TimeStar    @ {args.ts_host}:{args.ts_port}: FAILED ({e})")
        print("  Start TimeStar first:  cd build && ./bin/timestar_http_server -c 4")
        sys.exit(1)

    if not args.skip_influx:
        try:
            r = requests.get(f"http://127.0.0.1:{INFLUX_PORT}/health", timeout=5)
            assert r.status_code == 200
            print(f"  InfluxDB    @ 127.0.0.1:{INFLUX_PORT}: OK")
        except Exception as e:
            print(f"  InfluxDB    @ 127.0.0.1:{INFLUX_PORT}: FAILED ({e})")
            sys.exit(1)

    if not args.skip_timescale:
        import psycopg2
        try:
            conn = psycopg2.connect(
                host="127.0.0.1", port=TSDB_PORT,
                user=TSDB_USER, password=TSDB_PASS, dbname=TSDB_DB,
                connect_timeout=5,
            )
            conn.close()
            print(f"  TimescaleDB @ 127.0.0.1:{TSDB_PORT}: OK")
        except Exception as e:
            print(f"  TimescaleDB @ 127.0.0.1:{TSDB_PORT}: FAILED ({e})")
            sys.exit(1)

    if not args.skip_quest:
        try:
            r = requests.get(f"http://127.0.0.1:{QUEST_HTTP_PORT}/exec",
                             params={"query": "SELECT 1;"}, timeout=5)
            assert r.status_code == 200
            print(f"  QuestDB     @ 127.0.0.1:{QUEST_HTTP_PORT}: OK")
        except Exception as e:
            print(f"  QuestDB     @ 127.0.0.1:{QUEST_HTTP_PORT}: FAILED ({e})")
            sys.exit(1)
    print()

    # ── Insert benchmark ────────────────────────────────────────
    ts_insert = None
    influx_insert = None
    tsdb_insert = None
    quest_insert = None

    if not args.skip_insert:
        print("[3/5] Insert benchmark …")
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

        # Wait for TimeStar background flushes (WAL→TSM conversions + compaction)
        print("  Waiting 30s for TimeStar background flushes …")
        time.sleep(30)

        if not args.skip_influx:
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

        if not args.skip_timescale:
            print("  Running TimescaleDB insert …")
            tsdb_insert = run_insert_bench_timescaledb(
                args.batches, args.batch_size,
                args.hosts, args.racks, args.seed, args.concurrency)
            print(f"    {tsdb_insert.ok}/{args.batches} OK, "
                  f"{fmt_pts(tsdb_insert.throughput)} pts/sec, "
                  f"wall={tsdb_insert.wall_seconds:.2f}s")
            if tsdb_insert.first_error:
                print(f"    first error: {tsdb_insert.first_error[:120]}")

        if not args.skip_quest:
            print("  Running QuestDB insert …")
            quest_insert = run_insert_bench_questdb(
                args.batches, args.batch_size,
                args.hosts, args.racks, args.seed, args.concurrency)
            print(f"    {quest_insert.ok}/{args.batches} OK, "
                  f"{fmt_pts(quest_insert.throughput)} pts/sec, "
                  f"wall={quest_insert.wall_seconds:.2f}s")
            if quest_insert.first_error:
                print(f"    first error: {quest_insert.first_error[:120]}")

        print()

        # ── Insert comparison table ─────────────────────────────
        print_separator("-")
        print(" INSERT RESULTS")
        print_separator("-")
        header = f"  {'Metric':<30s}  {'TimeStar':>12s}"
        if influx_insert:
            header += f"  {'InfluxDB':>12s}"
        if tsdb_insert:
            header += f"  {'TimescaleDB':>12s}"
        if quest_insert:
            header += f"  {'QuestDB':>12s}"
        print(header)
        print(f"  {'-'*30}  {'-'*12}" +
              (f"  {'-'*12}" if influx_insert else "") +
              (f"  {'-'*12}" if tsdb_insert else "") +
              (f"  {'-'*12}" if quest_insert else ""))

        def print_insert_row(label, ts_v, ix_v, tsdb_v, unit="", higher_better=False):
            row = f"  {label:<30s}  {ts_v:>12.2f}{unit}"
            if ix_v is not None:
                row += f"  {ix_v:>12.2f}{unit}"
            if tsdb_v is not None:
                row += f"  {tsdb_v:>12.2f}{unit}"
            print(row)

        def get_vals(*sources):
            """Build list of values for display, None for missing sources."""
            return [s if s is not None else None for s in sources]

        row_data = [
            ("Throughput (pts/sec)", "", [
                ts_insert.throughput,
                influx_insert.throughput if influx_insert else None,
                tsdb_insert.throughput if tsdb_insert else None,
                quest_insert.throughput if quest_insert else None,
            ]),
            ("Wall time", "s", [
                ts_insert.wall_seconds,
                influx_insert.wall_seconds if influx_insert else None,
                tsdb_insert.wall_seconds if tsdb_insert else None,
                quest_insert.wall_seconds if quest_insert else None,
            ]),
            ("Avg batch latency", "ms", [
                ts_insert.avg_latency_ms,
                influx_insert.avg_latency_ms if influx_insert else None,
                tsdb_insert.avg_latency_ms if tsdb_insert else None,
                quest_insert.avg_latency_ms if quest_insert else None,
            ]),
        ]
        for label, unit, vals in row_data:
            row = f"  {label:<30s}"
            for v in vals:
                if v is not None:
                    row += f"  {v:>12.2f}{unit}"
            print(row)
        print()

    # ── Query benchmark ─────────────────────────────────────────
    if not args.skip_query:
        print("[4/5] Query benchmark …")
        print("  Waiting 30s for all databases to settle (compaction/WAL flush) …")
        time.sleep(30)

        suite = build_query_suite(args.batches, args.batch_size)

        # Warmup queries
        print("  Warming up (3 iterations each) …")
        for name, ts_req, flux_q, tsdb_sql, quest_sql, iters in suite:
            run_query_bench_timestar(args.ts_host, args.ts_port, name, ts_req, 3)
            if not args.skip_influx:
                run_query_bench_influxdb(INFLUX_PORT, name, flux_q, 3)
            if not args.skip_timescale:
                run_query_bench_timescaledb(name, tsdb_sql, 3)
            if not args.skip_quest:
                run_query_bench_questdb(name, quest_sql, 3)

        print()
        print_separator("-")
        print(" QUERY RESULTS")
        print_separator("-")
        header = f"  {'Query':<32s}  {'TimeStar':>10s}"
        if not args.skip_influx:
            header += f"  {'InfluxDB':>10s}"
        if not args.skip_timescale:
            header += f"  {'Timescale':>10s}"
        if not args.skip_quest:
            header += f"  {'QuestDB':>10s}"
        header += f"  {'Best':>10s}"
        print(header)
        sep = f"  {'-'*32}  {'-'*10}"
        if not args.skip_influx:
            sep += f"  {'-'*10}"
        if not args.skip_timescale:
            sep += f"  {'-'*10}"
        if not args.skip_quest:
            sep += f"  {'-'*10}"
        sep += f"  {'-'*10}"
        print(sep)

        ts_total_ms = 0
        influx_total_ms = 0
        tsdb_total_ms = 0
        quest_total_ms = 0

        for name, ts_req, flux_q, tsdb_sql, quest_sql, iters in suite:
            ts_res = run_query_bench_timestar(
                args.ts_host, args.ts_port, name, ts_req, iters)
            ts_avg = ts_res.avg_ms
            ts_total_ms += ts_res.total_ms

            influx_res = None
            influx_avg = 0
            if not args.skip_influx:
                influx_res = run_query_bench_influxdb(
                    INFLUX_PORT, name, flux_q, iters)
                influx_avg = influx_res.avg_ms
                influx_total_ms += influx_res.total_ms

            tsdb_res = None
            tsdb_avg = 0
            if not args.skip_timescale:
                tsdb_res = run_query_bench_timescaledb(name, tsdb_sql, iters)
                tsdb_avg = tsdb_res.avg_ms
                tsdb_total_ms += tsdb_res.total_ms

            quest_res = None
            quest_avg = 0
            if not args.skip_quest:
                quest_res = run_query_bench_questdb(name, quest_sql, iters)
                quest_avg = quest_res.avg_ms
                quest_total_ms += quest_res.total_ms

            # Determine winner
            candidates = [("TimeStar", ts_avg, ts_res.successes > 0)]
            if influx_res:
                candidates.append(("InfluxDB", influx_avg, influx_res.successes > 0))
            if tsdb_res:
                candidates.append(("Timescale", tsdb_avg, tsdb_res.successes > 0))
            if quest_res:
                candidates.append(("QuestDB", quest_avg, quest_res.successes > 0))

            valid = [(n, v) for n, v, ok in candidates if ok and v > 0]
            if valid:
                best_name, best_val = min(valid, key=lambda x: x[1])
                if best_name == "TimeStar":
                    others = [v for n, v in valid if n != "TimeStar"]
                    if others:
                        ratio = min(others) / best_val if best_val > 0 else 0
                        winner = f"TS {ratio:.1f}x"
                    else:
                        winner = "TimeStar"
                else:
                    ratio = ts_avg / best_val if best_val > 0 else 0
                    winner = f"{best_name} {ratio:.1f}x"
            else:
                winner = "N/A"

            row = f"  {name:<32s}  {fmt_ms(ts_avg):>10s}"
            if not args.skip_influx:
                row += f"  {fmt_ms(influx_avg):>10s}"
            if not args.skip_timescale:
                row += f"  {fmt_ms(tsdb_avg):>10s}"
            if not args.skip_quest:
                row += f"  {fmt_ms(quest_avg):>10s}"
            row += f"  {winner:>10s}"
            # Show first error for any failed database
            errs = []
            if influx_res and influx_res.successes == 0 and influx_res.error:
                errs.append(f"IX:{influx_res.error[:40]}")
            if tsdb_res and tsdb_res.successes == 0 and tsdb_res.error:
                errs.append(f"TS:{tsdb_res.error[:40]}")
            if quest_res and quest_res.successes == 0 and quest_res.error:
                errs.append(f"Q:{quest_res.error[:50]}")
            if errs:
                row += f"  {' | '.join(errs)}"
            print(row)

        print()
        total_row = f"  {'TOTAL':>32s}  {fmt_ms(ts_total_ms):>10s}"
        if not args.skip_influx:
            total_row += f"  {fmt_ms(influx_total_ms):>10s}"
        if not args.skip_timescale:
            total_row += f"  {fmt_ms(tsdb_total_ms):>10s}"
        if not args.skip_quest:
            total_row += f"  {fmt_ms(quest_total_ms):>10s}"
        print(total_row)
        print()

    # ── Final summary ───────────────────────────────────────────
    print_separator("=")
    print(" SUMMARY")
    print_separator("=")

    def _ratio_str(ts_v, other_v, other_name):
        if other_v <= 0 or ts_v <= 0:
            return ""
        r = ts_v / other_v
        if r > 1:
            return f"TimeStar {r:.1f}x faster than {other_name}"
        return f"{other_name} {1/r:.1f}x faster than TimeStar"

    if ts_insert:
        ts_tput = ts_insert.throughput
        if influx_insert and influx_insert.throughput > 0:
            print(f"  Insert vs InfluxDB:    {_ratio_str(ts_tput, influx_insert.throughput, 'InfluxDB')}")
        if tsdb_insert and tsdb_insert.throughput > 0:
            print(f"  Insert vs TimescaleDB: {_ratio_str(ts_tput, tsdb_insert.throughput, 'TimescaleDB')}")
        if quest_insert and quest_insert.throughput > 0:
            print(f"  Insert vs QuestDB:     {_ratio_str(ts_tput, quest_insert.throughput, 'QuestDB')}")
    if not args.skip_query:
        if ts_total_ms > 0 and influx_total_ms > 0:
            r = influx_total_ms / ts_total_ms
            print(f"  Query vs InfluxDB:     TimeStar {r:.1f}x faster ({fmt_ms(ts_total_ms)} vs {fmt_ms(influx_total_ms)})")
        if ts_total_ms > 0 and tsdb_total_ms > 0:
            r = tsdb_total_ms / ts_total_ms
            print(f"  Query vs TimescaleDB:  TimeStar {r:.1f}x faster ({fmt_ms(ts_total_ms)} vs {fmt_ms(tsdb_total_ms)})")
        if ts_total_ms > 0 and quest_total_ms > 0:
            r = quest_total_ms / ts_total_ms
            print(f"  Query vs QuestDB:      TimeStar {r:.1f}x faster ({fmt_ms(ts_total_ms)} vs {fmt_ms(quest_total_ms)})")

    print_separator("=")
    print()

    # ── Cleanup ─────────────────────────────────────────────────
    if args.cleanup and not args.no_docker:
        print("Cleaning up containers …")
        if not args.skip_influx:
            stop_influxdb()
        if not args.skip_timescale:
            stop_timescaledb()
        if not args.skip_quest:
            stop_questdb()


if __name__ == "__main__":
    main()
