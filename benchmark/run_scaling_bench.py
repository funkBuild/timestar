#!/usr/bin/env python3
"""
TimeStar vs InfluxDB 2.7 — Multi-core scaling benchmark.

Fully automated: for each core count it starts fresh instances of all
databases, loads identical data, runs an identical query suite, records
results, and tears everything down before the next round.

Usage:
    python3 benchmark/run_scaling_bench.py
    python3 benchmark/run_scaling_bench.py --cores 1 2 4 8 12 --batches 200
    python3 benchmark/run_scaling_bench.py --runs 3          # 3 runs per config

Results are saved to benchmark/results_<timestamp>.json and a summary
table is printed to stdout.
"""

import argparse
import json
import math
import os
import random
import signal
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Optional

import requests

# ── Paths ──────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
BUILD_DIR = PROJECT_ROOT / "build"
TIMESTAR_BIN = BUILD_DIR / "bin" / "timestar_http_server"

# ── Data schema (matches timestar_insert_bench.cpp) ────────────────────

MEASUREMENT = "server.metrics"
FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature",
]
NUM_FIELDS = len(FIELD_NAMES)
MINUTE_NS = 60_000_000_000
BASE_TS = 1_000_000_000_000_000_000

# ── InfluxDB 2.7 Docker settings ─────────────────────────────────────

INFLUX_CONTAINER = "timestar-scaling-bench-influxdb"
INFLUX_PORT = 8087
INFLUX_ORG = "bench"
INFLUX_BUCKET = "bench"
INFLUX_TOKEN = "benchtoken123456"  # noqa: S105
INFLUX_IMAGE = "influxdb:2.7"

# ── InfluxDB 3 Core Docker settings ──────────────────────────────────

INFLUX3_CONTAINER = "timestar-scaling-bench-influxdb3"
INFLUX3_PORT = 8188
INFLUX3_DB = "bench"
INFLUX3_IMAGE = "influxdb:3-core"

TIMESTAR_PORT = 8086

# ── Data generation ────────────────────────────────────────────────────

def generate_batch(seed, host_id, rack_id, start_ts, count):
    """Return (line_protocol_str, timestar_json_str) for identical data."""
    rng = random.Random(seed ^ (host_id << 32) ^ start_ts)
    host_tag = f"host-{host_id:02d}"
    rack_tag = f"rack-{rack_id}"

    lp_lines = []
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


def pregenerate_payloads(batches, batch_size, num_hosts, num_racks, seed):
    """Pre-generate all payloads for both targets. Returns list of (lp, json)."""
    payloads = []
    rng = random.Random(seed)
    for b in range(batches):
        hid = rng.randint(1, num_hosts)
        rid = rng.randint(1, num_racks)
        start_ts = BASE_TS + b * batch_size * MINUTE_NS
        payloads.append(generate_batch(seed, hid, rid, start_ts, batch_size))
    return payloads

# ── Process management ─────────────────────────────────────────────────

def kill_timestar():
    """Kill any running TimeStar server."""
    subprocess.run(
        ["pkill", "-f", "timestar_http_server"],
        capture_output=True,
    )
    time.sleep(0.5)


def start_timestar(cores, port=TIMESTAR_PORT):
    """Start TimeStar, return Popen handle."""
    kill_timestar()
    # Clean shard directories
    for d in BUILD_DIR.glob("shard_*"):
        subprocess.run(["rm", "-rf", str(d)], capture_output=True)

    log = open("/tmp/timestar_scaling_bench.log", "w")
    proc = subprocess.Popen(
        [str(TIMESTAR_BIN), "-c", str(cores), "--port", str(port)],
        cwd=str(BUILD_DIR),
        stdout=log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    # Wait for health
    for _ in range(30):
        try:
            r = requests.get(f"http://127.0.0.1:{port}/health", timeout=2)
            if r.status_code == 200:
                return proc
        except Exception:
            pass
        time.sleep(0.5)
    proc.kill()
    raise RuntimeError("TimeStar did not start within 15s")


def stop_timestar(proc):
    """Gracefully stop TimeStar."""
    if proc and proc.poll() is None:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=5)


def _clear_port(port):
    """Remove any Docker container bound to the given port."""
    port_check = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"publish={port}"],
        capture_output=True, text=True,
    )
    for cid in port_check.stdout.strip().split():
        if cid:
            subprocess.run(["docker", "rm", "-f", cid], capture_output=True)


def start_influxdb(cores, memory="8g"):
    """Start fresh InfluxDB 2.7 container, removing any previous one."""
    subprocess.run(
        ["docker", "rm", "-f", INFLUX_CONTAINER],
        capture_output=True,
    )
    _clear_port(INFLUX_PORT)
    subprocess.run([
        "docker", "run", "-d",
        "--name", INFLUX_CONTAINER,
        "--cpus", str(cores),
        "--memory", memory,
        "-p", f"{INFLUX_PORT}:8086",
        "-e", "DOCKER_INFLUXDB_INIT_MODE=setup",
        "-e", "DOCKER_INFLUXDB_INIT_USERNAME=admin",
        "-e", "DOCKER_INFLUXDB_INIT_PASSWORD=adminadmin",
        "-e", f"DOCKER_INFLUXDB_INIT_ORG={INFLUX_ORG}",
        "-e", f"DOCKER_INFLUXDB_INIT_BUCKET={INFLUX_BUCKET}",
        "-e", f"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN={INFLUX_TOKEN}",
        INFLUX_IMAGE,
    ], check=True, capture_output=True)

    for _ in range(30):
        try:
            r = requests.get(f"http://127.0.0.1:{INFLUX_PORT}/health", timeout=2)
            if r.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("InfluxDB 2.7 did not start within 30s")


def stop_influxdb():
    subprocess.run(["docker", "rm", "-f", INFLUX_CONTAINER], capture_output=True)


def start_influxdb3(cores, memory="8g"):
    """Start fresh InfluxDB 3 Core container."""
    subprocess.run(
        ["docker", "rm", "-f", INFLUX3_CONTAINER],
        capture_output=True,
    )
    _clear_port(INFLUX3_PORT)
    subprocess.run([
        "docker", "run", "-d",
        "--name", INFLUX3_CONTAINER,
        "--cpus", str(cores),
        "--memory", memory,
        "-p", f"{INFLUX3_PORT}:8181",
        INFLUX3_IMAGE,
        "influxdb3", "serve",
        "--node-id=node0",
        "--object-store=file",
        "--data-dir=/var/lib/influxdb3/data",
        "--without-auth",
    ], check=True, capture_output=True)

    for _ in range(30):
        try:
            r = requests.get(f"http://127.0.0.1:{INFLUX3_PORT}/health", timeout=2)
            if r.ok:
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        raise RuntimeError("InfluxDB 3 Core did not start within 30s")

    # Create the database
    r = requests.post(
        f"http://127.0.0.1:{INFLUX3_PORT}/api/v3/configure/database",
        json={"db": INFLUX3_DB},
        timeout=10,
    )
    if not r.ok:
        raise RuntimeError(f"Failed to create InfluxDB 3 database: {r.text}")


def stop_influxdb3():
    subprocess.run(["docker", "rm", "-f", INFLUX3_CONTAINER], capture_output=True)


# ── Data size measurement ─────────────────────────────────────────────

def measure_data_size_timestar():
    """Return total bytes used by TimeStar shard directories."""
    result = subprocess.run(
        ["du", "-sb", "--total"] + [str(d) for d in sorted(BUILD_DIR.glob("shard_*"))],
        capture_output=True, text=True,
    )
    for line in result.stdout.strip().split("\n"):
        if "total" in line:
            return int(line.split()[0])
    return 0


def measure_data_size_influxdb():
    """Return total bytes used by InfluxDB 2.7 engine directory."""
    result = subprocess.run(
        ["docker", "exec", INFLUX_CONTAINER,
         "du", "-sb", "/var/lib/influxdb2/engine/"],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        return int(result.stdout.strip().split()[0])
    return 0


def measure_data_size_influxdb3():
    """Return total bytes used by InfluxDB 3 data directory."""
    result = subprocess.run(
        ["docker", "exec", INFLUX3_CONTAINER,
         "du", "-sb", "/var/lib/influxdb3/data/"],
        capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        return int(result.stdout.strip().split()[0])
    return 0


# ── Insert benchmark ───────────────────────────────────────────────────

@dataclass
class InsertResult:
    total_points: int = 0
    wall_seconds: float = 0.0
    ok: int = 0
    fail: int = 0
    first_error: str = ""
    latencies_ms: list = field(default_factory=list)

    @property
    def throughput(self):
        return self.total_points / self.wall_seconds if self.wall_seconds > 0 else 0

    @property
    def avg_latency_ms(self):
        return sum(self.latencies_ms) / len(self.latencies_ms) if self.latencies_ms else 0

    def percentile(self, p):
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[min(int(len(s) * p), len(s) - 1)]


def run_insert(target, payloads, batch_size, concurrency):
    """Run insert benchmark against the specified target. Returns InsertResult."""
    result = InsertResult()
    pts_per_batch = batch_size * NUM_FIELDS

    def send_one(idx):
        lp, ts_json = payloads[idx]
        t0 = time.perf_counter()
        try:
            if target == "influxdb":
                r = requests.post(
                    f"http://127.0.0.1:{INFLUX_PORT}/api/v2/write"
                    f"?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns",
                    data=lp,
                    headers={
                        "Authorization": f"Token {INFLUX_TOKEN}",
                        "Content-Type": "text/plain; charset=utf-8",
                    },
                    timeout=120,
                )
                ok = r.status_code == 204
                err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
            elif target == "influxdb3":
                r = requests.post(
                    f"http://127.0.0.1:{INFLUX3_PORT}/api/v3/write_lp"
                    f"?db={INFLUX3_DB}&precision=nanosecond",
                    data=lp,
                    headers={"Content-Type": "text/plain; charset=utf-8"},
                    timeout=120,
                )
                ok = r.status_code == 204
                err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
            else:
                r = requests.post(
                    f"http://127.0.0.1:{TIMESTAR_PORT}/write",
                    data=ts_json,
                    headers={"Content-Type": "application/json"},
                    timeout=120,
                )
                ok = 200 <= r.status_code < 300
                err = "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
        except Exception as e:
            ok = False
            err = str(e)
        return ok, err, (time.perf_counter() - t0) * 1000

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(send_one, i): i for i in range(len(payloads))}
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

NARROW_END = BASE_TS + 100 * MINUTE_NS
MED_END = BASE_TS + 10000 * MINUTE_NS


def span_to_interval(ns):
    hours = math.ceil(ns / (3600 * 1_000_000_000))
    return f"{hours}h"


def ns_to_rfc(ns):
    secs = ns // 1_000_000_000
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(secs))


def build_query_suite(total_batches, batch_size):
    """Returns list of (name, ts_request_dict, flux_query_str, sql_query_str, iterations)."""
    end_ts = BASE_TS + total_batches * batch_size * MINUTE_NS
    full_span = end_ts - BASE_TS
    narrow_span = NARROW_END - BASE_TS

    full_iv = span_to_interval(full_span)
    narrow_iv = span_to_interval(narrow_span)

    n0 = ns_to_rfc(BASE_TS)
    n1 = ns_to_rfc(NARROW_END)
    m1 = ns_to_rfc(MED_END)
    f1 = ns_to_rfc(end_ts)
    M = MEASUREMENT
    B = INFLUX_BUCKET
    # SQL table name needs double-quoting because of the dot
    T = f'"server.metrics"'

    suite = [
        # 1. latest (full range -> single value)
        ("latest: single field",
         {"query": f"latest:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": end_ts,
          "aggregationInterval": full_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{f1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") |> last()',
         f"SELECT cpu_usage, time FROM {T} ORDER BY time DESC LIMIT 1",
         20),
        # 2. avg narrow
        ("avg: narrow, 1 field",
         {"query": f"avg:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": NARROW_END,
          "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") |> mean()',
         f"SELECT avg(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{n1}'",
         20),
        # 3. max narrow
        ("max: narrow, 1 field",
         {"query": f"max:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": NARROW_END,
          "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") |> max()',
         f"SELECT max(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{n1}'",
         20),
        # 4. sum narrow
        ("sum: narrow, 1 field",
         {"query": f"sum:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": NARROW_END,
          "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") |> sum()',
         f"SELECT sum(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{n1}'",
         20),
        # 5. count narrow
        ("count: narrow, 1 field",
         {"query": f"count:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": NARROW_END,
          "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") |> count()',
         f"SELECT count(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{n1}'",
         20),
        # 6. avg + tag filter
        ("avg: host filter",
         {"query": f"avg:{M}(cpu_usage){{host:host-01}}", "startTime": BASE_TS,
          "endTime": NARROW_END, "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage" and r.host=="host-01") |> mean()',
         f"SELECT avg(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{n1}' AND host = 'host-01'",
         20),
        # 7. group by host
        ("avg: group by host",
         {"query": f"avg:{M}(cpu_usage) by {{host}}", "startTime": BASE_TS,
          "endTime": NARROW_END, "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") '
         f'|> group(columns:["host"]) |> mean()',
         f"SELECT host, avg(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{n1}' GROUP BY host",
         10),
        # 8. 1h buckets, medium range
        ("avg: 1h buckets, medium",
         {"query": f"avg:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": MED_END,
          "aggregationInterval": "1h"},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{m1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") '
         f'|> aggregateWindow(every:1h,fn:mean)',
         f"SELECT date_bin('1 hour', time) AS bucket, avg(cpu_usage) FROM {T} "
         f"WHERE time >= '{n0}' AND time < '{m1}' GROUP BY bucket ORDER BY bucket",
         10),
        # 9. all fields narrow
        ("avg: narrow, all fields",
         {"query": f"avg:{M}()", "startTime": BASE_TS, "endTime": NARROW_END,
          "aggregationInterval": narrow_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{n1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}") |> mean()',
         f"SELECT {', '.join(f'avg({f})' for f in FIELD_NAMES)} FROM {T} "
         f"WHERE time >= '{n0}' AND time < '{n1}'",
         10),
        # 10. full range single field
        ("avg: full range, 1 field",
         {"query": f"avg:{M}(cpu_usage)", "startTime": BASE_TS, "endTime": end_ts,
          "aggregationInterval": full_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{f1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") |> mean()',
         f"SELECT avg(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{f1}'",
         5),
        # 11. group by host, full range
        ("avg: group by host, full",
         {"query": f"avg:{M}(cpu_usage) by {{host}}", "startTime": BASE_TS,
          "endTime": end_ts, "aggregationInterval": full_iv},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{f1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage") '
         f'|> group(columns:["host"]) |> mean()',
         f"SELECT host, avg(cpu_usage) FROM {T} WHERE time >= '{n0}' AND time < '{f1}' GROUP BY host",
         5),
        # 12. 5m buckets + tag filter
        ("avg: 5m buckets, tag",
         {"query": f"avg:{M}(cpu_usage){{host:host-05}}", "startTime": BASE_TS,
          "endTime": MED_END, "aggregationInterval": "5m"},
         f'from(bucket:"{B}") |> range(start:{n0},stop:{m1}) '
         f'|> filter(fn:(r)=>r._measurement=="{M}" and r._field=="cpu_usage" and r.host=="host-05") '
         f'|> aggregateWindow(every:5m,fn:mean)',
         f"SELECT date_bin('5 minutes', time) AS bucket, avg(cpu_usage) FROM {T} "
         f"WHERE time >= '{n0}' AND time < '{m1}' AND host = 'host-05' GROUP BY bucket ORDER BY bucket",
         10),
    ]
    return suite


@dataclass
class QueryResult:
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
    def avg_ms(self):
        return self.total_ms / self.successes if self.successes > 0 else 0

    def percentile(self, p):
        if not self.latencies_ms:
            return 0
        s = sorted(self.latencies_ms)
        return s[min(int(len(s) * p), len(s) - 1)]


def _run_query_generic(name, iterations, request_fn):
    """Generic query runner: request_fn() should return (ok, content_bytes)."""
    result = QueryResult(name=name, iterations=iterations)
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            ok, content_bytes, err = request_fn()
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
            if not result.response_bytes:
                result.response_bytes = content_bytes
        elif not result.error:
            result.error = err
    if result.successes == 0:
        result.min_ms = 0
    return result


def run_query_ts(name, ts_req, iterations):
    url = f"http://127.0.0.1:{TIMESTAR_PORT}/query"
    def req():
        r = requests.post(url, json=ts_req, timeout=60)
        ok = 200 <= r.status_code < 300
        return ok, len(r.content), "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
    return _run_query_generic(name, iterations, req)


def run_query_influx(name, flux_query, iterations):
    url = f"http://127.0.0.1:{INFLUX_PORT}/api/v2/query?org={INFLUX_ORG}"
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type": "application/vnd.flux",
        "Accept": "application/csv",
    }
    def req():
        r = requests.post(url, data=flux_query, headers=headers, timeout=60)
        ok = 200 <= r.status_code < 300
        return ok, len(r.content), "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
    return _run_query_generic(name, iterations, req)


def run_query_influx3(name, sql_query, iterations):
    url = f"http://127.0.0.1:{INFLUX3_PORT}/api/v3/query_sql"
    def req():
        r = requests.post(url, json={"db": INFLUX3_DB, "q": sql_query, "format": "json"},
                          timeout=60)
        ok = 200 <= r.status_code < 300
        return ok, len(r.content), "" if ok else f"HTTP {r.status_code}: {r.text[:200]}"
    return _run_query_generic(name, iterations, req)


# ── Per-config round ───────────────────────────────────────────────────

DB_LABELS = {"ts": "TimeStar", "ix": "InfluxDB 2.7", "ix3": "InfluxDB 3"}


@dataclass
class RoundResult:
    cores: int = 0
    ts_insert: dict = field(default_factory=dict)
    ix_insert: dict = field(default_factory=dict)
    ix3_insert: dict = field(default_factory=dict)
    ts_queries: list = field(default_factory=list)
    ix_queries: list = field(default_factory=list)
    ix3_queries: list = field(default_factory=list)
    ts_query_total_ms: float = 0.0
    ix_query_total_ms: float = 0.0
    ix3_query_total_ms: float = 0.0
    ts_data_bytes: int = 0
    ix_data_bytes: int = 0
    ix3_data_bytes: int = 0


def _insert_dict(ins):
    return {
        "throughput": ins.throughput,
        "wall_seconds": ins.wall_seconds,
        "ok": ins.ok, "fail": ins.fail,
        "avg_ms": ins.avg_latency_ms,
        "p50_ms": ins.percentile(0.50),
        "p95_ms": ins.percentile(0.95),
        "p99_ms": ins.percentile(0.99),
    }


def _query_dict(qr):
    return {
        "name": qr.name, "avg_ms": qr.avg_ms,
        "p50_ms": qr.percentile(0.50),
        "p95_ms": qr.percentile(0.95),
        "min_ms": qr.min_ms if qr.successes else 0,
        "max_ms": qr.max_ms,
        "ok": qr.successes,
        "resp_bytes": qr.response_bytes,
    }


def run_round(cores, batches, batch_size, num_hosts, num_racks,
              seed, concurrency, warmup_batches, memory, no_influx3=False):
    """Run a full insert+query round for a given core count."""
    total_pts = batches * batch_size * NUM_FIELDS
    print(f"\n{'=' * 70}")
    print(f"  CORES = {cores}  |  {total_pts:,} points  "
          f"({batches} batches x {batch_size} ts x {NUM_FIELDS} fields)")
    print(f"{'=' * 70}")

    result = RoundResult(cores=cores)

    # ── Start databases ─────────────────────────────────────────
    print(f"  Starting InfluxDB 2.7 (cpus={cores}, mem={memory}) ...")
    start_influxdb(cores, memory)
    if not no_influx3:
        print(f"  Starting InfluxDB 3 Core (cpus={cores}, mem={memory}) ...")
        start_influxdb3(cores, memory)
    print(f"  Starting TimeStar (cores={cores}) ...")
    ts_proc = start_timestar(cores)
    print(f"  All databases ready.\n")

    # ── Pre-generate payloads ───────────────────────────────────
    print(f"  Pre-generating {batches} payloads ...")
    payloads = pregenerate_payloads(batches, batch_size, num_hosts, num_racks, seed)
    warmup_payloads = pregenerate_payloads(
        warmup_batches, batch_size, num_hosts, num_racks, seed + 999)

    # ── Warmup inserts ──────────────────────────────────────────
    print(f"  Warmup: {warmup_batches} batches each ...")
    run_insert("timestar", warmup_payloads, batch_size, concurrency)
    run_insert("influxdb", warmup_payloads, batch_size, concurrency)
    if not no_influx3:
        run_insert("influxdb3", warmup_payloads, batch_size, concurrency)

    # ── Timed inserts ───────────────────────────────────────────
    insert_targets = [("timestar", "TimeStar"), ("influxdb", "InfluxDB 2.7")]
    if not no_influx3:
        insert_targets.append(("influxdb3", "InfluxDB 3"))
    for target, label in insert_targets:
        print(f"  INSERT: {label} ...", end=" ", flush=True)
        ins = run_insert(target, payloads, batch_size, concurrency)
        print(f"{ins.throughput/1e6:.2f}M pts/sec  "
              f"({ins.ok}/{batches} ok, {ins.wall_seconds:.2f}s)")
        if ins.first_error:
            print(f"    error: {ins.first_error[:100]}")
        if target == "timestar":
            result.ts_insert = _insert_dict(ins)
        elif target == "influxdb":
            result.ix_insert = _insert_dict(ins)
        else:
            result.ix3_insert = _insert_dict(ins)
        # Brief pause for background flushes
        time.sleep(3)

    # ── Measure data sizes ──────────────────────────────────────
    # Allow extra time for compaction / Parquet conversion
    time.sleep(5)
    result.ts_data_bytes = measure_data_size_timestar()
    result.ix_data_bytes = measure_data_size_influxdb()
    if not no_influx3:
        result.ix3_data_bytes = measure_data_size_influxdb3()
    size_msg = (f"\n  DATA SIZE: TS={result.ts_data_bytes/1e6:.0f}MB  "
                f"IX2={result.ix_data_bytes/1e6:.0f}MB")
    if not no_influx3:
        size_msg += f"  IX3={result.ix3_data_bytes/1e6:.0f}MB"
    print(size_msg)

    # ── Queries ─────────────────────────────────────────────────
    suite = build_query_suite(batches, batch_size)

    # Warmup queries
    print(f"\n  QUERY warmup (3 iters each) ...", flush=True)
    for name, ts_req, flux_q, sql_q, iters in suite:
        run_query_ts(name, ts_req, 3)
        run_query_influx(name, flux_q, 3)
        if not no_influx3:
            run_query_influx3(name, sql_q, 3)

    # Timed queries
    print(f"  QUERY benchmark ({len(suite)} queries):")
    ts_total = ix_total = ix3_total = 0.0

    for name, ts_req, flux_q, sql_q, iters in suite:
        ts_qr = run_query_ts(name, ts_req, iters)
        ix_qr = run_query_influx(name, flux_q, iters)
        ix3_qr = None if no_influx3 else run_query_influx3(name, sql_q, iters)

        ts_avg = ts_qr.avg_ms
        ix_avg = ix_qr.avg_ms
        ix3_avg = ix3_qr.avg_ms if ix3_qr else 0.0
        ts_total += ts_qr.total_ms
        ix_total += ix_qr.total_ms
        if ix3_qr:
            ix3_total += ix3_qr.total_ms

        line = f"    {name:<30s}  TS={ts_avg:>8.2f}  IX2={ix_avg:>8.2f}"
        if ix3_qr:
            line += f"  IX3={ix3_avg:>8.2f}"
        print(line + " ms")

        result.ts_queries.append(_query_dict(ts_qr))
        result.ix_queries.append(_query_dict(ix_qr))
        result.ix3_queries.append(_query_dict(ix3_qr) if ix3_qr else {})

    result.ts_query_total_ms = ts_total
    result.ix_query_total_ms = ix_total
    result.ix3_query_total_ms = ix3_total

    totals_msg = f"\n  QUERY totals:  TS={ts_total:.1f}ms  IX2={ix_total:.1f}ms"
    if not no_influx3:
        totals_msg += f"  IX3={ix3_total:.1f}ms"
    print(totals_msg)

    # ── Teardown ────────────────────────────────────────────────
    print(f"  Stopping databases ...", end=" ", flush=True)
    stop_timestar(ts_proc)
    stop_influxdb()
    if not no_influx3:
        stop_influxdb3()
    print("done.")

    return result


# ── Formatting ─────────────────────────────────────────────────────────

def fmt_pts(n):
    if n >= 1e6:
        return f"{n/1e6:.2f}M"
    if n >= 1e3:
        return f"{n/1e3:.1f}K"
    return f"{n:.0f}"


def fmt_bytes(n):
    if n >= 1e9:
        return f"{n/1e9:.2f} GB"
    if n >= 1e6:
        return f"{n/1e6:.0f} MB"
    if n >= 1e3:
        return f"{n/1e3:.0f} KB"
    return f"{n} B"


def ratio_str(ts_val, ix_val, higher_is_better=True):
    """Return 'TS Nx' or 'IX Nx' string."""
    if ts_val == 0 and ix_val == 0:
        return "N/A"
    if higher_is_better:
        if ix_val == 0:
            return "TS inf"
        r = ts_val / ix_val
        return f"TS {r:.1f}x" if r >= 1 else f"IX {1/r:.1f}x"
    else:
        if ts_val == 0:
            return "TS inf"
        r = ix_val / ts_val
        return f"TS {r:.1f}x" if r >= 1 else f"IX {1/r:.1f}x"


def print_summary(results, batches, batch_size):
    """Print consolidated tables from all rounds."""
    total_pts = batches * batch_size * NUM_FIELDS
    cores_list = [r.cores for r in results]
    has_ix3 = bool(results[0].ix3_insert)

    label = "TimeStar vs InfluxDB 2.7" + (" vs InfluxDB 3 Core" if has_ix3 else "")
    print(f"\n\n{'=' * 100}")
    print(f" CONSOLIDATED RESULTS — {label}")
    print(f" {total_pts:,} points per round  "
          f"({batches} batches x {batch_size} ts x {NUM_FIELDS} fields)")
    print(f"{'=' * 100}")

    # ── Insert throughput table ──────────────────────────────────
    print(f"\n INSERT THROUGHPUT (pts/sec)")
    hdr = f" {'Cores':>5s}  {'TimeStar':>14s}  {'InfluxDB 2.7':>14s}"
    sep = f" {'-'*5}  {'-'*14}  {'-'*14}"
    if has_ix3:
        hdr += f"  {'InfluxDB 3':>14s}  {'TS/IX2':>10s}  {'TS/IX3':>10s}"
        sep += f"  {'-'*14}  {'-'*10}  {'-'*10}"
    else:
        hdr += f"  {'TS/IX2':>10s}"
        sep += f"  {'-'*10}"
    print(hdr)
    print(sep)
    for r in results:
        ts_t = r.ts_insert.get("throughput", 0)
        ix_t = r.ix_insert.get("throughput", 0)
        r_ix = f"{ts_t/ix_t:.1f}x" if ix_t > 0 else "N/A"
        row = f" {r.cores:>5d}  {fmt_pts(ts_t):>14s}  {fmt_pts(ix_t):>14s}"
        if has_ix3:
            ix3_t = r.ix3_insert.get("throughput", 0)
            r_ix3 = f"{ts_t/ix3_t:.1f}x" if ix3_t > 0 else "N/A"
            row += f"  {fmt_pts(ix3_t):>14s}  {r_ix:>10s}  {r_ix3:>10s}"
        else:
            row += f"  {r_ix:>10s}"
        print(row)

    # ── Data size table ──────────────────────────────────────────
    print(f"\n DATA SIZE ON DISK")
    hdr = f" {'Cores':>5s}  {'TimeStar':>14s}  {'InfluxDB 2.7':>14s}"
    sep = f" {'-'*5}  {'-'*14}  {'-'*14}"
    if has_ix3:
        hdr += f"  {'InfluxDB 3':>14s}  {'IX2/TS':>10s}  {'IX3/TS':>10s}"
        sep += f"  {'-'*14}  {'-'*10}  {'-'*10}"
    else:
        hdr += f"  {'IX2/TS':>10s}"
        sep += f"  {'-'*10}"
    print(hdr)
    print(sep)
    for r in results:
        ts_b = r.ts_data_bytes
        ix_b = r.ix_data_bytes
        r_ix = f"{ix_b/ts_b:.1f}x" if ts_b > 0 else "N/A"
        row = f" {r.cores:>5d}  {fmt_bytes(ts_b):>14s}  {fmt_bytes(ix_b):>14s}"
        if has_ix3:
            ix3_b = r.ix3_data_bytes
            r_ix3 = f"{ix3_b/ts_b:.1f}x" if ts_b > 0 else "N/A"
            row += f"  {fmt_bytes(ix3_b):>14s}  {r_ix:>10s}  {r_ix3:>10s}"
        else:
            row += f"  {r_ix:>10s}"
        print(row)

    # ── Query latency table (avg ms per query, per core count) ──
    if results[0].ts_queries:
        query_names = [q["name"] for q in results[0].ts_queries]

        print(f"\n QUERY LATENCY — avg ms")
        header = f" {'Query':<30s}"
        for c in cores_list:
            if has_ix3:
                header += f"  {'TS':>6s} {'IX2':>6s} {'IX3':>6s}"
            else:
                header += f"  {'TS':>6s} {'IX2':>6s}"
        print(header)
        header2 = f" {'':30s}"
        for c in cores_list:
            w = 18 if has_ix3 else 12
            header2 += "  " + ("--- " + str(c) + "c ").rjust(w, "-")
        print(header2)

        for qi, qname in enumerate(query_names):
            row = f" {qname:<30s}"
            for r in results:
                ts_ms = r.ts_queries[qi]["avg_ms"]
                ix_ms = r.ix_queries[qi]["avg_ms"]
                row += f"  {ts_ms:>6.2f} {ix_ms:>6.2f}"
                if has_ix3:
                    ix3_ms = r.ix3_queries[qi].get("avg_ms", 0)
                    row += f" {ix3_ms:>6.2f}"
            print(row)

        # Total query time row
        row = f" {'TOTAL (ms)':>30s}"
        for r in results:
            row += f"  {r.ts_query_total_ms:>6.0f} {r.ix_query_total_ms:>6.0f}"
            if has_ix3:
                row += f" {r.ix3_query_total_ms:>6.0f}"
        print(row)

        # ── Query winner summary ────────────────────────────────
        print(f"\n QUERY SPEEDUP vs InfluxDB (>1 = TS faster)")
        header = f" {'Query':<30s}"
        for c in cores_list:
            header += f"  {'IX2':>8s}"
            if has_ix3:
                header += f" {'IX3':>8s}"
        print(header)
        header2 = f" {'':30s}"
        for c in cores_list:
            w = 16 if has_ix3 else 8
            header2 += "  " + ("---- " + str(c) + "c ").rjust(w, "-")
        print(header2)

        for qi, qname in enumerate(query_names):
            row = f" {qname:<30s}"
            for r in results:
                ts_ms = r.ts_queries[qi]["avg_ms"]
                ix_ms = r.ix_queries[qi]["avg_ms"]
                if ts_ms > 0 and ix_ms > 0:
                    ratio = ix_ms / ts_ms
                    row += f"  {ratio:>7.1f}x"
                else:
                    row += f"  {'N/A':>8s}"
                if has_ix3:
                    ix3_ms = r.ix3_queries[qi].get("avg_ms", 0)
                    if ts_ms > 0 and ix3_ms > 0:
                        ratio = ix3_ms / ts_ms
                        row += f" {ratio:>7.1f}x"
                    else:
                        row += f" {'N/A':>8s}"
            print(row)

        # Overall query ratio
        row = f" {'OVERALL':>30s}"
        for r in results:
            t = r.ts_query_total_ms
            i2 = r.ix_query_total_ms
            r2 = f"{i2/t:.1f}x" if t > 0 else "N/A"
            row += f"  {r2:>8s}"
            if has_ix3:
                i3 = r.ix3_query_total_ms
                r3 = f"{i3/t:.1f}x" if t > 0 else "N/A"
                row += f" {r3:>8s}"
        print(row)

    print(f"\n{'=' * 100}\n")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="TimeStar vs InfluxDB 2.7 — multi-core scaling benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            Example:
              python3 benchmark/run_scaling_bench.py --cores 1 2 4 8 12
              python3 benchmark/run_scaling_bench.py --cores 4 --batches 200 --runs 3
        """),
    )
    parser.add_argument("--cores", type=int, nargs="+", default=[1, 2, 4, 8, 12],
                        help="Core counts to benchmark (default: 1 2 4 8 12)")
    parser.add_argument("--batches", type=int, default=100,
                        help="Insert batches per round (default: 100)")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Timestamps per batch (default: 10000)")
    parser.add_argument("--hosts", type=int, default=10,
                        help="Simulated hosts (default: 10)")
    parser.add_argument("--racks", type=int, default=2,
                        help="Simulated racks (default: 2)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="HTTP client threads (default: 8)")
    parser.add_argument("--seed", type=int, default=42,
                        help="PRNG seed (default: 42)")
    parser.add_argument("--warmup-batches", type=int, default=5,
                        help="Warmup batches before timed insert (default: 5)")
    parser.add_argument("--memory", default="8g",
                        help="Docker memory limit for InfluxDB (default: 8g)")
    parser.add_argument("--runs", type=int, default=1,
                        help="Runs per core count; results are averaged (default: 1)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSON path (default: benchmark/results_<ts>.json)")
    parser.add_argument("--no-influx3", action="store_true", default=False,
                        help="Skip InfluxDB 3 Core benchmarks (only run TimeStar vs InfluxDB 2.7)")
    args = parser.parse_args()

    # Validate
    if not TIMESTAR_BIN.exists():
        print(f"ERROR: TimeStar binary not found at {TIMESTAR_BIN}")
        print(f"  Build first: cd build && cmake .. && make -j$(nproc)")
        sys.exit(1)

    r = subprocess.run(["docker", "info"], capture_output=True)
    if r.returncode != 0:
        print("ERROR: Docker is not available")
        sys.exit(1)

    # Ensure images are available
    images_to_pull = [INFLUX_IMAGE]
    if not args.no_influx3:
        images_to_pull.append(INFLUX3_IMAGE)
    for img in images_to_pull:
        print(f"Pulling {img} ...")
        subprocess.run(["docker", "pull", img], capture_output=True, check=True)
    print("Images ready.\n")

    total_pts = args.batches * args.batch_size * NUM_FIELDS
    print(f"{'=' * 70}")
    bench_label = "TimeStar vs InfluxDB 2.7" + ("" if args.no_influx3 else " vs InfluxDB 3 Core")
    print(f" {bench_label} — Scaling Benchmark")
    print(f"{'=' * 70}")
    print(f"  Cores:        {args.cores}")
    print(f"  Runs/config:  {args.runs}")
    print(f"  Batches:      {args.batches} x {args.batch_size} ts x {NUM_FIELDS} fields "
          f"= {total_pts:,} pts/round")
    print(f"  Hosts/Racks:  {args.hosts}/{args.racks}")
    print(f"  Concurrency:  {args.concurrency} threads")
    print(f"  Memory:       {args.memory}")
    print(f"{'=' * 70}")

    all_results = []
    raw_data = {"args": vars(args), "rounds": []}

    for cores in args.cores:
        if args.runs == 1:
            rr = run_round(
                cores, args.batches, args.batch_size,
                args.hosts, args.racks, args.seed,
                args.concurrency, args.warmup_batches, args.memory,
                no_influx3=args.no_influx3,
            )
            all_results.append(rr)
            raw_data["rounds"].append(asdict(rr))
        else:
            # Multiple runs: collect all, then average
            runs = []
            for run_idx in range(args.runs):
                print(f"\n  --- Run {run_idx+1}/{args.runs} for {cores} cores ---")
                rr = run_round(
                    cores, args.batches, args.batch_size,
                    args.hosts, args.racks, args.seed + run_idx,
                    args.concurrency, args.warmup_batches, args.memory,
                    no_influx3=args.no_influx3,
                )
                runs.append(rr)
                raw_data["rounds"].append(asdict(rr))

            # Average the runs
            avg = RoundResult(cores=cores)
            n = len(runs)

            # Average insert metrics
            for key in runs[0].ts_insert:
                if isinstance(runs[0].ts_insert[key], (int, float)):
                    avg.ts_insert[key] = sum(r.ts_insert[key] for r in runs) / n
                    avg.ix_insert[key] = sum(r.ix_insert[key] for r in runs) / n
                    if not args.no_influx3:
                        avg.ix3_insert[key] = sum(r.ix3_insert[key] for r in runs) / n

            # Average query metrics
            for qi in range(len(runs[0].ts_queries)):
                ts_q = {}
                ix_q = {}
                ix3_q = {}
                for key in runs[0].ts_queries[qi]:
                    if isinstance(runs[0].ts_queries[qi][key], (int, float)):
                        ts_q[key] = sum(r.ts_queries[qi][key] for r in runs) / n
                        ix_q[key] = sum(r.ix_queries[qi][key] for r in runs) / n
                        if not args.no_influx3 and runs[0].ix3_queries[qi]:
                            ix3_q[key] = sum(r.ix3_queries[qi].get(key, 0) for r in runs) / n
                    else:
                        ts_q[key] = runs[0].ts_queries[qi][key]
                        ix_q[key] = runs[0].ix_queries[qi][key]
                        if not args.no_influx3 and runs[0].ix3_queries[qi]:
                            ix3_q[key] = runs[0].ix3_queries[qi][key]
                avg.ts_queries.append(ts_q)
                avg.ix_queries.append(ix_q)
                avg.ix3_queries.append(ix3_q)

            avg.ts_query_total_ms = sum(r.ts_query_total_ms for r in runs) / n
            avg.ix_query_total_ms = sum(r.ix_query_total_ms for r in runs) / n
            if not args.no_influx3:
                avg.ix3_query_total_ms = sum(r.ix3_query_total_ms for r in runs) / n

            avg.ts_data_bytes = sum(r.ts_data_bytes for r in runs) // n
            avg.ix_data_bytes = sum(r.ix_data_bytes for r in runs) // n
            if not args.no_influx3:
                avg.ix3_data_bytes = sum(r.ix3_data_bytes for r in runs) // n

            all_results.append(avg)

    # ── Save raw results ────────────────────────────────────────
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = args.output or str(SCRIPT_DIR / f"results_{ts}.json")
    with open(out_path, "w") as f:
        json.dump(raw_data, f, indent=2, default=str)
    print(f"\nRaw results saved to {out_path}")

    # ── Print consolidated summary ──────────────────────────────
    print_summary(all_results, args.batches, args.batch_size)


if __name__ == "__main__":
    main()
