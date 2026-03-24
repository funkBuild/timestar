#!/usr/bin/env python3
"""
TimeStar Python Benchmark Tool

Generates synthetic time series data and benchmarks insert and query
throughput against a TimeStar server.  Supports both protobuf and JSON
wire formats for A/B comparison.

The data generation mirrors the C++ insert benchmark
(bin/timestar_insert_bench.cpp) so results are directly comparable.

Usage:
    # Protobuf insert benchmark
    python timestar_bench.py --format protobuf --batch-size 10000 --batches 100

    # JSON insert benchmark (for comparison)
    python timestar_bench.py --format json --batch-size 10000 --batches 100

    # Full benchmark (insert + query)
    python timestar_bench.py --format protobuf --batch-size 10000 --batches 100

    # Query-only (assumes data already loaded)
    python timestar_bench.py --query-only --format protobuf

    # Multi-threaded insert
    python timestar_bench.py --connections 4 --batch-size 10000 --batches 200

Compare with C++ benchmark:
    ./bin/timestar_insert_bench -c 4 --batch-size 10000 --batches 100 \\
        --format protobuf --connections 8 --hosts 10 --racks 2
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

import requests

import timestar_pb2

# ---------------------------------------------------------------------------
# Constants matching the C++ benchmark
# ---------------------------------------------------------------------------

MINUTE_NS = 60_000_000_000
SECOND_NS = 1_000_000_000
BASE_TS = 1_000_000_000_000_000_000  # ~2001-09-09

FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature",
]

FIELDS_PER_ROW = len(FIELD_NAMES)

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------


def _seed_rng(seed: int, host_id: int, start_ts: int) -> random.Random:
    """Create a deterministic RNG matching the C++ benchmark's seeding."""
    combined = seed ^ (host_id << 32) ^ start_ts
    return random.Random(combined)


def build_payload_json(seed: int, host_id: int, rack_id: int,
                       start_ts: int, count: int) -> str:
    """Build a JSON payload matching the C++ benchmark format.

    Returns a JSON string with array-format timestamps and fields.
    """
    rng = _seed_rng(seed, host_id, start_ts)

    timestamps = [start_ts + i * MINUTE_NS for i in range(count)]

    fields: dict[str, list[float]] = {}
    for fname in FIELD_NAMES:
        fields[fname] = [rng.uniform(0.0, 100.0) for _ in range(count)]

    payload = {
        "measurement": "server.metrics",
        "tags": {
            "host": f"host-{host_id:02d}",
            "rack": f"rack-{rack_id}",
        },
        "timestamps": timestamps,
        "fields": fields,
    }
    return json.dumps(payload)


def build_payload_protobuf(seed: int, host_id: int, rack_id: int,
                           start_ts: int, count: int) -> bytes:
    """Build a serialized protobuf WriteRequest matching the C++ benchmark.

    Returns serialized bytes ready to POST.
    """
    rng = _seed_rng(seed, host_id, start_ts)

    req = timestar_pb2.WriteRequest()
    wp = req.writes.add()
    wp.measurement = "server.metrics"
    wp.tags["host"] = f"host-{host_id:02d}"
    wp.tags["rack"] = f"rack-{rack_id}"

    for i in range(count):
        wp.timestamps.append(start_ts + i * MINUTE_NS)

    for fname in FIELD_NAMES:
        wf = timestar_pb2.WriteField()
        dv = wf.double_values
        for _ in range(count):
            dv.values.append(rng.uniform(0.0, 100.0))
        wp.fields[fname].CopyFrom(wf)

    return req.SerializeToString()


def build_query_protobuf(query_str: str, start: int, end: int,
                         interval: str = "") -> bytes:
    """Build a serialized QueryRequest protobuf."""
    req = timestar_pb2.QueryRequest()
    req.query = query_str
    req.start_time = start
    req.end_time = end
    if interval:
        req.aggregation_interval = interval
    return req.SerializeToString()


def build_query_json(query_str: str, start: int, end: int,
                     interval: str = "") -> str:
    """Build a JSON query request body."""
    body: dict[str, Any] = {
        "query": query_str,
        "startTime": start,
        "endTime": end,
    }
    if interval:
        body["aggregationInterval"] = interval
    return json.dumps(body)


# ---------------------------------------------------------------------------
# Latency tracking
# ---------------------------------------------------------------------------

@dataclass
class LatencyStats:
    """Collects latency samples and computes percentiles."""
    samples_ms: list[float] = field(default_factory=list)

    def add(self, duration_sec: float) -> None:
        self.samples_ms.append(duration_sec * 1000.0)

    def merge(self, other: LatencyStats) -> None:
        self.samples_ms.extend(other.samples_ms)

    def percentile(self, p: float) -> float:
        if not self.samples_ms:
            return 0.0
        s = sorted(self.samples_ms)
        idx = int(p * (len(s) - 1))
        return s[idx]

    @property
    def count(self) -> int:
        return len(self.samples_ms)

    @property
    def mean(self) -> float:
        return statistics.mean(self.samples_ms) if self.samples_ms else 0.0

    @property
    def min_ms(self) -> float:
        return min(self.samples_ms) if self.samples_ms else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.samples_ms) if self.samples_ms else 0.0

    def print_line(self, label: str) -> None:
        if not self.samples_ms:
            return
        print(f"  {label:<22s}  min={self.min_ms:>8.2f}  avg={self.mean:>8.2f}"
              f"  med={self.percentile(0.50):>8.2f}"
              f"  p95={self.percentile(0.95):>8.2f}"
              f"  p99={self.percentile(0.99):>8.2f}"
              f"  max={self.max_ms:>8.2f}  (ms, n={self.count})")


# ---------------------------------------------------------------------------
# Worker result
# ---------------------------------------------------------------------------

@dataclass
class WorkerResult:
    latency: LatencyStats = field(default_factory=LatencyStats)
    requests_ok: int = 0
    requests_fail: int = 0
    requests_http_err: int = 0
    total_points: int = 0
    wall_time_sec: float = 0.0
    first_error: str = ""
    payload_bytes: int = 0


# ---------------------------------------------------------------------------
# Insert worker
# ---------------------------------------------------------------------------

def insert_worker(worker_id: int, base_url: str, fmt: str,
                  batch_size: int, start_batch: int, end_batch: int,
                  num_hosts: int, num_racks: int, seed: int) -> WorkerResult:
    """Run insert batches [start_batch, end_batch) on a single thread.

    Pre-generates all payloads before timing to isolate server throughput
    from Python serialization cost.
    """
    result = WorkerResult()
    num_batches = end_batch - start_batch
    if num_batches <= 0:
        return result

    # Deterministic host/rack assignment per batch
    rng = random.Random(seed + start_batch)

    # Pre-generate payloads
    payloads: list[bytes | str] = []
    for i in range(num_batches):
        host_id = rng.randint(1, num_hosts)
        rack_id = rng.randint(1, num_racks)
        start_ts = BASE_TS + (start_batch + i) * batch_size * MINUTE_NS
        if fmt == "protobuf":
            payloads.append(build_payload_protobuf(seed, host_id, rack_id,
                                                   start_ts, batch_size))
        else:
            payloads.append(build_payload_json(seed, host_id, rack_id,
                                              start_ts, batch_size))

    result.payload_bytes = sum(
        len(p) if isinstance(p, bytes) else len(p.encode()) for p in payloads
    )

    if fmt == "protobuf":
        content_type = "application/x-protobuf"
    else:
        content_type = "application/json"

    session = requests.Session()
    url = f"{base_url}/write"

    wall_start = time.monotonic()

    for payload in payloads:
        t0 = time.monotonic()
        try:
            resp = session.post(url, data=payload,
                                headers={"Content-Type": content_type},
                                timeout=60)
            if 200 <= resp.status_code < 300:
                result.requests_ok += 1
                result.total_points += batch_size * FIELDS_PER_ROW
            else:
                result.requests_http_err += 1
                if not result.first_error:
                    result.first_error = f"HTTP {resp.status_code}: {resp.text[:256]}"
        except Exception as e:
            result.requests_fail += 1
            if not result.first_error:
                result.first_error = f"Exception: {e}"
        t1 = time.monotonic()
        result.latency.add(t1 - t0)

    result.wall_time_sec = time.monotonic() - wall_start
    session.close()
    return result


# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------

def build_query_suite(fmt: str) -> list[dict[str, Any]]:
    """Build the query benchmark suite matching the C++ benchmark patterns."""
    end_ts = BASE_TS + 100 * 10000 * MINUTE_NS
    narrow_end = BASE_TS + 100 * MINUTE_NS
    med_end = BASE_TS + 10000 * MINUTE_NS

    queries = [
        # Aggregation queries - full range
        {"name": "avg, all fields, full range",
         "query": "avg:server.metrics()", "start": BASE_TS, "end": end_ts,
         "iterations": 10},
        {"name": "max, single field, full",
         "query": "max:server.metrics(cpu_usage)", "start": BASE_TS, "end": end_ts,
         "iterations": 20},
        {"name": "sum, single field, full",
         "query": "sum:server.metrics(memory_usage)", "start": BASE_TS, "end": end_ts,
         "iterations": 20},
        {"name": "count, all fields, full",
         "query": "count:server.metrics()", "start": BASE_TS, "end": end_ts,
         "iterations": 10},
        {"name": "latest, single field, full",
         "query": "latest:server.metrics(cpu_usage)", "start": BASE_TS, "end": end_ts,
         "iterations": 20},

        # Tag-filtered queries
        {"name": "avg, tag filter, full",
         "query": "avg:server.metrics(cpu_usage){host:host-01}",
         "start": BASE_TS, "end": end_ts, "iterations": 20},
        {"name": "avg, two-tag filter",
         "query": "avg:server.metrics(cpu_usage){host:host-01,rack:rack-1}",
         "start": BASE_TS, "end": end_ts, "iterations": 20},

        # Narrow time range
        {"name": "avg, narrow range (100min)",
         "query": "avg:server.metrics(cpu_usage)", "start": BASE_TS, "end": narrow_end,
         "iterations": 50},
        {"name": "latest, narrow range",
         "query": "latest:server.metrics(temperature)", "start": BASE_TS, "end": narrow_end,
         "iterations": 50},

        # Medium time range
        {"name": "avg, medium range (~7d)",
         "query": "avg:server.metrics(cpu_usage)", "start": BASE_TS, "end": med_end,
         "iterations": 20},

        # Group-by queries
        {"name": "avg, group by host",
         "query": "avg:server.metrics(cpu_usage){} by {host}",
         "start": BASE_TS, "end": end_ts, "iterations": 10},
        {"name": "avg, group by rack",
         "query": "avg:server.metrics(cpu_usage){} by {rack}",
         "start": BASE_TS, "end": end_ts, "iterations": 10},

        # Time-bucketed aggregation
        {"name": "avg, 1h buckets, full",
         "query": "avg:server.metrics(cpu_usage)", "start": BASE_TS, "end": end_ts,
         "interval": "1h", "iterations": 10},
        {"name": "avg, 5m buckets, medium",
         "query": "avg:server.metrics(cpu_usage)", "start": BASE_TS, "end": med_end,
         "interval": "5m", "iterations": 10},

        # Metadata queries (these are GET requests, not POST /query)
        {"name": "GET /measurements",
         "metadata": "measurements", "iterations": 50},
        {"name": "GET /tags",
         "metadata": "tags", "params": {"measurement": "server.metrics"},
         "iterations": 50},
        {"name": "GET /fields",
         "metadata": "fields", "params": {"measurement": "server.metrics"},
         "iterations": 50},
    ]
    return queries


def run_single_query(session: requests.Session, base_url: str, fmt: str,
                     qdef: dict[str, Any]) -> tuple[float, bool, str]:
    """Execute a single query and return (elapsed_sec, success, error_msg)."""
    if "metadata" in qdef:
        # GET request for metadata endpoints
        endpoint = qdef["metadata"]
        params = qdef.get("params", {})
        accept = ("application/x-protobuf" if fmt == "protobuf"
                  else "application/json")
        t0 = time.monotonic()
        try:
            resp = session.get(f"{base_url}/{endpoint}",
                               params=params,
                               headers={"Accept": accept},
                               timeout=30)
            elapsed = time.monotonic() - t0
            if 200 <= resp.status_code < 300:
                return elapsed, True, ""
            return elapsed, False, f"HTTP {resp.status_code}"
        except Exception as e:
            return time.monotonic() - t0, False, str(e)

    # POST /query
    query_str = qdef["query"]
    start = qdef["start"]
    end = qdef["end"]
    interval = qdef.get("interval", "")

    if fmt == "protobuf":
        data = build_query_protobuf(query_str, start, end, interval)
        ct = "application/x-protobuf"
    else:
        data = build_query_json(query_str, start, end, interval)
        ct = "application/json"

    t0 = time.monotonic()
    try:
        resp = session.post(f"{base_url}/query",
                            data=data,
                            headers={"Content-Type": ct, "Accept": ct},
                            timeout=60)
        elapsed = time.monotonic() - t0
        if 200 <= resp.status_code < 300:
            return elapsed, True, ""
        return elapsed, False, f"HTTP {resp.status_code}: {resp.text[:128]}"
    except Exception as e:
        return time.monotonic() - t0, False, str(e)


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def run_insert_benchmark(args: argparse.Namespace, base_url: str) -> None:
    """Run the insert benchmark."""
    total_batches = args.warmup + args.batches
    total_points = args.batches * args.batch_size * FIELDS_PER_ROW

    # -- Warmup --
    if args.warmup > 0:
        print(f"Running {args.warmup} warmup batches...")
        warmup_seed = args.seed + 1_000_000_000

        if args.connections == 1:
            insert_worker(0, base_url, args.format, args.batch_size,
                          0, args.warmup, args.hosts, args.racks, warmup_seed)
        else:
            per_conn = math.ceil(args.warmup / args.connections)
            with ThreadPoolExecutor(max_workers=args.connections) as pool:
                futures = []
                for c in range(args.connections):
                    s = c * per_conn
                    e = min(s + per_conn, args.warmup)
                    if s >= e:
                        break
                    futures.append(pool.submit(
                        insert_worker, c, base_url, args.format,
                        args.batch_size, s, e, args.hosts, args.racks,
                        warmup_seed))
                for f in as_completed(futures):
                    f.result()  # Raise on error
        print("Warmup complete.\n")

    # -- Timed run --
    print(f"Running {args.batches} timed batches"
          f" ({args.connections} connection(s))...")

    global_start = time.monotonic()

    if args.connections == 1:
        results = [insert_worker(0, base_url, args.format, args.batch_size,
                                 0, args.batches, args.hosts, args.racks,
                                 args.seed)]
    else:
        per_conn = math.ceil(args.batches / args.connections)
        results = []
        with ThreadPoolExecutor(max_workers=args.connections) as pool:
            futures = []
            for c in range(args.connections):
                s = c * per_conn
                e = min(s + per_conn, args.batches)
                if s >= e:
                    break
                futures.append(pool.submit(
                    insert_worker, c, base_url, args.format,
                    args.batch_size, s, e, args.hosts, args.racks,
                    args.seed))
            for f in as_completed(futures):
                results.append(f.result())

    global_end = time.monotonic()
    wall_sec = global_end - global_start

    # -- Aggregate --
    combined = LatencyStats()
    total_ok = 0
    total_fail = 0
    total_http_err = 0
    total_pts = 0
    total_bytes = 0
    first_error = ""

    for r in results:
        combined.merge(r.latency)
        total_ok += r.requests_ok
        total_fail += r.requests_fail
        total_http_err += r.requests_http_err
        total_pts += r.total_points
        total_bytes += r.payload_bytes
        if not first_error and r.first_error:
            first_error = r.first_error

    # -- Print results --
    print(f"\n{'=' * 70}")
    print(" INSERT RESULTS")
    print(f"{'=' * 70}")
    print()
    print(f"  Requests:       {total_ok} OK, {total_http_err} HTTP errors,"
          f" {total_fail} connection failures")
    if first_error:
        print(f"  First error:    {first_error}")
    print(f"  Points written: {total_pts:,}")
    print(f"  Payload size:   {total_bytes:,} bytes"
          f" ({total_bytes / (1024*1024):.1f} MB)")
    print(f"  Wall time:      {wall_sec:.3f} s")
    print(f"  Throughput:     {total_pts / wall_sec:,.0f} pts/sec")
    print(f"  Batch rate:     {total_ok / wall_sec:.1f} batches/sec")
    print(f"  Data rate:      {total_bytes / wall_sec / (1024*1024):.1f} MB/sec")
    print()
    print("  Latency per batch:")
    combined.print_line("all connections")

    if len(results) > 1:
        print()
        print("  Per-connection throughput:")
        for i, r in enumerate(results):
            if r.requests_ok == 0 and r.requests_fail == 0:
                continue
            sec = r.wall_time_sec
            pts_sec = r.total_points / sec if sec > 0 else 0
            print(f"    conn {i:>2}: {r.total_points:>10,} pts"
                  f" in {sec:.2f}s = {pts_sec:,.0f} pts/sec"
                  f"  ({r.requests_ok} ok, {r.requests_http_err} http_err,"
                  f" {r.requests_fail} fail)")
    print()


def run_query_benchmark(args: argparse.Namespace, base_url: str) -> None:
    """Run the query benchmark suite."""
    print(f"{'=' * 70}")
    print(" QUERY BENCHMARK")
    print(f"{'=' * 70}")
    print()

    queries = build_query_suite(args.format)
    session = requests.Session()

    # Table header
    print(f"  {'Query':<35s}  {'Iters':>5}  {'Avg ms':>8}  {'Med ms':>8}"
          f"  {'p95 ms':>8}  {'p99 ms':>8}  {'OK':>4}  {'Fail':>4}")
    print(f"  {'-' * 35}  {'-' * 5}  {'-' * 8}  {'-' * 8}"
          f"  {'-' * 8}  {'-' * 8}  {'-' * 4}  {'-' * 4}")

    all_latency = LatencyStats()
    total_ok = 0
    total_fail = 0

    for qdef in queries:
        iters = qdef["iterations"]
        stats = LatencyStats()
        ok = 0
        fail = 0
        last_error = ""

        for _ in range(iters):
            elapsed, success, err = run_single_query(session, base_url,
                                                     args.format, qdef)
            stats.add(elapsed)
            if success:
                ok += 1
            else:
                fail += 1
                last_error = err

        all_latency.merge(stats)
        total_ok += ok
        total_fail += fail

        name = qdef["name"][:35]
        print(f"  {name:<35s}  {iters:>5}  {stats.mean:>8.2f}"
              f"  {stats.percentile(0.50):>8.2f}"
              f"  {stats.percentile(0.95):>8.2f}"
              f"  {stats.percentile(0.99):>8.2f}"
              f"  {ok:>4}  {fail:>4}")
        if fail > 0 and last_error:
            print(f"    ^ error: {last_error[:80]}")

    session.close()

    print(f"\n  {'TOTAL':<35s}  {total_ok + total_fail:>5}"
          f"  {all_latency.mean:>8.2f}"
          f"  {all_latency.percentile(0.50):>8.2f}"
          f"  {all_latency.percentile(0.95):>8.2f}"
          f"  {all_latency.percentile(0.99):>8.2f}"
          f"  {total_ok:>4}  {total_fail:>4}")

    total_iters = total_ok + total_fail
    total_sec = sum(all_latency.samples_ms) / 1000.0
    print(f"\n  Total queries:  {total_iters}")
    print(f"  Total time:     {total_sec:.3f} s")
    print(f"  Queries/sec:    {total_iters / total_sec:.1f}" if total_sec > 0
          else "  Queries/sec:    N/A")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TimeStar Python Benchmark Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Protobuf insert benchmark
  python timestar_bench.py --format protobuf --batch-size 10000 --batches 100

  # JSON insert benchmark (for comparison)
  python timestar_bench.py --format json --batch-size 10000 --batches 100

  # Multi-threaded with 4 connections
  python timestar_bench.py --connections 4 --batch-size 10000 --batches 200

  # Query-only benchmark
  python timestar_bench.py --query-only

  # Insert-only benchmark
  python timestar_bench.py --insert-only --batches 50

Compare with C++ benchmark:
  ./bin/timestar_insert_bench -c 4 --batch-size 10000 --batches 100 \\
      --format protobuf --connections 8 --hosts 10 --racks 2
""")

    parser.add_argument("--host", default="localhost",
                        help="TimeStar server host (default: localhost)")
    parser.add_argument("--port", type=int, default=8086,
                        help="TimeStar server port (default: 8086)")
    parser.add_argument("--format", choices=["protobuf", "json"],
                        default="protobuf",
                        help="Wire format (default: protobuf)")
    parser.add_argument("--batch-size", type=int, default=10000,
                        help="Timestamps per batch request (default: 10000)")
    parser.add_argument("--batches", type=int, default=100,
                        help="Number of timed batches (default: 100)")
    parser.add_argument("--warmup", type=int, default=10,
                        help="Number of warmup batches, not timed (default: 10)")
    parser.add_argument("--hosts", type=int, default=10,
                        help="Number of simulated hosts (default: 10)")
    parser.add_argument("--racks", type=int, default=2,
                        help="Number of simulated racks (default: 2)")
    parser.add_argument("--connections", type=int, default=1,
                        help="Concurrent connections/threads (default: 1)")
    parser.add_argument("--seed", type=int, default=42,
                        help="PRNG seed for reproducibility (default: 42)")
    parser.add_argument("--query-only", action="store_true",
                        help="Skip insert, run only queries")
    parser.add_argument("--insert-only", action="store_true",
                        help="Skip queries, run only inserts")

    args = parser.parse_args()
    base_url = f"http://{args.host}:{args.port}"

    # -- Print header --
    print(f"{'=' * 70}")
    print(" TimeStar Python Benchmark")
    print(f"{'=' * 70}")
    print(f"  Server:         {args.host}:{args.port}")
    print(f"  Format:         {args.format}")
    print(f"  Batch size:     {args.batch_size} timestamps"
          f" x {FIELDS_PER_ROW} fields"
          f" = {args.batch_size * FIELDS_PER_ROW:,} pts")
    if not args.query_only:
        print(f"  Batches:        {args.warmup + args.batches}"
              f" ({args.warmup} warmup + {args.batches} timed)")
        print(f"  Total points:   {args.batches * args.batch_size * FIELDS_PER_ROW:,}"
              f" (timed)")
        print(f"  Connections:    {args.connections}")
    print(f"  Hosts/Racks:    {args.hosts} / {args.racks}")
    print(f"  Seed:           {args.seed}")
    print(f"{'=' * 70}")
    print()

    # -- Health check --
    try:
        resp = requests.get(f"{base_url}/health", timeout=5)
        if resp.status_code != 200:
            print(f"ERROR: server health check failed"
                  f" (HTTP {resp.status_code})")
            sys.exit(1)
        print("Server health check: OK\n")
    except requests.ConnectionError:
        print(f"ERROR: cannot connect to {base_url}")
        print("       Start the server first:"
              " ./build/bin/timestar_http_server --port 8086")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: health check failed: {e}")
        sys.exit(1)

    # -- Run benchmarks --
    if not args.query_only:
        run_insert_benchmark(args, base_url)

    if not args.insert_only:
        run_query_benchmark(args, base_url)

    print(f"{'=' * 70}")
    print(" Benchmark complete.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
