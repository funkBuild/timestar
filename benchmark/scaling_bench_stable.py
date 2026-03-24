#!/usr/bin/env python3
"""
Scaling benchmark with compaction-aware query scheduling.
Inserts all data, waits for compaction to fully complete, THEN runs queries.
"""

import glob
import json
import math
import os
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "test_api", "python"))
import timestar_pb2 as pb

# ── Constants ────────────────────────────────────────────────────────────
MEASUREMENT = "server.metrics"
FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature",
]
MINUTE_NS = 60_000_000_000
BASE_TS = 1_000_000_000_000_000_000
SEED = 42

HOST = os.environ.get("TIMESTAR_HOST", "127.0.0.1")
PORT = int(os.environ.get("TIMESTAR_PORT", "8086"))
BUILD_DIR = os.environ.get("TIMESTAR_BUILD_DIR",
    os.path.join(os.path.dirname(__file__), "..", "build-release"))
CONCURRENCY = 8
BATCH_SIZE = 10_000
POINTS_PER_BATCH = BATCH_SIZE * len(FIELD_NAMES)
BATCHES_PER_ITER = 1000  # 100M pts
NUM_HOSTS = 10
NUM_RACKS = 2
TARGET_TOTAL = 1_000_000_000


def generate_batch_proto(seed, host_id, rack_id, start_ts, count):
    rng = random.Random(seed ^ (host_id << 32) ^ start_ts)
    req = pb.WriteRequest()
    point = req.writes.add()
    point.measurement = MEASUREMENT
    point.tags["host"] = f"host-{host_id:02d}"
    point.tags["rack"] = f"rack-{rack_id}"
    for i in range(count):
        point.timestamps.append(start_ts + i * MINUTE_NS)
    for fname in FIELD_NAMES:
        wf = pb.WriteField()
        da = pb.DoubleArray()
        for i in range(count):
            da.values.append(round(rng.uniform(0.0, 100.0), 6))
        wf.double_values.CopyFrom(da)
        point.fields[fname].CopyFrom(wf)
    return req.SerializeToString()


def run_insert(iteration, batches):
    batch_offset = iteration * BATCHES_PER_ITER
    rng = random.Random(SEED + batch_offset)
    payloads = []
    for b in range(batches):
        hid = rng.randint(1, NUM_HOSTS)
        rid = rng.randint(1, NUM_RACKS)
        start_ts = BASE_TS + (batch_offset + b) * BATCH_SIZE * MINUTE_NS
        payloads.append(generate_batch_proto(SEED, hid, rid, start_ts, BATCH_SIZE))

    tls = threading.local()
    def get_sess():
        if not hasattr(tls, "s"):
            tls.s = requests.Session()
        return tls.s

    ok = 0
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        def do_write(idx):
            return get_sess().post(
                f"http://{HOST}:{PORT}/write", data=payloads[idx],
                headers={"Content-Type": "application/x-protobuf"}, timeout=120)
        futs = {pool.submit(do_write, i): i for i in range(batches)}
        for f in as_completed(futs):
            r = f.result()
            if 200 <= r.status_code < 300:
                ok += 1
    wall = time.perf_counter() - t0
    return ok * POINTS_PER_BATCH, wall


def count_tsm_files():
    """Count total TSM files across all shards."""
    total = 0
    for shard_dir in sorted(glob.glob(os.path.join(BUILD_DIR, "shard_*"))):
        tsm_dir = os.path.join(shard_dir, "tsm")
        if os.path.isdir(tsm_dir):
            total += len([f for f in os.listdir(tsm_dir) if f.endswith(".tsm")])
    return total


def get_tsm_file_details():
    """Get per-shard TSM file counts and total size."""
    details = {}
    total_size = 0
    for shard_dir in sorted(glob.glob(os.path.join(BUILD_DIR, "shard_*"))):
        shard_name = os.path.basename(shard_dir)
        tsm_dir = os.path.join(shard_dir, "tsm")
        if os.path.isdir(tsm_dir):
            files = [f for f in os.listdir(tsm_dir) if f.endswith(".tsm")]
            size = sum(os.path.getsize(os.path.join(tsm_dir, f)) for f in files)
            details[shard_name] = {"count": len(files), "size_mb": size / (1024*1024)}
            total_size += size
    return details, total_size / (1024*1024)


def wait_for_compaction(label="", max_wait=300):
    """Wait until TSM file count stabilizes (compaction complete)."""
    print(f"  Waiting for compaction to complete{' (' + label + ')' if label else ''}...")
    prev_count = count_tsm_files()
    stable_ticks = 0
    required_stable = 5  # 5 consecutive stable readings = done
    poll_interval = 10   # seconds between checks

    for i in range(max_wait // poll_interval):
        time.sleep(poll_interval)
        curr_count = count_tsm_files()
        details, total_mb = get_tsm_file_details()

        elapsed = (i + 1) * poll_interval
        shard_summary = ", ".join(f"{k}:{v['count']}" for k, v in sorted(details.items()))

        if curr_count == prev_count:
            stable_ticks += 1
            print(f"    [{elapsed:>3d}s] {curr_count} TSM files ({total_mb:.0f} MB) "
                  f"[stable {stable_ticks}/{required_stable}] ({shard_summary})")
            if stable_ticks >= required_stable:
                print(f"  Compaction complete — {curr_count} TSM files, {total_mb:.0f} MB total")
                return True
        else:
            stable_ticks = 0
            delta = curr_count - prev_count
            direction = "compacted" if delta < 0 else "added"
            print(f"    [{elapsed:>3d}s] {curr_count} TSM files ({total_mb:.0f} MB) "
                  f"[{direction} {abs(delta)}] ({shard_summary})")
            prev_count = curr_count

    print(f"  WARNING: compaction did not stabilize in {max_wait}s")
    return False


def run_query(sess, query_str, start_ts, end_ts, interval=None, iters=10):
    qr = pb.QueryRequest()
    qr.query = query_str
    qr.start_time = start_ts
    qr.end_time = end_ts
    if interval:
        qr.aggregation_interval = interval
    data = qr.SerializeToString()
    hdrs = {"Content-Type": "application/x-protobuf", "Accept": "application/x-protobuf"}

    lats = []
    resp_size = 0
    for _ in range(iters):
        try:
            t0 = time.perf_counter()
            r = sess.post(f"http://{HOST}:{PORT}/query", data=data, headers=hdrs, timeout=300)
            lats.append((time.perf_counter() - t0) * 1000)
            if resp_size == 0:
                resp_size = len(r.content)
        except Exception:
            if not lats:
                lats.append(300_000.0)
            continue
    if not lats:
        return 0.0, 0.0, 0.0, 0
    lats.sort()
    avg = sum(lats) / len(lats)
    p95 = lats[int(len(lats) * 0.95)] if len(lats) >= 2 else lats[-1]
    return avg, lats[0], p95, resp_size


def run_query_suite(cumulative_points, end_ts):
    sess = requests.Session()
    # Warmup
    qr = pb.QueryRequest()
    qr.query = "latest:server.metrics(cpu_usage)"
    qr.start_time = BASE_TS
    qr.end_time = end_ts
    qr.aggregation_interval = "9999h"
    for _ in range(3):
        sess.post(f"http://{HOST}:{PORT}/query", data=qr.SerializeToString(),
                  headers={"Content-Type": "application/x-protobuf",
                           "Accept": "application/x-protobuf"})

    full_span_hours = max(1, math.ceil((end_ts - BASE_TS) / (3600 * 1e9)))
    full_interval = f"{full_span_hours}h"

    narrow_end = BASE_TS + 100 * MINUTE_NS
    narrow_hours = max(1, math.ceil((narrow_end - BASE_TS) / (3600 * 1e9)))
    narrow_interval = f"{narrow_hours}h"

    results = {}
    queries = [
        ("latest_1f",    "latest:server.metrics(cpu_usage)",                BASE_TS, end_ts,     full_interval,  20),
        ("avg_narrow",   "avg:server.metrics(cpu_usage)",                   BASE_TS, narrow_end, narrow_interval, 20),
        ("avg_full_1f",  "avg:server.metrics(cpu_usage)",                   BASE_TS, end_ts,     full_interval,  10),
        ("avg_full_all", "avg:server.metrics()",                            BASE_TS, end_ts,     full_interval,   5),
        ("avg_host",     "avg:server.metrics(cpu_usage){host:host-01}",     BASE_TS, end_ts,     full_interval,  20),
        ("grp_host",     "avg:server.metrics(cpu_usage) by {host}",         BASE_TS, end_ts,     full_interval,  10),
        ("avg_1d",       "avg:server.metrics(cpu_usage)",                   BASE_TS, end_ts,     "1d",            3),
        ("count_full",   "count:server.metrics(cpu_usage)",                 BASE_TS, end_ts,     full_interval,  10),
        ("max_full",     "max:server.metrics(cpu_usage)",                   BASE_TS, end_ts,     full_interval,  10),
        ("sum_full",     "sum:server.metrics(cpu_usage)",                   BASE_TS, end_ts,     full_interval,  10),
        ("avg_7d",       "avg:server.metrics(cpu_usage)",                   BASE_TS, end_ts,     "7d",            3),
        ("grp_host_1d",  "avg:server.metrics(cpu_usage) by {host}",         BASE_TS, end_ts,     "1d",            3),
    ]

    for name, q, st, et, interval, iters in queries:
        avg, mn, p95, sz = run_query(sess, q, st, et, interval, iters)
        results[name] = {"avg": avg, "min": mn, "p95": p95, "bytes": sz}

    return results


def fmt_pts(n):
    if n >= 1e9: return f"{n/1e9:.1f}B"
    if n >= 1e6: return f"{n/1e6:.0f}M"
    return str(int(n))

def fmt_ms(ms):
    if ms >= 1000: return f"{ms/1000:.2f}s"
    return f"{ms:.2f}"


def main():
    iterations = TARGET_TOTAL // (BATCHES_PER_ITER * POINTS_PER_BATCH)

    print("=" * 110)
    print(" TimeStar Scaling Benchmark (compaction-aware)")
    print("=" * 110)
    print(f"  Server:       {HOST}:{PORT}")
    print(f"  Build dir:    {BUILD_DIR}")
    print(f"  Iterations:   {iterations} x {fmt_pts(BATCHES_PER_ITER * POINTS_PER_BATCH)} = {fmt_pts(TARGET_TOTAL)}")
    print(f"  Batch:        {BATCH_SIZE} ts x {len(FIELD_NAMES)} fields = {fmt_pts(POINTS_PER_BATCH)}/batch")
    print(f"  Strategy:     Insert all → wait compaction → query all")
    print("=" * 110)
    print()

    all_results = []
    cumulative_points = 0

    for i in range(iterations):
        iter_points = BATCHES_PER_ITER * POINTS_PER_BATCH
        cumulative_points += iter_points
        end_ts = BASE_TS + (i + 1) * BATCHES_PER_ITER * BATCH_SIZE * MINUTE_NS

        # ── Insert ──
        print(f"[{i+1}/{iterations}] Inserting {fmt_pts(iter_points)} (total: {fmt_pts(cumulative_points)})...")
        pts, wall = run_insert(i, BATCHES_PER_ITER)
        throughput = pts / wall if wall > 0 else 0
        print(f"  Insert: {fmt_pts(throughput)} pts/sec in {wall:.1f}s")

        # ── Wait for compaction ──
        wait_for_compaction(f"after {fmt_pts(cumulative_points)} inserted", max_wait=600)

        # ── Query ──
        print(f"  Querying over {fmt_pts(cumulative_points)} points (compaction complete)...")
        qresults = run_query_suite(cumulative_points, end_ts)

        row = {"points": cumulative_points, "insert_pts_sec": throughput, "insert_wall": wall}
        row.update(qresults)
        all_results.append(row)

        print(f"  latest={fmt_ms(qresults['latest_1f']['avg'])}ms  "
              f"avg_f1={fmt_ms(qresults['avg_full_1f']['avg'])}ms  "
              f"avg_fall={fmt_ms(qresults['avg_full_all']['avg'])}ms  "
              f"grp_host={fmt_ms(qresults['grp_host']['avg'])}ms  "
              f"avg_1d={fmt_ms(qresults['avg_1d']['avg'])}ms")
        print()

    # ── Results table ──
    print()
    print("=" * 140)
    print(" SCALING RESULTS (post-compaction, steady-state)")
    print("=" * 140)

    query_names = ["latest_1f", "avg_narrow", "avg_full_1f", "avg_full_all",
                   "avg_host", "grp_host", "avg_1d", "count_full",
                   "max_full", "sum_full", "avg_7d", "grp_host_1d"]
    short_names = ["latest", "avg_nar", "avg_f1", "avg_fall",
                   "avg_h", "grp_h", "avg_1d", "cnt_f",
                   "max_f", "sum_f", "avg_7d", "grp_h1d"]

    hdr = f"{'Points':>8s}  {'Insert':>10s}"
    for sn in short_names:
        hdr += f"  {sn:>8s}"
    print(hdr)
    hdr2 = f"{'':>8s}  {'(M pts/s)':>10s}"
    for _ in short_names:
        hdr2 += f"  {'(ms)':>8s}"
    print(hdr2)
    print("-" * 140)

    for row in all_results:
        line = f"{fmt_pts(row['points']):>8s}  {row['insert_pts_sec']/1e6:>10.1f}"
        for qn in query_names:
            line += f"  {row[qn]['avg']:>8.2f}"
        print(line)

    print("-" * 140)

    if len(all_results) >= 2:
        base = all_results[0]
        last = all_results[-1]
        ratio_line = f"{'ratio':>8s}  {last['insert_pts_sec']/max(base['insert_pts_sec'],1):>10.2f}"
        for qn in query_names:
            r = last[qn]['avg'] / max(base[qn]['avg'], 0.001)
            ratio_line += f"  {r:>8.2f}"
        print(ratio_line)

    print("=" * 140)

    # ── P95 table ──
    print()
    print("P95 Latencies (ms):")
    print("-" * 140)
    hdr = f"{'Points':>8s}"
    for sn in short_names:
        hdr += f"  {sn:>8s}"
    print(hdr)
    print("-" * 140)
    for row in all_results:
        line = f"{fmt_pts(row['points']):>8s}"
        for qn in query_names:
            line += f"  {row[qn]['p95']:>8.2f}"
        print(line)
    print("-" * 140)

    # ── Disk usage ──
    print()
    details, total_mb = get_tsm_file_details()
    print(f"Final disk usage: {total_mb:.0f} MB across {sum(v['count'] for v in details.values())} TSM files")
    for shard, info in sorted(details.items()):
        print(f"  {shard}: {info['count']} files, {info['size_mb']:.0f} MB")


if __name__ == "__main__":
    main()
