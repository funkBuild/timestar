#!/usr/bin/env python3
"""
TimeStar vs InfluxDB 2.7 Equivalence Test

Loads identical data into both databases, runs 17 query types, and compares results.
"""

import json
import math
import os
import random
import subprocess
import sys
import time
import urllib.request
import urllib.error
import traceback

# ── Constants ────────────────────────────────────────────────────────────
BASE_TS = 1_000_000_000_000_000_000
MINUTE_NS = 60_000_000_000
SECOND_NS = 1_000_000_000

BATCH_COUNT = 100
BATCH_SIZE = 10_000
SEED = 42

NUM_HOSTS = 10
NUM_RACKS = 2

END_TS = BASE_TS + BATCH_COUNT * BATCH_SIZE * MINUTE_NS
NARROW_END = BASE_TS + 100 * MINUTE_NS
MED_END = BASE_TS + 10_000 * MINUTE_NS

FIELD_NAMES = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m",
    "load_avg_15m", "temperature"
]

TIMESTAR_URL = "http://127.0.0.1:8086"
INFLUX_URL = "http://127.0.0.1:8087"
INFLUX_ORG = "bench"
INFLUX_BUCKET = "bench"
INFLUX_TOKEN = "benchtoken123456"

TOLERANCE = 0.01  # 1% relative error tolerance


# ── HTTP helpers ─────────────────────────────────────────────────────────

def http_post(url, data, headers=None, timeout=120):
    if headers is None:
        headers = {}
    if isinstance(data, str):
        data = data.encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except Exception as e:
        return 0, str(e)


def http_get(url, headers=None, timeout=30):
    if headers is None:
        headers = {}
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except Exception as e:
        return 0, str(e)


# ── Data generation ──────────────────────────────────────────────────────

def build_batch(global_seed, host_id, rack_id, start_ts, count):
    rng = random.Random(global_seed ^ (host_id << 32) ^ start_ts)
    timestamps = [start_ts + i * MINUTE_NS for i in range(count)]
    fields = {}
    for fname in FIELD_NAMES:
        fields[fname] = [rng.uniform(0.0, 100.0) for _ in range(count)]
    return timestamps, fields, host_id, rack_id


def build_timestar_payload(timestamps, fields, host_id, rack_id):
    payload = {
        "measurement": "server.metrics",
        "tags": {"host": f"host-{host_id:02d}", "rack": f"rack-{rack_id}"},
        "timestamps": timestamps,
        "fields": {fname: [round(v, 6) for v in vals] for fname, vals in fields.items()}
    }
    return json.dumps(payload)


def build_influx_line_protocol(timestamps, fields, host_id, rack_id):
    lines = []
    host_tag = f"host-{host_id:02d}"
    rack_tag = f"rack-{rack_id}"
    for i, ts in enumerate(timestamps):
        field_parts = []
        for fname in FIELD_NAMES:
            val = fields[fname][i]
            field_parts.append(f"{fname}={val:.6g}")
        field_str = ",".join(field_parts)
        lines.append(f"server.metrics,host={host_tag},rack={rack_tag} {field_str} {ts}")
    return "\n".join(lines)


# ── Database setup ───────────────────────────────────────────────────────

def setup_influxdb():
    print("Setting up InfluxDB 2.7...")
    subprocess.run(["docker", "rm", "-f", "influx-equiv"], capture_output=True)
    cmd = [
        "docker", "run", "-d", "--name", "influx-equiv",
        "-p", "8087:8086",
        "-e", "DOCKER_INFLUXDB_INIT_MODE=setup",
        "-e", "DOCKER_INFLUXDB_INIT_USERNAME=admin",
        "-e", "DOCKER_INFLUXDB_INIT_PASSWORD=password123456",
        "-e", f"DOCKER_INFLUXDB_INIT_ORG={INFLUX_ORG}",
        "-e", f"DOCKER_INFLUXDB_INIT_BUCKET={INFLUX_BUCKET}",
        "-e", f"DOCKER_INFLUXDB_INIT_ADMIN_TOKEN={INFLUX_TOKEN}",
        "-e", "INFLUXD_STORAGE_WAL_FSYNC_DELAY=100ms",
        "influxdb:2.7"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Failed to start InfluxDB: {result.stderr}")
        return False
    print("  Waiting for InfluxDB to be ready...")
    for i in range(30):
        time.sleep(1)
        try:
            status, _ = http_get(f"{INFLUX_URL}/health")
            if status == 200:
                print("  InfluxDB is ready.")
                return True
        except:
            pass
    print("  InfluxDB failed to start in 30s")
    return False


def setup_timestar():
    print("Setting up TimeStar...")
    subprocess.run(["killall", "-TERM", "timestar_http_server"], capture_output=True)
    time.sleep(2)
    build_dir = "/home/matt/Desktop/source/tsdb/build-release"
    for d in os.listdir(build_dir):
        if d.startswith("shard_"):
            subprocess.run(["rm", "-rf", os.path.join(build_dir, d)])
    server_bin = os.path.join(build_dir, "bin", "timestar_http_server")
    proc = subprocess.Popen(
        [server_bin, "-c", "4", "--memory", "24G", "--overprovisioned"],
        cwd=build_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    print("  Waiting for TimeStar to be ready...")
    for i in range(15):
        time.sleep(1)
        try:
            status, _ = http_get(f"{TIMESTAR_URL}/health")
            if status == 200:
                print("  TimeStar is ready.")
                return proc
        except:
            pass
    print("  TimeStar failed to start in 15s")
    proc.kill()
    return None


# ── Data loading ─────────────────────────────────────────────────────────

def load_data():
    print(f"\nLoading data: {BATCH_COUNT} batches x {BATCH_SIZE} timestamps x {len(FIELD_NAMES)} fields = {BATCH_COUNT * BATCH_SIZE * len(FIELD_NAMES):,} points")

    rng = random.Random(SEED)
    batch_assignments = []
    for i in range(BATCH_COUNT):
        hid = rng.randint(1, NUM_HOSTS)
        rid = rng.randint(1, NUM_RACKS)
        start_ts = BASE_TS + i * BATCH_SIZE * MINUTE_NS
        batch_assignments.append((hid, rid, start_ts))

    # Load into TimeStar
    print("\n  Loading into TimeStar...")
    ts_start = time.time()
    ts_ok = 0
    for idx, (hid, rid, start_ts) in enumerate(batch_assignments):
        timestamps, fields, _, _ = build_batch(SEED, hid, rid, start_ts, BATCH_SIZE)
        payload = build_timestar_payload(timestamps, fields, hid, rid)
        status, body = http_post(f"{TIMESTAR_URL}/write", payload,
                                 headers={"Content-Type": "application/json"}, timeout=60)
        if 200 <= status < 300:
            ts_ok += 1
        elif idx < 3:
            print(f"    Batch {idx} failed (HTTP {status}): {body[:200]}")
        if (idx + 1) % 20 == 0:
            print(f"    TimeStar: {idx+1}/{BATCH_COUNT} ({ts_ok} ok)")
    ts_elapsed = time.time() - ts_start
    ts_pts = ts_ok * BATCH_SIZE * len(FIELD_NAMES)
    print(f"    TimeStar: {ts_ok}/{BATCH_COUNT} batches, {ts_pts:,} pts in {ts_elapsed:.1f}s")

    # Load into InfluxDB
    print("\n  Loading into InfluxDB...")
    influx_start = time.time()
    influx_ok = 0
    for idx, (hid, rid, start_ts) in enumerate(batch_assignments):
        timestamps, fields, _, _ = build_batch(SEED, hid, rid, start_ts, BATCH_SIZE)
        lp = build_influx_line_protocol(timestamps, fields, hid, rid)
        status, body = http_post(
            f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns",
            lp, headers={"Authorization": f"Token {INFLUX_TOKEN}", "Content-Type": "text/plain"},
            timeout=120
        )
        if 200 <= status < 300:
            influx_ok += 1
        elif influx_ok < 3:
            print(f"    Batch {idx} failed (HTTP {status}): {body[:200]}")
        if (idx + 1) % 20 == 0:
            print(f"    InfluxDB: {idx+1}/{BATCH_COUNT} ({influx_ok} ok)")
    influx_elapsed = time.time() - influx_start
    influx_pts = influx_ok * BATCH_SIZE * len(FIELD_NAMES)
    print(f"    InfluxDB: {influx_ok}/{BATCH_COUNT} batches, {influx_pts:,} pts in {influx_elapsed:.1f}s")

    return ts_ok == BATCH_COUNT and influx_ok == BATCH_COUNT


def restart_timestar():
    print("\n  Restarting TimeStar to flush WAL -> TSM...")
    subprocess.run(["killall", "-TERM", "timestar_http_server"], capture_output=True)
    time.sleep(3)
    build_dir = "/home/matt/Desktop/source/tsdb/build-release"
    server_bin = os.path.join(build_dir, "bin", "timestar_http_server")
    proc = subprocess.Popen(
        [server_bin, "-c", "4", "--memory", "24G", "--overprovisioned"],
        cwd=build_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    for i in range(15):
        time.sleep(1)
        try:
            status, _ = http_get(f"{TIMESTAR_URL}/health")
            if status == 200:
                print("  TimeStar restarted and healthy.")
                return proc
        except:
            pass
    print("  TimeStar restart failed!")
    return None


# ── Query helpers ────────────────────────────────────────────────────────

def query_timestar(query_str, start_ts, end_ts, agg_interval=None):
    body = {"query": query_str, "startTime": start_ts, "endTime": end_ts}
    if agg_interval:
        body["aggregationInterval"] = agg_interval
    status, resp = http_post(f"{TIMESTAR_URL}/query", json.dumps(body),
                             headers={"Content-Type": "application/json"}, timeout=120)
    if status != 200:
        return {"error": f"HTTP {status}: {resp[:300]}"}
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        return {"error": f"Invalid JSON: {resp[:300]}"}


def ns_to_rfc3339(ns):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ns // 1_000_000_000))


def make_flux_range(start_ts, end_ts):
    return f'from(bucket: "{INFLUX_BUCKET}") |> range(start: {ns_to_rfc3339(start_ts)}, stop: {ns_to_rfc3339(end_ts)})'


def query_influxdb(flux_query, timeout=120):
    status, body = http_post(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        flux_query,
        headers={"Authorization": f"Token {INFLUX_TOKEN}",
                 "Content-Type": "application/vnd.flux",
                 "Accept": "application/csv"},
        timeout=timeout
    )
    if status != 200:
        return {"error": f"HTTP {status}: {body[:300]}"}
    return parse_influx_csv(body)


def parse_influx_csv(csv_text):
    """Parse InfluxDB annotated CSV response.

    Format: \r\n line endings. Lines starting with # are annotations.
    Header row like: ,result,table,_start,_stop,_value,...
    Data rows like: ,_result,0,...
    """
    csv_text = csv_text.replace("\r\n", "\n").replace("\r", "\n")
    lines = csv_text.strip().split("\n")
    header = None
    parsed_rows = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split(",")
        if "result" in parts and "table" in parts:
            header = [p.strip() for p in parts]
            continue
        if header is None:
            continue
        if len(parts) >= len(header):
            row = {}
            for i, h in enumerate(header):
                if h and i < len(parts):
                    row[h] = parts[i].strip()
            parsed_rows.append(row)
    return {"rows": parsed_rows}


# ── TimeStar response extraction ────────────────────────────────────────

def ts_extract_single(resp, field="cpu_usage"):
    """Extract single aggregated value(s) from TimeStar response."""
    if "error" in resp:
        return None, resp["error"]
    if resp.get("status") != "success":
        return None, f"Status: {resp.get('status')}"
    values = []
    for s in resp.get("series", []):
        for fname, fdata in s.get("fields", {}).items():
            if field is None or fname == field:
                values.extend(fdata.get("values", []))
    return values, None


def ts_extract_grouped(resp, field="cpu_usage", group_keys=None):
    """Extract grouped values from TimeStar response.

    TimeStar group-by uses 'groupTags' array like ["host=host-01", "rack=rack-1"]
    instead of 'tags' dict.
    """
    if "error" in resp:
        return None, resp["error"]
    if resp.get("status") != "success":
        return None, f"Status: {resp.get('status')}"

    groups = {}
    for s in resp.get("series", []):
        # Parse groupTags: ["host=host-01", "rack=rack-1"] -> {host: host-01, rack: rack-1}
        tag_dict = {}
        for gt in s.get("groupTags", []):
            if "=" in gt:
                k, v = gt.split("=", 1)
                tag_dict[k] = v
        # Also check regular tags
        for k, v in s.get("tags", {}).items():
            tag_dict[k] = v

        if group_keys:
            key = tuple(tag_dict.get(k, "") for k in group_keys)
        else:
            key = ("all",)

        for fname, fdata in s.get("fields", {}).items():
            if field is None or fname == field:
                if key not in groups:
                    groups[key] = {"values": [], "timestamps": []}
                groups[key]["values"].extend(fdata.get("values", []))
                groups[key]["timestamps"].extend(fdata.get("timestamps", []))

    return groups, None


def ts_extract_all_fields(resp):
    """Extract per-field values from TimeStar response."""
    if "error" in resp:
        return None, resp["error"]
    if resp.get("status") != "success":
        return None, f"Status: {resp.get('status')}"
    field_values = {}
    for s in resp.get("series", []):
        for fname, fdata in s.get("fields", {}).items():
            if fname not in field_values:
                field_values[fname] = []
            field_values[fname].extend(fdata.get("values", []))
    return field_values, None


# ── InfluxDB response extraction ────────────────────────────────────────

def influx_extract_values(resp):
    if "error" in resp:
        return None, resp["error"]
    values = []
    for r in resp.get("rows", []):
        val = r.get("_value", "")
        if val != "":
            try:
                values.append(float(val))
            except (ValueError, TypeError):
                values.append(val)
    return values, None


def influx_extract_grouped(resp, group_keys):
    if "error" in resp:
        return None, resp["error"]
    groups = {}
    for r in resp.get("rows", []):
        key = tuple(r.get(k, "") for k in group_keys)
        val = r.get("_value", "")
        if val == "":
            continue
        try:
            val = float(val)
        except (ValueError, TypeError):
            continue
        if key not in groups:
            groups[key] = {"values": [], "timestamps": []}
        groups[key]["values"].append(val)
        groups[key]["timestamps"].append(r.get("_time", ""))
    return groups, None


def influx_extract_all_fields(resp):
    if "error" in resp:
        return None, resp["error"]
    field_values = {}
    for r in resp.get("rows", []):
        fname = r.get("_field", "")
        val = r.get("_value", "")
        if val == "" or fname == "":
            continue
        try:
            val = float(val)
        except (ValueError, TypeError):
            continue
        if fname not in field_values:
            field_values[fname] = []
        field_values[fname].append(val)
    return field_values, None


# ── Comparison helpers ───────────────────────────────────────────────────

def values_match(a, b, tolerance=TOLERANCE):
    try:
        a, b = float(a), float(b)
    except (ValueError, TypeError):
        return str(a) == str(b)
    if math.isnan(a) and math.isnan(b):
        return True
    if a == b:
        return True
    if abs(b) < 1e-10:
        return abs(a - b) < 1e-10
    rel_err = abs(a - b) / max(abs(a), abs(b))
    return rel_err <= tolerance


def fmt_val(v):
    if isinstance(v, float):
        return f"{v:.6f}"
    return str(v)


# ── Query runner ─────────────────────────────────────────────────────────

def run_query(name, ts_query, ts_start, ts_end, ts_interval,
              flux_query, query_type="single", field="cpu_usage",
              group_keys=None):
    """Run a query against both databases and compare results.

    query_type: "single" (one aggregated value), "count" (integer comparison),
                "grouped" (per-group comparison), "bucketed" (time buckets),
                "all_fields" (per-field comparison), "grouped_bucketed"
    """
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")

    # Query TimeStar
    ts_resp = query_timestar(ts_query, ts_start, ts_end, agg_interval=ts_interval)

    # Query InfluxDB
    influx_resp = query_influxdb(flux_query)

    # ── single value comparison ──────────────────────────────────────
    if query_type == "single":
        ts_vals, ts_err = ts_extract_single(ts_resp, field)
        influx_vals, influx_err = influx_extract_values(influx_resp)

        if ts_err:
            print(f"  TimeStar ERROR: {ts_err}")
            return False, f"TimeStar error: {ts_err}"
        if influx_err:
            print(f"  InfluxDB ERROR: {influx_err}")
            return False, f"InfluxDB error: {influx_err}"

        print(f"  TimeStar:  {len(ts_vals)} pts, values={[fmt_val(v) for v in ts_vals[:5]]}")
        print(f"  InfluxDB:  {len(influx_vals)} pts, values={[fmt_val(v) for v in influx_vals[:5]]}")

        if len(ts_vals) == 0 and len(influx_vals) == 0:
            print(f"  PASS: Both returned 0 points")
            return True, "Both returned 0 points"

        if len(ts_vals) >= 1 and len(influx_vals) >= 1:
            ok = values_match(ts_vals[0], influx_vals[0])
            detail = f"TS={fmt_val(ts_vals[0])}, Influx={fmt_val(influx_vals[0])}"
            if not ok:
                rel = abs(float(ts_vals[0]) - float(influx_vals[0])) / max(abs(float(ts_vals[0])), abs(float(influx_vals[0])), 1e-15)
                detail += f", rel_err={rel:.6f}"
            print(f"  {'PASS' if ok else 'FAIL'}: {detail}")
            return ok, detail

        detail = f"Count mismatch: TS={len(ts_vals)}, Influx={len(influx_vals)}"
        print(f"  FAIL: {detail}")
        return False, detail

    # ── count comparison ─────────────────────────────────────────────
    if query_type == "count":
        ts_vals, ts_err = ts_extract_single(ts_resp, field)
        influx_vals, influx_err = influx_extract_values(influx_resp)

        if ts_err:
            return False, f"TimeStar error: {ts_err}"
        if influx_err:
            return False, f"InfluxDB error: {influx_err}"

        print(f"  TimeStar:  {ts_vals[:5]}")
        print(f"  InfluxDB:  {influx_vals[:5]}")

        if len(ts_vals) >= 1 and len(influx_vals) >= 1:
            ts_c = int(ts_vals[0])
            influx_c = int(influx_vals[0])
            ok = ts_c == influx_c
            detail = f"TS={ts_c}, Influx={influx_c}"
            print(f"  {'PASS' if ok else 'FAIL'}: {detail}")
            return ok, detail

        return False, f"No values: TS={len(ts_vals)}, Influx={len(influx_vals)}"

    # ── grouped comparison ───────────────────────────────────────────
    if query_type == "grouped":
        ts_groups, ts_err = ts_extract_grouped(ts_resp, field, group_keys)
        influx_groups, influx_err = influx_extract_grouped(influx_resp, group_keys)

        if ts_err:
            return False, f"TimeStar error: {ts_err}"
        if influx_err:
            return False, f"InfluxDB error: {influx_err}"

        print(f"  TimeStar:  {len(ts_groups)} groups: {sorted(ts_groups.keys())}")
        print(f"  InfluxDB:  {len(influx_groups)} groups: {sorted(influx_groups.keys())}")

        if len(ts_groups) != len(influx_groups):
            detail = f"Group count: TS={len(ts_groups)}, Influx={len(influx_groups)}"
            print(f"  FAIL: {detail}")
            return False, detail

        all_ok = True
        for key in sorted(ts_groups.keys()):
            if key not in influx_groups:
                print(f"    {key}: missing from InfluxDB")
                all_ok = False
                continue
            ts_v = ts_groups[key]["values"]
            inf_v = influx_groups[key]["values"]
            if ts_v and inf_v:
                ok = values_match(ts_v[0], inf_v[0])
                print(f"    {key}: {'PASS' if ok else 'FAIL'} TS={fmt_val(ts_v[0])}, Influx={fmt_val(inf_v[0])}")
                if not ok:
                    all_ok = False
            else:
                print(f"    {key}: no values (TS={len(ts_v)}, Influx={len(inf_v)})")

        detail = f"{len(ts_groups)} groups"
        print(f"  {'PASS' if all_ok else 'FAIL'}: {detail}")
        return all_ok, detail

    # ── bucketed comparison ──────────────────────────────────────────
    if query_type == "bucketed":
        ts_vals, ts_err = ts_extract_single(ts_resp, field)
        influx_vals, influx_err = influx_extract_values(influx_resp)

        if ts_err:
            return False, f"TimeStar error: {ts_err}"
        if influx_err:
            return False, f"InfluxDB error: {influx_err}"

        print(f"  TimeStar:  {len(ts_vals)} buckets, first 5={[fmt_val(v) for v in ts_vals[:5]]}")
        print(f"  InfluxDB:  {len(influx_vals)} buckets, first 5={[fmt_val(v) for v in influx_vals[:5]]}")

        count_diff = abs(len(ts_vals) - len(influx_vals))
        n = min(len(ts_vals), len(influx_vals))

        # Bucket counts may differ by 1 due to boundary interpretation
        if count_diff > 1:
            detail = f"Bucket count mismatch: TS={len(ts_vals)}, Influx={len(influx_vals)}"
            print(f"  FAIL: {detail}")
            return False, detail

        # Spot-check values
        match = sum(1 for i in range(n) if values_match(ts_vals[i], influx_vals[i]))
        ok = match >= n - 1 and n > 0  # allow 1 boundary mismatch
        detail = f"buckets TS={len(ts_vals)} Influx={len(influx_vals)}, matched {match}/{n}"
        print(f"  {'PASS' if ok else 'FAIL'}: {detail}")
        return ok, detail

    # ── all_fields comparison ────────────────────────────────────────
    if query_type == "all_fields":
        ts_fields, ts_err = ts_extract_all_fields(ts_resp)
        influx_fields, influx_err = influx_extract_all_fields(influx_resp)

        if ts_err:
            return False, f"TimeStar error: {ts_err}"
        if influx_err:
            return False, f"InfluxDB error: {influx_err}"

        print(f"  TimeStar:  {sorted(ts_fields.keys())}")
        print(f"  InfluxDB:  {sorted(influx_fields.keys())}")

        all_ok = True
        for fname in FIELD_NAMES:
            ts_v = ts_fields.get(fname, [])
            inf_v = influx_fields.get(fname, [])
            if ts_v and inf_v:
                ok = values_match(ts_v[0], inf_v[0])
                print(f"    {fname}: {'PASS' if ok else 'FAIL'} TS={fmt_val(ts_v[0])}, Influx={fmt_val(inf_v[0])}")
                if not ok:
                    all_ok = False
            else:
                print(f"    {fname}: missing (TS={len(ts_v)}, Influx={len(inf_v)})")
                all_ok = False

        detail = f"TS {len(ts_fields)} fields, Influx {len(influx_fields)} fields"
        print(f"  {'PASS' if all_ok else 'FAIL'}: {detail}")
        return all_ok, detail

    # ── grouped_bucketed comparison ──────────────────────────────────
    if query_type == "grouped_bucketed":
        ts_groups, ts_err = ts_extract_grouped(ts_resp, field, group_keys)
        influx_groups, influx_err = influx_extract_grouped(influx_resp, group_keys)

        if ts_err:
            return False, f"TimeStar error: {ts_err}"
        if influx_err:
            return False, f"InfluxDB error: {influx_err}"

        print(f"  TimeStar:  {len(ts_groups)} groups")
        print(f"  InfluxDB:  {len(influx_groups)} groups")

        if len(ts_groups) != len(influx_groups):
            detail = f"Group count: TS={len(ts_groups)}, Influx={len(influx_groups)}"
            print(f"  FAIL: {detail}")
            return False, detail

        all_ok = True
        for key in sorted(ts_groups.keys()):
            if key not in influx_groups:
                print(f"    {key}: missing from InfluxDB")
                all_ok = False
                continue
            ts_v = ts_groups[key]["values"]
            inf_v = influx_groups[key]["values"]
            count_diff = abs(len(ts_v) - len(inf_v))
            n = min(len(ts_v), len(inf_v))
            match = sum(1 for i in range(min(5, n)) if values_match(ts_v[i], inf_v[i]))
            check_n = min(5, n)
            bucket_ok = count_diff <= 1 and (match >= check_n - 1 if check_n > 0 else True)
            print(f"    {key}: TS={len(ts_v)} Influx={len(inf_v)} buckets, spot {match}/{check_n}")
            if not bucket_ok:
                all_ok = False
                if check_n > 0:
                    print(f"      TS:     {[fmt_val(v) for v in ts_v[:5]]}")
                    print(f"      Influx: {[fmt_val(v) for v in inf_v[:5]]}")

        detail = f"{len(ts_groups)} groups"
        print(f"  {'PASS' if all_ok else 'FAIL'}: {detail}")
        return all_ok, detail

    return False, f"Unknown query_type: {query_type}"


# ── Query definitions ────────────────────────────────────────────────────

def run_all_queries():
    results = []

    def add(name, ok, detail):
        results.append((name, ok, detail))

    base_filter = f'''|> filter(fn: (r) => r["_measurement"] == "server.metrics")
  |> filter(fn: (r) => r["_field"] == "cpu_usage")'''

    # Q1: latest cpu_usage full range
    ok, d = run_query(
        "Q1: latest cpu_usage full range",
        "latest:server.metrics(cpu_usage)", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group()
  |> last()''',
        "single")
    add("Q1: latest cpu_usage full range", ok, d)

    # Q2: avg cpu_usage narrow
    ok, d = run_query(
        "Q2: avg cpu_usage narrow",
        "avg:server.metrics(cpu_usage)", BASE_TS, NARROW_END, None,
        f'''{make_flux_range(BASE_TS, NARROW_END)}
  {base_filter}
  |> group()
  |> mean()''',
        "single")
    add("Q2: avg cpu_usage narrow", ok, d)

    # Q3: max cpu_usage narrow
    ok, d = run_query(
        "Q3: max cpu_usage narrow",
        "max:server.metrics(cpu_usage)", BASE_TS, NARROW_END, None,
        f'''{make_flux_range(BASE_TS, NARROW_END)}
  {base_filter}
  |> group()
  |> max()''',
        "single")
    add("Q3: max cpu_usage narrow", ok, d)

    # Q4: count cpu_usage narrow
    ok, d = run_query(
        "Q4: count cpu_usage narrow",
        "count:server.metrics(cpu_usage)", BASE_TS, NARROW_END, None,
        f'''{make_flux_range(BASE_TS, NARROW_END)}
  {base_filter}
  |> group()
  |> count()''',
        "count")
    add("Q4: count cpu_usage narrow", ok, d)

    # Q5: avg cpu_usage {host:host-02} narrow
    # host-02 has data in the narrow range (batch 0 with seed=42)
    ok, d = run_query(
        "Q5: avg cpu_usage {host:host-02} narrow",
        "avg:server.metrics(cpu_usage){host:host-02}", BASE_TS, NARROW_END, None,
        f'''{make_flux_range(BASE_TS, NARROW_END)}
  {base_filter}
  |> filter(fn: (r) => r["host"] == "host-02")
  |> group()
  |> mean()''',
        "single")
    add("Q5: avg cpu_usage {host:host-02} narrow", ok, d)

    # Q6: avg cpu_usage by {host} narrow
    ok, d = run_query(
        "Q6: avg cpu_usage by {host} narrow",
        "avg:server.metrics(cpu_usage) by {host}", BASE_TS, NARROW_END, None,
        f'''{make_flux_range(BASE_TS, NARROW_END)}
  {base_filter}
  |> group(columns: ["host"])
  |> mean()''',
        "grouped", group_keys=["host"])
    add("Q6: avg cpu_usage by {host} narrow", ok, d)

    # Q7: avg cpu_usage 1h buckets medium
    ok, d = run_query(
        "Q7: avg cpu_usage 1h buckets medium",
        "avg:server.metrics(cpu_usage)", BASE_TS, MED_END, "1h",
        f'''{make_flux_range(BASE_TS, MED_END)}
  {base_filter}
  |> group()
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)''',
        "bucketed")
    add("Q7: avg cpu_usage 1h buckets medium", ok, d)

    # Q8: avg all fields narrow
    ok, d = run_query(
        "Q8: avg all fields narrow",
        "avg:server.metrics()", BASE_TS, NARROW_END, None,
        f'''{make_flux_range(BASE_TS, NARROW_END)}
  |> filter(fn: (r) => r["_measurement"] == "server.metrics")
  |> group(columns: ["_field"])
  |> mean()''',
        "all_fields")
    add("Q8: avg all fields narrow", ok, d)

    # Q9: avg cpu_usage full range
    ok, d = run_query(
        "Q9: avg cpu_usage full range",
        "avg:server.metrics(cpu_usage)", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group()
  |> mean()''',
        "single")
    add("Q9: avg cpu_usage full range", ok, d)

    # Q10: avg cpu_usage by {host} full
    ok, d = run_query(
        "Q10: avg cpu_usage by {host} full",
        "avg:server.metrics(cpu_usage) by {host}", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group(columns: ["host"])
  |> mean()''',
        "grouped", group_keys=["host"])
    add("Q10: avg cpu_usage by {host} full", ok, d)

    # Q11: first cpu_usage full
    ok, d = run_query(
        "Q11: first cpu_usage full",
        "first:server.metrics(cpu_usage)", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group()
  |> first()''',
        "single")
    add("Q11: first cpu_usage full", ok, d)

    # Q12: min cpu_usage full
    ok, d = run_query(
        "Q12: min cpu_usage full",
        "min:server.metrics(cpu_usage)", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group()
  |> min()''',
        "single")
    add("Q12: min cpu_usage full", ok, d)

    # Q13: stddev cpu_usage full
    ok, d = run_query(
        "Q13: stddev cpu_usage full",
        "stddev:server.metrics(cpu_usage)", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group()
  |> stddev()''',
        "single")
    add("Q13: stddev cpu_usage full", ok, d)

    # Q14: spread cpu_usage full
    ok, d = run_query(
        "Q14: spread cpu_usage full",
        "spread:server.metrics(cpu_usage)", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group()
  |> spread()''',
        "single")
    add("Q14: spread cpu_usage full", ok, d)

    # Q15: avg cpu_usage {host:host-01,rack:rack-1} full
    ok, d = run_query(
        "Q15: avg cpu_usage {host:host-01,rack:rack-1} full",
        "avg:server.metrics(cpu_usage){host:host-01,rack:rack-1}", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> filter(fn: (r) => r["host"] == "host-01")
  |> filter(fn: (r) => r["rack"] == "rack-1")
  |> group()
  |> mean()''',
        "single")
    add("Q15: avg cpu_usage {host:host-01,rack:rack-1} full", ok, d)

    # Q16: avg cpu_usage by {host,rack} full
    ok, d = run_query(
        "Q16: avg cpu_usage by {host,rack} full",
        "avg:server.metrics(cpu_usage) by {host,rack}", BASE_TS, END_TS, None,
        f'''{make_flux_range(BASE_TS, END_TS)}
  {base_filter}
  |> group(columns: ["host", "rack"])
  |> mean()''',
        "grouped", group_keys=["host", "rack"])
    add("Q16: avg cpu_usage by {host,rack} full", ok, d)

    # Q17: avg cpu_usage by {host} 5m buckets medium
    ok, d = run_query(
        "Q17: avg cpu_usage by {host} 5m buckets medium",
        "avg:server.metrics(cpu_usage) by {host}", BASE_TS, MED_END, "5m",
        f'''{make_flux_range(BASE_TS, MED_END)}
  {base_filter}
  |> group(columns: ["host"])
  |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)''',
        "grouped_bucketed", group_keys=["host"])
    add("Q17: avg cpu_usage by {host} 5m buckets medium", ok, d)

    return results


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  TimeStar vs InfluxDB 2.7 Equivalence Test")
    print("=" * 70)
    print(f"  Points: {BATCH_COUNT * BATCH_SIZE * len(FIELD_NAMES):,}")
    print(f"  Seed: {SEED}, Hosts: {NUM_HOSTS}, Racks: {NUM_RACKS}")
    print("=" * 70)

    ts_proc = None
    try:
        if not setup_influxdb():
            print("FATAL: Failed to setup InfluxDB")
            return 1

        ts_proc = setup_timestar()
        if ts_proc is None:
            print("FATAL: Failed to setup TimeStar")
            return 1

        if not load_data():
            print("WARNING: Some batches failed, continuing...")

        ts_proc = restart_timestar()
        if ts_proc is None:
            print("FATAL: Failed to restart TimeStar")
            return 1

        print("\n  Waiting 15s for InfluxDB to settle...")
        time.sleep(15)

        print("\n" + "=" * 70)
        print("  Running 17 equivalence queries...")
        print("=" * 70)

        results = run_all_queries()

        # Summary
        print("\n\n" + "=" * 70)
        print("  EQUIVALENCE TEST SUMMARY")
        print("=" * 70)

        passed = sum(1 for _, ok, _ in results if ok)
        total = len(results)
        for name, ok, detail in results:
            print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")

        print(f"\n  Result: {passed}/{total} passed")
        print("=" * 70)
        return 0 if passed == total else 1

    except Exception as e:
        print(f"\nFATAL EXCEPTION: {e}")
        traceback.print_exc()
        return 1
    finally:
        print("\n  Cleaning up...")
        subprocess.run(["killall", "-TERM", "timestar_http_server"], capture_output=True)
        subprocess.run(["docker", "rm", "-f", "influx-equiv"], capture_output=True)
        print("  Done.")


if __name__ == "__main__":
    sys.exit(main())
