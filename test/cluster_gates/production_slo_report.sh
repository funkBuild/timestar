#!/bin/bash
# Serial same-candidate SLO qualification. Reuses the existing discriminating
# live gates and publishes one exact-v1 JSON report bound to the binary hash,
# commit, resource settings, thresholds, and raw transcripts.
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }

REPO="$(git rev-parse --show-toplevel)"
if ! DIRTY=$(git -C "$REPO" status --porcelain --untracked-files=no --ignore-submodules=all); then
    echo "ABORT: could not verify the candidate worktree" >&2
    exit 2
fi
if [ -n "$DIRTY" ]; then
    echo "ABORT: production SLO evidence requires a clean tracked worktree" >&2
    printf '%s\n' "$DIRTY" | sed 's/^/  /' >&2
    exit 2
fi

export GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
export GATE_MAX_NODE_FAILURE_ERROR_BPS="${GATE_MAX_NODE_FAILURE_ERROR_BPS:-5000}"
export GATE_MAX_FAILOVER_RECOVERY_MS="${GATE_MAX_FAILOVER_RECOVERY_MS:-15000}"
export GATE_MAX_FAILOVER_QUERY_P99_MS="${GATE_MAX_FAILOVER_QUERY_P99_MS:-2000}"
export GATE_MAX_SNAPSHOT_INSTALL_MS="${GATE_MAX_SNAPSHOT_INSTALL_MS:-360000}"
export GATE_MAX_SNAPSHOT_CATCHUP_MS="${GATE_MAX_SNAPSHOT_CATCHUP_MS:-750000}"
export GATE_MAX_MOVEMENT_P99_MS="${GATE_MAX_MOVEMENT_P99_MS:-5000}"
export GATE_MIN_DIP_PCT="${GATE_MIN_DIP_PCT:-10}"
export GATE_MAX_STORM_5XX="${GATE_MAX_STORM_5XX:-100}"
for threshold_name in GATE_MAX_FAILOVER_RECOVERY_MS GATE_MAX_FAILOVER_QUERY_P99_MS GATE_MAX_SNAPSHOT_INSTALL_MS \
    GATE_MAX_SNAPSHOT_CATCHUP_MS GATE_MAX_MOVEMENT_P99_MS GATE_MIN_DIP_PCT; do
    threshold_value="${!threshold_name}"
    case "$threshold_value" in
        ''|*[!0-9]*|0) echo "ABORT: $threshold_name must be a positive integer" >&2; exit 2 ;;
    esac
done
for threshold_name in GATE_MAX_NODE_FAILURE_ERROR_BPS GATE_MAX_STORM_5XX; do
    threshold_value="${!threshold_name}"
    case "$threshold_value" in
        ''|*[!0-9]*) echo "ABORT: $threshold_name must be a non-negative integer" >&2; exit 2 ;;
    esac
done
[ "$GATE_MAX_NODE_FAILURE_ERROR_BPS" -le 10000 ] || {
    echo "ABORT: GATE_MAX_NODE_FAILURE_ERROR_BPS cannot exceed 10000" >&2; exit 2; }
[ "$GATE_MIN_DIP_PCT" -le 100 ] || {
    echo "ABORT: GATE_MIN_DIP_PCT cannot exceed 100" >&2; exit 2; }

REPORT_DIR="$GATE_TMP_ROOT/tsgate_slo_report"
NODE_LOG="$REPORT_DIR/node_kill.log"
SNAPSHOT_LOG="$REPORT_DIR/snapshot_catchup.log"
MOVEMENT_LOG="$REPORT_DIR/movement.log"
REPORT="$REPORT_DIR/report.v1.json"
fresh_gate_data_dirs "$REPORT_DIR" || exit 2

cleanup() {
    kill_cluster 1961
    kill_cluster 1951
    kill_cluster 1924
}
trap cleanup EXIT

run_gate() { # LABEL SCRIPT LOG
    local label="$1" script="$2" log="$3" rc
    echo "=== SLO arm: $label ==="
    "$script" "$BIN" >"$log" 2>&1
    rc=$?
    grep -E '^GATE_METRIC |^GATE (PASSED|FAILED|VOID)' "$log" | sed 's/^/  /'
    if [ "$rc" -ne 0 ]; then
        echo "SLO arm failed: $label (exit $rc); tail follows" >&2
        tail -n 80 "$log" >&2
        exit "$rc"
    fi
}

run_gate "one-node failure, recovery, and survivor query latency" \
    ./node_kill_round.sh "$NODE_LOG"
run_gate "empty-node snapshot installation and exact catch-up" \
    ./restart_catchup_gate.sh "$SNAPSHOT_LOG"
run_gate "leadership-movement throughput and latency impact" \
    ./skewed_rebalance_gate.sh "$MOVEMENT_LOG"

metric() { # LOG NAME
    awk -v name="$2" '$1 == "GATE_METRIC" && $2 == name { print $3; exit }' "$1"
}
require_metric() { # LOG NAME
    local value
    value=$(metric "$1" "$2")
    if [ -z "$value" ]; then
        echo "ABORT: missing GATE_METRIC $2 in $1" >&2
        exit 2
    fi
    printf '%s' "$value"
}

NODE_BATCHES=$(require_metric "$NODE_LOG" node_failure_batches)
NODE_PROBES=$(require_metric "$NODE_LOG" node_failure_probes)
NODE_ERRORS=$(require_metric "$NODE_LOG" node_failure_http_errors)
NODE_ERROR_BPS=$(require_metric "$NODE_LOG" node_failure_error_bps)
NODE_RECOVERY_MS=$(require_metric "$NODE_LOG" node_failure_recovery_ms)
NODE_QUERY_P99_MS=$(require_metric "$NODE_LOG" node_failure_query_p99_ms)
NODE_QUERY_SAMPLES=$(require_metric "$NODE_LOG" node_failure_query_samples)
SNAPSHOT_INSTALL_MS=$(require_metric "$SNAPSHOT_LOG" snapshot_install_ms)
SNAPSHOT_CATCHUP_MS=$(require_metric "$SNAPSHOT_LOG" snapshot_catchup_ms)
SNAPSHOT_CHUNKS=$(require_metric "$SNAPSHOT_LOG" snapshot_chunks_after_rejoin)
SNAPSHOT_PREFIX_WRITES=$(require_metric "$SNAPSHOT_LOG" snapshot_prefix_writes)
SNAPSHOT_SUFFIX_WRITES=$(require_metric "$SNAPSHOT_LOG" snapshot_suffix_writes)
MOVEMENT_TRANSFERS=$(require_metric "$MOVEMENT_LOG" transfers_initiated)
MOVEMENT_CONTROL_TPUT=$(require_metric "$MOVEMENT_LOG" control_tput)
MOVEMENT_STORM_TPUT=$(require_metric "$MOVEMENT_LOG" storm_tput)
MOVEMENT_RETAINED_PCT=$(require_metric "$MOVEMENT_LOG" storm_pct)
MOVEMENT_HTTP_ERRORS=$(require_metric "$MOVEMENT_LOG" storm_http_errors)
MOVEMENT_CONTROL_P99_MS=$(require_metric "$MOVEMENT_LOG" control_p99_ms)
MOVEMENT_P99_MS=$(require_metric "$MOVEMENT_LOG" movement_p99_ms)
MOVEMENT_BATCHES=$(require_metric "$MOVEMENT_LOG" movement_batches)
MOVEMENT_BATCH_SIZE=$(require_metric "$MOVEMENT_LOG" movement_batch_size)
MOVEMENT_CONNECTIONS=$(require_metric "$MOVEMENT_LOG" movement_connections)
MOVEMENT_HOSTS=$(require_metric "$MOVEMENT_LOG" movement_hosts)

COMMIT=$(git -C "$REPO" rev-parse HEAD)
BINARY_SHA256=$(sha256sum "$BIN" | awk '{print $1}')
SERVER_ENV_SHA256=$(printf '%s' "$GATE_SERVER_ENV" | sha256sum | awk '{print $1}')
GENERATED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
HOST=$(hostname)

export COMMIT BINARY_SHA256 SERVER_ENV_SHA256 GENERATED_AT HOST BIN GATE_SERVER_MEMORY
export NODE_BATCHES NODE_PROBES NODE_ERRORS NODE_ERROR_BPS NODE_RECOVERY_MS
export NODE_QUERY_P99_MS NODE_QUERY_SAMPLES SNAPSHOT_INSTALL_MS SNAPSHOT_CATCHUP_MS
export SNAPSHOT_CHUNKS SNAPSHOT_PREFIX_WRITES SNAPSHOT_SUFFIX_WRITES
export MOVEMENT_TRANSFERS MOVEMENT_CONTROL_TPUT MOVEMENT_STORM_TPUT
export MOVEMENT_RETAINED_PCT MOVEMENT_HTTP_ERRORS MOVEMENT_CONTROL_P99_MS MOVEMENT_P99_MS
export MOVEMENT_BATCHES MOVEMENT_BATCH_SIZE MOVEMENT_CONNECTIONS MOVEMENT_HOSTS
export GATE_MAX_NODE_FAILURE_ERROR_BPS GATE_MAX_FAILOVER_RECOVERY_MS
export GATE_MAX_FAILOVER_QUERY_P99_MS GATE_MAX_SNAPSHOT_INSTALL_MS
export GATE_MAX_SNAPSHOT_CATCHUP_MS GATE_MAX_MOVEMENT_P99_MS GATE_MIN_DIP_PCT
export GATE_MAX_STORM_5XX

python3 - "$REPORT" <<'PY'
import json
import os
import sys

def integer(name):
    return int(os.environ[name])

def number(name):
    return float(os.environ[name])

report = {
    "version": 1,
    "generated_at": os.environ["GENERATED_AT"],
    "host": os.environ["HOST"],
    "candidate": {
        "commit": os.environ["COMMIT"],
        "binary": os.environ["BIN"],
        "binary_sha256": os.environ["BINARY_SHA256"],
    },
    "settings": {
        "server_memory_per_process": os.environ["GATE_SERVER_MEMORY"],
        "server_environment_sha256": os.environ["SERVER_ENV_SHA256"],
        "node_failure_smp": 4,
        "snapshot_catchup_smp": 1,
        "movement_smp": 4,
        "scratch_root": os.environ["GATE_TMP_ROOT"],
    },
    "thresholds": {
        "node_failure_error_basis_points": integer("GATE_MAX_NODE_FAILURE_ERROR_BPS"),
        "node_failure_recovery_ms": integer("GATE_MAX_FAILOVER_RECOVERY_MS"),
        "node_failure_query_p99_ms": integer("GATE_MAX_FAILOVER_QUERY_P99_MS"),
        "snapshot_install_ms": integer("GATE_MAX_SNAPSHOT_INSTALL_MS"),
        "snapshot_catchup_ms": integer("GATE_MAX_SNAPSHOT_CATCHUP_MS"),
        "movement_p99_ms": integer("GATE_MAX_MOVEMENT_P99_MS"),
        "movement_throughput_retained_percent": integer("GATE_MIN_DIP_PCT"),
        "movement_http_errors": integer("GATE_MAX_STORM_5XX"),
    },
    "measurements": {
        "node_failure": {
            "batches": integer("NODE_BATCHES"),
            "probes": integer("NODE_PROBES"),
            "http_errors": integer("NODE_ERRORS"),
            "error_basis_points": integer("NODE_ERROR_BPS"),
            "all_group_leader_recovery_ms": integer("NODE_RECOVERY_MS"),
            "survivor_query_p99_ms": integer("NODE_QUERY_P99_MS"),
            "survivor_query_samples": integer("NODE_QUERY_SAMPLES"),
        },
        "snapshot_catchup": {
            "prefix_writes": integer("SNAPSHOT_PREFIX_WRITES"),
            "suffix_writes": integer("SNAPSHOT_SUFFIX_WRITES"),
            "install_ms": integer("SNAPSHOT_INSTALL_MS"),
            "exact_readback_ms": integer("SNAPSHOT_CATCHUP_MS"),
            "chunks_after_rejoin": integer("SNAPSHOT_CHUNKS"),
        },
        "movement": {
            "batches": integer("MOVEMENT_BATCHES"),
            "batch_size": integer("MOVEMENT_BATCH_SIZE"),
            "connections": integer("MOVEMENT_CONNECTIONS"),
            "hosts": integer("MOVEMENT_HOSTS"),
            "transfers_initiated": integer("MOVEMENT_TRANSFERS"),
            "control_points_per_second": number("MOVEMENT_CONTROL_TPUT"),
            "storm_points_per_second": number("MOVEMENT_STORM_TPUT"),
            "throughput_retained_percent": integer("MOVEMENT_RETAINED_PCT"),
            "http_errors": integer("MOVEMENT_HTTP_ERRORS"),
            "control_p99_ms": number("MOVEMENT_CONTROL_P99_MS"),
            "storm_p99_ms": number("MOVEMENT_P99_MS"),
        },
    },
    "transcripts": {
        "node_failure": "node_kill.log",
        "snapshot_catchup": "snapshot_catchup.log",
        "movement": "movement.log",
    },
}

with open(sys.argv[1], "x", encoding="utf-8") as out:
    json.dump(report, out, indent=2, sort_keys=True)
    out.write("\n")
PY

echo "PRODUCTION_SLO_REPORT PASSED"
echo "  report: $REPORT"
echo "  candidate: $COMMIT ($BINARY_SHA256)"
echo "  node failure: $NODE_ERRORS/$NODE_BATCHES errors, ${NODE_RECOVERY_MS}ms recovery, ${NODE_QUERY_P99_MS}ms query p99"
echo "  snapshot catch-up: ${SNAPSHOT_INSTALL_MS}ms install, ${SNAPSHOT_CATCHUP_MS}ms exact readback"
echo "  movement: ${MOVEMENT_RETAINED_PCT}% throughput retained, ${MOVEMENT_P99_MS}ms p99"
