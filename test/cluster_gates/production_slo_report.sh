#!/bin/bash
# Serial same-candidate SLO qualification. Reuses the existing discriminating
# live gates and publishes one exact-v1 JSON report bound to the binary hash,
# commit, resource settings, thresholds, and raw transcripts.
set -u
CALLER_DIR=$PWD
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN" >&2; exit 2; }
BENCH="$GATE_BENCH_BINARY"
[ -x "$BENCH" ] || { echo "no insert benchmark at $BENCH" >&2; exit 2; }
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
CANDIDATE_COMMIT=$(git -C "$REPO" rev-parse HEAD) || {
    echo "ABORT: could not resolve the candidate commit" >&2
    exit 2
}
verify_candidate_binary "$REPO" "$CANDIDATE_COMMIT" "$BIN" timestar_http_server || exit $?
SERVER_REVISION="$VERIFIED_BINARY_REVISION"
SERVER_SHA256="$VERIFIED_BINARY_SHA256"
verify_candidate_binary "$REPO" "$CANDIDATE_COMMIT" "$BENCH" timestar_insert_bench || exit $?
BENCH_REVISION="$VERIFIED_BINARY_REVISION"
BENCH_SHA256="$VERIFIED_BINARY_SHA256"

export GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-2G}"
export GATE_SERVER_SMP="${GATE_SERVER_SMP:-4}"
GATE_SNAPSHOT_SERVER_MEMORY="${GATE_SNAPSHOT_SERVER_MEMORY:-1G}"
GATE_SNAPSHOT_SERVER_SMP=1
export GATE_BENCH_SMP="${GATE_BENCH_SMP:-1}"
export GATE_BENCH_MEMORY="${GATE_BENCH_MEMORY:-1G}"
export GATE_BENCH_BINARY
export GATE_MAX_NODE_FAILURE_ERROR_BPS="${GATE_MAX_NODE_FAILURE_ERROR_BPS:-5000}"
export GATE_MAX_FAILOVER_RECOVERY_MS="${GATE_MAX_FAILOVER_RECOVERY_MS:-30000}"
export GATE_MAX_FAILOVER_QUERY_P99_MS="${GATE_MAX_FAILOVER_QUERY_P99_MS:-2000}"
export GATE_MAX_SNAPSHOT_INSTALL_MS="${GATE_MAX_SNAPSHOT_INSTALL_MS:-360000}"
export GATE_MAX_SNAPSHOT_CATCHUP_MS="${GATE_MAX_SNAPSHOT_CATCHUP_MS:-750000}"
export GATE_MAX_MOVEMENT_P99_MS="${GATE_MAX_MOVEMENT_P99_MS:-5000}"
export GATE_MIN_DIP_PCT="${GATE_MIN_DIP_PCT:-10}"
export GATE_MAX_STORM_5XX="${GATE_MAX_STORM_5XX:-100}"

# A local regression may use the bounded defaults above, but it is explicitly
# provisional. Production evidence must name one independently approved,
# time-bounded exact-v1 policy. Its values replace every resource/threshold
# seam, and both its canonical and source-byte identities are checked again
# after the long serial campaign.
SLO_POLICY_FILE="${GATE_SLO_POLICY_FILE:-}"
SLO_POLICY_STATUS=provisional
SLO_POLICY_SHA256=""
SLO_POLICY_SOURCE_SHA256=""
POLICY_VALUES_BEFORE=""
if [ -n "$SLO_POLICY_FILE" ]; then
    case "$SLO_POLICY_FILE" in
        /*) ;;
        *) SLO_POLICY_FILE="$CALLER_DIR/$SLO_POLICY_FILE" ;;
    esac
    POLICY_VALUES_BEFORE=$(python3 ./production_slo_policy.py --policy "$SLO_POLICY_FILE" --emit-values) || exit $?
    mapfile -t POLICY_VALUES <<<"$POLICY_VALUES_BEFORE"
    [ "${#POLICY_VALUES[@]}" -eq 16 ] || {
        echo "ABORT: approved SLO policy emitted ${#POLICY_VALUES[@]} values, expected 16" >&2; exit 2; }
    SLO_POLICY_STATUS=approved
    SLO_POLICY_SHA256="${POLICY_VALUES[0]}"
    SLO_POLICY_SOURCE_SHA256="${POLICY_VALUES[1]}"
    export GATE_SERVER_MEMORY="${POLICY_VALUES[2]}"
    export GATE_SERVER_SMP="${POLICY_VALUES[3]}"
    GATE_SNAPSHOT_SERVER_MEMORY="${POLICY_VALUES[4]}"
    GATE_SNAPSHOT_SERVER_SMP="${POLICY_VALUES[5]}"
    export GATE_BENCH_MEMORY="${POLICY_VALUES[6]}"
    export GATE_BENCH_SMP="${POLICY_VALUES[7]}"
    export GATE_MAX_NODE_FAILURE_ERROR_BPS="${POLICY_VALUES[8]}"
    export GATE_MAX_FAILOVER_RECOVERY_MS="${POLICY_VALUES[9]}"
    export GATE_MAX_FAILOVER_QUERY_P99_MS="${POLICY_VALUES[10]}"
    export GATE_MAX_SNAPSHOT_INSTALL_MS="${POLICY_VALUES[11]}"
    export GATE_MAX_SNAPSHOT_CATCHUP_MS="${POLICY_VALUES[12]}"
    export GATE_MAX_MOVEMENT_P99_MS="${POLICY_VALUES[13]}"
    export GATE_MIN_DIP_PCT="${POLICY_VALUES[14]}"
    export GATE_MAX_STORM_5XX="${POLICY_VALUES[15]}"
fi
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

run_gate() { # LABEL SCRIPT LOG SERVER_MEMORY
    local label="$1" script="$2" log="$3" server_memory="$4" rc
    echo "=== SLO arm: $label ==="
    GATE_SERVER_MEMORY="$server_memory" "$script" "$BIN" >"$log" 2>&1
    rc=$?
    grep -E '^GATE_METRIC |^GATE (PASSED|FAILED|VOID)' "$log" | sed 's/^/  /'
    if [ "$rc" -ne 0 ]; then
        echo "SLO arm failed: $label (exit $rc); tail follows" >&2
        tail -n 80 "$log" >&2
        exit "$rc"
    fi
}

run_gate "one-node failure, recovery, and survivor query latency" \
    ./node_kill_round.sh "$NODE_LOG" "$GATE_SERVER_MEMORY"
run_gate "empty-node snapshot installation and exact catch-up" \
    ./restart_catchup_gate.sh "$SNAPSHOT_LOG" "$GATE_SNAPSHOT_SERVER_MEMORY"
run_gate "leadership-movement throughput and latency impact" \
    ./skewed_rebalance_gate.sh "$MOVEMENT_LOG" "$GATE_SERVER_MEMORY"

# A build or replacement during the serial campaign would mix candidates in a
# single report. Bind the report only after proving both executables are the
# exact bytes authenticated before the first arm.
verify_candidate_binary_unchanged "$BIN" "$SERVER_SHA256" timestar_http_server || exit $?
verify_candidate_binary_unchanged "$BENCH" "$BENCH_SHA256" timestar_insert_bench || exit $?
if [ "$SLO_POLICY_STATUS" = approved ]; then
    POLICY_VALUES_AFTER=$(python3 ./production_slo_policy.py --policy "$SLO_POLICY_FILE" --emit-values) || exit $?
    [ "$POLICY_VALUES_AFTER" = "$POLICY_VALUES_BEFORE" ] || {
        echo "ABORT: approved SLO policy changed while qualification was running" >&2; exit 2; }
fi

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
NODE_BATCH_SIZE=$(require_metric "$NODE_LOG" node_failure_batch_size)
NODE_CONNECTIONS=$(require_metric "$NODE_LOG" node_failure_connections)
NODE_HOSTS=$(require_metric "$NODE_LOG" node_failure_hosts)
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

COMMIT="$CANDIDATE_COMMIT"
SERVER_ENV_SHA256=$(printf '%s' "$GATE_SERVER_ENV" | sha256sum | awk '{print $1}')
GENERATED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
HOST=$(hostname)

export COMMIT SERVER_REVISION SERVER_SHA256 BENCH_REVISION BENCH_SHA256 SERVER_ENV_SHA256 GENERATED_AT HOST
export BIN BENCH GATE_SERVER_MEMORY GATE_SERVER_SMP
export GATE_SNAPSHOT_SERVER_MEMORY GATE_SNAPSHOT_SERVER_SMP
export GATE_BENCH_SMP GATE_BENCH_MEMORY NODE_BATCHES NODE_BATCH_SIZE NODE_CONNECTIONS
export NODE_HOSTS NODE_PROBES NODE_ERRORS NODE_ERROR_BPS NODE_RECOVERY_MS
export NODE_QUERY_P99_MS NODE_QUERY_SAMPLES SNAPSHOT_INSTALL_MS SNAPSHOT_CATCHUP_MS
export SNAPSHOT_CHUNKS SNAPSHOT_PREFIX_WRITES SNAPSHOT_SUFFIX_WRITES
export MOVEMENT_TRANSFERS MOVEMENT_CONTROL_TPUT MOVEMENT_STORM_TPUT
export MOVEMENT_RETAINED_PCT MOVEMENT_HTTP_ERRORS MOVEMENT_CONTROL_P99_MS MOVEMENT_P99_MS
export MOVEMENT_BATCHES MOVEMENT_BATCH_SIZE MOVEMENT_CONNECTIONS MOVEMENT_HOSTS
export GATE_MAX_NODE_FAILURE_ERROR_BPS GATE_MAX_FAILOVER_RECOVERY_MS
export GATE_MAX_FAILOVER_QUERY_P99_MS GATE_MAX_SNAPSHOT_INSTALL_MS
export GATE_MAX_SNAPSHOT_CATCHUP_MS GATE_MAX_MOVEMENT_P99_MS GATE_MIN_DIP_PCT
export GATE_MAX_STORM_5XX
export SLO_POLICY_FILE SLO_POLICY_STATUS SLO_POLICY_SHA256 SLO_POLICY_SOURCE_SHA256

python3 - "$REPORT" <<'PY'
import json
import os
import pathlib
import sys

import production_slo_policy

def integer(name):
    return int(os.environ[name])

def number(name):
    return float(os.environ[name])

if os.environ["SLO_POLICY_STATUS"] == "approved":
    policy_document, policy_sha256, policy_source_sha256 = production_slo_policy.load_policy(
        pathlib.Path(os.environ["SLO_POLICY_FILE"])
    )
    if (
        policy_sha256 != os.environ["SLO_POLICY_SHA256"]
        or policy_source_sha256 != os.environ["SLO_POLICY_SOURCE_SHA256"]
    ):
        raise SystemExit("approved SLO policy identity changed before report publication")
else:
    policy_document = None
    policy_sha256 = None
    policy_source_sha256 = None

report = {
    "version": 1,
    "generated_at": os.environ["GENERATED_AT"],
    "host": os.environ["HOST"],
    "candidate": {
        "commit": os.environ["COMMIT"],
        "server": {
            "binary": os.environ["BIN"],
            "embedded_revision": os.environ["SERVER_REVISION"],
            "sha256": os.environ["SERVER_SHA256"],
        },
        "benchmark": {
            "binary": os.environ["BENCH"],
            "embedded_revision": os.environ["BENCH_REVISION"],
            "sha256": os.environ["BENCH_SHA256"],
        },
    },
    "slo_policy": {
        "status": os.environ["SLO_POLICY_STATUS"],
        "sha256": policy_sha256,
        "source_sha256": policy_source_sha256,
        "document": policy_document,
    },
    "settings": {
        "high_volume_server_memory_per_process": os.environ["GATE_SERVER_MEMORY"],
        "high_volume_server_smp": integer("GATE_SERVER_SMP"),
        "bench_smp": integer("GATE_BENCH_SMP"),
        "bench_memory": os.environ["GATE_BENCH_MEMORY"],
        "server_environment_sha256": os.environ["SERVER_ENV_SHA256"],
        "node_failure_smp": integer("GATE_SERVER_SMP"),
        "snapshot_catchup_server_memory_per_process": os.environ["GATE_SNAPSHOT_SERVER_MEMORY"],
        "snapshot_catchup_smp": integer("GATE_SNAPSHOT_SERVER_SMP"),
        "movement_smp": integer("GATE_SERVER_SMP"),
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
            "batch_size": integer("NODE_BATCH_SIZE"),
            "connections": integer("NODE_CONNECTIONS"),
            "hosts": integer("NODE_HOSTS"),
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
echo "  candidate: $COMMIT"
echo "  SLO policy: $SLO_POLICY_STATUS${SLO_POLICY_SHA256:+ ($SLO_POLICY_SHA256)}"
echo "  server sha256: $SERVER_SHA256"
echo "  benchmark sha256: $BENCH_SHA256"
echo "  node failure: $NODE_ERRORS/$NODE_BATCHES errors, ${NODE_RECOVERY_MS}ms recovery, ${NODE_QUERY_P99_MS}ms query p99"
echo "  snapshot catch-up: ${SNAPSHOT_INSTALL_MS}ms install, ${SNAPSHOT_CATCHUP_MS}ms exact readback"
echo "  movement: ${MOVEMENT_RETAINED_PCT}% throughput retained, ${MOVEMENT_P99_MS}ms p99"
