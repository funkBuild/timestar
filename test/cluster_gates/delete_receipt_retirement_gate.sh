#!/bin/bash
# P1 PRODUCTION-SERVER GATE: keep one VShard under sustained exact-delete load
# past its 1,024-receipt capacity and prove the entire bounded-retirement chain:
#
#   * every replica retains at most 1,024 receipts;
#   * the replicated retirement floor advances in two distinct waves;
#   * an evicted retry is a stable 409 while a retained retry is still a no-op 200;
#   * each replica snapshots through the entry that advanced its floor;
#   * each private v1 Raft journal reclaims at least one sealed segment; and
#   * repeated compaction retains only the current canonical snapshot sidecar.
#
# The deliberately long measurement makes each otherwise tiny delete entry about
# 2 KiB. This crosses the production 1-MiB private-journal rotation target with
# only 1,100 requests, keeping the gate low-volume while exercising real segment
# rotation and deletion. Three one-reactor, 1-GiB processes are the only material
# memory users, and every durable/request artifact lives under build/tmp.
#
# Usage: delete_receipt_retirement_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
GATE_SHUTDOWN_TIMEOUT_SECONDS="${GATE_SHUTDOWN_TIMEOUT_SECONDS:-120}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }

PORTS="19870 19871 19872"
PREFIX=1987
CLUSTER_UUID="30112233445566778899aabbccddeeff"
PEERS="127.0.0.1:19870,127.0.0.1:19871,127.0.0.1:19872"
GATE_ROOT="$BUILD_DIR/tmp"
DIR1="$GATE_ROOT/tsgate_dr1"
DIR2="$GATE_ROOT/tsgate_dr2"
DIR3="$GATE_ROOT/tsgate_dr3"
WORK="$GATE_ROOT/tsgate_dr_work"
DIRS="$DIR1 $DIR2 $DIR3 $WORK"
CAPACITY=1024
FIRST_WAVE=1025
TOTAL_DELETES=1100
RETRYABLE_DELETE_RESPONSES=0
declare -a NODE_PIDS

mkdir -p "$GATE_ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs $DIRS || exit 2
trap 'gate_cleanup 1987 "$DIR1" "$DIR2" "$DIR3" "$WORK"' EXIT

node_port() { echo $((19869 + $1)); }
node_dir() {
    case "$1" in
        1) echo "$DIR1" ;;
        2) echo "$DIR2" ;;
        3) echo "$DIR3" ;;
        *) return 2 ;;
    esac
}

start_node() {
    local node="$1" dir port
    dir=$(node_dir "$node")
    port=$(node_port "$node")
    env $GATE_SERVER_ENV \
        TIMESTAR_DATA_DIR="$dir" \
        TIMESTAR_SHUTDOWN_TIMEOUT_SECONDS="$GATE_SHUTDOWN_TIMEOUT_SECONDS" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID="$CLUSTER_UUID" \
        TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true \
        TIMESTAR_CLUSTER_NODE_ID="$node" TIMESTAR_CLUSTER_PEERS="$PEERS" \
        TIMESTAR_CLUSTER_SHARED_JOURNAL=0 \
        TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES=32 TIMESTAR_CLUSTER_SNAPSHOT_BYTES=0 \
        TIMESTAR_CLUSTER_SNAPSHOT_MIN_INTERVAL_S=1 TIMESTAR_WAL_SIZE_THRESHOLD=4096 \
        "$BIN" --port "$port" --smp 1 --memory "$GATE_SERVER_MEMORY" \
        >>"$dir/s.log" 2>&1 &
    NODE_PIDS[$node]=$!
}

for node in 1 2 3; do start_node "$node"; done
wait_all_led "$PORTS" 4096 180 || gate_exit
wait_leadership_settled "$PORTS" 40 || gate_exit

# The maximum legal measurement is 10,000 bytes. Keep ample framing room
# while making every replicated delete large enough to rotate real journals.
printf -v MEASUREMENT '%*s' 2000 ''
MEASUREMENT=${MEASUREMENT// /r}
SERIES_KEY="$MEASUREMENT,host=receipt value"
POINT_TS=1700000000000000000
WRITE_BODY="{\"measurement\":\"$MEASUREMENT\",\"tags\":{\"host\":\"receipt\"},\"fields\":{\"value\":1.0},\"timestamp\":$POINT_TS}"

echo "=== seed and flush one long-key series ==="
for i in $(seq 0 3); do
    code=$(curl -sS -m15 -o "$WORK/write.json" -w '%{http_code}' \
        -X POST 'http://127.0.0.1:19870/write' -H 'Content-Type: application/json' \
        -d "${WRITE_BODY/$POINT_TS/$((POINT_TS + i))}")
    assert_eq "seed write $((i + 1)) HTTP status" "$code" 200
done

TSM_READY=0
for _ in $(seq 1 60); do
    TSM_READY=1
    for dir in "$DIR1" "$DIR2" "$DIR3"; do
        find "$dir" -type f -name '*.tsm' -print -quit 2>/dev/null | grep -q . || TSM_READY=0
    done
    [ "$TSM_READY" = 1 ] && break
    sleep 1
done
assert_eq "all replicas materialized seed TSM data" "$TSM_READY" 1

# `%3N` is not portable: some `date` implementations ignore the width and
# append all nine nanosecond digits, producing a 19-digit value the API rightly
# rejects as far-future. Whole seconds expressed in milliseconds are exact and
# leave plenty of room for this gate's 1.1-second monotonic sequence.
BASE_MS=$(date +%s)000
DELETE_BODY="{\"series\":\"$SERIES_KEY\",\"startTime\":$POINT_TS,\"endTime\":$POINT_TS}"

run_delete_wave() { # first last
    local first="$1" last="$2" i key issued code attempt
    for i in $(seq "$first" "$last"); do
        printf -v key '%032x' "$i"
        issued=$((BASE_MS + i))
        code=000
        for attempt in $(seq 1 20); do
            code=$(curl -sS -m15 -o "$WORK/delete.json" -w '%{http_code}' \
                -X POST 'http://127.0.0.1:19870/delete' -H 'Content-Type: application/json' \
                -H "Idempotency-Key: $key" -H "Idempotency-Key-Timestamp: $issued" \
                -d "$DELETE_BODY")
            [ "$code" = 200 ] && break
            case "$code" in 000|503) ;; *) break ;; esac
            RETRYABLE_DELETE_RESPONSES=$((RETRYABLE_DELETE_RESPONSES + 1))
            sleep 0.1
        done
        if [ "$code" != 200 ]; then
            gate_fail "delete $i did not commit after $attempt attempt(s), last HTTP $code "\
                "(body: $(head -c 300 "$WORK/delete.json" 2>/dev/null))"
            return 1
        fi
        if [ $((i % 100)) -eq 0 ]; then echo "  committed $i/$TOTAL_DELETES deletes"; fi
    done
}

receipt_census() {
    local p status retained max capacity groups pending floor index
    CENSUS_MIN_FLOOR=0
    CENSUS_MIN_RETAINED=0
    CENSUS_MAX_RETAINED=0
    CENSUS_MAX_PER_VSHARD=0
    CENSUS_CAPACITY=0
    CENSUS_MIN_GROUPS=0
    CENSUS_PENDING=0
    CENSUS_MIN_INDEX=0
    local first=1
    for p in $PORTS; do
        status=$(cluster_status "$p")
        retained=$(status_field "$status" delete_receipts_retained)
        max=$(status_field "$status" delete_receipts_max_per_vshard)
        capacity=$(status_field "$status" delete_receipt_capacity_per_vshard)
        groups=$(status_field "$status" delete_receipt_groups_with_retired_floor)
        pending=$(status_field "$status" delete_receipt_retirement_snapshot_pending)
        floor=$(status_field "$status" delete_receipt_retired_before_max_ms)
        index=$(status_field "$status" delete_receipt_retired_at_max_index)
        if [ -z "$retained" ] || [ -z "$max" ] || [ -z "$floor" ]; then
            return 1
        fi
        CENSUS_MAX_RETAINED=$((retained > CENSUS_MAX_RETAINED ? retained : CENSUS_MAX_RETAINED))
        CENSUS_MAX_PER_VSHARD=$((max > CENSUS_MAX_PER_VSHARD ? max : CENSUS_MAX_PER_VSHARD))
        CENSUS_CAPACITY=$capacity
        CENSUS_PENDING=$((CENSUS_PENDING + pending))
        if [ "$first" = 1 ]; then
            CENSUS_MIN_FLOOR=$floor
            CENSUS_MIN_RETAINED=$retained
            CENSUS_MIN_GROUPS=$groups
            CENSUS_MIN_INDEX=$index
            first=0
        else
            [ "$floor" -lt "$CENSUS_MIN_FLOOR" ] && CENSUS_MIN_FLOOR=$floor
            [ "$retained" -lt "$CENSUS_MIN_RETAINED" ] && CENSUS_MIN_RETAINED=$retained
            [ "$groups" -lt "$CENSUS_MIN_GROUPS" ] && CENSUS_MIN_GROUPS=$groups
            [ "$index" -lt "$CENSUS_MIN_INDEX" ] && CENSUS_MIN_INDEX=$index
        fi
    done
    return 0
}

echo "=== drive the first capacity-crossing delete wave ==="
run_delete_wave 1 "$FIRST_WAVE" || gate_exit

# Four seed points were written and every receipt targets only POINT_TS. Prove
# the canonical raw-series key erased that real point; receipt accounting alone
# also advances for a syntactically valid no-op target.
DELETE_READBACK_OK=0
for _ in $(seq 1 30); do
    DELETE_READBACK_OK=1
    for p in $PORTS; do
        response=$(curl -sS -m20 -X POST "http://127.0.0.1:$p/query" -H 'Content-Type: application/json' \
            -d "{\"query\":\"count:$MEASUREMENT(value){host:receipt}\",\"startTime\":$((POINT_TS - 1)),\"endTime\":$((POINT_TS + 4))}")
        count=$(printf '%s' "$response" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2)
        [ "${count:-0}" -eq 3 ] || DELETE_READBACK_OK=0
    done
    [ "$DELETE_READBACK_OK" = 1 ] && break
    sleep 1
done
assert_eq "exact raw-series delete removed one real seed point on every node" "$DELETE_READBACK_OK" 1

FIRST_CAPACITY_FLOOR=$((BASE_MS + 1))
for _ in $(seq 1 60); do
    receipt_census && [ "$CENSUS_MIN_FLOOR" -ge "$FIRST_CAPACITY_FLOOR" ] && \
        [ "$CENSUS_MIN_RETAINED" -eq "$CAPACITY" ] && [ "$CENSUS_MIN_GROUPS" -ge 1 ] && break
    sleep 1
done
assert_eq "reported per-VShard receipt capacity" "$CENSUS_CAPACITY" "$CAPACITY"
assert_eq "minimum retained receipts across replicas" "$CENSUS_MIN_RETAINED" "$CAPACITY"
assert_eq "maximum retained receipts across replicas" "$CENSUS_MAX_RETAINED" "$CAPACITY"
assert_le "maximum receipts in one VShard" "$CENSUS_MAX_PER_VSHARD" "$CAPACITY"
assert_ge "replicas with a retired floor" "$CENSUS_MIN_GROUPS" 1
assert_ge "first capacity retirement floor" "$CENSUS_MIN_FLOOR" "$FIRST_CAPACITY_FLOOR"
FLOOR_AFTER_FIRST_WAVE=$CENSUS_MIN_FLOOR

echo "=== keep deleting and prove the floor advances rather than stopping at capacity ==="
run_delete_wave $((FIRST_WAVE + 1)) "$TOTAL_DELETES" || gate_exit
FINAL_CAPACITY_FLOOR=$((BASE_MS + TOTAL_DELETES - CAPACITY))
for _ in $(seq 1 60); do
    receipt_census && [ "$CENSUS_MIN_FLOOR" -ge "$FINAL_CAPACITY_FLOOR" ] && \
        [ "$CENSUS_MIN_RETAINED" -eq "$CAPACITY" ] && break
    sleep 1
done
assert_ge "second capacity retirement floor" "$CENSUS_MIN_FLOOR" "$FINAL_CAPACITY_FLOOR"
assert_eq "minimum retained receipts after sustained load" "$CENSUS_MIN_RETAINED" "$CAPACITY"
assert_eq "maximum retained receipts after sustained load" "$CENSUS_MAX_RETAINED" "$CAPACITY"
assert_eq "maximum per-VShard receipts after sustained load" "$CENSUS_MAX_PER_VSHARD" "$CAPACITY"

printf -v FIRST_KEY '%032x' 1
FIRST_TS=$((BASE_MS + 1))
OLD_CODE=$(curl -sS -m15 -o "$WORK/old_retry.json" -w '%{http_code}' \
    -X POST 'http://127.0.0.1:19871/delete' -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $FIRST_KEY" -H "Idempotency-Key-Timestamp: $FIRST_TS" -d "$DELETE_BODY")
assert_eq "evicted delete retry HTTP status" "$OLD_CODE" 409

printf -v LATEST_KEY '%032x' "$TOTAL_DELETES"
LATEST_TS=$((BASE_MS + TOTAL_DELETES))
LATEST_CODE=$(curl -sS -m15 -o "$WORK/latest_retry.json" -w '%{http_code}' \
    -X POST 'http://127.0.0.1:19872/delete' -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $LATEST_KEY" -H "Idempotency-Key-Timestamp: $LATEST_TS" -d "$DELETE_BODY")
assert_eq "retained delete retry HTTP status" "$LATEST_CODE" 200

echo "=== wait for retirement-covering snapshots and sealed-segment GC ==="
SNAPSHOT_COMPLETE=0
GC_COMPLETE=0
for _ in $(seq 1 150); do
    receipt_census || { sleep 1; continue; }
    SNAPSHOT_COMPLETE=1
    GC_COMPLETE=1
    for p in $PORTS; do
        status=$(cluster_status "$p")
        snapshots=$(status_field "$status" snapshots_taken)
        pending=$(status_field "$status" delete_receipt_retirement_snapshot_pending)
        deleted=$(status_field "$status" journal_segments_deleted)
        [ "${snapshots:-0}" -ge 1 ] || SNAPSHOT_COMPLETE=0
        [ "${pending:-1}" -eq 0 ] || SNAPSHOT_COMPLETE=0
        [ "${deleted:-0}" -ge 1 ] || GC_COMPLETE=0
    done
    [ "$SNAPSHOT_COMPLETE" = 1 ] && [ "$GC_COMPLETE" = 1 ] && break
    sleep 1
done
assert_eq "every replica snapshotted through its retirement entry" "$SNAPSHOT_COMPLETE" 1
assert_eq "every replica reclaimed a sealed Raft journal segment" "$GC_COMPLETE" 1

# Repeated snapshots used to leave each superseded sidecar on disk while a
# stale shared handle survived elsewhere in Raft. There is exactly one durable
# descriptor per VShard, so no private sidecar directory may retain more than
# one canonical payload after the gate has quiesced.
SIDECAR_LEAKS=0
for dir in "$DIR1" "$DIR2" "$DIR3"; do
    leaks=$(find "$dir/cluster_raft" -type f -path '*/snapshot_sidecars/snapshot_v1_g*.bin' \
        -printf '%h\n' 2>/dev/null | sort | uniq -c | awk '$1 > 1 { count++ } END { print count + 0 }')
    SIDECAR_LEAKS=$((SIDECAR_LEAKS + leaks))
done
assert_eq "VShards retaining superseded snapshot sidecars" "$SIDECAR_LEAKS" 0

for p in $PORTS; do
    status=$(cluster_status "$p")
    echo "  node $p: retained=$(status_field "$status" delete_receipts_retained)" \
         "max_per_vshard=$(status_field "$status" delete_receipts_max_per_vshard)" \
         "floor_ms=$(status_field "$status" delete_receipt_retired_before_max_ms)" \
         "retirement_index=$(status_field "$status" delete_receipt_retired_at_max_index)" \
         "snapshot_pending=$(status_field "$status" delete_receipt_retirement_snapshot_pending)" \
         "snapshots=$(status_field "$status" snapshots_taken)" \
         "journal_segments_deleted=$(status_field "$status" journal_segments_deleted)"
done

echo "GATE_METRIC delete_receipt_floor_first_ms $FLOOR_AFTER_FIRST_WAVE"
echo "GATE_METRIC delete_receipt_floor_final_ms $CENSUS_MIN_FLOOR"
echo "GATE_METRIC delete_receipts_max_per_vshard $CENSUS_MAX_PER_VSHARD"
echo "GATE_METRIC retryable_delete_responses $RETRYABLE_DELETE_RESPONSES"
gate_exit
