#!/bin/bash
# P1 PRODUCTION-SERVER GATE: export one live RF=3 cluster, interrupt and
# resume the durable coordinator, reject malformed artifacts, prepare a fresh
# three-voter cluster through an interrupted import, enforce the all-voter
# release ceremony, and read the acknowledged baseline back after restart.
#
# At most three one-reactor, 1-GiB servers run at once. Source and restored
# clusters reuse the same ports only after the source is dead, and every root,
# archive, response, log, and process temporary lives under build/tmp.
#
# Usage: cluster_backup_restore_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
GATE_SHUTDOWN_TIMEOUT_SECONDS="${GATE_SHUTDOWN_TIMEOUT_SECONDS:-120}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
RESTORE_BIN="$(dirname "$BIN")/timestar_cluster_restore"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }
[ -x "$RESTORE_BIN" ] || { echo "no restore finalizer at $RESTORE_BIN"; exit 2; }

PORTS="19940 19941 19942"
PREFIX=1994
PEERS="127.0.0.1:19940,127.0.0.1:19941,127.0.0.1:19942"
AUTH_TOKEN="backup-restore-gate-operator-token"
SOURCE_UUID="20112233445566778899aabbccddeeff"
RESTORED_UUID="30112233445566778899aabbccddeeff"
OPERATION_ID="11111111111111111111111111111111"
BASE_TS=1785715000000000000
BASE_POINTS="${GATE_BASE_POINTS:-24}"
LIVE_POINTS_PER_ARM="${GATE_LIVE_POINTS_PER_ARM:-24}"
GATE_ROOT="$GATE_TMP_ROOT"
WORK="$GATE_ROOT/tsgate_br_work"
ARCHIVE="$GATE_ROOT/tsgate_br_archive"
EXPORT_STATE="$ARCHIVE.export.v1"
SOURCE1="$GATE_ROOT/tsgate_br_source1"
SOURCE2="$GATE_ROOT/tsgate_br_source2"
SOURCE3="$GATE_ROOT/tsgate_br_source3"
RESTORE1="$GATE_ROOT/tsgate_br_restore1"
RESTORE2="$GATE_ROOT/tsgate_br_restore2"
RESTORE3="$GATE_ROOT/tsgate_br_restore3"
BADROOT1="$GATE_ROOT/tsgate_br_badroot1"
BADROOT2="$GATE_ROOT/tsgate_br_badroot2"
BADROOT3="$GATE_ROOT/tsgate_br_badroot3"
BADROOT4="$GATE_ROOT/tsgate_br_badroot4"
MISSING_ARCHIVE="$GATE_ROOT/tsgate_br_missing"
CORRUPT_ARCHIVE="$GATE_ROOT/tsgate_br_corrupt"
EXTRA_ARCHIVE="$GATE_ROOT/tsgate_br_extra"
RELEASE="$WORK/restore.tsrr1"
BAD_RELEASE="$WORK/incomplete.tsrr1"
BACKUP_KEY="$WORK/backup-auth.key"
WRONG_BACKUP_KEY="$WORK/wrong-backup-auth.key"
ALL_ROOTS="$SOURCE1 $SOURCE2 $SOURCE3 $RESTORE1 $RESTORE2 $RESTORE3 $BADROOT1 $BADROOT2 $BADROOT3 $BADROOT4 $WORK $ARCHIVE $EXPORT_STATE $MISSING_ARCHIVE $CORRUPT_ARCHIVE $EXTRA_ARCHIVE"
declare -a NODE_PIDS
declare -a SOURCE_NODE_UUIDS

mkdir -p "$GATE_ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs "$SOURCE1" "$SOURCE2" "$SOURCE3" "$RESTORE1" "$RESTORE2" "$RESTORE3" \
    "$BADROOT1" "$BADROOT2" "$BADROOT3" "$BADROOT4" "$WORK" "$ARCHIVE" || exit 2
remove_gate_data_dirs "$EXPORT_STATE" "$MISSING_ARCHIVE" "$CORRUPT_ARCHIVE" "$EXTRA_ARCHIVE" || exit 2
printf '%s\n' '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' >"$BACKUP_KEY"
printf '%s\n' '1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' >"$WRONG_BACKUP_KEY"
chmod 600 "$BACKUP_KEY" "$WRONG_BACKUP_KEY"
trap 'gate_cleanup 1994 $ALL_ROOTS' EXIT

node_port() { echo $((19939 + $1)); }

source_dir() {
    case "$1" in
        1) echo "$SOURCE1" ;;
        2) echo "$SOURCE2" ;;
        3) echo "$SOURCE3" ;;
        *) return 2 ;;
    esac
}

restore_dir() {
    case "$1" in
        1) echo "$RESTORE1" ;;
        2) echo "$RESTORE2" ;;
        3) echo "$RESTORE3" ;;
        *) return 2 ;;
    esac
}

launch_node() { # NODE DATA_DIR CLUSTER_UUID LOG [server arguments...]
    local node="$1" dir="$2" uuid="$3" log="$4"
    shift 4
    env $GATE_SERVER_ENV TMPDIR="$WORK" \
        TIMESTAR_DATA_DIR="$dir" \
        TIMESTAR_SHUTDOWN_TIMEOUT_SECONDS="$GATE_SHUTDOWN_TIMEOUT_SECONDS" \
        TIMESTAR_AUTH_ENABLED=true TIMESTAR_AUTH_TOKEN="$AUTH_TOKEN" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID="$uuid" \
        TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true \
        TIMESTAR_CLUSTER_CONTROL_ENABLED=true TIMESTAR_CLUSTER_CONTROL_SEED_NODE_ID=1 \
        TIMESTAR_CLUSTER_BACKUP_AUTH_KEY_FILE="$BACKUP_KEY" \
        TIMESTAR_CLUSTER_FAILURE_DOMAIN="rack-$node" \
        TIMESTAR_CLUSTER_NODE_ID="$node" TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port "$(node_port "$node")" --smp 1 --memory "$GATE_SERVER_MEMORY" "$@" \
        >>"$log" 2>&1 &
    NODE_PIDS[$node]=$!
}

start_source_node() { # NODE [init]
    local node="$1" init="${2:-}" dir
    local -a args
    dir=$(source_dir "$node")
    args=()
    [ "$init" = "init" ] && args=(--cluster-init)
    launch_node "$node" "$dir" "$SOURCE_UUID" "$dir/source.log" "${args[@]}"
}

start_restored_node() { # NODE [activation arguments...]
    local node="$1" dir
    shift
    dir=$(restore_dir "$node")
    launch_node "$node" "$dir" "$RESTORED_UUID" "$dir/restored.log" "$@"
}

kill_node() { # NODE
    local node="$1" pid="${NODE_PIDS[$1]:-}"
    [ -n "$pid" ] || return 0
    kill -9 "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    NODE_PIDS[$node]=""
}

forget_cluster_pids() {
    NODE_PIDS[1]=""
    NODE_PIDS[2]=""
    NODE_PIDS[3]=""
}

control_leader() { # optional candidate ports
    local ports="${1:-$PORTS}" p status leader
    for p in $ports; do
        status=$(cluster_status "$p")
        leader=$(status_field "$status" control_leader)
        if [ -n "$leader" ] && [ "$leader" -ge 1 ] && [ "$leader" -le 3 ]; then
            echo "$leader"
            return 0
        fi
    done
    return 1
}

wait_control_leader() { # PORTS [excluded node]
    local ports="$1" excluded="${2:-0}" poll leader
    for poll in $(seq 1 180); do
        leader=$(control_leader "$ports" 2>/dev/null || true)
        if [ -n "$leader" ] && [ "$leader" -ne "$excluded" ]; then
            echo "$leader"
            return 0
        fi
        sleep 1
    done
    gate_fail "Group-0 did not elect an eligible leader"
    return 1
}

mint_join_token() {
    local leader port code body poll
    JOIN_TOKEN=""
    for poll in $(seq 1 60); do
        leader=$(control_leader 2>/dev/null || true)
        [ -n "$leader" ] || { sleep 1; continue; }
        port=$(node_port "$leader")
        code=$(curl -sS -m10 -o "$WORK/token.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join-token" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' -d '{}' 2>/dev/null)
        body=$(sed -n '1p' "$WORK/token.json" 2>/dev/null)
        if [ "$code" = "200" ]; then
            JOIN_TOKEN=$(printf '%s' "$body" | grep -o '"token":"[^"]*"' | cut -d'"' -f4)
            [ -n "$JOIN_TOKEN" ] && return 0
        fi
        sleep 1
    done
    gate_fail "could not mint a Group-0 join token"
    return 1
}

join_node_with_token() { # NODE TOKEN
    local node="$1" token="$2" code body poll port
    port=$(node_port "$node")
    for poll in $(seq 1 180); do
        code=$(curl -sS -m10 -o "$WORK/join_$node.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"token\":\"$token\"}" 2>/dev/null)
        body=$(sed -n '1p' "$WORK/join_$node.json" 2>/dev/null)
        if [ "$code" = "200" ] && printf '%s' "$body" | grep -q '"status":"active"'; then
            return 0
        fi
        if [ "$code" != "200" ] && [ "$code" != "409" ] && [ "$code" != "503" ]; then
            gate_fail "node $node join returned HTTP ${code:-unreachable}: ${body:-<empty>}"
            return 1
        fi
        sleep 1
    done
    gate_fail "node $node did not become Active through Group 0"
    return 1
}

join_node() { # NODE
    mint_join_token || return 1
    join_node_with_token "$1" "$JOIN_TOKEN"
}

wait_control_voters() { # COUNT PORTS
    local expected="$1" ports="$2" poll p voters ok
    for poll in $(seq 1 240); do
        ok=1
        for p in $ports; do
            voters=$(status_field "$(cluster_status "$p")" control_voters)
            [ "${voters:-0}" -eq "$expected" ] || { ok=0; break; }
        done
        [ "$ok" = 1 ] && return 0
        sleep 1
    done
    gate_fail "Group-0 voter set did not converge to $expected"
    return 1
}

write_until_acked() { # MEASUREMENT TIMESTAMP VALUE
    local measurement="$1" timestamp="$2" value="$3" attempt port code
    for attempt in $(seq 1 60); do
        port=$(node_port $((1 + (attempt % 3))))
        code=$(curl -sS -m15 -o "$WORK/write.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/write" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"measurement\":\"$measurement\",\"tags\":{\"host\":\"one\"},\"fields\":{\"value\":$value},\"timestamp\":$timestamp}" \
            2>/dev/null)
        [ "$code" -ge 200 ] 2>/dev/null && [ "$code" -lt 300 ] && return 0
        sleep 1
    done
    gate_fail "write for $measurement at $timestamp was not acknowledged"
    return 1
}

query_count() { # PORT MEASUREMENT START END
    local port="$1" measurement="$2" start="$3" end="$4" response
    response=$(curl -sS -m30 -X POST "http://127.0.0.1:$port/query" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:$measurement(value){}\",\"startTime\":$start,\"endTime\":$end}" \
        2>/dev/null)
    printf '%s' "$response" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2
}

wait_query_count() { # PORT MEASUREMENT START END EXPECTED
    local port="$1" measurement="$2" start="$3" end="$4" expected="$5" poll count
    for poll in $(seq 1 120); do
        count=$(query_count "$port" "$measurement" "$start" "$end")
        [ "${count:-}" = "$expected" ] && return 0
        sleep 1
    done
    gate_fail "$measurement count on :$port was ${count:-missing}, expected $expected"
    return 1
}

backup_status() { # coordinator port
    curl -sS -m10 "http://127.0.0.1:$1/cluster/backup/export" \
        -H "Authorization: Bearer $AUTH_TOKEN" 2>/dev/null
}

start_or_resume_export() {
    local poll leader port code body
    for poll in $(seq 1 60); do
        leader=$(control_leader 2>/dev/null || true)
        [ -n "$leader" ] || { sleep 1; continue; }
        port=$(node_port "$leader")
        code=$(curl -sS -m20 -o "$WORK/export_start.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/backup/export" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"archive_directory\":\"$ARCHIVE\",\"operation_id\":\"$OPERATION_ID\"}" 2>/dev/null)
        body=$(sed -n '1p' "$WORK/export_start.json" 2>/dev/null)
        if [ "$code" = "202" ] || [ "$code" = "200" ]; then
            EXPORT_NODE="$leader"
            EXPORT_PORT="$port"
            EXPORT_BODY="$body"
            return 0
        fi
        sleep 1
    done
    gate_fail "backup export could not start or resume: ${body:-unreachable}"
    return 1
}

write_live_arm() { # label coordinator-port
    local label="$1" coordinator="$2" i status phase code
    local attempted=0 acked=0 retryable=0 other=0
    for i in $(seq 1 "$LIVE_POINTS_PER_ARM"); do
        status=$(backup_status "$coordinator")
        phase=$(printf '%s' "$status" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
        [ "$phase" = "running" ] || break
        attempted=$((attempted + 1))
        code=$(curl -sS -m15 -o "$WORK/live_write.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$coordinator/write" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"measurement\":\"backup_live\",\"tags\":{\"host\":\"one\"},\"fields\":{\"value\":$((LIVE_ATTEMPTED + 1)).5},\"timestamp\":$((BASE_TS + 1000 + LIVE_ATTEMPTED))}" \
            2>/dev/null)
        case "$code" in
            2*) acked=$((acked + 1)) ;;
            5*) retryable=$((retryable + 1)) ;;
            *) other=$((other + 1)) ;;
        esac
        LIVE_ATTEMPTED=$((LIVE_ATTEMPTED + 1))
    done
    LIVE_ACKED=$((LIVE_ACKED + acked))
    echo "  $label live writes while export reported running: attempted=$attempted acknowledged=$acked retryable=$retryable other=$other"
    assert_ge "$label acknowledged writes during export" "$acked" 1
    assert_eq "$label non-retryable write failures" "$other" 0
}

run_prepare() { # NODE DATA_DIR ARCHIVE LOG [background]
    local node="$1" dir="$2" archive="$3" log="$4" mode="${5:-foreground}"
    local -a extra_env command
    read -r -a extra_env <<<"$GATE_SERVER_ENV"
    command=(env "${extra_env[@]}" "TMPDIR=$WORK"
        "TIMESTAR_DATA_DIR=$dir"
        "TIMESTAR_AUTH_ENABLED=true" "TIMESTAR_AUTH_TOKEN=$AUTH_TOKEN"
        "TIMESTAR_CLUSTER_ENABLED=true" "TIMESTAR_CLUSTER_PARTITIONED=true"
        "TIMESTAR_CLUSTER_REPLICATION_FACTOR=3" "TIMESTAR_CLUSTER_UUID=$RESTORED_UUID"
        "TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true"
        "TIMESTAR_CLUSTER_CONTROL_ENABLED=true" "TIMESTAR_CLUSTER_CONTROL_SEED_NODE_ID=1"
        "TIMESTAR_CLUSTER_BACKUP_AUTH_KEY_FILE=$BACKUP_KEY"
        "TIMESTAR_CLUSTER_FAILURE_DOMAIN=rack-$node"
        "TIMESTAR_CLUSTER_NODE_ID=$node" "TIMESTAR_CLUSTER_PEERS=$PEERS"
        "$BIN" --port "$(node_port "$node")" --smp 1 --memory "$GATE_SERVER_MEMORY"
        --cluster-restore "$archive")
    if [ "$mode" = "background" ]; then
        "${command[@]}" >>"$log" 2>&1 &
        PREPARE_PID=$!
        return 0
    fi
    "${command[@]}" >>"$log" 2>&1
}

run_bad_archive() { # LABEL NODE DATA_DIR ARCHIVE [AUTH_KEY]
    local label="$1" node="$2" dir="$3" archive="$4" key="${5:-$BACKUP_KEY}" rc log="$WORK/bad_$1.log"
    timeout 300 env $GATE_SERVER_ENV TMPDIR="$WORK" \
        TIMESTAR_DATA_DIR="$dir" \
        TIMESTAR_AUTH_ENABLED=true TIMESTAR_AUTH_TOKEN="$AUTH_TOKEN" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID="$RESTORED_UUID" \
        TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true \
        TIMESTAR_CLUSTER_CONTROL_ENABLED=true TIMESTAR_CLUSTER_CONTROL_SEED_NODE_ID=1 \
        TIMESTAR_CLUSTER_BACKUP_AUTH_KEY_FILE="$key" \
        TIMESTAR_CLUSTER_FAILURE_DOMAIN="rack-$node" \
        TIMESTAR_CLUSTER_NODE_ID="$node" TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port "$(node_port "$node")" --smp 1 --memory "$GATE_SERVER_MEMORY" \
        --cluster-restore "$archive" >"$log" 2>&1
    rc=$?
    if [ "$rc" -ne 0 ] && grep -q 'archive is incomplete, corrupt, unauthenticated, or not exact v1' "$log"; then
        gate_ok "$label artifact was rejected before restore"
    else
        gate_fail "$label artifact rejection returned $rc without the exact validation diagnostic"
    fi
}

node_uuid() { # data directory
    grep -o '"node_uuid"[[:space:]]*:[[:space:]]*"[0-9a-f]*"' "$1/node.json" 2>/dev/null | head -1 | grep -o '[0-9a-f]\{32\}'
}

echo "=== bootstrap the source cluster and admit all Group-0 voters ==="
start_source_node 1 init
start_source_node 2
start_source_node 3
wait_all_led "$PORTS" 4096 240 || gate_exit
wait_healthy "$PORTS" 240 || gate_exit
wait_control_leader "$PORTS" >/dev/null || gate_exit
join_node 2 || gate_exit
join_node 3 || gate_exit
wait_control_voters 3 "$PORTS" || gate_exit
mint_join_token || gate_exit
OLD_JOIN_TOKEN="$JOIN_TOKEN"

echo "=== write an acknowledged pre-export baseline ==="
for i in $(seq 1 "$BASE_POINTS"); do
    write_until_acked backup_baseline $((BASE_TS + i)) "$i.5" || gate_exit
done
wait_query_count 19940 backup_baseline "$BASE_TS" $((BASE_TS + BASE_POINTS + 1)) "$BASE_POINTS" || gate_exit

echo "=== interrupt the durable export and resume it under a new Group-0 leader ==="
LIVE_ATTEMPTED=0
LIVE_ACKED=0
start_or_resume_export || gate_exit
printf '%s' "$EXPORT_BODY" | grep -q '"status":"running"' \
    && gate_ok "export started in the background" \
    || { gate_fail "export did not report running: $EXPORT_BODY"; gate_exit; }
write_live_arm before-leader-loss "$EXPORT_PORT"

CURSOR=0
for _ in $(seq 1 300); do
    STATUS=$(backup_status "$EXPORT_PORT")
    PHASE=$(printf '%s' "$STATUS" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    CURSOR=$(printf '%s' "$STATUS" | grep -o '"completed_vshards":[0-9]*' | cut -d: -f2)
    [ "$PHASE" = "running" ] && [ "${CURSOR:-0}" -ge 16 ] && [ "$CURSOR" -lt 4096 ] && break
    sleep 1
done
if [ "$PHASE" != "running" ] || [ "${CURSOR:-0}" -lt 16 ] || [ "$CURSOR" -ge 4096 ]; then
    gate_fail "did not observe an interruptible export cursor (phase=${PHASE:-missing}, cursor=${CURSOR:-missing})"
    gate_exit
fi
OLD_LEADER="$EXPORT_NODE"
gate_ok "export checkpoint retained $CURSOR completed VShards before node $OLD_LEADER was killed"
kill_node "$OLD_LEADER"

SURVIVOR_PORTS=""
for node in 1 2 3; do
    [ "$node" -ne "$OLD_LEADER" ] && SURVIVOR_PORTS="$SURVIVOR_PORTS $(node_port "$node")"
done
NEW_LEADER=$(wait_control_leader "$SURVIVOR_PORTS" "$OLD_LEADER") || gate_exit
start_or_resume_export || gate_exit
assert_eq "export resumed on a different Group-0 leader" "$EXPORT_NODE" "$NEW_LEADER"
start_source_node "$OLD_LEADER"
write_live_arm after-leader-loss "$EXPORT_PORT"

for _ in $(seq 1 1800); do
    STATUS=$(backup_status "$EXPORT_PORT")
    PHASE=$(printf '%s' "$STATUS" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)
    CURSOR=$(printf '%s' "$STATUS" | grep -o '"completed_vshards":[0-9]*' | cut -d: -f2)
    [ "$PHASE" = "complete" ] && break
    [ "$PHASE" = "failed" ] && break
    sleep 1
done
assert_eq "resumed export phase" "${PHASE:-missing}" complete
assert_eq "resumed export completed VShards" "${CURSOR:-missing}" 4096
assert_eq "published archive unit count" "$(find "$ARCHIVE/vshards" -maxdepth 1 -type f -name '*.tsp1' 2>/dev/null | wc -l)" 4096
[ -f "$ARCHIVE/manifest.tsbk1" ] \
    && gate_ok "manifest was published last" \
    || gate_fail "complete export has no manifest.tsbk1"
assert_ge "acknowledged writes while export was live" "$LIVE_ACKED" 2

SOURCE_LIVE_COUNT=$(query_count "$EXPORT_PORT" backup_live $((BASE_TS + 999)) $((BASE_TS + 1001 + LIVE_ATTEMPTED)))
assert_ge "source live-write readback" "${SOURCE_LIVE_COUNT:-0}" "$LIVE_ACKED"
assert_le "source live-write readback" "${SOURCE_LIVE_COUNT:-999999}" "$LIVE_ATTEMPTED"
for node in 1 2 3; do
    SOURCE_NODE_UUIDS[$node]=$(node_uuid "$(source_dir "$node")")
done

echo "=== stop the source before all offline validation and restore work ==="
kill_cluster "$PREFIX"
forget_cluster_pids
require_ports_free $PORTS

echo "=== reject missing, corrupt, and extra exact-v1 artifact shapes ==="
run_bad_archive unauthenticated 1 "$BADROOT4" "$ARCHIVE" "$WRONG_BACKUP_KEY"
cp -al -- "$ARCHIVE" "$MISSING_ARCHIVE"
rm -- "$MISSING_ARCHIVE/vshards/0000.tsp1"
cp -al -- "$ARCHIVE" "$CORRUPT_ARCHIVE"
cp --reflink=auto -- "$CORRUPT_ARCHIVE/vshards/0000.tsp1" "$WORK/corrupt-unit.tsp1"
mv -f -- "$WORK/corrupt-unit.tsp1" "$CORRUPT_ARCHIVE/vshards/0000.tsp1"
printf 'x' >>"$CORRUPT_ARCHIVE/vshards/0000.tsp1"
cp -al -- "$ARCHIVE" "$EXTRA_ARCHIVE"
printf 'unexpected' >"$EXTRA_ARCHIVE/extra.bin"
run_bad_archive missing 1 "$BADROOT1" "$MISSING_ARCHIVE"
run_bad_archive corrupt 2 "$BADROOT2" "$CORRUPT_ARCHIVE"
run_bad_archive extra 3 "$BADROOT3" "$EXTRA_ARCHIVE"

echo "=== interrupt one offline import, then resume all three prepared voters ==="
run_prepare 1 "$RESTORE1" "$ARCHIVE" "$RESTORE1/prepare.log" background
INTERRUPTED=0
for _ in $(seq 1 1000); do
    if [ -f "$RESTORE1/cluster_restore.v1" ] && kill -0 "$PREPARE_PID" 2>/dev/null; then
        SEEDED=$(find "$RESTORE1/cluster_raft" -type f 2>/dev/null | wc -l)
        if [ "$SEEDED" -ge 4 ]; then
            kill -9 "$PREPARE_PID" 2>/dev/null
            wait "$PREPARE_PID" 2>/dev/null
            INTERRUPTED=1
            break
        fi
    fi
    kill -0 "$PREPARE_PID" 2>/dev/null || break
    sleep 0.02
done
if [ "$INTERRUPTED" = 1 ]; then
    gate_ok "offline import was killed after durable partial progress"
else
    wait "$PREPARE_PID" 2>/dev/null
    gate_fail "offline import completed before its durable partial state could be interrupted"
    gate_exit
fi
run_prepare 1 "$RESTORE1" "$ARCHIVE" "$RESTORE1/prepare.log" || { gate_fail "node 1 import resume failed"; gate_exit; }
grep -q 'resumed=true' "$RESTORE1/prepare.log" \
    && gate_ok "node 1 import reported a durable resume" \
    || gate_fail "node 1 import did not report resumed=true"
run_prepare 2 "$RESTORE2" "$ARCHIVE" "$RESTORE2/prepare.log" || { gate_fail "node 2 import failed"; gate_exit; }
run_prepare 3 "$RESTORE3" "$ARCHIVE" "$RESTORE3/prepare.log" || { gate_fail "node 3 import failed"; gate_exit; }

for node in 1 2 3; do
    dir=$(restore_dir "$node")
    [ -f "$dir/cluster_restore.v1" ] \
        && gate_ok "node $node has a completed restore marker" \
        || gate_fail "node $node has no restore marker"
    old_uuid="${SOURCE_NODE_UUIDS[$node]:-}"
    new_uuid=$(node_uuid "$dir")
    if [ -n "$old_uuid" ] && [ -n "$new_uuid" ] && [ "$old_uuid" != "$new_uuid" ]; then
        gate_ok "node $node uses a fresh persistent identity"
    else
        gate_fail "node $node reused or omitted its source identity"
    fi
done

echo "=== refuse an incomplete release and finalize all voters exactly once ==="
"$RESTORE_BIN" finalize --output "$BAD_RELEASE" \
    "$RESTORE1/cluster_restore.v1" "$RESTORE2/cluster_restore.v1" >"$WORK/bad_release.log" 2>&1
BAD_RELEASE_RC=$?
if [ "$BAD_RELEASE_RC" -ne 0 ] && [ ! -e "$BAD_RELEASE" ] && grep -q 'missing or has extra participant markers' "$WORK/bad_release.log"; then
    gate_ok "two of three participant markers cannot authorize activation"
else
    gate_fail "incomplete all-voter finalization returned $BAD_RELEASE_RC or published a release"
fi
"$RESTORE_BIN" finalize --output "$RELEASE" \
    "$RESTORE1/cluster_restore.v1" "$RESTORE2/cluster_restore.v1" "$RESTORE3/cluster_restore.v1" \
    >"$WORK/release.log" 2>&1 || { gate_fail "complete restore finalization failed"; gate_exit; }

echo "=== kill the control seed after activation receipt, then start the fresh cluster ==="
start_restored_node 1 --cluster-restore-release "$RELEASE" --cluster-init
ACTIVATION_PID="${NODE_PIDS[1]}"
ACTIVATION_INTERRUPTED=0
for _ in $(seq 1 1000); do
    if [ -f "$RESTORE1/cluster_restore_release.v1" ] && kill -0 "$ACTIVATION_PID" 2>/dev/null; then
        kill -9 "$ACTIVATION_PID" 2>/dev/null
        wait "$ACTIVATION_PID" 2>/dev/null
        NODE_PIDS[1]=""
        ACTIVATION_INTERRUPTED=1
        break
    fi
    kill -0 "$ACTIVATION_PID" 2>/dev/null || break
    sleep 0.02
done
assert_eq "activation was interrupted after its durable receipt" "$ACTIVATION_INTERRUPTED" 1
start_restored_node 1
start_restored_node 2 --cluster-restore-release "$RELEASE"
start_restored_node 3 --cluster-restore-release "$RELEASE"
wait_all_led "$PORTS" 4096 600 || gate_exit
wait_control_leader "$PORTS" >/dev/null || gate_exit

echo "=== reject source authority, then admit only the fresh restored identities ==="
OLD_TOKEN_REJECTED=0
for _ in $(seq 1 60); do
    OLD_CODE=$(curl -sS -m10 -o "$WORK/old_token.json" -w '%{http_code}' -X POST \
        "http://127.0.0.1:19941/cluster/join" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"token\":\"$OLD_JOIN_TOKEN\"}" 2>/dev/null)
    if [ "$OLD_CODE" = "401" ] && grep -q '"status":"rejected"' "$WORK/old_token.json"; then
        OLD_TOKEN_REJECTED=1
        break
    fi
    sleep 1
done
assert_eq "source-cluster join authority is rejected" "$OLD_TOKEN_REJECTED" 1
join_node 2 || gate_exit
join_node 3 || gate_exit
wait_control_voters 3 "$PORTS" || gate_exit
wait_healthy "$PORTS" 600 || gate_exit

echo "=== require exact restored baseline and bounded concurrent-write subset ==="
EXPECTED_RESTORED_LIVE=""
for port in $PORTS; do
    wait_query_count "$port" backup_baseline "$BASE_TS" $((BASE_TS + BASE_POINTS + 1)) "$BASE_POINTS" || gate_exit
    RESTORED_LIVE=$(query_count "$port" backup_live $((BASE_TS + 999)) $((BASE_TS + 1001 + LIVE_ATTEMPTED)))
    assert_ge "restored concurrent subset on :$port" "${RESTORED_LIVE:-0}" 0
    assert_le "restored concurrent subset on :$port" "${RESTORED_LIVE:-999999}" "${SOURCE_LIVE_COUNT:-0}"
    if [ -z "$EXPECTED_RESTORED_LIVE" ]; then
        EXPECTED_RESTORED_LIVE="$RESTORED_LIVE"
    else
        assert_eq "restored concurrent subset agrees on :$port" "$RESTORED_LIVE" "$EXPECTED_RESTORED_LIVE"
    fi
done

echo "=== restart every restored voter over imported journals and read back again ==="
kill_cluster "$PREFIX"
forget_cluster_pids
require_ports_free $PORTS
start_restored_node 1
start_restored_node 2
start_restored_node 3
wait_all_led "$PORTS" 4096 600 || gate_exit
wait_healthy "$PORTS" 600 || gate_exit
for port in $PORTS; do
    wait_query_count "$port" backup_baseline "$BASE_TS" $((BASE_TS + BASE_POINTS + 1)) "$BASE_POINTS" || gate_exit
    wait_query_count "$port" backup_live $((BASE_TS + 999)) $((BASE_TS + 1001 + LIVE_ATTEMPTED)) \
        "$EXPECTED_RESTORED_LIVE" || gate_exit
done

gate_exit
