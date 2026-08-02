#!/bin/bash
# P0 PRODUCTION-SERVER GATE: prove that a clustered pattern delete keeps the
# first Group-0-frozen target expansion through an ambiguous coordinator crash,
# leader failover, retry, and restart.
#
# The gate writes 6,000 small series so the post-freeze VShard fan-out remains
# in flight long enough to observe without a synthetic sleep/failpoint. It
# polls a different Group-0 voter until that voter has applied the complete
# frozen plan, then kills the HTTP coordinator before curl receives a reply.
# A newly matching series is written after the crash. Retrying the byte-exact
# request with its original idempotency headers must delete only the 6,000
# frozen targets and leave the new match untouched.
#
# Three one-reactor, 1-GiB processes are the only substantial memory users.
# All server and request data lives under build/tmp and is removed on exit.
# Run this gate alone.
#
# Usage: pattern_delete_failover_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
GATE_SHUTDOWN_TIMEOUT_SECONDS="${GATE_SHUTDOWN_TIMEOUT_SECONDS:-120}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }

PORTS="19850 19851 19852"
PREFIX=1985
AUTH_TOKEN="pattern-delete-gate-operator-token"
CLUSTER_UUID="20112233445566778899aabbccddeeff"
PEERS="127.0.0.1:19850,127.0.0.1:19851,127.0.0.1:19852"
GATE_ROOT="$BUILD_DIR/tmp"
DIR1="$GATE_ROOT/tsgate_pd1"
DIR2="$GATE_ROOT/tsgate_pd2"
DIR3="$GATE_ROOT/tsgate_pd3"
WORK="$GATE_ROOT/tsgate_pd_work"
DIRS="$DIR1 $DIR2 $DIR3 $WORK"
TARGETS=6000
BATCH_SIZE=500
POINT_TS=1700000000000000000
declare -a NODE_PIDS

mkdir -p "$GATE_ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs $DIRS || exit 2
trap 'gate_cleanup 1985 "$DIR1" "$DIR2" "$DIR3" "$WORK"' EXIT

node_port() { echo $((19849 + $1)); }
node_dir() {
    case "$1" in
        1) echo "$DIR1" ;;
        2) echo "$DIR2" ;;
        3) echo "$DIR3" ;;
        *) return 2 ;;
    esac
}

start_node() { # NODE [init]
    local node="$1" init="${2:-}" dir port
    local -a init_arg
    dir=$(node_dir "$node")
    port=$(node_port "$node")
    init_arg=()
    [ "$init" = "init" ] && init_arg=(--cluster-init)
    env $GATE_SERVER_ENV \
        TIMESTAR_DATA_DIR="$dir" \
        TIMESTAR_SHUTDOWN_TIMEOUT_SECONDS="$GATE_SHUTDOWN_TIMEOUT_SECONDS" \
        TIMESTAR_AUTH_ENABLED=true TIMESTAR_AUTH_TOKEN="$AUTH_TOKEN" \
        TIMESTAR_CLUSTER_ENABLED=true TIMESTAR_CLUSTER_PARTITIONED=true \
        TIMESTAR_CLUSTER_REPLICATION_FACTOR=3 TIMESTAR_CLUSTER_UUID="$CLUSTER_UUID" \
        TIMESTAR_CLUSTER_DEVELOPMENT_ALLOW_INSECURE_TRANSPORT=true \
        TIMESTAR_CLUSTER_CONTROL_ENABLED=true TIMESTAR_CLUSTER_CONTROL_SEED_NODE_ID=1 \
        TIMESTAR_UNSAFE_TEST_INITIAL_REPLICAS=1,2,3 \
        TIMESTAR_CLUSTER_FAILURE_DOMAIN="rack-$node" \
        TIMESTAR_CLUSTER_NODE_ID="$node" TIMESTAR_CLUSTER_PEERS="$PEERS" \
        "$BIN" --port "$port" --smp 1 --memory "$GATE_SERVER_MEMORY" "${init_arg[@]}" \
        >>"$dir/s.log" 2>&1 &
    NODE_PIDS[$node]=$!
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
    for poll in $(seq 1 120); do
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
    for poll in $(seq 1 30); do
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

join_node() { # NODE
    local node="$1" port code body poll
    port=$(node_port "$node")
    mint_join_token || return 1
    for poll in $(seq 1 120); do
        code=$(curl -sS -m10 -o "$WORK/join_$node.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"token\":\"$JOIN_TOKEN\"}" 2>/dev/null)
        body=$(sed -n '1p' "$WORK/join_$node.json" 2>/dev/null)
        if [ "$code" = "200" ] && printf '%s' "$body" | grep -q '"status":"active"'; then
            return 0
        fi
        if [ "$code" != "200" ] && [ "$code" != "409" ]; then
            gate_fail "node $node join returned HTTP ${code:-unreachable}: ${body:-<empty>}"
            return 1
        fi
        sleep 1
    done
    gate_fail "node $node did not become Active through Group 0"
    return 1
}

wait_control_voters() { # COUNT PORTS
    local expected="$1" ports="$2" poll p status voters ok
    for poll in $(seq 1 180); do
        ok=1
        for p in $ports; do
            status=$(cluster_status "$p")
            voters=$(status_field "$status" control_voters)
            [ "${voters:-0}" -eq "$expected" ] || { ok=0; break; }
        done
        [ "$ok" = 1 ] && return 0
        sleep 1
    done
    gate_fail "Group-0 voter set did not converge to $expected"
    return 1
}

make_write_batch() { # OFFSET COUNT OUTPUT
    local offset="$1" count="$2" output="$3"
    awk -v first="$offset" -v count="$count" -v ts="$POINT_TS" 'BEGIN {
        printf "{\"writes\":["
        for (i = 0; i < count; ++i) {
            n = first + i
            if (i != 0) printf ","
            printf "{\"measurement\":\"pattern_gate\",\"tags\":{\"class\":\"frozen\",\"host\":\"h%05d\"},\"fields\":{\"value\":1.0},\"timestamp\":%s}", n, ts
        }
        print "]}"
    }' >"$output"
}

write_target_batch() { # OFFSET COUNT PORT
    local offset="$1" count="$2" port="$3" attempt code body
    make_write_batch "$offset" "$count" "$WORK/write.json"
    for attempt in $(seq 1 20); do
        code=$(curl -sS -m60 -o "$WORK/write_response.json" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/write" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            --data-binary "@$WORK/write.json" 2>/dev/null)
        body=$(sed -n '1p' "$WORK/write_response.json" 2>/dev/null)
        if [ "$code" = "200" ] && printf '%s' "$body" | grep -q "\"points_written\":$count"; then
            return 0
        fi
        if [ "$code" = "503" ]; then sleep 1; continue; fi
        gate_fail "target batch at $offset returned HTTP ${code:-unreachable}: ${body:-<empty>}"
        return 1
    done
    gate_fail "target batch at $offset did not commit after retries"
    return 1
}

write_late_match() { # PORT
    local port="$1" code body
    code=$(curl -sS -m30 -o "$WORK/late_write.json" -w '%{http_code}' -X POST \
        "http://127.0.0.1:$port/write" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"pattern_gate\",\"tags\":{\"class\":\"frozen\",\"host\":\"late\"},\"fields\":{\"value\":2.0},\"timestamp\":$POINT_TS}" 2>/dev/null)
    body=$(sed -n '1p' "$WORK/late_write.json" 2>/dev/null)
    [ "$code" = "200" ] && return 0
    gate_fail "new matching series returned HTTP ${code:-unreachable}: ${body:-<empty>}"
    return 1
}

query_count() { # PORT -- one result series per unique host tag
    local port="$1" response
    response=$(curl -sS -m60 -X POST "http://127.0.0.1:$port/query" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:pattern_gate(value){} by {host}\",\"startTime\":$((POINT_TS - 1)),\"endTime\":$((POINT_TS + 1))}" \
        2>/dev/null)
    printf '%s' "$response" | grep -o '"series_count":[0-9]*' | head -1 | cut -d: -f2
}

query_late_count() { # PORT
    local port="$1" response
    response=$(curl -sS -m60 -X POST "http://127.0.0.1:$port/query" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:pattern_gate(value){host:late}\",\"startTime\":$((POINT_TS - 1)),\"endTime\":$((POINT_TS + 1))}" \
        2>/dev/null)
    printf '%s' "$response" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2
}

wait_query_count() { # PORT EXPECTED POLLS
    local port="$1" expected="$2" polls="${3:-120}" poll count=""
    for poll in $(seq 1 "$polls"); do
        count=$(query_count "$port")
        [ "${count:-}" = "$expected" ] && return 0
        sleep 1
    done
    gate_fail "pattern_gate count on :$port was ${count:-missing}, expected $expected"
    return 1
}

wait_late_visible() { # PORT
    local port="$1" poll count=""
    for poll in $(seq 1 120); do
        count=$(query_late_count "$port")
        [ "${count:-}" = 1 ] && return 0
        sleep 1
    done
    gate_fail "post-freeze matching series was not query-visible on :$port"
    return 1
}

delete_request() { # PORT OUTPUT_CODE OUTPUT_BODY
    local port="$1" code_file="$2" body_file="$3"
    curl -sS -m300 -o "$body_file" -w '%{http_code}' -X POST \
        "http://127.0.0.1:$port/delete" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -H "Idempotency-Key: $DELETE_KEY" -H "Idempotency-Key-Timestamp: $DELETE_ISSUED_MS" \
        --data-binary "@$WORK/delete.json" >"$code_file" 2>"$WORK/delete_curl.err"
}

echo "=== bootstrap three bounded production servers and admit all Group-0 voters ==="
start_node 1 init
start_node 2
start_node 3
wait_all_led "$PORTS" 4096 180 || gate_exit
wait_healthy "$PORTS" 180 || gate_exit
wait_control_leader "$PORTS" >/dev/null || gate_exit
join_node 2 || gate_exit
join_node 3 || gate_exit
wait_control_voters 3 "$PORTS" || gate_exit
gate_ok "three Group-0 voters are available for pattern-plan failover"

echo "=== write and index $TARGETS targets across real VShard groups ==="
OFFSET=0
while [ "$OFFSET" -lt "$TARGETS" ]; do
    write_target_batch "$OFFSET" "$BATCH_SIZE" 19850 || gate_exit
    OFFSET=$((OFFSET + BATCH_SIZE))
done
wait_query_count 19850 "$TARGETS" 180 || gate_exit
gate_ok "$TARGETS pattern targets are query-visible before discovery"

DELETE_KEY="9f1d73046ce64e719adbc8a11f431b52"
DELETE_ISSUED_MS=$(($(date +%s) * 1000))
printf '%s' '{"measurement":"pattern_gate","tags":{"class":"frozen"},"startTime":0,"endTime":18446744073709551615}' \
    >"$WORK/delete.json"

echo "=== make the first outcome ambiguous only after a second voter applies the frozen plan ==="
OLD_LEADER=$(wait_control_leader "$PORTS") || gate_exit
OLD_PORT=$(node_port "$OLD_LEADER")
WITNESS_NODE=1
[ "$WITNESS_NODE" -eq "$OLD_LEADER" ] && WITNESS_NODE=2
WITNESS_PORT=$(node_port "$WITNESS_NODE")
delete_request "$OLD_PORT" "$WORK/first_code" "$WORK/first_body" &
DELETE_PID=$!

PLAN_TARGETS=0
PLAN_BYTES=0
for _ in $(seq 1 600); do
    STATUS=$(cluster_status "$WITNESS_PORT")
    PLAN_COUNT=$(status_field "$STATUS" control_frozen_delete_plans)
    PLAN_TARGETS=$(status_field "$STATUS" control_frozen_delete_plan_targets)
    PLAN_BYTES=$(status_field "$STATUS" control_frozen_delete_plan_bytes)
    [ "${PLAN_COUNT:-0}" -eq 1 ] && [ "${PLAN_TARGETS:-0}" -eq "$TARGETS" ] && break
    sleep 0.05
done
assert_eq "witness-applied frozen plan count" "${PLAN_COUNT:-missing}" 1
assert_eq "witness-applied frozen target count" "${PLAN_TARGETS:-missing}" "$TARGETS"
assert_ge "bounded frozen plan bytes are non-zero" "${PLAN_BYTES:-0}" 1
if ! kill -0 "$DELETE_PID" 2>/dev/null; then
    wait "$DELETE_PID" 2>/dev/null
    gate_fail "first pattern delete completed before its committed outcome could be made ambiguous"
    gate_exit
fi
gate_ok "node $WITNESS_NODE applied the complete plan while the client still awaited node $OLD_LEADER"

kill -9 "${NODE_PIDS[$OLD_LEADER]}" 2>/dev/null
wait "${NODE_PIDS[$OLD_LEADER]}" 2>/dev/null
NODE_PIDS[$OLD_LEADER]=""
wait "$DELETE_PID" 2>/dev/null
DELETE_RC=$?
FIRST_CODE=$(sed -n '1p' "$WORK/first_code" 2>/dev/null)
[ "$DELETE_RC" -ne 0 ] && gate_ok "the first client observed an ambiguous transport failure" \
    || gate_fail "the first client unexpectedly completed with HTTP ${FIRST_CODE:-missing}"

SURVIVOR_PORTS=""
for node in 1 2 3; do
    [ "$node" -ne "$OLD_LEADER" ] && SURVIVOR_PORTS="$SURVIVOR_PORTS $(node_port "$node")"
done
NEW_LEADER=$(wait_control_leader "$SURVIVOR_PORTS" "$OLD_LEADER") || gate_exit
wait_all_led "$SURVIVOR_PORTS" 4096 300 || gate_exit
wait_healthy "$SURVIVOR_PORTS" 300 || gate_exit
NEW_PORT=$(node_port "$NEW_LEADER")
gate_ok "Group-0 and every VShard recovered without coordinator node $OLD_LEADER"

echo "=== add a new match, then retry the exact identity against the new leader ==="
write_late_match "$NEW_PORT" || gate_exit
wait_late_visible "$NEW_PORT" || gate_exit
delete_request "$NEW_PORT" "$WORK/retry_code" "$WORK/retry_body"
RETRY_RC=$?
RETRY_CODE=$(sed -n '1p' "$WORK/retry_code" 2>/dev/null)
RETRY_BODY=$(sed -n '1p' "$WORK/retry_body" 2>/dev/null)
assert_eq "exact retry curl status" "$RETRY_RC" 0
assert_eq "exact retry HTTP status" "${RETRY_CODE:-missing}" 200
printf '%s' "$RETRY_BODY" | grep -q "\"seriesDeleted\":$TARGETS" \
    && gate_ok "retry reported the original $TARGETS exact targets" \
    || gate_fail "retry did not return the frozen target count: ${RETRY_BODY:-<empty>}"
wait_query_count "$NEW_PORT" 1 180 || gate_exit
gate_ok "the post-freeze matching series survived exact frozen-plan reuse"

echo "=== reject changed request bytes under the retained identity ==="
printf '%s' '{"measurement":"pattern_gate","tags":{"class":"different"},"startTime":0,"endTime":18446744073709551615}' \
    >"$WORK/conflict.json"
CONFLICT_CODE=$(curl -sS -m30 -o "$WORK/conflict_response.json" -w '%{http_code}' -X POST \
    "http://127.0.0.1:$NEW_PORT/delete" \
    -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $DELETE_KEY" -H "Idempotency-Key-Timestamp: $DELETE_ISSUED_MS" \
    --data-binary "@$WORK/conflict.json" 2>/dev/null)
CONFLICT_BODY=$(sed -n '1p' "$WORK/conflict_response.json" 2>/dev/null)
assert_eq "changed-body retry HTTP status" "${CONFLICT_CODE:-missing}" 409
printf '%s' "$CONFLICT_BODY" | grep -q 'DELETE_IDEMPOTENCY_CONFLICT' \
    && gate_ok "changed request bytes were rejected by the retained plan identity" \
    || gate_fail "changed-body conflict omitted its stable error code: ${CONFLICT_BODY:-<empty>}"

echo "=== restart the killed coordinator and recover the same plan and data state ==="
start_node "$OLD_LEADER"
wait_healthy "$PORTS" 600 || gate_exit
for _ in $(seq 1 180); do
    STATUS=$(cluster_status "$OLD_PORT")
    PLAN_COUNT=$(status_field "$STATUS" control_frozen_delete_plans)
    PLAN_TARGETS=$(status_field "$STATUS" control_frozen_delete_plan_targets)
    [ "${PLAN_COUNT:-0}" -eq 1 ] && [ "${PLAN_TARGETS:-0}" -eq "$TARGETS" ] && break
    sleep 1
done
assert_eq "restarted node frozen plan count" "${PLAN_COUNT:-missing}" 1
assert_eq "restarted node frozen target count" "${PLAN_TARGETS:-missing}" "$TARGETS"
wait_query_count "$OLD_PORT" 1 180 || gate_exit
gate_ok "restart recovered the plan and the post-freeze series without resurrection"

gate_exit
