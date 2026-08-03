#!/bin/bash
# P0 PRODUCTION-SERVER GATE: prove that the exact-v1 clustered retention API,
# Group-0 sweep cursor, and per-VShard cutoff commands survive controller loss.
#
# The gate is deliberately low-volume. Three one-reactor, 1 GiB servers are the
# only substantial memory users, and all durable roots live under build/tmp.
# One sweep still reaches all 4,096 real VShard groups; the test kills its
# Group-0 leader after the durable cursor advances, waits for another admitted
# voter to complete the sweep, then restarts the old leader and proves it caught
# up to the completed global sequence.
#
# Usage: retention_failover_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
GATE_SHUTDOWN_TIMEOUT_SECONDS="${GATE_SHUTDOWN_TIMEOUT_SECONDS:-120}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }

PORTS="19830 19831 19832"
PREFIX=1983
AUTH_TOKEN="retention-gate-operator-token"
CLUSTER_UUID="10112233445566778899aabbccddeeff"
PEERS="127.0.0.1:19830,127.0.0.1:19831,127.0.0.1:19832"
GATE_ROOT="$GATE_TMP_ROOT"
DIR1="$GATE_ROOT/tsgate_rt1"
DIR2="$GATE_ROOT/tsgate_rt2"
DIR3="$GATE_ROOT/tsgate_rt3"
DIRS="$DIR1 $DIR2 $DIR3"
declare -a NODE_PIDS

mkdir -p "$GATE_ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs $DIRS || exit 2
trap 'gate_cleanup 1983 "$DIR1" "$DIR2" "$DIR3"' EXIT

node_port() { echo $((19829 + $1)); }
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

stop_node() { # NODE
    local node="$1" pid="${NODE_PIDS[$1]:-}" polls
    [ -n "$pid" ] || return 0
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null
        for polls in $(seq 1 $((GATE_SHUTDOWN_TIMEOUT_SECONDS + 30))); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 1
        done
    fi
    if kill -0 "$pid" 2>/dev/null; then
        gate_fail "node $node did not stop within $((GATE_SHUTDOWN_TIMEOUT_SECONDS + 30))s"
        kill -9 "$pid" 2>/dev/null
    fi
    wait "$pid" 2>/dev/null
    NODE_PIDS[$node]=""
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
    local leader port code body response poll
    response="$GATE_ROOT/tsgate_rt_token.json"
    JOIN_TOKEN=""
    for poll in $(seq 1 30); do
        leader=$(control_leader 2>/dev/null || true)
        [ -n "$leader" ] || { sleep 1; continue; }
        port=$(node_port "$leader")
        code=$(curl -sS -m10 -o "$response" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join-token" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' -d '{}' 2>/dev/null)
        body=$(sed -n '1p' "$response" 2>/dev/null)
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
    local node="$1" port code body response poll
    port=$(node_port "$node")
    response="$GATE_ROOT/tsgate_rt_join_$node.json"
    mint_join_token || return 1
    for poll in $(seq 1 120); do
        code=$(curl -sS -m10 -o "$response" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"token\":\"$JOIN_TOKEN\"}" 2>/dev/null)
        body=$(sed -n '1p' "$response" 2>/dev/null)
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

put_policy() { # EXPECTED_VERSION TTL EXPECTED_HTTP
    local expected="$1" ttl="$2" wanted="$3" poll leader port code body response
    response="$GATE_ROOT/tsgate_rt_policy.json"
    for poll in $(seq 1 30); do
        leader=$(control_leader 2>/dev/null || true)
        [ -n "$leader" ] || { sleep 1; continue; }
        port=$(node_port "$leader")
        code=$(curl -sS -m15 -o "$response" -w '%{http_code}' -X PUT \
            "http://127.0.0.1:$port/retention" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"measurement\":\"retention_gate\",\"expectedVersion\":$expected,\"ttl\":\"$ttl\"}" \
            2>/dev/null)
        body=$(sed -n '1p' "$response" 2>/dev/null)
        if [ "$code" = "$wanted" ]; then
            RETENTION_BODY="$body"
            return 0
        fi
        if [ "$code" = "409" ] && printf '%s' "$body" | grep -q 'control leader changed'; then
            sleep 1
            continue
        fi
        gate_fail "PUT /retention returned HTTP ${code:-unreachable}, expected $wanted: ${body:-<empty>}"
        return 1
    done
    gate_fail "PUT /retention could not reach the current Group-0 leader"
    return 1
}

delete_policy() { # EXPECTED_VERSION
    local expected="$1" poll leader port code body response
    response="$GATE_ROOT/tsgate_rt_delete.json"
    for poll in $(seq 1 30); do
        leader=$(control_leader 2>/dev/null || true)
        [ -n "$leader" ] || { sleep 1; continue; }
        port=$(node_port "$leader")
        code=$(curl -sS -m15 -o "$response" -w '%{http_code}' -X DELETE \
            "http://127.0.0.1:$port/retention?measurement=retention_gate&expected_version=$expected" \
            -H "Authorization: Bearer $AUTH_TOKEN" 2>/dev/null)
        body=$(sed -n '1p' "$response" 2>/dev/null)
        if [ "$code" = "200" ]; then
            RETENTION_BODY="$body"
            return 0
        fi
        if [ "$code" = "409" ]; then sleep 1; continue; fi
        gate_fail "DELETE /retention returned HTTP ${code:-unreachable}: ${body:-<empty>}"
        return 1
    done
    gate_fail "DELETE /retention could not reach the current Group-0 leader"
    return 1
}

write_point() { # MEASUREMENT TIMESTAMP VALUE PORT
    local measurement="$1" timestamp="$2" value="$3" port="$4" code
    code=$(curl -sS -m15 -o "$GATE_ROOT/tsgate_rt_write.json" -w '%{http_code}' -X POST \
        "http://127.0.0.1:$port/write" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"measurement\":\"$measurement\",\"tags\":{\"host\":\"a\"},\"fields\":{\"value\":$value},\"timestamp\":$timestamp}" \
        2>/dev/null)
    [ "$code" -ge 200 ] && [ "$code" -lt 300 ] && return 0
    gate_fail "write for $measurement at $timestamp returned HTTP ${code:-unreachable}"
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
    for poll in $(seq 1 60); do
        count=$(query_count "$port" "$measurement" "$start" "$end")
        [ "${count:-}" = "$expected" ] && return 0
        sleep 1
    done
    gate_fail "$measurement count on :$port was ${count:-missing}, expected $expected"
    return 1
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
gate_ok "three Group-0 voters are available for controller failover"

NOW=$(date +%s%N)
OLD=$((NOW - 10 * 1000000000))
NEW=$((NOW + 10 * 1000000000))
START=$((OLD - 1))
END=$((NEW + 1))
write_point retention_gate "$OLD" 1.0 19830 || gate_exit
write_point retention_gate "$NEW" 2.0 19830 || gate_exit
write_point retention_control "$OLD" 3.0 19830 || gate_exit
wait_query_count 19830 retention_gate "$START" "$END" 2 || gate_exit
wait_query_count 19830 retention_control "$START" "$END" 1 || gate_exit

echo "=== create one exact-version policy and reject a lost update ==="
put_policy 0 1s 200 || gate_exit
printf '%s' "$RETENTION_BODY" | grep -q '"version":1' \
    && gate_ok "policy creation returned CAS version 1" \
    || gate_fail "policy creation did not return version 1: $RETENTION_BODY"
put_policy 0 1s 200 || gate_exit
printf '%s' "$RETENTION_BODY" | grep -q '"version":1' \
    && gate_ok "exact PUT retry remained at version 1" \
    || gate_fail "exact PUT retry changed version: $RETENTION_BODY"
put_policy 0 2s 409 || gate_exit
printf '%s' "$RETENTION_BODY" | grep -q '"currentVersion":1' \
    && gate_ok "conflicting PUT exposed current version 1" \
    || gate_fail "CAS conflict omitted current version: $RETENTION_BODY"

echo "=== kill the controller after durable fan-out progress and resume elsewhere ==="
OLD_LEADER=""
for _ in $(seq 1 180); do
    OLD_LEADER=$(control_leader 2>/dev/null || true)
    [ -n "$OLD_LEADER" ] || { sleep 1; continue; }
    STATUS=$(cluster_status "$(node_port "$OLD_LEADER")")
    ACTIVE=$(status_bool "$STATUS" control_retention_sweep_active)
    CURSOR=$(status_field "$STATUS" control_retention_next_vshard)
    [ "$ACTIVE" = "true" ] && [ "${CURSOR:-0}" -ge 128 ] && [ "$CURSOR" -lt 4096 ] && break
    sleep 1
done
if [ -z "$OLD_LEADER" ] || [ "${ACTIVE:-false}" != "true" ] || [ "${CURSOR:-0}" -lt 128 ] || [ "$CURSOR" -ge 4096 ]; then
    gate_fail "did not observe an in-progress durable retention cursor"
    gate_exit
fi
gate_ok "controller node $OLD_LEADER reached durable VShard cursor $CURSOR"
kill -9 "${NODE_PIDS[$OLD_LEADER]}" 2>/dev/null
wait "${NODE_PIDS[$OLD_LEADER]}" 2>/dev/null
NODE_PIDS[$OLD_LEADER]=""

SURVIVOR_PORTS=""
for node in 1 2 3; do
    [ "$node" -ne "$OLD_LEADER" ] && SURVIVOR_PORTS="$SURVIVOR_PORTS $(node_port "$node")"
done
NEW_LEADER=$(wait_control_leader "$SURVIVOR_PORTS" "$OLD_LEADER") || gate_exit
gate_ok "Group-0 leadership moved from node $OLD_LEADER to node $NEW_LEADER"

COMPLETE_PORT=$(node_port "$NEW_LEADER")
for _ in $(seq 1 240); do
    STATUS=$(cluster_status "$COMPLETE_PORT")
    LAST_SWEEP=$(status_field "$STATUS" control_retention_last_sweep_id)
    CUTOFF_RECORDS=$(status_field "$STATUS" control_retention_cutoff_records)
    ACTIVE=$(status_bool "$STATUS" control_retention_sweep_active)
    [ "${LAST_SWEEP:-0}" -ge 1 ] && [ "${CUTOFF_RECORDS:-0}" -eq 1 ] && [ "$ACTIVE" = "false" ] && break
    sleep 1
done
assert_eq "completed global retention sweep id" "${LAST_SWEEP:-missing}" 1
assert_eq "completed retention cutoff records" "${CUTOFF_RECORDS:-missing}" 1
assert_eq "retention sweep is idle after completion" "${ACTIVE:-missing}" false
wait_query_count "$COMPLETE_PORT" retention_gate "$START" "$END" 1 || gate_exit
wait_query_count "$COMPLETE_PORT" retention_control "$START" "$END" 1 || gate_exit
gate_ok "expired target point was removed and the control measurement was preserved"

echo "=== restart the old controller and prove snapshot/log state catches up ==="
start_node "$OLD_LEADER"
wait_healthy "$PORTS" 600 || gate_exit
for _ in $(seq 1 120); do
    STATUS=$(cluster_status "$(node_port "$OLD_LEADER")")
    LAST_SWEEP=$(status_field "$STATUS" control_retention_last_sweep_id)
    CUTOFF_RECORDS=$(status_field "$STATUS" control_retention_cutoff_records)
    [ "${LAST_SWEEP:-0}" -eq 1 ] && [ "${CUTOFF_RECORDS:-0}" -eq 1 ] && break
    sleep 1
done
assert_eq "restarted node recovered global retention sweep id" "${LAST_SWEEP:-missing}" 1
assert_eq "restarted node recovered cutoff record" "${CUTOFF_RECORDS:-missing}" 1
wait_query_count "$(node_port "$OLD_LEADER")" retention_gate "$START" "$END" 1 || gate_exit

echo "=== delete, expose the tombstone version, and recreate without ambiguity ==="
delete_policy 1 || gate_exit
printf '%s' "$RETENTION_BODY" | grep -q '"version":2' \
    && gate_ok "DELETE returned tombstone version 2" \
    || gate_fail "DELETE omitted tombstone version 2: $RETENTION_BODY"
LEADER=$(control_leader 2>/dev/null || true)
GET_BODY=$(curl -sS -m10 "http://127.0.0.1:$(node_port "$LEADER")/retention?measurement=retention_gate" \
    -H "Authorization: Bearer $AUTH_TOKEN" 2>/dev/null)
printf '%s' "$GET_BODY" | grep -q '"currentVersion":2' \
    && gate_ok "404 exposed tombstone version 2" \
    || gate_fail "404 hid the tombstone version: $GET_BODY"
put_policy 2 2s 200 || gate_exit
printf '%s' "$RETENTION_BODY" | grep -q '"version":3' \
    && gate_ok "policy recreated from tombstone version 2 as version 3" \
    || gate_fail "policy recreation did not return version 3: $RETENTION_BODY"

gate_exit
