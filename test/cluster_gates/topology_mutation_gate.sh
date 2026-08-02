#!/bin/bash
# P0 PRODUCTION-SERVER GATE: drive the real authenticated Group-0 topology
# routes through two crash-recovery windows and prove one VShard's contribution
# is neither lost nor duplicated.
#
# The unsafe, startup-validated test placement assigns all 4096 VShards to
# {1,2,3}, leaving node 4 as a real spare. The probe key is a checked golden
# vector for VShard 0 (placement_table_test.cpp), so a move to node 4 cannot be
# vacuous. Four moves exercise replacement in both directions while writes
# continue; node 4 has exactly one serving reference when it is drained:
#
#   epoch 1 {1,2,3}
#       -> 2 {1,2,4}, node 3 exits after its Engine WAL generation is deleted
#       -> 3 {1,2,3}, node 4 retires normally
#       -> 4 {1,2,4}, node 3 exits after journal quarantine but before its marker
#       -> 5 {1,2,3}, after node 3 recovers, its quarantine is aged/reclaimed,
#                        and the deleted generation is materialized again
#
# The two TIMESTAR_UNSAFE_TEST_* variables are accepted only with the existing
# local insecure-transport escape hatch and Group-0 enabled. They make the
# unmodified server exit(86) at an exact durable checkpoint; normal deployments
# never arm the hooks. The script asserts the exit code and on-disk shape, so a
# renamed/missing checkpoint fails instead of silently becoming a happy-path run.
#
# This is intentionally light on data but heavy on process state. It defaults to
# one reactor and 1 GiB per process (4 GiB aggregate) and stores durable roots in
# build/tmp rather than quota-limited /tmp. Do not run it concurrently with any
# other live gate. Override GATE_SERVER_MEMORY only when the host can afford the
# four-process aggregate.
#
# Usage: topology_mutation_gate.sh [SERVER_BINARY]

GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-1G}"
GATE_SHUTDOWN_TIMEOUT_SECONDS="${GATE_SHUTDOWN_TIMEOUT_SECONDS:-120}"
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

BIN="${1:-$BUILD_DIR/bin/timestar_http_server}"
[ -x "$BIN" ] || { echo "no server binary at $BIN"; exit 2; }

PORTS="19810 19811 19812 19813"
PREFIX=1981
AUTH_TOKEN="topology-gate-operator-token"
CLUSTER_UUID="00112233445566778899aabbccddeeff"
PEERS="127.0.0.1:19810,127.0.0.1:19811,127.0.0.1:19812,127.0.0.1:19813"
GATE_ROOT="$BUILD_DIR/tmp"
DIR1="$GATE_ROOT/tsgate_tm1"
DIR2="$GATE_ROOT/tsgate_tm2"
DIR3="$GATE_ROOT/tsgate_tm3"
DIR4="$GATE_ROOT/tsgate_tm4"
DIRS="$DIR1 $DIR2 $DIR3 $DIR4"
BASE_TS=1700000000000000000
NEXT_POINT=0
TOTAL_ATTEMPTED=0
TOTAL_ACKED=0
declare -a NODE_PIDS

mkdir -p "$GATE_ROOT"
kill_cluster "$PREFIX"
require_ports_free $PORTS
fresh_gate_data_dirs $DIRS || exit 2
trap 'gate_cleanup 1981 "$DIR1" "$DIR2" "$DIR3" "$DIR4"' EXIT

node_port() { echo $((19809 + $1)); }
node_dir() {
    case "$1" in
        1) echo "$DIR1" ;;
        2) echo "$DIR2" ;;
        3) echo "$DIR3" ;;
        4) echo "$DIR4" ;;
        *) return 2 ;;
    esac
}

start_node() { # NODE [FAILPOINT] [init]
    local node="$1" failpoint="${2:-}" init="${3:-}" dir port
    local -a unsafe_env init_arg
    dir=$(node_dir "$node")
    port=$(node_port "$node")
    unsafe_env=()
    init_arg=()
    if [ -n "$failpoint" ]; then
        unsafe_env=("TIMESTAR_UNSAFE_TEST_RETIREMENT_CRASH=$failpoint"
                    "TIMESTAR_UNSAFE_TEST_RETIREMENT_VSHARD=0")
    fi
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
        "${unsafe_env[@]}" \
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

wait_expected_crash() { # NODE LABEL
    local node="$1" label="$2" pid="${NODE_PIDS[$1]}" polls rc
    for polls in $(seq 1 90); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        gate_fail "$label failpoint did not terminate node $node"
        kill -9 "$pid" 2>/dev/null
    fi
    wait "$pid" 2>/dev/null
    rc=$?
    NODE_PIDS[$node]=""
    assert_eq "$label failpoint exit status" "$rc" 86
}

control_leader() {
    local p status leader
    for p in $PORTS; do
        status=$(cluster_status "$p")
        leader=$(status_field "$status" control_leader)
        if [ -n "$leader" ] && [ "$leader" -ge 1 ] && [ "$leader" -le 4 ]; then
            echo "$leader"
            return 0
        fi
    done
    return 1
}

wait_for_control_leader() {
    local polls leader
    for polls in $(seq 1 90); do
        leader=$(control_leader 2>/dev/null || true)
        if [ -n "$leader" ]; then
            echo "  Group-0 leader is node $leader"
            return 0
        fi
        sleep 1
    done
    gate_fail "Group-0 did not elect a leader"
    return 1
}

CONTROL_CODE=""
CONTROL_BODY=""
post_control() { # PATH JSON EXPECTED_CODE
    local path="$1" body="$2" expected="$3" attempt leader port response
    response="$GATE_ROOT/tsgate_tm_control_response.json"
    for attempt in $(seq 1 30); do
        leader=$(control_leader 2>/dev/null || true)
        if [ -z "$leader" ]; then
            sleep 1
            continue
        fi
        port=$(node_port "$leader")
        CONTROL_CODE=$(curl -sS -m10 -o "$response" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port$path" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' -d "$body" 2>/dev/null)
        CONTROL_BODY=$(cat "$response" 2>/dev/null)
        # A 409 can be either the expected policy rejection or a transient
        # redirect from a node that lost Group-0 leadership after status was
        # sampled. Never accept the latter as evidence for the former.
        if [ "$CONTROL_CODE" = "409" ] && printf '%s' "$CONTROL_BODY" | grep -q 'control leader changed'; then
            sleep 1
            continue
        fi
        if [ "$CONTROL_CODE" = "$expected" ]; then
            return 0
        fi
        gate_fail "$path returned HTTP ${CONTROL_CODE:-unreachable}, expected $expected: ${CONTROL_BODY:-<empty>}"
        return 1
    done
    gate_fail "$path could not reach the current Group-0 leader"
    return 1
}

mint_join_token() {
    local attempt leader port response code body
    response="$GATE_ROOT/tsgate_tm_join_token.json"
    JOIN_TOKEN=""
    for attempt in $(seq 1 30); do
        leader=$(control_leader 2>/dev/null || true)
        [ -n "$leader" ] || { sleep 1; continue; }
        port=$(node_port "$leader")
        code=$(curl -sS -m10 -o "$response" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join-token" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' -d '{}' 2>/dev/null)
        body=$(cat "$response" 2>/dev/null)
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
    local node="$1" port attempt code body response
    port=$(node_port "$node")
    response="$GATE_ROOT/tsgate_tm_join_$node.json"
    mint_join_token || return 1
    for attempt in $(seq 1 120); do
        code=$(curl -sS -m10 -o "$response" -w '%{http_code}' -X POST \
            "http://127.0.0.1:$port/cluster/join" \
            -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"token\":\"$JOIN_TOKEN\"}" 2>/dev/null)
        body=$(cat "$response" 2>/dev/null)
        if [ "$code" = "200" ] && printf '%s' "$body" | grep -q '"status":"active"'; then
            echo "  node $node admitted through Group 0"
            return 0
        fi
        if [ "$code" = "200" ] && printf '%s' "$body" | grep -q '"status":"joining"'; then
            sleep 1
            continue
        fi
        if [ "$code" = "409" ]; then
            sleep 1
            continue
        fi
        gate_fail "node $node join returned HTTP ${code:-unreachable}: ${body:-<empty>}"
        return 1
    done
    gate_fail "node $node did not become Active through Group 0"
    return 1
}

wait_control_nodes() { # COUNT PORTS POLLS
    local expected="$1" ports="$2" max="${3:-120}" poll p status count ok
    for poll in $(seq 1 "$max"); do
        ok=1
        for p in $ports; do
            status=$(cluster_status "$p")
            count=$(status_field "$status" control_nodes)
            [ "${count:-0}" -eq "$expected" ] || { ok=0; break; }
        done
        if [ "$ok" = 1 ]; then
            echo "  all reachable nodes observe $expected Group-0 node records"
            return 0
        fi
        sleep 1
    done
    gate_fail "Group-0 node records did not converge to $expected"
    return 1
}

wait_serving_epoch() { # EPOCH PORTS POLLS
    local expected="$1" ports="$2" max="${3:-120}" poll p status epoch ok
    for poll in $(seq 1 "$max"); do
        ok=1
        for p in $ports; do
            status=$(cluster_status "$p")
            epoch=$(status_field "$status" control_serving_map_epoch)
            [ "${epoch:-0}" -eq "$expected" ] || { ok=0; break; }
        done
        if [ "$ok" = 1 ]; then
            echo "  serving-map epoch $expected published on: $ports"
            return 0
        fi
        sleep 1
    done
    gate_fail "serving-map epoch $expected did not reach: $ports"
    return 1
}

CAMPAIGN_PID=""
CAMPAIGN_FILE=""
start_campaign() { # LABEL POINTS
    local label="$1" points="$2" first="$NEXT_POINT"
    CAMPAIGN_FILE="$GATE_ROOT/tsgate_tm_campaign_$label.txt"
    NEXT_POINT=$((NEXT_POINT + points))
    (
        ok=0
        retryable=0
        other=0
        i=0
        while [ "$i" -lt "$points" ]; do
            code=$(curl -sS -m10 -o "$GATE_ROOT/tsgate_tm_write_$label.json" -w '%{http_code}' -X POST \
                http://127.0.0.1:19810/write \
                -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
                -d "{\"measurement\":\"topology_probe\",\"tags\":{\"host\":\"h1438\"},\"fields\":{\"value\":1.0},\"timestamp\":$((BASE_TS + first + i))}" \
                2>/dev/null)
            case "$code" in
                2*) ok=$((ok + 1)) ;;
                5*) retryable=$((retryable + 1)) ;;
                *) other=$((other + 1)) ;;
            esac
            i=$((i + 1))
            sleep 0.02
        done
        echo "$points $ok $retryable $other" >"$CAMPAIGN_FILE"
    ) &
    CAMPAIGN_PID=$!
}

finish_campaign() { # LABEL
    local label="$1" attempted acked retryable other
    wait "$CAMPAIGN_PID"
    read -r attempted acked retryable other <"$CAMPAIGN_FILE"
    echo "  $label writes: attempted=$attempted acknowledged=$acked retryable=$retryable other=$other"
    TOTAL_ATTEMPTED=$((TOTAL_ATTEMPTED + attempted))
    TOTAL_ACKED=$((TOTAL_ACKED + acked))
    assert_ge "$label acknowledged writes (anti-vacuity)" "$acked" 1
    assert_eq "$label non-retryable write failures" "$other" 0
}

probe_count() { # PORT
    local response
    response=$(curl -sS -m30 -X POST "http://127.0.0.1:$1/query" \
        -H "Authorization: Bearer $AUTH_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"query\":\"count:topology_probe(value){}\",\"startTime\":$BASE_TS,\"endTime\":$((BASE_TS + NEXT_POINT + 1))}" \
        2>/dev/null)
    printf '%s' "$response" | grep -o '"point_count":[0-9]*' | head -1 | cut -d: -f2
}

verify_probe() { # LABEL PORTS
    local label="$1" ports="$2" p poll count ok
    for p in $ports; do
        count=""
        for poll in $(seq 1 60); do
            count=$(probe_count "$p")
            if [ -n "$count" ] && [ "$count" -ge "$TOTAL_ACKED" ] && [ "$count" -le "$TOTAL_ATTEMPTED" ]; then
                break
            fi
            sleep 1
        done
        echo "  $label read-back on :$p -> ${count:-<none>} points; acked=$TOTAL_ACKED attempted=$TOTAL_ATTEMPTED"
        assert_ge "$label has every acknowledged point on :$p" "${count:-0}" "$TOTAL_ACKED"
        assert_le "$label has no fabricated/duplicate point on :$p" "${count:-999999999}" "$TOTAL_ATTEMPTED"
    done
}

wait_retired_counter() { # PORT MINIMUM
    local port="$1" minimum="$2" poll value
    for poll in $(seq 1 90); do
        value=$(status_field "$(cluster_status "$port")" replicas_retired)
        [ "${value:-0}" -ge "$minimum" ] && return 0
        sleep 1
    done
    gate_fail "node on :$port did not publish replicas_retired >= $minimum"
    return 1
}

wait_for_marker() { # DIRECTORY
    local dir="$1" poll count
    for poll in $(seq 1 90); do
        count=$(find "$dir" -maxdepth 1 -type f -name 'retired_at_*.v1' 2>/dev/null | wc -l)
        [ "$count" -eq 1 ] && return 0
        sleep 1
    done
    gate_fail "exactly one retirement marker did not appear in $dir"
    return 1
}

echo "=== bootstrap four production servers and admit nodes through Group 0 ==="
start_node 1 "" init
start_node 2
start_node 3 engine-wal-generation-deleted
start_node 4
wait_all_led "$PORTS" 4096 180 || gate_exit
# The intentionally empty spare is correctly not ready to serve until the first
# move materializes VShard 0 there. Require readiness from the initial serving
# set now. Later checks follow the current serving set because an intentionally
# empty spare is not ready to serve until it receives a VShard.
wait_healthy "19810 19811 19812" 180 || gate_exit
wait_for_control_leader || gate_exit
join_node 2 || gate_exit
join_node 3 || gate_exit
join_node 4 || gate_exit
wait_control_nodes 4 "$PORTS" 180 || gate_exit
wait_serving_epoch 1 "$PORTS" 60 || gate_exit

start_campaign baseline 20
finish_campaign baseline
assert_eq "baseline writes are all acknowledged" "$TOTAL_ACKED" "$TOTAL_ATTEMPTED"
verify_probe baseline "$PORTS"

echo "=== move VShard 0 to node 4; crash node 3 inside Engine cleanup ==="
start_campaign engine_crash 300
post_control /cluster/vshards/move '{"job_id":"topology-engine-crash","map_epoch":1,"vshard":0,"destination":4,"victim":3}' 202 || gate_exit
wait_serving_epoch 2 "19810 19811 19813" 180 || gate_exit
wait_expected_crash 3 "Engine cleanup"
finish_campaign engine_crash
if [ -d "$DIR3/cluster_raft/vshard_0" ] && [ ! -e "$DIR3/cluster_raft/retired/v1_vshard_0_epoch_2" ]; then
    gate_ok "Engine-cleanup crash retained the active journal retry token"
else
    gate_fail "Engine-cleanup crash did not leave only the active VShard-0 journal"
fi

start_node 3
wait_healthy "$PORTS" 600 || gate_exit
wait_serving_epoch 2 "$PORTS" 120 || gate_exit
ENGINE_RETIRED="$DIR3/cluster_raft/retired/v1_vshard_0_epoch_2"
wait_for_marker "$ENGINE_RETIRED" || gate_exit
verify_probe "after Engine-cleanup recovery" "$PORTS"

echo "=== move VShard 0 back; node 3 is materialized from the surviving replicas ==="
start_campaign first_return 300
post_control /cluster/vshards/move '{"job_id":"topology-first-return","map_epoch":2,"vshard":0,"destination":3,"victim":4}' 202 || gate_exit
wait_serving_epoch 3 "$PORTS" 180 || gate_exit
finish_campaign first_return
wait_retired_counter 19813 1 || gate_exit
verify_probe "after first movement back" "$PORTS"

echo "=== re-arm node 3 and crash after durable journal quarantine, before its marker ==="
stop_node 3
start_node 3 journal-quarantined
wait_healthy "19810 19811 19812" 600 || gate_exit
wait_serving_epoch 3 "$PORTS" 120 || gate_exit
start_campaign journal_crash 300
post_control /cluster/vshards/move '{"job_id":"topology-journal-crash","map_epoch":3,"vshard":0,"destination":4,"victim":3}' 202 || gate_exit
wait_serving_epoch 4 "19810 19811 19813" 180 || gate_exit
wait_expected_crash 3 "journal quarantine"
finish_campaign journal_crash
JOURNAL_RETIRED="$DIR3/cluster_raft/retired/v1_vshard_0_epoch_4"
if [ ! -e "$DIR3/cluster_raft/vshard_0" ] && [ -d "$JOURNAL_RETIRED" ] &&
   [ "$(find "$JOURNAL_RETIRED" -maxdepth 1 -type f -name 'retired_at_*.v1' | wc -l)" -eq 0 ]; then
    gate_ok "journal-quarantine crash left a durable retired generation without a grace marker"
else
    gate_fail "journal-quarantine crash did not stop at the expected on-disk boundary"
fi

start_node 3
wait_healthy "19810 19811 19813" 600 || gate_exit
wait_serving_epoch 4 "$PORTS" 120 || gate_exit
wait_for_marker "$JOURNAL_RETIRED" || gate_exit
verify_probe "after journal-quarantine recovery" "$PORTS"

echo "=== age and reclaim the quarantine, then automatically evacuate draining node 4 ==="
stop_node 3
MARKER=$(find "$JOURNAL_RETIRED" -maxdepth 1 -type f -name 'retired_at_*.v1')
if [ "$(printf '%s\n' "$MARKER" | grep -c .)" -ne 1 ]; then
    gate_fail "cannot age an ambiguous retirement marker set"
    gate_exit
fi
mv -- "$MARKER" "$JOURNAL_RETIRED/retired_at_0.v1"
start_node 3
wait_healthy "19810 19811 19813" 600 || gate_exit
for _ in $(seq 1 120); do
    RECLAIMED=$(status_field "$(cluster_status 19812)" retired_journals_reclaimed)
    [ "${RECLAIMED:-0}" -ge 1 ] && [ ! -e "$JOURNAL_RETIRED" ] && break
    sleep 1
done
assert_ge "node 3 reclaimed an expired retired journal" "${RECLAIMED:-0}" 1
if [ ! -e "$JOURNAL_RETIRED" ]; then
    gate_ok "expired VShard-0 journal generation is absent"
else
    gate_fail "expired VShard-0 journal generation still exists"
fi

start_campaign automatic_evacuation 300
UNAUTH_CODE=$(curl -sS -m10 -o "$GATE_ROOT/tsgate_tm_unauth.json" -w '%{http_code}' -X POST \
    http://127.0.0.1:19810/cluster/nodes/drain -H 'Content-Type: application/json' -d '{"node":4}' 2>/dev/null)
assert_eq "unauthenticated drain is rejected" "$UNAUTH_CODE" 401
post_control /cluster/nodes/drain '{"node":4}' 202 || gate_exit
post_control /cluster/nodes/drain '{"node":4}' 202 || gate_exit
CONTROL_STATUS=$(cluster_status 19810)
assert_eq "drain victim starts with exactly one serving-map reference" \
    "$(status_field "$CONTROL_STATUS" control_drain_references)" 1
post_control /cluster/nodes/remove '{"node":4}' 409 || gate_exit
if printf '%s' "$CONTROL_BODY" | grep -q '"status":"rejected"'; then
    gate_ok "removal remains blocked while node 4 has a serving-map reference"
else
    gate_fail "premature node 4 removal did not report a policy rejection: $CONTROL_BODY"
fi
wait_serving_epoch 5 "$PORTS" 180 || gate_exit
finish_campaign automatic_evacuation
wait_retired_counter 19813 2 || gate_exit
verify_probe "after automatic drain evacuation" "$PORTS"

echo "=== evict the caught-up Group-0 learner and accept final removal ==="
for _ in $(seq 1 120); do
    CONTROL_STATUS=$(cluster_status 19810)
    DRAIN_REFS=$(status_field "$CONTROL_STATUS" control_drain_references)
    TOPOLOGY_PLANS=$(status_field "$CONTROL_STATUS" control_topology_plans)
    TOPOLOGY_CUTOVERS=$(status_field "$CONTROL_STATUS" control_topology_cutovers)
    [ "${DRAIN_REFS:-1}" -eq 0 ] && [ "${TOPOLOGY_PLANS:-0}" -ge 1 ] && \
        [ "${TOPOLOGY_CUTOVERS:-0}" -ge 1 ] && break
    sleep 1
done
assert_eq "automatic drain has no serving-map references" "${DRAIN_REFS:-missing}" 0
assert_ge "automatic drain planned a replacement" "${TOPOLOGY_PLANS:-0}" 1
assert_ge "automatic drain published a cutover" "${TOPOLOGY_CUTOVERS:-0}" 1
post_control /cluster/nodes/remove '{"node":4}' 202 || gate_exit
for _ in $(seq 1 120); do
    CONTROL_STATUS=$(cluster_status 19810)
    CONTROL_LEARNERS=$(status_field "$CONTROL_STATUS" control_learners)
    REMOVALS_PENDING=$(status_field "$CONTROL_STATUS" control_removals_pending)
    [ "${CONTROL_LEARNERS:-1}" -eq 0 ] && [ "${REMOVALS_PENDING:-1}" -eq 0 ] && break
    sleep 1
done
assert_eq "removed node applied its record before Group-0 eviction" "${CONTROL_LEARNERS:-missing}" 0
assert_eq "final Group-0 membership cleanup completed" "${REMOVALS_PENDING:-missing}" 0
post_control /cluster/nodes/remove '{"node":4}' 202 || gate_exit
if printf '%s' "$CONTROL_BODY" | grep -q '"status":"removed"'; then
    gate_ok "Draining -> Removed is accepted after complete evacuation"
else
    gate_fail "idempotent final removal did not report removed: $CONTROL_BODY"
fi

for p in $PORTS; do
    VERSION=$(status_field "$(cluster_status "$p")" protocol_version)
    assert_eq "node :$p reports exact protocol version" "${VERSION:-missing}" 1
done
assert_eq "unexpected segmentation faults" "$(grep -l 'Segmentation fault' "$DIR1"/s.log "$DIR2"/s.log "$DIR3"/s.log "$DIR4"/s.log 2>/dev/null | wc -l)" 0

echo "GATE_METRIC topology_probe_attempted $TOTAL_ATTEMPTED"
echo "GATE_METRIC topology_probe_acknowledged $TOTAL_ACKED"
echo "GATE_METRIC topology_final_serving_epoch 5"
gate_exit
