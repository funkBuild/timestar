#!/bin/bash
# Shared helpers for the cluster write-path gates. Source, don't execute.
#
# NOTE ON pkill: only ever from a SCRIPT FILE, never inlined into `bash -c`, because an
# inline pattern matches the invoking shell's own argv and kills it mid-command.

BUILD_DIR="${BUILD_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../build" && pwd)}"
GATE_FAILURES=0

gate_fail() {
    echo "  GATE FAILURE: $*" >&2
    GATE_FAILURES=$((GATE_FAILURES + 1))
}
gate_ok() { echo "  ok: $*"; }

# assert_eq NAME ACTUAL EXPECTED
assert_eq() {
    if [ "$2" = "$3" ]; then gate_ok "$1 = $2"; else gate_fail "$1 = $2, expected $3"; fi
}
# assert_ge NAME ACTUAL MIN
assert_ge() {
    if [ "${2:-0}" -ge "$3" ] 2>/dev/null; then gate_ok "$1 = $2 (>= $3)"; else gate_fail "$1 = ${2:-<empty>}, expected >= $3"; fi
}
# assert_le NAME ACTUAL MAX
assert_le() {
    if [ "${2:-999999999}" -le "$3" ] 2>/dev/null; then gate_ok "$1 = $2 (<= $3)"; else gate_fail "$1 = ${2:-<empty>}, expected <= $3"; fi
}

gate_exit() {
    if [ "$GATE_FAILURES" -gt 0 ]; then
        echo "GATE FAILED ($GATE_FAILURES assertion(s))"
        exit 1
    fi
    echo "GATE PASSED"
    exit 0
}

kill_cluster() { # $1 = port prefix used by this gate, e.g. 492
    pkill -u "$(id -u)" -9 -f "timestar_http_server.*--port $1" 2>/dev/null
    pkill -u "$(id -u)" -9 -f 'timestar_insert_bench' 2>/dev/null
    sleep 2
}

cluster_status() { curl -s -m3 "http://127.0.0.1:$1/cluster/status" 2>/dev/null; }
status_field() { printf '%s' "$1" | grep -o "\"$2\":[0-9]*" | cut -d: -f2; }

# wait_all_led "PORT..." TOTAL_VSHARDS MAX_POLLS -- every VShard has SOME leader
# somewhere, summed across nodes. Use this when the gate does not need leadership to be
# evenly spread (and especially when it goes on to create imbalance deliberately): on a
# fresh 5-node RF=3 cluster the background balancer needs minutes to reach fair shares,
# and waiting for that is not what the gate is testing.
#
# It deliberately does NOT look at `vshards_leaderless`. That counter is only meaningful
# at RF == N: a node reports every VShard it does not HOST as leaderless (counts() reads
# kNoNode from its own registry), so on a 5-node RF=3 cluster each node reports ~1638 and
# the sum never falls. Same pre-existing accounting that makes gatherLeaders() fail reads
# on RF < N clusters; it belongs to the query plan, not here. The summed vshards_led IS
# sound -- a node only ever claims leadership it actually holds.
wait_all_led() {
    local ports="$1" total="$2" polls="${3:-120}"
    for _ in $(seq 1 "$polls"); do
        sleep 2
        local ok=1 led=0 s
        for p in $ports; do
            s=$(cluster_status "$p"); [ -z "$s" ] && { ok=0; break; }
            led=$((led + $(status_field "$s" vshards_led)))
        done
        [ "$ok" = "0" ] && continue
        if [ "$led" -ge $((total - 16)) ]; then
            echo "  cluster converged (led=$led/$total across $(echo "$ports" | wc -w) nodes)"
            return 0
        fi
    done
    gate_fail "cluster did not converge within $((polls * 2))s (led=$led/$total)"
    return 1
}

# wait_balanced "PORT..." TOTAL_VSHARDS NODES MAX_POLLS -- leadership spread and settled.
wait_balanced() {
    local ports="$1" total="$2" nodes="$3" polls="${4:-90}"
    local fair=$((total / nodes)) lo hi stable=0
    lo=$((fair * 80 / 100))
    hi=$((fair * 120 / 100))
    for _ in $(seq 1 "$polls"); do
        sleep 2
        local ok=1 leaderless=0 maxl=0 minl=999999 s l x
        for p in $ports; do
            s=$(cluster_status "$p"); [ -z "$s" ] && { ok=0; break; }
            l=$(status_field "$s" vshards_led); x=$(status_field "$s" vshards_leaderless)
            leaderless=$((leaderless + ${x:-9999}))
            [ "${l:-0}" -gt "$maxl" ] && maxl=${l:-0}
            [ "${l:-0}" -lt "$minl" ] && minl=${l:-0}
        done
        [ "$ok" = "0" ] && continue
        if [ "$minl" -ge "$lo" ] && [ "$maxl" -le "$hi" ] && [ "$leaderless" -le 8 ]; then
            stable=$((stable + 1))
            [ "$stable" -ge 2 ] && { echo "  cluster converged and balanced (led $minl..$maxl, fair $fair)"; return 0; }
        else
            stable=0
        fi
    done
    gate_fail "cluster did not converge/balance within $((polls * 2))s"
    return 1
}

# Refuse to start if ANYTHING is still listening on the ports this gate uses.
#
# A survivor from an earlier run does not merely conflict: seastar exits on the failed
# bind, so one node silently never comes up and the gate measures a degraded cluster (or,
# worse, the survivor answers some requests and the gate measures TWO clusters at once).
# The Raft and data-plane listeners sit at +2000/+1000 offsets from the HTTP port, so all
# three have to be checked -- an earlier version of these gates only killed by HTTP port
# and was bitten by exactly that.
require_ports_free() { # $@ = HTTP ports
    local busy="" p
    for p in "$@"; do
        for q in "$p" $((p + 1000)) $((p + 2000)); do
            if ss -ltn 2>/dev/null | grep -qE "[:.]$q\b"; then busy="$busy $q"; fi
        done
    done
    if [ -n "$busy" ]; then
        echo "ABORT: ports still in use:$busy -- a previous run's server is still alive" >&2
        ss -ltnp 2>/dev/null | grep -E "$(echo "$busy" | tr ' ' '|' | sed 's/^|//')" >&2
        exit 2
    fi
}
