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
    # MATCH ON THE PORT, NOT THE BINARY NAME. Every gate takes an optional server binary
    # as $1 so a "before" binary can be measured the same way. The old pattern
    # `timestar_http_server.*--port $1` matched only binaries still CALLED
    # timestar_http_server, so a run against a saved or renamed comparison binary left its
    # three servers alive and the NEXT gate aborted on "ports still in use". Measured
    # exactly that way while A/B-ing D-1 against a copy saved as `ts_pre_d1`.
    #
    # (fault_injection_ab.sh is NOT an example: it builds a real
    # `<build>/bin/timestar_http_server` and launches it by full path, so the old pattern
    # did match it. The fix is for hand-saved binaries, which is how people actually
    # measure a "before".)
    #
    # THE PREFIX IS NOT UNIQUE PER GATE -- 493 covers both fault_injection (49310-49312)
    # and deposed_primary (49310-49314), 492 covers both backpressure and
    # rolling_rebalance -- so this DOES reach a same-band gate's cluster. That is fine,
    # and it is why the README's "run them ONE AT A TIME" rule is a rule: with only one
    # gate live, anything else in the band is a stray from a crashed run and killing it is
    # the cleanup you want. Do not read this as licence to run two gates at once; they
    # would fight over ports and data dirs long before they fought over this pkill.
    #
    # Safe to match loosely here because this lives in a SCRIPT FILE: the same pattern
    # inlined into `bash -c` would match the invoking shell's own argv and kill it
    # mid-command (see the note at the top of this file).
    pkill -u "$(id -u)" -9 -f -- "--port $1" 2>/dev/null
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
# It deliberately does NOT look at `vshards_leaderless`, which is now conservatism rather
# than necessity. That counter USED to be meaningful only at RF == N: a node reported
# every VShard it does not HOST as leaderless (counts() read kNoNode from its own
# registry), so on a 5-node RF=3 cluster each node reported ~1638 and the sum never fell.
# D-13 fixed the accounting at its source -- counts() now skips VShards this node does not
# host, exactly as gatherLeaders() does. It is still a PER-NODE count of that node's own
# replicas, so at RF < N a leaderless VShard is reported by each of its RF holders and a
# cross-node SUM over-counts by up to RF; wait_all_led wants a cluster-wide total, which
# the summed vshards_led gives exactly (a node only ever claims leadership it holds).
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

# wait_leadership_settled "PORT..." MAX_POLLS -- the led-per-node vector is IDENTICAL
# across two consecutive polls, i.e. no VShard changed hands in the interval.
#
# A rebalance storm leaves VShards mid-transfer for a while, and a write to one of those
# is momentarily leaderless -- a real condition, but it is the ROLLING-REBALANCE gate's
# subject, not this one. A DEPOSED PRIMARY is a steady state: the primary is alive, some
# other node leads, and nothing is moving. Measuring before leadership settles conflates
# the two and makes the deposed-primary result depend on how much churn the storm
# happened to leave behind.
#
# It doubles as a check on leader stability: if leadership never stops moving on an idle
# cluster (e.g. spurious checkQuorum step-downs), this never settles and the gate fails
# here rather than silently blaming the write path.
# "Settled" is a TOLERANCE, not exact equality: the background leadership balancer runs
# every few seconds over 4096 groups and never stops nudging, so an exact repeat of the
# per-node led vector may never occur. What distinguishes settled from churning is the
# SIZE of the movement -- single digits per poll versus the hundreds a rebalance storm
# produces.
wait_leadership_settled() {
    local ports="$1" polls="${2:-40}" tol="${3:-32}" prev="" cur delta
    for _ in $(seq 1 "$polls"); do
        sleep 2
        cur=""
        for p in $ports; do cur="$cur $(status_field "$(cluster_status "$p")" vshards_led)"; done
        case "$cur" in *[0-9]*) ;; *) continue ;; esac
        if [ -n "$prev" ]; then
            local before="$prev"
            delta=$(awk -v a="$prev" -v b="$cur" 'BEGIN{na=split(a,A," ");split(b,B," ");d=0;
                for(i=1;i<=na;i++){x=A[i]-B[i]; if(x<0)x=-x; d+=x} print d}')
            if [ "${delta:-9999}" -le "$tol" ]; then
                echo "  leadership settled at [$cur] (moved $delta VShards in the last 2s)"
                return 0
            fi
        fi
        prev="$cur"
    done
    gate_fail "leadership never settled within $((polls * 2))s -- the last two samples differ by ${delta:-?} VShards, i.e. leaders are still moving in bulk on an idle cluster"
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
