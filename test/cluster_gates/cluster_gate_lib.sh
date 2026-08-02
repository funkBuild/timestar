#!/bin/bash
# Shared helpers for the cluster write-path gates. Source, don't execute.
#
# NOTE ON pkill: only ever from a SCRIPT FILE, never inlined into `bash -c`, because an
# inline pattern matches the invoking shell's own argv and kills it mid-command.

BUILD_DIR="${BUILD_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../build" && pwd)}"
GATE_FAILURES=0

# EXTRA SERVER ENVIRONMENT, applied by every gate to every node it starts.
#
#     GATE_SERVER_ENV="TIMESTAR_CLUSTER_SHARED_JOURNAL=1" ./fault_injection_gate.sh
#
# Why a passthrough rather than editing a gate when a flag needs exercising: a gate's value
# is that the arm under test differs from the recorded arm ONLY in the thing being tested.
# Editing the launch line to add a flag makes the run unquotable against the recorded one
# (the script itself changed), and the edit tends to survive into the next run by accident.
# This keeps the flag in the INVOCATION, where the transcript records it.
#
# Deliberately word-split (it is a list of KEY=VAL pairs, not one argument), so values
# containing spaces are not supported -- no server env var takes one, and quoting rules
# that half-work are worse than a stated limit. It is prepended, so a gate's own settings
# for the same key still win: `env A=1 A=2` takes the LAST assignment, and a gate that
# pins a value pins it for a reason (backpressure_gate's in-flight budget, for one).
GATE_SERVER_ENV="${GATE_SERVER_ENV:-}"

# Every gate runs multiple Seastar processes on one host. Without an explicit
# budget each process sizes itself from the host's full available RAM, so three
# otherwise healthy nodes can collectively overcommit the machine and kill the
# harness during catch-up/compaction. Keep the default aggregate bounded and
# reproducible; a capacity run may override this in its invocation.
GATE_SERVER_MEMORY="${GATE_SERVER_MEMORY:-8G}"

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

# remove_gate_data_dirs DIR... -- remove only this harness's narrowly-named
# roots under /tmp or build/tmp, retrying races and proving they are gone before
# returning. New low-volume correctness gates prefer build/tmp so they do not
# consume the host's quota-limited tmpfs; historical load gates retain /tmp so
# their recorded I/O shape stays comparable.
#
# A plain `rm -rf; mkdir -p` is not a reset unless removal actually succeeded.
# snapshot_durability_gate exposed this with "Directory not empty" and then
# continued into the next arm.  It also had redundant top-level rm calls while
# the prior arm's restarted servers were STILL RUNNING, guaranteeing a race.
# Never let a release gate measure state whose provenance is ambiguous.
remove_gate_data_dirs() {
    local d suffix pass remaining
    for d in "$@"; do
        case "$d" in
            /tmp/tsgate_*) suffix="${d#/tmp/tsgate_}" ;;
            "$BUILD_DIR"/tmp/tsgate_*) suffix="${d#"$BUILD_DIR"/tmp/tsgate_}" ;;
            *) suffix="" ;;
        esac
        if [ -z "$suffix" ] || [[ "$suffix" == */* ]]; then
            echo "ABORT: refusing to recursively remove non-gate path: $d" >&2
            return 2
        fi
    done

    for pass in 1 2 3 4 5; do
        rm -rf -- "$@" 2>/dev/null
        remaining=""
        for d in "$@"; do [ -e "$d" ] && remaining="$remaining $d"; done
        [ -z "$remaining" ] && return 0
        echo "  gate data cleanup retry $pass/5; still present:$remaining" >&2
        sleep 1
    done
    echo "ABORT: gate data directories survived verified cleanup:$remaining" >&2
    return 1
}

fresh_gate_data_dirs() {
    local d
    remove_gate_data_dirs "$@" || return
    for d in "$@"; do
        mkdir -p -- "$d" || {
            echo "ABORT: could not create fresh gate data directory: $d" >&2
            return 1
        }
    done
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
    # SINCE D-27 THE PREFIX IS UNIQUE PER GATE (four digits: 1921 backpressure, 1922
    # rolling rebalance, 1931 deposed primary, 1941 fault injection,
    # 1951 restart catchup, 1961 node kill, 1971 snapshot durability, 1973 restart
    # readback -- the README's port table is authoritative). It used to be three, and 492
    # covered both backpressure and rolling_rebalance, so a cleanup reached a same-band
    # gate's cluster. That was harmless under the ONE AT A TIME rule and is now moot.
    #
    # Keep the rule anyway: two gates at once fight over data dirs, disk and CPU long
    # before they fight over this pkill, and the README's self-amplifying disk failure is
    # what that costs. With only one gate live, anything else in ITS band is a stray from a
    # crashed run and killing it is the cleanup you want.
    #
    # Safe to match loosely here because this lives in a SCRIPT FILE: the same pattern
    # inlined into `bash -c` would match the invoking shell's own argv and kill it
    # mid-command (see the note at the top of this file).
    pkill -u "$(id -u)" -9 -f -- "--port $1" 2>/dev/null
    pkill -u "$(id -u)" -9 -f 'timestar_insert_bench' 2>/dev/null
    sleep 2
}

# gate_cleanup PREFIX DATADIR... -- kill this gate's cluster AND RECLAIM ITS DISK.
#
# THE rm IS THE POINT, and it is not tidiness. These gates run on a tmpfs /tmp with a
# per-user QUOTA, and a gate that leaves 3-10 G of data dirs behind does not merely waste
# space: the next gate's bench slows down, the 0.3 s resetter fires more rounds, it slows
# further, and the run fails as a plausible-looking "regression" (the README's
# self-amplifying failure). Left unattended it exhausts the quota outright, at which point
# "Disk quota exceeded" reaches every process on the box -- including the shell driving
# the gate, which is how one session lost its whole harness mid-run.
#
# Only `restart_readback_gate.sh` did this before; every other gate leaked its dirs.
#
# The node LOGS are what a failed run is diagnosed from, so the tail of each is copied out
# BEFORE the delete -- a few hundred KB that survives, instead of gigabytes that must not.
# `GATE_KEEP_DATA=1` keeps everything, for when the tail is not enough.
gate_cleanup() {
    local prefix="$1"; shift
    kill_cluster "$prefix"
    if [ "${GATE_KEEP_DATA:-0}" = "1" ]; then
        echo "  GATE_KEEP_DATA=1: leaving $* in place ($(du -shc "$@" 2>/dev/null | tail -1 | cut -f1))"
        return
    fi
    local tails="/tmp/tsgate_${prefix}_tails.log" d f
    : >"$tails"
    for d in "$@"; do
        [ -d "$d" ] || continue
        for f in "$d"/*.log; do
            [ -f "$f" ] || continue
            { echo "=== $f (last 300 lines) ==="; tail -n 300 "$f"; } >>"$tails"
        done
    done
    if remove_gate_data_dirs "$@"; then
        echo "  cleaned: $* (log tails kept in $tails)"
    else
        echo "  CLEANUP INCOMPLETE: $* (log tails kept in $tails)" >&2
    fi
}

cluster_status() { curl -s -m3 "http://127.0.0.1:$1/cluster/status" 2>/dev/null; }
status_field() { printf '%s' "$1" | grep -o "\"$2\":[0-9]*" | cut -d: -f2; }
status_bool() { printf '%s' "$1" | grep -o "\"$2\":\(true\|false\)" | cut -d: -f2; }

# wait_healthy "PORT..." MAX_POLLS -- every node satisfies the public readiness
# contract. Leadership balance alone is not enough: /health also fences apply
# lag/failures, unresolved peers and snapshot capability. Benches perform their
# own /health preflight, so starting one before this converges produces an empty
# campaign that looks like a client or admission failure.
wait_healthy() {
    local ports="$1" polls="${2:-60}" p ok last=""
    for _ in $(seq 1 "$polls"); do
        ok=1
        for p in $ports; do
            if ! curl -fsS -m3 "http://127.0.0.1:$p/health" >/dev/null 2>&1; then
                ok=0
                last=$(curl -s -m3 "http://127.0.0.1:$p/health" 2>/dev/null | head -c 300)
                break
            fi
        done
        if [ "$ok" = "1" ]; then
            echo "  all nodes satisfy /health readiness"
            return 0
        fi
        sleep 1
    done
    gate_fail "cluster did not become healthy within ${polls}s (last response: ${last:-unreachable})"
    return 1
}

# report_journal_counters "PORT..." -- print the Raft journal's fsync coalescing (D-10) for
# each node, plus GATE_METRIC totals. INFORMATIONAL: no gate asserts on it, because the
# ratio depends on how many groups happened to drain together, which is load-shaped.
#
# It is here rather than in one gate because the number is only interesting on a run that
# also injected something: `journal_sync_requests / journal_fsyncs` is 1.0 for the default
# per-VShard layout (each group syncs its own fd, so there is nothing to coalesce) and > 1
# only with TIMESTAR_CLUSTER_SHARED_JOURNAL=1. On this box the journal lives on tmpfs, so
# the ratio -- not a throughput delta -- is the whole of the available evidence.
report_journal_counters() {
    local ports="$1" s shared f r tf=0 tr=0
    for p in $ports; do
        s=$(cluster_status "$p")
        shared=$(status_bool "$s" journal_shared)
        f=$(status_field "$s" journal_fsyncs)
        r=$(status_field "$s" journal_sync_requests)
        [ -z "$f" ] && { echo "  journal counters on :$p unavailable"; continue; }
        tf=$((tf + f)); tr=$((tr + r))
        echo "  :$p journal_shared=${shared:-?} fsyncs=$f sync_requests=$r" \
            "coalescing=$(awk -v a="$r" -v b="$f" 'BEGIN{printf (b>0)?"%.2f":"n/a", a/b}')"
    done
    echo "GATE_METRIC journal_fsyncs_total $tf"
    echo "GATE_METRIC journal_sync_requests_total $tr"
    echo "GATE_METRIC journal_coalescing $(awk -v a="$tr" -v b="$tf" 'BEGIN{printf (b>0)?"%.3f":"0", a/b}')"
}

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
# PORTS MUST SIT BELOW THE EPHEMERAL RANGE (`cat /proc/sys/net/ipv4/ip_local_port_range`,
# 32768-60999 here), and this function now ENFORCES that rather than merely advising it
# (debt D-27). A gate starts its nodes in a burst and each dials the others' Raft and data
# ports, so a gate whose own ports sit inside that range races the kernel for them: an
# outbound connection from an earlier node gets handed the port a later node still has to
# bind, seastar exits on the failed listen, and the gate reports "cluster did not converge"
# with one node silently absent. It scales with node count and with how busy the box is --
# `deposed_primary_gate.sh` (five nodes) hit it four times in one session (49312 once, then
# 51312 on three consecutive runs) before being moved to 19310+, which fixed it outright.
#
# D-27 moved EVERY gate below the range; the check below is what keeps a new one from
# reintroducing the race. It is an ABORT and not a warning on purpose: the failure it
# prevents does not look like a port problem when it happens -- it looks like a 300 s
# convergence timeout with one node missing, and the cause is only visible in a node log.
#
# It is checked against the LIVE kernel range, not a hardcoded 32768: a box configured with
# a wider range (some tune it down to 10000) has a correspondingly wider hazard, and a gate
# that is safe here would silently be unsafe there.
#
# All three of a node's listeners are checked -- HTTP, data plane (+1000) and Raft (+2000)
# -- both for the in-use test and for the range test, so a base port that is itself safe
# but whose Raft port is not still fails here rather than at bind time.
require_ports_free() { # $@ = HTTP ports
    local busy="" ephemeral="" p q lo hi
    read -r lo hi < /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null
    : "${lo:=32768}" "${hi:=60999}"
    for p in "$@"; do
        for q in "$p" $((p + 1000)) $((p + 2000)); do
            if ss -ltn 2>/dev/null | grep -qE "[:.]$q\b"; then busy="$busy $q"; fi
            if [ "$q" -ge "$lo" ] && [ "$q" -le "$hi" ]; then ephemeral="$ephemeral $q"; fi
        done
    done
    if [ -n "$ephemeral" ]; then
        echo "ABORT: gate ports inside the kernel ephemeral range ($lo-$hi):$ephemeral" >&2
        echo "       A node's own outbound dial can be handed one of these before a later" >&2
        echo "       node binds it; seastar then exits on the failed listen and the gate" >&2
        echo "       reports 'cluster did not converge'. Pick a base below $lo - 2000." >&2
        echo "       See debt D-27 and test/cluster_gates/README.md's port table." >&2
        exit 2
    fi
    if [ -n "$busy" ]; then
        echo "ABORT: ports still in use:$busy -- a previous run's server is still alive" >&2
        ss -ltnp 2>/dev/null | grep -E "$(echo "$busy" | tr ' ' '|' | sed 's/^|//')" >&2
        exit 2
    fi
}
