#!/bin/bash
# A/B: PROVE fault_injection_gate.sh DISCRIMINATES (debt D-4).
#
# A gate that passes is only evidence if a binary WITHOUT the fix would fail it. That
# claim was made in the gate README from a hand-run session ("the same tree with only the
# three 4a files reverted FAILS it, 9 bench HTTP errors + 1 probe 5xx") and there was no
# way to re-check it. This script is that check, automated.
#
# WHAT IT COMPARES. HEAD, against HEAD with exactly the three write-scaleout 4a files
# reverted to the commit before 4a landed:
#
#     git checkout fcb2a94^ -- lib/cluster/data/write_errors.hpp \
#                              lib/cluster/data/replicated_write_router.cpp \
#                              lib/cluster/data/replicated_write_router.hpp
#
# `lib/cluster/reconnect_policy.hpp` -- also added by fcb2a94 -- is deliberately NOT
# reverted: 4b's connection jitter and keepalive depend on it and are not what is under
# test.
#
# WHY WHOLE-FILE CHECKOUT AND NOT `git revert fcb2a94`. A hunk-level revert of 4a
# CONFLICTS: two later commits (d101c07's leader-refusal labelling, c052253's lifetime
# fix) touch the same lines, so there is no clean way to remove only 4a's hunks. The
# whole-file checkout therefore takes those three files back FURTHER than 4a alone --
# it drops the later fixes in them too. That makes "the reverted binary produced errors"
# on its own a weaker claim than it looks, which is why this script also asserts the
# errors carry the [D6] SIGNATURE (`RetryableWriteError ... last: transport`): the retry
# giving up against a socket the transport had not re-dialled yet. That signature is
# specific to the pacing fix; the other reverted changes do not produce it.
#
# EXPENSIVE. This builds a second server binary from an isolated worktree (a fresh build
# dir means building seastar too -- tens of minutes the first time; later runs are
# incremental if you keep GATE_AB_BUILD_DIR) and then runs the full storm gate TWICE,
# each of which is several minutes. It is a release/on-demand check, NOT a per-commit CI
# gate. The gate it validates, fault_injection_gate.sh, is the one CI runs.
#
# It never touches your working tree: the revert happens in a `git worktree`, not here.
#
# Usage: fault_injection_ab.sh
#   GATE_AB_BUILD_DIR=<dir>   reuse (and keep) the comparison build dir
#   GATE_AB_KEEP=1            leave the worktree in place afterwards
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

REPO="$(git rev-parse --show-toplevel)"
GATE="$(pwd)/fault_injection_gate.sh"
AB_WORKTREE="${GATE_AB_WORKTREE:-/tmp/tsgate_ab_worktree}"
AB_BUILD="${GATE_AB_BUILD_DIR:-/tmp/tsgate_ab_build}"

# The revert set, and the commit to take it from. Named once.
REVERT_AT="fcb2a94^"
REVERT_FILES="lib/cluster/data/write_errors.hpp
lib/cluster/data/replicated_write_router.cpp
lib/cluster/data/replicated_write_router.hpp"

[ -x "$BUILD_DIR/bin/timestar_http_server" ] || { echo "no HEAD server binary at $BUILD_DIR/bin"; exit 2; }

# These gates are disk-hungry and the failure mode is SELF-AMPLIFYING (see the README):
# less headroom -> slower bench -> the 0.3 s resetter fires more rounds -> slower still,
# and a space problem reads as a regression. Two full storm runs need the room up front.
FREE_G=$(df -BG --output=avail /tmp | tail -1 | tr -dc '0-9')
if [ "${FREE_G:-0}" -lt 30 ]; then
    echo "ABORT: only ${FREE_G}G free on /tmp; these gates need >= 30G (see README)"
    exit 2
fi

# THE TWO ARMS MUST DIFFER ONLY BY THE REVERT. The reverted arm is built from a clean
# worktree at HEAD; the HEAD arm is whatever `$BUILD_DIR/bin/timestar_http_server`
# currently is, i.e. the WORKING TREE. Uncommitted changes therefore land in one arm and
# not the other, and the A/B silently measures them too. Refuse rather than report a
# difference that is not the one named.
DIRTY=$(git -C "$REPO" status --porcelain --untracked-files=no)
if [ -n "$DIRTY" ]; then
    echo "ABORT: working tree is dirty; the two arms would differ by more than the revert."
    printf '%s\n' "$DIRTY" | sed 's/^/  /'
    echo "  Commit or stash, rebuild \$BUILD_DIR, and re-run."
    exit 2
fi

cleanup_ab() {
    if [ "${GATE_AB_KEEP:-0}" != "1" ]; then
        git -C "$REPO" worktree remove --force "$AB_WORKTREE" 2>/dev/null
    fi
}
trap cleanup_ab EXIT

# ---------------------------------------------------------------------------
echo "=== building the 4a-reverted comparison binary ==="
git -C "$REPO" worktree remove --force "$AB_WORKTREE" 2>/dev/null
rm -rf "$AB_WORKTREE"
git -C "$REPO" worktree add --detach "$AB_WORKTREE" HEAD >/dev/null 2>&1 || {
    echo "ABORT: could not create the comparison worktree at $AB_WORKTREE"; exit 2; }

git -C "$AB_WORKTREE" rev-parse --verify "$REVERT_AT" >/dev/null 2>&1 || {
    echo "ABORT: $REVERT_AT does not resolve -- the 4a commit is not in this history"; exit 2; }
# shellcheck disable=SC2086
git -C "$AB_WORKTREE" checkout "$REVERT_AT" -- $REVERT_FILES || {
    echo "ABORT: the revert checkout failed"; exit 2; }

# ANTI-VACUITY, and the one that actually bites: if the revert is a no-op -- history
# rewritten, files moved, the fix re-landed elsewhere -- the two binaries are IDENTICAL
# and every assertion below compares a thing to itself. The reverted run would then
# produce zero errors and this script would "fail to prove discrimination" for a reason
# that has nothing to do with the server. Catch it here, where the message is honest.
CHANGED_LIST=$(git -C "$AB_WORKTREE" diff --name-only HEAD -- | sort)
CHANGED=$(printf '%s\n' "$CHANGED_LIST" | grep -c .)
EXPECTED_LIST=$(printf '%s\n' $REVERT_FILES | sort)
echo "  reverted $CHANGED file(s) to $REVERT_AT"
if [ "$CHANGED_LIST" != "$EXPECTED_LIST" ]; then
    echo "ABORT: the revert did not touch exactly the three 4a files."
    echo "       expected:"; printf '         %s\n' $EXPECTED_LIST
    echo "       got:"; printf '%s\n' "$CHANGED_LIST" | sed 's/^/         /'
    echo "       Zero files means the comparison binary would BE HEAD and every assertion"
    echo "       below would compare a thing to itself. A different set means the 4a fix"
    echo "       has moved; update REVERT_AT/REVERT_FILES in this script."
    exit 2
fi

mkdir -p "$AB_BUILD"
( cd "$AB_BUILD" && cmake "$AB_WORKTREE" >/tmp/tsgate_ab_cmake.log 2>&1 ) || {
    echo "ABORT: cmake failed; see /tmp/tsgate_ab_cmake.log"; tail -20 /tmp/tsgate_ab_cmake.log; exit 2; }
echo "  compiling (this is the slow part; first run builds seastar too)"
( cd "$AB_BUILD" && make timestar_http_server -j"$(nproc)" >/tmp/tsgate_ab_build.log 2>&1 ) || {
    echo "ABORT: build failed; see /tmp/tsgate_ab_build.log"; tail -30 /tmp/tsgate_ab_build.log; exit 2; }
AB_BIN="$AB_BUILD/bin/timestar_http_server"
[ -x "$AB_BIN" ] || { echo "ABORT: no binary at $AB_BIN"; exit 2; }
echo "  comparison binary: $AB_BIN"

# ---------------------------------------------------------------------------
# Pull the gate's own numbers back out of its output. Every assertion it prints has the
# shape "  ok: NAME = N (...)" or "  GATE FAILURE: NAME = N, expected ...", so one parse
# serves both outcomes -- which matters, because the reverted run is EXPECTED to fail and
# its numbers are the whole point.
gate_number() { # $1 = log file, $2 = assertion name prefix
    grep -F "$2 = " "$1" | head -1 | sed "s/.*$2 = //" | grep -oE '^[0-9]+'
}

run_gate() { # $1 = binary, $2 = log file  -- returns the gate's exit code
    echo "=== storm run: $1 ==="
    "$GATE" "$1" >"$2" 2>&1
    local rc=$?
    grep -E '^  (ok|GATE FAILURE)|^GATE' "$2" | sed 's/^/    /'
    return $rc
}

# ONE AT A TIME, sequentially, each with the fresh dirs the gate makes for itself. Running
# them concurrently would have them fight over ports, data dirs and disk -- and the disk
# part is what the README's self-amplifying failure is about.
run_gate "$AB_BIN" /tmp/tsgate_ab_reverted.log
REVERTED_RC=$?
run_gate "$BUILD_DIR/bin/timestar_http_server" /tmp/tsgate_ab_head.log
HEAD_RC=$?

# ---------------------------------------------------------------------------
R_ROUNDS=$(gate_number /tmp/tsgate_ab_reverted.log "reset rounds injected")
R_CONNS=$(gate_number /tmp/tsgate_ab_reverted.log "peer connections actually destroyed")
R_BENCH=$(gate_number /tmp/tsgate_ab_reverted.log "bench client HTTP errors across the reset storm")
R_PROBE=$(gate_number /tmp/tsgate_ab_reverted.log "probe 5xx across the reset storm")
H_ROUNDS=$(gate_number /tmp/tsgate_ab_head.log "reset rounds injected")
H_CONNS=$(gate_number /tmp/tsgate_ab_head.log "peer connections actually destroyed")
H_BENCH=$(gate_number /tmp/tsgate_ab_head.log "bench client HTTP errors across the reset storm")
H_PROBE=$(gate_number /tmp/tsgate_ab_head.log "probe 5xx across the reset storm")

echo "=== A/B result ==="
echo "  reverted: ${R_ROUNDS:-?} rounds / ${R_CONNS:-?} conns -> ${R_BENCH:-?} bench errors, ${R_PROBE:-?} probe 5xx (gate rc=$REVERTED_RC)"
echo "  HEAD:     ${H_ROUNDS:-?} rounds / ${H_CONNS:-?} conns -> ${H_BENCH:-?} bench errors, ${H_PROBE:-?} probe 5xx (gate rc=$HEAD_RC)"

# BOTH RUNS MUST HAVE TAKEN A COMPARABLE STORM. Comparing a heavy storm against a light
# one says nothing about the binaries, and the resetter's round count is load-dependent
# (it fires on a wall clock while the bench length is not). The gate's own floors are the
# lower bound; here we additionally require the two runs to be within 2x of each other.
assert_ge "reverted run: reset rounds"  "${R_ROUNDS:-0}" "${GATE_MIN_RESET_ROUNDS:-70}"
assert_ge "reverted run: connections destroyed" "${R_CONNS:-0}" "${GATE_MIN_RESET_CONNS:-180}"
assert_ge "HEAD run: reset rounds"      "${H_ROUNDS:-0}" "${GATE_MIN_RESET_ROUNDS:-70}"
assert_ge "HEAD run: connections destroyed" "${H_CONNS:-0}" "${GATE_MIN_RESET_CONNS:-180}"
if [ "${R_CONNS:-0}" -gt 0 ] && [ "${H_CONNS:-0}" -gt 0 ]; then
    RATIO=$(awk -v a="${R_CONNS}" -v b="${H_CONNS}" 'BEGIN{ r = (a>b) ? a/b : b/a; printf "%d", 100*r }')
    assert_le "storm intensity ratio between the two runs (%, 100 = identical)" "$RATIO" 200
fi

# THE DISCRIMINATION CLAIM, in both directions.
R_TOTAL=$(( ${R_BENCH:-0} + ${R_PROBE:-0} ))
H_TOTAL=$(( ${H_BENCH:-0} + ${H_PROBE:-0} ))
assert_ge "REVERTED binary: client errors under the storm" "$R_TOTAL" 1
assert_eq "HEAD binary: client errors under the storm" "$H_TOTAL" 0

# ...AND THE ERRORS MUST BE THE RIGHT ONES. The whole-file revert reaches past 4a (see the
# header), so "it produced errors" alone does not prove the pacing fix is what the gate
# measures. [D6] has a verbatim signature -- the retry exhausting its budget against a
# socket the transport had not re-dialled yet -- and this is what pins the mechanism.
if grep -qE 'RetryableWriteError.*last: transport' /tmp/tsgate_ab_reverted.log; then
    gate_ok "REVERTED binary: failures carry the [D6] signature (last: transport)"
else
    gate_fail "REVERTED binary failed, but NOT with the [D6] signature -- the gate may be \
catching something other than the retry pacing. First error line: \
$(grep -m1 'First error' /tmp/tsgate_ab_reverted.log)"
fi
assert_eq "HEAD binary: gate exit code" "$HEAD_RC" 0
if [ "$REVERTED_RC" -eq 0 ]; then
    gate_fail "the REVERTED binary PASSED the gate -- the gate does not discriminate"
else
    gate_ok "the reverted binary failed the gate, as it must"
fi

echo "  full logs: /tmp/tsgate_ab_reverted.log /tmp/tsgate_ab_head.log"
gate_exit
