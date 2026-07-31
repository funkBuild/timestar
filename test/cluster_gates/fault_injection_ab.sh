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
# WHAT "DISCRIMINATES" MEANS HERE, since D-21. It is no longer "the reverted arm produced
# an error and HEAD produced none": HEAD is not reliably zero (0, 1, 0 and one void across
# six consecutive single-storm runs of one unchanged binary), so that shape was satisfied by
# HEAD's own noise. The gate now runs K storms and budgets their TOTAL, and this script
# asserts a SEPARATION -- HEAD within the budget, the reverted arm at 3x it or worse. The
# real margin is wider than that bound: a reverted binary produces 9-10 errors in a SINGLE
# storm where HEAD's whole K-storm budget is 3.
#
# EXPENSIVE. This builds a second server binary from an isolated worktree (a fresh build
# dir means building seastar too -- tens of minutes the first time; later runs are
# incremental because the build dir is KEPT by default) and then runs the full storm gate
# TWICE, each of which is now K storms rather than one. It is a release/on-demand check,
# NOT a per-commit CI gate. The gate it validates, fault_injection_gate.sh, is the one CI
# runs.
#
# It never touches your working tree: the revert happens in a `git worktree`, not here.
#
# Usage: fault_injection_ab.sh
#   GATE_AB_BUILD_DIR=<dir>   comparison build dir; default ../tsdb-ab-build BESIDE THE
#                             REPO, deliberately NOT on /tmp -- see the note at its
#                             assignment. Kept between runs so the second A/B is
#                             incremental; delete it by hand to reclaim ~10 G.
#   GATE_AB_WORKTREE=<dir>    comparison worktree; default ../tsdb-ab-worktree
#   GATE_AB_KEEP=1            leave the worktree in place afterwards
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

REPO="$(git rev-parse --show-toplevel)"
GATE="$(pwd)/fault_injection_gate.sh"
# NEITHER OF THESE MAY LIVE ON /tmp, and that is a hard requirement rather than a
# preference. /tmp here is a tmpfs with a per-user quota, and the storm gate needs every
# gigabyte of it for its data dirs; a comparison worktree (~1 G with seastar) plus a full
# build tree (~10 G) inside the same quota is precisely the self-amplifying disk failure
# the README describes -- and it would be spent on build output, which is not what the
# quota is for. They default beside the repo instead, on real disk, so the build tree also
# SURVIVES between runs and the second A/B is incremental.
AB_WORKTREE="${GATE_AB_WORKTREE:-$(dirname "$REPO")/tsdb-ab-worktree}"
AB_BUILD="${GATE_AB_BUILD_DIR:-$(dirname "$REPO")/tsdb-ab-build}"

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
# `--ignore-submodules=all` IS LOAD-BEARING, not tidiness. `external/seastar` is a gitlink
# whose `.git` file points at a modules directory that does not exist in this checkout, so
# a plain `git status` ABORTS on it -- "fatal: not a git repository" to stderr, NOTHING to
# stdout, and a zero-length $DIRTY. Measured: with a genuinely modified tracked file in the
# tree, the unguarded form still reported clean. The guard was therefore vacuous and could
# never have fired, which is the worst state for a check whose whole job is to refuse.
DIRTY=$(git -C "$REPO" status --porcelain --untracked-files=no --ignore-submodules=all)
if [ -n "$DIRTY" ]; then
    echo "ABORT: working tree is dirty; the two arms would differ by more than the revert."
    printf '%s\n' "$DIRTY" | sed 's/^/  /'
    echo "  Commit or stash, rebuild \$BUILD_DIR, and re-run."
    exit 2
fi

cleanup_ab() {
    if [ "${GATE_AB_KEEP:-0}" != "1" ]; then
        git -C "$REPO" worktree remove --force "$AB_WORKTREE" 2>/dev/null
        # The seastar symlink below makes the submodule path look modified, which `git
        # worktree remove` can refuse even with --force. Take the directory out by hand
        # and prune the registration, or the NEXT run's `worktree add` fails on a path
        # that is still claimed.
        rm -rf "$AB_WORKTREE"
        git -C "$REPO" worktree prune 2>/dev/null
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

# SEASTAR HAS TO BE PUT BACK BY HAND. `external/seastar` is a SUBMODULE, and `git worktree
# add` leaves a submodule as an empty directory -- so `add_subdirectory(external/seastar)`
# fails and cmake aborts before compiling a line. `git submodule update --init` cannot fix
# it either: this checkout has no `.git/modules/external/seastar`, so it would have to
# clone ~1 G from GitHub over the network for a dependency that is byte-identical to the
# one already on disk.
#
# A SYMLINK IS CORRECT HERE, not a shortcut. The revert set is three files under
# lib/cluster/data; seastar is not in it and cannot be, so both arms are meant to compile
# the SAME seastar source. The build is out-of-source (everything lands in $AB_BUILD), so
# sharing the source directory cannot let one arm's objects reach the other's.
if [ ! -e "$AB_WORKTREE/external/seastar/CMakeLists.txt" ]; then
    [ -e "$REPO/external/seastar/CMakeLists.txt" ] || {
        echo "ABORT: $REPO/external/seastar is not populated either -- nothing to share"; exit 2; }
    rmdir "$AB_WORKTREE/external/seastar" 2>/dev/null
    ln -sfn "$REPO/external/seastar" "$AB_WORKTREE/external/seastar" || {
        echo "ABORT: could not link seastar into the comparison worktree"; exit 2; }
    echo "  seastar shared by symlink from $REPO (submodules are not populated in a worktree)"
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
# Pull the gate's own numbers back out of its output, from the `GATE_METRIC name value`
# lines it prints BEFORE its assertions -- so they are there whether the run passed or
# failed, and the reverted arm (which is EXPECTED to fail) reports its numbers too.
#
# This used to scrape the assertion text and DID NOT WORK: it looked for
# "client errors across the reset storms = " while the gate prints
# "client errors across the reset storms (bench + probe, 3 storms) = 6", so `grep -F`
# matched nothing, both arms parsed empty, and the separation assertions below ran on
# `${R_TOTAL:-0}` vs `${H_TOTAL:-999}` -- i.e. they compared two defaults and would have
# reported a discrimination nobody measured.
gate_number() { # $1 = log file, $2 = metric name
    grep -E "^GATE_METRIC $2 " "$1" | head -1 | awk '{print $3}' | grep -oE '^[0-9]+'
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
# The error counter is now an AGGREGATE over the gate's K storms (debt D-21); the gate
# prints the per-storm vector next to it, which is what to read when the two arms are
# closer than expected.
R_ROUNDS=$(gate_number /tmp/tsgate_ab_reverted.log reset_rounds_total)
R_CONNS=$(gate_number /tmp/tsgate_ab_reverted.log reset_conns_total)
R_TOTAL=$(gate_number /tmp/tsgate_ab_reverted.log storm_errors_total)
H_ROUNDS=$(gate_number /tmp/tsgate_ab_head.log reset_rounds_total)
H_CONNS=$(gate_number /tmp/tsgate_ab_head.log reset_conns_total)
H_TOTAL=$(gate_number /tmp/tsgate_ab_head.log storm_errors_total)
# A metric that did not parse is a HARNESS failure, and it must not be allowed to reach
# the separation assertions as a default -- that is exactly how the old scrape produced an
# unmeasured "pass". Fail here, where the message names the cause.
for _m in R_ROUNDS R_CONNS R_TOTAL H_ROUNDS H_CONNS H_TOTAL; do
    eval "_v=\${$_m:-}"
    [ -n "$_v" ] || gate_fail "could not parse $_m from the gate log -- did the run reach its GATE_METRIC lines? (a VOID or a timeout does not)"
done

echo "=== A/B result ==="
echo "  reverted: ${R_ROUNDS:-?} rounds / ${R_CONNS:-?} conns -> ${R_TOTAL:-?} client errors (gate rc=$REVERTED_RC)"
echo "  HEAD:     ${H_ROUNDS:-?} rounds / ${H_CONNS:-?} conns -> ${H_TOTAL:-?} client errors (gate rc=$HEAD_RC)"
grep -h "storms: errors\[" /tmp/tsgate_ab_reverted.log | sed 's/^/    reverted /'
grep -h "storms: errors\[" /tmp/tsgate_ab_head.log | sed 's/^/    HEAD     /'

# A VOID run (exit 3) is neither arm's answer: the gate's own fault-free control failed, so
# the storm that followed measured the environment. Re-draw rather than reporting an A/B
# that did not happen -- and say which arm voided, because the reverted arm voiding looks
# superficially like the discrimination this script exists to prove.
if [ "$REVERTED_RC" -eq 3 ] || [ "$HEAD_RC" -eq 3 ]; then
    echo "ABORT (VOID): the $([ "$REVERTED_RC" -eq 3 ] && echo REVERTED || echo HEAD) arm voided --"
    echo "  its fault-free baseline through the proxy failed, so neither arm's storm is"
    echo "  comparable. Re-run; check /tmp free space first (see the gate README)."
    exit 3
fi

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

# THE DISCRIMINATION CLAIM, in both directions -- and it is a SEPARATION, not a
# zero-versus-nonzero (debt D-21). HEAD is not reliably zero: measured over six consecutive
# single-storm runs of one unchanged HEAD binary, the draws were 0, 1, 0 and one void. So
# "the reverted arm produced at least one error" no longer means anything on its own -- it
# is satisfied by HEAD's own noise. The arms are separated by a FACTOR instead: the gate's
# budget is what HEAD must stay inside, and the reverted arm must exceed it several times
# over. The margin is real -- a reverted binary produces 9-10 errors in a SINGLE storm
# where HEAD's whole K-storm budget is 3.
BUDGET="${GATE_MAX_STORM_ERRORS:-3}"
assert_le "HEAD binary: client errors under the storms" "${H_TOTAL:-999}" "$BUDGET"
assert_ge "REVERTED binary: client errors under the storms" "${R_TOTAL:-0}" "$((BUDGET * 3))"

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
