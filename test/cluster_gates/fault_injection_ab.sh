#!/bin/bash
# A/B: PROVE fault_injection_gate.sh DISCRIMINATES (debt D-4).
#
# A gate that passes is only evidence if a binary WITHOUT the fix would fail it. That
# claim was made in the gate README from a hand-run session ("the same tree with only the
# three 4a files reverted FAILS it, 9 bench HTTP errors + 1 probe 5xx") and there was no
# way to re-check it. This script is that check, automated.
#
# WHAT IT COMPARES. HEAD, against HEAD with 4a's PACING and nothing else undone: the
# retry's pause between attempts goes back to a FLAT 20 ms for the Transport/Overloaded
# classes, which is the whole of what write-scaleout 4a changed about behaviour.
#
# THE FILE CHECKOUT THIS USED TO DO NO LONGER COMPILES, and finding that out is what
# executing this script end to end (debt D-19) was for. It ran
#
#     git checkout fcb2a94^ -- lib/cluster/data/write_errors.hpp \
#                              lib/cluster/data/replicated_write_router.cpp \
#                              lib/cluster/data/replicated_write_router.hpp
#
# which still produces exactly the intended 3-file diff -- and then fails to build, because
# later work depends on symbols the pre-4a versions of those files do not define:
#
#     replicated_vshard_host.cpp:620: 'LeaderRefused' is not a member of 'WriteFailure'
#     cluster_data_plane.hpp:109:     'kElectionDeadline' is not a member of
#                                     'ReplicatedBatchWriteRouter'
#
# So the incantation was stale, its "verified to produce the intended diff" check could
# never have caught it, and the comparison binary it describes has never existed.
#
# A SURGICAL PATCH REPLACES IT, and the result is a STRONGER A/B than the checkout was. The
# two edits are:
#
#   1. `write_errors.hpp`: `writeFailureRetryDelay` stops doubling for Transport/Overloaded
#      and returns the base -- i.e. the flat 20 ms of the pre-4a loop.
#   2. `replicated_write_router.hpp`: the coupling `static_assert` is neutralised. That
#      assert exists PRECISELY to make [D6] a compile error ("the write retry schedule must
#      outlast the transport reconnect window"), so disabling it is the honest inverse of
#      the fix rather than collateral.
#
# The old checkout reached PAST 4a -- it dropped d101c07's and c052253's later fixes in
# those files too, which is why the header used to hedge that "the reverted binary produced
# errors" was a weaker claim than it looked. Nothing is hedged now: the arms differ by the
# retry pause and by nothing else.
#
# THE [D6] SIGNATURE IS INFORMATIONAL, NOT THE DISCRIMINATOR, and saying otherwise was this
# script's last wrong evidence claim. It names a failure CLASS that BOTH binaries reach:
# measured on one run's retained logs, HEAD carried the identical
# `uncommitted after 6 attempt(s) (last: transport)` give-up TWICE where the reverted arm
# carried it six times, at the same attempt count. Its PRESENCE therefore proves nothing --
# what separates the arms is the RATE, which is what the separation assertion is on.
#
# WHAT "DISCRIMINATES" MEANS HERE, since D-21. It is no longer "the reverted arm produced
# an error and HEAD produced none": HEAD is not reliably zero (0, 1, 0 and one void across
# six consecutive single-storm runs of one unchanged binary), so that shape was satisfied by
# HEAD's own noise. The gate now runs K storms and budgets their TOTAL, and this script
# asserts a SEPARATION -- HEAD within the budget, the reverted arm at 3x it or worse. The
# real margin is wider than that bound: a reverted binary produces 9-10 errors in a SINGLE
# storm where HEAD's whole K-storm budget is 6.
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
#   GATE_BUILD_JOBS=<n>       comparison-build concurrency; default 2
#   GATE_AB_KEEP=1            leave the worktree in place afterwards
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

REPO="$(git rev-parse --show-toplevel)"
GATE="$(pwd)/fault_injection_gate.sh"

# THE A/B STORMS HARDER THAN THE CI GATE, and it must, because the two are sized for
# different things. The discrimination signal is the reverted binary failing writes it
# cannot re-dial for, so it scales with the number of RESET ROUNDS a bench is exposed to;
# the CI gate's own sizing is chosen for DISK (K+1 benches against one cluster, ~27 G of a
# 62 G tmpfs) and not for signal. Measured at the gate's default 1000 batches / K=3, three
# A/B draws came out HEAD 0/4/2 against REVERTED 7/22/4 -- real, but the third draw's arms
# OVERLAP and the reverted binary passed the gate outright. At 2000 batches a storm injects
# ~147 rounds instead of ~79, which is the intensity behind the original "9-10 errors in a
# SINGLE storm" observation.
#
# So: twice the bench, two storms instead of three. Both arms get the identical setting --
# that is the whole point, and the storm-intensity ratio is asserted below -- and the run
# stays at 3 benches, i.e. the ~34 G peak measured for that shape -- which is why the
# free-space precondition below is 40 G and not the 30 G it used to be: 34 G is the exact
# point at which the K=3 sizing was killed for filling this tmpfs, so a 30 G floor admitted
# a run that could not fit.
export GATE_BENCH_BATCHES="${GATE_BENCH_BATCHES:-2000}"
export GATE_STORM_ROUNDS="${GATE_STORM_ROUNDS:-2}"
# NEITHER OF THESE MAY LIVE ON /tmp, and that is a hard requirement rather than a
# preference. On hosts where /tmp is tmpfs, a comparison worktree (~1 G with seastar)
# plus a full build tree (~10 G) consumes raw memory before either storm arm starts.
# Gate data now defaults to repository-local disk for the same reason. These larger,
# reusable trees default beside the repo so the build also SURVIVES between runs and the
# second A/B is incremental.
AB_WORKTREE="${GATE_AB_WORKTREE:-$(dirname "$REPO")/tsdb-ab-worktree}"
AB_BUILD="${GATE_AB_BUILD_DIR:-$(dirname "$REPO")/tsdb-ab-build}"
GATE_BUILD_JOBS="${GATE_BUILD_JOBS:-2}"
case "$GATE_BUILD_JOBS" in
    ''|*[!0-9]*|0) echo "ABORT: GATE_BUILD_JOBS must be a positive integer" >&2; exit 2 ;;
esac

# The two files the pacing patch touches, named once. See the header for why this is a
# patch and not a `git checkout` of the pre-4a versions.
REVERT_FILES="lib/cluster/data/replicated_write_router.hpp
lib/cluster/data/write_errors.hpp"

# patch_anchor FILE OLD NEW -- exact, unique, and it REFUSES rather than half-applying.
# A sed that silently matches nothing would build a comparison binary identical to HEAD,
# which is the one failure mode this whole script cannot survive: every assertion below
# would then compare a thing to itself.
patch_anchor() {
    python3 - "$AB_WORKTREE/$1" "$2" "$3" <<'PY'
import sys
path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    s = f.read()
n = s.count(old)
if n != 1:
    sys.exit("anchor matched %d times (need exactly 1) in %s:\n%s" % (n, path, old))
with open(path, "w") as f:
    f.write(s.replace(old, new))
PY
}

[ -x "$BUILD_DIR/bin/timestar_http_server" ] || { echo "no HEAD server binary at $BUILD_DIR/bin"; exit 2; }

# These gates are disk-hungry and the failure mode is SELF-AMPLIFYING (see the README):
# less headroom -> slower bench -> the 0.3 s resetter fires more rounds -> slower still,
# and a space problem reads as a regression. Two full storm runs need the room up front.
# 40 G, not the 30 G this asked for before. The A/B runs the gate at 2000 batches / K=2 =
# THREE benches against one cluster, whose measured peak is ~34 G -- so a 30 G floor
# admitted a run that could not fit, and 34 G is the exact point at which the K=3 sizing was
# killed for filling the tmpfs. The floor must exceed the peak, not approach it.
require_gate_space_gb 40 "fault-injection A/B" || exit 2

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

# EDIT 1: the geometric back-off becomes flat -- the pre-4a pause, for the two classes
# whose cure is waiting for something to come back.
patch_anchor lib/cluster/data/write_errors.hpp \
'            int64_t ms = kWriteRetryDelayBase.count();
            for (unsigned i = 1; i < attempt && ms < kWriteRetryDelayMax.count(); ++i)
                ms *= 2;' \
'            int64_t ms = kWriteRetryDelayBase.count();  // A/B: flat, i.e. pre-4a
            (void)attempt;' || { echo "ABORT: the pacing anchor has moved; see the header"; exit 2; }

# EDIT 2: the compiler fence that makes edit 1 illegal. It is not collateral -- that assert
# IS the fix's guarantee ("the write retry schedule must outlast the transport reconnect
# window"), so turning it off is the exact inverse of 4a and nothing more.
patch_anchor lib/cluster/data/replicated_write_router.hpp \
'    static_assert(worstCaseWriteRetrySpan(WriteFailure::Transport, kMaxAttempts) > cluster::worstCaseReconnectBackoff(),' \
'    static_assert(true,  // A/B: the fence that forbids the flat schedule, deliberately off' \
    || { echo "ABORT: the coupling static_assert has moved; see the header"; exit 2; }

# ANTI-VACUITY, and the one that actually bites: if the patch is a no-op -- files moved, the
# pacing re-expressed elsewhere -- the two binaries are IDENTICAL and every assertion below
# compares a thing to itself. The reverted run would then produce zero errors and this
# script would "fail to prove discrimination" for a reason that has nothing to do with the
# server. Catch it here, where the message is honest. (patch_anchor already refuses a
# missing anchor; this catches the opposite -- a patch that touched more than it should.)
CHANGED_LIST=$(git -C "$AB_WORKTREE" diff --name-only HEAD -- | sort)
CHANGED=$(printf '%s\n' "$CHANGED_LIST" | grep -c .)
EXPECTED_LIST=$(printf '%s\n' $REVERT_FILES | sort)
echo "  patched $CHANGED file(s) back to the pre-4a flat retry pacing"
if [ "$CHANGED_LIST" != "$EXPECTED_LIST" ]; then
    echo "ABORT: the pacing patch did not touch exactly the two expected files."
    echo "       expected:"; printf '         %s\n' $EXPECTED_LIST
    echo "       got:"; printf '%s\n' "$CHANGED_LIST" | sed 's/^/         /'
    exit 2
fi
# And it must be SMALL: two anchors, five lines. A patch that grew is a patch that is no
# longer "4a's pacing and nothing else".
PATCH_LINES=$(git -C "$AB_WORKTREE" diff --numstat HEAD -- | awk '{a+=$1; d+=$2} END{print a+d}')
[ "${PATCH_LINES:-99}" -le 12 ] || {
    echo "ABORT: the pacing patch changed $PATCH_LINES lines; it should change ~5."; exit 2; }

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
( cd "$AB_BUILD" && cmake "$AB_WORKTREE" >"$GATE_TMP_ROOT/tsgate_ab_cmake.log" 2>&1 ) || {
    echo "ABORT: cmake failed; see $GATE_TMP_ROOT/tsgate_ab_cmake.log"; tail -20 "$GATE_TMP_ROOT/tsgate_ab_cmake.log"; exit 2; }
echo "  compiling (this is the slow part; first run builds seastar too)"
( cd "$AB_BUILD" && make timestar_http_server -j"$GATE_BUILD_JOBS" >"$GATE_TMP_ROOT/tsgate_ab_build.log" 2>&1 ) || {
    echo "ABORT: build failed; see $GATE_TMP_ROOT/tsgate_ab_build.log"; tail -30 "$GATE_TMP_ROOT/tsgate_ab_build.log"; exit 2; }
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

REVERTED_LOG="$GATE_TMP_ROOT/tsgate_ab_reverted.log"
HEAD_LOG="$GATE_TMP_ROOT/tsgate_ab_head.log"

# ONE AT A TIME, sequentially, each with the fresh dirs the gate makes for itself. Running
# them concurrently would have them fight over ports, data dirs and disk -- and the disk
# part is what the README's self-amplifying failure is about.
run_gate "$AB_BIN" "$REVERTED_LOG"
REVERTED_RC=$?
run_gate "$BUILD_DIR/bin/timestar_http_server" "$HEAD_LOG"
HEAD_RC=$?

# ---------------------------------------------------------------------------
# The error counter is now an AGGREGATE over the gate's K storms (debt D-21); the gate
# prints the per-storm vector next to it, which is what to read when the two arms are
# closer than expected.
R_ROUNDS=$(gate_number "$REVERTED_LOG" reset_rounds_total)
R_CONNS=$(gate_number "$REVERTED_LOG" reset_conns_total)
R_TOTAL=$(gate_number "$REVERTED_LOG" storm_errors_total)
H_ROUNDS=$(gate_number "$HEAD_LOG" reset_rounds_total)
H_CONNS=$(gate_number "$HEAD_LOG" reset_conns_total)
H_TOTAL=$(gate_number "$HEAD_LOG" storm_errors_total)
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
grep -h "storms: errors\[" "$REVERTED_LOG" | sed 's/^/    reverted /'
grep -h "storms: errors\[" "$HEAD_LOG" | sed 's/^/    HEAD     /'

# A VOID run (exit 3) is neither arm's answer: the gate's own fault-free control failed, so
# the storm that followed measured the environment. Re-draw rather than reporting an A/B
# that did not happen -- and say which arm voided, because the reverted arm voiding looks
# superficially like the discrimination this script exists to prove.
if [ "$REVERTED_RC" -eq 3 ] || [ "$HEAD_RC" -eq 3 ]; then
    echo "ABORT (VOID): the $([ "$REVERTED_RC" -eq 3 ] && echo REVERTED || echo HEAD) arm voided --"
    echo "  its fault-free baseline through the proxy failed, so neither arm's storm is"
    echo "  comparable. Re-run; check $GATE_TMP_ROOT free space first (see the gate README)."
    exit 3
fi

# BOTH RUNS MUST HAVE TAKEN A COMPARABLE STORM. Comparing a heavy storm against a light
# one says nothing about the binaries, and the resetter's round count is load-dependent
# (it fires on a wall clock while the bench length is not). The gate's own floors are the
# lower bound; here we additionally require the two runs to be within 2x of each other.
# THE FLOORS SCALE WITH K, because these are RUN TOTALS. They were flat 70/180 -- the
# gate's old PER-STORM numbers -- so at K=2 they demanded less than one storm's worth of
# resets across two, and a run in which the second storm never fired would have passed.
# The gate's own per-storm floors are 35/100 (see its BENCH_BATCHES note); at the A/B's
# 2000 batches a storm injects ~140 rounds / ~390 connections, so these stay conservative.
AB_MIN_ROUNDS=$(( ${GATE_MIN_RESET_ROUNDS:-35} * GATE_STORM_ROUNDS ))
AB_MIN_CONNS=$(( ${GATE_MIN_RESET_CONNS:-100} * GATE_STORM_ROUNDS ))
assert_ge "reverted run: reset rounds (K=$GATE_STORM_ROUNDS)"  "${R_ROUNDS:-0}" "$AB_MIN_ROUNDS"
assert_ge "reverted run: connections destroyed" "${R_CONNS:-0}" "$AB_MIN_CONNS"
assert_ge "HEAD run: reset rounds (K=$GATE_STORM_ROUNDS)"      "${H_ROUNDS:-0}" "$AB_MIN_ROUNDS"
assert_ge "HEAD run: connections destroyed" "${H_CONNS:-0}" "$AB_MIN_CONNS"
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
# where HEAD's whole K-storm budget is 6.
# THE CLAIM IS A WITHIN-RUN RATIO, NOT TWO ABSOLUTE THRESHOLDS, and getting there took two
# corrections in a row -- which is itself the argument for the shape.
#
#   * It started as `REVERTED >= 3 * GATE_MAX_STORM_ERRORS` = 9, from a hand-run session's
#     "9-10 in a SINGLE storm": a figure taken at 2000 batches AND under the old whole-file
#     checkout. The first real run measured REVERTED 7, so 9 would have failed the A/B on a
#     correct result.
#   * Replacing it with an absolute 5 fixed that draw and broke the next one: the HEAD arm
#     drew [0 4 0] = 4 against the gate's then-budget of 3 (now 6), so the A/B reported a failure
#     about a binary it was not testing.
#
# Both mistakes are the same mistake: an absolute threshold on a heavy-tailed count, set
# from a handful of runs. The two arms of ONE A/B run share a box, a disk, a proxy and a
# storm within 3% of each other (the intensity ratio is asserted above), so the RATIO
# between them cancels exactly the variance the absolutes kept tripping over. Measured:
#
#     draw   HEAD   REVERTED   ratio
#     1      0      7          >= floor (HEAD 0 -> the min-1 rule applies)
#     2      4      22         5.5x
#
# The factor is 3 with a floor of 3, so a HEAD of 0 still demands a reverted arm that
# produced real errors, and a noisy HEAD raises the bar instead of failing the run.
BUDGET="${GATE_MAX_STORM_ERRORS:-6}"
SEP_FACTOR="${GATE_AB_SEPARATION_FACTOR:-3}"
# ...AND IT MUST ALSO CLEAR HEAD'S OWN NOISE FLOOR. `3 * max(H,1)` alone is vacuous when
# both arms draw near zero: HEAD has drawn as many as 4 unaided, so a run of HEAD 0 against
# REVERTED 3 would "separate" on numbers that are both inside the good binary's ordinary
# spread. The floor is therefore the LARGER of the ratio and one past the gate's own budget,
# so the reverted arm must land somewhere HEAD is not merely unlikely to go but is asserted
# not to go.
SEP_RATIO=$(( (H_TOTAL > 1 ? H_TOTAL : 1) * SEP_FACTOR ))
SEP_FLOOR=$(( SEP_RATIO > BUDGET + 1 ? SEP_RATIO : BUDGET + 1 ))
assert_ge "SEPARATION: reverted errors ($R_TOTAL) vs max(${SEP_FACTOR}x HEAD's $H_TOTAL, budget+1)" "$R_TOTAL" "$SEP_FLOOR"
# AN OVERLAP DRAW FAILS RED, AND THAT IS THE INTENDED OUTCOME rather than a defect in the
# script: measured at the gate's own lighter sizing, one draw in three came out HEAD 2
# against REVERTED 4 and no honest threshold separates those. A red A/B means RE-DRAW (and,
# if it repeats, the storm is not intense enough to discriminate on this box) -- it does not
# on its own mean the pacing fix has regressed.
# ADVISORY, deliberately. HEAD's own absolute budget is `fault_injection_gate.sh`'s business,
# and an unlucky HEAD draw says nothing about whether this script discriminated -- which is
# the only question it exists to answer. Reported so a run that is drifting is visible.
if [ "${H_TOTAL:-0}" -le "$BUDGET" ]; then
    gate_ok "HEAD binary: client errors under the storms = $H_TOTAL (within its own budget $BUDGET)"
else
    echo "  (advisory) HEAD drew $H_TOTAL errors, over fault_injection_gate.sh's budget of $BUDGET --"
    echo "             re-draw that gate on its own before reading anything into it; the"
    echo "             separation above is what this script asserts."
fi

# ...AND THE ERRORS MUST BE THE RIGHT ONES. The whole-file revert reaches past 4a (see the
# header), so "it produced errors" alone does not prove the pacing fix is what the gate
# measures. [D6] has a verbatim signature -- the retry exhausting its budget against a
# socket the transport had not re-dialled yet -- and this is what pins the mechanism.
# THE PATTERN WAS WRONG AND HAD NEVER MATCHED ANYTHING. It looked for
# `RetryableWriteError.*last: transport`, but the exception's TYPE NAME is not in the
# message the client sees -- the body reads "ReplicatedBatchWriteRouter: N VShard slice(s)
# uncommitted after 6 attempt(s) (last: transport); retry the write". So this check could
# only ever have reported "NOT the [D6] signature", on every arm, forever. Executing the
# script end to end (D-19) is what surfaced it.
#
# THE SIGNATURE PINS A CLASS BOTH BINARIES CAN REACH -- it is NOT the discriminator, and
# saying otherwise was this script's last wrong evidence claim. Measured on one run's
# retained logs: the REVERTED arm carried 6 x `uncommitted after 6 attempt(s)
# (last: transport)` and the HEAD arm carried 2 x THE SAME STRING, at the same attempt
# count. HEAD reaches the transport class too; it just reaches it far less often. So the
# check below is INFORMATIONAL on both arms, and the discrimination is the COUNT separation
# asserted above -- which is why that assertion is the one that fails the script.
#
# It is still worth printing: a reverted arm failing with some OTHER class would mean the
# patch is not exercising the pacing at all, and both counts side by side make the
# "same class, different rate" shape visible instead of implied.
D6_SIGNATURE='uncommitted after [0-9]+ attempt\(s\) \(last: transport\)'
R_SIG=$(grep -cE "$D6_SIGNATURE" "$REVERTED_LOG")
H_SIG=$(grep -cE "$D6_SIGNATURE" "$HEAD_LOG")
echo "  [D6] class ([last: transport] give-ups logged): REVERTED $R_SIG, HEAD $H_SIG"
if [ "$R_SIG" -gt 0 ]; then
    gate_ok "REVERTED binary: failures carry the [D6] class (informational; HEAD carries it $H_SIG time(s) too)"
else
    gate_fail "REVERTED binary failed, but NOT with the [D6] class -- the patch may not be \
exercising the retry pacing at all. First error line: \
$(grep -m1 'First error' "$REVERTED_LOG")"
fi
# Also advisory, and for the same reason as the budget above: the HEAD arm's exit code is
# just its budget check restated, so asserting it here would re-import the absolute
# threshold this script deliberately does not use. (VOID -- exit 3 -- is different and is
# handled earlier: it aborts the comparison outright.)
[ "$HEAD_RC" -eq 0 ] && gate_ok "HEAD binary: gate exit code = 0" ||
    echo "  (advisory) HEAD arm exited $HEAD_RC; see the budget note above"
if [ "$REVERTED_RC" -eq 0 ]; then
    gate_fail "the REVERTED binary PASSED the gate -- the gate does not discriminate"
else
    gate_ok "the reverted binary failed the gate, as it must"
fi

echo "  full logs: $REVERTED_LOG $HEAD_LOG"
gate_exit
