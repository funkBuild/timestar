#!/bin/bash
# Deterministic counterfactual for write-scaleout 4a retry pacing.
#
# The production reset gate remains a live, durable-disk smoke/availability
# gate. It is intentionally tolerant of bounded retryable 503s, so its noisy
# error count is not a reliable discriminator: one measured sequential A/B
# gave the flat-pacing binary 0 errors and HEAD 6 under 1%-matched reset
# intensity. This script instead applies exactly the two-line 4a inverse in an
# isolated worktree and runs the two focused tests that own the invariant:
#
#   * the full retry schedule must span multiple reconnect windows;
#   * a wall-clock blip longer than one reconnect backoff is absorbed.
#
# HEAD must pass both and the reverted binary must fail both. The comparison
# build and compiler scratch stay on disk, never /tmp.
#
# Usage: fault_injection_ab.sh
#   GATE_AB_BUILD_DIR=<dir>   reusable comparison build beside the repository
#   GATE_AB_WORKTREE=<dir>    disposable detached worktree beside the repository
#   GATE_BUILD_JOBS=<n>       comparison-build concurrency (default 2)
#   GATE_AB_KEEP=1            keep the patched worktree after the run
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh
. ./benchmark_result_lib.sh

REPO="$(git rev-parse --show-toplevel)"
AB_WORKTREE=$(realpath -m -- "${GATE_AB_WORKTREE:-$(dirname "$REPO")/tsdb-ab-worktree}")
AB_BUILD=$(realpath -m -- "${GATE_AB_BUILD_DIR:-$(dirname "$REPO")/tsdb-ab-build}")
GATE_BUILD_JOBS="${GATE_BUILD_JOBS:-2}"
HEAD_TEST="$BUILD_DIR/test/timestar_unit_test"
FILTER='WriteRetryPacingTest.TransportScheduleOutlastsTheReconnectBackoff:ReplicatedBatchWriteRouterTest.BlipLongerThanTheReconnectBackoffIsAbsorbed'
REVERTED_LOG="$GATE_TMP_ROOT/tsgate_ab_reverted.log"
HEAD_LOG="$GATE_TMP_ROOT/tsgate_ab_head.log"

case "$GATE_BUILD_JOBS" in
    ''|*[!0-9]*|0) echo "ABORT: GATE_BUILD_JOBS must be a positive integer" >&2; exit 2 ;;
esac
case "$AB_WORKTREE:$AB_BUILD" in
    /tmp/*|*:/tmp/*)
        echo "ABORT: A/B worktree/build must be resolved disk paths outside /tmp" >&2
        exit 2
        ;;
esac
case "$AB_WORKTREE:$AB_BUILD" in
    "$REPO":*|"$REPO"/*:*|*:"$REPO"|*:"$REPO"/*)
        echo "ABORT: A/B worktree/build must be outside the candidate repository" >&2
        exit 2
        ;;
esac
[ -x "$HEAD_TEST" ] || { echo "ABORT: no HEAD unit binary at $HEAD_TEST" >&2; exit 2; }
require_gate_space_gb 10 "retry-pacing A/B build" || exit 2

DIRTY=$(git -C "$REPO" status --porcelain --untracked-files=no --ignore-submodules=all) || {
    echo "ABORT: could not verify the candidate worktree" >&2; exit 2; }
[ -z "$DIRTY" ] || {
    echo "ABORT: working tree is dirty; the two arms would differ by more than the pacing revert" >&2
    printf '%s\n' "$DIRTY" | sed 's/^/  /' >&2
    exit 2
}

cleanup_ab() {
    if [ "${GATE_AB_KEEP:-0}" != 1 ]; then
        git -C "$REPO" worktree remove --force "$AB_WORKTREE" 2>/dev/null
        case "$AB_WORKTREE" in
            "$(dirname "$REPO")"/tsdb-ab-worktree) rm -rf -- "$AB_WORKTREE" ;;
        esac
        git -C "$REPO" worktree prune 2>/dev/null
    fi
}
trap cleanup_ab EXIT

patch_anchor() { # FILE OLD NEW
    python3 - "$AB_WORKTREE/$1" "$2" "$3" <<'PY'
import sys
path, old, new = sys.argv[1:]
with open(path, encoding="utf-8") as src:
    text = src.read()
count = text.count(old)
if count != 1:
    sys.exit("anchor matched %d times (need exactly 1) in %s" % (count, path))
with open(path, "w", encoding="utf-8") as dst:
    dst.write(text.replace(old, new))
PY
}

echo "=== build the exact two-anchor flat-pacing counterfactual ==="
git -C "$REPO" worktree remove --force "$AB_WORKTREE" 2>/dev/null
case "$AB_WORKTREE" in
    "$(dirname "$REPO")"/tsdb-ab-worktree) rm -rf -- "$AB_WORKTREE" ;;
esac
git -C "$REPO" worktree add --detach "$AB_WORKTREE" HEAD >/dev/null 2>&1 || {
    echo "ABORT: could not create comparison worktree at $AB_WORKTREE" >&2; exit 2; }

patch_anchor lib/cluster/data/write_errors.hpp \
'            int64_t ms = kWriteRetryDelayBase.count();
            for (unsigned i = 1; i < attempt && ms < kWriteRetryDelayMax.count(); ++i)
                ms *= 2;' \
'            int64_t ms = kWriteRetryDelayBase.count();  // A/B: flat, i.e. pre-4a
            (void)attempt;' || { echo "ABORT: retry-pacing anchor moved" >&2; exit 2; }
patch_anchor lib/cluster/data/replicated_write_router.hpp \
'    static_assert(worstCaseWriteRetrySpan(WriteFailure::Transport, kMaxAttempts) > cluster::worstCaseReconnectBackoff(),' \
'    static_assert(true,  // A/B: the fence forbidding the flat schedule is deliberately off' \
    || { echo "ABORT: retry-pacing compiler fence moved" >&2; exit 2; }

CHANGED=$(git -C "$AB_WORKTREE" diff --name-only HEAD -- | sort)
EXPECTED=$(printf '%s\n' lib/cluster/data/replicated_write_router.hpp lib/cluster/data/write_errors.hpp | sort)
[ "$CHANGED" = "$EXPECTED" ] || {
    echo "ABORT: counterfactual did not touch exactly the two pacing files" >&2
    printf '%s\n' "$CHANGED" | sed 's/^/  /' >&2
    exit 2
}

if [ ! -e "$AB_WORKTREE/external/seastar/CMakeLists.txt" ]; then
    [ -e "$REPO/external/seastar/CMakeLists.txt" ] || {
        echo "ABORT: populated Seastar source is unavailable" >&2; exit 2; }
    rmdir "$AB_WORKTREE/external/seastar" 2>/dev/null
    ln -sfn "$REPO/external/seastar" "$AB_WORKTREE/external/seastar" || exit 2
fi

mkdir -p "$AB_BUILD"
cmake -S "$AB_WORKTREE" -B "$AB_BUILD" >"$GATE_TMP_ROOT/tsgate_ab_cmake.log" 2>&1 || {
    tail -30 "$GATE_TMP_ROOT/tsgate_ab_cmake.log" >&2; exit 2; }
cmake --build "$AB_BUILD" --target timestar_unit_test -j"$GATE_BUILD_JOBS" \
    >"$GATE_TMP_ROOT/tsgate_ab_build.log" 2>&1 || {
    tail -40 "$GATE_TMP_ROOT/tsgate_ab_build.log" >&2; exit 2; }
REVERTED_TEST="$AB_BUILD/test/timestar_unit_test"
[ -x "$REVERTED_TEST" ] || { echo "ABORT: comparison unit binary is missing" >&2; exit 2; }

echo "=== run the same focused tests against reverted pacing and HEAD ==="
REVERTED_RC=0
run_bounded_command 120 10 "$REVERTED_TEST" --smp 1 --memory 1G "--gtest_filter=$FILTER" \
    >"$REVERTED_LOG" 2>&1 || REVERTED_RC=$?
HEAD_RC=0
run_bounded_command 120 10 "$HEAD_TEST" --smp 1 --memory 1G "--gtest_filter=$FILTER" \
    >"$HEAD_LOG" 2>&1 || HEAD_RC=$?

[ "$REVERTED_RC" -ne 0 ] \
    && gate_ok "flat-pacing counterfactual failed the focused suite (exit $REVERTED_RC)" \
    || gate_fail "flat-pacing counterfactual unexpectedly passed the focused suite"
for test_name in \
    WriteRetryPacingTest.TransportScheduleOutlastsTheReconnectBackoff \
    ReplicatedBatchWriteRouterTest.BlipLongerThanTheReconnectBackoffIsAbsorbed; do
    grep -Fq "[  FAILED  ] $test_name" "$REVERTED_LOG" \
        && gate_ok "counterfactual fails $test_name" \
        || gate_fail "counterfactual did not fail $test_name"
done
assert_eq "HEAD focused retry-pacing suite exit status" "$HEAD_RC" 0
grep -Fq '[  PASSED  ] 2 tests.' "$HEAD_LOG" \
    && gate_ok "HEAD passes both retry-pacing invariants" \
    || gate_fail "HEAD did not report both focused tests passed"

echo "  logs: $REVERTED_LOG $HEAD_LOG"
gate_exit
