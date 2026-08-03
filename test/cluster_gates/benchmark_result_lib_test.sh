#!/bin/bash
set -eu
cd "$(dirname "$0")" || exit 2
. ./benchmark_result_lib.sh

TEST_ROOT="${GATE_TMP_ROOT:-../../build/tmp}/tsgate_benchmark_result_test.$$"
case "$TEST_ROOT" in
    */tsgate_benchmark_result_test.*) ;;
    *) echo "refusing unsafe test root: $TEST_ROOT" >&2; exit 2 ;;
esac
mkdir -p -- "$TEST_ROOT"
cleanup() {
    case "$TEST_ROOT" in
        */tsgate_benchmark_result_test.*) rm -rf -- "$TEST_ROOT" ;;
    esac
}
trap cleanup EXIT

run_bounded_command 2 1 bash -c 'exit 0'
BOUND_START=$SECONDS
if run_bounded_command 1 1 bash -c 'trap "" TERM; while :; do :; done'; then
    echo "bounded command accepted a TERM-resistant child as successful" >&2
    exit 1
fi
BOUND_ELAPSED=$((SECONDS - BOUND_START))
[ "$BOUND_ELAPSED" -le 4 ] || {
    echo "bounded command took ${BOUND_ELAPSED}s; hard-kill grace was not enforced" >&2
    exit 1
}

VALID="$TEST_ROOT/valid.log"
printf '%s\n' \
    'Requests:       500 OK, 7 HTTP errors, 2 connection failures' \
    'Throughput:     12345 pts/sec' \
    'batch latency   p99=12.0' >"$VALID"
parse_benchmark_result "$VALID"
[ "$BENCHMARK_RESULT_HTTP_ERRORS" = "7" ]
[ "$BENCHMARK_RESULT_CONNECTION_FAILURES" = "2" ]
[ "$BENCHMARK_RESULT_THROUGHPUT" = "12345" ]

MALFORMED="$TEST_ROOT/malformed.log"
printf '%s\n' 'fatal: benchmark failed before producing a summary' >"$MALFORMED"
if parse_benchmark_result "$MALFORMED"; then
    echo "accepted a benchmark transcript without its required summary" >&2
    exit 1
fi

EVIDENCE="$TEST_ROOT/evidence.log"
record_benchmark_failure baseline 9 "$MALFORMED" "$EVIDENCE"
rm -f -- "$MALFORMED"
grep -Fqx 'benchmark: baseline' "$EVIDENCE"
grep -Fqx 'exit_code: 9' "$EVIDENCE"
grep -Fqx 'fatal: benchmark failed before producing a summary' "$EVIDENCE"

MISSING_EVIDENCE="$TEST_ROOT/missing-evidence.log"
record_benchmark_failure storm-2 124 "$TEST_ROOT/absent.log" "$MISSING_EVIDENCE"
grep -Fqx 'benchmark: storm-2' "$MISSING_EVIDENCE"
grep -Fqx 'exit_code: 124' "$MISSING_EVIDENCE"
grep -Fq '<transcript missing:' "$MISSING_EVIDENCE"

echo "benchmark result helper tests passed"
