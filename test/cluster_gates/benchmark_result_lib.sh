#!/bin/bash
# Helpers for live gates that consume timestar_insert_bench transcripts.
# Source this file; do not execute it directly.

# Bound both command execution and graceful shutdown. GNU timeout otherwise
# waits indefinitely when a Seastar process accepts TERM but cannot finish its
# outstanding requests.
run_bounded_command() { # TIMEOUT_SECONDS KILL_AFTER_SECONDS COMMAND...
    local timeout_seconds="$1" kill_after_seconds="$2"
    shift 2
    timeout --signal=TERM --kill-after="${kill_after_seconds}s" "${timeout_seconds}s" "$@"
}

# Parse the summary emitted by timestar_insert_bench. Results are returned in
# BENCHMARK_RESULT_HTTP_ERRORS, BENCHMARK_RESULT_CONNECTION_FAILURES, and
# BENCHMARK_RESULT_THROUGHPUT. Connection failures are optional because older
# focused drivers did not print that counter, but the two release-gating fields
# must both be present or the transcript is not a usable measurement.
parse_benchmark_result() { # TRANSCRIPT
    local transcript="$1"
    BENCHMARK_RESULT_HTTP_ERRORS=""
    BENCHMARK_RESULT_CONNECTION_FAILURES=""
    BENCHMARK_RESULT_THROUGHPUT=""

    [ -r "$transcript" ] || return 1
    BENCHMARK_RESULT_HTTP_ERRORS=$(grep -o '[0-9]* HTTP errors' "$transcript" | head -1 | cut -d' ' -f1)
    BENCHMARK_RESULT_CONNECTION_FAILURES=$(grep -o '[0-9]* connection failures' "$transcript" |
        head -1 | cut -d' ' -f1)
    BENCHMARK_RESULT_THROUGHPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' "$transcript" |
        head -1 | grep -oE '[0-9.]+')

    [ -n "$BENCHMARK_RESULT_HTTP_ERRORS" ] && [ -n "$BENCHMARK_RESULT_THROUGHPUT" ]
}

# Preserve a failed benchmark independently of the gate's disposable data
# roots. The destination is published by rename only after the label, status,
# and complete transcript have been written.
record_benchmark_failure() { # LABEL EXIT_CODE TRANSCRIPT DESTINATION
    local label="$1" exit_code="$2" transcript="$3" destination="$4"
    local temporary="${destination}.tmp.$$"

    {
        printf 'benchmark: %s\n' "$label"
        printf 'exit_code: %s\n' "$exit_code"
        printf '%s\n' '--- transcript ---'
        if [ -r "$transcript" ]; then
            cat -- "$transcript"
        else
            printf '<transcript missing: %s>\n' "$transcript"
        fi
    } >"$temporary" || return 1
    mv -f -- "$temporary" "$destination"
}
