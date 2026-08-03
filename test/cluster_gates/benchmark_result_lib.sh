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
# BENCHMARK_RESULT_HTTP_ERRORS, BENCHMARK_RESULT_HTTP_503,
# BENCHMARK_RESULT_HTTP_OTHER, BENCHMARK_RESULT_CONNECTION_FAILURES, and
# BENCHMARK_RESULT_THROUGHPUT. The status histogram must account for every HTTP
# error so a release gate cannot accept an opaque/non-retryable response inside
# a numeric error budget.
parse_benchmark_result() { # TRANSCRIPT
    local transcript="$1"
    local statuses entry status count status_total=0
    BENCHMARK_RESULT_HTTP_ERRORS=""
    BENCHMARK_RESULT_HTTP_503=0
    BENCHMARK_RESULT_HTTP_OTHER=0
    BENCHMARK_RESULT_CONNECTION_FAILURES=""
    BENCHMARK_RESULT_THROUGHPUT=""

    [ -r "$transcript" ] || return 1
    BENCHMARK_RESULT_HTTP_ERRORS=$(grep -o '[0-9]* HTTP errors' "$transcript" | head -1 | cut -d' ' -f1)
    BENCHMARK_RESULT_CONNECTION_FAILURES=$(grep -o '[0-9]* connection failures' "$transcript" |
        head -1 | cut -d' ' -f1)
    BENCHMARK_RESULT_THROUGHPUT=$(grep -oE 'Throughput:[[:space:]]*[0-9.]+' "$transcript" |
        head -1 | grep -oE '[0-9.]+')

    [ -n "$BENCHMARK_RESULT_HTTP_ERRORS" ] && [ -n "$BENCHMARK_RESULT_THROUGHPUT" ] || return 1
    statuses=$(sed -n 's/^[[:space:]]*HTTP errors by status:[[:space:]]*//p' "$transcript" | head -1)
    [ -n "$statuses" ] || return 1
    if [ "$statuses" != none ]; then
        for entry in $statuses; do
            status=${entry%%=*}
            count=${entry#*=}
            case "$status" in ''|*[!0-9]*) return 1 ;; esac
            case "$count" in ''|*[!0-9]*) return 1 ;; esac
            status_total=$((status_total + count))
            if [ "$status" = 503 ]; then
                BENCHMARK_RESULT_HTTP_503=$((BENCHMARK_RESULT_HTTP_503 + count))
            else
                BENCHMARK_RESULT_HTTP_OTHER=$((BENCHMARK_RESULT_HTTP_OTHER + count))
            fi
        done
    fi
    [ "$status_total" -eq "$BENCHMARK_RESULT_HTTP_ERRORS" ]
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
