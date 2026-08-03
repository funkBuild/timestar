#!/bin/bash
# Focused tests for release-candidate executable authentication. These use
# tiny fake executables under repository-local scratch; no server is started.
set -u
cd "$(dirname "$0")" || exit 2
. ./cluster_gate_lib.sh

REPO=$(git rev-parse --show-toplevel) || exit 2
HEAD_COMMIT=$(git -C "$REPO" rev-parse HEAD) || exit 2
CASE_DIR=$(mktemp -d "$GATE_TMP_ROOT/tsgate_candidate_identity.XXXXXX") || exit 2
FAILURES=0

cleanup() {
    case "$CASE_DIR" in
        "$GATE_TMP_ROOT"/tsgate_candidate_identity.*) rm -rf -- "$CASE_DIR" ;;
        *) echo "refusing to clean unexpected test path: $CASE_DIR" >&2 ;;
    esac
}
trap cleanup EXIT

write_version_fixture() { # PATH REVISION COMPONENT [EXIT_CODE]
    local path="$1" revision="$2" component="$3" exit_code="${4:-0}"
    printf '%s\n' \
        '#!/bin/sh' \
        "printf '%s\\n' 'TimeStar 1.3.0 ($revision)' 'Component: $component' 'Built: test' 'Compiler: test'" \
        "exit $exit_code" >"$path"
    chmod 700 "$path"
}

expect_rejected() { # LABEL PATH COMPONENT
    local label="$1" path="$2" component="$3"
    if verify_candidate_binary "$REPO" "$HEAD_COMMIT" "$path" "$component" >/dev/null 2>&1; then
        echo "FAIL: accepted $label" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok: rejected $label"
    fi
}

GOOD="$CASE_DIR/good"
write_version_fixture "$GOOD" "$HEAD_COMMIT" timestar_insert_bench
if verify_candidate_binary "$REPO" "$HEAD_COMMIT" "$GOOD" timestar_insert_bench; then
    EXPECTED_SHA=$(sha256sum -- "$GOOD" | awk '{print $1}')
    [ "$VERIFIED_BINARY_REVISION" = "$HEAD_COMMIT" ] || {
        echo "FAIL: returned the wrong revision" >&2; FAILURES=$((FAILURES + 1)); }
    [ "$VERIFIED_BINARY_SHA256" = "$EXPECTED_SHA" ] || {
        echo "FAIL: returned the wrong digest" >&2; FAILURES=$((FAILURES + 1)); }
    echo "ok: authenticated exact candidate and digest"
else
    echo "FAIL: rejected exact candidate" >&2
    FAILURES=$((FAILURES + 1))
    EXPECTED_SHA=""
fi

UNKNOWN="$CASE_DIR/unknown"
write_version_fixture "$UNKNOWN" unknown timestar_insert_bench
expect_rejected "unknown revision" "$UNKNOWN" timestar_insert_bench

DIRTY="$CASE_DIR/dirty"
write_version_fixture "$DIRTY" "${HEAD_COMMIT}-dirty" timestar_insert_bench
expect_rejected "dirty revision" "$DIRTY" timestar_insert_bench

WRONG_COMPONENT="$CASE_DIR/wrong-component"
write_version_fixture "$WRONG_COMPONENT" "$HEAD_COMMIT" timestar_http_server
expect_rejected "wrong component" "$WRONG_COMPONENT" timestar_insert_bench

MALFORMED="$CASE_DIR/malformed"
printf '%s\n' '#!/bin/sh' "printf '%s\\n' 'not a version' 'Component: timestar_insert_bench'" >"$MALFORMED"
chmod 700 "$MALFORMED"
expect_rejected "malformed version output" "$MALFORMED" timestar_insert_bench

NONZERO="$CASE_DIR/nonzero"
write_version_fixture "$NONZERO" "$HEAD_COMMIT" timestar_insert_bench 9
expect_rejected "non-zero version command" "$NONZERO" timestar_insert_bench

STALE_COMMIT=$(git -C "$REPO" rev-parse HEAD^ 2>/dev/null || true)
if [ -n "$STALE_COMMIT" ]; then
    STALE="$CASE_DIR/stale"
    write_version_fixture "$STALE" "$STALE_COMMIT" timestar_insert_bench
    expect_rejected "stale resolvable revision" "$STALE" timestar_insert_bench
fi

if [ -n "$EXPECTED_SHA" ]; then
    printf '%s\n' '# changed after preflight' >>"$GOOD"
    if verify_candidate_binary_unchanged "$GOOD" "$EXPECTED_SHA" timestar_insert_bench >/dev/null 2>&1; then
        echo "FAIL: accepted a changed binary" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok: rejected binary changed during qualification"
    fi
fi

if [ "$FAILURES" -ne 0 ]; then
    echo "candidate identity tests failed: $FAILURES" >&2
    exit 1
fi
echo "candidate identity tests passed"
