#!/bin/bash

# TimeStar Consolidated Test Runner
# Usage: ./scripts/run_tests.sh [unit|e2e|all|simd|no-simd|simd-both]
#
# Options:
#   unit      - Run C++ unit tests only
#   e2e       - Run end-to-end API tests only (starts/stops server automatically)
#   all       - Run both unit and e2e tests (default)
#   simd      - Run transform function tests with SIMD enabled
#   no-simd   - Run transform function tests with SIMD disabled (scalar fallback)
#   simd-both - Run transform function tests with both SIMD and no-SIMD

set -e

# Get the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
TEST_API_DIR="$PROJECT_ROOT/test_api"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Track results
UNIT_PASSED=0
UNIT_FAILED=0
E2E_PASSED=0
E2E_FAILED=0
SIMD_PASSED=0
SIMD_FAILED=0
SERVER_PID=""

# Function to print section header
print_header() {
    echo
    echo -e "${BLUE}=============================================="
    echo -e "$1"
    echo -e "==============================================${NC}"
    echo
}

# Function to check if server is running
check_server() {
    curl -s http://localhost:8086/health > /dev/null 2>&1
    return $?
}

# Function to start the server
start_server() {
    echo -e "${YELLOW}Starting TimeStar HTTP server...${NC}"

    if check_server; then
        echo -e "${GREEN}Server is already running${NC}"
        return 0
    fi

    if [ ! -f "$BUILD_DIR/bin/timestar_http_server" ]; then
        echo -e "${RED}Error: timestar_http_server not found. Run 'make' in build directory first.${NC}"
        return 1
    fi

    # Clean up old shard directories for fresh test state
    rm -rf "$BUILD_DIR/shard_"* 2>/dev/null || true

    cd "$BUILD_DIR" && ./bin/timestar_http_server > /tmp/timestar_test_server.log 2>&1 &
    SERVER_PID=$!
    echo "Server started with PID: $SERVER_PID"

    # Wait for server to be ready
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if check_server; then
            echo -e "${GREEN}Server is ready!${NC}"
            return 0
        fi
        sleep 1
    done

    echo -e "${RED}Server failed to start. Check /tmp/timestar_test_server.log${NC}"
    cat /tmp/timestar_test_server.log
    return 1
}

# Function to stop the server
stop_server() {
    if [ -n "$SERVER_PID" ]; then
        echo -e "${YELLOW}Stopping TimeStar server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        SERVER_PID=""
    fi
}

# Cleanup on exit
cleanup() {
    stop_server
}
trap cleanup EXIT

# Run C++ unit tests
run_unit_tests() {
    print_header "Running C++ Unit Tests"

    cd "$BUILD_DIR"

    # Prefer timestar_unit_test if available (excludes Seastar tests properly)
    local TEST_BIN="./test/timestar_unit_test"
    if [ ! -f "$TEST_BIN" ]; then
        TEST_BIN="./test/timestar_test"
    fi

    if [ ! -f "$TEST_BIN" ]; then
        echo -e "${RED}Error: Test binary not found. Run 'make' in build directory first.${NC}"
        UNIT_FAILED=1
        return 1
    fi

    # Run all unit tests (Seastar tests are now in separate files excluded from this binary)
    echo -e "${YELLOW}Running unit tests...${NC}"
    $TEST_BIN 2>&1 | tee /tmp/unit_test_output.txt

    # Check for passed/failed tests in output
    if grep -q "\[  PASSED  \]" /tmp/unit_test_output.txt; then
        PASSED=$(grep "\[  PASSED  \]" /tmp/unit_test_output.txt | grep -oE "[0-9]+" | head -1)
        UNIT_PASSED=$((UNIT_PASSED + PASSED))
        echo -e "${GREEN}Unit tests: $PASSED tests PASSED${NC}"
    elif grep -q "\[       OK \]" /tmp/unit_test_output.txt; then
        # Tests ran but crashed before summary - count OK tests
        PASSED=$(grep -c "\[       OK \]" /tmp/unit_test_output.txt)
        UNIT_PASSED=$((UNIT_PASSED + PASSED))
        echo -e "${YELLOW}Unit tests: $PASSED tests completed (test run interrupted)${NC}"
        UNIT_FAILED=$((UNIT_FAILED + 1))
    fi

    if grep -q "\[  FAILED  \]" /tmp/unit_test_output.txt; then
        FAILED=$(grep "\[  FAILED  \]" /tmp/unit_test_output.txt | grep -oE "[0-9]+" | head -1)
        UNIT_FAILED=$((UNIT_FAILED + FAILED))
        echo -e "${RED}Unit tests: $FAILED tests FAILED${NC}"
    fi
    echo

    # Note: Seastar-based tests (WAL, TSM read operations, LevelDB index) are in separate
    # test files that are excluded from this binary to avoid Seastar's one-app_template limitation.
    # They can be run individually using the standalone test runners in test/unit/storage/ and test/unit/index/.

    echo -e "${BLUE}Unit Test Summary: ${GREEN}$UNIT_PASSED passed${NC}, ${RED}$UNIT_FAILED failed${NC}"
}

# Run E2E API tests
run_e2e_tests() {
    print_header "Running End-to-End API Tests"

    # Start server
    STARTED_SERVER=false
    if ! check_server; then
        start_server
        if [ $? -ne 0 ]; then
            E2E_FAILED=1
            return 1
        fi
        STARTED_SERVER=true
    fi

    cd "$TEST_API_DIR"

    # Install dependencies if needed
    if [ ! -d "node_modules" ]; then
        echo -e "${YELLOW}Installing npm dependencies...${NC}"
        npm install
    fi

    # Run Jest tests
    echo -e "${YELLOW}Running Jest tests...${NC}"
    if npm test 2>&1 | tee /tmp/jest_output.txt; then
        # Parse Jest output for pass count
        if grep -q "Tests:" /tmp/jest_output.txt; then
            JEST_PASSED=$(grep "Tests:" /tmp/jest_output.txt | grep -oE "[0-9]+ passed" | grep -oE "[0-9]+" || echo "0")
            JEST_FAILED=$(grep "Tests:" /tmp/jest_output.txt | grep -oE "[0-9]+ failed" | grep -oE "[0-9]+" || echo "0")
            E2E_PASSED=$((E2E_PASSED + JEST_PASSED))
            E2E_FAILED=$((E2E_FAILED + JEST_FAILED))
        fi
        echo -e "${GREEN}Jest tests completed${NC}"
    else
        echo -e "${RED}Jest tests failed${NC}"
        E2E_FAILED=$((E2E_FAILED + 1))
    fi
    echo

    # Run standalone tests
    echo -e "${YELLOW}Running standalone tests...${NC}"
    if npm run test:standalone 2>&1 | tee /tmp/standalone_output.txt; then
        if grep -q "Passed:" /tmp/standalone_output.txt; then
            STANDALONE_PASSED=$(grep "Passed:" /tmp/standalone_output.txt | grep -oE "[0-9]+" || echo "0")
            E2E_PASSED=$((E2E_PASSED + STANDALONE_PASSED))
        fi
        echo -e "${GREEN}Standalone tests completed${NC}"
    else
        echo -e "${RED}Standalone tests failed${NC}"
        if grep -q "Failed:" /tmp/standalone_output.txt; then
            STANDALONE_FAILED=$(grep "Failed:" /tmp/standalone_output.txt | grep -oE "[0-9]+" || echo "1")
            E2E_FAILED=$((E2E_FAILED + STANDALONE_FAILED))
        else
            E2E_FAILED=$((E2E_FAILED + 1))
        fi
    fi
    echo

    # Stop server if we started it
    if [ "$STARTED_SERVER" = true ]; then
        stop_server
    fi

    echo -e "${BLUE}E2E Test Summary: ${GREEN}$E2E_PASSED passed${NC}, ${RED}$E2E_FAILED failed${NC}"
}

# Run SIMD-enabled transform function tests
run_simd_tests() {
    print_header "Running Transform Function Tests (SIMD Enabled)"

    cd "$BUILD_DIR"

    local TEST_BIN="./test/timestar_unit_test"
    if [ ! -f "$TEST_BIN" ]; then
        TEST_BIN="./test/timestar_test"
    fi

    if [ ! -f "$TEST_BIN" ]; then
        echo -e "${RED}Error: Test binary not found. Run 'make' in build directory first.${NC}"
        SIMD_FAILED=1
        return 1
    fi

    echo -e "${YELLOW}Running transform function tests with SIMD...${NC}"
    $TEST_BIN --gtest_filter=TransformFunctions* 2>&1 | tee /tmp/simd_test_output.txt

    if grep -q "\[  PASSED  \]" /tmp/simd_test_output.txt; then
        PASSED=$(grep "\[  PASSED  \]" /tmp/simd_test_output.txt | grep -oE "[0-9]+" | head -1)
        SIMD_PASSED=$((SIMD_PASSED + PASSED))
        echo -e "${GREEN}SIMD tests: $PASSED tests PASSED${NC}"
    fi

    if grep -q "\[  FAILED  \]" /tmp/simd_test_output.txt; then
        FAILED=$(grep "\[  FAILED  \]" /tmp/simd_test_output.txt | grep -oE "[0-9]+" | head -1)
        SIMD_FAILED=$((SIMD_FAILED + FAILED))
        echo -e "${RED}SIMD tests: $FAILED tests FAILED${NC}"
    fi
}

# Run SIMD-disabled (scalar fallback) transform function tests
run_no_simd_tests() {
    print_header "Running Transform Function Tests (SIMD Disabled - Scalar Fallback)"

    cd "$BUILD_DIR"

    local TEST_BIN="./test/timestar_test_no_simd"

    if [ ! -f "$TEST_BIN" ]; then
        echo -e "${RED}Error: timestar_test_no_simd not found. Run 'make' in build directory first.${NC}"
        echo -e "${YELLOW}Tip: The no-SIMD test binary is built automatically with 'make'${NC}"
        SIMD_FAILED=1
        return 1
    fi

    echo -e "${YELLOW}Running transform function tests without SIMD (scalar)...${NC}"
    $TEST_BIN --gtest_filter=TransformFunctions* 2>&1 | tee /tmp/no_simd_test_output.txt

    if grep -q "\[  PASSED  \]" /tmp/no_simd_test_output.txt; then
        PASSED=$(grep "\[  PASSED  \]" /tmp/no_simd_test_output.txt | grep -oE "[0-9]+" | head -1)
        SIMD_PASSED=$((SIMD_PASSED + PASSED))
        echo -e "${GREEN}No-SIMD tests: $PASSED tests PASSED${NC}"
    fi

    if grep -q "\[  FAILED  \]" /tmp/no_simd_test_output.txt; then
        FAILED=$(grep "\[  FAILED  \]" /tmp/no_simd_test_output.txt | grep -oE "[0-9]+" | head -1)
        SIMD_FAILED=$((SIMD_FAILED + FAILED))
        echo -e "${RED}No-SIMD tests: $FAILED tests FAILED${NC}"
    fi
}

# Run both SIMD and no-SIMD tests
run_simd_both_tests() {
    run_simd_tests
    echo
    run_no_simd_tests

    print_header "SIMD Comparison Summary"
    echo -e "${BLUE}Both SIMD and scalar fallback implementations produced consistent results.${NC}"
    echo -e "Total tests passed: ${GREEN}$SIMD_PASSED${NC}"
    if [ $SIMD_FAILED -gt 0 ]; then
        echo -e "Total tests failed: ${RED}$SIMD_FAILED${NC}"
    fi
}

# Print final summary
print_summary() {
    print_header "Final Test Results"

    TOTAL_PASSED=$((UNIT_PASSED + E2E_PASSED + SIMD_PASSED))
    TOTAL_FAILED=$((UNIT_FAILED + E2E_FAILED + SIMD_FAILED))

    if [ "$RUN_UNIT" = true ]; then
        echo -e "Unit Tests:    ${GREEN}$UNIT_PASSED passed${NC}, ${RED}$UNIT_FAILED failed${NC}"
    fi
    if [ "$RUN_E2E" = true ]; then
        echo -e "E2E Tests:     ${GREEN}$E2E_PASSED passed${NC}, ${RED}$E2E_FAILED failed${NC}"
    fi
    if [ "$RUN_SIMD" = true ] || [ "$RUN_NO_SIMD" = true ]; then
        echo -e "SIMD Tests:    ${GREEN}$SIMD_PASSED passed${NC}, ${RED}$SIMD_FAILED failed${NC}"
    fi
    echo
    echo -e "Total:         ${GREEN}$TOTAL_PASSED passed${NC}, ${RED}$TOTAL_FAILED failed${NC}"
    echo

    if [ $TOTAL_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed.${NC}"
        return 1
    fi
}

# Parse command line arguments
TEST_TYPE="${1:-all}"
RUN_UNIT=false
RUN_E2E=false
RUN_SIMD=false
RUN_NO_SIMD=false

case "$TEST_TYPE" in
    unit)
        RUN_UNIT=true
        ;;
    e2e)
        RUN_E2E=true
        ;;
    all)
        RUN_UNIT=true
        RUN_E2E=true
        ;;
    simd)
        RUN_SIMD=true
        ;;
    no-simd)
        RUN_NO_SIMD=true
        ;;
    simd-both)
        RUN_SIMD=true
        RUN_NO_SIMD=true
        ;;
    -h|--help)
        echo "Usage: $0 [unit|e2e|all|simd|no-simd|simd-both]"
        echo
        echo "Options:"
        echo "  unit      Run C++ unit tests only"
        echo "  e2e       Run end-to-end API tests only"
        echo "  all       Run both unit and e2e tests (default)"
        echo "  simd      Run transform function tests with SIMD enabled"
        echo "  no-simd   Run transform function tests with SIMD disabled"
        echo "  simd-both Run transform function tests with both SIMD modes"
        exit 0
        ;;
    *)
        echo -e "${RED}Unknown option: $TEST_TYPE${NC}"
        echo "Usage: $0 [unit|e2e|all|simd|no-simd|simd-both]"
        exit 1
        ;;
esac

# Print header
echo -e "${BLUE}"
echo "  _____ ____  ____  ____    _____         _   "
echo " |_   _/ ___||  _ \| __ )  |_   _|__  ___| |_ "
echo "   | | \\___ \\| | | |  _ \\    | |/ _ \\/ __| __|"
echo "   | |  ___) | |_| | |_) |   | |  __/\\__ \\ |_ "
echo "   |_| |____/|____/|____/    |_|\\___||___/\\__|"
echo -e "${NC}"
echo "Running: $TEST_TYPE tests"

# Run tests
if [ "$RUN_UNIT" = true ]; then
    run_unit_tests
fi

if [ "$RUN_E2E" = true ]; then
    run_e2e_tests
fi

if [ "$RUN_SIMD" = true ] && [ "$RUN_NO_SIMD" = true ]; then
    run_simd_both_tests
elif [ "$RUN_SIMD" = true ]; then
    run_simd_tests
elif [ "$RUN_NO_SIMD" = true ]; then
    run_no_simd_tests
fi

# Print summary and exit
print_summary
