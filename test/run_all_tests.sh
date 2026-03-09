#!/bin/bash

# Comprehensive test runner for TimeStar project
# Runs all tests including Seastar-based tests (individually to avoid segfaults)

echo "🔧 TimeStar Complete Test Suite"
echo "============================"
echo

TOTAL_PASSED=0
TOTAL_FAILED=0
FAILED_TESTS=()

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/build"

# Run all non-Seastar tests together
echo "📊 Running non-Seastar tests..."
./test/timestar_test --gtest_filter="-TSMCompactorTest.*:TSMStringTest.*" 2>&1 | tee /tmp/test_output.txt | grep -E "^\[==========\]|^\[  PASSED  \]|^\[  FAILED  \]"

if grep -q "\[  PASSED  \]" /tmp/test_output.txt; then
    PASSED=$(grep "\[  PASSED  \]" /tmp/test_output.txt | grep -oE "[0-9]+" | head -1)
    TOTAL_PASSED=$((TOTAL_PASSED + PASSED))
    echo "✅ Non-Seastar tests: $PASSED tests PASSED"
else
    echo "❌ Non-Seastar tests: FAILED"
    TOTAL_FAILED=$((TOTAL_FAILED + 1))
fi
echo

# Run TSMCompactor tests individually (Seastar-based)
echo "🗄️  Running TSMCompactor tests (Seastar-based individually)..."
declare -a COMPACTOR_TESTS=(
    "BasicCompaction"
    "DeduplicationDuringCompaction"
    "NewerValuesOverwriteOlderDuringCompaction"
    "ReferenceCountingPreventsDelete"
    "MultiLevelCompactionPreservesNewerValues"
    "CompactionPlanGeneration"
    "LeveledCompactionStrategy"
    "ConcurrentReadsDuringCompaction"
    "CompactionStatistics"
    "FullCompaction"
    "TimeBasedCompactionStrategy"
    "MixedDataTypeCompaction"
    "CompactionErrorHandling"
)

COMPACTOR_PASSED=0
for test in "${COMPACTOR_TESTS[@]}"; do
    echo -n "  🧪 Testing $test... "
    timeout 5 ./test/timestar_test --gtest_filter="TSMCompactorTest.$test" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ PASSED"
        ((COMPACTOR_PASSED++))
        ((TOTAL_PASSED++))
    else
        echo "❌ FAILED/TIMEOUT"
        FAILED_TESTS+=("TSMCompactorTest.$test")
        ((TOTAL_FAILED++))
    fi
done
echo "  Summary: $COMPACTOR_PASSED/${#COMPACTOR_TESTS[@]} PASSED"
echo

# Run TSMString tests individually (Seastar-based)
echo "🗄️  Running TSMString tests (Seastar-based individually)..."
declare -a STRING_TESTS=(
    "WriteAndReadStringData"
    "WriteMixedDataTypes"
    "LargeStringDataset"
    "EmptyAndSpecialStrings"
    "StringBlockBoundaries"
    "StringSeriesCompression"
    "MemoryStoreWithStrings"
)

STRING_PASSED=0
for test in "${STRING_TESTS[@]}"; do
    echo -n "  🧪 Testing $test... "
    timeout 5 ./test/timestar_test --gtest_filter="TSMStringTest.$test" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ PASSED"
        ((STRING_PASSED++))
        ((TOTAL_PASSED++))
    else
        echo "❌ FAILED/TIMEOUT"
        FAILED_TESTS+=("TSMStringTest.$test")
        ((TOTAL_FAILED++))
    fi
done
echo "  Summary: $STRING_PASSED/${#STRING_TESTS[@]} PASSED"
echo

# Final summary
echo "================================"
echo "📈 FINAL RESULTS:"
echo "  ✅ Total Passed: $TOTAL_PASSED tests"
echo "  ❌ Total Failed: $TOTAL_FAILED tests"

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo "  🎉 ALL TESTS PASSED!"
    echo
    echo "📝 Note: Seastar tests are run individually to avoid multi-instance"
    echo "     limitations. This is a known constraint of seastar's app_template."
    exit 0
else
    echo "  ❌ FAILED TESTS:"
    for failed in "${FAILED_TESTS[@]}"; do
        echo "     - $failed"
    done
    exit 1
fi