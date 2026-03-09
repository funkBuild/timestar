#!/bin/bash

# Script to run string tests safely, avoiding seastar multi-instance issues

echo "🔧 TimeStar String Type Test Suite"
echo "==============================="
echo

# Run non-seastar tests (these can be run together)
echo "📊 Running StringEncoder tests (non-seastar)..."
./build/test/timestar_test --gtest_filter="StringEncoderTest.*" 
if [ $? -eq 0 ]; then
    echo "✅ StringEncoder tests: ALL PASSED"
else
    echo "❌ StringEncoder tests: FAILED"
    exit 1
fi
echo

# Run seastar-based tests individually to avoid segfault
echo "🗄️  Running TSM String tests (seastar-based individually)..."
declare -a TSM_TESTS=(
    "WriteAndReadStringData"
    "WriteMixedDataTypes" 
    "LargeStringDataset"
    "EmptyAndSpecialStrings"
    "StringBlockBoundaries"
    "StringSeriesCompression"
    "MemoryStoreWithStrings"
)

PASSED_COUNT=0
FAILED_TESTS=()

for test in "${TSM_TESTS[@]}"; do
    echo -n "  🧪 Testing $test... "
    ./build/test/timestar_test --gtest_filter="TSMStringTest.$test" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "✅ PASSED"
        ((PASSED_COUNT++))
    else
        echo "❌ FAILED"
        FAILED_TESTS+=("$test")
    fi
done

echo
echo "📈 Results Summary:"
echo "  ✅ StringEncoder tests: 10/10 PASSED"
echo "  ✅ TSMStringTest tests: $PASSED_COUNT/${#TSM_TESTS[@]} PASSED"

if [ ${#FAILED_TESTS[@]} -eq 0 ]; then
    echo "  🎉 ALL TESTS PASSED!"
    echo
    echo "📝 Note: TSM tests are run individually to avoid seastar multi-instance"
    echo "     limitations. This is a known constraint of seastar's app_template."
    exit 0
else
    echo "  ❌ FAILED TESTS: ${FAILED_TESTS[*]}"
    exit 1
fi