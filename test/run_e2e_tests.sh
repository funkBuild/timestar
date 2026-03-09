#!/bin/bash

# End-to-End Test Runner for TimeStar HTTP Server
# Runs both quick tests and comprehensive test suite

set -e

cd "$(dirname "$0")"

echo "=============================================="
echo "TimeStar HTTP Server - End-to-End Test Runner"
echo "=============================================="

# Check if Python and requests are available
if ! python3 -c "import requests" 2>/dev/null; then
    echo "Installing required Python packages..."
    python3 -m pip install requests --user
fi

# Make test scripts executable
chmod +x test/test_e2e_quick.py
chmod +x test/test_e2e_comprehensive.py

# Run quick tests first
echo
echo "1. Running Quick Tests..."
echo "----------------------------------------------"
if python3 test/test_e2e_quick.py; then
    echo "✓ Quick tests passed"
    QUICK_PASSED=true
else
    echo "✗ Quick tests failed"
    QUICK_PASSED=false
fi

echo
echo "2. Running Comprehensive Test Suite..."
echo "----------------------------------------------"
if python3 test/test_e2e_comprehensive.py; then
    echo "✓ Comprehensive tests passed"
    COMPREHENSIVE_PASSED=true
else
    echo "✗ Comprehensive tests failed"
    COMPREHENSIVE_PASSED=false
fi

# Summary
echo
echo "=============================================="
echo "FINAL RESULTS"
echo "=============================================="
echo "Quick Tests:        $([ "$QUICK_PASSED" = "true" ] && echo "✓ PASSED" || echo "✗ FAILED")"
echo "Comprehensive:      $([ "$COMPREHENSIVE_PASSED" = "true" ] && echo "✓ PASSED" || echo "✗ FAILED")"

if [ "$QUICK_PASSED" = "true" ] && [ "$COMPREHENSIVE_PASSED" = "true" ]; then
    echo
    echo "🎉 ALL TESTS PASSED! The TimeStar HTTP server is working correctly."
    exit 0
else
    echo
    echo "❌ SOME TESTS FAILED. Check the output above for details."
    exit 1
fi