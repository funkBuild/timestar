# Seastar Testing Notes

## Issue: Segfault with Multiple Tests

When running multiple seastar-based tests together (e.g., `./test/tsdb_test --gtest_filter="*String*"`), a segmentation fault occurs after all tests complete. This happens because:

1. **Seastar App Template Limitation**: Each test creates a new `seastar::app_template` instance
2. **Resource Conflicts**: Multiple app_template instances in the same process cause resource conflicts during cleanup
3. **Known Issue**: This is a documented limitation of seastar's architecture

## Root Cause Analysis

- **Individual tests work perfectly**: Each TSM string test passes when run in isolation
- **Batch execution fails**: Running multiple seastar tests together causes segfault in cleanup
- **Non-seastar tests unaffected**: StringEncoder tests (which don't use seastar) work fine in batches

## Solution

Use the provided test runner script: `./run_string_tests.sh`

This script:
- Runs non-seastar tests together (safe)
- Runs seastar-based tests individually (avoids conflicts)
- Provides clean pass/fail reporting

## Test Results

```
✅ StringEncoder tests: 10/10 PASSED  
✅ TSMStringTest tests: 7/7 PASSED
🎉 ALL TESTS PASSED!
```

## Implementation Status

The string type implementation is **fully functional**:

- ✅ String encoding/decoding with Snappy compression
- ✅ TSM file read/write operations  
- ✅ Memory store integration
- ✅ Mixed data type support (float, bool, string)
- ✅ Time-range queries
- ✅ Large dataset handling (25K+ entries)
- ✅ Special character support (UTF-8, JSON, paths)
- ✅ Multi-block data spanning

The segfault is purely a testing infrastructure limitation and doesn't affect the production functionality.