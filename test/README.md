# TimeStar Test Suite

This directory contains the test suite for the TimeStar project, organized by areas of concern.

## Test Organization

Tests are organized into the following categories:

### Unit Tests (`unit/`)
Focused tests for individual components with minimal dependencies.

- **`unit/storage/`** - Storage layer tests (TSM, WAL, Memory Store, Compactor)
- **`unit/encoding/`** - Encoding/compression algorithm tests (FFOR integer, Float, String encoding)
- **`unit/query/`** - Query system tests (Parser, Planner, Aggregator, Series Matcher)
- **`unit/index/`** - Index system tests (NativeIndex, key encoding, postings)
- **`unit/http/`** - HTTP handler tests (Query handler, Write handler)

### Integration Tests (`integration/`)
Tests that verify interaction between multiple components.

- Write path integration
- Query execution integration
- Coroutine functionality

### End-to-End Tests (`e2e/`)
Complete workflow tests that exercise the full system.

- Query end-to-end tests
- Delete operation tests
- Full data lifecycle tests

### Performance Tests (`performance/`)
Benchmarks and performance measurement tests.

- Write performance benchmarks
- Query performance tests
- Compaction benchmarks

## Running Tests

### Run All Tests
```bash
make timestar_test
./test/timestar_test
```

### Run Specific Test Categories
```bash
# Unit tests only
make test-unit

# Integration tests only  
make test-integration

# End-to-end tests only
make test-e2e

# Performance tests only
make test-perf
```

### Run Specific Test Suites
```bash
# Run all storage tests
./test/timestar_test --gtest_filter="*Storage*"

# Run all query tests
./test/timestar_test --gtest_filter="*Query*"

# Run specific test class
./test/timestar_test --gtest_filter="MemoryStoreTest.*"
```

## Test Executables

The build system creates multiple test executables:

- `timestar_test` - All tests combined (default)
- `timestar_unit_test` - Unit tests only
- `timestar_integration_test` - Integration tests only
- `timestar_e2e_test` - End-to-end tests only
- `timestar_perf_test` - Performance tests only

## Seastar-Based Tests

Some tests require the Seastar framework and are compiled separately due to special compilation requirements. These tests use coroutines and async I/O:

- Storage: `tsm_compactor_test`, `tsm_tombstone_test`, `tsm_string_test`
- Index: `native_index_*_test`
- Integration: Most integration tests
- E2E: `delete_e2e_test`, `flexible_delete_test`
- Performance: All performance tests

These tests are currently excluded from the main test executable and will be integrated in a future update.

## Adding New Tests

1. Determine the appropriate category for your test
2. Place the test file in the corresponding directory
3. Follow the naming convention: `<component>_test.cpp`
4. Include Google Test headers and use TEST/TEST_F macros
5. If using Seastar, add the test to the SEASTAR_TESTS list in CMakeLists.txt

## Test Conventions

- Use descriptive test names that explain what is being tested
- Each test should be independent and not rely on other tests
- Clean up any test data/files in TearDown() or use RAII
- Use EXPECT_* for non-critical assertions, ASSERT_* for critical ones
- Mock external dependencies when appropriate
- Keep unit tests fast (< 100ms per test)