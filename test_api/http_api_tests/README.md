# HTTP API Test Suite

This directory contains Python-based integration tests for the TSDB HTTP API. These tests verify the server's HTTP endpoints are working correctly.

## Prerequisites

```bash
# Install required Python packages
pip install requests
```

## Starting the TSDB Server

Before running tests, ensure the TSDB HTTP server is running:

```bash
# From the build directory
./bin/tsdb_http_server
# Or with custom port
./bin/tsdb_http_server --port 8086
```

## Test Files

### Core Test Suites

- **`test_http_write.py`** - Tests the `/write` endpoint
  - Single point writes
  - Batch writes  
  - Different data types (float, bool, string, int)
  - Error handling
  - Stress testing

- **`test_query_e2e.py`** - Tests the `/query` endpoint
  - Query parsing
  - Time range queries
  - Aggregation methods
  - Field selection
  - Tag filtering

- **`test_http_e2e.py`** - Full integration test
  - Write-then-query workflows
  - Multiple series handling
  - Complex queries

### Quick Tests

- **`test_e2e_quick.py`** - Quick smoke test (~30 seconds)
  - Basic write/query verification
  - Health check
  - Essential functionality

- **`test_e2e_comprehensive.py`** - Comprehensive test suite (~5 minutes)
  - All endpoints
  - Edge cases
  - Error conditions
  - Performance characteristics

### Utility Scripts

- **`test_tsdb_client.py`** - Interactive test client
  - Random data generation
  - Continuous testing
  - Manual exploration

### Performance Tests

- **`benchmark_http.py`** - HTTP performance benchmark
  - Measures throughput (writes/second)
  - Query latency
  - 1000+ operations

- **`benchmark_scaled.py`** - Large-scale test
  - 4 weeks of data at 1-minute intervals
  - Multiple fields and tags
  - Realistic workload simulation

## Running Tests

### Run Individual Tests

```bash
# Basic write test
python test_http_write.py

# Query test
python test_query_e2e.py

# Quick smoke test
python test_e2e_quick.py

# With custom server location
python test_http_write.py --host localhost --port 8086
```

### Run with Options

Most test scripts support these options:
- `--host` - Server hostname (default: localhost)
- `--port` - Server port (default: 8086)
- `--stress` - Run stress test mode (where applicable)
- `--verbose` - Verbose output

### Run All Tests

```bash
# Run all tests in sequence
for test in test_*.py; do
    echo "Running $test..."
    python "$test"
done
```

### Performance Testing

```bash
# Basic benchmark
python benchmark_http.py

# Scaled benchmark (large dataset)
python benchmark_scaled.py

# Continuous stress test
python test_http_write.py --stress
```

## Expected Output

Successful tests will show:
- ✓ checkmarks for passed tests
- Response times
- Summary statistics

Failed tests will show:
- ✗ marks for failures
- Error messages
- Response details for debugging

## Test Coverage

The test suite covers:

1. **Write Operations**
   - Single and batch writes
   - All data types (float, bool, string, integer)
   - Tag and field handling
   - Timestamp formats

2. **Query Operations**
   - Time range selection
   - Aggregation methods (avg, min, max, sum, latest)
   - Field selection
   - Tag filtering (scopes)
   - Group-by operations
   - Aggregation intervals

3. **Error Handling**
   - Invalid JSON
   - Missing fields
   - Malformed queries
   - Server errors

4. **Performance**
   - Write throughput
   - Query latency
   - Concurrent operations
   - Large datasets

## Adding New Tests

When adding new Python test scripts:

1. Follow the naming convention: `test_<feature>.py`
2. Include a docstring describing the test purpose
3. Support `--host` and `--port` arguments
4. Use clear output with ✓/✗ indicators
5. Return appropriate exit codes (0 for success, 1 for failure)

## Notes

- These tests assume the TSDB server is already running
- Tests use the HTTP API on port 8086 by default
- Some tests generate significant data - ensure adequate disk space
- Performance tests may take several minutes to complete