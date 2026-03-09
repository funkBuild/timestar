# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Time Series Database (TimeStar) implementation built with C++23 and the Seastar framework. The database uses TSM (Time-Structured Merge) files for storage and WAL (Write-Ahead Log) for durability.

## Build Commands

```bash
# Configure the build (from project root)
mkdir -p build && cd build
cmake ..

# Build the project
make -j$(nproc)

# Run tests (from build directory)
./test/timestar_test

# Run a specific test
./test/timestar_test --gtest_filter=TestName*

# Run string tests safely (avoids seastar segfaults)
./run_string_tests.sh
```

## Running Tests

### C++ Unit Tests

The project includes comprehensive C++ unit tests using Google Test:

```bash
# From the build directory, run all tests
./test/timestar_test

# Run specific test suites
./test/timestar_test --gtest_filter=MemoryStoreTest*
./test/timestar_test --gtest_filter=TSMTest*
./test/timestar_test --gtest_filter=QueryParserTest*

# List all available tests
./test/timestar_test --gtest_list_tests

# Run with verbose output
./test/timestar_test --gtest_print_time=1
```

Test coverage includes:
- Storage components (TSM, WAL, MemoryStore)
- Encoding algorithms (Float, Boolean, String, Integer)
- Query system (Parser, Planner, Aggregator, Runner)
- HTTP handlers (Write, Query, Metadata)
- Index system (LevelDB metadata index)
- End-to-end integration tests

### API Integration Tests

The `test_api/` directory contains JavaScript-based API integration tests:

```bash
# Start the TimeStar server first
./bin/timestar_http_server --port 8086

# In another terminal, navigate to test_api directory
cd test_api/

# Install dependencies (first time only)
npm install

# Run Jest test suite (46 tests)
npm test

# Run standalone integration tests (8 tests)
npm run test:standalone

# Run all tests
npm run test:all

# Run tests with coverage
npm run test:coverage

# Watch mode for development
npm run test:watch
```

Test files include:
- `http_api_tests/comprehensive_query.test.js` - Query functionality tests
- `http_api_tests/metadata_api.test.js` - Metadata API tests
- `http_api_tests/delete_and_reinsert.test.js` - Delete operation tests
- Standalone tests for specific features (strings, booleans, parsing, etc.)

### Expected Test Results

When all tests pass, you should see:
- **C++ Tests**: 207 tests from 21 test suites, all passing
- **Jest Tests**: 46 tests from 3 test suites, all passing
- **Standalone Tests**: 8 tests, all passing

```

## Key Executables

- `build/bin/timestar` - Main TimeStar binary
- `build/bin/timestar_server` - TimeStar server implementation  
- `build/bin/timestar_http_server` - HTTP API server with JSON write endpoint
- `build/test/timestar_test` - Unit tests using Google Test

## Architecture

### Core Components

The codebase is organized around these key abstractions:

1. **Engine** (`lib/engine.hpp/cpp`) - Main orchestrator that coordinates TSM files and WAL operations across shards
   - Manages TSMFileManager and WALFileManager
   - Handles inserts, queries, and memory store rollovers
   - Implements sharding with per-shard data directories

2. **TSM Files** (`lib/tsm.hpp/cpp`) - Time-Structured Merge tree storage format
   - Immutable files containing compressed time series data
   - Indexed by series ID with min/max time bounds
   - Supports Float, Boolean, and String value types
   - Files ranked by tier number and sequence number for compaction
   - String values compressed using Snappy compression with variable-length prefixes

3. **WAL** (`lib/wal.hpp/cpp`) - Write-Ahead Log for durability
   - Ensures data persistence before acknowledgment
   - Used to recover in-memory stores on restart
   - Supports write, delete, and delete-range operations

4. **Memory Store** (`lib/memory_store.hpp/cpp`) - In-memory buffer for recent writes
   - Holds data before flushing to TSM files
   - Provides fast reads for recent data
   - Rolled over periodically to create new TSM files

5. **Encoders** - Compression algorithms for efficient storage:
   - `integer_encoder` - Integer compression using Simple8b
   - `float_encoder` - Float compression using XOR encoding
   - `bool_encoder` - Boolean value compression
   - `string_encoder` - String compression using Snappy with variable-length prefixes
   - `tsxor_encoder` - Timestamp compression using XOR

6. **Query Runner** (`lib/query_runner.hpp/cpp`) - Executes queries across TSM files and memory stores
   - Merges results from multiple sources
   - Handles time range filtering

### Data Flow

1. Writes go to WAL first for durability
2. Data is kept in MemoryStore for fast access
3. MemoryStore periodically rolled over to TSM files
4. TSM files are compacted in background to optimize storage

### Seastar Integration

The project uses Seastar for async I/O and coroutines:
- All I/O operations return `seastar::future<>`
- Uses `co_await` for async operations
- Sharding leverages Seastar's shard-per-core model

## Compiler Requirements

The build system automatically selects the appropriate compiler:
- Prefers GCC 14+ for full C++23 support
- Falls back to GCC 13 with C++20 for Seastar only
- Clang < 19 is explicitly blocked due to libstdc++ 13 incompatibilities

## Dependencies

- Seastar (included as submodule in `external/seastar`)
- Google Test (fetched automatically for tests)
- Snappy compression library
- LevelDB (install with `sudo apt install libleveldb-dev` on Ubuntu/Debian)
- Threads library

## Data Storage

The database creates shard directories (`shard_0`, `shard_1`, etc.) in the build directory, each containing:
- `wal/` - Write-ahead log files
- `tsm/` - TSM data files
- `index/` - LevelDB index files for metadata and series lookup

## String Type Implementation

The TimeStar includes full support for string time series data with the following implementation:

### String Encoder (`lib/string_encoder.hpp/cpp`)

- **Compression**: Uses Snappy compression for efficient string storage
- **Encoding Format**: Variable-length prefixes followed by compressed string data
- **API**: 
  - `encode(values, buffer)` - Encodes vector of strings into compressed buffer
  - `decode(slice, count, values)` - Decodes compressed data back to string vector

### TSM String Support

- **Type System**: String values use `TSMValueType::String` enum
- **Template Support**: TSM read/write operations fully templated for `std::string`
- **Query Integration**: String queries supported through `TSMResult<std::string>`
- **Memory Store**: In-memory string data before TSM flush
- **File Format**: String blocks stored with Snappy compression in TSM files

### Testing Framework

Due to Seastar's architecture limitation (single `app_template` per process), string tests are structured as:

- **StringEncoder Tests**: Unit tests for encode/decode operations (10 tests)
- **TSM String Tests**: Integration tests with actual file I/O (7 tests)  
- **Test Runner**: `run_string_tests.sh` script runs tests safely to avoid segfaults
- **Documentation**: `SEASTAR_TESTING.md` explains testing constraints and solutions

### Features Verified

- ✅ String encoding/decoding with Snappy compression
- ✅ TSM file read/write operations  
- ✅ Memory store integration
- ✅ Mixed data type support (float, bool, string)
- ✅ Time-range queries
- ✅ Large dataset handling (25K+ entries)
- ✅ Special character support (UTF-8, JSON, paths)
- ✅ Multi-block data spanning

## LevelDB Index System

The TimeStar includes a LevelDB-based indexing system for efficient metadata queries and series discovery:

### Index Architecture (`lib/leveldb_index.hpp/cpp`)

- **Per-Shard Storage**: Each shard maintains its own LevelDB index in `shard_N/index/`
- **Key Encoding**: Different index types use prefixed keys for separation:
  - `0x01`: Series mapping (`measurement+tags+field → series_id`)
  - `0x02`: Measurement fields (`measurement → [field1, field2, ...]`)
  - `0x03`: Measurement tags (`measurement → [tag_key1, tag_key2, ...]`)
  - `0x04`: Tag values (`measurement+tag_key → [value1, value2, ...]`)

### Features

- **Series ID Generation**: Automatic numeric series ID assignment for efficient storage
- **Metadata Indexing**: Fast lookup of measurements, fields, and tag key/values
- **Batch Operations**: Efficient bulk indexing during data ingestion
- **Snappy Compression**: Built-in LevelDB compression for space efficiency
- **Bloom Filters**: Optimized point lookups with configurable bloom filter

### Engine Integration

- **Automatic Indexing**: All inserts are indexed transparently via `Engine::insert()`
- **Query Support**: New query methods like `queryBySeries()` for structured queries
- **Metadata APIs**: Methods to discover measurement fields, tags, and tag values
- **Backwards Compatibility**: Existing string-based series keys still supported

### API Examples

```cpp
// Get/create series ID for a measurement
uint64_t seriesId = co_await index.getOrCreateSeriesId("weather", 
    {{"location", "us-west"}, {"host", "server01"}}, "temperature");

// Query metadata
auto fields = co_await engine.getMeasurementFields("weather");
auto tags = co_await engine.getMeasurementTags("weather"); 
auto locations = co_await engine.getTagValues("weather", "location");

// Structured queries
auto result = co_await engine.queryBySeries("weather",
    {{"location", "us-west"}}, "temperature", startTime, endTime);
```

## HTTP Write API

The TimeStar includes an HTTP server with a JSON-based write API for data ingestion:

### Server (`bin/timestar_http_server.cpp`)

- **Port**: Default 8086 (InfluxDB-compatible)
- **Sharding**: Automatic distribution based on full series key hash (measurement + tags + field)
  - Each unique series (measurement,tags,field combination) is independently sharded
  - Provides better load balancing across all CPU cores
  - Different fields from the same measurement can be on different shards
- **Concurrency**: Full Seastar async/await with coroutines

### Write Endpoint (`lib/http_write_handler.hpp/cpp`)

**Endpoint**: `POST /write`

**Single Point Format**:
```json
{
  "measurement": "temperature",
  "tags": {
    "location": "us-midwest",
    "host": "server-01"
  },
  "fields": {
    "value": 82.5,
    "humidity": 65.0
  },
  "timestamp": 1465839830100400200
}
```

**Batch Write Format**:
```json
{
  "writes": [
    {
      "measurement": "temperature",
      "tags": {"location": "us-west"},
      "fields": {"value": 75.0},
      "timestamp": 1465839830100400200
    }
  ]
}
```

### Features

- **Type Detection**: Automatic detection of field types (float, bool, string, int)
- **Index Integration**: Automatic series ID generation and metadata indexing
- **Error Handling**: Detailed JSON error responses with HTTP status codes
- **Batch Support**: Efficient batch writes with partial failure handling
- **Default Timestamps**: Auto-generates nanosecond timestamps if not provided

### Additional Endpoints

- `GET /health` - Health check endpoint
- `POST /query` - Query endpoint for time series data retrieval
- `GET /measurements` - List measurements (placeholder)

### Testing

#### C++ Tests
The `test/` directory contains all C++ unit and integration tests:
```bash
# Run all tests
ctest
# Or run the test binary directly
./test/timestar_test

# Run a specific test
./test/timestar_test --gtest_filter=TestName*
```

#### API Tests
The `test_api/` directory contains JavaScript and Python API tests:
```bash
# Run functional tests
python test_api/test_http_write.py

# Run stress test (1000 points in batches of 100)
python test_api/test_http_write.py --stress

# Test against custom host/port
python test_api/test_http_write.py --host localhost --port 8086
```

### Starting the Server

```bash
# Start with default port (8086)
./build/bin/timestar_http_server

# Start with custom port
./build/bin/timestar_http_server --port 9000
```

## HTTP Query API

The TimeStar provides a query endpoint with a simplified string-based query format inspired by time series databases.

### Query Endpoint (`POST /query`)

**Request Format**:
```json
{
  "query": "aggregationMethod:measurement(fields){scopes} by {aggregationTagKeys}",
  "startTime": 1704067200000000000,  // Nanoseconds since epoch (Jan 1, 2024)
  "endTime": 1706745599000000000,    // Nanoseconds since epoch (Jan 31, 2024)
  "aggregationInterval": 300000000000 // Optional: Time bucket interval in nanoseconds (5 minutes)
}
```

### Query String Format

The query string follows this pattern:
```
aggregationMethod:measurement(fields){scopes} by {aggregationTagKeys}
```

#### Components:

1. **Aggregation Methods** (required):
   - `avg` - Average of values (default)
   - `min` - Minimum value
   - `max` - Maximum value  
   - `sum` - Sum of values
   - `latest` - Most recent value

2. **Measurement** (required):
   - The measurement name to query
   - Must match existing measurement in database
   - Case-sensitive

3. **Fields** (optional):
   - Comma-separated list within parentheses
   - Empty parentheses `()` returns all fields
   - Example: `(value,humidity)` or `()`

4. **Scopes** (optional):
   - Filter conditions in `key:value` format within braces
   - Multiple scopes separated by commas (AND condition)
   - Must use exact values (no wildcards currently)
   - Empty braces `{}` for no filtering
   - Example: `{location:us-west,sensor:temp-01}`

5. **Group By** (optional):
   - Tag keys for grouping results after `by` keyword
   - Multiple keys separated by commas
   - Omit entire `by {}` clause if not grouping
   - Example: `by {location,sensor}`

6. **Aggregation Interval** (optional):
   - Time interval for bucketing data points
   - Can be specified as:
     - Numeric value in nanoseconds: `"aggregationInterval": 300000000000`
     - String with unit suffix: `"aggregationInterval": "5m"`
   - Supported units:
     - `ns` - nanoseconds
     - `us`, `µs` - microseconds
     - `ms` - milliseconds  
     - `s` - seconds
     - `m` - minutes
     - `h` - hours
     - `d` - days
   - Decimal values supported: `"1.5s"`, `"0.5m"`
   - When specified, results are grouped into time buckets
   - Aggregation method applies within each bucket

### Time Format

**Timestamp Format**:
- Timestamps are numeric values representing time since Unix epoch (January 1, 1970)
- The API accepts any numeric precision:
  - Seconds: `1704067200`
  - Milliseconds: `1704067200000`
  - Microseconds: `1704067200000000`
  - Nanoseconds: `1704067200000000000` (recommended for precision)
- Internally, all timestamps are converted to nanoseconds
- To convert: multiply seconds by 10^9, milliseconds by 10^6, microseconds by 10^3

### Query Examples

#### Simple Query
```json
{
  "query": "avg:temperature()",
  "startTime": 1704067200000000000,  // Jan 1, 2024 00:00:00 in nanoseconds
  "endTime": 1704585600000000000     // Jan 7, 2024 00:00:00 in nanoseconds
}
```

#### Filtered Query with Specific Fields
```json
{
  "query": "max:cpu(usage_percent){host:server-01,datacenter:dc1}",
  "startTime": 1707998400000000000,  // Feb 15, 2024 12:00:00 in nanoseconds
  "endTime": 1708020000000000000     // Feb 15, 2024 18:00:00 in nanoseconds
}
```

#### Grouped Aggregation
```json
{
  "query": "avg:temperature(value,humidity){location:us-west} by {sensor}",
  "startTime": 1709251200000000000,  // Mar 1, 2024 00:00:00 in nanoseconds
  "endTime": 1709254800000000000     // Mar 1, 2024 01:00:00 in nanoseconds
}
```

#### Latest Values Query
```json
{
  "query": "latest:system.metrics(cpu,memory){server:prod-01}",
  "startTime": 1709251200000000000,  // Mar 1, 2024 00:00:00 in nanoseconds
  "endTime": 1709254800000000000     // Mar 1, 2024 01:00:00 in nanoseconds
}
```

#### Time-Bucketed Aggregation (5-minute intervals)
```json
{
  "query": "avg:temperature(value){location:us-west}",
  "startTime": 1709251200000000000,  // Mar 1, 2024 00:00:00 in nanoseconds
  "endTime": 1709337600000000000,    // Mar 2, 2024 00:00:00 in nanoseconds
  "aggregationInterval": "5m"         // Average values in 5-minute buckets
}
```

#### Hourly Maximum Values
```json
{
  "query": "max:cpu(usage_percent){datacenter:dc1}",
  "startTime": 1709251200000000000,  // Mar 1, 2024 00:00:00 in nanoseconds
  "endTime": 1709337600000000000,    // Mar 2, 2024 00:00:00 in nanoseconds
  "aggregationInterval": 3600000000000 // 1 hour in nanoseconds
}
```

### Response Format

**Successful Response**:
```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "tags": {
        "location": "us-west",
        "sensor": "temp-01"
      },
      "fields": {
        "value": {
          "timestamps": [1638202821000000000, 1638202822000000000],
          "values": [23.5, 23.6]
        }
      }
    }
  ],
  "statistics": {
    "series_count": 1,
    "point_count": 2,
    "execution_time_ms": 12.5
  }
}
```

**Response with Time Buckets** (when using aggregationInterval):
```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "tags": {
        "location": "us-west"
      },
      "fields": {
        "value": {
          "timestamps": [
            1709251200000000000,  // Mar 1, 2024 00:00:00 (bucket start)
            1709251500000000000,  // Mar 1, 2024 00:05:00 (bucket start)
            1709251800000000000   // Mar 1, 2024 00:10:00 (bucket start)
          ],
          "values": [23.5, 24.2, 23.8]  // Aggregated values per bucket
        }
      }
    }
  ],
  "statistics": {
    "series_count": 1,
    "point_count": 3,  // Number of buckets
    "execution_time_ms": 15.3
  }
}
```

**Error Response**:
```json
{
  "status": "error",
  "error": {
    "code": "INVALID_QUERY",
    "message": "Invalid query format: missing measurement"
  }
}
```

### Query Features

- **Multi-shard coordination**: Queries automatically fan out to all relevant shards
- **Parallel execution**: Each shard processes queries independently
- **Result aggregation**: Results merged and aggregated from all shards
- **Time filtering**: Efficient time-based data retrieval using TSM indexes
- **Tag filtering**: Fast lookups using LevelDB index for series discovery

### Performance Considerations

- Queries with specific tag filters (scopes) are faster as they limit series scanning
- Time ranges should be as narrow as possible for optimal performance
- Aggregations reduce data transfer and memory usage
- Group-by operations may increase result size significantly

### Future Query Enhancements

- Wildcard and regex support in scopes
- Time-based aggregation intervals (e.g., 5m, 1h intervals)
- Join operations between multiple measurements
- Continuous queries and materialized views
- InfluxQL compatibility layer

## Performance Logging Configuration

The TimeStar includes compile-time controls for verbose logging in performance-critical paths. This allows developers to enable detailed logging for debugging without impacting production performance.

### Logging Control Defines

Located in `lib/utils/logging_config.hpp`:

- **`TIMESTAR_LOG_INSERT_PATH`** (default: 0): Controls logging in the insert/write path
  - Includes: WAL writes, memory store inserts, TSM writes, HTTP write handler
  - Enable by setting to 1 or defining during compilation

- **`TIMESTAR_LOG_QUERY_PATH`** (default: 0): Controls logging in the query/read path  
  - Includes: Query parsing, planning, execution, result merging
  - Enable by setting to 1 or defining during compilation

### Enabling Logging

#### Method 1: Compile-time flags
```bash
# Enable insert path logging
cmake -DCMAKE_CXX_FLAGS="-DTIMESTAR_LOG_INSERT_PATH=1" ..
make

# Enable query path logging
cmake -DCMAKE_CXX_FLAGS="-DTIMESTAR_LOG_QUERY_PATH=1" ..
make

# Enable both
cmake -DCMAKE_CXX_FLAGS="-DTIMESTAR_LOG_INSERT_PATH=1 -DTIMESTAR_LOG_QUERY_PATH=1" ..
make
```

#### Method 2: Edit the header file
Edit `lib/utils/logging_config.hpp` and change the default values:
```cpp
#define TIMESTAR_LOG_INSERT_PATH 1  // Enable insert path logging
#define TIMESTAR_LOG_QUERY_PATH 1   // Enable query path logging
```

### Performance Impact

When disabled (default), the logging macros compile to no-ops with zero runtime overhead. When enabled, logging statements use Seastar's logging framework with appropriate log levels (trace, debug, info).

### Affected Components

**Insert Path:**
- `lib/storage/wal.cpp` - WAL operations
- `lib/storage/memory_store.cpp` - Memory store inserts
- `lib/http/http_write_handler.cpp` - HTTP write endpoint

**Query Path:**
- `lib/query/query_runner.cpp` - Query execution
- `lib/http/http_query_handler.cpp` - HTTP query endpoint (future)