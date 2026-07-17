## Project Overview

This is a Time Series Database (TimeStar) implementation built with C++23 and the Seastar framework. The database uses TSM (Time-Structured Merge) files for storage and WAL (Write-Ahead Log) for durability.

This project must be developed inline with the below requirements -
- SIMD is used whenever possible using Google Highway
- All data structures and code emphasizes efficiency and compute/memory optimal solutions
- The database must be able to store more data than system memory allows, focusing on efficient cache usage over holding raw data in memory

## Build Commands

```bash
# Configure the build (from project root)
mkdir -p build && cd build
cmake ..

# Build the project
make -j$(nproc)

# Run tests (from build directory)
./test/timestar_unit_test

# Run a specific test
./test/timestar_unit_test --gtest_filter=TestName*

# Run string tests safely (avoids seastar segfaults)
../test/run_string_tests.sh
```

## Running Tests

### C++ Unit Tests

The project includes comprehensive C++ unit tests using Google Test:

```bash
# From the build directory, run all tests
./test/timestar_unit_test

# Run specific test suites
./test/timestar_unit_test --gtest_filter=MemoryStoreTest*
./test/timestar_unit_test --gtest_filter=TSMTest*
./test/timestar_unit_test --gtest_filter=QueryParserTest*

# List all available tests
./test/timestar_unit_test --gtest_list_tests

# Run with verbose output
./test/timestar_unit_test --gtest_print_time=1
```

Test coverage includes:
- Storage components (TSM, WAL, MemoryStore)
- Encoding algorithms (Float, Boolean, String, Integer)
- Query system (Parser, Planner, Aggregator, Runner)
- HTTP handlers (Write, Query, Metadata)
- Index system (distributed NativeIndex, per-shard metadata)
- End-to-end integration tests

### API Integration Tests

The `test_api/` directory contains JavaScript-based API integration tests:

```bash
# Start the TimeStar server first (from build directory)
./build/bin/timestar_http_server --port 8086

# In another terminal, navigate to test_api directory
cd test_api/

# Install dependencies (first time only)
npm install

# Run Jest test suite (161 tests across 10 suites)
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
- **C++ Tests**: 3178+ tests across 254+ test suites, all passing
- **Jest Tests**: 161 tests from 10 test suites, all passing
- **Standalone Tests**: 8 tests, all passing

## Key Executables

- `build/bin/timestar_http_server` - HTTP API server with JSON write endpoint
- `build/bin/timestar_benchmark` - General benchmark tool
- `build/bin/timestar_insert_bench` - Insert throughput benchmark
- `build/bin/timestar_query_bench` - Query throughput benchmark
- `build/bin/expression_benchmark` - Expression evaluation benchmark
- `build/bin/forecast_benchmark` - Forecasting benchmark
- `build/test/timestar_unit_test` - Unit tests using Google Test
- `build/test/timestar_test` - Full test suite (unit + integration)

## Architecture

### Core Components

The codebase is organized around these key abstractions:

1. **Engine** (`lib/core/engine.hpp/cpp`) - Main orchestrator that coordinates TSM files and WAL operations across shards
   - Manages TSMFileManager and WALFileManager
   - Handles inserts, queries, and memory store rollovers
   - Implements sharding with per-shard data directories

2. **TSM Files** (`lib/storage/tsm.hpp/cpp`) - Time-Structured Merge tree storage format
   - Immutable files containing compressed time series data
   - Indexed by series ID with min/max time bounds
   - Supports Float, Boolean, and String value types
   - Files ranked by tier number and sequence number for compaction
   - String values compressed using zstd compression with variable-length prefixes

3. **WAL** (`lib/storage/wal.hpp/cpp`) - Write-Ahead Log for durability
   - Ensures data persistence before acknowledgment
   - Used to recover in-memory stores on restart
   - Supports write, delete, and delete-range operations

4. **Memory Store** (`lib/storage/memory_store.hpp/cpp`) - In-memory buffer for recent writes
   - Holds data before flushing to TSM files
   - Provides fast reads for recent data
   - Rolled over periodically to create new TSM files

5. **Encoders** - Compression algorithms for efficient storage:
   - `integer_encoder` - Integer compression using Simple8b
   - `float_encoder` - Float compression using ALP (Adaptive Lossless floating-Point)
   - `bool_encoder` - Boolean value compression
   - `string_encoder` - String compression using zstd with variable-length prefixes
   - Timestamps share the integer FFOR path (Simple8b family)

6. **Query Runner** (`lib/query/query_runner.hpp/cpp`) - Executes queries across TSM files and memory stores
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
- Glaze (fetched automatically for JSON parsing)
- CRoaring (fetched automatically for roaring bitmap postings)
- Zstd (fetched automatically for compression)
- Threads library

## Data Storage

The database creates shard directories (`shard_0`, `shard_1`, etc.) under the directory configured by the `[server] data_dir` key in `timestar.toml` (or the `TIMESTAR_DATA_DIR` environment variable). The path may be absolute or relative to the server's working directory and is created on startup if missing; the default is `"."`, i.e. the working directory (the build directory when started from there). Each shard directory contains:
- `wal/` - Write-ahead log files
- `tsm/` - TSM data files
- `native_index/` - NativeIndex files (SSTable + WAL) for metadata and series lookup

## String Type Implementation

The TimeStar includes full support for string time series data with the following implementation:

### String Encoder (`lib/encoding/string_encoder.hpp/cpp`)

- **Compression**: Uses zstd compression for efficient string storage
- **Encoding Format**: Variable-length prefixes followed by compressed string data
- **API**: 
  - `encode(values, buffer)` - Encodes vector of strings into compressed buffer
  - `decode(slice, count, values)` - Decodes compressed data back to string vector

### TSM String Support

- **Type System**: String values use `TSMValueType::String` enum
- **Template Support**: TSM read/write operations fully templated for `std::string`
- **Query Integration**: String queries supported through `TSMResult<std::string>`
- **Memory Store**: In-memory string data before TSM flush
- **File Format**: String blocks stored with zstd compression in TSM files

### Testing Framework

Due to Seastar's architecture limitation (single `app_template` per process), string tests are structured as:

- **StringEncoder Tests**: Unit tests for encode/decode operations (10 tests)
- **TSM String Tests**: Integration tests with actual file I/O (7 tests)  
- **Test Runner**: `test/run_string_tests.sh` script runs tests safely to avoid segfaults

### Features Verified

- ✅ String encoding/decoding with zstd compression
- ✅ TSM file read/write operations  
- ✅ Memory store integration
- ✅ Mixed data type support (float, bool, string)
- ✅ Time-range queries
- ✅ Large dataset handling (25K+ entries)
- ✅ Special character support (UTF-8, JSON, paths)
- ✅ Multi-block data spanning

## Distributed Index System

The TimeStar uses a Seastar-native NativeIndex (LSM-tree based) for metadata, with each shard maintaining its own index co-located with its data. Schema metadata (measurements, fields, tags) is broadcast to all shards via `invoke_on_all` for local cache reads.

### Index Architecture (`lib/index/native/native_index.hpp/cpp`)

- **Per-Shard Storage**: Each shard maintains its own NativeIndex in `shard_N/native_index/`
- **Co-located Metadata**: Series metadata lives on the same shard as the series data
- **Schema Broadcast**: Field/tag/type metadata is broadcast to all shards for local reads
- **No Shard-0 Bottleneck**: All index operations are local — no cross-shard RPCs for metadata
- **Key Encoding**: Different index types use prefixed keys for separation:
  - `0x01`: Series mapping (`measurement+tags+field → series_id`)
  - `0x02`: Measurement fields (`measurement → [field1, field2, ...]`)
  - `0x03`: Measurement tags (`measurement → [tag_key1, tag_key2, ...]`)
  - `0x04`: Tag values (`measurement+tag_key → [value1, value2, ...]`)
  - `0x05`: Series metadata (`series_id → metadata`)
  - `0x06`: Tag index (`measurement+tag_key+tag_value → series_ids`)
  - `0x07`: Group-by index (`measurement+tag_key+tag_value → series_ids`)
  - `0x08`: Field stats (`series_id+field → stats`)
  - `0x09`: Field type (`measurement+field → field type`)
  - `0x0A`: Measurement series (`measurement+\0+series_id → empty, for fast lookup`)
  - `0x0B`: Retention policy (`measurement → JSON retention policy`)
  - `0x0D`: Time-series day bitmap (`measurement+\0+day(4B BE, big-endian for lexicographic ordering) → roaring bitmap of active local IDs`)

### Features

- **Series ID Generation**: Automatic numeric series ID assignment for efficient storage
- **Metadata Indexing**: Fast lookup of measurements, fields, and tag key/values
- **Batch Operations**: Efficient bulk indexing during data ingestion
- **Zstd Compression**: Built-in zstd compression for space efficiency
- **Bloom Filters**: Optimized point lookups with configurable bloom filter

### Engine Integration

- **Local Indexing**: Each shard indexes its own metadata via `Engine::insert()` — no cross-shard forwarding
- **Schema Broadcast**: `indexMetadataSync()` dispatches metadata to owning shards, then broadcasts schema changes to all shards via `broadcastSchemaUpdate()`
- **Scatter-Gather Queries**: Query handler discovers series on each shard locally, then merges results
- **Local Metadata API**: `/measurements`, `/tags`, `/fields` endpoints read from local schema caches — no shard-0 bottleneck
- **Query Support**: `queryBySeries()` and structured queries work with local index
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

### Write Endpoint (`lib/http/http_write_handler.hpp/cpp`)

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

- **Type Detection**: Automatic detection of field types (float, bool, string, int) from the JSON token shape (`10` → int, `10.0` → float). An optional per-point `"field_types": {"<field>": "float"|"int"|"bool"|"string"}` object overrides detection — important for JS clients, whose serializers emit `10.0` as `10`. See docs/api-write.md "Explicit field types"
- **Index Integration**: Automatic series ID generation and metadata indexing
- **Error Handling**: Detailed JSON error responses with HTTP status codes
- **Batch Support**: Efficient batch writes with partial failure handling
- **Default Timestamps**: Auto-generates nanosecond timestamps if not provided
- **Duplicate points overwrite (last-write-wins)**: rewriting an identical measurement+tags+field+timestamp REPLACES the earlier point (InfluxDB-compatible). Queries only ever see the newest write — raw reads return one point and aggregations count it once, regardless of placement (same batch, memory store, across flush, across TSM files, after compaction)

### Additional Endpoints

- `GET /health` - Health check endpoint
- `POST /query` - Query endpoint for time series data retrieval
- `GET /measurements` - List measurements
- `GET /tags` - Get tag keys and values
- `GET /fields` - Get field names and types
- `GET /cardinality` - Estimate series cardinality for a measurement (HyperLogLog estimate; optional `tag_key`/`tag_value` pair)
- `POST /delete` - Delete data by series, pattern, or time range
- `POST /subscribe` - SSE streaming subscriptions
- `GET /subscriptions` - List active subscriptions
- `PUT/GET/DELETE /retention` - Retention and downsampling policies
- `POST /derived` - Derived queries, anomaly detection, forecasting

### Testing

#### C++ Tests
The `test/` directory contains all C++ unit and integration tests:
```bash
# Run all tests
ctest
# Or run the test binary directly
./test/timestar_unit_test

# Run a specific test
./test/timestar_unit_test --gtest_filter=TestName*
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
   - `count` - Count of values
   - `latest` - Most recent value
   - `first` - First value
   - `median` - Median value
   - `stddev` - Standard deviation
   - `stdvar` - Standard variance
   - `spread` - Difference between max and min

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
   - Supports exact match, wildcards (`*`, `?`), and regex (`~pattern` or `/pattern/`)
   - Empty braces `{}` for no filtering
   - Example: `{location:us-west,sensor:temp-01}` or `{host:server-*}` or `{host:~server-[0-9]+}`

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
   - Bare numeric strings (no unit suffix) are interpreted as nanoseconds —
     `"300000000000"` == `300000000000` == `"300000000000ns"`. This applies to
     every transport (JSON numeric, JSON string, and the protobuf QueryRequest
     `aggregation_interval` string field), so all forms agree.
   - When specified, results are grouped into time buckets
   - Aggregation method applies within each bucket

### Non-Numeric Fields in Queries (canonical semantics)

**String and boolean** fields are non-numeric: they never aggregate
numerically and are always returned in the type they were written in (booleans
as JSON `true`/`false`, never `1`/`0`). The aggregation method named in the
query is **ignored** for them; they are always included, never silently
dropped:

- **No aggregationInterval**: non-numeric fields pass through raw — every
  stored (timestamp, value) pair in the requested range is returned unchanged.
  This holds for every aggregation method (including `latest`/`first`, which
  still collapse numeric fields to a single point) and for group-by queries
  (non-numeric series are returned per-series with their full tag set; they are
  not merged into groups).
- **With aggregationInterval**: non-numeric fields are reduced to
  **LATEST-per-bucket** — one value per epoch-aligned bucket
  (`bucketStart = ts / interval * interval`, same bucket layout as numeric
  aggregation), where the value is the one with the greatest timestamp inside
  the bucket. Returned timestamps are bucket starts, matching numeric fields.
- These rules are independent of data placement (memory store vs TSM) and of
  the internal query plan (pushdown / streaming / fallback). Both pushdown
  paths refuse non-numeric series so placement cannot change the answer.

Booleans were previously coerced to `1.0`/`0.0` and aggregated arithmetically,
which also dropped their series tags (only numeric fields route through the
aggregator, which keeps group-by tags only). They now follow the string rule
exactly — see `test/unit/http/dynamo_equivalence_test.cpp`.

`timestar::isNonNumericValueType()` (lib/storage/tsm.hpp) is the single
definition of this rule at the `TSMValueType` level: every gate that keeps
these types off a numeric path calls it rather than spelling out
`String || Boolean`. Two related spellings are NOT yet unified and remain
drift risks: the variant-level checks in `http_query_handler.cpp` (a
`FieldValues` alternative, not a `TSMValueType`), and the `String`-only checks
inside `TSM::aggregateSeries*` (`tsm.cpp`), which still accept Boolean and
would fold it as `1.0`/`0.0` — safe today only because every caller gates
first (`query_runner.cpp`, `engine.cpp`). A new caller that forgets the gate
reintroduces bool-as-1/0.

The rule holds on **every** path that returns stored values, not just
`POST /query`:

- **SSE `/subscribe` with an aggregationInterval** (`streaming_aggregator.cpp`):
  non-numeric buckets reduce to LATEST-per-bucket in the written type, so a live
  stream and a backfill of the same window agree. Booleans used to fold as
  `1.0`/`0.0` (a 5m `avg` streamed `0.6` where `/query` said `true`) and strings
  were dropped from the stream entirely unless the method was COUNT.
- **Formula arithmetic** (`/derived`, streaming derived, SSE formulas) is
  numeric-only, so a non-numeric operand is rejected rather than coerced:
  `DerivedQueryExecutor` throws for booleans exactly as it always did for
  strings, and the streaming formula paths map non-numeric to NaN (= missing,
  see docs/nan_policy.md) via the single `timestar::streamingValueAsNumeric()`
  helper. Coercing booleans to `1.0`/`0.0` let a formula compute over a type the
  query path refuses to aggregate — and with an interval it silently ran on
  LATEST-per-bucket values rather than the every-point series the author meant.

Response shape is also type-independent: non-numeric results are appended
BEFORE `consolidateSeriesFields()`, so "one series per measurement+tags" holds
for every field type. (Under a `by {tags}` clause a non-numeric field is still
returned per-series with its full tag set, so it does not merge into the grouped
numeric series — grouping does not apply to it.)

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

### Aggregation Result Shape (canonical semantics)

The shape of a query result is a pure function of the query — it must never
depend on where the data happens to live (memory store vs TSM), on cache
warm-up, or on startTime alignment.

The governing rule: **aggregate ACROSS SERIES at equal timestamps; never
aggregate over time unless the caller asks.** Grouping and temporal bucketing
are orthogonal — a `by {tags}` clause must never change the time axis. Only an
`aggregationInterval` (or `latest`/`first`, which name a single point by
definition) reduces the time axis.

**With `aggregationInterval` (> 0):**
- Buckets are **epoch-aligned**: each point lands in bucket
  `floor(timestamp / interval) * interval`. Bucket boundaries do NOT shift
  with the query's startTime.
- `endTime` is inclusive; a range that ends exactly on a bucket boundary
  includes that boundary's bucket.
- Empty buckets are omitted (no gap filling).
- A misaligned range shorter than one interval can still return two buckets
  when it crosses an epoch boundary.

**Without `aggregationInterval` (interval == 0):**
- `latest` / `first`: one collapsed value per series/group, on every path.
  These are the only methods that collapse a range by definition.
- Every other method: **per-timestamp aggregation across matching series** —
  N points, one per distinct timestamp, aggregated across the series that
  share that timestamp (raw passthrough for a single series). This holds
  **with or without `by {tags}`**: grouping only decides *which* series fold
  together, never how many timestamps survive. Internal consumers (`/derived`
  formulas, anomaly detection, forecasting) rely on this N-point shape for
  their no-interval sub-queries.

A `by {tags}` clause used to silently switch the interval == 0 default to
"collapse the whole range to one point per group" (InfluxQL-style "GROUP BY
tag"), so two queries differing only by a grouping clause disagreed on the time
axis. That is fixed — pinned by
`DynamoEquivalenceTest.GroupByDoesNotCollapseTheTimeAxis`.

**Grouping never changes the VALUES either**, only which series fold together.
A method whose fold-of-one is not the identity — `spread`/`stddev`/`stdvar` are
0 over a single value, not the value — must be folded through an
`AggregationState`, never short-circuited from raw values. `timestar::
methodCanFoldRaw()` (aggregator.hpp) is the single definition of that rule;
every site that emits raw values into a response must consult it. Skipping it
made `spread:m(v){}` and `spread:m(v){} by {p}` disagree, and made the answer
depend on shard fan-out (single-shard finalize vs multi-shard merge).

Implementation note: the interval == 0 collapse-vs-raw choice is carried by
the `foldNoInterval` parameter of `Engine::queryAggregated()` /
`QueryRunner::queryTsmAggregated()`. It **defaults to `false`** — the shape
rules forbid collapsing a range the caller did not ask to collapse, so no
production caller passes `true` (only `non_bucketed_pushdown_test.cpp`, which
pins the capability itself). `latest`/`first` collapse inside the runner
regardless of the flag, which is the only collapse the shape rules allow.

`HttpQueryHandler::executeQuery()` takes its `QueryRequest` **by value**: it is
a coroutine, and the SSE backfill loop passes a loop-body local through
`seastar::with_timeout`, which does NOT cancel the inner future — aliasing the
caller's request is a use-after-free once a backfill times out.

**Unknown `by {tag}` keys:** a grouping key that no discovered series carries
returns `[]`, matching the scope path (an unknown scope key finds no postings
bitmap, hence no series). Silently ignoring it answered `by {devicId}` with a
plausible-but-wrong fleet-wide aggregate. Validated against the discovered
series rather than the tag index, so a schema broadcast still in flight cannot
turn a valid grouping into an empty result.

### Special Float Values (canonical semantics)

Aggregation results are placement-independent: memory store, TSM block-stats
pushdown, SIMD folds, and scalar folds all agree. Full policy in
`docs/nan_policy.md`.

- **NaN = missing data.** Skipped by EVERY aggregation method on EVERY path:
  `count` counts only non-NaN values, `avg = sum-of-non-NaN / count-of-non-NaN`.
  Raw (non-aggregated) reads return NaN verbatim (serialized as JSON `null`;
  protobuf carries it natively). TSM float block stats (including
  `blockCount`) are NaN-skipped at write time; blocks containing NaN carry no
  extended stats (see `docs/tsm_format.md`).
- **±Infinity is valid data.** Raw reads return it exactly. Aggregations let
  it participate arithmetically: SUM/AVG propagate (`+Inf + -Inf = NaN` is
  the correct IEEE aggregate), MIN/MAX order it, COUNT counts it. Min/max
  identities are ±infinity, and Kahan sums reset their compensation term when
  the sum is non-finite (otherwise Inf silently degrades to NaN).
- **-0.0 round-trips raw reads bit-exactly** (ALP stores NaN/±Inf/-0.0 as
  raw-bit exceptions). Aggregated results may normalize -0.0 to +0.0 (IEEE
  addition/comparison semantics) — documented behaviour, not a bug.

### Query Features

- **Multi-shard coordination**: Queries automatically fan out to all relevant shards
- **Parallel execution**: Each shard processes queries independently
- **Result aggregation**: Results merged and aggregated from all shards
- **Time filtering**: Efficient time-based data retrieval using TSM indexes
- **Tag filtering**: Fast lookups using NativeIndex for series discovery

### Performance Considerations

- Queries with specific tag filters (scopes) are faster as they limit series scanning
- Time ranges should be as narrow as possible for optimal performance
- Aggregations reduce data transfer and memory usage
- Group-by operations may increase result size significantly

### Future Query Enhancements

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
