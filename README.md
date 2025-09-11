# TSDB - Time Series Database

A high-performance time series database built with C++23 and the Seastar framework, implementing the TSM (Time-Structured Merge) storage engine.

## Features

- **TSM Storage Engine**: Efficient time-structured merge tree for time series data
- **Multi-Type Support**: Float, Boolean, String, and Integer value types
- **Compression**: Multiple compression algorithms (Simple8b, Simple16, XOR, Snappy)
- **HTTP API**: InfluxDB-compatible write and query endpoints
- **Sharding**: Per-core sharding using Seastar framework
- **WAL**: Write-ahead logging for durability
- **Compaction**: Background compaction for storage optimization
- **Indexing**: LevelDB-based metadata indexing for fast lookups

## Building

### Prerequisites

- GCC 14+ or GCC 13 (with limited C++20 support)
- CMake 3.10+
- Seastar framework (included as submodule)
- LevelDB development libraries
- Snappy compression library

### Ubuntu/Debian Setup

```bash
# Install dependencies
sudo apt install cmake g++ libleveldb-dev libsnappy-dev

# Clone and build
git clone --recursive https://github.com/yourusername/tsdb.git
cd tsdb
mkdir build && cd build
cmake ..
make -j$(nproc)
```

## Running

### Start the HTTP Server

```bash
# Default port 8086
./bin/tsdb_http_server

# Custom port
./bin/tsdb_http_server --port 9000
```

### Write Data

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"location": "us-west"},
    "fields": {"value": 23.5},
    "timestamp": 1704067200000000000
  }'
```

### Query Data

```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value){location:us-west}",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Project Structure

```
tsdb/
├── lib/                 # Core library implementation
│   ├── engine.*        # Main database engine
│   ├── tsm.*           # TSM file format
│   ├── wal.*           # Write-ahead log
│   ├── memory_store.*  # In-memory buffer
│   ├── *_encoder.*     # Compression algorithms
│   └── http_*_handler.* # HTTP API handlers
├── bin/                 # Executable sources
│   └── tsdb_http_server.cpp
├── test/               # Test suites
│   ├── unit/           # Unit tests
│   ├── integration/    # Integration tests
│   ├── e2e/           # End-to-end tests
│   ├── performance/   # Performance benchmarks
│   └── http_api_tests/ # Python HTTP API tests
├── docs/               # Documentation
├── external/           # External dependencies
│   └── seastar/       # Seastar framework
└── CLAUDE.md          # Claude Code instructions
```

## Testing

### Run C++ Tests

```bash
# All tests
./test/tsdb_test

# Specific category
make test-unit
make test-integration
make test-e2e
make test-perf

# Specific test suite
./test/tsdb_test --gtest_filter="TSMTest.*"
```

### Run HTTP API Tests

```bash
cd test/http_api_tests
python test_http_write.py
python test_query_e2e.py
python test_e2e_quick.py
```

## Documentation

- [CLAUDE.md](CLAUDE.md) - Development guidance for Claude Code
- [docs/](docs/) - Technical documentation
  - Query implementation and execution plans
  - Seastar integration details
  - TSM tombstone design

## Architecture

The database uses a multi-tier architecture:

1. **HTTP Layer**: RESTful API for writes and queries
2. **Engine Layer**: Coordinates storage and retrieval across shards
3. **Storage Layer**: TSM files, WAL, and in-memory stores
4. **Index Layer**: LevelDB for metadata and series lookup
5. **Compression Layer**: Various encoding algorithms for efficiency

Data flow:
- Writes → WAL → Memory Store → TSM Files (on rollover)
- Queries → Memory Store + TSM Files → Aggregation → Response
- Background → Compaction of TSM files

## Performance

- Designed for high write throughput (100k+ points/second)
- Sub-millisecond query latency for recent data
- Efficient compression (typical 10:1 ratio)
- Scales with CPU cores via Seastar sharding

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]