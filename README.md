# TSDB

A high-performance time series database built with C++23 and the Seastar framework.

[![CI](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)

## Features

**Storage Engine**
- TSM (Time-Structured Merge) files with tiered compaction
- Write-ahead log for crash recovery
- In-memory buffer for fast recent reads
- Per-measurement retention policies with downsampling

**Data Types & Compression**
- Float, integer, boolean, and string value types
- ALP + Gorilla XOR float compression (~44% better than XOR alone)
- Simple8b integer compression
- Snappy string compression
- XOR delta-of-delta timestamp compression

**Query System**
- 11 aggregation methods (avg, min, max, sum, count, latest, first, median, stddev, stdvar, spread)
- Time-bucketed aggregation with flexible interval syntax
- Tag filtering with exact match, wildcards, and regex
- Group-by on arbitrary tag keys
- 50+ expression functions (math, transforms, rolling windows, cross-series, gap-fill, smoothing)
- Derived queries combining multiple sub-queries with formulas

**Analytics**
- Anomaly detection (basic rolling window, agile SARIMA, robust STL)
- Forecasting (linear regression, seasonal STL with auto-periodicity detection)
- Confidence intervals and anomaly scoring

**Streaming**
- Real-time Server-Sent Events (SSE) subscriptions
- Multi-query streaming with cross-query formulas
- Optional historical backfill on connect

**Infrastructure**
- Shard-per-core via Seastar (lock-free, linear scaling)
- LevelDB metadata indexing
- TOML configuration with Seastar tuning passthrough
- React dashboard frontend

## Quick Start

### Build

```bash
# Install dependencies (Ubuntu/Debian)
sudo apt install cmake g++-14 libleveldb-dev libsnappy-dev libssl-dev \
  libboost-all-dev liblz4-dev libgnutls28-dev libsctp-dev libhwloc-dev \
  libnuma-dev libpciaccess-dev libcrypto++-dev libxml2-dev xfslibs-dev \
  systemtap-sdt-dev libyaml-cpp-dev libxxhash-dev ragel

# Clone and build
git clone --recursive https://github.com/yourusername/tsdb.git
cd tsdb
mkdir -p build && cd build
cmake ..
make -j$(nproc)
```

### Run

```bash
./bin/tsdb_http_server --port 8086
```

### Write Data

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"location": "us-west", "sensor": "temp-01"},
    "fields": {"value": 23.5, "humidity": 65.0},
    "timestamp": 1704067200000000000
  }'
```

### Query Data

```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value){location:us-west} by {sensor}",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

### Derived Query with Anomaly Detection

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": [{"query": "avg:temperature(value){location:us-west}", "name": "a"}],
    "formula": "anomalies(a, '\''basic'\'', 2)",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Configuration

Generate a default TOML config and customize it:

```bash
./bin/tsdb_http_server --dump-config > tsdb.toml
./bin/tsdb_http_server --config tsdb.toml
```

Key config sections: `[server]`, `[storage]`, `[http]`, `[index]`, `[engine]`, `[streaming]`, `[seastar]`. See [docs/architecture.md](docs/architecture.md) for details.

## API Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/write` | [Ingest time series data](docs/api-write.md) |
| POST | `/query` | [Query with aggregation and filtering](docs/api-query.md) |
| POST | `/derived` | [Derived queries, anomaly detection, forecasting](docs/api-derived.md) |
| POST | `/delete` | [Delete data by series, pattern, or time range](docs/api-delete.md) |
| GET | `/measurements` | [List measurements](docs/api-metadata.md) |
| GET | `/tags` | [Get tag keys and values](docs/api-metadata.md) |
| GET | `/fields` | [Get field names and types](docs/api-metadata.md) |
| POST | `/subscribe` | [SSE streaming subscriptions](docs/api-streaming.md) |
| GET | `/subscriptions` | [List active subscriptions](docs/api-streaming.md) |
| PUT/GET/DELETE | `/retention` | [Retention and downsampling policies](docs/api-retention.md) |
| GET | `/health` | [Health check](docs/api-health.md) |

## Query Language

```
method:measurement(fields){scopes} by {groupKeys}
```

**Methods:** `avg`, `min`, `max`, `sum`, `count`, `latest`, `first`, `median`, `stddev`, `stdvar`, `spread`

**Scopes:** exact (`host:server-01`), wildcard (`host:server-*`), regex (`host:~server-[0-9]+`)

**Intervals:** `"5m"`, `"1h"`, `"30s"`, `"1.5d"` (units: ns, us, ms, s, m, h, d)

See [docs/query-language.md](docs/query-language.md) and [docs/expression-functions.md](docs/expression-functions.md).

## Project Structure

```
tsdb/
├── lib/                    Core library
│   ├── core/              Engine orchestration
│   ├── storage/           TSM, WAL, MemoryStore
│   ├── encoding/          Compression (float, int, bool, string, timestamp)
│   ├── query/             Query parser, runner, aggregator, expressions
│   │   ├── anomaly/       Anomaly detection algorithms
│   │   ├── forecast/      Forecasting and STL decomposition
│   │   └── transform/     SIMD-optimized transform functions
│   ├── http/              HTTP API handlers
│   ├── index/             LevelDB metadata index
│   ├── config/            TOML configuration
│   ├── retention/         Retention policy enforcement
│   └── utils/             Logging, signals
├── bin/                    Server and benchmark executables
├── test/                   C++ unit tests (Google Test)
├── test_api/               JavaScript API integration tests
├── frontend/               React dashboard
├── docs/                   Documentation
└── external/               Seastar submodule
```

## Testing

### C++ Unit Tests

```bash
cd build

# All tests (~2200 tests across 150+ suites)
./test/tsdb_test

# Specific suite
./test/tsdb_test --gtest_filter="QueryParserTest.*"

# List all tests
./test/tsdb_test --gtest_list_tests
```

### API Integration Tests

```bash
# Start the server first
./bin/tsdb_http_server --port 8086

# In another terminal
cd test_api && npm install && npm test
```

## Performance

Benchmarked on a 4-core system with 8 connections per shard:

- **Write throughput:** 11.8M points/sec (100M points, 10 fields, 10K batch size)
- **Median write latency:** 74ms
- **P99 write latency:** 405ms
- **Compression:** ~44% improvement with ALP over baseline XOR for floats

## License

[AGPL-3.0](LICENSE)
