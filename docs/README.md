# TSDB Documentation

This directory contains technical documentation for the TSDB project.

## Contents

### Query System Documentation

- **[QUERY_IMPLEMENTATION_PLAN.md](QUERY_IMPLEMENTATION_PLAN.md)** - Detailed plan for implementing the query system, including parser, planner, and execution components.

- **[QUERY_EXECUTION_PLAN.md](QUERY_EXECUTION_PLAN.md)** - Architecture and execution flow for query processing across shards.

### Seastar Framework Integration

- **[SEASTAR_TESTING.md](SEASTAR_TESTING.md)** - Guide for writing and running tests with Seastar's async/coroutine framework.

- **[SEASTAR_GTEST_INTEGRATION.md](SEASTAR_GTEST_INTEGRATION.md)** - Integration strategy for using Google Test with Seastar-based code.

- **[SEASTAR_TEST_INTEGRATION_SUMMARY.md](SEASTAR_TEST_INTEGRATION_SUMMARY.md)** - Summary of test integration approaches and best practices.

### Storage System Documentation

- **[TSM_TOMBSTONE_PLAN.md](TSM_TOMBSTONE_PLAN.md)** - Design and implementation plan for the TSM tombstone system used for logical deletion of time series data.

## Additional Resources

- **Parent [README.md](../README.md)** - Main project documentation
- **[CLAUDE.md](../CLAUDE.md)** - Development guidance for Claude Code
- **[Test Documentation](../test/README.md)** - Test suite organization and usage
- **[HTTP API Tests](../test/http_api_tests/README.md)** - Python-based API test documentation

## Architecture Diagrams

```
Data Write Path:
HTTP Request → Engine → WAL → Memory Store → TSM File (on flush)
                     ↓
                Index Update (LevelDB)

Query Path:
HTTP Request → Query Parser → Query Planner → Query Runner
                                            ↓
                            Memory Store + TSM Files
                                            ↓
                                    Aggregator → Response
```

## Key Concepts

### TSM (Time-Structured Merge)
- Immutable file format for time series data
- Organized by series key with time-ordered blocks
- Supports multiple compression algorithms
- Tiered compaction strategy

### WAL (Write-Ahead Log)
- Ensures durability before acknowledgment
- Enables recovery of in-memory data
- Supports write, delete, and delete-range operations

### Sharding
- Per-core data isolation using Seastar
- Series keys distributed via consistent hashing
- Parallel query execution across shards

### Indexing
- LevelDB for metadata storage
- Series ID mapping for efficient lookups
- Tag and field indexing for query optimization