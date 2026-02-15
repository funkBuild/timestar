# TSDB Examples

This directory contains example code demonstrating how to use TSDB.

## Examples

### Basic Usage

- **write_data.sh** - Example of writing data to TSDB via HTTP API
- **query_data.sh** - Example of querying data from TSDB
- **aggregation_query.sh** - Example of aggregation queries

## Running the Examples

1. Start the TSDB server:
   ```bash
   ./build/bin/tsdb_http_server --port 8086
   ```

2. Run any example script:
   ```bash
   ./examples/write_data.sh
   ./examples/query_data.sh
   ```

## API Reference

See the [CLAUDE.md](../CLAUDE.md) file for detailed API documentation.
