# TimeStar Examples

This directory contains example code demonstrating how to use TimeStar.

## Examples

### Basic Usage

- **write_data.sh** - Example of writing data to TimeStar via HTTP API
- **query_data.sh** - Example of querying data from TimeStar
- **aggregation_query.sh** - Example of aggregation queries

## Running the Examples

1. Start the TimeStar server:
   ```bash
   ./build/bin/timestar_http_server --port 8086
   ```

2. Run any example script:
   ```bash
   ./examples/write_data.sh
   ./examples/query_data.sh
   ```

## API Reference

See the [CLAUDE.md](../CLAUDE.md) file for detailed API documentation.
