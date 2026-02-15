#!/bin/bash
# Example: Writing data to TSDB
# Usage: ./write_data.sh

TSDB_HOST="${TSDB_HOST:-localhost}"
TSDB_PORT="${TSDB_PORT:-8086}"

echo "Writing sample data to TSDB at ${TSDB_HOST}:${TSDB_PORT}"

# Write a single point
curl -X POST "http://${TSDB_HOST}:${TSDB_PORT}/write" \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {
      "location": "us-west",
      "sensor": "temp-01"
    },
    "fields": {
      "value": 23.5,
      "humidity": 65.0
    },
    "timestamp": '$(date +%s%N)'
  }'

echo ""

# Write batch data
curl -X POST "http://${TSDB_HOST}:${TSDB_PORT}/write" \
  -H "Content-Type: application/json" \
  -d '{
    "writes": [
      {
        "measurement": "cpu",
        "tags": {"host": "server-01", "datacenter": "dc1"},
        "fields": {"usage_percent": 45.2, "idle_percent": 54.8},
        "timestamp": '$(date +%s%N)'
      },
      {
        "measurement": "cpu",
        "tags": {"host": "server-02", "datacenter": "dc1"},
        "fields": {"usage_percent": 62.1, "idle_percent": 37.9},
        "timestamp": '$(date +%s%N)'
      },
      {
        "measurement": "memory",
        "tags": {"host": "server-01", "datacenter": "dc1"},
        "fields": {"used_gb": 12.5, "total_gb": 32.0},
        "timestamp": '$(date +%s%N)'
      }
    ]
  }'

echo ""
echo "Data written successfully!"
