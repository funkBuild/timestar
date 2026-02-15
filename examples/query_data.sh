#!/bin/bash
# Example: Querying data from TSDB
# Usage: ./query_data.sh

TSDB_HOST="${TSDB_HOST:-localhost}"
TSDB_PORT="${TSDB_PORT:-8086}"

# Calculate time range (last hour)
END_TIME=$(date +%s%N)
START_TIME=$((END_TIME - 3600000000000))  # 1 hour ago

echo "Querying data from TSDB at ${TSDB_HOST}:${TSDB_PORT}"
echo ""

# Simple query - get all temperature data
echo "=== Query 1: All temperature data ==="
curl -s -X POST "http://${TSDB_HOST}:${TSDB_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature()",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""

# Filtered query - get CPU data for specific host
echo "=== Query 2: CPU usage for server-01 ==="
curl -s -X POST "http://${TSDB_HOST}:${TSDB_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:cpu(usage_percent){host:server-01}",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""

# Query with specific fields
echo "=== Query 3: Memory usage ==="
curl -s -X POST "http://${TSDB_HOST}:${TSDB_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "latest:memory(used_gb,total_gb)",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .
