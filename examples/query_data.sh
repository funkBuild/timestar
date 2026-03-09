#!/bin/bash
# Example: Querying data from TimeStar
# Usage: ./query_data.sh

TIMESTAR_HOST="${TIMESTAR_HOST:-localhost}"
TIMESTAR_PORT="${TIMESTAR_PORT:-8086}"

# Calculate time range (last hour)
END_TIME=$(date +%s%N)
START_TIME=$((END_TIME - 3600000000000))  # 1 hour ago

echo "Querying data from TimeStar at ${TIMESTAR_HOST}:${TIMESTAR_PORT}"
echo ""

# Simple query - get all temperature data
echo "=== Query 1: All temperature data ==="
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature()",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""

# Filtered query - get CPU data for specific host
echo "=== Query 2: CPU usage for server-01 ==="
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:cpu(usage_percent){host:server-01}",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""

# Query with specific fields
echo "=== Query 3: Memory usage ==="
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "latest:memory(used_gb,total_gb)",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .
