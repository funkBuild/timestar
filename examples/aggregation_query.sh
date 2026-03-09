#!/bin/bash
# Example: Aggregation queries in TimeStar
# Usage: ./aggregation_query.sh

TIMESTAR_HOST="${TIMESTAR_HOST:-localhost}"
TIMESTAR_PORT="${TIMESTAR_PORT:-8086}"

# Calculate time range (last 24 hours)
END_TIME=$(date +%s%N)
START_TIME=$((END_TIME - 86400000000000))  # 24 hours ago

echo "Running aggregation queries on TimeStar at ${TIMESTAR_HOST}:${TIMESTAR_PORT}"
echo ""

# Time-bucketed aggregation (5-minute intervals)
echo "=== Query 1: Average temperature in 5-minute buckets ==="
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value)",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME',
    "aggregationInterval": "5m"
  }' | jq .

echo ""

# Group-by aggregation
echo "=== Query 2: Average CPU by host (grouped) ==="
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:cpu(usage_percent) by {host}",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""

# Min/Max aggregation
echo "=== Query 3: Min and Max temperature ==="
echo "Min:"
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "min:temperature(value)",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""
echo "Max:"
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "max:temperature(value)",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME'
  }' | jq .

echo ""

# Hourly aggregation with grouping
echo "=== Query 4: Hourly average CPU by datacenter ==="
curl -s -X POST "http://${TIMESTAR_HOST}:${TIMESTAR_PORT}/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:cpu(usage_percent){} by {datacenter}",
    "startTime": '$START_TIME',
    "endTime": '$END_TIME',
    "aggregationInterval": "1h"
  }' | jq .
