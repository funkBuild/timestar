#!/bin/bash
now=$(date +%s)000000000
year_ago=$((now - 525600 * 60 * 1000000000))

echo "Testing baseline aggregation performance..."
echo "Query: avg by rack with 12h intervals"

for i in {1..3}; do
  echo -n "Run $i: "
  curl -s -X POST http://localhost:8086/query \
    -H "Content-Type: application/json" \
    -d "{
      \"query\": \"avg:server.metrics(cpu_usage,memory_usage,network_in,network_out){} by {rack}\",
      \"startTime\": $year_ago,
      \"endTime\": $now,
      \"aggregationInterval\": \"12h\"
    }" | jq -r '.statistics.executionTimeMs'
  sleep 1
done
