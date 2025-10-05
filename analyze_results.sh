#!/bin/bash

echo "=== METADATA CACHE ANALYSIS ==="
echo

# Count cache hits and misses
cache_misses=$(grep -c "Cache miss for series" server.log 2>/dev/null || echo "0")
cache_hits=$(grep -c "Cache hit for series" server.log 2>/dev/null || echo "0")
total_lookups=$((cache_hits + cache_misses))

echo "Cache Statistics:"
echo "  Cache hits: $cache_hits"
echo "  Cache misses: $cache_misses"  
echo "  Total lookups: $total_lookups"
if [ $total_lookups -gt 0 ]; then
  hit_rate=$((cache_hits * 100 / total_lookups))
  echo "  Hit rate: ${hit_rate}%"
fi
echo

# Analyze HTTP performance
echo "=== HTTP HANDLER PERFORMANCE ===" 
echo

# Extract metadata times
grep "\\[PERF\\] \\[HTTP\\] processMultiWritePoint breakdown" server.log 2>/dev/null | \
  awk -F'Metadata: |μs' '{print $2}' | \
  awk '{sum+=$1; count++; if(NR==1 || $1<min) min=$1; if(NR==1 || $1>max) max=$1} 
       END {if(count>0) printf "Metadata indexing time:\n  Count: %d\n  Average: %.2fμs\n  Min: %.2fμs\n  Max: %.2fμs\n  Total: %.2fms\n", count, sum/count, min, max, sum/1000}'

echo
echo "Overall HTTP request time:"
grep "\\[PERF\\] \\[HTTP\\] processMultiWritePoint breakdown" server.log 2>/dev/null | \
  awk -F'Total: |μs' '{print $2}' | \
  awk '{sum+=$1; count++; if(NR==1 || $1<min) min=$1; if(NR==1 || $1>max) max=$1} 
       END {if(count>0) printf "  Count: %d requests\n  Average: %.2fμs\n  Min: %.2fμs\n  Max: %.2fμs\n  Total: %.2fms\n", count, sum/count, min, max, sum/1000}'

echo
