#!/bin/bash
# q_curl.sh <server-bin> <label> <json-body> [iters]: median TTFB / total / size via curl against a copy of q2
S=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
bin=$1; label=$2; body=$3; iters=${4:-25}
pid=$(SRV_CPUS=20-23 $S/bench/srv.sh start $bin $S/bench/q_deps 18186) || exit 1
for i in $(seq 1 $((iters+3))); do taskset -c 14 curl -s -o /dev/null -w "%{time_starttransfer} %{time_total} %{size_download}\n" -X POST -H 'Content-Type: application/json' -d "$body" http://127.0.0.1:18186/query; done | tail -n $iters | sort -n | awk -v l="$label" '{a[NR]=$1; b[NR]=$2; s=$3} END {printf "%-6s ttfb %.2f ms  total %.2f ms  bytes %d\n", l, a[int((NR+1)/2)]*1000, b[int((NR+1)/2)]*1000, s}'
$S/bench/srv.sh stop $pid
