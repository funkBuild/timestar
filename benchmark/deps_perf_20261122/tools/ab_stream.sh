#!/bin/bash
# ab_stream.sh <rounds> <iters> <query> [extra-json]: same binary, streamed vs buffered JSON responses.
S=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
B=${BIN:-/home/matt/Desktop/source/tsdb/build/bin/timestar_http_server}
rounds=$1; iters=$2; query=$3; extra=${4:-}
END=$((1000000000000000000 + 100*10000*60000000000*11)); tck=$(getconf CLK_TCK)
body="{\"query\":\"$query\",\"startTime\":1000000000000000000,\"endTime\":$END$extra}"
for r in $(seq 1 $rounds); do
  order="buffered streamed"; [ $((r % 2)) = 0 ] && order="streamed buffered"
  for mode in $order; do
    if [ $mode = buffered ]; then export TIMESTAR_HTTP_STREAM_RESPONSE_MIN_POINTS=0; else unset TIMESTAR_HTTP_STREAM_RESPONSE_MIN_POINTS; fi
    pid=$(SRV_CPUS=${SRV_CPUS:-20-23} $S/bench/srv.sh start $B $S/bench/q_prof 18186) || exit 1
    for i in 1 2 3; do curl -s -o /dev/null -X POST -d "$body" http://127.0.0.1:18186/query; done
    c0=$(awk -v t=$tck '{print ($14+$15)/t}' /proc/$pid/stat)
    lat=$(for i in $(seq 1 $iters); do taskset -c 14 curl -s -o /dev/null -w "%{time_starttransfer} %{time_total}\n" -X POST -d "$body" http://127.0.0.1:18186/query; done | sort -n -k2 | awk '{a[NR]=$1; b[NR]=$2} END {printf "%.2f %.2f", a[int((NR+1)/2)]*1000, b[int((NR+1)/2)]*1000}')
    c1=$(awk -v t=$tck '{print ($14+$15)/t}' /proc/$pid/stat)
    rss=$(grep VmHWM /proc/$pid/status | awk '{print $2}')
    $S/bench/srv.sh stop $pid
    echo "$mode $(echo "$c0 $c1 $iters" | awk '{printf "%.2f", ($2-$1)*1000/$3}') $lat $rss"
  done
done | tee /tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad/bench/ab_stream_last.txt | awk '{print}' > /dev/null
python3 - <<'PY'
import statistics
rows=[l.split() for l in open('/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad/bench/ab_stream_last.txt')]
for m in ('buffered','streamed'):
    r=[x for x in rows if x[0]==m]
    print('%-9s server CPU %.2f ms/query | ttfb %.2f ms | total %.2f ms | peak RSS %.0f MB' % (m, statistics.median(float(x[1]) for x in r), statistics.median(float(x[2]) for x in r), statistics.median(float(x[3]) for x in r), statistics.median(float(x[4]) for x in r)/1024))
PY
