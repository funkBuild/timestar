#!/bin/bash
# ab_bigq.sh <datadir> <rounds> <iters> <json-body> : base vs new server, large JSON query; server CPU/query, TTFB, total.
S=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
BASE=${BASE_BIN:-/home/matt/Desktop/source/tsdb/build/perf-baseline-round2/timestar_http_server}; NEW=/home/matt/Desktop/source/tsdb/build/bin/timestar_http_server
dir=$1; rounds=$2; iters=$3; body=$4; tck=$(getconf CLK_TCK); : > $S/bench/ab_bigq_last.txt
for r in $(seq 1 $rounds); do
  order="base new"; [ $((r % 2)) = 0 ] && order="new base"
  for v in $order; do
    bin=$BASE; [ $v = new ] && bin=$NEW
    pid=$(SRV_CPUS=${SRV_CPUS:-20-23} $S/bench/srv.sh start $bin $dir 18186) || exit 1
    for i in 1 2 3; do curl -s -o /dev/null -X POST -d "$body" http://127.0.0.1:18186/query; done
    c0=$(awk -v t=$tck '{print ($14+$15)/t}' /proc/$pid/stat)
    lat=$(for i in $(seq 1 $iters); do taskset -c 14 curl -s -o /dev/null -w "%{time_starttransfer} %{time_total} %{size_download}\n" -X POST -d "$body" http://127.0.0.1:18186/query; done | sort -n -k2 | awk '{a[NR]=$1; b[NR]=$2; s=$3} END {printf "%.2f %.2f %d", a[int((NR+1)/2)]*1000, b[int((NR+1)/2)]*1000, s}')
    c1=$(awk -v t=$tck '{print ($14+$15)/t}' /proc/$pid/stat)
    $S/bench/srv.sh stop $pid
    echo "$v $(echo "$c0 $c1 $iters" | awk '{printf "%.2f", ($2-$1)*1000/$3}') $lat" >> $S/bench/ab_bigq_last.txt
  done
done
python3 - <<PY
import statistics
rows=[l.split() for l in open('$S/bench/ab_bigq_last.txt')]
for m in ('base','new'):
    r=[x for x in rows if x[0]==m]
    print('%-5s server CPU %.2f ms/query | ttfb %.2f ms | total %.2f ms | bytes %s' % (m, statistics.median(float(x[1]) for x in r), statistics.median(float(x[2]) for x in r), statistics.median(float(x[3]) for x in r), r[0][4]))
PY
