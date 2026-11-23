#!/bin/bash
# ab_insert.sh <format> <batches> <pairs> <tag> [extra bench args]
S=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
BASE=${BASE_BIN:-/home/matt/Desktop/source/tsdb/build/perf-baseline-20261122/timestar_http_server}
NEW=/home/matt/Desktop/source/tsdb/build/bin/timestar_http_server
BENCH=/home/matt/Desktop/source/tsdb/build/bin/timestar_insert_bench
fmt=$1; batches=$2; pairs=$3; tag=$4; shift 4
mkdir -p $S/bench/res; : > $S/bench/res/ins_$tag.txt
tck=$(getconf CLK_TCK)
for i in $(seq 1 $pairs); do
  if [ $((i % 2)) = 1 ]; then order="base new"; else order="new base"; fi
  for v in $order; do
    bin=$BASE; [ $v = new ] && bin=$NEW
    rm -rf $S/bench/ins_dir
    pid=$($S/bench/srv.sh start $bin $S/bench/ins_dir 18186) || exit 1
    out=$(taskset -c 12-15 $BENCH -c ${CLIENT_SMP:-1} --server-port 18186 --format $fmt --batches $batches --batch-size ${BATCH_SIZE:-10000} --warmup 10 --connections 8 --hosts 10 --racks 2 --verify false "$@" 2>&1)
    thr=$(echo "$out" | grep Throughput | awk '{print $2}')
    ok=$(echo "$out" | grep 'Requests:' | sed 's/ *Requests: *//')
    cpu=$(awk -v t=$tck '{print ($14+$15)/t}' /proc/$pid/stat)
    rss=$(grep VmHWM /proc/$pid/status | awk '{print $2}')
    $S/bench/srv.sh stop $pid
    disk=$(du -sb $S/bench/ins_dir/shard_*/tsm 2>/dev/null | awk '{s+=$1} END {print s}')
    echo "$v $thr $cpu $rss $disk | $ok" | tee -a $S/bench/res/ins_$tag.txt
  done
done
python3 - <<PY
import statistics
rows=[l.split('|')[0].split() for l in open('$S/bench/res/ins_$tag.txt')]
for v in ('base','new'):
    r=[x for x in rows if x[0]==v]
    print(v, 'throughput median %.2fM pts/s' % (statistics.median(float(x[1]) for x in r)/1e6), 'server CPU-s median %.2f' % statistics.median(float(x[2]) for x in r), 'peak RSS MB %.0f' % (statistics.median(float(x[3]) for x in r)/1024), 'TSM bytes %.1f MB' % (statistics.median(float(x[4]) for x in r)/1e6))
PY
