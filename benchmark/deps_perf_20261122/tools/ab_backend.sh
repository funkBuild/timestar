#!/bin/bash
# ab_backend.sh <format> <batches> <rounds> <tag> <backend...>   — same (new) binary, different --reactor-backend
S=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
BIN=${BIN:-/home/matt/Desktop/source/tsdb/build/bin/timestar_http_server}
BENCH=/home/matt/Desktop/source/tsdb/build/bin/timestar_insert_bench
DIR=${DATA_DIR:-$S/bench/ins_dir}
fmt=$1; batches=$2; rounds=$3; tag=$4; shift 4
backends=("$@")
mkdir -p $S/bench/res; : > $S/bench/res/be_$tag.txt
tck=$(getconf CLK_TCK)
for i in $(seq 1 $rounds); do
  order=("${backends[@]}"); if [ $((i % 2)) = 0 ]; then order=(); for ((k=${#backends[@]}-1;k>=0;k--)); do order+=("${backends[k]}"); done; fi
  for be in "${order[@]}"; do
    rm -rf $DIR
    pid=$(SRV_EXTRA="--reactor-backend $be" $S/bench/srv.sh start $BIN $DIR 18186) || exit 1
    out=$(taskset -c 12-15 $BENCH -c 1 --server-port 18186 --format $fmt --batches $batches --batch-size 10000 --warmup 10 --connections 8 --hosts 10 --racks 2 --verify false 2>&1)
    thr=$(echo "$out" | grep Throughput | awk '{print $2}')
    p99=$(echo "$out" | grep -i -E "p99" | head -1 | grep -o -E "[0-9.]+ ?ms" | head -1 | tr -d ' ')
    # process-wide CPU (all threads, incl. backend workers)
    cpu=$(awk -v t=$tck '{print ($14+$15)/t}' /proc/$pid/stat)
    rss=$(grep VmHWM /proc/$pid/status | awk '{print $2}')
    thr_n=$(ls /proc/$pid/task | wc -l)
    $S/bench/srv.sh stop $pid
    echo "$be $thr $cpu $rss $thr_n $p99" | tee -a $S/bench/res/be_$tag.txt
  done
done
python3 - <<PY
import statistics
rows=[l.split() for l in open('$S/bench/res/be_$tag.txt')]
for v in dict.fromkeys(x[0] for x in rows):
    r=[x for x in rows if x[0]==v]
    print('%-20s throughput median %.2fM pts/s  process CPU-s median %.2f  peak RSS MB %.0f  threads %s' % (v, statistics.median(float(x[1]) for x in r)/1e6, statistics.median(float(x[2]) for x in r), statistics.median(float(x[3]) for x in r)/1024, r[0][4]))
PY
