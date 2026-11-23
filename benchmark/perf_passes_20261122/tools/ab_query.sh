#!/bin/bash
# ab_query.sh <datadir> <pairs> <tag>  : alternate baseline/new servers over the same (read-only) data dir
S=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
BASE=/home/matt/Desktop/source/tsdb/build/perf-baseline-20261122/timestar_http_server
NEW=/home/matt/Desktop/source/tsdb/build/bin/timestar_http_server
dir=$1; pairs=$2; tag=$3; mkdir -p $S/bench/res
for i in $(seq 1 $pairs); do
  if [ $((i % 2)) = 1 ]; then order="base new"; else order="new base"; fi
  for v in $order; do
    bin=$BASE; [ $v = new ] && bin=$NEW
    pid=$($S/bench/srv.sh start $bin $dir 18186) || exit 1
    taskset -c 14 python3 $S/bench/qbench.py 18186 $S/bench/res/${tag}_${v}_$i.json ${ITERS:-15} 3
    $S/bench/srv.sh stop $pid
  done
done
