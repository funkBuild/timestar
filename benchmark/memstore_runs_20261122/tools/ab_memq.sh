#!/bin/bash
# ab_memq.sh <batches> <pairs> <tag>: load <batches> JSON-array batches (kept in the memory store), then query.
S=${SCRATCH:?set SCRATCH to a scratch directory}
BASE=${BASE_BIN:?}; NEW=${TIMESTAR_BUILD:?set TIMESTAR_BUILD to the build directory}/bin/timestar_http_server
BENCH=${TIMESTAR_BUILD:?set TIMESTAR_BUILD to the build directory}/bin/timestar_insert_bench
batches=$1; pairs=$2; tag=$3; mkdir -p $S/bench/res
for i in $(seq 1 $pairs); do
  if [ $((i % 2)) = 1 ]; then order="base new"; else order="new base"; fi
  for v in $order; do
    bin=$BASE; [ $v = new ] && bin=$NEW
    rm -rf $S/bench/memq_dir
    pid=$($S/bench/srv.sh start $bin $S/bench/memq_dir 18186) || exit 1
    taskset -c 12-15 $BENCH -c 1 --server-port 18186 --format ${FMT:-json} --batches $batches --batch-size ${BATCH_SIZE:-10000} --warmup 0 --connections 8 --hosts 10 --racks 2 --verify false 2>&1 | grep -E "Requests:" | sed 's/^ */  /'
    tsm=$(find $S/bench/memq_dir -name '*.tsm' | wc -l)
    taskset -c 14 python3 $S/bench/memq.py 18186 $S/bench/res/${tag}_${v}_$i.json ${ITERS:-9}
    echo "  $v pair $i: tsm files at query time: $tsm"
    $S/bench/srv.sh stop $pid
  done
done
