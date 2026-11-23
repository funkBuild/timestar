#!/bin/bash
# usage: srv.sh start <server-bin> <datadir> <port> [sprof-out]   -> prints pid
#        srv.sh stop <pid>
set -u
cmd=$1; shift
SP=/tmp/claude-1000/-home-matt-Desktop-source-tsdb/9342c4ea-1293-40f9-832b-6ed676dbaaf6/scratchpad
if [ "$cmd" = start ]; then
  bin=$1; dir=$2; port=$3; prof=${4:-}
  mkdir -p "$dir"; cd "$dir"
  if [ -n "$prof" ]; then
    SPROF_OUT="$prof" LD_PRELOAD=$SP/prof/libsprof.so "$bin" --port "$port" -c ${SRV_SMP:-4} --cpuset ${SRV_CPUS:-8-11} -m ${SRV_MEM:-8G} ${SRV_EXTRA:-} > server.log 2>&1 &
  else
    "$bin" --port "$port" -c ${SRV_SMP:-4} --cpuset ${SRV_CPUS:-8-11} -m ${SRV_MEM:-8G} ${SRV_EXTRA:-} > server.log 2>&1 &
  fi
  pid=$!
  for i in $(seq 1 100); do
    if curl -s -o /dev/null "http://127.0.0.1:$port/health"; then echo $pid; exit 0; fi
    if ! kill -0 $pid 2>/dev/null; then echo "server died" >&2; tail -5 server.log >&2; exit 1; fi
    sleep 0.1
  done
  echo "timeout" >&2; kill -9 $pid; exit 1
else
  pid=$1
  kill -TERM $pid 2>/dev/null
  for i in $(seq 1 300); do kill -0 $pid 2>/dev/null || exit 0; sleep 0.1; done
  kill -9 $pid 2>/dev/null
fi
