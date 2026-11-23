"""Run both server versions with the same current native benchmark client."""
from pathlib import Path
import subprocess, sys, os
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve(); root.mkdir(parents=True,exist_ok=False)
for rep in range(1,4):
 for mode in (['after','before'] if rep%2 else ['before','after']):
  env=dict(os.environ,TIMESTAR_BENCH_BIN_DIR=str(repo/('build/bin' if mode=='after' else 'build/perf-query-pass3-before')))
  subprocess.run(['python3',str(repo/'benchmark/query_pass2_20260911/run_query_suite.py'),str(root/f'{mode}-{rep}')],env=env,check=True)
  print(mode,rep,'HTTP suite complete',flush=True)
