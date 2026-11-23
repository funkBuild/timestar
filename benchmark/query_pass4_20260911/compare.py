"""Interleave preserved pass-three and current query measurements."""
from pathlib import Path
import subprocess,os,sys,json,re
repo=Path(__file__).resolve().parents[2]; root=Path(sys.argv[1]).resolve(); root.mkdir(parents=True,exist_ok=False)
for rep in [1,2,3]:
 for mode in (['after','before'] if rep%2 else ['before','after']):
  binary=repo/f'build/perf-query-pass4-{mode}/query_cpu_bench'
  perf=repo/('build/test/timestar_perf_test' if mode=='after' else 'build/perf-query-pass4-before/timestar_perf_test')
  for label,command in [('cpu',[str(binary)]),('existing-merge',[str(perf),'--gtest_filter=AggregationStallBench.S2_*'])]:
   with (root/f'{mode}-{label}-{rep}.log').open('w') as log:
    subprocess.run(['/usr/bin/time','-v',*command,'-c','1','--cpuset','4','--memory','4G','--overprovisioned'],cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True)
  print(mode,rep,'CPU benchmarks complete',flush=True)
# The formatter is an unchanged control workload; its output must be identical.
hashes=[re.search(r'bytes=(\d+) hash=(\d+)',p.read_text()).groups() for p in root.glob('*-cpu-*.log')]
assert len(set(hashes))==1
for rep in [1,2,3]:
 for mode in (['after','before'] if rep%2 else ['before','after']):
  env=dict(os.environ,TIMESTAR_BENCH_BIN_DIR=str(repo/('build/bin' if mode=='after' else 'build/perf-query-pass4-before')))
  subprocess.run(['python3',str(repo/'benchmark/query_pass2_20260911/run_query_suite.py'),str(root/f'{mode}-native-{rep}')],env=env,check=True)
  with (root/f'{mode}-cardinality-{rep}.log').open('w') as log:
   subprocess.run(['python3',str(repo/'benchmark/performance_review_20260911/high_cardinality_probe.py'),str(root/f'{mode}-cardinality-{rep}')],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
  print(mode,rep,'HTTP benchmarks complete',flush=True)
for rep in [1,2,3]:
 a=json.loads((root/f'before-cardinality-{rep}/results.json').read_text()); b=json.loads((root/f'after-cardinality-{rep}/results.json').read_text())
 for name in a:
  norm=lambda series:sorted(series,key=lambda s:json.dumps(s,sort_keys=True))
  assert norm(a[name]['series'])==norm(b[name]['series']),name
print('High-cardinality responses match.',flush=True)
