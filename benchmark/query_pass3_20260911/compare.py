"""Compare preserved pass-two binaries to the current build; fresh output root."""
from pathlib import Path
import subprocess, sys, os, re
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve(); root.mkdir(parents=True,exist_ok=False)
for i in range(1,4):
 for mode in (['after','before'] if i%2 else ['before','after']):
  with (root/f'{mode}-cpu-{i}.log').open('w') as log:
   subprocess.run(['/usr/bin/time','-v',str(repo/f'build/perf-query-pass3-{mode}/query_cpu_bench'),'-c','1','--cpuset','4','--memory','4G','--overprovisioned'],cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True)
  print(mode,'CPU',i,'complete',flush=True)
# Byte-for-byte hash comparison of fixed serializer output, excluding timing.
checks=[]
for file in root.glob('*-cpu-*.log'):
 m=re.search(r'bytes=(\d+) hash=(\d+)',file.read_text()); assert m,file
 checks.append(m.groups())
assert len(set(checks))==1,checks
for mode in ['before','after']:
 env=dict(os.environ,TIMESTAR_BENCH_BIN_DIR=str(repo/('build/bin' if mode=='after' else 'build/perf-query-pass3-before')))
 subprocess.run(['python3',str(repo/'benchmark/query_pass2_20260911/run_query_suite.py'),str(root/f'{mode}-native')],env=env,check=True)
 print(mode,'native suite complete',flush=True)
 with (root/f'{mode}-cardinality.log').open('w') as log:
  subprocess.run(['python3',str(repo/'benchmark/performance_review_20260911/high_cardinality_probe.py'),str(root/f'{mode}-cardinality')],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
 print(mode,'cardinality complete',flush=True)
import json
a=json.loads((root/'before-cardinality/results.json').read_text())
b=json.loads((root/'after-cardinality/results.json').read_text())
for name in a:
 norm=lambda series:sorted(series,key=lambda s:json.dumps(s,sort_keys=True))
 assert norm(a[name]['series'])==norm(b[name]['series']),name
print('High-cardinality results match exactly',flush=True)
