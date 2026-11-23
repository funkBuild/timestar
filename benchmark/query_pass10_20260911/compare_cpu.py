from pathlib import Path
import json,re,statistics,subprocess,sys,os
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False)
bench=sys.argv[2] if len(sys.argv)>2 else 'decode_cpu_bench'
assert bench in ('decode_cpu_bench','unpack_cpu_bench')
cpu=int(os.environ.get('PIN_CPU','4'));pairs=int(os.environ.get('BENCH_PAIRS','3'))
assert pairs>0
(root/'method.json').write_text(json.dumps(dict(cpu=cpu,pairs=pairs,benchmark=bench),indent=2)+'\n')
results=[]
for rep in range(1,pairs+1):
 for mode in (('after','before') if rep%2 else ('before','after')):
  log=root/f'{mode}-{rep}.log'
  with log.open('w') as f:
   subprocess.run(['taskset','-c',str(cpu),str(repo/f'build/perf-query-pass10-{mode}'/bench)],stdout=f,stderr=subprocess.STDOUT,check=True)
  for line in log.read_text().splitlines():
   if 'median_ns=' not in line:continue
   key=line.split(' median_ns=')[0]
   fields=dict(token.split('=',1) for token in line.split() if '=' in token)
   results.append(dict(mode=mode,rep=rep,key=key,**{k:float(v) for k,v in fields.items() if k.endswith('_ns') or k.endswith('_bytes')}))
  assert sum(r['mode']==mode and r['rep']==rep for r in results)==(60 if bench=='decode_cpu_bench' else 130)
  print(mode,rep,'complete',flush=True)
summary={}
for key in dict.fromkeys(r['key'] for r in results):
 row={mode:statistics.median(r['median_ns'] for r in results if r['mode']==mode and r['key']==key) for mode in ('before','after')}
 row['speedup']=row['before']/row['after']
 row['paired_speedups']=[next(r['median_ns'] for r in results if r['mode']=='before' and r['key']==key and r['rep']==rep)/next(r['median_ns'] for r in results if r['mode']=='after' and r['key']==key and r['rep']==rep) for rep in range(1,pairs+1)]
 row['median_paired_speedup']=statistics.median(row['paired_speedups'])
 summary[key]=row
(root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
