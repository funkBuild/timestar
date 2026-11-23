from pathlib import Path
import json,re,statistics,subprocess,sys
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False)
results=[]
for rep in (1,2,3):
 for mode in (('after','before') if rep%2 else ('before','after')):
  log=root/f'{mode}-{rep}.log'
  with log.open('w') as f:
   subprocess.run(['taskset','-c','4',str(repo/f'build/perf-insert-pass9-{mode}/search_insert_bench')],stdout=f,stderr=subprocess.STDOUT,check=True)
  for shape,n,count,median,low,high in re.findall(r'(\w+) n=(\d+) count=(\d+) median_ms=([\d.]+) min_ms=([\d.]+) max_ms=([\d.]+)',log.read_text()):
   results.append(dict(mode=mode,rep=rep,shape=shape,n=int(n),count=int(count),median_ms=float(median),min_ms=float(low),max_ms=float(high)))
  assert sum(r['mode']==mode and r['rep']==rep for r in results)==40
  print(mode,rep,'complete',flush=True)
summary={}
for n in (4097,65537):
 for count in (1,7,8,9,128):
  for shape in ('random','sorted','early_miss','late_miss'):
   row={mode:statistics.median(r['median_ms'] for r in results if r['mode']==mode and r['n']==n and r['count']==count and r['shape']==shape) for mode in ('before','after')}
   row['speedup']=row['before']/row['after'];summary[f'{shape}_{n}_{count}']=row
(root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
