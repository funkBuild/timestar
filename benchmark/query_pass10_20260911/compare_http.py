from pathlib import Path
import json,os,shutil,statistics,subprocess,sys
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False)
results=[]
for rep in (1,2,3):
 for mode in (('after','before') if rep%2 else ('before','after')):
  run=root/f'{mode}-{rep}'
  env=dict(os.environ,TIMESTAR_BENCH_BIN_DIR=str(repo/('build/bin' if mode=='after' else 'build/perf-query-pass10-before')))
  subprocess.run([sys.executable,str(Path(__file__).with_name('tsm_http_probe.py')),str(run)],env=env,check=True,timeout=180)
  results.append(dict(mode=mode,rep=rep,results=json.loads((run/'results.json').read_text())))
  for path in run.glob('shard_*'):
   if path.is_dir():shutil.rmtree(path)
  print(mode,rep,'verified HTTP complete',flush=True)
summary={name:{mode:statistics.median(r['results'][name]['median_ms'] for r in results if r['mode']==mode) for mode in ('before','after')} for name in results[0]['results']}
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
