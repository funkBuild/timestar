from pathlib import Path
import subprocess,os,time,signal,urllib.request,json,re,sys,shutil
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve();root.mkdir(exist_ok=False,parents=True)
server_bin=Path(sys.argv[2]).resolve()
rounds=int(os.environ.get('ROUNDS','8'));quota=os.environ.get('QUOTA','0.005');memory=os.environ.get('MEMORY','8G')
results=[]
for rep in range(rounds):
 run=root/str(rep);run.mkdir()
 rss=[];failure=None;server_rc=None
 cmd=[str(server_bin),'-c','4','--cpuset','8-11','--memory',memory,'--overprovisioned','--port','18086','--log-level','warn','--task-quota-ms',quota]
 with (run/'server.log').open('w') as log:
  server=subprocess.Popen(cmd,cwd=run,stdout=log,stderr=subprocess.STDOUT)
  try:
   for _ in range(200):
    if server.poll() is not None:raise RuntimeError('server exited at startup')
    try:urllib.request.urlopen('http://127.0.0.1:18086/health',timeout=1).read();break
    except OSError:time.sleep(.1)
   else:raise RuntimeError('server startup timeout')
   with (run/'insert.log').open('w') as out:
    client=subprocess.Popen([str(repo/'build/bin/timestar_insert_bench'),'-c','1','--cpuset','14','--memory','2G','--overprovisioned','--server-port','18086','--format','protobuf','--batches','200','--batch-size','10000','--connections','8','--warmup','10','--verify','false'],cwd=run,stdout=out,stderr=subprocess.STDOUT)
    deadline=time.monotonic()+180
    while client.poll() is None:
     try:
      status=Path(f'/proc/{server.pid}/status').read_text()
      rss.append({k:int(re.search(r'^'+k+r':\s+(\d+) kB',status,re.M).group(1)) for k in ['VmRSS','VmHWM']})
     except (FileNotFoundError,AttributeError):pass
     if time.monotonic()>deadline:client.kill();client.wait();raise RuntimeError('client timed out')
     time.sleep(.01)
   output=(run/'insert.log').read_text()
   if client.returncode or '200 OK, 0 HTTP errors, 0 connection failures' not in output:raise RuntimeError('insert requests failed')
   for field in ['cpu_usage','memory_usage','disk_io_read','disk_io_write','network_in','network_out','load_avg_1m','load_avg_5m','load_avg_15m','temperature']:
    body=json.dumps({'query':f'count:server.metrics({field})','startTime':0,'endTime':18446744073709551615,'aggregationInterval':18446744073709551615}).encode()
    with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:18086/query',data=body,headers={'Content-Type':'application/json'}),timeout=30) as response:data=json.load(response)
    count=sum(v for series in data['series'] for v in series['fields'][field]['values'])
    if count!=2100000:raise RuntimeError(f'count mismatch {field} {count}')
  except Exception as exc:failure=str(exc)
  finally:
   if server.poll() is None:
    server.send_signal(signal.SIGTERM)
    try:server.wait(timeout=50)
    except subprocess.TimeoutExpired:server.kill();server.wait();failure=failure or 'shutdown timeout'
   server_rc=server.returncode
   if server_rc:failure=failure or f'server exit {server_rc}'
 result={'rep':rep,'binary':str(server_bin),'quota_ms':quota,'memory':memory,'failure':failure,'server_rc':server_rc,'peak_observed_rss_kib':max((v['VmRSS'] for v in rss),default=0),'peak_hwm_kib':max((v['VmHWM'] for v in rss),default=0)}
 results.append(result);(root/'results.json').write_text(json.dumps(results,indent=2)+'\n');(run/'memory.json').write_text(json.dumps(rss)+'\n');print(result,flush=True)
 if failure:sys.exit(1)
 for path in run.glob('shard_*'):
  if path.is_dir():shutil.rmtree(path)
