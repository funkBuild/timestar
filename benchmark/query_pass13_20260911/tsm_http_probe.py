from pathlib import Path
import bisect,re,hashlib,http.client,json,math,os,random,signal,socket,statistics,subprocess,sys,time
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False)
server_bin=Path(os.environ['TIMESTAR_BENCH_BIN_DIR'])/'timestar_http_server'
server_cpus=os.environ.get('SERVER_CPUS','0-3');client_cpu=int(os.environ.get('CLIENT_CPU','6'))
original_affinity=os.sched_getaffinity(0)
(root/'method.json').write_text(json.dumps(dict(server_cpus=server_cpus,client_cpu=client_cpu))+'\n')
n=65536; fixtures={};payloads=[];queries={}
for shape in ('regular','jitter','int_regular','int_jitter'):
 rng=random.Random(42);timestamps=[];ts=1700000000000000000
 for i in range(n):
  timestamps.append(ts);ts+=1000000+(rng.randrange(1<<20) if shape.endswith('jitter') else 0)
 values=[i%127-63 if shape.startswith('int_') else i%127+.25 for i in range(n)];fixtures[shape]=(timestamps,values)
 payloads.append(json.dumps({'measurement':'decode_'+shape,'tags':{'host':'h0'},'timestamps':timestamps,'fields':{'v':values}},separators=(',',':')).encode())
 for method,filter,width in (('avg','full',100000000),('sum','middle',100000000),('avg','early',0),('count','full',18446744073709551615)):
  lo=timestamps[n//3]+1 if filter=='middle' else timestamps[0]
  hi=timestamps[2*n//3]-1 if filter=='middle' else timestamps[1023] if filter=='early' else timestamps[-1]
  buckets={}
  for t,v in zip(timestamps,values):
   if lo<=t<=hi:buckets.setdefault((t//width)*width if width else t,[]).append(v)
  expected=[(t,len(vs) if method=='count' else math.fsum(vs)/(len(vs) if method=='avg' else 1)) for t,vs in sorted(buckets.items())]
  query={'query':f'{method}:decode_{shape}(v)','startTime':lo,'endTime':hi}
  if width:query['aggregationInterval']=width
  queries[f'{shape}_{method}_{filter}']=(json.dumps(query).encode(),expected)

def start_server(name):
 os.sched_setaffinity(0,original_affinity)
 log=(root/name).open('w')
 server=subprocess.Popen([str(server_bin),'-c','4','--cpuset',server_cpus,'--memory','8G','--overprovisioned','--port','18086','--log-level','warn'],cwd=root,stdout=log,stderr=subprocess.STDOUT)
 os.sched_setaffinity(0,{client_cpu})
 try:
  for _ in range(200):
   if server.poll() is not None:raise RuntimeError('server exited')
   try:
    conn=connect();conn.request('GET','/health');r=conn.getresponse();r.read();conn.close()
    if r.status==200:return server,log
   except OSError:time.sleep(.1)
  raise RuntimeError('server not ready')
 except BaseException:
  stop_server(server,log);raise

def stop_server(server,log):
 if server.poll() is None:
  status=Path(f'/proc/{server.pid}/status').read_text()
  memory={key:int(re.search(r'^'+key+r':\s+(\d+) kB',status,re.M).group(1)) for key in ('VmRSS','VmHWM')}
  Path(log.name+'.memory.json').write_text(json.dumps(memory,indent=2)+'\n')
 server.send_signal(signal.SIGTERM)
 try:server.wait(timeout=50)
 except subprocess.TimeoutExpired:server.kill();server.wait();raise
 finally:log.close()
 assert server.returncode==0,server.returncode

def connect():
 conn=http.client.HTTPConnection('127.0.0.1',18086,timeout=30);conn.connect();conn.sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1);return conn

server,log=start_server('load.log')
try:
 conn=connect()
 for body in payloads:
  conn.request('POST','/write',body=body,headers={'Content-Type':'application/json'});r=conn.getresponse();body=r.read();assert r.status==200,(r.status,body)
 conn.close()
finally:stop_server(server,log)
# Graceful shutdown flushes every nonempty store and removes all WAL files.
assert not list(root.glob('shard_*/*.wal')),'WAL remained after flush'
files=sorted(root.glob('shard_*/tsm/*.tsm'));assert files,'fixture must be in TSM storage'
manifest={str(p.relative_to(root)):dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files}
(root/'storage.json').write_text(json.dumps(manifest,indent=2)+'\n')
server,log=start_server('query.log')
results={}
try:
 for name,(body,expected) in queries.items():
  samples=[]
  for c in range(8):
   conn=connect()
   try:
    for i in range(25):
     t=time.perf_counter_ns();conn.request('POST','/query',body=body,headers={'Content-Type':'application/json'});r=conn.getresponse();raw=r.read();ms=(time.perf_counter_ns()-t)/1e6
     assert r.status==200,(r.status,raw)
     response=json.loads(raw);series=response['series'];assert len(series)==1
     field=series[0]['fields']['v'];actual=list(zip(field['timestamps'],field['values']))
     assert len(actual)==len(expected),(name,len(actual),len(expected))
     assert all(t==et and math.isclose(v,ev,rel_tol=1e-12,abs_tol=1e-8) for (t,v),(et,ev) in zip(actual,expected)),(name,actual[:3],expected[:3])
     if i>=5:samples.append(ms)
   finally:conn.close()
  results[name]={'median_ms':statistics.median(samples),'samples_ms':samples,'points':len(expected)}
 assert all(p.stat().st_size==0 for p in root.glob('shard_*/*.wal')),'writes present during query phase'
 assert sorted(str(p.relative_to(root)) for p in root.glob('shard_*/tsm/*.tsm'))==sorted(manifest)
 (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
 print('TSM query responses verified',flush=True)
finally:stop_server(server,log)
