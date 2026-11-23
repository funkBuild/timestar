"""Durable HTTP corrections to one resident series, followed by exact aggregate checks."""
from pathlib import Path
import http.client,json,math,os,signal,socket,statistics,subprocess,sys,time
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False)
server_bin=Path(os.environ['TIMESTAR_BENCH_BIN_DIR'])/'timestar_http_server'
n=1048576
base=1710000000000000000
values=[float(i%1000) for i in range(n)]
def payload(points, vals):
 return json.dumps({'measurement':'overwrite_probe','tags':{'host':'h0'},'timestamps':[base+i*1000000000 for i in points],'fields':{'v':vals}},separators=(',',':')).encode()
initial=[payload(range(start,start+65536),values[start:start+65536]) for start in range(0,n,65536)]
requests=[]
for round in range(220):
 points=[];incoming=[]
 for j in range(128):
  pos=(j*7919+round*103)%n
  pos=min((pos//1000)*1000+1+pos%998,n-1)
  value=100+j+.25
  points.append(pos);incoming.append(value);values[pos]=value
 requests.append(payload(points,incoming))
expected_sum=math.fsum(values);mean=expected_sum/n
expected={'sum':expected_sum,'avg':mean,'count':n,'min':min(values),'max':max(values),
          'stddev':math.sqrt(math.fsum((v-mean)**2 for v in values)/n),'first':values[0],'latest':values[-1]}
with (root/'server.log').open('w') as log:
 server=subprocess.Popen([str(server_bin),'-c','4','--cpuset','0-3','--memory','8G','--overprovisioned','--port','18086','--log-level','warn'],cwd=root,stdout=log,stderr=subprocess.STDOUT)
 os.sched_setaffinity(0,{6})
 conn=None
 try:
  for _ in range(150):
   if server.poll() is not None:raise RuntimeError('server exited')
   try:
    conn=http.client.HTTPConnection('127.0.0.1',18086,timeout=30);conn.request('GET','/health');r=conn.getresponse();r.read()
    if r.status==200:break
   except OSError:time.sleep(.1)
  else:raise RuntimeError('server not ready')
  conn.sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
  def post(path, body):
   start=time.perf_counter_ns();conn.request('POST',path,body=body,headers={'Content-Type':'application/json'});r=conn.getresponse();body=r.read();elapsed=(time.perf_counter_ns()-start)/1e6
   assert r.status==200,(r.status,body)
   return elapsed,json.loads(body)
  for body in initial:post('/write',body)
  assert not list(root.glob('shard_*/tsm/*.tsm')), 'initial series must remain resident'
  samples=[]
  for i,body in enumerate(requests):
   elapsed,result=post('/write',body)
   if i>=20:samples.append(elapsed)
  assert not list(root.glob('shard_*/tsm/*.tsm')), 'corrections must remain resident'
  query_samples={};verified={}
  for method,value in expected.items():
   body=json.dumps({'query':f'{method}:overwrite_probe(v)','startTime':0,'endTime':18446744073709551615,'aggregationInterval':18446744073709551615}).encode()
   times=[]
   for i in range(560):
    if i % 70 == 0:
     conn.close()
     conn=http.client.HTTPConnection('127.0.0.1',18086,timeout=30);conn.connect()
     conn.sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
    elapsed,result=post('/query',body)
    actual=[v for series in result['series'] for v in series['fields']['v']['values']]
    assert len(actual)==1 and math.isclose(actual[0],value,rel_tol=2e-10,abs_tol=1e-8),(method,actual,value)
    if i%70>=20:times.append(elapsed)
   query_samples[method]={'median_ms':statistics.median(times),'samples_ms':times}
   verified[method]={'actual':actual[0],'expected':value}
  assert not list(root.glob('shard_*/tsm/*.tsm')), 'query must use resident series'
  results={'median_ms':statistics.median(samples),'samples_ms':samples,'queries':query_samples,'verified':verified,'tsm_files':[]}
  (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
  print('overwrite_ms',results['median_ms'],flush=True)
 finally:
  if conn:conn.close()
  server.send_signal(signal.SIGTERM)
  try:server.wait(timeout=60)
  except subprocess.TimeoutExpired:server.kill();server.wait()
