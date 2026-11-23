from pathlib import Path
import subprocess, os, sys, json, time, signal, http.client, socket, statistics
root=Path(sys.argv[1]).resolve(); root.mkdir(parents=True,exist_ok=False)
server_bin=Path(os.environ['TIMESTAR_BENCH_BIN_DIR'])/'timestar_http_server'
# All generation is outside request timing.
payloads=[]
for batch in range(30):
    writes=[]
    for i in range(batch*100,(batch+1)*100):
        writes.append({'measurement':'cardinality_probe','tags':{'host':f'host-{i:04d}','rack':f'rack-{i%10}'},'timestamps':list(range(1000,1016)),'fields':{'v':[float(i)+.5]*16}})
    payloads.append(json.dumps({'writes':writes},separators=(',',':')).encode())
queries={
  'grouped':{'query':'avg:cardinality_probe(v) by {rack}','startTime':0,'endTime':18446744073709551615},
  'bucketed':{'query':'avg:cardinality_probe(v)','startTime':0,'endTime':2000,'aggregationInterval':3000},
  'filtered':{'query':'avg:cardinality_probe(v){host:host-1*}','startTime':0,'endTime':2000,'aggregationInterval':3000}
}
with (root/'server.log').open('w') as log:
    server=subprocess.Popen([str(server_bin),'-c','4','--cpuset','0-3','--memory','8G','--overprovisioned','--port','18086','--log-level','warn'],cwd=root,stdout=log,stderr=subprocess.STDOUT)
    conn=None
    try:
        for _ in range(150):
            if server.poll() is not None: raise RuntimeError('server exited')
            try:
                conn=http.client.HTTPConnection('127.0.0.1',18086,timeout=30)
                conn.request('GET','/health'); r=conn.getresponse(); r.read()
                if r.status==200: break
            except Exception: time.sleep(.1)
        else: raise RuntimeError('server not ready')
        conn.sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        for payload in payloads:
            conn.request('POST','/write',body=payload,headers={'Content-Type':'application/json'})
            r=conn.getresponse(); body=r.read(); assert r.status==200,(r.status,body)
        results={}
        for name,q in queries.items():
            payload=json.dumps(q,separators=(',',':')).encode(); samples=[]; first=None
            for i in range(120):
                if i % 15 == 0:
                    conn.close()
                    conn=http.client.HTTPConnection('127.0.0.1',18086,timeout=30)
                    conn.connect()
                    conn.sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
                t=time.perf_counter(); conn.request('POST','/query',body=payload,headers={'Content-Type':'application/json'})
                r=conn.getresponse(); body=r.read(); ms=(time.perf_counter()-t)*1000
                assert r.status==200,(r.status,body)
                decoded=json.loads(body)
                assert decoded['statistics']['point_count']>0, decoded
                if first is None: first=decoded['series']
                if i%15>=5: samples.append(ms)
            results[name]={'median_ms':statistics.median(samples),'mean_ms':statistics.mean(samples),'samples_ms':samples,'series':first}
        (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
        print(json.dumps({k:{s:t for s,t in v.items() if s not in ['samples_ms','series']} for k,v in results.items()},indent=2))
    finally:
        if conn: conn.close()
        server.send_signal(signal.SIGTERM); server.wait(timeout=60)
