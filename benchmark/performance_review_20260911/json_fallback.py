import sys
import os
import pathlib, subprocess, time, signal, json, http.client, statistics, socket
root = pathlib.Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
bin = pathlib.Path(os.environ.get('TIMESTAR_BENCH_BIN_DIR', str(pathlib.Path(__file__).resolve().parents[2] / 'build' / 'bin')))
# Pre-generate equal-length payloads. Only the first double differs: 0.5 vs 0.0.
payloads=[]
for i in range(35):
    pair=[('fractional',0.5),('zero_float',0.0)]
    if i % 2: pair.reverse()
    for kind, first in pair:
        seq=len(payloads)
        ts=[1_700_000_000_000_000_000+(seq*10000+p)*1_000_000_000 for p in range(10000)]
        fields={f'f{f}':[first]+[25.25]*9999 for f in range(10)}
        payloads.append((kind,i,json.dumps({'measurement':'parse_bench','timestamps':ts,'fields':fields},separators=(',',':')).encode()))
with (root/'server.log').open('w') as log:
    server=subprocess.Popen([str(bin/'timestar_http_server'),'-c','4','--cpuset','0-3','--memory','8G','--overprovisioned','--port','18086','--log-level','warn'],cwd=root,stdout=log,stderr=subprocess.STDOUT)
    conn=None
    try:
        for _ in range(100):
            if server.poll() is not None:
                raise RuntimeError('server exited; see server.log')
            try:
                conn=http.client.HTTPConnection('127.0.0.1',18086,timeout=15)
                conn.request('GET','/health'); resp=conn.getresponse(); resp.read()
                assert resp.status==200
                conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                break
            except Exception:
                if conn: conn.close()
                time.sleep(.1)
        else:
            raise RuntimeError('server health check timed out')
        samples={'fractional':[],'zero_float':[]}
        for off in range(0,len(payloads),2):
            pair=payloads[off:off+2]
            for kind,i,payload in pair:
                t=time.perf_counter()
                conn.request('POST','/write',body=payload,headers={'Content-Type':'application/json'})
                resp=conn.getresponse(); body=resp.read()
                elapsed=(time.perf_counter()-t)*1000
                assert resp.status==200,(resp.status,body)
                result=json.loads(body)
                assert result['points_written']==100000,result
                if i>=5: samples[kind].append(elapsed)
        out={k:{'n':len(v),'median_ms':statistics.median(v),'mean_ms':statistics.mean(v),'min_ms':min(v),'samples_ms':v} for k,v in samples.items()}
        (root/'results.json').write_text(json.dumps(out,indent=2)+'\n')
        print(json.dumps({k:{s:t for s,t in v.items() if s!='samples_ms'} for k,v in out.items()},indent=2))
    finally:
        if conn: conn.close()
        server.send_signal(signal.SIGTERM)
        server.wait(timeout=60)
