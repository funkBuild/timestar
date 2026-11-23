from pathlib import Path
import subprocess, os, sys, json, time, signal, http.client, socket, statistics, hashlib
root=Path(sys.argv[1]).resolve(); root.mkdir(parents=True,exist_ok=False)
server_bin=Path(os.environ['TIMESTAR_BENCH_BIN_DIR'])/'timestar_http_server'
# All generation is outside request timing.
n = 262144
start_ns = 1_710_000_000_000_000_000
# One numeric series, about 4 MiB of timestamp/value arrays: targets memory reads.
payloads = [json.dumps({'measurement': 'bucket_probe', 'tags': {'host': 'h0'},
    'timestamps': [start_ns + i * 1_000_000_000 for i in range(n)],
    'fields': {'v': [(i % 127) + .25 for i in range(n)]}}, separators=(',', ':')).encode()]
queries = {}
for method, width in [('avg', 300), ('avg', 4096), ('count', 300), ('count', 4096), ('stddev', 300), ('avg', 0)]:
    q = {'query': f'{method}:bucket_probe(v)', 'startTime': start_ns, 'endTime': start_ns + (n - 1) * 1_000_000_000}
    if width:
        q['aggregationInterval'] = width * 1_000_000_000
    queries[f'{method}_{width}'] = q
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
        tsm_files = [str(p.relative_to(root)) for p in root.glob('shard_*/tsm/*.tsm')]
        assert not tsm_files, ('fixture must remain in memory during timing', tsm_files)
        (root/'storage_before_queries.json').write_text(json.dumps({'tsm_files': tsm_files})+'\n')
        results={}
        for name,q in queries.items():
            payload=json.dumps(q,separators=(',',':')).encode(); samples=[]; first=None
            interval = q.get('aggregationInterval', 0)
            expected_points = q['endTime'] // interval - q['startTime'] // interval + 1 if interval else n
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
                assert decoded['statistics']['point_count'] == expected_points, decoded
                if first is None: first=decoded['series']
                if i%15>=5: samples.append(ms)
            results[name]={'median_ms':statistics.median(samples),'mean_ms':statistics.mean(samples),'samples_ms':samples,'series_hash':hashlib.sha256(json.dumps(first,sort_keys=True,separators=(',',':')).encode()).hexdigest(), 'point_count':decoded['statistics']['point_count']}
        assert not list(root.glob('shard_*/tsm/*.tsm')), 'fixture flushed during measurement'
        (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
        print(json.dumps({k:{s:t for s,t in v.items() if s not in ['samples_ms','series']} for k,v in results.items()},indent=2))
    finally:
        if conn: conn.close()
        server.send_signal(signal.SIGTERM); server.wait(timeout=60)
