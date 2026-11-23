#!/usr/bin/env python3
"""qbench.py <port> <out.json> [iters] [warmup] : time a fixed query list; reports client ms and server execution_time_ms."""
import http.client, json, sys, time, statistics, hashlib
port = int(sys.argv[1]); out = sys.argv[2]; iters = int(sys.argv[3]) if len(sys.argv) > 3 else 15; warm = int(sys.argv[4]) if len(sys.argv) > 4 else 3
BASE = 1_000_000_000_000_000_000; MIN = 60_000_000_000
END = BASE + 100 * 10000 * MIN * 11
Q = []
for layout in ('blocky', 'interleaved', 'aligned'):
    for method in ('avg', 'max', 'latest', 'stddev'):
        Q.append((f'{layout}: {method} all hosts, no interval', {'query': f'{method}:{layout}(v)', 'startTime': BASE, 'endTime': END}))
    Q.append((f'{layout}: avg by rack, no interval', {'query': f'avg:{layout}(v) by {{rack}}', 'startTime': BASE, 'endTime': END}))
    Q.append((f'{layout}: avg all hosts, 1h buckets', {'query': f'avg:{layout}(v)', 'startTime': BASE, 'endTime': END, 'aggregationInterval': '1h'}))
c = http.client.HTTPConnection('127.0.0.1', port)
res = {}
for name, body in Q:
    payload = json.dumps(body); cl = []; sv = []; digest = None; pts = 0
    for i in range(warm + iters):
        t0 = time.perf_counter()
        c.request('POST', '/query', body=payload, headers={'Content-Type': 'application/json'})
        r = c.getresponse(); d = r.read()
        t1 = time.perf_counter()
        assert r.status == 200, d[:300]
        if i == 0:
            j = json.loads(d); pts = j['statistics']['point_count']
            j['statistics'].pop('execution_time_ms', None)
            digest = hashlib.sha256(json.dumps(j, sort_keys=True).encode()).hexdigest()[:16]
        if i >= warm:
            cl.append((t1 - t0) * 1000)
            k = d.rfind(b'"execution_time_ms":'); sv.append(float(d[k + 20:d.find(b'}', k)]))
    res[name] = {'client_ms': statistics.median(cl), 'server_ms': statistics.median(sv), 'points': pts, 'digest': digest}
json.dump(res, open(out, 'w'), indent=1)
