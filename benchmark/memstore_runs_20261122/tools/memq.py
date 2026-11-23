#!/usr/bin/env python3
"""memq.py <port> <out.json> [iters]: queries over data that is still in the memory store.
Records the FIRST execution of each query (which pays any lazy consolidation), the median of the rest,
and the full result for comparison."""
import http.client, json, sys, time, statistics
port = int(sys.argv[1]); out = sys.argv[2]; iters = int(sys.argv[3]) if len(sys.argv) > 3 else 9
END = 9_000_000_000_000_000_000
Q = [
 ('raw one series',                 {'query': 'avg:server.metrics(cpu_usage){host:host-01}'}),
 ('avg across hosts, no interval',  {'query': 'avg:server.metrics(memory_usage)'}),
 ('stddev across hosts, no interval', {'query': 'stddev:server.metrics(disk_io_read)'}),
 ('max all fields, 1h buckets',     {'query': 'max:server.metrics()', 'aggregationInterval': '1h'}),
 ('avg by host, whole range (stats)', {'query': 'avg:server.metrics(disk_io_write) by {host}', 'aggregationInterval': '3650d'}),
 ('latest by host, 1m buckets',     {'query': 'latest:server.metrics(cpu_usage) by {host}', 'aggregationInterval': '1m'}),
 ('count by rack, 5m buckets',      {'query': 'count:server.metrics(memory_usage) by {rack}', 'aggregationInterval': '5m'}),
]
c = http.client.HTTPConnection('127.0.0.1', port)
res = {}
for name, body in Q:
    body = dict(body, startTime=0, endTime=END)
    payload = json.dumps(body); sv = []; first = None; result = None
    for i in range(1 + iters):
        c.request('POST', '/query', body=payload, headers={'Content-Type': 'application/json'})
        r = c.getresponse(); d = r.read()
        assert r.status == 200, d[:300]
        k = d.rfind(b'"execution_time_ms":'); ms = float(d[k + 20:d.find(b'}', k)])
        if i == 0:
            first = ms; j = json.loads(d); pts = j['statistics']['point_count']; result = j['series']
        else:
            sv.append(ms)
    res[name] = {'first_ms': first, 'server_ms': statistics.median(sv), 'points': pts, 'result': result}
json.dump(res, open(out, 'w'))
