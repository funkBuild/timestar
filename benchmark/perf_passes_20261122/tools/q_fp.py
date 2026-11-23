import http.client, json, sys, time, statistics
port = int(sys.argv[1]); BASE = 1_000_000_000_000_000_000; MIN = 60_000_000_000; END = BASE + 100 * 10000 * MIN
c = http.client.HTTPConnection('127.0.0.1', port); out = {}
for name, q, extra in [('avg 7m buckets, 1 field, all hosts (decode 1M pts)', 'avg:server.metrics(cpu_usage)', {'aggregationInterval': '7m'}),
                       ('max 7m buckets, all 10 fields (decode 10M pts)', 'max:server.metrics()', {'aggregationInterval': '7m'}),
                       ('raw avg, 1 field, no interval (1M pts out)', 'avg:server.metrics(cpu_usage)', {})]:
    body = json.dumps(dict({'query': q, 'startTime': BASE, 'endTime': END}, **extra)); sv = []; cl = []
    for i in range(18):
        t0 = time.perf_counter(); c.request('POST', '/query', body=body, headers={'Content-Type': 'application/json'}); d = c.getresponse().read(); t1 = time.perf_counter()
        k = d.rfind(b'"execution_time_ms":'); sv.append(float(d[k + 20:d.find(b'}', k)])); cl.append((t1 - t0) * 1000)
    out[name] = (statistics.median(sv[3:]), statistics.median(cl[3:]), len(d))
print(json.dumps(out))
