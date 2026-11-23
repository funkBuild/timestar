#!/usr/bin/env python3
"""Load three measurements with 10 series x 100k points each:
  blocky      - each host owns disjoint 10k-point stretches (like timestar_insert_bench)
  interleaved - every host reports once a minute at its own second offset (no shared timestamps, no runs)
  aligned     - every host reports at identical timestamps
"""
import http.client, json, random, sys
port = int(sys.argv[1]); HOSTS = 10; PER = 100000; CH = 10000
BASE = 1_000_000_000_000_000_000; MIN = 60_000_000_000; SEC = 1_000_000_000
c = http.client.HTTPConnection('127.0.0.1', port)
rnd = random.Random(7)
def post(body):
    c.request('POST', '/write', body=json.dumps(body), headers={'Content-Type': 'application/json'})
    r = c.getresponse(); d = r.read()
    assert r.status == 200, d[:300]
for layout in ('blocky', 'interleaved', 'aligned'):
    for h in range(HOSTS):
        for k in range(PER // CH):
            if layout == 'blocky':
                start = BASE + (k * HOSTS + h) * CH * MIN; ts = [start + i * MIN for i in range(CH)]
            elif layout == 'interleaved':
                ts = [BASE + (k * CH + i) * MIN + h * SEC for i in range(CH)]
            else:
                ts = [BASE + (k * CH + i) * MIN for i in range(CH)]
            post({'measurement': layout, 'tags': {'host': f'host-{h:02d}', 'rack': f'rack-{h % 2}'}, 'timestamps': ts,
                  'fields': {'v': [round(rnd.uniform(0, 100), 3) + 0.0005 for _ in range(CH)]}})
print('loaded')
