import http.client, json, sys
port = int(sys.argv[1]); n = int(sys.argv[2]); BASE = 1_000_000_000_000_000_000; MIN = 60_000_000_000; END = BASE + 100 * 10000 * MIN
body = json.dumps(json.loads(sys.argv[3]) | {'startTime': BASE, 'endTime': END})
c = http.client.HTTPConnection('127.0.0.1', port)
for i in range(n):
    c.request('POST', '/query', body=body, headers={'Content-Type': 'application/json'}); d = c.getresponse().read()
print(len(d), d[-90:])
