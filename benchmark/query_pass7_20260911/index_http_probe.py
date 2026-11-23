"""HTTP index workload; check exact cardinalities and query values after timing."""
from pathlib import Path
import http.client
import json
import os
import signal
import socket
import statistics
import subprocess
import sys
import time

root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
server_bin = Path(os.environ['TIMESTAR_BENCH_BIN_DIR']) / 'timestar_http_server'
day_width = 86400 * 10**9
day = 20000 * day_width
hosts = 8192
payloads = []
for start in range(0, hosts, 128):
    writes = [{'measurement': 'index_probe',
               'tags': {'a_cohort': 'odd' if h % 2 else 'even', 'b_region': f'r{h % 4}', 'host': f'host-{h}'},
               'timestamps': [day + (0 if h % 256 == 0 else day_width)], 'fields': {'v': [float(h)]}}
              for h in range(start, start + 128)]
    payloads.append(json.dumps({'writes': writes}, separators=(',', ':')).encode())
queries = {
    'time_nonempty_cached': 'avg:index_probe(v){a_cohort:even,b_region:r0} by {host}',
    'time_empty_uncached': 'avg:index_probe(v){a_cohort:odd,b_region:r0} by {host}',
    'single_tag_cached_control': 'avg:index_probe(v){host:host-0}',
}
with (root / 'server.log').open('w') as log:
    server = subprocess.Popen([str(server_bin), '-c', '4', '--cpuset', '0-3', '--memory', '8G',
                               '--overprovisioned', '--port', '18086', '--log-level', 'warn'],
                              cwd=root, stdout=log, stderr=subprocess.STDOUT)
    # Pin only the client after spawning the server, preserving its allowed CPUs.
    os.sched_setaffinity(0, {6})
    conn = None
    try:
        for _ in range(150):
            if server.poll() is not None:
                raise RuntimeError('server exited')
            try:
                conn = http.client.HTTPConnection('127.0.0.1', 18086, timeout=30)
                conn.request('GET', '/health')
                response = conn.getresponse()
                response.read()
                if response.status == 200:
                    break
            except OSError:
                time.sleep(.1)
        else:
            raise RuntimeError('server not ready')
        conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for payload in payloads:
            conn.request('POST', '/write', body=payload, headers={'Content-Type': 'application/json'})
            response = conn.getresponse()
            body = response.read()
            assert response.status == 200, (response.status, body)
        results = {}
        for name in ['cardinality_host', 'cardinality_region', *queries]:
            samples = []
            for i in range(2400):
                if i % 30 == 0:
                    conn.close()
                    conn = http.client.HTTPConnection('127.0.0.1', 18086, timeout=30)
                    conn.connect()
                    conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                if name.startswith('cardinality'):
                    tag = 'host' if name == 'cardinality_host' else 'b_region'
                    value = f'host-{i}' if tag == 'host' else 'r0'
                    path = f'/cardinality?measurement=index_probe&tag_key={tag}&tag_value={value}'
                    method, payload = 'GET', None
                else:
                    path, method = '/query', 'POST'
                    payload = json.dumps({'query': queries[name], 'startTime': day,
                                          'endTime': day + day_width - 1,
                                          'aggregationInterval': day_width}).encode()
                start = time.perf_counter_ns()
                conn.request(method, path, body=payload, headers={'Content-Type': 'application/json'})
                response = conn.getresponse()
                body = response.read()
                elapsed = (time.perf_counter_ns() - start) / 10**6
                assert response.status == 200, (name, response.status, body)
                decoded = json.loads(body)
                if name.startswith('cardinality'):
                    assert decoded['estimated_series_count'] == (1 if name == 'cardinality_host' else 2048), decoded
                else:
                    values = sorted(v for series in decoded.get('series', [])
                                    for v in series['fields']['v']['values'])
                    expected = ([] if name == 'time_empty_uncached' else
                                [0] if name == 'single_tag_cached_control' else list(range(0, hosts, 256)))
                    assert values == expected, (name, values, expected)
                if i % 30 >= 5:
                    samples.append(elapsed)
            results[name] = {'median_ms': statistics.median(samples), 'samples_ms': samples}
        (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
        print(json.dumps({name: row['median_ms'] for name, row in results.items()}), flush=True)
    finally:
        if conn:
            conn.close()
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=60)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
