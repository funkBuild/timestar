from pathlib import Path
import subprocess, time, urllib.request, signal, json, re, shutil, sys
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve()
root.mkdir(exist_ok=False)
results=[]
for rep in range(1,4):
    for fmt in ['json','protobuf']:
        for version in (['after','before'] if rep%2 else ['before','after']):
            run=root/f'{fmt}-{rep}-{version}'; run.mkdir()
            server_bin=repo/('build/perf-insert-pass6-before/timestar_http_server' if version=='before' else 'build/bin/timestar_http_server')
            pressure=Path('/proc/pressure/io').read_text()
            with (run/'server.log').open('w') as log:
                server=subprocess.Popen([str(server_bin),'-c','4','--cpuset','0-3','--memory','8G','--overprovisioned','--port','18086','--log-level','warn'],cwd=run,stdout=log,stderr=subprocess.STDOUT)
                try:
                    for _ in range(150):
                        if server.poll() is not None: raise RuntimeError('server exited')
                        try:
                            urllib.request.urlopen('http://127.0.0.1:18086/health',timeout=1).read(); break
                        except Exception: time.sleep(.1)
                    else: raise RuntimeError('server not ready')
                    with (run/'insert.log').open('w') as out:
                        subprocess.run([str(repo/'build/bin/timestar_insert_bench'),'-c','1','--cpuset','6','--memory','2G','--overprovisioned','--server-port','18086','--format',fmt,'--batches','200','--batch-size','10000','--connections','8','--warmup','10','--verify','false'],cwd=run,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=180)
                    output=(run/'insert.log').read_text()
                    assert '200 OK, 0 HTTP errors, 0 connection failures' in output
                    # The native all-fields verification exceeds the query point limit.
                    # Verify each field with a compact COUNT query outside timing.
                    verified = {}
                    for field in ['cpu_usage', 'memory_usage', 'disk_io_read', 'disk_io_write', 'network_in',
                                  'network_out', 'load_avg_1m', 'load_avg_5m', 'load_avg_15m', 'temperature']:
                        payload = json.dumps({'query': f'count:server.metrics({field})', 'startTime': 0,
                                              'endTime': 18446744073709551615,
                                              'aggregationInterval': 18446744073709551615}).encode()
                        request = urllib.request.Request('http://127.0.0.1:18086/query', data=payload,
                                                         headers={'Content-Type': 'application/json'})
                        with urllib.request.urlopen(request, timeout=30) as response:
                            result = json.load(response)
                        count = sum(value for series in result['series'] for value in series['fields'][field]['values'])
                        assert count == 2100000, (field, count, result)
                        verified[field] = count
                    (run/'verification.json').write_text(json.dumps(verified, indent=2)+'\n')
                    results.append(dict(version=version,format=fmt,rep=rep,points_per_second=int(re.search(r'Throughput:\s+(\d+)',output)[1]),io_pressure_before=pressure,io_pressure_after=Path('/proc/pressure/io').read_text()))
                    (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
                    print(results[-1],flush=True)
                finally:
                    server.send_signal(signal.SIGTERM)
                    try: server.wait(timeout=60)
                    except subprocess.TimeoutExpired: server.kill(); server.wait()
            # Keep logs; remove only fresh benchmark databases.
            for path in run.glob('shard_*'):
                if path.is_dir(): shutil.rmtree(path)
