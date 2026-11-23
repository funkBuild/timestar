from pathlib import Path
import subprocess, time, urllib.request, signal, json, re, shutil, sys
repo=Path(__file__).resolve().parents[2]
root=Path(sys.argv[1]).resolve()
root.mkdir(exist_ok=False)
results=[]
for rep in range(1,65):
    for fmt in ['protobuf']:
        for version in ['after']:
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
                    results.append(dict(version=version,format=fmt,rep=rep,successful=('200 OK, 0 HTTP errors, 0 connection failures' in output), points_per_second=int(re.search(r'Throughput:\s+(\d+)',output)[1]),io_pressure_before=pressure,io_pressure_after=Path('/proc/pressure/io').read_text()))
                    (root/'results.json').write_text(json.dumps(results,indent=2)+'\n')
                    print(results[-1],flush=True)
                    if not results[-1]['successful']: raise RuntimeError('Optimized server failure reproduced; see server.log')
                finally:
                    server.send_signal(signal.SIGTERM)
                    try: server.wait(timeout=60)
                    except subprocess.TimeoutExpired: server.kill(); server.wait()
            # Keep logs; remove only fresh benchmark databases.
            for path in run.glob('shard_*'):
                if path.is_dir(): shutil.rmtree(path)
