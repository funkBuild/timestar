import sys
import os
import pathlib, subprocess, time, urllib.request, signal, json
root = pathlib.Path(sys.argv[1]).resolve()
bin = pathlib.Path(os.environ.get('TIMESTAR_BENCH_BIN_DIR', str(pathlib.Path(__file__).resolve().parents[2] / 'build' / 'bin')))
root.mkdir(parents=True, exist_ok=False)
summary = []
for rep in range(1, 4):
    for fmt in (['json','protobuf'] if rep % 2 else ['protobuf','json']):
        run = root / f'{fmt}-{rep}'
        run.mkdir(exist_ok=True)
        with (run / 'server.log').open('w') as log:
            server = subprocess.Popen([str(bin/'timestar_http_server'), '-c','4','--cpuset','0-3','--memory','8G','--overprovisioned','--port','18086','--log-level','warn'],cwd=run,stdout=log,stderr=subprocess.STDOUT)
            try:
                for _ in range(150):
                    if server.poll() is not None: raise RuntimeError('server exited')
                    try:
                        urllib.request.urlopen('http://127.0.0.1:18086/health',timeout=1).read()
                        break
                    except Exception: time.sleep(.1)
                else:
                    raise RuntimeError('server health check timed out')
                common = ['-c','1','--cpuset','6','--memory','2G','--overprovisioned','--server-port','18086']
                cmd = [str(bin/'timestar_insert_bench'),*common,'--format',fmt,'--batches','100','--batch-size','10000','--connections','8','--warmup','5']
                with (run/'insert.log').open('w') as out:
                    subprocess.run(cmd,cwd=run,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=180)
                lines = (run/'insert.log').read_text().splitlines()
                metrics = [s.strip() for s in lines if any(k in s for k in ['Requests:','Throughput:','Wall time:','Including setup','batch latency','ERROR','First error:'])]
                summary.append({'format':fmt,'rep':rep,'metrics':metrics})
                print(fmt,rep,*metrics,sep='\n',flush=True)
                if rep == 1 and fmt == 'protobuf':
                    with (run/'queries.log').open('w') as out:
                        subprocess.run([str(bin/'timestar_query_bench'),*common],cwd=run,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=180)
                    print('Query suite complete',flush=True)
            finally:
                server.send_signal(signal.SIGTERM)
                try: server.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    server.kill(); server.wait()
(root/'http-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
