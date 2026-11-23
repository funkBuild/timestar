"""Load the existing native benchmark fixture, then run its query suite."""
from pathlib import Path
import os, subprocess, sys, time, urllib.request, signal
repo = Path(__file__).resolve().parents[2]
root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
server_bin = Path(os.environ['TIMESTAR_BENCH_BIN_DIR']) / 'timestar_http_server'
with (root / 'server.log').open('w') as log:
    server = subprocess.Popen([str(server_bin), '-c', '4', '--cpuset', '0-3', '--memory', '8G',
                               '--overprovisioned', '--port', '18086', '--log-level', 'warn'],
                              cwd=root, stdout=log, stderr=subprocess.STDOUT)
    try:
        for _ in range(150):
            if server.poll() is not None:
                raise RuntimeError('server exited')
            try:
                urllib.request.urlopen('http://127.0.0.1:18086/health', timeout=1).read()
                break
            except Exception:
                time.sleep(.1)
        else:
            raise RuntimeError('server did not become ready')
        common = ['-c', '1', '--cpuset', '6', '--memory', '2G', '--overprovisioned', '--server-port', '18086']
        for name, args in [
            ('insert', ['--format', 'protobuf', '--batches', '100', '--batch-size', '10000', '--connections', '8', '--warmup', '5']),
            ('query', [])]:
            with (root / f'{name}.log').open('w') as out:
                subprocess.run([str(repo / 'build/bin' / f'timestar_{name}_bench'), *common, *args],
                               cwd=root, stdout=out, stderr=subprocess.STDOUT, check=True, timeout=180)
        assert '100 OK, 0 HTTP errors, 0 connection failures' in (root / 'insert.log').read_text()
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=60)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
