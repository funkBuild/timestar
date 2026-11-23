"""Alternate the preserved and current WAL append drivers on tmpfs."""
from pathlib import Path
import re
import subprocess
import sys

repo = Path(__file__).resolve().parents[2]
root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
sizes = {}
for rep in (1, 2, 3):
    for mode in (("after", "before") if rep % 2 else ("before", "after")):
        run = root / f"{mode}-{rep}"
        run.mkdir()
        path = root / f"{mode}-{rep}.log"
        with path.open("w") as log:
            subprocess.run([str(repo / f"build/perf-insert-pass6-{mode}/wal_cpu_bench"),
                            "-c", "1", "--cpuset", "4", "--memory", "4G", "--overprovisioned"],
                           cwd=run, stdout=log, stderr=subprocess.STDOUT, check=True)
        for shape, count in re.findall(r"^(\w+) points=\d+ bytes=(\d+)", path.read_text(), re.M):
            assert sizes.setdefault(shape, count) == count, shape
        print(mode, rep, "complete; byte counts match", flush=True)
