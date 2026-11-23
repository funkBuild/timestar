"""Alternate index versions on one CPU, with fresh databases for each run."""
from pathlib import Path
import json
import re
import shutil
import statistics
import subprocess
import sys

repo = Path(__file__).resolve().parents[2]
root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
results = []
for rep in (1, 2, 3):
    for mode in (("after", "before") if rep % 2 else ("before", "after")):
        run = root / f"{mode}-{rep}"
        run.mkdir()
        binary = repo / f"build/perf-query-pass7-{mode}/index_cpu_bench"
        with (run / "benchmark.log").open("w") as log:
            subprocess.run([str(binary), "-c", "1", "--cpuset", "4", "--memory", "4G",
                            "--overprovisioned"], cwd=run, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=180)
        for match in re.finditer(r"(\w+) median_ns=([\d.]+) min_ns=([\d.]+) max_ns=([\d.]+) allocs_per_op=([\d.]+)",
                                (run / "benchmark.log").read_text()):
            shape, median, low, high, allocs = match.groups()
            results.append(dict(mode=mode, rep=rep, shape=shape, median_ns=float(median),
                                min_ns=float(low), max_ns=float(high), allocs_per_op=float(allocs)))
        assert len([r for r in results if r['mode'] == mode and r['rep'] == rep]) == 8
        # Remove only the database created by this run; retain all logs.
        shutil.rmtree(run / "shard_0")
        (root / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(mode, rep, "complete", flush=True)
summary = {}
for shape in sorted({r['shape'] for r in results}):
    summary[shape] = {}
    for mode in ('before', 'after'):
        rows = [r for r in results if r['shape'] == shape and r['mode'] == mode]
        summary[shape][mode] = {key: statistics.median(r[key] for r in rows)
                                for key in ('median_ns', 'allocs_per_op')}
    summary[shape]['speedup'] = summary[shape]['before']['median_ns'] / summary[shape]['after']['median_ns']
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
