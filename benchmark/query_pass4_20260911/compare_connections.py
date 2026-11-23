"""Repeat the short-query probe across eight connections per query/run."""
from pathlib import Path
import json
import os
import statistics
import subprocess
import sys

repo = Path(__file__).resolve().parents[2]
root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
results = {mode: [] for mode in ("before", "after")}
for rep in (1, 2, 3):
    for mode in (("after", "before") if rep % 2 else ("before", "after")):
        binary_dir = repo / ("build/bin" if mode == "after" else "build/perf-query-pass4-before")
        env = dict(os.environ, TIMESTAR_BENCH_BIN_DIR=str(binary_dir))
        output = root / f"{mode}-{rep}"
        with (root / f"{mode}-{rep}.log").open("w") as log:
            subprocess.run(
                [sys.executable, str(Path(__file__).with_name("cardinality_connections.py")), str(output)],
                env=env, stdout=log, stderr=subprocess.STDOUT, check=True,
            )
        results[mode].append(json.loads((output / "results.json").read_text()))
        print(mode, rep, "complete", flush=True)

normalize = lambda series: sorted(series, key=lambda item: json.dumps(item, sort_keys=True))
for name in results["before"][0]:
    expected = normalize(results["before"][0][name]["series"])
    for mode, runs in results.items():
        assert all(normalize(run[name]["series"]) == expected for run in runs), name
        samples = [value for run in runs for value in run[name]["samples_ms"]]
        print(mode, name, "run medians:", [run[name]["median_ms"] for run in runs],
              "pooled median:", statistics.median(samples))
