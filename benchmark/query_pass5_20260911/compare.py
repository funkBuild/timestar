"""Interleave pass-four and current bucket benchmarks without other test/build load."""
from pathlib import Path
import json
import os
import subprocess
import sys

repo = Path(__file__).resolve().parents[2]
root = Path(sys.argv[1]).resolve()
root.mkdir(parents=True, exist_ok=False)
for rep in (1, 2, 3):
    for mode in (("after", "before") if rep % 2 else ("before", "after")):
        perf = repo / ("build/test/timestar_perf_test" if mode == "after" else "build/perf-query-pass5-before/timestar_perf_test")
        commands = {
            "cpu": ["taskset", "-c", "4", str(repo / f"build/perf-query-pass5-{mode}/bucket_cpu_bench")],
            "existing": [str(perf), "--gtest_filter=QueryFoldBench.*", "-c", "1", "--cpuset", "4", "--memory", "4G", "--overprovisioned"],
        }
        for label, cmd in commands.items():
            with (root / f"{mode}-{label}-{rep}.log").open("w") as log:
                subprocess.run(cmd, cwd=root, stdout=log, stderr=subprocess.STDOUT, check=True)
        print(mode, rep, "CPU complete", flush=True)

reference = None
for rep in (1, 2, 3):
    for mode in (("after", "before") if rep % 2 else ("before", "after")):
        output = root / f"{mode}-http-{rep}"
        binary_dir = repo / ("build/bin" if mode == "after" else "build/perf-query-pass5-before")
        env = dict(os.environ, TIMESTAR_BENCH_BIN_DIR=str(binary_dir))
        with (root / f"{mode}-http-{rep}.log").open("w") as log:
            subprocess.run([sys.executable, str(Path(__file__).with_name("bucket_http_probe.py")), str(output)],
                           env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        data = json.loads((output / "results.json").read_text())
        series = {name: (result["series_hash"], result["point_count"]) for name, result in data.items()}
        if reference is None:
            reference = series
        assert series == reference, (mode, rep)
        print(mode, rep, "HTTP complete; all response series match", flush=True)
