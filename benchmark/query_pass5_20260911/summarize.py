"""Archive matched logs and calculate medians without discarding slower cases."""
from pathlib import Path
import json
import re
import shutil
import statistics
import sys

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
dest.mkdir(parents=True, exist_ok=False)
summary = {}
for mode in ("before", "after"):
    result = summary[mode] = {"cpu": {}, "existing": {}, "http": {}}
    for rep in (1, 2, 3):
        for label in ("cpu", "existing"):
            path = source / f"{mode}-{label}-{rep}.log"
            shutil.copy2(path, dest / path.name)
        path = source / f"{mode}-http-{rep}/results.json"
        shutil.copy2(path, dest / f"{mode}-http-{rep}.json")
        storage = source / f"{mode}-http-{rep}/storage_before_queries.json"
        if storage.exists():
            shutil.copy2(storage, dest / f"{mode}-storage-{rep}.json")
        for method, width, median in re.findall(
            r"^(avg|count) width=(\d+).*?median_ms=([0-9.]+)",
            (source / f"{mode}-cpu-{rep}.log").read_text(), re.M,
        ):
            result["cpu"].setdefault(f"{method}_{width}", {"run_medians_ms": []})["run_medians_ms"].append(float(median))
        log = (source / f"{mode}-existing-{rep}.log").read_text()
        for name, pattern in {
            "Q1_bucketed_avg": r"\[Q1\].*?: ([0-9.]+) ms",
            "Q2_scalar_avg_control": r"\[Q2\].*?: ([0-9.]+) ms",
            "Q4_stddev_control": r"\[Q4\].*?: ([0-9.]+) ms",
            "Q5_timestamp_count": r"\[Q5\].*?: ([0-9.]+) ms",
        }.items():
            result["existing"].setdefault(name, {"run_best_ms": []})["run_best_ms"].append(float(re.search(pattern, log)[1]))
        for name, data in json.loads(path.read_text()).items():
            item = result["http"].setdefault(name, {"run_medians_ms": [], "samples_ms": [],
                                                   "series_hash": data["series_hash"], "point_count": data["point_count"]})
            assert item["series_hash"] == data["series_hash"]
            assert item["point_count"] == data["point_count"]
            item["run_medians_ms"].append(data["median_ms"])
            item["samples_ms"].extend(data["samples_ms"])
    for item in result["cpu"].values():
        item["median_ms"] = statistics.median(item["run_medians_ms"])
    for item in result["existing"].values():
        item["median_ms"] = statistics.median(item["run_best_ms"])
    for item in result["http"].values():
        samples = item.pop("samples_ms")
        item["pooled_median_ms"] = statistics.median(samples)
        item["sample_count"] = len(samples)
        item["median_ms"] = statistics.median(item["run_medians_ms"])
for name, before in summary["before"]["http"].items():
    after = summary["after"]["http"][name]
    assert before["series_hash"] == after["series_hash"], name
    assert before["point_count"] == after["point_count"], name
(dest / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
for section, field in (("existing", "median_ms"), ("cpu", "median_ms"), ("http", "pooled_median_ms")):
    for name, data in summary["before"][section].items():
        before, after = data[field], summary["after"][section][name][field]
        print(section, name, f"{before:.6f} -> {after:.6f} ms; {before / after:.3f}x")
