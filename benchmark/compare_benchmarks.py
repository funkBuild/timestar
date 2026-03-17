#!/usr/bin/env python3
"""Compare benchmark results against a baseline and flag regressions.

Usage:
    python3 compare_benchmarks.py baseline.json current.json [--threshold 15] [--update-baseline]

Exit code:
    0 = no regressions (or --update-baseline)
    1 = regressions detected exceeding threshold
"""

import json
import sys
import shutil
from pathlib import Path


def load_results(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    return data.get("metrics", data)


def compare(baseline: dict, current: dict, threshold_pct: float) -> list[dict]:
    """Compare metrics. Returns list of comparison results.

    For throughput metrics (MBps, higher is better): regression = current < baseline
    For latency metrics (ns_per_val, lower is better): regression = current > baseline
    """
    latency_keywords = {"ns_per_val", "ms", "latency", "time"}
    results = []

    all_keys = sorted(set(list(baseline.keys()) + list(current.keys())))

    for key in all_keys:
        base_val = float(baseline.get(key, 0))
        curr_val = float(current.get(key, 0))

        if base_val == 0:
            results.append({
                "metric": key,
                "baseline": base_val,
                "current": curr_val,
                "change_pct": 0.0,
                "status": "NEW",
                "regression": False,
            })
            continue

        change_pct = ((curr_val - base_val) / base_val) * 100.0

        # Determine if this is a latency metric (lower is better)
        is_latency = any(kw in key.lower() for kw in latency_keywords)

        if is_latency:
            regression = change_pct > threshold_pct  # got slower
        else:
            regression = change_pct < -threshold_pct  # throughput dropped

        status = "REGRESSION" if regression else "OK"

        results.append({
            "metric": key,
            "baseline": base_val,
            "current": curr_val,
            "change_pct": change_pct,
            "status": status,
            "regression": regression,
        })

    return results


def format_markdown(results: list[dict], threshold: float, baseline_commit: str, current_commit: str) -> str:
    """Format results as a Markdown table."""
    lines = [
        "## Benchmark Regression Report",
        "",
        f"Baseline: `{baseline_commit}` | Current: `{current_commit}` | Threshold: {threshold}%",
        "",
        "| Metric | Baseline | Current | Change | Status |",
        "|--------|----------|---------|--------|--------|",
    ]

    has_regression = False
    for r in results:
        change_str = f"{r['change_pct']:+.1f}%" if r["status"] != "NEW" else "NEW"
        status_icon = {
            "OK": "pass",
            "NEW": "new",
            "REGRESSION": "FAIL",
        }[r["status"]]

        if r["regression"]:
            has_regression = True

        lines.append(
            f"| {r['metric']} | {r['baseline']:.2f} | {r['current']:.2f} | {change_str} | {status_icon} |"
        )

    lines.append("")
    if has_regression:
        lines.append("**Regressions detected.** Review benchmark results before merging.")
    else:
        lines.append("All benchmarks within threshold.")

    return "\n".join(lines)


def main():
    args = sys.argv[1:]

    if len(args) < 2:
        print(f"Usage: {sys.argv[0]} baseline.json current.json [--threshold N] [--update-baseline]")
        sys.exit(1)

    baseline_path = args[0]
    current_path = args[1]
    threshold = 15.0
    update_baseline = False

    i = 2
    while i < len(args):
        if args[i] == "--threshold" and i + 1 < len(args):
            threshold = float(args[i + 1])
            i += 2
        elif args[i] == "--update-baseline":
            update_baseline = True
            i += 1
        else:
            i += 1

    if update_baseline:
        shutil.copy2(current_path, baseline_path)
        print(f"Baseline updated: {baseline_path} <- {current_path}")
        sys.exit(0)

    baseline_data = load_results(baseline_path)
    current_data = load_results(current_path)

    # Load commit info
    with open(baseline_path) as f:
        baseline_meta = json.load(f)
    with open(current_path) as f:
        current_meta = json.load(f)

    baseline_commit = baseline_meta.get("git_commit", "unknown")
    current_commit = current_meta.get("git_commit", "unknown")

    results = compare(baseline_data, current_data, threshold)
    report = format_markdown(results, threshold, baseline_commit, current_commit)
    print(report)

    # Write report to file for CI artifact
    report_path = Path(current_path).parent / "benchmark_report.md"
    report_path.write_text(report)
    print(f"\nReport written to {report_path}")

    has_regression = any(r["regression"] for r in results)
    sys.exit(1 if has_regression else 0)


if __name__ == "__main__":
    main()
