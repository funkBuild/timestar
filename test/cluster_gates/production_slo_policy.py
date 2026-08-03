#!/usr/bin/env python3
"""Validate and identify an approved exact-v1 production SLO policy."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import stat
import sys

MAX_POLICY_BYTES = 64 << 10
MAX_MEMORY_BYTES = 1 << 40
MIN_FREE_MEMORY_PER_REACTOR = 256 << 20
THRESHOLD_LIMITS = {
    "node_failure_error_basis_points": ("max", 5000),
    "node_failure_recovery_ms": ("max", 30000),
    "node_failure_query_p99_ms": ("max", 2000),
    "snapshot_install_ms": ("max", 360000),
    "snapshot_catchup_ms": ("max", 750000),
    "movement_p99_ms": ("max", 5000),
    "movement_throughput_retained_percent": ("min", 10),
    "movement_http_errors": ("max", 100),
}
DEPLOYMENT_KEYS = {
    "high_volume_server_memory_per_process",
    "high_volume_server_smp",
    "snapshot_catchup_server_memory_per_process",
    "snapshot_catchup_smp",
    "bench_memory",
    "bench_smp",
}


class PolicyError(RuntimeError):
    pass


def parse_utc(value: object, label: str) -> dt.datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise PolicyError(f"{label} must be a UTC timestamp ending in Z")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise PolicyError(f"{label} is invalid: {exc}") from exc
    if parsed.tzinfo != dt.timezone.utc:
        raise PolicyError(f"{label} must be UTC")
    return parsed


def parse_memory_bytes(value: object, label: str) -> int:
    if not isinstance(value, str):
        raise PolicyError(f"{label} must be a memory string such as 2G")
    match = re.fullmatch(r"([1-9][0-9]*)([KMGT])", value)
    if match is None:
        raise PolicyError(f"{label} must be a positive integer followed by K, M, G, or T")
    amount = int(match.group(1)) << {"K": 10, "M": 20, "G": 30, "T": 40}[match.group(2)]
    if amount > MAX_MEMORY_BYTES:
        raise PolicyError(f"{label} exceeds the 1-TiB qualification bound")
    return amount


def require_text(value: object, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value:
        raise PolicyError(f"{label} must be a non-empty UTF-8 string of at most {maximum} bytes")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PolicyError(f"{label} is not valid UTF-8") from exc
    if len(encoded) > maximum:
        raise PolicyError(f"{label} must be a non-empty UTF-8 string of at most {maximum} bytes")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise PolicyError(f"{label} contains a control character")
    return value


def validate_policy(policy: object, now: dt.datetime | None = None) -> dict:
    if not isinstance(policy, dict) or set(policy) != {
        "version",
        "name",
        "approval",
        "approved_at",
        "expires_at",
        "deployment",
        "thresholds",
    }:
        raise PolicyError("policy has an incomplete or obsolete exact-v1 shape")
    if type(policy["version"]) is not int or policy["version"] != 1:
        raise PolicyError("policy is not exact version 1")
    require_text(policy["name"], "policy name", 128)
    approval = policy["approval"]
    if not isinstance(approval, dict) or set(approval) != {"authority", "reference"}:
        raise PolicyError("policy approval must contain exactly authority and reference")
    require_text(approval["authority"], "approval authority", 256)
    require_text(approval["reference"], "approval reference", 512)
    approved_at = parse_utc(policy["approved_at"], "approved_at")
    expires_at = parse_utc(policy["expires_at"], "expires_at")
    if approved_at >= expires_at:
        raise PolicyError("approved_at must precede expires_at")
    if expires_at - approved_at > dt.timedelta(days=366):
        raise PolicyError("policy approval may remain valid for at most 366 days")
    current = now or dt.datetime.now(dt.timezone.utc)
    if current.tzinfo is None:
        raise PolicyError("policy validation time must be timezone-aware")
    current = current.astimezone(dt.timezone.utc)
    if current < approved_at:
        raise PolicyError("policy approval is not effective yet")
    if current >= expires_at:
        raise PolicyError("policy approval has expired")

    deployment = policy["deployment"]
    if not isinstance(deployment, dict) or set(deployment) != DEPLOYMENT_KEYS:
        raise PolicyError("policy deployment has an incomplete or obsolete shape")
    for key in ("high_volume_server_smp", "snapshot_catchup_smp", "bench_smp"):
        value = deployment[key]
        if type(value) is not int or value <= 0 or value > 64:
            raise PolicyError(f"{key} must be an integer from 1 through 64")
    if deployment["snapshot_catchup_smp"] != 1:
        raise PolicyError("snapshot_catchup_smp must remain 1 for the focused catch-up gate")
    if deployment["bench_smp"] != 1:
        raise PolicyError("bench_smp must remain 1 for the bounded load driver")
    server_memory = parse_memory_bytes(
        deployment["high_volume_server_memory_per_process"], "high_volume_server_memory_per_process"
    )
    snapshot_memory = parse_memory_bytes(
        deployment["snapshot_catchup_server_memory_per_process"],
        "snapshot_catchup_server_memory_per_process",
    )
    bench_memory = parse_memory_bytes(deployment["bench_memory"], "bench_memory")
    if server_memory // deployment["high_volume_server_smp"] <= MIN_FREE_MEMORY_PER_REACTOR:
        raise PolicyError("high-volume server memory must exceed 256 MiB per reactor")
    if snapshot_memory // deployment["snapshot_catchup_smp"] <= MIN_FREE_MEMORY_PER_REACTOR:
        raise PolicyError("snapshot server memory must exceed 256 MiB per reactor")
    if bench_memory < 512 << 20:
        raise PolicyError("benchmark memory must be at least 512 MiB")

    thresholds = policy["thresholds"]
    if not isinstance(thresholds, dict) or set(thresholds) != set(THRESHOLD_LIMITS):
        raise PolicyError("policy thresholds have an incomplete or obsolete shape")
    for key, (direction, safety_limit) in THRESHOLD_LIMITS.items():
        value = thresholds[key]
        if type(value) is not int or value < 0:
            raise PolicyError(f"{key} must be a non-negative integer")
        if key not in {"node_failure_error_basis_points", "movement_http_errors"} and value == 0:
            raise PolicyError(f"{key} must be positive")
        if direction == "max" and value > safety_limit:
            raise PolicyError(f"{key}={value} is weaker than the safety ceiling {safety_limit}")
        if direction == "min" and value < safety_limit:
            raise PolicyError(f"{key}={value} is weaker than the safety floor {safety_limit}")
        if key == "movement_throughput_retained_percent" and value > 100:
            raise PolicyError("movement_throughput_retained_percent cannot exceed 100")
    return json.loads(json.dumps(policy))


def load_policy(path: pathlib.Path, now: dt.datetime | None = None) -> tuple[dict, str, str]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PolicyError(f"could not open policy {path}: {exc}") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size <= 0 or before.st_size > MAX_POLICY_BYTES:
            raise PolicyError(f"policy must be a non-empty regular file no larger than {MAX_POLICY_BYTES} bytes")
        payload = bytearray()
        while len(payload) <= MAX_POLICY_BYTES:
            chunk = os.read(descriptor, min(1 << 16, MAX_POLICY_BYTES - len(payload) + 1))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != before.st_size or (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise PolicyError("policy changed while it was being read")
    finally:
        os.close(descriptor)
    if len(payload) > MAX_POLICY_BYTES:
        raise PolicyError(f"policy exceeds the {MAX_POLICY_BYTES}-byte bound")
    try:
        parsed = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError(f"policy is not valid UTF-8 JSON: {exc}") from exc
    policy = validate_policy(parsed, now)
    canonical = json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return policy, hashlib.sha256(canonical).hexdigest(), hashlib.sha256(payload).hexdigest()


def emitted_values(policy: dict, canonical_sha256: str, source_sha256: str) -> list[str]:
    deployment = policy["deployment"]
    thresholds = policy["thresholds"]
    return [
        canonical_sha256,
        source_sha256,
        deployment["high_volume_server_memory_per_process"],
        str(deployment["high_volume_server_smp"]),
        deployment["snapshot_catchup_server_memory_per_process"],
        str(deployment["snapshot_catchup_smp"]),
        deployment["bench_memory"],
        str(deployment["bench_smp"]),
        *(str(thresholds[key]) for key in THRESHOLD_LIMITS),
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True, type=pathlib.Path)
    parser.add_argument("--emit-values", action="store_true")
    args = parser.parse_args(argv)
    try:
        policy, canonical_sha256, source_sha256 = load_policy(args.policy)
    except PolicyError as exc:
        print(f"ABORT: {exc}", file=sys.stderr)
        return 2
    if args.emit_values:
        print("\n".join(emitted_values(policy, canonical_sha256, source_sha256)))
    else:
        print("PRODUCTION_SLO_POLICY PASSED")
        print(f"  policy: {policy['name']}")
        print(f"  sha256: {canonical_sha256}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
