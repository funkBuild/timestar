#!/usr/bin/env python3
"""Bind one distinct-host fault arm to exact-v1 before/after candidate evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import ipaddress
import json
import os
import pathlib
import stat
import sys

import multi_host_candidate_preflight as preflight

MAX_PREFLIGHT_BYTES = 1 << 20
MAX_TRANSCRIPT_BYTES = 4 << 30
FAULT_ARMS = (
    "authenticated-rebalance",
    "voter-stop-restart",
    "bidirectional-partition",
    "storage-failure-recovery",
    "peer-certificate-recovery",
    "backup-resume-restore",
)


class EvidenceError(RuntimeError):
    pass


def parse_utc_timestamp(value: object, label: str) -> dt.datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise EvidenceError(f"{label} generated_at must be a UTC timestamp ending in Z")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise EvidenceError(f"{label} generated_at is invalid: {exc}") from exc
    if parsed.tzinfo != dt.timezone.utc:
        raise EvidenceError(f"{label} generated_at must be UTC")
    return parsed


def read_regular_file(path: pathlib.Path, maximum: int, label: str) -> tuple[bytes, dict]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise EvidenceError(f"could not open {label} {path}: {exc}") from exc
    digest = hashlib.sha256()
    payload = bytearray() if maximum == MAX_PREFLIGHT_BYTES else None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise EvidenceError(f"{label} {path} is not a regular file")
        if before.st_size <= 0:
            raise EvidenceError(f"{label} {path} is empty")
        if before.st_size > maximum:
            raise EvidenceError(f"{label} {path} exceeds the {maximum}-byte bound")
        total = 0
        while True:
            chunk = os.read(descriptor, min(1 << 20, maximum - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum:
                raise EvidenceError(f"{label} {path} exceeds the {maximum}-byte bound")
            digest.update(chunk)
            if payload is not None:
                payload.extend(chunk)
        after = os.fstat(descriptor)
        before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        if total != before.st_size or before_identity != after_identity:
            raise EvidenceError(f"{label} {path} changed while it was being read")
    finally:
        os.close(descriptor)
    return bytes(payload or b""), {
        "path": str(path),
        "bytes": before.st_size,
        "sha256": digest.hexdigest(),
    }


def load_preflight(path: pathlib.Path, label: str) -> tuple[dict, dict]:
    payload, identity = read_regular_file(path, MAX_PREFLIGHT_BYTES, label)
    try:
        report = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"{label} {path} is not valid UTF-8 JSON: {exc}") from exc
    validate_preflight(report, label)
    return report, identity


def validate_preflight(report: object, label: str) -> None:
    if not isinstance(report, dict) or type(report.get("version")) is not int or report.get("version") != 1:
        raise EvidenceError(f"{label} is not an exact-v1 multi-host preflight report")
    required = {"version", "generated_at", "candidate", "expected_deployment", "cluster_uuid", "nodes"}
    if set(report) != required:
        raise EvidenceError(f"{label} has an incomplete or obsolete exact-v1 report shape")
    try:
        candidate = preflight.candidate_from_slo_report(report)
    except preflight.QualificationError as exc:
        raise EvidenceError(f"{label} candidate identity is invalid: {exc}") from exc
    parse_utc_timestamp(report["generated_at"], label)
    cluster_uuid = report["cluster_uuid"]
    if (
        not isinstance(cluster_uuid, str)
        or len(cluster_uuid) != 32
        or any(character not in "0123456789abcdef" for character in cluster_uuid)
    ):
        raise EvidenceError(f"{label} cluster UUID is invalid")
    deployment = report["expected_deployment"]
    if not isinstance(deployment, dict) or set(deployment) != {"server_smp", "server_memory_per_process"}:
        raise EvidenceError(f"{label} expected deployment is incomplete")
    smp = deployment["server_smp"]
    memory = deployment["server_memory_per_process"]
    try:
        parsed_deployment = preflight.deployment_settings_from_slo_report(
            {
                "version": 1,
                "settings": {
                    "high_volume_server_smp": smp,
                    "high_volume_server_memory_per_process": memory,
                },
            }
        )
    except preflight.QualificationError as exc:
        raise EvidenceError(f"{label} expected deployment is invalid: {exc}") from exc
    if parsed_deployment != deployment:
        raise EvidenceError(f"{label} expected deployment is not canonical")

    nodes = report["nodes"]
    if not isinstance(nodes, list) or len(nodes) < 3:
        raise EvidenceError(f"{label} must contain at least three qualified nodes")
    required_node = {
        "endpoint",
        "resolved_addresses",
        "node_id",
        "cluster_uuid",
        "failure_domain",
        "embedded_revision",
        "peers",
        "server_smp",
        "uncommitted_raft_limit_bytes",
        "control_leader_here",
        "control_leader",
        "control_term",
        "control_controller_leader",
        "control_controller_term",
        "control_commit_index",
        "control_applied_index",
        "control_snapshot_index",
        "control_nodes",
        "control_voters",
        "map_epoch",
        "serving_map_epoch",
        "vshards_hosted",
        "vshards_led",
    }
    node_ids: set[int] = set()
    endpoints: set[str] = set()
    failure_domains: set[str] = set()
    endpoint_addresses: set[str] = set()
    peer_maps: list[dict[int, str]] = []
    for node in nodes:
        if not isinstance(node, dict) or set(node) != required_node:
            raise EvidenceError(f"{label} contains an incomplete or obsolete node record")
        node_id = node["node_id"]
        if type(node_id) is not int or node_id <= 0 or node_id in node_ids:
            raise EvidenceError(f"{label} contains an invalid or duplicate node ID {node_id!r}")
        node_ids.add(node_id)
        if not isinstance(node["endpoint"], str) or not node["endpoint"].startswith("https://"):
            raise EvidenceError(f"{label} node {node_id} has an invalid HTTPS endpoint")
        if node["endpoint"] in endpoints:
            raise EvidenceError(f"{label} repeats node endpoint {node['endpoint']}")
        endpoints.add(node["endpoint"])
        if not isinstance(node["failure_domain"], str) or not node["failure_domain"]:
            raise EvidenceError(f"{label} node {node_id} has an invalid failure domain")
        if node["failure_domain"] in failure_domains:
            raise EvidenceError(f"{label} repeats failure domain {node['failure_domain']}")
        failure_domains.add(node["failure_domain"])
        if node["cluster_uuid"] != cluster_uuid or node["embedded_revision"] != candidate["server"]["embedded_revision"]:
            raise EvidenceError(f"{label} node {node_id} is not bound to its report identity")
        if node["server_smp"] != smp:
            raise EvidenceError(f"{label} node {node_id} does not match the expected reactor count")
        if node["uncommitted_raft_limit_bytes"] != smp * preflight.UNCOMMITTED_RAFT_BYTES_PER_REACTOR:
            raise EvidenceError(f"{label} node {node_id} does not match the expected proposal budget")
        numeric_fields = (
            "server_smp",
            "uncommitted_raft_limit_bytes",
            "control_leader",
            "control_term",
            "control_controller_leader",
            "control_controller_term",
            "control_commit_index",
            "control_applied_index",
            "control_snapshot_index",
            "control_nodes",
            "control_voters",
            "map_epoch",
            "serving_map_epoch",
            "vshards_hosted",
            "vshards_led",
        )
        for key in numeric_fields:
            if type(node[key]) is not int or node[key] < 0:
                raise EvidenceError(f"{label} node {node_id} has invalid {key}={node[key]!r}")
        if node["vshards_led"] > node["vshards_hosted"]:
            raise EvidenceError(f"{label} node {node_id} leads more VShards than it hosts")
        if type(node["control_leader_here"]) is not bool:
            raise EvidenceError(f"{label} node {node_id} has an invalid control_leader_here flag")
        if node["control_nodes"] != len(nodes) or node["control_voters"] != len(nodes):
            raise EvidenceError(f"{label} node {node_id} does not report the complete all-voter topology")
        if node["map_epoch"] != node["serving_map_epoch"] or type(node["map_epoch"]) is not int or node["map_epoch"] <= 0:
            raise EvidenceError(f"{label} node {node_id} does not report one stable positive map epoch")
        if not isinstance(node["resolved_addresses"], list) or not node["resolved_addresses"]:
            raise EvidenceError(f"{label} node {node_id} has no resolved endpoint addresses")
        for address in node["resolved_addresses"]:
            try:
                parsed_address = ipaddress.ip_address(address)
            except (TypeError, ValueError) as exc:
                raise EvidenceError(f"{label} node {node_id} has invalid resolved address {address!r}") from exc
            if (
                parsed_address.is_loopback
                or parsed_address.is_unspecified
                or parsed_address.is_multicast
                or parsed_address.is_link_local
                or parsed_address.is_reserved
            ):
                raise EvidenceError(f"{label} node {node_id} has non-production resolved address {address}")
            if address in endpoint_addresses:
                raise EvidenceError(f"{label} repeats resolved endpoint address {address}")
            endpoint_addresses.add(address)
        peers = node["peers"]
        if not isinstance(peers, list):
            raise EvidenceError(f"{label} node {node_id} has an invalid peer map")
        peer_map: dict[int, str] = {}
        for peer in peers:
            if not isinstance(peer, dict) or set(peer) != {"node", "address"}:
                raise EvidenceError(f"{label} node {node_id} has an invalid peer record")
            peer_id = peer["node"]
            if type(peer_id) is not int or peer_id <= 0 or peer_id in peer_map or not isinstance(peer["address"], str):
                raise EvidenceError(f"{label} node {node_id} has an invalid or duplicate peer ID")
            peer_map[peer_id] = peer["address"]
        peer_maps.append(peer_map)
    for peer_map in peer_maps:
        if set(peer_map) != node_ids:
            raise EvidenceError(f"{label} peer maps do not cover the qualified node set")
    if any(peer_map != peer_maps[0] for peer_map in peer_maps[1:]):
        raise EvidenceError(f"{label} nodes disagree on their peer maps")
    if len(set(peer_maps[0].values())) != len(peer_maps[0]):
        raise EvidenceError(f"{label} peer map repeats an inter-node address")
    common_fields = (
        "control_leader",
        "control_term",
        "control_controller_leader",
        "control_controller_term",
        "control_nodes",
        "control_voters",
        "map_epoch",
        "serving_map_epoch",
    )
    for key in common_fields:
        if len({node[key] for node in nodes}) != 1:
            raise EvidenceError(f"{label} nodes disagree on {key}")
    control_leader = nodes[0]["control_leader"]
    if control_leader not in node_ids or nodes[0]["control_term"] <= 0:
        raise EvidenceError(f"{label} does not report a qualified positive-term Group-0 leader")
    for node in nodes:
        node_id = node["node_id"]
        if node["control_leader_here"] != (node_id == control_leader):
            raise EvidenceError(f"{label} node {node_id} has an inconsistent Group-0 leader flag")
        if (
            node["control_controller_leader"] != control_leader
            or node["control_controller_term"] != node["control_term"]
        ):
            raise EvidenceError(f"{label} node {node_id} has a stale Group-0 controller stamp")
        if node["control_applied_index"] != node["control_commit_index"] or node["control_commit_index"] <= 0:
            raise EvidenceError(f"{label} node {node_id} has unapplied or empty Group-0 state")
        if node["control_snapshot_index"] > node["control_applied_index"]:
            raise EvidenceError(f"{label} node {node_id} has a snapshot beyond its applied Group-0 state")
    if sum(node["vshards_hosted"] for node in nodes) != preflight.VSHARD_COUNT * 3:
        raise EvidenceError(f"{label} does not contain complete RF=3 hosting")
    if sum(node["vshards_led"] for node in nodes) != preflight.VSHARD_COUNT:
        raise EvidenceError(f"{label} does not contain exactly one leader per VShard")


def stable_node_identity(node: dict) -> dict:
    keys = (
        "endpoint",
        "resolved_addresses",
        "node_id",
        "cluster_uuid",
        "failure_domain",
        "embedded_revision",
        "peers",
        "server_smp",
        "uncommitted_raft_limit_bytes",
        "control_nodes",
        "control_voters",
        "map_epoch",
        "serving_map_epoch",
        "vshards_hosted",
    )
    return {key: node[key] for key in keys}


def pair_reports(before: dict, after: dict, fault_arm: str) -> dict:
    validate_preflight(before, "before report")
    validate_preflight(after, "after report")
    if fault_arm not in FAULT_ARMS:
        raise EvidenceError(f"unknown fault arm {fault_arm!r}")
    if parse_utc_timestamp(after["generated_at"], "after report") <= parse_utc_timestamp(
        before["generated_at"], "before report"
    ):
        raise EvidenceError("after report must have a later generated_at than before report")
    for key, description in (
        ("candidate", "candidate identity"),
        ("expected_deployment", "deployment profile"),
        ("cluster_uuid", "cluster UUID"),
    ):
        if after[key] != before[key]:
            raise EvidenceError(f"before and after reports use different {description}")
    before_nodes = {node["node_id"]: stable_node_identity(node) for node in before["nodes"]}
    after_nodes = {node["node_id"]: stable_node_identity(node) for node in after["nodes"]}
    if before_nodes != after_nodes:
        raise EvidenceError("before and after reports use different node, peer, or stable-map topology")
    return {
        "fault_arm": fault_arm,
        "candidate": before["candidate"],
        "expected_deployment": before["expected_deployment"],
        "cluster_uuid": before["cluster_uuid"],
        "node_ids": sorted(before_nodes),
    }


def parse_server_transcript(value: str) -> tuple[int, pathlib.Path]:
    node_text, separator, path_text = value.partition("=")
    if not separator or not node_text.isdecimal() or int(node_text) <= 0 or not path_text:
        raise EvidenceError("--server-transcript must be NODE_ID=PATH with a positive node ID")
    return int(node_text), pathlib.Path(path_text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", required=True, type=pathlib.Path)
    parser.add_argument("--after", required=True, type=pathlib.Path)
    parser.add_argument("--fault-arm", required=True, choices=FAULT_ARMS)
    parser.add_argument("--infrastructure-transcript", required=True, type=pathlib.Path)
    parser.add_argument("--workload-transcript", required=True, type=pathlib.Path)
    parser.add_argument("--server-transcript", required=True, action="append", metavar="NODE_ID=PATH")
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args(argv)

    try:
        before, before_identity = load_preflight(args.before, "before report")
        after, after_identity = load_preflight(args.after, "after report")
        paired = pair_reports(before, after, args.fault_arm)
        infrastructure = read_regular_file(
            args.infrastructure_transcript, MAX_TRANSCRIPT_BYTES, "infrastructure transcript"
        )[1]
        workload = read_regular_file(args.workload_transcript, MAX_TRANSCRIPT_BYTES, "workload transcript")[1]
        server_paths: dict[int, pathlib.Path] = {}
        for value in args.server_transcript:
            node_id, path = parse_server_transcript(value)
            if node_id in server_paths:
                raise EvidenceError(f"duplicate server transcript for node {node_id}")
            server_paths[node_id] = path
        if set(server_paths) != set(paired["node_ids"]):
            raise EvidenceError("server transcripts must cover each qualified node ID exactly once")
        evidence_paths = [
            args.before,
            args.after,
            args.infrastructure_transcript,
            args.workload_transcript,
            *server_paths.values(),
        ]
        resolved_paths = [path.resolve(strict=True) for path in evidence_paths]
        if len(set(resolved_paths)) != len(resolved_paths):
            raise EvidenceError("before, after, infrastructure, workload, and per-node transcripts must be distinct files")
        server_evidence = {
            str(node_id): read_regular_file(server_paths[node_id], MAX_TRANSCRIPT_BYTES, f"node {node_id} transcript")[1]
            for node_id in sorted(server_paths)
        }
        paired.update(
            {
                "version": 1,
                "generated_at": dt.datetime.now(dt.timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
                "before": before_identity | {"generated_at": before["generated_at"]},
                "after": after_identity | {"generated_at": after["generated_at"]},
                "transcripts": {
                    "infrastructure": infrastructure,
                    "workload": workload,
                    "servers": server_evidence,
                },
            }
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as destination:
            json.dump(paired, destination, indent=2, sort_keys=True)
            destination.write("\n")
    except (EvidenceError, OSError) as exc:
        print(f"ABORT: {exc}", file=sys.stderr)
        return 2
    print("MULTI_HOST_EVIDENCE_BUNDLE PASSED")
    print(f"  report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
