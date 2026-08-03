#!/usr/bin/env python3
"""Read-only exact-v1 preflight for an already deployed multi-host cluster."""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import json
import math
import pathlib
import re
import socket
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

MAX_RESPONSE_BYTES = 1 << 20
MAX_TOKEN_BYTES = 64 << 10
VSHARD_COUNT = 4096
UNCOMMITTED_RAFT_BYTES_PER_REACTOR = 64 << 20
DEFAULT_PEER_PORT = 8086
MAX_PEER_BASE_PORT = (1 << 16) - 3  # the data and Raft listeners use +1/+2


class QualificationError(RuntimeError):
    pass


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, _file_pointer, _code, _message, _headers, new_url):
        raise QualificationError(f"{request.full_url} redirected to {new_url}; redirects are forbidden")


def normalize_endpoint(value: str) -> tuple[str, str, int]:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname:
        raise QualificationError(f"invalid node endpoint {value!r}: expected https://host[:port]")
    if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise QualificationError(f"invalid node endpoint {value!r}: credentials, paths, queries, and fragments are forbidden")
    try:
        parsed_port = parsed.port
    except ValueError as exc:
        raise QualificationError(f"invalid node endpoint {value!r}: {exc}") from exc
    port = 443 if parsed_port is None else parsed_port
    if port == 0:
        raise QualificationError(f"invalid node endpoint {value!r}: port must be greater than zero")
    host = parsed.hostname
    rendered_host = f"[{host}]" if ":" in host else host
    return f"https://{rendered_host}:{port}", host, port


def normalize_peer_address(value: object, endpoint: str) -> tuple[str, str, int]:
    if not isinstance(value, str) or not value:
        raise QualificationError(f"{endpoint} reports an invalid empty peer address")
    parsed = urllib.parse.urlsplit(f"//{value}")
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise QualificationError(f"{endpoint} reports invalid peer address {value!r}")
    try:
        parsed_port = parsed.port
    except ValueError as exc:
        raise QualificationError(f"{endpoint} reports invalid peer address {value!r}: {exc}") from exc
    port = DEFAULT_PEER_PORT if parsed_port is None else parsed_port
    if port == 0 or port > MAX_PEER_BASE_PORT:
        raise QualificationError(f"{endpoint} reports peer address {value!r} outside the usable port range")
    host = parsed.hostname
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{rendered_host}:{port}", host, port


def resolve_addresses(host: str, port: int) -> set[str]:
    try:
        results = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise QualificationError(f"could not resolve {host}:{port}: {exc}") from exc
    addresses = {entry[4][0].split("%", 1)[0] for entry in results}
    if not addresses:
        raise QualificationError(f"{host}:{port} resolved to no stream address")
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if ip.is_loopback or ip.is_unspecified or ip.is_multicast or ip.is_link_local or ip.is_reserved:
            raise QualificationError(f"{host}:{port} resolves to non-production address {address}")
    return addresses


def read_bounded_text(path: pathlib.Path, maximum_bytes: int, label: str) -> str:
    with path.open("rb") as source:
        payload = source.read(maximum_bytes + 1)
    if len(payload) > maximum_bytes:
        raise QualificationError(f"{label} exceeds the {maximum_bytes}-byte bound")
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QualificationError(f"{label} is not UTF-8: {exc}") from exc


def fetch_json(url: str, timeout: float, opener: urllib.request.OpenerDirector, token: str | None) -> dict:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with opener.open(request, timeout=timeout) as response:
            payload = response.read(MAX_RESPONSE_BYTES + 1)
            if len(payload) > MAX_RESPONSE_BYTES:
                raise QualificationError(f"response from {url} exceeds {MAX_RESPONSE_BYTES} bytes")
            if response.status != 200:
                raise QualificationError(f"{url} returned HTTP {response.status}")
    except QualificationError:
        raise
    except (OSError, urllib.error.URLError) as exc:
        raise QualificationError(f"could not read {url}: {exc}") from exc
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError(f"{url} did not return valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise QualificationError(f"{url} returned a non-object JSON value")
    return value


def require_equal(status: dict, key: str, expected: object, endpoint: str) -> None:
    actual = status.get(key)
    if type(actual) is not type(expected) or actual != expected:
        raise QualificationError(f"{endpoint} reports {key}={status.get(key)!r}, expected {expected!r}")


def validate_bearer_token(token: str) -> str:
    token = token.strip()
    if not token:
        raise QualificationError("bearer token file is empty")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in token):
        raise QualificationError("bearer token contains an HTTP control character")
    if re.fullmatch(r"[A-Za-z0-9\-._~+/]+=*", token) is None:
        raise QualificationError("bearer token is not an RFC 6750 b64token")
    return token


def candidate_from_slo_report(report: object) -> dict:
    if not isinstance(report, dict) or type(report.get("version")) is not int or report.get("version") != 1:
        raise QualificationError("candidate SLO report is not exact version 1")
    candidate = report.get("candidate")
    if not isinstance(candidate, dict) or set(candidate) != {"commit", "server", "benchmark"}:
        raise QualificationError("candidate SLO report has an incomplete or obsolete candidate identity")
    commit = candidate["commit"]
    if not isinstance(commit, str) or len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise QualificationError("candidate SLO report has an invalid commit")
    for component in ("server", "benchmark"):
        identity = candidate[component]
        if not isinstance(identity, dict) or set(identity) != {"binary", "embedded_revision", "sha256"}:
            raise QualificationError(f"candidate SLO report has an invalid {component} identity")
        binary = identity["binary"]
        revision = identity["embedded_revision"]
        digest = identity["sha256"]
        if not isinstance(binary, str) or not binary:
            raise QualificationError(f"candidate SLO report has an invalid {component} binary path")
        if not isinstance(revision, str) or not revision or revision == "unknown" or revision.endswith("-dirty"):
            raise QualificationError(f"candidate SLO report has an unqualified {component} revision")
        if not isinstance(digest, str) or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise QualificationError(f"candidate SLO report has an invalid {component} SHA-256")
    if candidate["server"]["embedded_revision"] != candidate["benchmark"]["embedded_revision"]:
        raise QualificationError("candidate SLO report mixes server and benchmark revisions")
    return copy_json(candidate)


def deployment_settings_from_slo_report(report: object) -> dict:
    if not isinstance(report, dict) or type(report.get("version")) is not int or report.get("version") != 1:
        raise QualificationError("candidate SLO report is not exact version 1")
    settings = report.get("settings")
    if not isinstance(settings, dict):
        raise QualificationError("candidate SLO report has no deployment settings")
    reactors = settings.get("high_volume_server_smp")
    memory = settings.get("high_volume_server_memory_per_process")
    if type(reactors) is not int or reactors <= 0 or reactors > 1024:
        raise QualificationError("candidate SLO report has an invalid high-volume server reactor count")
    if not isinstance(memory, str) or re.fullmatch(r"[1-9][0-9]*[KMGT]?", memory) is None:
        raise QualificationError("candidate SLO report has an invalid high-volume server memory setting")
    return {"server_smp": reactors, "server_memory_per_process": memory}


def copy_json(value: object) -> object:
    """Copy JSON-compatible evidence without sharing caller-owned dictionaries."""
    return json.loads(json.dumps(value))


def qualify(
    nodes: list[str],
    expected_revision: str,
    expected_server_smp: int,
    resolver=resolve_addresses,
    fetcher=None,
) -> dict:
    if len(nodes) < 3:
        raise QualificationError("multi-host qualification requires at least three node endpoints")
    if not expected_revision or expected_revision == "unknown" or expected_revision.endswith("-dirty"):
        raise QualificationError("expected revision must be a clean, known embedded revision")
    if type(expected_server_smp) is not int or expected_server_smp <= 0 or expected_server_smp > 1024:
        raise QualificationError("expected server reactor count must be an integer from 1 through 1,024")
    expected_uncommitted_limit = expected_server_smp * UNCOMMITTED_RAFT_BYTES_PER_REACTOR

    normalized: list[tuple[str, str, int]] = [normalize_endpoint(node) for node in nodes]
    if len({endpoint for endpoint, _, _ in normalized}) != len(normalized):
        raise QualificationError("node endpoints must be unique")

    resolved: list[set[str]] = []
    for endpoint, host, port in normalized:
        addresses = resolver(host, port)
        for previous_endpoint, previous_addresses in zip((item[0] for item in normalized), resolved):
            overlap = addresses & previous_addresses
            if overlap:
                raise QualificationError(
                    f"{endpoint} and {previous_endpoint} resolve to the same address(es): {', '.join(sorted(overlap))}"
                )
        resolved.append(addresses)

    if fetcher is None:
        raise QualificationError("internal error: no JSON fetcher supplied")

    records = []
    strict_zero = (
        "unresolved_peers",
        "vshards_leaderless",
        "apply_lag_entries",
        "apply_groups_behind",
        "apply_failures",
        "raft_durability_failures",
        "tick_errors",
        "control_apply_lag_entries",
        "control_apply_failures",
        "control_tick_errors",
        "control_durability_failed",
    )
    for (endpoint, _, _), addresses in zip(normalized, resolved):
        version = fetcher(f"{endpoint}/version")
        status = fetcher(f"{endpoint}/cluster/status")
        require_equal(version, "component", "timestar_http_server", endpoint)
        require_equal(version, "git_commit", expected_revision, endpoint)
        for key, expected in (
            ("clustered", True),
            ("replicated", True),
            ("replication_factor", 3),
            ("healthy", True),
            ("reactor_count", expected_server_smp),
            ("snapshot_trigger", True),
            ("snapshot_production_limit_per_shard", 1),
            ("journal_shared", False),
            ("protocol_version", 1),
            ("control_enabled", True),
            ("control_hosted", True),
            ("control_initialized", True),
            ("control_locally_ready", True),
            ("control_voter", True),
            ("control_joint_config", False),
            ("control_current_term_commit", True),
            ("control_learners", 0),
            ("control_draining_nodes", 0),
            ("control_drain_references", 0),
            ("control_drain_blocked", False),
            ("control_removals_pending", 0),
        ):
            require_equal(status, key, expected, endpoint)
        require_equal(status, "uncommitted_raft_limit_bytes", expected_uncommitted_limit, endpoint)
        for key in strict_zero:
            require_equal(status, key, False if key == "control_durability_failed" else 0, endpoint)
        peers = status.get("peers")
        if not isinstance(peers, list):
            raise QualificationError(f"{endpoint} reports peers={peers!r}, expected an array")
        peer_map: dict[int, str] = {}
        for peer in peers:
            if not isinstance(peer, dict) or set(peer) != {"node", "address"}:
                raise QualificationError(f"{endpoint} reports an invalid peer record {peer!r}")
            peer_id = peer["node"]
            if type(peer_id) is not int or peer_id <= 0 or peer_id in peer_map:
                raise QualificationError(f"{endpoint} reports invalid or duplicate peer node {peer_id!r}")
            address, _, _ = normalize_peer_address(peer["address"], endpoint)
            peer_map[peer_id] = address
        records.append(
            {
                "endpoint": endpoint,
                "resolved_addresses": sorted(addresses),
                "node_id": status.get("node_id"),
                "cluster_uuid": status.get("cluster_uuid"),
                "failure_domain": status.get("failure_domain"),
                "embedded_revision": version["git_commit"],
                "peers": [{"node": peer_id, "address": peer_map[peer_id]} for peer_id in sorted(peer_map)],
                "server_smp": status.get("reactor_count"),
                "uncommitted_raft_limit_bytes": status.get("uncommitted_raft_limit_bytes"),
                "control_leader_here": status.get("control_leader_here"),
                "control_leader": status.get("control_leader"),
                "control_term": status.get("control_term"),
                "control_controller_leader": status.get("control_controller_leader"),
                "control_controller_term": status.get("control_controller_term"),
                "control_commit_index": status.get("control_commit_index"),
                "control_applied_index": status.get("control_applied_index"),
                "control_snapshot_index": status.get("control_snapshot_index"),
                "control_nodes": status.get("control_nodes"),
                "control_voters": status.get("control_voters"),
                "map_epoch": status.get("control_map_epoch"),
                "serving_map_epoch": status.get("control_serving_map_epoch"),
                "vshards_hosted": status.get("vshards_hosted"),
                "vshards_led": status.get("vshards_led"),
            }
        )

    node_ids = [record["node_id"] for record in records]
    if any(type(node_id) is not int or node_id <= 0 for node_id in node_ids) or len(set(node_ids)) != len(records):
        raise QualificationError("nodes must report distinct positive node IDs")
    cluster_uuids = {record["cluster_uuid"] for record in records}
    cluster_uuid = records[0]["cluster_uuid"]
    if (
        len(cluster_uuids) != 1
        or not isinstance(cluster_uuid, str)
        or len(cluster_uuid) != 32
        or any(character not in "0123456789abcdef" for character in cluster_uuid)
    ):
        raise QualificationError("nodes must report one common 32-character cluster UUID")
    domains = [record["failure_domain"] for record in records]
    if any(not isinstance(domain, str) or not domain for domain in domains) or len(set(domains)) != len(records):
        raise QualificationError("nodes must report distinct non-empty failure domains")

    expected_peer_ids = set(node_ids)
    peer_maps: list[dict[int, str]] = []
    for record in records:
        peer_map = {peer["node"]: peer["address"] for peer in record["peers"]}
        if set(peer_map) != expected_peer_ids:
            raise QualificationError(
                f"{record['endpoint']} peer IDs {sorted(peer_map)} do not match qualified node IDs {sorted(node_ids)}"
            )
        peer_maps.append(peer_map)
    if any(peer_map != peer_maps[0] for peer_map in peer_maps[1:]):
        raise QualificationError("nodes disagree on the configured peer address map")
    peer_resolved: dict[int, set[str]] = {}
    for peer_id, address in peer_maps[0].items():
        _, host, port = normalize_peer_address(address, f"peer {peer_id}")
        addresses = resolver(host, port)
        for previous_id, previous_addresses in peer_resolved.items():
            overlap = addresses & previous_addresses
            if overlap:
                raise QualificationError(
                    f"peer nodes {peer_id} and {previous_id} resolve to the same address(es): "
                    f"{', '.join(sorted(overlap))}"
                )
        peer_resolved[peer_id] = addresses

    numeric_fields = (
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
    for record in records:
        if type(record["control_leader_here"]) is not bool:
            raise QualificationError(
                f"{record['endpoint']} reports invalid control_leader_here={record['control_leader_here']!r}"
            )
        for key in numeric_fields:
            if type(record[key]) is not int or record[key] < 0:
                raise QualificationError(f"{record['endpoint']} reports invalid {key}={record[key]!r}")

    for key in (
        "control_leader",
        "control_term",
        "control_controller_leader",
        "control_controller_term",
        "control_nodes",
        "control_voters",
        "map_epoch",
        "serving_map_epoch",
    ):
        values = {record[key] for record in records}
        if len(values) != 1:
            raise QualificationError(f"nodes disagree on {key}: {sorted(values, key=str)}")
    if records[0]["control_leader"] not in node_ids:
        raise QualificationError("the common control leader is not one of the qualified nodes")
    if records[0]["control_term"] <= 0 or records[0]["control_controller_term"] <= 0:
        raise QualificationError("the qualified Group-0 term must be positive")
    if records[0]["control_nodes"] != len(records) or records[0]["control_voters"] != len(records):
        raise QualificationError("the supplied endpoints do not cover the complete all-voter control topology")
    for record in records:
        if record["control_leader_here"] != (record["node_id"] == record["control_leader"]):
            raise QualificationError(f"{record['endpoint']} reports an inconsistent control_leader_here flag")
        if (
            record["control_controller_leader"] != record["control_leader"]
            or record["control_controller_term"] != record["control_term"]
        ):
            raise QualificationError(f"{record['endpoint']} control controller is not stamped by the current leader term")
        if record["control_applied_index"] != record["control_commit_index"]:
            raise QualificationError(f"{record['endpoint']} has unapplied committed Group-0 entries")
        if record["control_commit_index"] <= 0:
            raise QualificationError(f"{record['endpoint']} has no committed Group-0 state")
        if record["control_snapshot_index"] > record["control_applied_index"]:
            raise QualificationError(f"{record['endpoint']} reports a Group-0 snapshot beyond its applied index")
    if records[0]["map_epoch"] != records[0]["serving_map_epoch"] or not isinstance(records[0]["map_epoch"], int) or records[0]["map_epoch"] <= 0:
        raise QualificationError("the control and serving maps are not at one positive stable epoch")
    if sum(record["vshards_led"] for record in records) != VSHARD_COUNT:
        raise QualificationError("the qualified nodes do not report exactly 4,096 leaders")
    if sum(record["vshards_hosted"] for record in records) != VSHARD_COUNT * 3:
        raise QualificationError("the qualified nodes do not report exactly RF=3 hosting for 4,096 VShards")

    return {
        "version": 1,
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "expected_revision": expected_revision,
        "expected_server_smp": expected_server_smp,
        "cluster_uuid": cluster_uuid,
        "nodes": records,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", action="append", required=True, help="base HTTPS endpoint; repeat for every node")
    parser.add_argument("--candidate-report", required=True, type=pathlib.Path, help="exact-v1 production SLO report")
    parser.add_argument("--output", required=True, type=pathlib.Path, help="new exact-v1 JSON evidence file")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--ca-file")
    parser.add_argument("--client-cert")
    parser.add_argument("--client-key")
    parser.add_argument("--bearer-token-file", type=pathlib.Path)
    args = parser.parse_args(argv)

    if not math.isfinite(args.timeout) or args.timeout <= 0 or args.timeout > 60:
        parser.error("--timeout must be greater than zero and at most 60 seconds")
    if bool(args.client_cert) != bool(args.client_key):
        parser.error("--client-cert and --client-key must be supplied together")
    try:
        context = ssl.create_default_context(cafile=args.ca_file)
        if args.client_cert:
            context.load_cert_chain(args.client_cert, args.client_key)
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), RejectRedirects())
        token = None
        if args.bearer_token_file:
            token = validate_bearer_token(
                read_bounded_text(args.bearer_token_file, MAX_TOKEN_BYTES, "bearer token file")
            )

        def live_fetch(url: str) -> dict:
            return fetch_json(url, args.timeout, opener, token)

        candidate_text = read_bounded_text(args.candidate_report, MAX_RESPONSE_BYTES, "candidate SLO report")
        candidate_report = json.loads(candidate_text)
        candidate = candidate_from_slo_report(candidate_report)
        deployment = deployment_settings_from_slo_report(candidate_report)
        report = qualify(
            args.node,
            candidate["server"]["embedded_revision"],
            deployment["server_smp"],
            fetcher=live_fetch,
        )
        report.pop("expected_revision")
        report.pop("expected_server_smp")
        report["candidate"] = candidate
        report["expected_deployment"] = deployment
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as destination:
            json.dump(report, destination, indent=2, sort_keys=True)
            destination.write("\n")
    except (QualificationError, OSError, ssl.SSLError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(f"ABORT: {exc}", file=sys.stderr)
        return 2
    print("MULTI_HOST_CANDIDATE_PREFLIGHT PASSED")
    print(f"  report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
