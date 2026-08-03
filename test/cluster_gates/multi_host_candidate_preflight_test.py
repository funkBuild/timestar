#!/usr/bin/env python3
import copy
import hashlib
import importlib.util
import json
import pathlib
import tempfile
import unittest

MODULE_PATH = pathlib.Path(__file__).with_name("multi_host_candidate_preflight.py")
SPEC = importlib.util.spec_from_file_location("multi_host_candidate_preflight", MODULE_PATH)
preflight = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(preflight)

REVISION = "v1.2.1-571-gc28066d"
NODES = ["https://node1.example:8443", "https://node2.example:8443", "https://node3.example:8443"]
SERVER_SMP = 4
SERVER_MEMORY_BYTES = 2 << 30


def policy_document() -> dict:
    return {
        "version": 1,
        "name": "production-rf3-2026q3",
        "approval": {"authority": "release board", "reference": "CHANGE-1234"},
        "approved_at": "2026-08-03T00:00:00Z",
        "expires_at": "2027-02-04T00:00:00Z",
        "deployment": {
            "high_volume_server_memory_per_process": "2G",
            "high_volume_server_smp": SERVER_SMP,
            "snapshot_catchup_server_memory_per_process": "1G",
            "snapshot_catchup_smp": 1,
            "bench_memory": "1G",
            "bench_smp": 1,
        },
        "thresholds": {
            "node_failure_error_basis_points": 5000,
            "node_failure_recovery_ms": 30000,
            "node_failure_query_p99_ms": 2000,
            "snapshot_install_ms": 360000,
            "snapshot_catchup_ms": 750000,
            "movement_p99_ms": 5000,
            "movement_throughput_retained_percent": 10,
            "movement_http_errors": 100,
        },
    }


def candidate_report() -> dict:
    document = policy_document()
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "version": 1,
        "candidate": {
            "commit": "a" * 40,
            "server": {"binary": "/candidate/server", "embedded_revision": REVISION, "sha256": "b" * 64},
            "benchmark": {"binary": "/candidate/bench", "embedded_revision": REVISION, "sha256": "c" * 64},
        },
        "settings": {
            "high_volume_server_smp": SERVER_SMP,
            "high_volume_server_memory_per_process": "2G",
            "snapshot_catchup_server_memory_per_process": "1G",
            "snapshot_catchup_smp": 1,
            "bench_memory": "1G",
            "bench_smp": 1,
        },
        "thresholds": copy.deepcopy(document["thresholds"]),
        "slo_policy": {
            "status": "approved",
            "sha256": hashlib.sha256(canonical).hexdigest(),
            "source_sha256": "d" * 64,
            "document": document,
        },
    }


def status(node_id: int, domain: str, led: int) -> dict:
    return {
        "clustered": True,
        "node_id": node_id,
        "cluster_uuid": "00112233445566778899aabbccddeeff",
        "failure_domain": domain,
        "reactor_count": SERVER_SMP,
        "server_memory_bytes": SERVER_MEMORY_BYTES,
        "peers": [
            {"node": 1, "address": "node1.example:8086"},
            {"node": 2, "address": "node2.example:8086"},
            {"node": 3, "address": "node3.example:8086"},
        ],
        "replication_factor": 3,
        "replicated": True,
        "unresolved_peers": 0,
        "vshards_hosted": 4096,
        "vshards_led": led,
        "vshards_leaderless": 0,
        "healthy": True,
        "snapshot_trigger": True,
        "snapshot_production_limit_per_shard": 1,
        "journal_shared": False,
        "uncommitted_raft_limit_bytes": SERVER_SMP * (64 << 20),
        "apply_lag_entries": 0,
        "apply_groups_behind": 0,
        "apply_failures": 0,
        "raft_durability_failures": 0,
        "tick_errors": 0,
        "control_enabled": True,
        "control_hosted": True,
        "control_initialized": True,
        "control_locally_ready": True,
        "control_voter": True,
        "control_joint_config": False,
        "control_current_term_commit": True,
        "control_leader_here": node_id == 1,
        "control_leader": 1,
        "control_term": 9,
        "control_controller_leader": 1,
        "control_controller_term": 9,
        "control_commit_index": 100,
        "control_applied_index": 100,
        "control_snapshot_index": 80,
        "control_nodes": 3,
        "control_voters": 3,
        "control_learners": 0,
        "control_draining_nodes": 0,
        "control_drain_references": 0,
        "control_drain_blocked": False,
        "control_removals_pending": 0,
        "control_map_epoch": 7,
        "control_serving_map_epoch": 7,
        "control_apply_lag_entries": 0,
        "control_apply_failures": 0,
        "control_durability_failed": False,
        "control_tick_errors": 0,
        "protocol_version": 1,
    }


class Fixture:
    def __init__(self):
        self.addresses = {
            "node1.example": {"10.20.0.1"},
            "node2.example": {"10.20.0.2"},
            "node3.example": {"10.20.0.3"},
        }
        self.responses = {}
        for endpoint, node_id, domain, led in zip(NODES, (1, 2, 3), ("az-a", "az-b", "az-c"), (1366, 1365, 1365)):
            self.responses[f"{endpoint}/version"] = {
                "version": "1.3.0",
                "component": "timestar_http_server",
                "git_commit": REVISION,
            }
            self.responses[f"{endpoint}/cluster/status"] = status(node_id, domain, led)

    def resolver(self, host, _port):
        return self.addresses[host]

    def fetcher(self, url):
        return copy.deepcopy(self.responses[url])

    def qualify(self):
        return preflight.qualify(
            NODES,
            REVISION,
            SERVER_SMP,
            SERVER_MEMORY_BYTES,
            resolver=self.resolver,
            fetcher=self.fetcher,
        )


class MultiHostCandidatePreflightTest(unittest.TestCase):
    def test_accepts_one_exact_healthy_cluster_on_distinct_hosts(self):
        fixture = Fixture()
        report = fixture.qualify()
        self.assertEqual(report["version"], 1)
        self.assertEqual(report["expected_revision"], REVISION)
        self.assertEqual([node["node_id"] for node in report["nodes"]], [1, 2, 3])

    def test_rejects_shared_resolved_address(self):
        fixture = Fixture()
        fixture.addresses["node3.example"] = {"10.20.0.2"}
        with self.assertRaisesRegex(preflight.QualificationError, "same address"):
            fixture.qualify()

    def test_rejects_mixed_revision(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[1]}/version"]["git_commit"] = "v1-stale"
        with self.assertRaisesRegex(preflight.QualificationError, "git_commit"):
            fixture.qualify()

    def test_rejects_wrong_deployed_component(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[1]}/version"]["component"] = "timestar_insert_bench"
        with self.assertRaisesRegex(preflight.QualificationError, "component"):
            fixture.qualify()

    def test_rejects_duplicate_failure_domain(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[2]}/cluster/status"]["failure_domain"] = "az-b"
        with self.assertRaisesRegex(preflight.QualificationError, "failure domains"):
            fixture.qualify()

    def test_rejects_durability_failure(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["raft_durability_failures"] = 1
        with self.assertRaisesRegex(preflight.QualificationError, "raft_durability_failures"):
            fixture.qualify()

    def test_rejects_snapshotting_disabled(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["snapshot_trigger"] = False
        with self.assertRaisesRegex(preflight.QualificationError, "snapshot_trigger"):
            fixture.qualify()

    def test_rejects_optional_shared_journal(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["journal_shared"] = True
        with self.assertRaisesRegex(preflight.QualificationError, "journal_shared"):
            fixture.qualify()

    def test_rejects_incomplete_replica_totals(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["vshards_hosted"] = 4095
        with self.assertRaisesRegex(preflight.QualificationError, "RF=3 hosting"):
            fixture.qualify()

    def test_rejects_one_reactor_budget_for_four_reactor_candidate(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["uncommitted_raft_limit_bytes"] = 64 << 20
        with self.assertRaisesRegex(preflight.QualificationError, "uncommitted_raft_limit_bytes"):
            fixture.qualify()

    def test_rejects_one_reactor_process_for_four_reactor_candidate(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["reactor_count"] = 1
        with self.assertRaisesRegex(preflight.QualificationError, "reactor_count"):
            fixture.qualify()

    def test_rejects_wrong_memory_allocation_for_candidate_profile(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["server_memory_bytes"] = 1 << 30
        with self.assertRaisesRegex(preflight.QualificationError, "server_memory_bytes"):
            fixture.qualify()

    def test_rejects_boolean_where_an_integer_status_counter_is_required(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[0]}/cluster/status"]["apply_failures"] = False
        with self.assertRaisesRegex(preflight.QualificationError, "apply_failures"):
            fixture.qualify()

    def test_rejects_peer_map_disagreement(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[2]}/cluster/status"]["peers"][1]["address"] = "other.example:8086"
        with self.assertRaisesRegex(preflight.QualificationError, "disagree on the configured peer"):
            fixture.qualify()

    def test_rejects_peer_map_that_does_not_cover_the_qualified_nodes(self):
        fixture = Fixture()
        for endpoint in NODES:
            fixture.responses[f"{endpoint}/cluster/status"]["peers"].pop()
        with self.assertRaisesRegex(preflight.QualificationError, "do not match qualified node IDs"):
            fixture.qualify()

    def test_rejects_peer_addresses_on_the_same_resolved_host(self):
        fixture = Fixture()
        for endpoint in NODES:
            fixture.responses[f"{endpoint}/cluster/status"]["peers"][2]["address"] = "node2.example:8086"
        with self.assertRaisesRegex(preflight.QualificationError, "peer nodes 3 and 2 resolve to the same"):
            fixture.qualify()

    def test_rejects_inconsistent_control_leader_flag(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[1]}/cluster/status"]["control_leader_here"] = True
        with self.assertRaisesRegex(preflight.QualificationError, "inconsistent control_leader_here"):
            fixture.qualify()

    def test_rejects_stale_control_controller_stamp(self):
        fixture = Fixture()
        for endpoint in NODES:
            fixture.responses[f"{endpoint}/cluster/status"]["control_controller_term"] = 8
        with self.assertRaisesRegex(preflight.QualificationError, "not stamped by the current leader term"):
            fixture.qualify()

    def test_rejects_unapplied_control_entry(self):
        fixture = Fixture()
        fixture.responses[f"{NODES[1]}/cluster/status"]["control_applied_index"] = 99
        with self.assertRaisesRegex(preflight.QualificationError, "unapplied committed Group-0"):
            fixture.qualify()

    def test_rejects_dirty_expected_revision(self):
        fixture = Fixture()
        with self.assertRaisesRegex(preflight.QualificationError, "clean"):
            preflight.qualify(
                NODES,
                f"{REVISION}-dirty",
                SERVER_SMP,
                SERVER_MEMORY_BYTES,
                resolver=fixture.resolver,
                fetcher=fixture.fetcher,
            )

    def test_rejects_non_production_endpoint(self):
        with self.assertRaisesRegex(preflight.QualificationError, "credentials"):
            preflight.normalize_endpoint("https://operator:secret@node1.example:8443")

    def test_rejects_plain_http_endpoint(self):
        with self.assertRaisesRegex(preflight.QualificationError, "expected https"):
            preflight.normalize_endpoint("http://node1.example:8086")

    def test_rejects_zero_endpoint_port_instead_of_replacing_it_with_the_default(self):
        with self.assertRaisesRegex(preflight.QualificationError, "greater than zero"):
            preflight.normalize_endpoint("https://node1.example:0")

    def test_rejects_bearer_header_injection(self):
        with self.assertRaisesRegex(preflight.QualificationError, "control character"):
            preflight.validate_bearer_token("good\nInjected: header")

    def test_rejects_non_bearer_token_grammar(self):
        with self.assertRaisesRegex(preflight.QualificationError, "RFC 6750"):
            preflight.validate_bearer_token("two words")

    def test_rejects_redirects(self):
        request = preflight.urllib.request.Request("https://node1.example:8443/version")
        with self.assertRaisesRegex(preflight.QualificationError, "redirects are forbidden"):
            preflight.RejectRedirects().redirect_request(
                request, None, 302, "Found", {}, "https://other.example:8443/version"
            )

    def test_bounds_file_reads_after_open(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "evidence.json"
            path.write_bytes(b"12345")
            with self.assertRaisesRegex(preflight.QualificationError, "4-byte bound"):
                preflight.read_bounded_text(path, 4, "test evidence")

    def test_accepts_complete_slo_candidate_identity(self):
        candidate = preflight.candidate_from_slo_report(candidate_report())
        self.assertEqual(candidate["server"]["embedded_revision"], REVISION)

    def test_accepts_explicit_high_volume_deployment_settings(self):
        settings = preflight.deployment_settings_from_slo_report(candidate_report())
        self.assertEqual(settings, {"server_smp": 4, "server_memory_per_process": "2G"})

    def test_accepts_policy_bound_to_the_report_thresholds_and_settings(self):
        evidence = preflight.approved_policy_from_slo_report(candidate_report())
        self.assertEqual(evidence["status"], "approved")

    def test_rejects_provisional_slo_report_for_multi_host_qualification(self):
        report = candidate_report()
        report["slo_policy"] = {"status": "provisional", "sha256": None, "source_sha256": None, "document": None}
        with self.assertRaisesRegex(preflight.QualificationError, "provisional"):
            preflight.approved_policy_from_slo_report(report)

    def test_rejects_threshold_not_bound_to_approved_policy(self):
        report = candidate_report()
        report["thresholds"]["node_failure_recovery_ms"] = 29999
        with self.assertRaisesRegex(preflight.QualificationError, "thresholds do not match"):
            preflight.approved_policy_from_slo_report(report)

    def test_rejects_policy_document_not_bound_to_its_canonical_hash(self):
        report = candidate_report()
        report["slo_policy"]["sha256"] = "e" * 64
        with self.assertRaisesRegex(preflight.QualificationError, "canonical SHA-256"):
            preflight.approved_policy_from_slo_report(report)

    def test_rejects_deployment_not_bound_to_approved_policy(self):
        report = candidate_report()
        report["settings"]["high_volume_server_smp"] = 3
        with self.assertRaisesRegex(preflight.QualificationError, "does not match"):
            preflight.approved_policy_from_slo_report(report)

    def test_rejects_boolean_report_version_as_not_exact_v1(self):
        report = candidate_report()
        report["version"] = True
        with self.assertRaisesRegex(preflight.QualificationError, "not exact version 1"):
            preflight.candidate_from_slo_report(report)

    def test_rejects_boolean_high_volume_reactor_count(self):
        report = candidate_report()
        report["settings"]["high_volume_server_smp"] = True
        with self.assertRaisesRegex(preflight.QualificationError, "reactor count"):
            preflight.deployment_settings_from_slo_report(report)

    def test_rejects_invalid_high_volume_memory_setting(self):
        report = candidate_report()
        report["settings"]["high_volume_server_memory_per_process"] = "a lot"
        with self.assertRaisesRegex(preflight.QualificationError, "memory setting"):
            preflight.deployment_settings_from_slo_report(report)

    def test_rejects_obsolete_server_only_candidate_identity(self):
        with self.assertRaisesRegex(preflight.QualificationError, "obsolete"):
            preflight.candidate_from_slo_report(
                {"version": 1, "candidate": {"commit": "a" * 40, "embedded_revision": REVISION}}
            )


if __name__ == "__main__":
    unittest.main()
