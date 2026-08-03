#!/usr/bin/env python3
import copy
import importlib.util
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


def status(node_id: int, domain: str, led: int) -> dict:
    return {
        "clustered": True,
        "node_id": node_id,
        "cluster_uuid": "00112233445566778899aabbccddeeff",
        "failure_domain": domain,
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
        "uncommitted_raft_limit_bytes": 64 << 20,
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
        "control_leader": 1,
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
        return preflight.qualify(NODES, REVISION, resolver=self.resolver, fetcher=self.fetcher)


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

    def test_rejects_dirty_expected_revision(self):
        fixture = Fixture()
        with self.assertRaisesRegex(preflight.QualificationError, "clean"):
            preflight.qualify(NODES, f"{REVISION}-dirty", resolver=fixture.resolver, fetcher=fixture.fetcher)

    def test_rejects_non_production_endpoint(self):
        with self.assertRaisesRegex(preflight.QualificationError, "credentials"):
            preflight.normalize_endpoint("https://operator:secret@node1.example:8443")

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
        candidate = preflight.candidate_from_slo_report(
            {
                "version": 1,
                "candidate": {
                    "commit": "a" * 40,
                    "server": {"binary": "/candidate/server", "embedded_revision": REVISION, "sha256": "b" * 64},
                    "benchmark": {"binary": "/candidate/bench", "embedded_revision": REVISION, "sha256": "c" * 64},
                },
            }
        )
        self.assertEqual(candidate["server"]["embedded_revision"], REVISION)

    def test_rejects_obsolete_server_only_candidate_identity(self):
        with self.assertRaisesRegex(preflight.QualificationError, "obsolete"):
            preflight.candidate_from_slo_report(
                {"version": 1, "candidate": {"commit": "a" * 40, "embedded_revision": REVISION}}
            )


if __name__ == "__main__":
    unittest.main()
