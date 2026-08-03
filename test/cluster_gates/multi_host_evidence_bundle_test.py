#!/usr/bin/env python3
import copy
import hashlib
import json
import pathlib
import tempfile
import unittest

import multi_host_candidate_preflight_test as preflight_test
import multi_host_evidence_bundle as bundle


def report(timestamp: str) -> dict:
    value = preflight_test.Fixture().qualify()
    value.pop("expected_revision")
    value.pop("expected_server_smp")
    value.pop("expected_server_memory_bytes")
    source = preflight_test.candidate_report()
    value["candidate"] = copy.deepcopy(source["candidate"])
    value["slo_policy"] = copy.deepcopy(source["slo_policy"])
    value["expected_deployment"] = {
        "server_smp": source["settings"]["high_volume_server_smp"],
        "server_memory_per_process": source["settings"]["high_volume_server_memory_per_process"],
    }
    value["generated_at"] = timestamp
    return value


def recovered_report() -> dict:
    value = report("2026-08-04T01:05:00Z")
    for node in value["nodes"]:
        node["control_leader"] = 2
        node["control_leader_here"] = node["node_id"] == 2
        node["control_term"] = 10
        node["control_controller_leader"] = 2
        node["control_controller_term"] = 10
        node["control_commit_index"] = 120
        node["control_applied_index"] = 120
    value["nodes"][0]["vshards_led"] = 1365
    value["nodes"][1]["vshards_led"] = 1366
    return value


def restored_report() -> dict:
    value = recovered_report()
    value["cluster_uuid"] = "ffeeddccbbaa99887766554433221100"
    for node in value["nodes"]:
        node["cluster_uuid"] = value["cluster_uuid"]
        node["node_uuid"] = f"{node['node_id'] + 100:032x}"
        node["endpoint"] = f"https://restore{node['node_id']}.example:8443"
        node["resolved_addresses"] = [f"10.30.0.{node['node_id']}"]
        node["peers"] = [
            {"node": peer_id, "address": f"restore{peer_id}.example:8086"} for peer_id in (1, 2, 3)
        ]
    return value


class MultiHostEvidenceBundleTest(unittest.TestCase):
    def setUp(self):
        self.before = report("2026-08-04T01:00:00Z")
        self.after = recovered_report()

    def test_accepts_recovery_with_new_leadership_but_identical_stable_topology(self):
        paired = bundle.pair_reports(self.before, self.after, "bidirectional-partition")
        self.assertEqual(paired["cluster_identity"]["transition"], "preserved")
        self.assertEqual(paired["node_ids"], [1, 2, 3])

    def test_accepts_backup_restore_only_with_fresh_cluster_and_node_identities(self):
        paired = bundle.pair_reports(self.before, restored_report(), "backup-resume-restore")
        self.assertEqual(paired["cluster_identity"]["transition"], "fresh-restore")
        self.assertNotEqual(paired["cluster_identity"]["before_uuid"], paired["cluster_identity"]["after_uuid"])
        self.assertTrue(set(paired["node_uuids"]["before"]).isdisjoint(paired["node_uuids"]["after"]))

    def test_rejects_backup_restore_that_reuses_source_identity(self):
        restored = restored_report()
        restored["nodes"][2]["node_uuid"] = self.before["nodes"][1]["node_uuid"]
        with self.assertRaisesRegex(bundle.EvidenceError, "reused a source persistent node UUID"):
            bundle.pair_reports(self.before, restored, "backup-resume-restore")

    def test_rejects_backup_restore_without_fresh_cluster_uuid(self):
        restored = restored_report()
        restored["cluster_uuid"] = self.before["cluster_uuid"]
        for node in restored["nodes"]:
            node["cluster_uuid"] = restored["cluster_uuid"]
        with self.assertRaisesRegex(bundle.EvidenceError, "fresh cluster UUID"):
            bundle.pair_reports(self.before, restored, "backup-resume-restore")

    def test_rejects_backup_restore_on_source_peer_addresses(self):
        restored = restored_report()
        for source, target in zip(self.before["nodes"], restored["nodes"]):
            target["peers"] = copy.deepcopy(source["peers"])
        with self.assertRaisesRegex(bundle.EvidenceError, "source endpoint or inter-node peer address"):
            bundle.pair_reports(self.before, restored, "backup-resume-restore")

    def test_rejects_different_candidate(self):
        self.after["candidate"]["server"]["sha256"] = "d" * 64
        with self.assertRaisesRegex(bundle.EvidenceError, "different candidate identity"):
            bundle.pair_reports(self.before, self.after, "voter-stop-restart")

    def test_rejects_different_cluster(self):
        self.after["cluster_uuid"] = "f" * 32
        for node in self.after["nodes"]:
            node["cluster_uuid"] = "f" * 32
        with self.assertRaisesRegex(bundle.EvidenceError, "different cluster UUID"):
            bundle.pair_reports(self.before, self.after, "voter-stop-restart")

    def test_rejects_changed_failure_domain(self):
        self.after["nodes"][2]["failure_domain"] = "az-a-relabelled"
        with self.assertRaisesRegex(bundle.EvidenceError, "different node, peer, or stable-map topology"):
            bundle.pair_reports(self.before, self.after, "storage-failure-recovery")

    def test_rejects_changed_map_epoch(self):
        for node in self.after["nodes"]:
            node["map_epoch"] = 8
            node["serving_map_epoch"] = 8
        with self.assertRaisesRegex(bundle.EvidenceError, "stable-map topology"):
            bundle.pair_reports(self.before, self.after, "authenticated-rebalance")

    def test_rejects_memory_allocation_not_bound_to_deployment(self):
        self.after["nodes"][0]["server_memory_bytes"] = 1 << 30
        with self.assertRaisesRegex(bundle.EvidenceError, "expected memory allocation"):
            bundle.pair_reports(self.before, self.after, "voter-stop-restart")

    def test_rejects_non_monotonic_evidence_time(self):
        self.after["generated_at"] = self.before["generated_at"]
        with self.assertRaisesRegex(bundle.EvidenceError, "later generated_at"):
            bundle.pair_reports(self.before, self.after, "peer-certificate-recovery")

    def test_rejects_incomplete_node_record(self):
        del self.after["nodes"][0]["peers"]
        with self.assertRaisesRegex(bundle.EvidenceError, "incomplete or obsolete node record"):
            bundle.pair_reports(self.before, self.after, "voter-stop-restart")

    def test_rejects_boolean_where_offline_evidence_requires_an_integer(self):
        self.after["nodes"][0]["vshards_led"] = True
        with self.assertRaisesRegex(bundle.EvidenceError, "invalid vshards_led"):
            bundle.pair_reports(self.before, self.after, "voter-stop-restart")

    def test_cli_hashes_each_required_transcript_without_embedding_contents(self):
        with tempfile.TemporaryDirectory() as directory_text:
            directory = pathlib.Path(directory_text)
            before_path = directory / "before.json"
            after_path = directory / "after.json"
            before_path.write_text(json.dumps(self.before), encoding="utf-8")
            after_path.write_text(json.dumps(self.after), encoding="utf-8")
            logs = {}
            for name in ("infrastructure", "workload", "node1", "node2", "node3"):
                path = directory / f"{name}.log"
                path.write_text(f"exact evidence for {name}\n", encoding="utf-8")
                logs[name] = path
            output = directory / "bundle.json"
            rc = bundle.main(
                [
                    "--before",
                    str(before_path),
                    "--after",
                    str(after_path),
                    "--fault-arm",
                    "bidirectional-partition",
                    "--infrastructure-transcript",
                    str(logs["infrastructure"]),
                    "--workload-transcript",
                    str(logs["workload"]),
                    "--server-transcript",
                    f"1={logs['node1']}",
                    "--server-transcript",
                    f"2={logs['node2']}",
                    "--server-transcript",
                    f"3={logs['node3']}",
                    "--output",
                    str(output),
                ]
            )
            self.assertEqual(rc, 0)
            evidence = json.loads(output.read_text(encoding="utf-8"))
            expected = hashlib.sha256(logs["node2"].read_bytes()).hexdigest()
            self.assertEqual(evidence["transcripts"]["servers"]["2"]["sha256"], expected)
            self.assertNotIn("exact evidence", output.read_text(encoding="utf-8"))

    def test_cli_rejects_missing_server_transcript(self):
        with tempfile.TemporaryDirectory() as directory_text:
            directory = pathlib.Path(directory_text)
            before_path = directory / "before.json"
            after_path = directory / "after.json"
            before_path.write_text(json.dumps(self.before), encoding="utf-8")
            after_path.write_text(json.dumps(self.after), encoding="utf-8")
            log = directory / "evidence.log"
            log.write_text("evidence\n", encoding="utf-8")
            rc = bundle.main(
                [
                    "--before",
                    str(before_path),
                    "--after",
                    str(after_path),
                    "--fault-arm",
                    "voter-stop-restart",
                    "--infrastructure-transcript",
                    str(log),
                    "--workload-transcript",
                    str(log),
                    "--server-transcript",
                    f"1={log}",
                    "--server-transcript",
                    f"2={log}",
                    "--output",
                    str(directory / "bundle.json"),
                ]
            )
            self.assertEqual(rc, 2)

    def test_cli_rejects_one_file_reused_for_distinct_evidence_roles(self):
        with tempfile.TemporaryDirectory() as directory_text:
            directory = pathlib.Path(directory_text)
            before_path = directory / "before.json"
            after_path = directory / "after.json"
            before_path.write_text(json.dumps(self.before), encoding="utf-8")
            after_path.write_text(json.dumps(self.after), encoding="utf-8")
            shared = directory / "shared.log"
            shared.write_text("not distinct evidence\n", encoding="utf-8")
            node_logs = []
            for node_id in (1, 2, 3):
                path = directory / f"node{node_id}.log"
                path.write_text(f"node {node_id}\n", encoding="utf-8")
                node_logs.append(path)
            rc = bundle.main(
                [
                    "--before",
                    str(before_path),
                    "--after",
                    str(after_path),
                    "--fault-arm",
                    "voter-stop-restart",
                    "--infrastructure-transcript",
                    str(shared),
                    "--workload-transcript",
                    str(shared),
                    "--server-transcript",
                    f"1={node_logs[0]}",
                    "--server-transcript",
                    f"2={node_logs[1]}",
                    "--server-transcript",
                    f"3={node_logs[2]}",
                    "--output",
                    str(directory / "bundle.json"),
                ]
            )
            self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
