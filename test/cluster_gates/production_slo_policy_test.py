#!/usr/bin/env python3
import datetime as dt
import json
import os
import pathlib
import tempfile
import unittest

import production_slo_policy as policy

NOW = dt.datetime(2026, 8, 4, 2, 0, tzinfo=dt.timezone.utc)


def valid_policy() -> dict:
    return {
        "version": 1,
        "name": "production-rf3-2026q3",
        "approval": {"authority": "TimeStar release board", "reference": "CHANGE-1234"},
        "approved_at": "2026-08-03T00:00:00Z",
        "expires_at": "2027-02-03T00:00:00Z",
        "deployment": {
            "high_volume_server_memory_per_process": "2G",
            "high_volume_server_smp": 4,
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


class ProductionSloPolicyTest(unittest.TestCase):
    def test_accepts_exact_v1_policy_at_the_existing_safety_envelope(self):
        self.assertEqual(policy.validate_policy(valid_policy(), NOW), valid_policy())

    def test_rejects_boolean_version_as_not_exact_v1(self):
        value = valid_policy()
        value["version"] = True
        with self.assertRaisesRegex(policy.PolicyError, "not exact version 1"):
            policy.validate_policy(value, NOW)

    def test_rejects_expired_or_not_yet_effective_approval(self):
        expired = valid_policy()
        expired["expires_at"] = "2026-08-04T01:00:00Z"
        with self.assertRaisesRegex(policy.PolicyError, "expired"):
            policy.validate_policy(expired, NOW)
        future = valid_policy()
        future["approved_at"] = "2026-08-04T03:00:00Z"
        with self.assertRaisesRegex(policy.PolicyError, "not effective"):
            policy.validate_policy(future, NOW)

    def test_rejects_unbounded_approval_lifetime(self):
        value = valid_policy()
        value["expires_at"] = "2028-08-04T00:00:00Z"
        with self.assertRaisesRegex(policy.PolicyError, "at most 366 days"):
            policy.validate_policy(value, NOW)

    def test_rejects_non_utf8_approval_text(self):
        value = valid_policy()
        value["approval"]["reference"] = "bad\ud800reference"
        with self.assertRaisesRegex(policy.PolicyError, "not valid UTF-8"):
            policy.validate_policy(value, NOW)

    def test_rejects_relaxed_failure_error_or_recovery_ceiling(self):
        for key, value in (("node_failure_error_basis_points", 5001), ("node_failure_recovery_ms", 30001)):
            candidate = valid_policy()
            candidate["thresholds"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(policy.PolicyError, "weaker than the safety ceiling"):
                policy.validate_policy(candidate, NOW)

    def test_rejects_relaxed_movement_floor_or_error_ceiling(self):
        for key, value, message in (
            ("movement_throughput_retained_percent", 9, "safety floor"),
            ("movement_http_errors", 101, "safety ceiling"),
        ):
            candidate = valid_policy()
            candidate["thresholds"][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(policy.PolicyError, message):
                policy.validate_policy(candidate, NOW)

    def test_rejects_boolean_threshold(self):
        value = valid_policy()
        value["thresholds"]["movement_http_errors"] = False
        with self.assertRaisesRegex(policy.PolicyError, "non-negative integer"):
            policy.validate_policy(value, NOW)

    def test_rejects_four_reactors_at_the_ingest_memory_floor(self):
        value = valid_policy()
        value["deployment"]["high_volume_server_memory_per_process"] = "1G"
        with self.assertRaisesRegex(policy.PolicyError, "must exceed 256 MiB per reactor"):
            policy.validate_policy(value, NOW)

    def test_rejects_unbounded_driver_or_nonfocused_snapshot_reactors(self):
        for key in ("bench_smp", "snapshot_catchup_smp"):
            value = valid_policy()
            value["deployment"][key] = 2
            with self.subTest(key=key), self.assertRaisesRegex(policy.PolicyError, "must remain 1"):
                policy.validate_policy(value, NOW)

    def test_rejects_obsolete_policy_shape(self):
        value = valid_policy()
        del value["thresholds"]["snapshot_install_ms"]
        with self.assertRaisesRegex(policy.PolicyError, "thresholds have an incomplete"):
            policy.validate_policy(value, NOW)

    def test_canonical_identity_ignores_formatting_but_source_identity_does_not(self):
        scratch = os.environ.get("GATE_TMP_ROOT") or os.environ.get("TMPDIR")
        with tempfile.TemporaryDirectory(dir=scratch) as directory_text:
            directory = pathlib.Path(directory_text)
            first = directory / "first.json"
            second = directory / "second.json"
            first.write_text(json.dumps(valid_policy(), indent=2), encoding="utf-8")
            second.write_text(json.dumps(valid_policy(), separators=(",", ":")), encoding="utf-8")
            first_policy, first_canonical, first_source = policy.load_policy(first, NOW)
            second_policy, second_canonical, second_source = policy.load_policy(second, NOW)
            self.assertEqual(first_policy, second_policy)
            self.assertEqual(first_canonical, second_canonical)
            self.assertNotEqual(first_source, second_source)

    def test_emits_fixed_order_values_for_the_shell_collector(self):
        value = valid_policy()
        emitted = policy.emitted_values(value, "a" * 64, "b" * 64)
        self.assertEqual(len(emitted), 16)
        self.assertEqual(emitted[:4], ["a" * 64, "b" * 64, "2G", "4"])
        self.assertEqual(emitted[-2:], ["10", "100"])

    def test_cli_validates_and_emits_the_collector_contract(self):
        scratch = os.environ.get("GATE_TMP_ROOT") or os.environ.get("TMPDIR")
        with tempfile.TemporaryDirectory(dir=scratch) as directory_text:
            path = pathlib.Path(directory_text) / "policy.json"
            path.write_text(json.dumps(valid_policy()), encoding="utf-8")
            self.assertEqual(policy.main(["--policy", str(path), "--emit-values"]), 0)

    def test_serial_collector_revalidates_policy_after_arms_before_publishing(self):
        collector = pathlib.Path(__file__).with_name("production_slo_report.sh").read_text(encoding="utf-8")
        initial = collector.find("POLICY_VALUES_BEFORE=$(python3 ./production_slo_policy.py")
        first_arm = collector.find('run_gate "one-node failure')
        final = collector.find("POLICY_VALUES_AFTER=$(python3 ./production_slo_policy.py")
        publication = collector.find('"slo_policy": {')
        self.assertGreaterEqual(initial, 0)
        self.assertGreater(first_arm, initial)
        self.assertGreater(final, first_arm)
        self.assertGreater(publication, final)
        for index, variable in enumerate(
            (
                "GATE_SERVER_MEMORY",
                "GATE_SERVER_SMP",
                "GATE_SNAPSHOT_SERVER_MEMORY",
                "GATE_SNAPSHOT_SERVER_SMP",
                "GATE_BENCH_MEMORY",
                "GATE_BENCH_SMP",
                "GATE_MAX_NODE_FAILURE_ERROR_BPS",
                "GATE_MAX_FAILOVER_RECOVERY_MS",
                "GATE_MAX_FAILOVER_QUERY_P99_MS",
                "GATE_MAX_SNAPSHOT_INSTALL_MS",
                "GATE_MAX_SNAPSHOT_CATCHUP_MS",
                "GATE_MAX_MOVEMENT_P99_MS",
                "GATE_MIN_DIP_PCT",
                "GATE_MAX_STORM_5XX",
            ),
            2,
        ):
            self.assertIn(f'{variable}="${{POLICY_VALUES[{index}]}}"', collector)


if __name__ == "__main__":
    unittest.main()
