#!/usr/bin/env python3
"""
Functional tests for the TimeStar Python client library.

These tests require a running TimeStar server on localhost:8086.
Run with:
    pytest test_client.py -v

Or skip server-dependent tests:
    pytest test_client.py -v -k "not server"

Import-only tests always pass and verify the library loads correctly.
"""

from __future__ import annotations

import json
import os
import sys
import time

import pytest

# Allow running from the python/ directory or project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import timestar_pb2
from timestar_client import TimestarClient, TimestarError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SERVER_HOST = os.environ.get("TIMESTAR_HOST", "localhost")
SERVER_PORT = int(os.environ.get("TIMESTAR_PORT", "8086"))


def server_available() -> bool:
    """Check if a TimeStar server is reachable."""
    import requests
    try:
        resp = requests.get(f"http://{SERVER_HOST}:{SERVER_PORT}/health",
                            timeout=2)
        return resp.status_code == 200
    except Exception:
        return False


needs_server = pytest.mark.skipif(
    not server_available(),
    reason=f"TimeStar server not available at {SERVER_HOST}:{SERVER_PORT}"
)


@pytest.fixture(params=["protobuf", "json"])
def client(request):
    """Yields a client for each wire format."""
    c = TimestarClient(host=SERVER_HOST, port=SERVER_PORT,
                       format=request.param)
    yield c
    c.close()


@pytest.fixture
def proto_client():
    """Yields a protobuf-only client."""
    c = TimestarClient(host=SERVER_HOST, port=SERVER_PORT, format="protobuf")
    yield c
    c.close()


@pytest.fixture
def json_client():
    """Yields a JSON-only client."""
    c = TimestarClient(host=SERVER_HOST, port=SERVER_PORT, format="json")
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Import / construction tests (no server needed)
# ---------------------------------------------------------------------------

class TestImports:
    """Verify that all modules import correctly."""

    def test_import_proto(self):
        """Proto bindings import successfully."""
        assert hasattr(timestar_pb2, "WriteRequest")
        assert hasattr(timestar_pb2, "QueryRequest")
        assert hasattr(timestar_pb2, "QueryResponse")
        assert hasattr(timestar_pb2, "HealthResponse")

    def test_import_client(self):
        """Client module imports successfully."""
        assert TimestarClient is not None
        assert TimestarError is not None

    def test_client_construction(self):
        """Client can be constructed with default args."""
        c = TimestarClient()
        assert c.base_url == "http://localhost:8086"
        assert c.format == "protobuf"
        c.close()

    def test_client_json_mode(self):
        """Client can be constructed in JSON mode."""
        c = TimestarClient(format="json")
        assert c.format == "json"
        c.close()

    def test_client_invalid_format(self):
        """Client rejects invalid format."""
        with pytest.raises(ValueError, match="format must be"):
            TimestarClient(format="xml")

    def test_client_context_manager(self):
        """Client works as a context manager."""
        with TimestarClient() as c:
            assert c.format == "protobuf"

    def test_client_repr(self):
        """Client has a useful repr."""
        c = TimestarClient(host="myhost", port=9999, format="json")
        r = repr(c)
        assert "myhost" in r
        assert "9999" in r
        assert "json" in r
        c.close()


class TestProtoSerialization:
    """Test protobuf message construction without a server."""

    def test_write_request_roundtrip(self):
        """WriteRequest can be serialized and deserialized."""
        req = timestar_pb2.WriteRequest()
        wp = req.writes.add()
        wp.measurement = "test_measurement"
        wp.tags["host"] = "server-01"
        wp.timestamps.append(1000000000)

        wf = timestar_pb2.WriteField()
        wf.double_values.values.append(42.5)
        wp.fields["value"].CopyFrom(wf)

        data = req.SerializeToString()
        assert len(data) > 0

        req2 = timestar_pb2.WriteRequest()
        req2.ParseFromString(data)
        assert req2.writes[0].measurement == "test_measurement"
        assert req2.writes[0].tags["host"] == "server-01"
        assert list(req2.writes[0].timestamps) == [1000000000]
        assert req2.writes[0].fields["value"].double_values.values[0] == 42.5

    def test_query_request_roundtrip(self):
        """QueryRequest can be serialized and deserialized."""
        req = timestar_pb2.QueryRequest()
        req.query = "avg:cpu(usage){host:server-01}"
        req.start_time = 1000
        req.end_time = 2000
        req.aggregation_interval = "5m"

        data = req.SerializeToString()
        req2 = timestar_pb2.QueryRequest()
        req2.ParseFromString(data)

        assert req2.query == "avg:cpu(usage){host:server-01}"
        assert req2.start_time == 1000
        assert req2.end_time == 2000
        assert req2.aggregation_interval == "5m"

    def test_delete_request_roundtrip(self):
        """DeleteRequest serialization."""
        req = timestar_pb2.DeleteRequest()
        req.measurement = "cpu"
        req.tags["host"] = "server-01"
        req.field = "usage"
        req.start_time = 1000
        req.end_time = 2000

        data = req.SerializeToString()
        req2 = timestar_pb2.DeleteRequest()
        req2.ParseFromString(data)

        assert req2.measurement == "cpu"
        assert req2.tags["host"] == "server-01"

    def test_array_format_write(self):
        """Array-format (columnar) write with multiple timestamps."""
        req = timestar_pb2.WriteRequest()
        wp = req.writes.add()
        wp.measurement = "server.metrics"
        wp.tags["host"] = "host-01"

        for i in range(100):
            wp.timestamps.append(1000000000 + i * 60000000000)

        wf = timestar_pb2.WriteField()
        for i in range(100):
            wf.double_values.values.append(float(i) * 1.5)
        wp.fields["cpu_usage"].CopyFrom(wf)

        data = req.SerializeToString()
        assert len(data) > 0

        req2 = timestar_pb2.WriteRequest()
        req2.ParseFromString(data)
        assert len(req2.writes[0].timestamps) == 100
        assert len(req2.writes[0].fields["cpu_usage"].double_values.values) == 100


class TestBenchmarkDataGen:
    """Test the benchmark data generation functions."""

    def test_json_payload_structure(self):
        """JSON payload has the expected structure."""
        from timestar_bench import build_payload_json
        payload = build_payload_json(seed=42, host_id=1, rack_id=1,
                                     start_ts=1000000000, count=10)
        data = json.loads(payload)
        assert data["measurement"] == "server.metrics"
        assert "host" in data["tags"]
        assert "rack" in data["tags"]
        assert len(data["timestamps"]) == 10
        assert len(data["fields"]) == 10  # 10 field names
        for fname in data["fields"]:
            assert len(data["fields"][fname]) == 10

    def test_protobuf_payload_structure(self):
        """Protobuf payload has the expected structure."""
        from timestar_bench import build_payload_protobuf
        data = build_payload_protobuf(seed=42, host_id=1, rack_id=1,
                                      start_ts=1000000000, count=10)
        req = timestar_pb2.WriteRequest()
        req.ParseFromString(data)
        wp = req.writes[0]
        assert wp.measurement == "server.metrics"
        assert "host" in wp.tags
        assert "rack" in wp.tags
        assert len(wp.timestamps) == 10
        assert len(wp.fields) == 10
        for fname, wf in wp.fields.items():
            assert len(wf.double_values.values) == 10

    def test_deterministic_generation(self):
        """Same seed produces identical payloads."""
        from timestar_bench import build_payload_json, build_payload_protobuf
        j1 = build_payload_json(42, 1, 1, 1000, 5)
        j2 = build_payload_json(42, 1, 1, 1000, 5)
        assert j1 == j2

        p1 = build_payload_protobuf(42, 1, 1, 1000, 5)
        p2 = build_payload_protobuf(42, 1, 1, 1000, 5)
        assert p1 == p2

    def test_different_seeds_differ(self):
        """Different seeds produce different payloads."""
        from timestar_bench import build_payload_json
        j1 = build_payload_json(42, 1, 1, 1000, 5)
        j2 = build_payload_json(99, 1, 1, 1000, 5)
        assert j1 != j2


# ---------------------------------------------------------------------------
# Server-dependent tests
# ---------------------------------------------------------------------------

@needs_server
class TestHealthEndpoint:
    """Test the /health endpoint."""

    def test_health_check(self, client):
        """Health check returns status ok."""
        result = client.health()
        assert result["status"] == "ok"


@needs_server
class TestWriteAndQuery:
    """Test write + query round-trip."""

    MEASUREMENT = "pytest_test"

    def _write_test_data(self, client: TimestarClient, suffix: str = "") -> int:
        """Write test data and return the timestamp used."""
        ts = int(time.time() * 1_000_000_000)  # Current time in ns
        points = [{
            "measurement": f"{self.MEASUREMENT}{suffix}",
            "tags": {"host": "test-host", "env": "test"},
            "fields": {"value": 42.5, "count": 100.0},
            "timestamp": ts,
        }]
        result = client.write(points)
        assert result["status"] == "success"
        return ts

    def test_single_point_write(self, client):
        """Write a single point."""
        ts = self._write_test_data(client, f"_{client.format}_single")
        assert ts > 0

    def test_batch_write(self, client):
        """Write a batch of points."""
        base_ts = int(time.time() * 1_000_000_000)
        meas = f"{self.MEASUREMENT}_{client.format}_batch"
        points = []
        for i in range(10):
            points.append({
                "measurement": meas,
                "tags": {"host": f"host-{i:02d}"},
                "fields": {"value": float(i) * 10.0},
                "timestamp": base_ts + i * 1_000_000_000,
            })
        result = client.write(points)
        assert result["status"] == "success"

    def test_array_format_write(self, client):
        """Write using array format (timestamps + field arrays)."""
        base_ts = int(time.time() * 1_000_000_000)
        meas = f"{self.MEASUREMENT}_{client.format}_array"
        timestamps = [base_ts + i * 60_000_000_000 for i in range(50)]
        points = [{
            "measurement": meas,
            "tags": {"host": "host-01"},
            "timestamps": timestamps,
            "fields": {
                "cpu": [float(i % 100) for i in range(50)],
                "mem": [float(50 + i % 50) for i in range(50)],
            },
        }]
        result = client.write(points)
        assert result["status"] == "success"

    def test_write_then_query(self, client):
        """Write data and query it back."""
        base_ts = 1_800_000_000_000_000_000  # Far-future timestamp
        meas = f"{self.MEASUREMENT}_{client.format}_wq"
        points = [{
            "measurement": meas,
            "tags": {"host": "query-test"},
            "fields": {"temperature": 23.5},
            "timestamp": base_ts,
        }]
        result = client.write(points)
        assert result["status"] == "success"

        # Query it back
        qresult = client.query(
            f"latest:{meas}(temperature){{host:query-test}}",
            start_time=base_ts - 1_000_000_000,
            end_time=base_ts + 1_000_000_000,
        )
        assert qresult["status"] == "success"
        assert len(qresult["series"]) > 0


@needs_server
class TestMetadataEndpoints:
    """Test metadata API endpoints."""

    MEASUREMENT = "pytest_metadata"

    @pytest.fixture(autouse=True)
    def _setup_data(self, client):
        """Ensure test data exists for metadata queries."""
        ts = int(time.time() * 1_000_000_000)
        meas = f"{self.MEASUREMENT}_{client.format}"
        points = [{
            "measurement": meas,
            "tags": {"region": "us-west", "host": "meta-host"},
            "fields": {"cpu": 50.0, "mem": 70.0},
            "timestamp": ts,
        }]
        client.write(points)
        self._meas = meas

    def test_list_measurements(self, client):
        """List measurements includes our test measurement."""
        measurements = client.list_measurements()
        assert isinstance(measurements, list)
        # Our measurement should exist (it was just written)
        assert self._meas in measurements

    def test_list_tags(self, client):
        """List tags for a measurement."""
        tags = client.list_tags(self._meas)
        assert isinstance(tags, dict)
        assert "region" in tags or "host" in tags

    def test_list_fields(self, client):
        """List fields for a measurement."""
        fields = client.list_fields(self._meas)
        assert isinstance(fields, list)
        field_names = [f["name"] for f in fields]
        assert "cpu" in field_names or "mem" in field_names


@needs_server
class TestDeleteEndpoint:
    """Test the /delete endpoint."""

    def test_delete_by_measurement(self, client):
        """Delete data by measurement name."""
        ts = int(time.time() * 1_000_000_000)
        meas = f"pytest_delete_{client.format}_{int(ts)}"
        # Write some data
        points = [{
            "measurement": meas,
            "tags": {"host": "del-test"},
            "fields": {"value": 1.0},
            "timestamp": ts,
        }]
        client.write(points)

        # Delete it
        result = client.delete(measurement=meas)
        assert result["status"] == "success"


@needs_server
class TestErrorHandling:
    """Test error handling."""

    def test_invalid_query(self, client):
        """Invalid query string returns an error."""
        with pytest.raises(TimestarError):
            client.query("not a valid query", 0, 1)

    def test_missing_measurement_in_tags(self, client):
        """Tags query without measurement returns an error."""
        with pytest.raises(TimestarError):
            client.list_tags("")


@needs_server
class TestFormatComparison:
    """Test that protobuf and JSON return equivalent results."""

    def test_health_both_formats(self, proto_client, json_client):
        """Both formats return healthy status."""
        proto_result = proto_client.health()
        json_result = json_client.health()
        assert proto_result["status"] == json_result["status"] == "ok"

    def test_write_query_both_formats(self, proto_client, json_client):
        """Both formats write and query successfully."""
        base_ts = 1_900_000_000_000_000_000

        for label, c in [("proto", proto_client), ("json", json_client)]:
            meas = f"pytest_cmp_{label}"
            c.write([{
                "measurement": meas,
                "tags": {"src": label},
                "fields": {"val": 99.9},
                "timestamp": base_ts,
            }])
            result = c.query(f"latest:{meas}(val)",
                             start_time=base_ts - 1_000_000_000,
                             end_time=base_ts + 1_000_000_000)
            assert result["status"] == "success"
            assert len(result["series"]) > 0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
