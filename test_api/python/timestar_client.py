"""
TimeStar Python Client Library

Supports both protobuf and JSON wire formats for communicating with the
TimeStar time series database HTTP API.

Usage:
    from timestar_client import TimestarClient

    client = TimestarClient(host="localhost", port=8086, format="protobuf")
    client.write([{
        "measurement": "cpu",
        "tags": {"host": "server-01"},
        "fields": {"usage": 73.5},
        "timestamp": 1704067200000000000,
    }])
    result = client.query("avg:cpu()", start_time=..., end_time=...)
"""

from __future__ import annotations

import json
import time
from typing import Any
from urllib.parse import urlencode

import requests

import timestar_pb2


class TimestarError(Exception):
    """Base exception for TimeStar client errors."""

    def __init__(self, message: str, status_code: int | None = None,
                 error_code: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code


class TimestarClient:
    """Client for TimeStar time series database.

    Supports both protobuf and JSON wire formats. Protobuf is more efficient
    for large payloads; JSON is useful for debugging and compatibility.

    Args:
        host: Server hostname.
        port: Server port (default 8086).
        format: Wire format - "protobuf" or "json" (default "protobuf").
        timeout: Request timeout in seconds (default 30).
    """

    PROTO_CONTENT_TYPE = "application/x-protobuf"
    JSON_CONTENT_TYPE = "application/json"

    def __init__(self, host: str = "localhost", port: int = 8086,
                 format: str = "protobuf", timeout: float = 30.0):
        if format not in ("protobuf", "json"):
            raise ValueError(f"format must be 'protobuf' or 'json', got '{format}'")
        self.base_url = f"http://{host}:{port}"
        self.format = format
        self.timeout = timeout
        self.session = requests.Session()

    @property
    def _content_type(self) -> str:
        return self.PROTO_CONTENT_TYPE if self.format == "protobuf" else self.JSON_CONTENT_TYPE

    @property
    def _accept(self) -> str:
        return self._content_type

    def _headers(self, **extra: str) -> dict[str, str]:
        h = {"Content-Type": self._content_type, "Accept": self._accept}
        h.update(extra)
        return h

    def _check_response(self, resp: requests.Response) -> None:
        """Raise TimestarError on non-2xx responses."""
        if 200 <= resp.status_code < 300:
            return
        # Try to extract error detail
        msg = f"HTTP {resp.status_code}"
        try:
            if self.format == "protobuf":
                sr = timestar_pb2.StatusResponse()
                sr.ParseFromString(resp.content)
                if sr.message:
                    msg = f"{msg}: {sr.message}"
            else:
                data = resp.json()
                if "error" in data:
                    err = data["error"]
                    if isinstance(err, dict):
                        msg = f"{msg}: {err.get('message', str(err))}"
                    else:
                        msg = f"{msg}: {err}"
                elif "message" in data:
                    msg = f"{msg}: {data['message']}"
        except Exception:
            if resp.text:
                msg = f"{msg}: {resp.text[:256]}"
        raise TimestarError(msg, status_code=resp.status_code)

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """GET /health - Check server health.

        Returns:
            dict with "status" key (value "ok" if healthy).
        """
        resp = self.session.get(
            f"{self.base_url}/health",
            headers={"Accept": self._accept},
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            msg = timestar_pb2.HealthResponse()
            msg.ParseFromString(resp.content)
            return {"status": msg.status}
        return resp.json()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, points: list[dict[str, Any]]) -> dict[str, Any]:
        """POST /write - Write data points.

        Each point dict may contain:
            measurement (str): Measurement name (required).
            tags (dict[str, str]): Tag key-value pairs.
            fields (dict[str, Any]): Field key-value pairs.
            timestamp (int): Single timestamp in nanoseconds.
            timestamps (list[int]): Array of timestamps for columnar writes.

        For columnar (array) writes, field values should be lists matching
        the length of timestamps.

        Returns:
            dict with "status", "points_written", etc.
        """
        if self.format == "protobuf":
            return self._write_protobuf(points)
        return self._write_json(points)

    def _write_json(self, points: list[dict[str, Any]]) -> dict[str, Any]:
        if len(points) == 1:
            body = points[0]
        else:
            body = {"writes": points}
        resp = self.session.post(
            f"{self.base_url}/write",
            headers=self._headers(),
            data=json.dumps(body),
            timeout=self.timeout,
        )
        self._check_response(resp)
        return resp.json()

    def _write_protobuf(self, points: list[dict[str, Any]]) -> dict[str, Any]:
        req = timestar_pb2.WriteRequest()
        for pt in points:
            wp = req.writes.add()
            wp.measurement = pt["measurement"]
            if "tags" in pt:
                for k, v in pt["tags"].items():
                    wp.tags[k] = v
            # Handle timestamps
            if "timestamps" in pt:
                for ts in pt["timestamps"]:
                    wp.timestamps.append(ts)
            elif "timestamp" in pt:
                wp.timestamps.append(pt["timestamp"])
            # Handle fields
            if "fields" in pt:
                for fname, fval in pt["fields"].items():
                    wf = timestar_pb2.WriteField()
                    if isinstance(fval, list):
                        # Array/columnar field
                        if len(fval) == 0:
                            continue
                        sample = fval[0]
                        if isinstance(sample, float) or isinstance(sample, int) and not isinstance(sample, bool):
                            dv = wf.double_values
                            for v in fval:
                                dv.values.append(float(v))
                        elif isinstance(sample, bool):
                            bv = wf.bool_values
                            for v in fval:
                                bv.values.append(v)
                        elif isinstance(sample, str):
                            sv = wf.string_values
                            for v in fval:
                                sv.values.append(v)
                    else:
                        # Scalar field — wrap in single-element array
                        if isinstance(fval, bool):
                            wf.bool_values.values.append(fval)
                        elif isinstance(fval, float):
                            wf.double_values.values.append(fval)
                        elif isinstance(fval, int):
                            wf.double_values.values.append(float(fval))
                        elif isinstance(fval, str):
                            wf.string_values.values.append(fval)
                    wp.fields[fname].CopyFrom(wf)
        data = req.SerializeToString()
        resp = self.session.post(
            f"{self.base_url}/write",
            headers=self._headers(),
            data=data,
            timeout=self.timeout,
        )
        self._check_response(resp)
        wr = timestar_pb2.WriteResponse()
        wr.ParseFromString(resp.content)
        return {
            "status": wr.status,
            "points_written": wr.points_written,
            "failed_writes": wr.failed_writes,
            "errors": list(wr.errors),
        }

    def write_raw_protobuf(self, data: bytes) -> dict[str, Any]:
        """POST /write with pre-serialized protobuf bytes.

        Useful for benchmarks where serialization cost is measured
        separately from network round-trip.

        Args:
            data: Serialized WriteRequest protobuf bytes.

        Returns:
            dict with "status", "points_written", etc.
        """
        resp = self.session.post(
            f"{self.base_url}/write",
            headers=self._headers(),
            data=data,
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            wr = timestar_pb2.WriteResponse()
            wr.ParseFromString(resp.content)
            return {
                "status": wr.status,
                "points_written": wr.points_written,
                "failed_writes": wr.failed_writes,
                "errors": list(wr.errors),
            }
        return resp.json()

    def write_raw_json(self, data: str) -> dict[str, Any]:
        """POST /write with pre-serialized JSON string.

        Useful for benchmarks where serialization cost is measured
        separately from network round-trip.
        """
        resp = self.session.post(
            f"{self.base_url}/write",
            headers={"Content-Type": self.JSON_CONTENT_TYPE,
                     "Accept": self._accept},
            data=data,
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            wr = timestar_pb2.WriteResponse()
            wr.ParseFromString(resp.content)
            return {
                "status": wr.status,
                "points_written": wr.points_written,
            }
        return resp.json()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query(self, query_str: str, start_time: int, end_time: int,
              aggregation_interval: str | None = None) -> dict[str, Any]:
        """POST /query - Execute a time series query.

        Args:
            query_str: Query string, e.g. "avg:temperature(value){location:us-west}".
            start_time: Start time in nanoseconds since epoch.
            end_time: End time in nanoseconds since epoch.
            aggregation_interval: Optional interval string, e.g. "5m" or "1h".

        Returns:
            dict with "status", "series", "statistics".
        """
        if self.format == "protobuf":
            return self._query_protobuf(query_str, start_time, end_time,
                                        aggregation_interval)
        return self._query_json(query_str, start_time, end_time,
                                aggregation_interval)

    def _query_json(self, query_str: str, start_time: int, end_time: int,
                    aggregation_interval: str | None) -> dict[str, Any]:
        body: dict[str, Any] = {
            "query": query_str,
            "startTime": start_time,
            "endTime": end_time,
        }
        if aggregation_interval:
            body["aggregationInterval"] = aggregation_interval
        resp = self.session.post(
            f"{self.base_url}/query",
            headers=self._headers(),
            data=json.dumps(body),
            timeout=self.timeout,
        )
        self._check_response(resp)
        return resp.json()

    def _query_protobuf(self, query_str: str, start_time: int, end_time: int,
                        aggregation_interval: str | None) -> dict[str, Any]:
        req = timestar_pb2.QueryRequest()
        req.query = query_str
        req.start_time = start_time
        req.end_time = end_time
        if aggregation_interval:
            req.aggregation_interval = aggregation_interval
        resp = self.session.post(
            f"{self.base_url}/query",
            headers=self._headers(),
            data=req.SerializeToString(),
            timeout=self.timeout,
        )
        self._check_response(resp)
        qr = timestar_pb2.QueryResponse()
        qr.ParseFromString(resp.content)
        return self._query_response_to_dict(qr)

    @staticmethod
    def _query_response_to_dict(qr: timestar_pb2.QueryResponse) -> dict[str, Any]:
        """Convert protobuf QueryResponse to a dict matching JSON format."""
        result: dict[str, Any] = {"status": qr.status, "series": []}
        for sr in qr.series:
            series_dict: dict[str, Any] = {
                "measurement": sr.measurement,
                "tags": dict(sr.tags),
                "fields": {},
            }
            for fname, fdata in sr.fields.items():
                field_dict: dict[str, Any] = {
                    "timestamps": list(fdata.timestamps),
                }
                which = fdata.WhichOneof("typed_values")
                if which == "double_values":
                    field_dict["values"] = list(fdata.double_values.values)
                elif which == "int64_values":
                    field_dict["values"] = list(fdata.int64_values.values)
                elif which == "bool_values":
                    field_dict["values"] = list(fdata.bool_values.values)
                elif which == "string_values":
                    field_dict["values"] = list(fdata.string_values.values)
                else:
                    field_dict["values"] = []
                series_dict["fields"][fname] = field_dict
            result["series"].append(series_dict)
        if qr.HasField("statistics"):
            result["statistics"] = {
                "series_count": qr.statistics.series_count,
                "point_count": qr.statistics.point_count,
                "execution_time_ms": qr.statistics.execution_time_ms,
            }
        if qr.error_code:
            result["error"] = {
                "code": qr.error_code,
                "message": qr.error_message,
            }
        return result

    def query_raw_protobuf(self, data: bytes) -> bytes:
        """POST /query with pre-serialized protobuf, return raw response bytes.

        For benchmarks that want to measure pure network throughput.
        """
        resp = self.session.post(
            f"{self.base_url}/query",
            headers=self._headers(),
            data=data,
            timeout=self.timeout,
        )
        self._check_response(resp)
        return resp.content

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete(self, measurement: str | None = None,
               tags: dict[str, str] | None = None,
               field: str | None = None,
               fields: list[str] | None = None,
               start_time: int | None = None,
               end_time: int | None = None,
               series: str | None = None) -> dict[str, Any]:
        """POST /delete - Delete data.

        Args:
            measurement: Measurement name.
            tags: Tag filters.
            field: Single field name.
            fields: Multiple field names.
            start_time: Start of time range (ns).
            end_time: End of time range (ns).
            series: Series key string (legacy format).

        Returns:
            dict with "status", "deleted_count", etc.
        """
        if self.format == "protobuf":
            return self._delete_protobuf(measurement, tags, field, fields,
                                         start_time, end_time, series)
        return self._delete_json(measurement, tags, field, fields,
                                 start_time, end_time, series)

    def _delete_json(self, measurement, tags, field, fields,
                     start_time, end_time, series) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if series:
            body["series"] = series
        if measurement:
            body["measurement"] = measurement
        if tags:
            body["tags"] = tags
        if field:
            body["field"] = field
        if fields:
            body["fields"] = fields
        if start_time is not None:
            body["startTime"] = start_time
        if end_time is not None:
            body["endTime"] = end_time
        resp = self.session.post(
            f"{self.base_url}/delete",
            headers=self._headers(),
            data=json.dumps(body),
            timeout=self.timeout,
        )
        self._check_response(resp)
        return resp.json()

    def _delete_protobuf(self, measurement, tags, field, fields,
                         start_time, end_time, series) -> dict[str, Any]:
        req = timestar_pb2.DeleteRequest()
        if series:
            req.series = series
        if measurement:
            req.measurement = measurement
        if tags:
            for k, v in tags.items():
                req.tags[k] = v
        if field:
            req.field = field
        if fields:
            for f in fields:
                req.fields.append(f)
        if start_time is not None:
            req.start_time = start_time
        if end_time is not None:
            req.end_time = end_time
        resp = self.session.post(
            f"{self.base_url}/delete",
            headers=self._headers(),
            data=req.SerializeToString(),
            timeout=self.timeout,
        )
        self._check_response(resp)
        dr = timestar_pb2.DeleteResponse()
        dr.ParseFromString(resp.content)
        return {
            "status": dr.status,
            "deleted_count": dr.deleted_count,
            "total_requests": dr.total_requests,
            "error_message": dr.error_message if dr.error_message else None,
        }

    # ------------------------------------------------------------------
    # Metadata: Measurements
    # ------------------------------------------------------------------

    def list_measurements(self, prefix: str | None = None,
                          offset: int = 0,
                          limit: int = 100) -> list[str]:
        """GET /measurements - List measurement names.

        Args:
            prefix: Optional prefix filter.
            offset: Pagination offset.
            limit: Maximum results to return.

        Returns:
            List of measurement name strings.
        """
        params: dict[str, Any] = {}
        if prefix:
            params["prefix"] = prefix
        if offset:
            params["offset"] = offset
        if limit != 100:
            params["limit"] = limit
        resp = self.session.get(
            f"{self.base_url}/measurements",
            headers={"Accept": self._accept},
            params=params,
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            mr = timestar_pb2.MeasurementsResponse()
            mr.ParseFromString(resp.content)
            return list(mr.measurements)
        data = resp.json()
        return data.get("measurements", [])

    # ------------------------------------------------------------------
    # Metadata: Tags
    # ------------------------------------------------------------------

    def list_tags(self, measurement: str,
                  tag: str | None = None) -> dict[str, list[str]]:
        """GET /tags - Get tag keys and values for a measurement.

        Args:
            measurement: Measurement name.
            tag: Optional specific tag key to get values for.

        Returns:
            dict mapping tag key -> list of values.
        """
        params: dict[str, str] = {"measurement": measurement}
        if tag:
            params["tag"] = tag
        resp = self.session.get(
            f"{self.base_url}/tags",
            headers={"Accept": self._accept},
            params=params,
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            tr = timestar_pb2.TagsResponse()
            tr.ParseFromString(resp.content)
            result = {}
            for k, tv in tr.tags.items():
                result[k] = list(tv.values)
            return result
        data = resp.json()
        return data.get("tags", {})

    # ------------------------------------------------------------------
    # Metadata: Fields
    # ------------------------------------------------------------------

    def list_fields(self, measurement: str) -> list[dict[str, str]]:
        """GET /fields - Get field names and types for a measurement.

        Args:
            measurement: Measurement name.

        Returns:
            List of dicts with "name" and "type" keys.
        """
        resp = self.session.get(
            f"{self.base_url}/fields",
            headers={"Accept": self._accept},
            params={"measurement": measurement},
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            fr = timestar_pb2.FieldsResponse()
            fr.ParseFromString(resp.content)
            return [{"name": fi.name, "type": fi.type} for fi in fr.fields]
        data = resp.json()
        return data.get("fields", [])

    # ------------------------------------------------------------------
    # Metadata: Cardinality
    # ------------------------------------------------------------------

    def get_cardinality(self, measurement: str) -> dict[str, Any]:
        """GET /cardinality - Estimate series and tag cardinality.

        Args:
            measurement: Measurement name.

        Returns:
            dict with "estimated_series_count", "tag_cardinalities", etc.
        """
        resp = self.session.get(
            f"{self.base_url}/cardinality",
            headers={"Accept": self._accept},
            params={"measurement": measurement},
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            cr = timestar_pb2.CardinalityResponse()
            cr.ParseFromString(resp.content)
            return {
                "status": cr.status,
                "measurement": cr.measurement,
                "estimated_series_count": cr.estimated_series_count,
                "tag_cardinalities": [
                    {"tag_key": tc.tag_key,
                     "estimated_count": tc.estimated_count}
                    for tc in cr.tag_cardinalities
                ],
            }
        return resp.json()

    # ------------------------------------------------------------------
    # Retention
    # ------------------------------------------------------------------

    def set_retention(self, measurement: str, ttl: str,
                      downsample: dict[str, str] | None = None) -> dict[str, Any]:
        """PUT /retention - Set a retention policy.

        Args:
            measurement: Measurement name.
            ttl: Duration string, e.g. "90d".
            downsample: Optional dict with "after", "interval", "method".

        Returns:
            dict with "status".
        """
        if self.format == "protobuf":
            req = timestar_pb2.RetentionPutRequest()
            req.measurement = measurement
            req.ttl = ttl
            if downsample:
                if "after" in downsample:
                    req.downsample.after = downsample["after"]
                if "interval" in downsample:
                    req.downsample.interval = downsample["interval"]
                if "method" in downsample:
                    req.downsample.method = downsample["method"]
            resp = self.session.put(
                f"{self.base_url}/retention",
                headers=self._headers(),
                data=req.SerializeToString(),
                timeout=self.timeout,
            )
        else:
            body: dict[str, Any] = {
                "measurement": measurement,
                "ttl": ttl,
            }
            if downsample:
                body["downsample"] = downsample
            resp = self.session.put(
                f"{self.base_url}/retention",
                headers=self._headers(),
                data=json.dumps(body),
                timeout=self.timeout,
            )
        self._check_response(resp)
        if self.format == "protobuf":
            sr = timestar_pb2.StatusResponse()
            sr.ParseFromString(resp.content)
            return {"status": sr.status, "message": sr.message}
        return resp.json()

    def get_retention(self, measurement: str | None = None) -> dict[str, Any]:
        """GET /retention - Get retention policy.

        Args:
            measurement: Optional measurement name.

        Returns:
            dict with retention policy details.
        """
        params = {}
        if measurement:
            params["measurement"] = measurement
        resp = self.session.get(
            f"{self.base_url}/retention",
            headers={"Accept": self._accept},
            params=params,
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            rr = timestar_pb2.RetentionGetResponse()
            rr.ParseFromString(resp.content)
            result: dict[str, Any] = {"status": rr.status}
            if rr.HasField("policy"):
                result["policy"] = {
                    "measurement": rr.policy.measurement,
                    "ttl": rr.policy.ttl,
                    "ttl_nanos": rr.policy.ttl_nanos,
                }
                if rr.policy.HasField("downsample"):
                    result["policy"]["downsample"] = {
                        "after": rr.policy.downsample.after,
                        "interval": rr.policy.downsample.interval,
                        "method": rr.policy.downsample.method,
                    }
            return result
        return resp.json()

    def delete_retention(self, measurement: str) -> dict[str, Any]:
        """DELETE /retention - Delete a retention policy.

        Args:
            measurement: Measurement name.

        Returns:
            dict with "status".
        """
        resp = self.session.delete(
            f"{self.base_url}/retention",
            headers={"Accept": self._accept},
            params={"measurement": measurement},
            timeout=self.timeout,
        )
        self._check_response(resp)
        if self.format == "protobuf":
            sr = timestar_pb2.StatusResponse()
            sr.ParseFromString(resp.content)
            return {"status": sr.status, "message": sr.message}
        return resp.json()

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self.session.close()

    def __enter__(self) -> TimestarClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"TimestarClient(base_url={self.base_url!r}, format={self.format!r})"
