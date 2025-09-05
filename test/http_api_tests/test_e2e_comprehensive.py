#!/usr/bin/env python3
"""
Comprehensive End-to-End Test Suite for TSDB HTTP Server

Tests all insert and query variations to ensure no bugs:
- Single and batch inserts
- All data types (float, int, bool, string)
- All aggregation methods (avg, min, max, sum, latest)
- Field filtering variations
- Tag filtering (scopes)
- Group by functionality
- Error conditions and edge cases
- Performance and load testing
"""

import json
import time
import random
import string
import requests
import subprocess
import threading
import unittest
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import concurrent.futures


class TSDBTestServer:
    """Manages TSDB HTTP server for testing"""
    
    def __init__(self, port: int = 8087):
        self.port = port
        self.base_url = f"http://localhost:{port}"
        self.process = None
        
    def start(self):
        """Start the TSDB HTTP server"""
        try:
            # Build the server first
            subprocess.run(["make", "-j4"], cwd="/home/matt/Desktop/source/tsdb/build", check=True)
            
            # Start server with limited shards to avoid memory issues
            self.process = subprocess.Popen(
                ["./bin/tsdb_http_server", "--port", str(self.port), "-c", "4"],
                cwd="/home/matt/Desktop/source/tsdb/build",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for server to start
            for _ in range(30):  # 3 second timeout
                try:
                    response = requests.get(f"{self.base_url}/health", timeout=1)
                    if response.status_code == 200:
                        print(f"TSDB server started on port {self.port}")
                        return
                except requests.exceptions.RequestException:
                    pass
                time.sleep(0.1)
                
            raise Exception("Server failed to start within timeout")
            
        except Exception as e:
            self.stop()
            raise Exception(f"Failed to start server: {e}")
    
    def stop(self):
        """Stop the TSDB HTTP server"""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            self.process = None


class TSDBE2ETestSuite(unittest.TestCase):
    """Comprehensive end-to-end test suite"""
    
    @classmethod
    def setUpClass(cls):
        """Start server before all tests"""
        cls.server = TSDBTestServer()
        cls.server.start()
        cls.base_url = cls.server.base_url
        
    @classmethod
    def tearDownClass(cls):
        """Stop server after all tests"""
        cls.server.stop()
    
    def setUp(self):
        """Setup for each test"""
        self.start_time = int(time.time() * 1_000_000_000)  # Current time in nanoseconds
        self.test_data_written = []  # Track what we write for verification
    
    def write_point(self, measurement: str, tags: Dict[str, str], fields: Dict[str, Any], 
                   timestamp: Optional[int] = None) -> requests.Response:
        """Write a single point to the database"""
        data = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields
        }
        if timestamp:
            data["timestamp"] = timestamp
            
        response = requests.post(f"{self.base_url}/write", json=data)
        if response.status_code == 200:
            self.test_data_written.append((measurement, tags, fields, timestamp))
        return response
    
    def write_batch(self, writes: List[Dict[str, Any]]) -> requests.Response:
        """Write a batch of points to the database"""
        data = {"writes": writes}
        response = requests.post(f"{self.base_url}/write", json=data)
        if response.status_code == 200:
            for write in writes:
                self.test_data_written.append((
                    write["measurement"], 
                    write.get("tags", {}), 
                    write["fields"], 
                    write.get("timestamp")
                ))
        return response
    
    def query(self, query_str: str, start_time: str, end_time: str) -> Dict[str, Any]:
        """Execute a query and return the response"""
        data = {
            "query": query_str,
            "startTime": start_time,
            "endTime": end_time
        }
        response = requests.post(f"{self.base_url}/query", json=data)
        self.assertEqual(response.status_code, 200, f"Query failed: {response.text}")
        return response.json()
    
    def format_time(self, timestamp_ns: int) -> int:
        """Return timestamp in nanoseconds for query"""
        return timestamp_ns

    # =============================================================================
    # INSERT TESTS - Single Point Variations
    # =============================================================================
    
    def test_single_insert_float(self):
        """Test single point insert with float values"""
        response = self.write_point("temperature", 
                                  {"location": "datacenter1", "sensor": "temp01"},
                                  {"value": 23.5, "humidity": 45.2})
        self.assertEqual(response.status_code, 200)
        
        # Verify data was written
        result = self.query("avg:temperature(){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        self.assertEqual(len(result["series"]), 1)
        self.assertEqual(result["series"][0]["measurement"], "temperature")
    
    def test_single_insert_integer(self):
        """Test single point insert with integer values"""
        response = self.write_point("cpu", 
                                  {"host": "server01"},
                                  {"cores": 8, "usage": 75})
        self.assertEqual(response.status_code, 200)
    
    def test_single_insert_boolean(self):
        """Test single point insert with boolean values"""
        response = self.write_point("system", 
                                  {"host": "server01"},
                                  {"active": True, "maintenance": False})
        self.assertEqual(response.status_code, 200)
    
    def test_single_insert_string(self):
        """Test single point insert with string values"""
        response = self.write_point("events", 
                                  {"source": "application"},
                                  {"message": "User login", "level": "info"})
        self.assertEqual(response.status_code, 200)
    
    def test_single_insert_mixed_types(self):
        """Test single point insert with mixed data types"""
        response = self.write_point("mixed", 
                                  {"location": "test"},
                                  {
                                      "temperature": 25.5,     # float
                                      "count": 10,             # int  
                                      "active": True,          # bool
                                      "status": "running"      # string
                                  })
        self.assertEqual(response.status_code, 200)
    
    def test_single_insert_with_timestamp(self):
        """Test single point insert with explicit timestamp"""
        timestamp = self.start_time + 5000000000  # 5 seconds later
        response = self.write_point("timestamped", 
                                  {"test": "explicit"},
                                  {"value": 42.0},
                                  timestamp)
        self.assertEqual(response.status_code, 200)

    # =============================================================================
    # INSERT TESTS - Batch Variations
    # =============================================================================
    
    def test_batch_insert_same_measurement(self):
        """Test batch insert with same measurement"""
        writes = []
        for i in range(5):
            writes.append({
                "measurement": "batch_test",
                "tags": {"sensor": f"s{i:02d}"},
                "fields": {"value": float(i * 10)},
                "timestamp": self.start_time + i * 1000000000
            })
        
        response = self.write_batch(writes)
        self.assertEqual(response.status_code, 200)
        
        # Verify all points were written
        result = self.query("avg:batch_test(){}", 
                          self.start_time - 1000,
                          self.start_time + 10000000000)
        self.assertEqual(len(result["series"]), 5)  # One series per sensor
    
    def test_batch_insert_different_measurements(self):
        """Test batch insert with different measurements"""
        writes = [
            {
                "measurement": "cpu",
                "tags": {"host": "server01"},
                "fields": {"usage": 45.0},
                "timestamp": self.start_time
            },
            {
                "measurement": "memory", 
                "tags": {"host": "server01"},
                "fields": {"used_gb": 8.5},
                "timestamp": self.start_time + 1000000000
            },
            {
                "measurement": "network",
                "tags": {"host": "server01", "interface": "eth0"},
                "fields": {"bytes_in": 1024, "bytes_out": 512},
                "timestamp": self.start_time + 2000000000
            }
        ]
        
        response = self.write_batch(writes)
        self.assertEqual(response.status_code, 200)
    
    def test_batch_insert_large(self):
        """Test large batch insert (100 points)"""
        writes = []
        for i in range(100):
            writes.append({
                "measurement": "large_batch",
                "tags": {
                    "region": f"region-{i % 5}",
                    "server": f"server-{i % 20:03d}"
                },
                "fields": {
                    "cpu": random.uniform(0, 100),
                    "memory": random.uniform(0, 16),
                    "active": random.choice([True, False])
                },
                "timestamp": self.start_time + i * 1000000
            })
        
        response = self.write_batch(writes)
        self.assertEqual(response.status_code, 200)

    # =============================================================================
    # QUERY TESTS - Aggregation Methods
    # =============================================================================
    
    def test_aggregation_avg(self):
        """Test AVG aggregation"""
        # Insert test data
        for i in range(5):
            self.write_point("avg_test", {"sensor": "temp01"}, 
                           {"value": float(10 + i * 5)}, 
                           self.start_time + i * 1000000000)
        
        result = self.query("avg:avg_test(value){}", 
                          self.start_time - 1000,
                          self.start_time + 10000000000)
        
        self.assertEqual(len(result["series"]), 1)
        values = result["series"][0]["fields"]["value"]["values"]
        self.assertEqual(len(values), 1)  # Single aggregated value
        self.assertAlmostEqual(values[0], 20.0, places=1)  # (10+15+20+25+30)/5 = 20
    
    def test_aggregation_min(self):
        """Test MIN aggregation"""
        # Insert test data
        values_inserted = [25.5, 10.2, 35.8, 5.1, 20.0]
        for i, val in enumerate(values_inserted):
            self.write_point("min_test", {"sensor": "temp01"}, 
                           {"value": val}, 
                           self.start_time + i * 1000000000)
        
        result = self.query("min:min_test(value){}", 
                          self.start_time - 1000,
                          self.start_time + 10000000000)
        
        values = result["series"][0]["fields"]["value"]["values"]
        self.assertEqual(values[0], 5.1)  # Minimum value
    
    def test_aggregation_max(self):
        """Test MAX aggregation"""
        # Insert test data
        values_inserted = [25.5, 10.2, 35.8, 5.1, 20.0]
        for i, val in enumerate(values_inserted):
            self.write_point("max_test", {"sensor": "temp01"}, 
                           {"value": val}, 
                           self.start_time + i * 1000000000)
        
        result = self.query("max:max_test(value){}", 
                          self.start_time - 1000,
                          self.start_time + 10000000000)
        
        values = result["series"][0]["fields"]["value"]["values"]
        self.assertEqual(values[0], 35.8)  # Maximum value
    
    def test_aggregation_sum(self):
        """Test SUM aggregation"""
        # Insert test data
        values_inserted = [10.0, 20.0, 30.0, 40.0]
        for i, val in enumerate(values_inserted):
            self.write_point("sum_test", {"sensor": "temp01"}, 
                           {"value": val}, 
                           self.start_time + i * 1000000000)
        
        result = self.query("sum:sum_test(value){}", 
                          self.start_time - 1000,
                          self.start_time + 10000000000)
        
        values = result["series"][0]["fields"]["value"]["values"]
        self.assertEqual(values[0], 100.0)  # Sum of all values
    
    def test_aggregation_latest(self):
        """Test LATEST aggregation"""
        # Insert test data with known timestamps
        values_inserted = [10.0, 20.0, 30.0, 25.0]  # Latest should be 25.0
        for i, val in enumerate(values_inserted):
            self.write_point("latest_test", {"sensor": "temp01"}, 
                           {"value": val}, 
                           self.start_time + i * 1000000000)
        
        result = self.query("latest:latest_test(value){}", 
                          self.start_time - 1000,
                          self.start_time + 10000000000)
        
        values = result["series"][0]["fields"]["value"]["values"]
        self.assertEqual(values[0], 25.0)  # Latest value

    # =============================================================================
    # QUERY TESTS - Field Filtering
    # =============================================================================
    
    def test_field_filtering_all_fields(self):
        """Test querying all fields with empty parentheses"""
        # Insert data with multiple fields
        self.write_point("multi_field", {"location": "test"}, 
                       {"temperature": 25.0, "humidity": 60.0, "pressure": 1013.25})
        
        result = self.query("avg:multi_field(){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 1)
        fields = result["series"][0]["fields"]
        self.assertIn("temperature", fields)
        self.assertIn("humidity", fields)
        self.assertIn("pressure", fields)
    
    def test_field_filtering_specific_field(self):
        """Test querying specific field"""
        # Insert data with multiple fields
        self.write_point("multi_field2", {"location": "test"}, 
                       {"temperature": 25.0, "humidity": 60.0, "pressure": 1013.25})
        
        result = self.query("avg:multi_field2(temperature){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 1)
        fields = result["series"][0]["fields"]
        self.assertIn("temperature", fields)
        self.assertNotIn("humidity", fields)
        self.assertNotIn("pressure", fields)
    
    def test_field_filtering_multiple_fields(self):
        """Test querying multiple specific fields"""
        # Insert data with multiple fields
        self.write_point("multi_field3", {"location": "test"}, 
                       {"temperature": 25.0, "humidity": 60.0, "pressure": 1013.25, "wind": 10.5})
        
        result = self.query("avg:multi_field3(temperature,humidity){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 1)
        fields = result["series"][0]["fields"]
        self.assertIn("temperature", fields)
        self.assertIn("humidity", fields)
        self.assertNotIn("pressure", fields)
        self.assertNotIn("wind", fields)
    
    def test_field_filtering_nonexistent_field(self):
        """Test querying non-existent field returns empty result"""
        # Insert data
        self.write_point("field_test", {"location": "test"}, {"temperature": 25.0})
        
        result = self.query("avg:field_test(nonexistent){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        # Should return empty series or series with no data
        self.assertTrue(len(result["series"]) == 0 or 
                       len(result["series"][0]["fields"]) == 0)

    # =============================================================================
    # QUERY TESTS - Tag Filtering (Scopes)
    # =============================================================================
    
    def test_scope_filtering_single_tag(self):
        """Test filtering by single tag"""
        # Insert data with different tags
        self.write_point("scope_test", {"location": "us-west", "sensor": "temp01"}, {"value": 20.0})
        self.write_point("scope_test", {"location": "us-east", "sensor": "temp02"}, {"value": 25.0})
        self.write_point("scope_test", {"location": "eu-west", "sensor": "temp03"}, {"value": 15.0})
        
        result = self.query("avg:scope_test(){location:us-west}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 1)
        self.assertEqual(result["series"][0]["tags"]["location"], "us-west")
    
    def test_scope_filtering_multiple_tags(self):
        """Test filtering by multiple tags (AND condition)"""
        # Insert data with different tag combinations
        self.write_point("scope_test2", {"location": "us-west", "type": "temperature", "sensor": "01"}, {"value": 20.0})
        self.write_point("scope_test2", {"location": "us-west", "type": "humidity", "sensor": "01"}, {"value": 60.0})
        self.write_point("scope_test2", {"location": "us-east", "type": "temperature", "sensor": "02"}, {"value": 25.0})
        
        result = self.query("avg:scope_test2(){location:us-west,type:temperature}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 1)
        tags = result["series"][0]["tags"]
        self.assertEqual(tags["location"], "us-west")
        self.assertEqual(tags["type"], "temperature")
    
    def test_scope_filtering_no_matches(self):
        """Test filtering with no matching tags"""
        # Insert data
        self.write_point("scope_test3", {"location": "us-west"}, {"value": 20.0})
        
        result = self.query("avg:scope_test3(){location:nonexistent}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 0)
    
    def test_scope_filtering_empty_scopes(self):
        """Test empty scopes returns all series"""
        # Insert data with different tags
        self.write_point("scope_test4", {"location": "us-west"}, {"value": 20.0})
        self.write_point("scope_test4", {"location": "us-east"}, {"value": 25.0})
        
        result = self.query("avg:scope_test4(){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        self.assertEqual(len(result["series"]), 2)

    # =============================================================================
    # QUERY TESTS - Group By Functionality
    # =============================================================================
    
    def test_groupby_single_tag(self):
        """Test group by single tag"""
        # Insert data with same measurement but different locations
        self.write_point("groupby_test", {"location": "us-west", "sensor": "temp01"}, {"value": 20.0})
        self.write_point("groupby_test", {"location": "us-west", "sensor": "temp02"}, {"value": 22.0})
        self.write_point("groupby_test", {"location": "us-east", "sensor": "temp03"}, {"value": 25.0})
        self.write_point("groupby_test", {"location": "us-east", "sensor": "temp04"}, {"value": 27.0})
        
        result = self.query("avg:groupby_test(){} by {location}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        # Should have 2 grouped series (one per location)
        self.assertEqual(len(result["series"]), 2)
        
        # Check that each series has the location tag
        locations = [s["tags"]["location"] for s in result["series"]]
        self.assertIn("us-west", locations)
        self.assertIn("us-east", locations)
    
    def test_groupby_multiple_tags(self):
        """Test group by multiple tags"""
        # Insert data with different combinations
        self.write_point("groupby_test2", {"location": "us-west", "type": "cpu", "host": "server01"}, {"value": 50.0})
        self.write_point("groupby_test2", {"location": "us-west", "type": "cpu", "host": "server02"}, {"value": 60.0})
        self.write_point("groupby_test2", {"location": "us-west", "type": "memory", "host": "server01"}, {"value": 70.0})
        self.write_point("groupby_test2", {"location": "us-east", "type": "cpu", "host": "server01"}, {"value": 55.0})
        
        result = self.query("avg:groupby_test2(){} by {location,type}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        # Should have 3 groups: us-west+cpu, us-west+memory, us-east+cpu
        self.assertEqual(len(result["series"]), 3)
    
    def test_groupby_with_scopes(self):
        """Test group by combined with scope filtering"""
        # Insert data
        self.write_point("groupby_scope", {"location": "us-west", "type": "cpu", "host": "server01"}, {"value": 50.0})
        self.write_point("groupby_scope", {"location": "us-west", "type": "cpu", "host": "server02"}, {"value": 60.0})
        self.write_point("groupby_scope", {"location": "us-west", "type": "memory", "host": "server01"}, {"value": 70.0})
        self.write_point("groupby_scope", {"location": "us-east", "type": "cpu", "host": "server01"}, {"value": 55.0})
        
        result = self.query("avg:groupby_scope(){location:us-west} by {type}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        # Should have 2 groups: cpu and memory (only from us-west)
        self.assertEqual(len(result["series"]), 2)
        types = [s["tags"]["type"] for s in result["series"]]
        self.assertIn("cpu", types)
        self.assertIn("memory", types)

    # =============================================================================
    # ERROR CONDITION TESTS
    # =============================================================================
    
    def test_invalid_measurement_name(self):
        """Test insert with invalid measurement name"""
        response = self.write_point("", {"tag": "value"}, {"field": 1.0})
        self.assertNotEqual(response.status_code, 200)
    
    def test_missing_fields(self):
        """Test insert without fields"""
        data = {
            "measurement": "test",
            "tags": {"tag": "value"}
            # Missing fields
        }
        response = requests.post(f"{self.base_url}/write", json=data)
        self.assertNotEqual(response.status_code, 200)
    
    def test_invalid_query_syntax(self):
        """Test invalid query syntax"""
        data = {
            "query": "invalid:syntax:here",
            "startTime": "01-01-2024 00:00:00",
            "endTime": "01-01-2024 23:59:59"
        }
        response = requests.post(f"{self.base_url}/query", json=data)
        self.assertNotEqual(response.status_code, 200)
    
    def test_invalid_time_format(self):
        """Test invalid time format"""
        data = {
            "query": "avg:test(){}",
            "startTime": "invalid-time",
            "endTime": "01-01-2024 23:59:59"
        }
        response = requests.post(f"{self.base_url}/query", json=data)
        self.assertNotEqual(response.status_code, 200)
    
    def test_query_nonexistent_measurement(self):
        """Test querying non-existent measurement"""
        result = self.query("avg:nonexistent_measurement(){}", 
                          self.start_time - 1000,
                          self.start_time + 1000000000)
        
        # Should return empty result, not error
        self.assertEqual(len(result["series"]), 0)
    
    def test_malformed_json(self):
        """Test malformed JSON requests"""
        response = requests.post(f"{self.base_url}/write", 
                               data="{ invalid json }", 
                               headers={"Content-Type": "application/json"})
        self.assertNotEqual(response.status_code, 200)

    # =============================================================================
    # EDGE CASE TESTS
    # =============================================================================
    
    def test_large_field_values(self):
        """Test very large numeric values"""
        large_value = 1.7976931348623157e+308  # Close to float64 max
        response = self.write_point("large_values", {"test": "max"}, {"big_number": large_value})
        self.assertEqual(response.status_code, 200)
    
    def test_unicode_strings(self):
        """Test Unicode strings in tags and fields"""
        response = self.write_point("unicode_test", 
                                  {"location": "东京", "description": "测试"}, 
                                  {"message": "Hello 世界! 🌍", "emoji": "🚀🔥💯"})
        self.assertEqual(response.status_code, 200)
    
    def test_special_characters_in_names(self):
        """Test special characters in measurement/tag/field names"""
        response = self.write_point("test-measurement_with.dots", 
                                  {"tag-with-dashes": "value_with_underscores"}, 
                                  {"field.with.dots": 42.0})
        self.assertEqual(response.status_code, 200)
    
    def test_empty_tag_values(self):
        """Test empty tag values"""
        response = self.write_point("empty_tags", {"empty": "", "normal": "value"}, {"field": 1.0})
        # Should succeed - empty strings are valid tag values
        self.assertEqual(response.status_code, 200)
    
    def test_long_strings(self):
        """Test very long strings"""
        long_string = "x" * 10000  # 10KB string
        response = self.write_point("long_strings", {"type": "long"}, {"data": long_string})
        self.assertEqual(response.status_code, 200)
    
    def test_many_tags(self):
        """Test measurement with many tags"""
        many_tags = {f"tag_{i:03d}": f"value_{i}" for i in range(50)}
        response = self.write_point("many_tags", many_tags, {"value": 1.0})
        self.assertEqual(response.status_code, 200)
    
    def test_many_fields(self):
        """Test measurement with many fields"""
        many_fields = {f"field_{i:03d}": float(i) for i in range(50)}
        response = self.write_point("many_fields", {"test": "many"}, many_fields)
        self.assertEqual(response.status_code, 200)

    # =============================================================================
    # PERFORMANCE AND LOAD TESTS
    # =============================================================================
    
    def test_concurrent_writes(self):
        """Test concurrent write operations"""
        def write_worker(worker_id: int, num_writes: int):
            success_count = 0
            for i in range(num_writes):
                try:
                    response = self.write_point(f"concurrent_test_{worker_id}", 
                                              {"worker": str(worker_id), "iteration": str(i)}, 
                                              {"value": float(i)})
                    if response.status_code == 200:
                        success_count += 1
                except Exception:
                    pass
            return success_count
        
        # Run 5 workers, each writing 20 points
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(write_worker, i, 20) for i in range(5)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # Should have high success rate
        total_successes = sum(results)
        self.assertGreater(total_successes, 80)  # At least 80% success rate
    
    def test_concurrent_queries(self):
        """Test concurrent query operations"""
        # First, write some test data
        for i in range(10):
            self.write_point("concurrent_query_test", 
                           {"sensor": f"temp_{i:02d}"}, 
                           {"value": float(i * 10)})
        
        def query_worker():
            try:
                result = self.query("avg:concurrent_query_test(){}", 
                                  self.start_time - 1000,
                                  self.start_time + 1000000000)
                return len(result["series"])
            except Exception:
                return 0
        
        # Run 10 concurrent queries
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(query_worker) for _ in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # All queries should succeed and return data
        self.assertTrue(all(r > 0 for r in results))
    
    def test_write_query_mixed_load(self):
        """Test mixed read/write workload"""
        def mixed_worker(duration_seconds: int):
            end_time = time.time() + duration_seconds
            operations = 0
            
            while time.time() < end_time:
                # Randomly choose write or query
                if random.random() < 0.7:  # 70% writes, 30% queries
                    try:
                        self.write_point("mixed_load", 
                                       {"worker": "mixed", "op": str(operations)}, 
                                       {"value": random.uniform(0, 100)})
                        operations += 1
                    except Exception:
                        pass
                else:
                    try:
                        self.query("avg:mixed_load(){}", 
                                 self.start_time - 1000,
                                 self.start_time + 1000000000)
                        operations += 1
                    except Exception:
                        pass
            
            return operations
        
        # Run mixed workload for 3 seconds
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(mixed_worker, 3) for _ in range(3)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # Should complete a reasonable number of operations
        total_ops = sum(results)
        self.assertGreater(total_ops, 50)  # At least 50 total operations


def run_comprehensive_tests():
    """Run the comprehensive test suite"""
    print("=" * 80)
    print("TSDB HTTP Server - Comprehensive End-to-End Test Suite")
    print("=" * 80)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TSDBE2ETestSuite)
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2, buffer=True)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 80)
    print(f"TESTS RUN: {result.testsRun}")
    print(f"FAILURES: {len(result.failures)}")
    print(f"ERRORS: {len(result.errors)}")
    print(f"SUCCESS RATE: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun) * 100:.1f}%")
    print("=" * 80)
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"\n{test}:\n{traceback}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"\n{test}:\n{traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_comprehensive_tests()
    exit(0 if success else 1)