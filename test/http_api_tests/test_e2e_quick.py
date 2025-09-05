#!/usr/bin/env python3
"""
Quick End-to-End Test Suite for TSDB HTTP Server

Focused test suite for rapid validation of core functionality:
- Basic insert/query flow
- All aggregation methods
- Field and tag filtering
- Error handling
"""

import json
import time
import requests
import subprocess
import sys
from datetime import datetime


class QuickE2ETest:
    """Quick end-to-end test for TSDB server"""
    
    def __init__(self, port=8088):
        self.port = port
        self.base_url = f"http://localhost:{port}"
        self.process = None
        self.start_time = int(time.time() * 1_000_000_000)
        
    def start_server(self):
        """Start TSDB server"""
        print(f"Starting TSDB server on port {self.port}...")
        
        try:
            # Build first
            result = subprocess.run(["make", "-j4"], 
                                  cwd="/home/matt/Desktop/source/tsdb/build", 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Build failed: {result.stderr}")
                return False
            
            # Start server with limited shards to avoid memory issues
            self.process = subprocess.Popen(
                ["./bin/tsdb_http_server", "--port", str(self.port), "-c", "4"],
                cwd="/home/matt/Desktop/source/tsdb/build",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            # Wait for startup
            for i in range(30):
                try:
                    response = requests.get(f"{self.base_url}/health", timeout=1)
                    if response.status_code == 200:
                        print(f"✓ Server started successfully")
                        return True
                except requests.exceptions.RequestException:
                    pass
                time.sleep(0.1)
            
            print("✗ Server failed to start within timeout")
            return False
            
        except Exception as e:
            print(f"✗ Failed to start server: {e}")
            return False
    
    def stop_server(self):
        """Stop TSDB server"""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
    
    def write_point(self, measurement, tags, fields, timestamp=None):
        """Write a single point"""
        data = {
            "measurement": measurement,
            "tags": tags,
            "fields": fields
        }
        if timestamp:
            data["timestamp"] = timestamp
            
        try:
            response = requests.post(f"{self.base_url}/write", json=data, timeout=5)
            if response.status_code != 200:
                print(f"Write failed: {response.status_code} {response.text}")
            return response.status_code == 200, response
        except Exception as e:
            print(f"Write exception: {e}")
            return False, str(e)
    
    def query(self, query_str, start_time, end_time):
        """Execute a query"""
        data = {
            "query": query_str,
            "startTime": start_time,
            "endTime": end_time
        }
        try:
            response = requests.post(f"{self.base_url}/query", json=data, timeout=5)
            if response.status_code == 200:
                return True, response.json()
            else:
                return False, response.text
        except Exception as e:
            return False, str(e)
    
    def format_time(self, timestamp_ns):
        """Return timestamp in nanoseconds for query"""
        return timestamp_ns
    
    def run_tests(self):
        """Run all quick tests"""
        if not self.start_server():
            return False
        
        try:
            tests_passed = 0
            tests_total = 0
            
            # Test 1: Basic Insert/Query
            print("\n1. Testing basic insert/query...")
            tests_total += 1
            timestamp = self.start_time + 1000000000  # 1 second from start
            success, resp = self.write_point("temperature", {"location": "test"}, {"value": 25.5}, timestamp)
            if success:
                time.sleep(0.1)  # Allow data to be indexed
                success, result = self.query("avg:temperature(){}", 
                                           self.start_time,
                                           self.start_time + 2*10**9)
                if success and len(result.get("series", [])) > 0:
                    print("✓ Basic insert/query works")
                    tests_passed += 1
                else:
                    print(f"✗ Query failed: {result}")
            else:
                print(f"✗ Insert failed: {resp}")
            
            # Test 2: All Aggregation Methods
            print("\n2. Testing aggregation methods...")
            # Insert test data for aggregations
            values = [10.0, 20.0, 30.0, 40.0, 50.0]
            for i, val in enumerate(values):
                success, _ = self.write_point("agg_test", {"sensor": "temp01"}, {"value": val}, 
                               self.start_time + 1000000000 + i * 10**9)
                if not success:
                    print(f"Failed to insert aggregation test data point {i}")
            
            time.sleep(0.2)  # Allow data to be indexed
            
            agg_methods = ["avg", "min", "max", "sum", "latest"]
            expected = {
                "avg": 30.0,   # (10+20+30+40+50)/5
                "min": 10.0,   # minimum
                "max": 50.0,   # maximum  
                "sum": 150.0,  # sum
                "latest": 50.0 # last value
            }
            
            for method in agg_methods:
                tests_total += 1
                success, result = self.query(f"{method}:agg_test(value){{}}", 
                                           self.start_time - 1000,
                                           self.start_time + 10**10)
                if (success and len(result.get("series", [])) > 0 and 
                    len(result["series"][0]["fields"]["value"]["values"]) > 0):
                    actual = result["series"][0]["fields"]["value"]["values"][0]
                    if abs(actual - expected[method]) < 0.001:
                        print(f"✓ {method.upper()} aggregation: {actual}")
                        tests_passed += 1
                    else:
                        print(f"✗ {method.upper()} aggregation: expected {expected[method]}, got {actual}")
                else:
                    print(f"✗ {method.upper()} aggregation failed: {result}")
            
            # Test 3: Field Filtering
            print("\n3. Testing field filtering...")
            tests_total += 1
            self.write_point("multi_field", {"loc": "test"}, 
                           {"temp": 25.0, "humidity": 60.0, "pressure": 1013.0})
            
            # Test specific field
            success, result = self.query("avg:multi_field(temp){}", 
                                       self.start_time - 1000,
                                       self.start_time + 10**9)
            if (success and len(result.get("series", [])) > 0):
                fields = result["series"][0]["fields"]
                if "temp" in fields and "humidity" not in fields:
                    print("✓ Field filtering works")
                    tests_passed += 1
                else:
                    print(f"✗ Field filtering failed: got fields {list(fields.keys())}")
            else:
                print(f"✗ Field filtering query failed: {result}")
            
            # Test 4: Tag Filtering (Scopes)
            print("\n4. Testing tag filtering...")
            tests_total += 1
            self.write_point("scope_test", {"location": "us-west", "sensor": "01"}, {"value": 20.0})
            self.write_point("scope_test", {"location": "us-east", "sensor": "02"}, {"value": 30.0})
            
            success, result = self.query("avg:scope_test(){location:us-west}", 
                                       self.start_time - 1000,
                                       self.start_time + 10**9)
            if (success and len(result.get("series", [])) == 1 and 
                result["series"][0]["tags"]["location"] == "us-west"):
                print("✓ Tag filtering works")
                tests_passed += 1
            else:
                print(f"✗ Tag filtering failed: {result}")
            
            # Test 5: Group By
            print("\n5. Testing group by...")
            tests_total += 1
            self.write_point("group_test", {"region": "west", "server": "s1"}, {"cpu": 50.0})
            self.write_point("group_test", {"region": "west", "server": "s2"}, {"cpu": 60.0})
            self.write_point("group_test", {"region": "east", "server": "s3"}, {"cpu": 70.0})
            
            success, result = self.query("avg:group_test(){} by {region}", 
                                       self.start_time - 1000,
                                       self.start_time + 10**9)
            if success and len(result.get("series", [])) == 2:
                regions = [s["tags"]["region"] for s in result["series"]]
                if "west" in regions and "east" in regions:
                    print("✓ Group by works")
                    tests_passed += 1
                else:
                    print(f"✗ Group by failed: missing regions {regions}")
            else:
                print(f"✗ Group by failed: {result}")
            
            # Test 6: Batch Insert
            print("\n6. Testing batch insert...")
            tests_total += 1
            batch_data = {
                "writes": [
                    {"measurement": "batch_test", "tags": {"id": "1"}, "fields": {"value": 10.0}},
                    {"measurement": "batch_test", "tags": {"id": "2"}, "fields": {"value": 20.0}},
                    {"measurement": "batch_test", "tags": {"id": "3"}, "fields": {"value": 30.0}}
                ]
            }
            
            try:
                response = requests.post(f"{self.base_url}/write", json=batch_data, timeout=5)
                if response.status_code == 200:
                    # Verify batch was written
                    success, result = self.query("avg:batch_test(){}", 
                                               self.start_time - 1000,
                                               self.start_time + 10**9)
                    if success and len(result.get("series", [])) == 3:
                        print("✓ Batch insert works")
                        tests_passed += 1
                    else:
                        print(f"✗ Batch insert verification failed: {result}")
                else:
                    print(f"✗ Batch insert failed: {response.status_code} {response.text}")
            except Exception as e:
                print(f"✗ Batch insert failed: {e}")
            
            # Test 7: Error Handling
            print("\n7. Testing error handling...")
            tests_total += 1
            
            # Invalid query should return error
            try:
                response = requests.post(f"{self.base_url}/query", 
                                       json={"query": "invalid::syntax", 
                                            "startTime": "01-01-2024 00:00:00",
                                            "endTime": "01-01-2024 23:59:59"}, 
                                       timeout=5)
                if response.status_code != 200:
                    print("✓ Error handling works")
                    tests_passed += 1
                else:
                    print("✗ Error handling failed: invalid query should return error")
            except Exception as e:
                print(f"✗ Error handling test failed: {e}")
            
            # Summary
            print(f"\n{'='*50}")
            print(f"QUICK TEST RESULTS")
            print(f"{'='*50}")
            print(f"Tests passed: {tests_passed}/{tests_total}")
            print(f"Success rate: {(tests_passed/tests_total)*100:.1f}%")
            
            if tests_passed == tests_total:
                print("🎉 All tests passed!")
                return True
            else:
                print("❌ Some tests failed")
                return False
                
        finally:
            self.stop_server()


def main():
    """Main entry point"""
    print("TSDB HTTP Server - Quick End-to-End Test")
    print("=" * 50)
    
    test = QuickE2ETest()
    success = test.run_tests()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())