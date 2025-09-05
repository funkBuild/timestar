#!/usr/bin/env python3
"""
End-to-end HTTP API integration test for TSDB

This test:
1. Starts the TSDB HTTP server
2. Tests various write scenarios
3. Verifies data persistence
4. Tests error handling
5. Shuts down the server cleanly
"""

import json
import requests
import subprocess
import time
import sys
import os
import signal
import argparse
from datetime import datetime
import random
import shutil

class TSDBHTTPTest:
    def __init__(self, host='localhost', port=8086, binary_path='./bin/tsdb_http_server'):
        self.host = host
        self.port = port
        self.base_url = f"http://{host}:{port}"
        self.binary_path = binary_path
        self.server_process = None
        self.test_data_dirs = ['shard_0', 'shard_1', 'shard_2', 'shard_3']
        
    def cleanup_data(self):
        """Remove test data directories"""
        for dir_name in self.test_data_dirs:
            if os.path.exists(dir_name):
                shutil.rmtree(dir_name)
                print(f"Cleaned up {dir_name}")
    
    def start_server(self):
        """Start the TSDB HTTP server"""
        print(f"Starting TSDB server on port {self.port}...")
        
        # Clean up any existing data
        self.cleanup_data()
        
        # Start the server process
        self.server_process = subprocess.Popen(
            [self.binary_path, '--port', str(self.port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # Create new process group for clean shutdown
        )
        
        # Wait for server to start (Seastar takes time to initialize all shards)
        max_attempts = 60
        for i in range(max_attempts):
            try:
                response = requests.get(f"{self.base_url}/health", timeout=1)
                if response.status_code == 200:
                    print(f"Server started successfully (PID: {self.server_process.pid})")
                    return True
            except requests.exceptions.RequestException:
                if i % 10 == 0:
                    print(f"  Waiting for server to start... ({i}/{max_attempts})")
                time.sleep(1)
        
        print("Failed to start server")
        self.stop_server()
        return False
    
    def stop_server(self):
        """Stop the TSDB HTTP server"""
        if self.server_process:
            print(f"Stopping server (PID: {self.server_process.pid})...")
            
            # Send SIGTERM to the process group
            try:
                os.killpg(os.getpgid(self.server_process.pid), signal.SIGTERM)
                self.server_process.wait(timeout=5)
            except:
                # Force kill if graceful shutdown fails
                try:
                    os.killpg(os.getpgid(self.server_process.pid), signal.SIGKILL)
                except:
                    pass
            
            self.server_process = None
            print("Server stopped")
    
    def get_timestamp_ns(self):
        """Get current timestamp in nanoseconds"""
        return int(time.time() * 1e9)
    
    def test_health_check(self):
        """Test health check endpoint"""
        print("\n=== Test: Health Check ===")
        response = requests.get(f"{self.base_url}/health")
        assert response.status_code == 200, f"Health check failed: {response.status_code}"
        print("✓ Health check passed")
        return True
    
    def test_single_point_write(self):
        """Test writing a single data point"""
        print("\n=== Test: Single Point Write ===")
        
        data = {
            "measurement": "temperature",
            "tags": {
                "location": "office",
                "sensor": "temp-01"
            },
            "fields": {
                "value": 23.5,
                "humidity": 45.2
            },
            "timestamp": self.get_timestamp_ns()
        }
        
        response = requests.post(f"{self.base_url}/write", json=data)
        assert response.status_code == 200, f"Write failed: {response.text}"
        
        result = response.json()
        assert result.get("status") == "success", f"Unexpected response: {result}"
        assert result.get("points_written") == 1, f"Wrong point count: {result}"
        
        print(f"✓ Wrote single point: {data['fields']['value']}°C")
        return True
    
    def test_batch_write(self):
        """Test batch writing multiple points"""
        print("\n=== Test: Batch Write ===")
        
        base_time = self.get_timestamp_ns()
        points = []
        
        for i in range(10):
            points.append({
                "measurement": "cpu_usage",
                "tags": {
                    "host": f"server-{i % 3:02d}",
                    "datacenter": "dc1"
                },
                "fields": {
                    "usage_percent": round(20 + random.random() * 60, 2),
                    "load_average": round(0.5 + random.random() * 2, 2)
                },
                "timestamp": base_time + (i * 1000000000)  # 1 second apart
            })
        
        data = {"writes": points}
        
        response = requests.post(f"{self.base_url}/write", json=data)
        assert response.status_code == 200, f"Batch write failed: {response.text}"
        
        result = response.json()
        assert result.get("status") == "success", f"Unexpected response: {result}"
        assert result.get("points_written") == len(points), f"Wrong point count: {result}"
        
        print(f"✓ Wrote {len(points)} points in batch")
        return True
    
    def test_mixed_data_types(self):
        """Test writing different data types"""
        print("\n=== Test: Mixed Data Types ===")
        
        data = {
            "measurement": "system_stats",
            "tags": {
                "host": "test-server",
                "env": "testing"
            },
            "fields": {
                "cpu_percent": 45.7,      # float
                "memory_gb": 16,           # integer
                "is_healthy": True,        # boolean
                "status": "running",       # string
                "uptime_hours": 1234       # integer
            },
            "timestamp": self.get_timestamp_ns()
        }
        
        response = requests.post(f"{self.base_url}/write", json=data)
        assert response.status_code == 200, f"Mixed type write failed: {response.text}"
        
        result = response.json()
        assert result.get("status") == "success", f"Unexpected response: {result}"
        
        print("✓ Successfully wrote mixed data types")
        return True
    
    def test_high_frequency_writes(self):
        """Test high frequency writes to measure throughput"""
        print("\n=== Test: High Frequency Writes ===")
        
        num_writes = 100
        start_time = time.time()
        base_timestamp = self.get_timestamp_ns()
        successful_writes = 0
        
        for i in range(num_writes):
            data = {
                "measurement": "stress_test",
                "tags": {
                    "test_run": str(int(start_time)),
                    "worker": f"worker-{i % 5}"
                },
                "fields": {
                    "value": random.random() * 100,
                    "sequence": i
                },
                "timestamp": base_timestamp + (i * 1000000)  # 1ms apart
            }
            
            try:
                response = requests.post(f"{self.base_url}/write", json=data, timeout=1)
                if response.status_code == 200:
                    successful_writes += 1
            except:
                pass
        
        elapsed = time.time() - start_time
        write_rate = successful_writes / elapsed if elapsed > 0 else 0
        
        print(f"✓ Wrote {successful_writes}/{num_writes} points")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Rate: {write_rate:.0f} writes/second")
        
        assert successful_writes >= num_writes * 0.95, f"Too many failed writes: {num_writes - successful_writes}"
        return True
    
    def test_error_handling(self):
        """Test error handling for invalid requests"""
        print("\n=== Test: Error Handling ===")
        
        test_cases = [
            {
                "name": "Missing measurement",
                "data": {
                    "tags": {"host": "test"},
                    "fields": {"value": 42},
                    "timestamp": self.get_timestamp_ns()
                },
                "expect_error": True
            },
            {
                "name": "Missing fields",
                "data": {
                    "measurement": "test",
                    "tags": {"host": "test"},
                    "timestamp": self.get_timestamp_ns()
                },
                "expect_error": True
            },
            {
                "name": "Empty fields",
                "data": {
                    "measurement": "test",
                    "tags": {"host": "test"},
                    "fields": {},
                    "timestamp": self.get_timestamp_ns()
                },
                "expect_error": True
            }
        ]
        
        for test_case in test_cases:
            response = requests.post(f"{self.base_url}/write", json=test_case["data"])
            
            if test_case["expect_error"]:
                # Should return error status
                if response.status_code >= 400:
                    print(f"✓ {test_case['name']}: Correctly rejected")
                else:
                    print(f"✗ {test_case['name']}: Should have failed")
                    return False
        
        # Test invalid JSON
        response = requests.post(
            f"{self.base_url}/write",
            data="not valid json",
            headers={'Content-Type': 'application/json'}
        )
        assert response.status_code >= 400, "Invalid JSON should be rejected"
        print("✓ Invalid JSON correctly rejected")
        
        return True
    
    def test_large_batch(self):
        """Test writing a large batch of points"""
        print("\n=== Test: Large Batch Write ===")
        
        batch_size = 1000
        base_time = self.get_timestamp_ns()
        points = []
        
        for i in range(batch_size):
            points.append({
                "measurement": "large_batch_test",
                "tags": {
                    "batch_id": str(int(time.time())),
                    "series": f"series-{i % 10}"
                },
                "fields": {
                    "value": random.gauss(50, 10),
                    "index": i
                },
                "timestamp": base_time + (i * 1000000)  # 1ms apart
            })
        
        data = {"writes": points}
        
        start_time = time.time()
        response = requests.post(f"{self.base_url}/write", json=data, timeout=10)
        elapsed = time.time() - start_time
        
        assert response.status_code == 200, f"Large batch failed: {response.text}"
        
        result = response.json()
        assert result.get("points_written") == batch_size, f"Not all points written: {result}"
        
        print(f"✓ Wrote {batch_size} points in {elapsed:.2f}s")
        print(f"  Rate: {batch_size/elapsed:.0f} points/second")
        return True
    
    def test_concurrent_writes(self):
        """Test concurrent write requests"""
        print("\n=== Test: Concurrent Writes ===")
        
        import concurrent.futures
        
        def write_data(worker_id):
            data = {
                "measurement": "concurrent_test",
                "tags": {
                    "worker": f"worker-{worker_id}"
                },
                "fields": {
                    "value": random.random() * 100
                },
                "timestamp": self.get_timestamp_ns()
            }
            
            try:
                response = requests.post(f"{self.base_url}/write", json=data, timeout=2)
                return response.status_code == 200
            except:
                return False
        
        num_workers = 20
        num_requests = 100
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(write_data, i % num_workers) for i in range(num_requests)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        successful = sum(1 for r in results if r)
        print(f"✓ Concurrent writes: {successful}/{num_requests} successful")
        
        assert successful >= num_requests * 0.9, f"Too many failed concurrent writes"
        return True
    
    def run_all_tests(self):
        """Run all tests"""
        print("="*60)
        print("TSDB HTTP API End-to-End Test Suite")
        print("="*60)
        
        if not self.start_server():
            print("Failed to start server, aborting tests")
            return False
        
        tests = [
            self.test_health_check,
            self.test_single_point_write,
            self.test_batch_write,
            self.test_mixed_data_types,
            self.test_error_handling,
            self.test_high_frequency_writes,
            self.test_large_batch,
            self.test_concurrent_writes
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"✗ Test failed with exception: {e}")
                failed += 1
        
        print("\n" + "="*60)
        print(f"Test Results: {passed} passed, {failed} failed")
        print("="*60)
        
        self.stop_server()
        self.cleanup_data()
        
        return failed == 0

def main():
    parser = argparse.ArgumentParser(description='TSDB HTTP API End-to-End Test')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8086, help='Server port')
    parser.add_argument('--binary', default='./bin/tsdb_http_server', 
                       help='Path to tsdb_http_server binary')
    
    args = parser.parse_args()
    
    # Check if binary exists
    if not os.path.exists(args.binary):
        print(f"Error: Server binary not found at {args.binary}")
        print("Make sure you've built the project first")
        return 1
    
    tester = TSDBHTTPTest(args.host, args.port, args.binary)
    
    if tester.run_all_tests():
        print("\n✅ All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())