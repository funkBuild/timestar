#!/usr/bin/env python3
"""
Test client for TSDB HTTP write endpoint

Usage:
    python test_http_write.py [--host localhost] [--port 8086]
"""

import json
import requests
import time
import random
import sys
import argparse
from datetime import datetime

def get_current_timestamp_ns():
    """Get current timestamp in nanoseconds"""
    return int(time.time() * 1e9)

def test_single_write(base_url):
    """Test a single point write"""
    print("\n=== Testing Single Write ===")
    
    data = {
        "measurement": "temperature",
        "tags": {
            "location": "us-midwest",
            "host": "server-01"
        },
        "fields": {
            "value": 82.5,
            "humidity": 65.0
        },
        "timestamp": get_current_timestamp_ns()
    }
    
    print(f"Sending: {json.dumps(data, indent=2)}")
    
    response = requests.post(f"{base_url}/write", json=data)
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")
    
    return response.status_code == 200

def test_batch_write(base_url):
    """Test batch write with multiple points"""
    print("\n=== Testing Batch Write ===")
    
    base_timestamp = get_current_timestamp_ns()
    points = []
    
    # Generate 10 data points
    for i in range(10):
        points.append({
            "measurement": "temperature",
            "tags": {
                "location": random.choice(["us-west", "us-east", "us-midwest"]),
                "host": f"server-{i:02d}"
            },
            "fields": {
                "value": round(70 + random.random() * 20, 2),
                "humidity": round(40 + random.random() * 40, 2)
            },
            "timestamp": base_timestamp + (i * 1000000000)  # 1 second apart
        })
    
    data = {"writes": points}
    
    print(f"Sending {len(points)} points...")
    
    response = requests.post(f"{base_url}/write", json=data)
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")
    
    return response.status_code == 200

def test_mixed_types(base_url):
    """Test write with mixed field types"""
    print("\n=== Testing Mixed Field Types ===")
    
    data = {
        "measurement": "system_metrics",
        "tags": {
            "host": "server-01",
            "datacenter": "dc1"
        },
        "fields": {
            "cpu_usage": 45.7,           # float
            "memory_gb": 16,              # integer
            "is_healthy": True,           # boolean
            "status": "running",          # string
            "error_count": 0              # integer
        },
        "timestamp": get_current_timestamp_ns()
    }
    
    print(f"Sending: {json.dumps(data, indent=2)}")
    
    response = requests.post(f"{base_url}/write", json=data)
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")
    
    return response.status_code == 200

def test_multiple_measurements(base_url):
    """Test write with different measurements"""
    print("\n=== Testing Multiple Measurements ===")
    
    base_timestamp = get_current_timestamp_ns()
    points = [
        {
            "measurement": "cpu",
            "tags": {"host": "server-01", "cpu": "cpu0"},
            "fields": {"usage_idle": 23.1, "usage_system": 45.0},
            "timestamp": base_timestamp
        },
        {
            "measurement": "disk",
            "tags": {"host": "server-01", "device": "sda1"},
            "fields": {"used": 24567890944, "free": 98765432100},
            "timestamp": base_timestamp
        },
        {
            "measurement": "network",
            "tags": {"host": "server-01", "interface": "eth0"},
            "fields": {"bytes_sent": 123456, "bytes_recv": 654321},
            "timestamp": base_timestamp
        }
    ]
    
    data = {"writes": points}
    
    print(f"Sending {len(points)} measurements...")
    
    response = requests.post(f"{base_url}/write", json=data)
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")
    
    return response.status_code == 200

def test_error_cases(base_url):
    """Test various error conditions"""
    print("\n=== Testing Error Cases ===")
    
    test_cases = [
        {
            "name": "Missing measurement",
            "data": {
                "tags": {"host": "server-01"},
                "fields": {"value": 42.0},
                "timestamp": get_current_timestamp_ns()
            }
        },
        {
            "name": "Missing fields",
            "data": {
                "measurement": "test",
                "tags": {"host": "server-01"},
                "timestamp": get_current_timestamp_ns()
            }
        },
        {
            "name": "Empty fields",
            "data": {
                "measurement": "test",
                "tags": {"host": "server-01"},
                "fields": {},
                "timestamp": get_current_timestamp_ns()
            }
        },
        {
            "name": "Invalid JSON",
            "data": "not valid json"
        }
    ]
    
    for test_case in test_cases:
        print(f"\nTesting: {test_case['name']}")
        
        try:
            if test_case['name'] == "Invalid JSON":
                response = requests.post(f"{base_url}/write", 
                                        data=test_case['data'],
                                        headers={'Content-Type': 'application/json'})
            else:
                response = requests.post(f"{base_url}/write", json=test_case['data'])
            
            print(f"Response Status: {response.status_code}")
            print(f"Response Body: {response.text}")
            
            # These should all fail with 4xx status
            if response.status_code >= 400 and response.status_code < 500:
                print("✓ Error handled correctly")
            else:
                print("✗ Expected 4xx error status")
                
        except Exception as e:
            print(f"Request failed: {e}")

def test_health_check(base_url):
    """Test health check endpoint"""
    print("\n=== Testing Health Check ===")
    
    response = requests.get(f"{base_url}/health")
    print(f"Response Status: {response.status_code}")
    print(f"Response Body: {response.text}")
    
    return response.status_code == 200

def stress_test(base_url, num_points=1000, batch_size=100):
    """Simple stress test"""
    print(f"\n=== Stress Test: {num_points} points in batches of {batch_size} ===")
    
    start_time = time.time()
    points_sent = 0
    errors = 0
    
    while points_sent < num_points:
        batch = []
        base_timestamp = get_current_timestamp_ns()
        
        for i in range(min(batch_size, num_points - points_sent)):
            batch.append({
                "measurement": "stress_test",
                "tags": {
                    "host": f"server-{i % 10:02d}",
                    "test_run": str(int(start_time))
                },
                "fields": {
                    "value": random.random() * 100,
                    "counter": points_sent + i
                },
                "timestamp": base_timestamp + i * 1000000  # 1ms apart
            })
        
        try:
            response = requests.post(f"{base_url}/write", 
                                    json={"writes": batch},
                                    timeout=5)
            if response.status_code == 200:
                points_sent += len(batch)
                print(f"Sent {points_sent}/{num_points} points")
            else:
                errors += 1
                print(f"Error: {response.status_code}")
        except Exception as e:
            errors += 1
            print(f"Request failed: {e}")
    
    elapsed = time.time() - start_time
    rate = points_sent / elapsed if elapsed > 0 else 0
    
    print(f"\nStress Test Results:")
    print(f"  Points sent: {points_sent}")
    print(f"  Errors: {errors}")
    print(f"  Time: {elapsed:.2f} seconds")
    print(f"  Rate: {rate:.0f} points/second")

def main():
    parser = argparse.ArgumentParser(description='Test TSDB HTTP write endpoint')
    parser.add_argument('--host', default='localhost', help='TSDB server host')
    parser.add_argument('--port', type=int, default=8086, help='TSDB server port')
    parser.add_argument('--stress', action='store_true', help='Run stress test')
    parser.add_argument('--stress-points', type=int, default=1000, 
                       help='Number of points for stress test')
    parser.add_argument('--stress-batch', type=int, default=100,
                       help='Batch size for stress test')
    
    args = parser.parse_args()
    
    base_url = f"http://{args.host}:{args.port}"
    print(f"Testing TSDB at {base_url}")
    
    # Check if server is running
    try:
        response = requests.get(f"{base_url}/health", timeout=2)
        print(f"Server is {'healthy' if response.status_code == 200 else 'unhealthy'}")
    except requests.exceptions.RequestException as e:
        print(f"Cannot connect to server at {base_url}: {e}")
        print("\nMake sure the TSDB HTTP server is running:")
        print("  ./build/bin/tsdb_http_server --port 8086")
        return 1
    
    # Run tests
    all_passed = True
    
    if not args.stress:
        # Run functional tests
        tests = [
            ("Health Check", test_health_check),
            ("Single Write", test_single_write),
            ("Batch Write", test_batch_write),
            ("Mixed Types", test_mixed_types),
            ("Multiple Measurements", test_multiple_measurements),
        ]
        
        for test_name, test_func in tests:
            try:
                passed = test_func(base_url)
                if passed:
                    print(f"✓ {test_name} PASSED")
                else:
                    print(f"✗ {test_name} FAILED")
                    all_passed = False
            except Exception as e:
                print(f"✗ {test_name} FAILED: {e}")
                all_passed = False
        
        # Error cases don't affect overall pass/fail
        test_error_cases(base_url)
        
        print("\n" + "="*50)
        if all_passed:
            print("✓ All tests PASSED")
        else:
            print("✗ Some tests FAILED")
    else:
        # Run stress test
        stress_test(base_url, args.stress_points, args.stress_batch)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())