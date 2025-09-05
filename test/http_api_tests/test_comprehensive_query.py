#!/usr/bin/env python3
"""
Comprehensive query API test to identify all problems
"""

import json
import requests
import time
import sys
from datetime import datetime

def test_comprehensive_query(base_url="http://localhost:8086"):
    """Run comprehensive query tests and identify problems"""
    
    problems = []
    
    print("=" * 60)
    print("COMPREHENSIVE QUERY API TEST")
    print("=" * 60)
    
    # Step 1: Write test data with known timestamps
    print("\n1. Writing test data...")
    
    # Use current time in nanoseconds
    current_time_ns = int(time.time() * 1e9)
    
    test_data = {
        "writes": [
            {
                "measurement": "test_metrics",
                "tags": {"location": "us-west", "host": "server1"},
                "fields": {"cpu": 45.0, "memory": 67.8},
                "timestamp": current_time_ns - 3600 * 1e9  # 1 hour ago
            },
            {
                "measurement": "test_metrics", 
                "tags": {"location": "us-west", "host": "server1"},
                "fields": {"cpu": 50.0, "memory": 70.0},
                "timestamp": current_time_ns - 1800 * 1e9  # 30 min ago
            },
            {
                "measurement": "test_metrics",
                "tags": {"location": "us-west", "host": "server1"},
                "fields": {"cpu": 55.0, "memory": 72.5},
                "timestamp": current_time_ns  # now
            },
            {
                "measurement": "test_metrics",
                "tags": {"location": "us-east", "host": "server2"},
                "fields": {"cpu": 30.0, "memory": 45.0},
                "timestamp": current_time_ns - 1800 * 1e9  # 30 min ago
            }
        ]
    }
    
    response = requests.post(f"{base_url}/write", json=test_data)
    if response.status_code != 200:
        problems.append(f"Write failed: {response.status_code} - {response.text}")
        return problems
    
    print(f"  ✓ Written {len(test_data['writes'])} points")
    print(f"  Timestamps range: {current_time_ns - 3600*1e9:.0f} to {current_time_ns:.0f}")
    
    # Wait for indexing
    time.sleep(2)
    
    # Step 2: Test queries with different time formats
    print("\n2. Testing time format handling...")
    
    test_queries = [
        {
            "name": "Query with nanosecond timestamps",
            "query": {
                "query": "avg:test_metrics(cpu){location:us-west}",
                "startTime": int(current_time_ns - 7200 * 1e9),  # 2 hours ago
                "endTime": int(current_time_ns + 3600 * 1e9)     # 1 hour future
            }
        },
        {
            "name": "Query with string date format",
            "query": {
                "query": "avg:test_metrics(cpu){location:us-west}",
                "startTime": "01-09-2025 00:00:00",
                "endTime": "10-09-2025 00:00:00"
            }
        },
        {
            "name": "Query with no time range",
            "query": {
                "query": "avg:test_metrics(cpu){location:us-west}"
            }
        },
        {
            "name": "Query all fields with empty parentheses", 
            "query": {
                "query": "avg:test_metrics(){location:us-west}",
                "startTime": int(current_time_ns - 7200 * 1e9),
                "endTime": int(current_time_ns + 3600 * 1e9)
            }
        },
        {
            "name": "Query without tag filters",
            "query": {
                "query": "avg:test_metrics(cpu){}",
                "startTime": int(current_time_ns - 7200 * 1e9),
                "endTime": int(current_time_ns + 3600 * 1e9)
            }
        },
        {
            "name": "Query with aggregation interval",
            "query": {
                "query": "avg:test_metrics(cpu){location:us-west}",
                "startTime": int(current_time_ns - 7200 * 1e9),
                "endTime": int(current_time_ns + 3600 * 1e9),
                "aggregationInterval": "30m"
            }
        },
        {
            "name": "Query with group by",
            "query": {
                "query": "avg:test_metrics(cpu){} by {host}",
                "startTime": int(current_time_ns - 7200 * 1e9),
                "endTime": int(current_time_ns + 3600 * 1e9)
            }
        },
        {
            "name": "Query with MAX aggregation",
            "query": {
                "query": "max:test_metrics(cpu,memory){location:us-west}",
                "startTime": int(current_time_ns - 7200 * 1e9),
                "endTime": int(current_time_ns + 3600 * 1e9)
            }
        }
    ]
    
    for test in test_queries:
        print(f"\n  Testing: {test['name']}")
        print(f"  Query: {json.dumps(test['query'], indent=2)}")
        
        response = requests.post(f"{base_url}/query", json=test['query'])
        
        if response.status_code != 200:
            problem = f"{test['name']}: HTTP {response.status_code} - {response.text}"
            problems.append(problem)
            print(f"  ✗ {problem}")
        else:
            try:
                result = response.json()
                
                if result.get("status") == "error":
                    problem = f"{test['name']}: {result.get('error', {}).get('message', 'Unknown error')}"
                    problems.append(problem)
                    print(f"  ✗ {problem}")
                elif result.get("status") == "success":
                    series_count = result.get("statistics", {}).get("series_count", 0)
                    point_count = result.get("statistics", {}).get("point_count", 0)
                    
                    if series_count == 0 and "no time range" not in test['name'].lower():
                        problem = f"{test['name']}: Query returned 0 series (expected data)"
                        problems.append(problem)
                        print(f"  ✗ {problem}")
                    else:
                        print(f"  ✓ Success: {series_count} series, {point_count} points")
                        
                        # Show first series if exists
                        if result.get("series"):
                            first_series = result["series"][0]
                            print(f"    Sample: {first_series.get('measurement')} with tags {first_series.get('tags')}")
            except Exception as e:
                problem = f"{test['name']}: Failed to parse response - {e}"
                problems.append(problem)
                print(f"  ✗ {problem}")
    
    # Step 3: Test edge cases
    print("\n3. Testing edge cases...")
    
    edge_cases = [
        {
            "name": "Invalid query syntax",
            "query": {
                "query": "invalid syntax here",
                "startTime": current_time_ns - 3600 * 1e9,
                "endTime": current_time_ns
            }
        },
        {
            "name": "Non-existent measurement",
            "query": {
                "query": "avg:nonexistent(value){}",
                "startTime": current_time_ns - 3600 * 1e9,
                "endTime": current_time_ns
            }
        },
        {
            "name": "Empty query string",
            "query": {
                "query": "",
                "startTime": current_time_ns - 3600 * 1e9,
                "endTime": current_time_ns
            }
        }
    ]
    
    for test in edge_cases:
        print(f"\n  Testing: {test['name']}")
        response = requests.post(f"{base_url}/query", json=test['query'])
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                series_count = result.get("statistics", {}).get("series_count", 0)
                if series_count > 0:
                    problem = f"{test['name']}: Should have failed but returned data"
                    problems.append(problem)
                    print(f"  ✗ {problem}")
                else:
                    print(f"  ✓ Correctly returned no data")
            else:
                print(f"  ✓ Correctly returned error: {result.get('error', {}).get('message')}")
        else:
            print(f"  ✓ Correctly failed with HTTP {response.status_code}")
    
    return problems

if __name__ == "__main__":
    port = 8086
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    base_url = f"http://localhost:{port}"
    
    problems = test_comprehensive_query(base_url)
    
    print("\n" + "=" * 60)
    print("TEST RESULTS")
    print("=" * 60)
    
    if problems:
        print(f"\n❌ Found {len(problems)} problems:\n")
        for i, problem in enumerate(problems, 1):
            print(f"{i}. {problem}")
        sys.exit(1)
    else:
        print("\n✅ All tests passed!")
        sys.exit(0)