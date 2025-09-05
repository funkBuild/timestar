#!/usr/bin/env python3
"""
End-to-end test for TSDB HTTP query endpoint
"""

import json
import requests
import time
import sys
from datetime import datetime

def test_query_endpoint(base_url="http://localhost:8086"):
    """Test the query endpoint with sample data"""
    
    print(f"Testing TSDB HTTP Server at {base_url}")
    
    # Step 1: Health check
    print("\n1. Health Check...")
    try:
        response = requests.get(f"{base_url}/health")
        if response.status_code == 200:
            print("   ✓ Server is healthy")
        else:
            print(f"   ✗ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ✗ Cannot connect to server: {e}")
        return False
    
    # Step 2: Write test data
    print("\n2. Writing test data...")
    
    # Single write
    single_write = {
        "measurement": "temperature",
        "tags": {
            "location": "us-west",
            "sensor": "temp-01"
        },
        "fields": {
            "value": 23.5
        },
        "timestamp": 1700000000000000000
    }
    
    response = requests.post(f"{base_url}/write", json=single_write)
    if response.status_code == 200:
        print(f"   ✓ Single write successful: {response.json()}")
    else:
        print(f"   ✗ Single write failed: {response.status_code} - {response.text}")
        return False
    
    # Batch write
    batch_write = {
        "writes": [
            {
                "measurement": "temperature",
                "tags": {"location": "us-west", "sensor": "temp-01"},
                "fields": {"value": 24.0},
                "timestamp": 1700000001000000000
            },
            {
                "measurement": "temperature",
                "tags": {"location": "us-west", "sensor": "temp-01"},
                "fields": {"value": 24.5},
                "timestamp": 1700000002000000000
            },
            {
                "measurement": "temperature",
                "tags": {"location": "us-east", "sensor": "temp-02"},
                "fields": {"value": 18.5},
                "timestamp": 1700000000000000000
            },
            {
                "measurement": "temperature",
                "tags": {"location": "us-east", "sensor": "temp-02"},
                "fields": {"value": 19.0},
                "timestamp": 1700000001000000000
            },
            {
                "measurement": "humidity",
                "tags": {"location": "us-west", "sensor": "hum-01"},
                "fields": {"value": 65.0},
                "timestamp": 1700000000000000000
            }
        ]
    }
    
    response = requests.post(f"{base_url}/write", json=batch_write)
    if response.status_code == 200:
        print(f"   ✓ Batch write successful: {response.json()}")
    else:
        print(f"   ✗ Batch write failed: {response.status_code} - {response.text}")
        return False
    
    # Step 3: Query the data
    print("\n3. Querying data...")
    
    # Wait a bit for data to be flushed
    time.sleep(1)
    
    # Query 1: Simple query for temperature in us-west
    query1 = {
        "query": "avg:temperature(value){location:us-west}",
        "startTime": "14-11-2023 00:00:00",
        "endTime": "15-11-2023 00:00:00"
    }
    
    print("\n   Query 1: Average temperature in us-west")
    print(f"   Request: {query1['query']}")
    
    response = requests.post(f"{base_url}/query", json=query1)
    if response.status_code == 200:
        result = response.json()
        if result.get("status") == "success":
            print(f"   ✓ Query successful")
            print(f"   Series count: {result.get('statistics', {}).get('series_count', 0)}")
            print(f"   Point count: {result.get('statistics', {}).get('point_count', 0)}")
            
            # Display results
            for series in result.get("series", []):
                print(f"\n   Series: {series['measurement']}")
                print(f"   Tags: {series['tags']}")
                for field, data in series.get("fields", {}).items():
                    timestamps = data[0]
                    values = data[1]
                    print(f"   Field '{field}':")
                    for i in range(min(5, len(timestamps))):  # Show first 5 points
                        print(f"     {timestamps[i]}: {values[i]}")
                    if len(timestamps) > 5:
                        print(f"     ... and {len(timestamps) - 5} more points")
        else:
            print(f"   ✗ Query failed: {result.get('error', {}).get('message', 'Unknown error')}")
    else:
        print(f"   ✗ Query request failed: {response.status_code}")
        try:
            print(f"   Response: {response.json()}")
        except:
            print(f"   Response: {response.text}")
    
    # Query 2: Query with multiple fields
    query2 = {
        "query": "avg:temperature(){location:us-west}",
        "startTime": "14-11-2023 00:00:00",
        "endTime": "15-11-2023 00:00:00"
    }
    
    print("\n   Query 2: All fields for temperature in us-west")
    print(f"   Request: {query2['query']}")
    
    response = requests.post(f"{base_url}/query", json=query2)
    if response.status_code == 200:
        result = response.json()
        if result.get("status") == "success":
            print(f"   ✓ Query successful")
            print(f"   Series count: {result.get('statistics', {}).get('series_count', 0)}")
        else:
            print(f"   ✗ Query failed: {result.get('error', {}).get('message', 'Unknown error')}")
    else:
        print(f"   ✗ Query request failed: {response.status_code}")
    
    # Query 3: Query with aggregation
    query3 = {
        "query": "max:temperature(value){}",
        "startTime": "14-11-2023 00:00:00",
        "endTime": "15-11-2023 00:00:00"
    }
    
    print("\n   Query 3: Max temperature across all locations")
    print(f"   Request: {query3['query']}")
    
    response = requests.post(f"{base_url}/query", json=query3)
    if response.status_code == 200:
        result = response.json()
        if result.get("status") == "success":
            print(f"   ✓ Query successful")
            print(f"   Series count: {result.get('statistics', {}).get('series_count', 0)}")
        else:
            print(f"   ✗ Query failed: {result.get('error', {}).get('message', 'Unknown error')}")
    else:
        print(f"   ✗ Query request failed: {response.status_code}")
    
    print("\n✅ All tests completed!")
    return True

if __name__ == "__main__":
    # Check if custom port is provided
    port = 8086
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    base_url = f"http://localhost:{port}"
    
    success = test_query_endpoint(base_url)
    sys.exit(0 if success else 1)