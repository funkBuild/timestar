#!/usr/bin/env python3
"""
Debug script to troubleshoot query issues
"""

import json
import requests
import time
import sys

def debug_query_issue(base_url="http://localhost:8086"):
    print("=" * 60)
    print("QUERY DEBUG DIAGNOSTICS")
    print("=" * 60)
    
    # Step 1: Write data with very specific timestamp
    print("\n1. Writing test data with specific timestamp...")
    
    # Use a fixed timestamp for debugging
    test_timestamp = 1700000000000000000  # Nov 14, 2023 in nanoseconds
    
    single_write = {
        "measurement": "debug_test",
        "tags": {
            "host": "debug_server",
            "location": "test_lab"
        },
        "fields": {
            "temperature": 25.5,
            "humidity": 60.0
        },
        "timestamp": test_timestamp
    }
    
    print(f"  Writing data point:")
    print(f"    Measurement: {single_write['measurement']}")
    print(f"    Tags: {single_write['tags']}")
    print(f"    Fields: {single_write['fields']}")
    print(f"    Timestamp: {test_timestamp}")
    print(f"    Timestamp (seconds): {test_timestamp / 1e9}")
    
    response = requests.post(f"{base_url}/write", json=single_write)
    print(f"\n  Write response: {response.status_code}")
    if response.status_code == 200:
        print(f"  Write result: {response.json()}")
    else:
        print(f"  Write error: {response.text}")
        return
    
    # Step 2: Wait and then query with various time ranges
    print("\n2. Waiting 3 seconds for data to be indexed...")
    time.sleep(3)
    
    print("\n3. Testing queries with different time ranges...")
    
    # Test queries with increasingly wider time ranges
    test_cases = [
        {
            "name": "Exact timestamp match",
            "start": test_timestamp,
            "end": test_timestamp
        },
        {
            "name": "1 second window around timestamp",
            "start": test_timestamp - int(1e9),
            "end": test_timestamp + int(1e9)
        },
        {
            "name": "1 hour window",
            "start": test_timestamp - int(3600e9),
            "end": test_timestamp + int(3600e9)
        },
        {
            "name": "1 day window",
            "start": test_timestamp - int(86400e9),
            "end": test_timestamp + int(86400e9)
        },
        {
            "name": "Very wide range (year 2020-2030)",
            "start": 1577836800000000000,  # Jan 1, 2020
            "end": 1893456000000000000     # Jan 1, 2030
        },
        {
            "name": "No time range specified",
            "start": None,
            "end": None
        }
    ]
    
    for test_case in test_cases:
        print(f"\n  Test: {test_case['name']}")
        
        query = {
            "query": "avg:debug_test(temperature){host:debug_server}"
        }
        
        if test_case['start'] is not None:
            query["startTime"] = test_case['start']
            query["endTime"] = test_case['end']
            print(f"    Start: {test_case['start']} ({test_case['start']/1e9:.0f}s)")
            print(f"    End:   {test_case['end']} ({test_case['end']/1e9:.0f}s)")
        else:
            print(f"    No time range specified")
        
        response = requests.post(f"{base_url}/query", json=query)
        
        if response.status_code != 200:
            print(f"    ✗ HTTP {response.status_code}: {response.text}")
        else:
            result = response.json()
            if result.get("status") == "success":
                series_count = result.get("statistics", {}).get("series_count", 0)
                point_count = result.get("statistics", {}).get("point_count", 0)
                
                if series_count > 0:
                    print(f"    ✓ Found {series_count} series with {point_count} points!")
                    # Show the data
                    for series in result.get("series", []):
                        print(f"      Series: {series.get('measurement')}")
                        print(f"      Tags: {series.get('tags')}")
                        for field, data in series.get("fields", {}).items():
                            if "timestamps" in data and "values" in data:
                                print(f"      Field '{field}':")
                                for i in range(min(3, len(data["timestamps"]))):
                                    print(f"        {data['timestamps'][i]}: {data['values'][i]}")
                else:
                    print(f"    ✗ No data found (0 series)")
            else:
                print(f"    ✗ Error: {result.get('error', {}).get('message', 'Unknown')}")
    
    # Step 4: Test query without any filters
    print("\n4. Testing queries without filters...")
    
    simple_queries = [
        {
            "name": "All data from measurement",
            "query": "avg:debug_test(){}"
        },
        {
            "name": "All fields with all tags", 
            "query": "avg:debug_test(temperature,humidity){}"
        },
        {
            "name": "Wrong measurement name",
            "query": "avg:wrong_measurement(){}"
        }
    ]
    
    for sq in simple_queries:
        print(f"\n  Query: {sq['name']}")
        print(f"    Query string: {sq['query']}")
        
        query = {
            "query": sq["query"],
            "startTime": 1577836800000000000,  # Jan 1, 2020
            "endTime": 1893456000000000000     # Jan 1, 2030
        }
        
        response = requests.post(f"{base_url}/query", json=query)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("status") == "success":
                series_count = result.get("statistics", {}).get("series_count", 0)
                print(f"    Result: {series_count} series found")
            else:
                print(f"    Error: {result.get('error', {}).get('message')}")
        else:
            print(f"    HTTP Error: {response.status_code}")
    
    # Step 5: Check what measurements exist
    print("\n5. Checking available measurements...")
    
    response = requests.get(f"{base_url}/measurements")
    if response.status_code == 200:
        try:
            measurements = response.json()
            print(f"  Available measurements: {measurements}")
        except:
            print(f"  Response: {response.text}")
    else:
        print(f"  Cannot get measurements: HTTP {response.status_code}")

if __name__ == "__main__":
    port = 8086
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    
    base_url = f"http://localhost:{port}"
    debug_query_issue(base_url)