#!/usr/bin/env python3

import requests
import json
import time

url = "http://localhost:8086"

# First, write some test data
write_data = {
    "measurement": "test_metric",
    "tags": {
        "host": "server01"
    },
    "fields": {
        "value": 123.45
    },
    "timestamp": int(time.time() * 1e9)  # Current time in nanoseconds
}

print("Writing test data...")
response = requests.post(f"{url}/write", json=write_data)
print(f"Write response: {response.status_code}")
if response.status_code != 200:
    print(f"Write error: {response.text}")
    exit(1)

# Wait a bit for data to be processed
time.sleep(1)

# Now try a simple query
query_data = {
    "query": "avg:test_metric(value){host:server01}",
    "startTime": int((time.time() - 3600) * 1e9),  # 1 hour ago
    "endTime": int((time.time() + 3600) * 1e9)      # 1 hour from now
}

print("\nQuerying data...")
print(f"Query: {query_data}")

try:
    response = requests.post(f"{url}/query", json=query_data)
    print(f"Query response: {response.status_code}")
    if response.status_code == 200:
        print(f"Query result: {json.dumps(response.json(), indent=2)}")
    else:
        print(f"Query error: {response.text}")
except Exception as e:
    print(f"Query failed with exception: {e}")