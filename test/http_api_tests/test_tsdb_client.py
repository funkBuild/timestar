#!/usr/bin/env python3
"""
TSDB HTTP Server Test Client
Tests write and query operations with random data generation
"""

import requests
import json
import time
import random
import string
import argparse
import sys
import math
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import statistics

class TSDBTestClient:
    def __init__(self, host: str = "localhost", port: int = 8086):
        self.base_url = f"http://{host}:{port}"
        self.write_url = f"{self.base_url}/write"
        self.query_url = f"{self.base_url}/query"
        self.health_url = f"{self.base_url}/health"
        
    def check_health(self) -> bool:
        """Check if server is healthy"""
        try:
            response = requests.get(self.health_url, timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f"Health check failed: {e}")
            return False
    
    def generate_random_measurement_name(self) -> str:
        """Generate a random measurement name"""
        prefix = random.choice(['sensor', 'device', 'system', 'metric'])
        suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        return f"{prefix}_{suffix}"
    
    def generate_random_tags(self, num_tags: int = 3) -> Dict[str, str]:
        """Generate random tags"""
        tag_names = ['location', 'host', 'region', 'environment', 'datacenter', 
                     'service', 'cluster', 'instance', 'zone', 'rack']
        tags = {}
        for _ in range(min(num_tags, len(tag_names))):
            tag_name = random.choice(tag_names)
            if tag_name not in tags:
                if tag_name == 'location':
                    tags[tag_name] = random.choice(['us-west', 'us-east', 'eu-central', 'ap-south'])
                elif tag_name == 'host':
                    tags[tag_name] = f"server-{random.randint(1, 100):03d}"
                elif tag_name == 'environment':
                    tags[tag_name] = random.choice(['prod', 'staging', 'dev', 'test'])
                elif tag_name == 'region':
                    tags[tag_name] = random.choice(['north', 'south', 'east', 'west'])
                else:
                    tags[tag_name] = f"{tag_name}_{random.randint(1, 10)}"
        return tags
    
    def generate_time_series_data(self, 
                                  measurement: str,
                                  tags: Dict[str, str],
                                  start_time: datetime,
                                  num_points: int = 100,
                                  interval_seconds: int = 60) -> List[Dict]:
        """Generate time series data points"""
        data_points = []
        current_time = start_time
        
        # Generate consistent patterns for testing
        base_temperature = 20.0 + random.uniform(-5, 5)
        base_humidity = 50.0 + random.uniform(-10, 10)
        base_pressure = 1013.25 + random.uniform(-10, 10)
        
        for i in range(num_points):
            # Add some realistic variations
            temp_variation = 5 * math.sin(2 * math.pi * i / 24)  # Daily cycle
            temp_noise = random.uniform(-1, 1)
            
            humidity_variation = 10 * math.sin(2 * math.pi * i / 24 + math.pi/4)
            humidity_noise = random.uniform(-2, 2)
            
            pressure_variation = 2 * math.sin(2 * math.pi * i / 48)  # 2-day cycle
            pressure_noise = random.uniform(-0.5, 0.5)
            
            point = {
                "measurement": measurement,
                "tags": tags,
                "fields": {
                    "temperature": round(base_temperature + temp_variation + temp_noise, 2),
                    "humidity": round(max(0, min(100, base_humidity + humidity_variation + humidity_noise)), 2),
                    "pressure": round(base_pressure + pressure_variation + pressure_noise, 2),
                    "status": random.choice([True, False, True, True]),  # Mostly true
                    "message": random.choice([
                        "normal", "normal", "normal", 
                        "warning: high temp", "info: calibrated"
                    ])
                },
                "timestamp": int(current_time.timestamp() * 1e9)  # Nanoseconds
            }
            data_points.append(point)
            current_time += timedelta(seconds=interval_seconds)
        
        return data_points
    
    def write_batch(self, data_points: List[Dict]) -> Tuple[bool, Optional[Dict]]:
        """Write batch of data points to TSDB"""
        payload = {"writes": data_points}
        
        try:
            response = requests.post(
                self.write_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                return True, response.json()
            else:
                print(f"Write failed with status {response.status_code}: {response.text}")
                return False, None
                
        except requests.exceptions.RequestException as e:
            print(f"Write request failed: {e}")
            return False, None
    
    def query_data(self, 
                   measurement: str,
                   start_time: datetime,
                   end_time: datetime,
                   tags: Optional[Dict[str, str]] = None,
                   fields: Optional[List[str]] = None,
                   aggregation: str = "avg") -> Tuple[bool, Optional[Dict]]:
        """Query data from TSDB"""
        
        # Build query string
        query_parts = [f"{aggregation}:{measurement}"]
        
        # Add fields
        if fields:
            query_parts[0] += f"({','.join(fields)})"
        else:
            query_parts[0] += "()"
        
        # Add tag filters (scopes)
        if tags:
            scope_parts = [f"{k}:{v}" for k, v in tags.items()]
            query_parts[0] += "{" + ",".join(scope_parts) + "}"
        else:
            query_parts[0] += "{}"
        
        query_string = query_parts[0]
        
        # Format timestamps
        start_str = start_time.strftime("%d-%m-%Y %H:%M:%S")
        end_str = end_time.strftime("%d-%m-%Y %H:%M:%S")
        
        payload = {
            "query": query_string,
            "startTime": start_str,
            "endTime": end_str
        }
        
        print(f"Executing query: {query_string}")
        print(f"Time range: {start_str} to {end_str}")
        
        try:
            response = requests.post(
                self.query_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                return True, response.json()
            else:
                print(f"Query failed with status {response.status_code}: {response.text}")
                return False, None
                
        except requests.exceptions.RequestException as e:
            print(f"Query request failed: {e}")
            return False, None
    
    def verify_query_results(self, 
                           original_data: List[Dict],
                           query_result: Dict,
                           field: str,
                           aggregation: str = "avg") -> bool:
        """Verify query results match expected values"""
        if query_result.get("status") != "success":
            print(f"Query failed: {query_result}")
            return False
        
        series = query_result.get("series", [])
        if not series:
            print("No series returned in query result")
            return False
        
        # Extract values from original data for the field
        original_values = []
        for point in original_data:
            if field in point["fields"]:
                value = point["fields"][field]
                # Only include numeric values for aggregation
                if isinstance(value, (int, float)):
                    original_values.append(value)
        
        if not original_values:
            print(f"No numeric values found for field {field}")
            return True  # Skip verification for non-numeric fields
        
        # Calculate expected aggregation
        if aggregation == "avg":
            expected = statistics.mean(original_values)
        elif aggregation == "min":
            expected = min(original_values)
        elif aggregation == "max":
            expected = max(original_values)
        elif aggregation == "sum":
            expected = sum(original_values)
        else:
            print(f"Unknown aggregation: {aggregation}")
            return False
        
        # Check if the field exists in any series
        field_found = False
        for s in series:
            if field in s.get("fields", {}):
                field_data = s["fields"][field]
                if "values" in field_data and field_data["values"]:
                    # For aggregated queries, we expect a single value
                    actual_value = field_data["values"][0] if isinstance(field_data["values"], list) else field_data["values"]
                    
                    # Allow small floating point differences
                    tolerance = abs(expected * 0.01) if expected != 0 else 0.01
                    
                    if abs(actual_value - expected) <= tolerance:
                        print(f"✓ Field {field} {aggregation}: expected={expected:.2f}, actual={actual_value:.2f}")
                        field_found = True
                    else:
                        print(f"✗ Field {field} {aggregation} mismatch: expected={expected:.2f}, actual={actual_value:.2f}")
                        return False
        
        return field_found

def run_comprehensive_test(client: TSDBTestClient) -> bool:
    """Run comprehensive test suite"""
    print("\n" + "="*60)
    print("TSDB Comprehensive Test Suite")
    print("="*60)
    
    # Check server health
    print("\n1. Checking server health...")
    if not client.check_health():
        print("✗ Server is not healthy")
        return False
    print("✓ Server is healthy")
    
    # Generate test data
    print("\n2. Generating test data...")
    measurement = client.generate_random_measurement_name()
    tags = client.generate_random_tags(num_tags=3)
    
    print(f"  Measurement: {measurement}")
    print(f"  Tags: {json.dumps(tags, indent=2)}")
    
    # Use recent timestamps to avoid issues with time zones
    start_time = datetime.now() - timedelta(hours=2)
    num_points = 50
    
    data_points = client.generate_time_series_data(
        measurement=measurement,
        tags=tags,
        start_time=start_time,
        num_points=num_points,
        interval_seconds=60
    )
    
    print(f"  Generated {len(data_points)} data points")
    print(f"  Sample point: {json.dumps(data_points[0], indent=2)}")
    
    # Write data
    print("\n3. Writing data to TSDB...")
    success, response = client.write_batch(data_points)
    if not success:
        print("✗ Failed to write data")
        return False
    print(f"✓ Successfully wrote {len(data_points)} points")
    
    # Wait for data to be indexed
    print("\n4. Waiting for data to be indexed...")
    time.sleep(2)
    
    # Perform queries
    print("\n5. Performing queries...")
    
    # Calculate query time range
    query_start = start_time - timedelta(minutes=10)
    query_end = start_time + timedelta(hours=3)
    
    test_passed = True
    
    # Test different aggregations
    aggregations = ["avg", "min", "max", "sum"]
    numeric_fields = ["temperature", "humidity", "pressure"]
    
    for agg in aggregations:
        print(f"\n  Testing {agg.upper()} aggregation:")
        
        for field in numeric_fields:
            success, result = client.query_data(
                measurement=measurement,
                start_time=query_start,
                end_time=query_end,
                tags=tags,
                fields=[field],
                aggregation=agg
            )
            
            if not success:
                print(f"    ✗ Query failed for field {field}")
                test_passed = False
                continue
            
            if not client.verify_query_results(data_points, result, field, agg):
                test_passed = False
    
    # Test query without tag filters
    print("\n  Testing query without tag filters:")
    success, result = client.query_data(
        measurement=measurement,
        start_time=query_start,
        end_time=query_end,
        tags=None,  # No tag filters
        fields=["temperature"],
        aggregation="avg"
    )
    
    if success:
        print("    ✓ Query without tags succeeded")
    else:
        print("    ✗ Query without tags failed")
        test_passed = False
    
    # Test query with partial tags
    if len(tags) > 1:
        print("\n  Testing query with partial tag filters:")
        partial_tags = {k: v for k, v in list(tags.items())[:1]}
        
        success, result = client.query_data(
            measurement=measurement,
            start_time=query_start,
            end_time=query_end,
            tags=partial_tags,
            fields=["temperature", "humidity"],
            aggregation="avg"
        )
        
        if success:
            print(f"    ✓ Query with partial tags succeeded (tags: {partial_tags})")
        else:
            print(f"    ✗ Query with partial tags failed")
            test_passed = False
    
    return test_passed

def main():
    parser = argparse.ArgumentParser(description="TSDB HTTP Server Test Client")
    parser.add_argument("--host", default="localhost", help="Server host (default: localhost)")
    parser.add_argument("--port", type=int, default=8086, help="Server port (default: 8086)")
    parser.add_argument("--stress", action="store_true", help="Run stress test with more data")
    
    args = parser.parse_args()
    
    client = TSDBTestClient(host=args.host, port=args.port)
    
    if args.stress:
        print("Running stress test...")
        # TODO: Implement stress test
        print("Stress test not yet implemented")
    else:
        success = run_comprehensive_test(client)
        
        print("\n" + "="*60)
        if success:
            print("✓ ALL TESTS PASSED")
            sys.exit(0)
        else:
            print("✗ SOME TESTS FAILED")
            sys.exit(1)

if __name__ == "__main__":
    main()