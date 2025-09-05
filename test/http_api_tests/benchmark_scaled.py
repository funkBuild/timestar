#!/usr/bin/env python3
"""
Scaled HTTP Benchmark Test for TSDB
Inserts 4 weeks of data at 1-minute intervals with 5 fields
Tests query performance with 15-minute aggregation intervals as returned data size scales up
"""

import requests
import json
import time
import statistics
import argparse
from datetime import datetime, timedelta

class ScaledTSDBBenchmark:
    def __init__(self, host='localhost', port=8086):
        self.base_url = f'http://{host}:{port}'
        self.write_url = f'{self.base_url}/write'
        self.query_url = f'{self.base_url}/query'
        self.measurement = 'metrics'
        self.tags = {'host': 'server01', 'datacenter': 'dc1', 'region': 'us-east'}
        self.fields = ['cpu_usage', 'memory_usage', 'disk_io', 'network_bandwidth', 'temperature']
        
    def insert_data(self):
        """Insert 4 weeks of data at 1-minute intervals with 5 fields"""
        print("Inserting 4 weeks of data at 1-minute intervals...")
        print(f"  Time range: 4 weeks")
        print(f"  Interval: 1 minute")
        print(f"  Fields: {len(self.fields)}")
        
        # Calculate data points
        minutes_in_4_weeks = 4 * 7 * 24 * 60  # 40,320 minutes
        total_points = minutes_in_4_weeks * len(self.fields)  # 201,600 total data points
        print(f"  Total points per field: {minutes_in_4_weeks:,}")
        print(f"  Total data points: {total_points:,}")
        
        # Start time: 4 weeks ago
        base_timestamp = int((time.time() - (4 * 7 * 24 * 60 * 60)) * 1e9)
        
        # Batch size for efficient insertion
        batch_size = 1000  # Insert 1000 minutes at a time
        total_batches = minutes_in_4_weeks // batch_size
        
        start_time = time.perf_counter()
        insert_latencies = []
        
        for batch_num in range(total_batches):
            writes = []
            
            for i in range(batch_size):
                minute_offset = batch_num * batch_size + i
                timestamp = base_timestamp + (minute_offset * 60 * 1e9)
                
                # Create fields with different starting offsets
                field_values = {}
                for field_idx, field_name in enumerate(self.fields):
                    # Each field starts at a different offset and increases
                    value = (field_idx * 1000) + minute_offset
                    field_values[field_name] = float(value)
                
                writes.append({
                    'measurement': self.measurement,
                    'tags': self.tags,
                    'fields': field_values,
                    'timestamp': int(timestamp)
                })
            
            # Send batch
            batch_start = time.perf_counter()
            try:
                response = requests.post(self.write_url, json={'writes': writes})
                response.raise_for_status()
            except Exception as e:
                print(f"Insert failed: {e}")
                continue
            batch_end = time.perf_counter()
            
            latency_ms = (batch_end - batch_start) * 1000
            insert_latencies.append(latency_ms)
            
            # Progress indicator
            if (batch_num + 1) % 10 == 0:
                progress = ((batch_num + 1) / total_batches) * 100
                print(f"  Progress: {progress:.1f}% ({batch_num + 1}/{total_batches} batches)", end='\r')
        
        end_time = time.perf_counter()
        total_duration = end_time - start_time
        
        print(f"  Progress: 100.0% ({total_batches}/{total_batches} batches) - Complete!")
        print(f"\nInsert Statistics:")
        print(f"  Total time: {total_duration:.2f} seconds")
        print(f"  Points/sec: {total_points/total_duration:,.0f}")
        print(f"  Avg batch latency: {statistics.mean(insert_latencies):.2f} ms")
        
        return base_timestamp, minutes_in_4_weeks
    
    def query_with_fields(self, field_list, start_timestamp, num_minutes, repetitions=10):
        """Query data with specified fields and measure performance"""
        # Build query string (without interval in string - it's a separate field)
        fields_str = ','.join(field_list)
        tags_str = ','.join([f"{k}:{v}" for k, v in self.tags.items()])
        query_string = f"avg:{self.measurement}({fields_str}){{{tags_str}}}"
        
        # Calculate the actual end timestamp based on inserted data
        # Data spans from start_timestamp to start_timestamp + (num_minutes * 60 * 1e9)
        end_timestamp = int(start_timestamp + (num_minutes * 60 * 1e9))
        
        # 15 minutes in nanoseconds = 15 * 60 * 1e9
        aggregation_interval_ns = 15 * 60 * 1000000000  # 900000000000 nanoseconds
        
        query_data = {
            'query': query_string,
            'startTime': int(start_timestamp),  # Already in nanoseconds
            'endTime': end_timestamp,  # End of inserted data range
            'aggregationInterval': aggregation_interval_ns  # 15-minute aggregation interval
        }
        
        latencies = []
        data_sizes = []
        
        # Debug: Print query details for first repetition
        if repetitions > 0:
            print(f"    Query: {query_string}")
            print(f"    Time range: {start_timestamp} to {end_timestamp}")
            print(f"    Aggregation: 15 minutes ({aggregation_interval_ns} ns)")
        
        for i in range(repetitions):
            start = time.perf_counter()
            try:
                response = requests.post(self.query_url, json=query_data)
                response.raise_for_status()
                result = response.json()
            except Exception as e:
                print(f"Query failed: {e}")
                if i == 0:  # Print response for debugging on first failure
                    print(f"Query data sent: {json.dumps(query_data, indent=2)}")
                continue
            end = time.perf_counter()
            
            latency_ms = (end - start) * 1000
            latencies.append(latency_ms)
            
            # Calculate returned data size
            if 'series' in result and len(result['series']) > 0:
                series = result['series'][0]
                total_points = 0
                if 'fields' in series:
                    for field_name, field_data in series['fields'].items():
                        if 'timestamps' in field_data:
                            total_points += len(field_data['timestamps'])
                data_sizes.append(total_points)
            else:
                # No series returned or empty series
                data_sizes.append(0)
                if i == 0:  # Debug on first iteration
                    print(f"    Warning: No data returned. Response: {json.dumps(result, indent=2)[:500]}")
        
        return {
            'fields': len(field_list),
            'latencies': latencies,
            'data_points': data_sizes[0] if data_sizes else 0,
            'mean_latency': statistics.mean(latencies) if latencies else 0,
            'median_latency': statistics.median(latencies) if latencies else 0,
            'p95_latency': sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
            'p99_latency': sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
        }
    
    def run_scaled_benchmark(self):
        """Run the scaled benchmark test"""
        print("\n" + "="*70)
        print("TSDB Scaled Performance Benchmark")
        print("="*70)
        
        # Check server health
        try:
            response = requests.get(f'{self.base_url}/health')
            response.raise_for_status()
            print(f"Server is healthy at {self.base_url}")
        except Exception as e:
            print(f"Failed to connect to server: {e}")
            return
        
        print("\n" + "-"*70)
        print("Phase 1: Data Insertion")
        print("-"*70)
        
        # Insert data
        start_timestamp, num_minutes = self.insert_data()
        
        # Wait a bit for data to settle
        print("\nWaiting for data to settle...")
        time.sleep(5)
        
        print("\n" + "-"*70)
        print("Phase 2: Query Performance Testing")
        print("-"*70)
        print("Testing query performance with increasing number of fields...")
        print("Each test queries the entire 4-week dataset with 15-minute aggregation intervals")
        print("(40,320 points per field aggregated into ~2,688 intervals)")
        print("Running 10 repetitions per configuration\n")
        
        results = []
        
        # Test with 1, 2, 3, 4, and 5 fields
        for num_fields in range(1, 6):
            field_list = self.fields[:num_fields]
            print(f"Testing with {num_fields} field(s): {', '.join(field_list)}")
            
            result = self.query_with_fields(field_list, start_timestamp, num_minutes, repetitions=10)
            results.append(result)
            
            print(f"  Data points returned: {result['data_points']:,}")
            print(f"  Mean latency: {result['mean_latency']:.2f} ms")
            print(f"  Median latency: {result['median_latency']:.2f} ms")
            print(f"  P95 latency: {result['p95_latency']:.2f} ms")
            print(f"  P99 latency: {result['p99_latency']:.2f} ms")
            print()
        
        print("\n" + "="*70)
        print("Performance Summary")
        print("="*70)
        print("\nQuery Performance vs. Data Size:")
        print("-"*70)
        print(f"{'Fields':<10} {'Data Points':<15} {'Mean (ms)':<12} {'Median (ms)':<12} {'P95 (ms)':<12} {'P99 (ms)':<12}")
        print("-"*70)
        
        for result in results:
            print(f"{result['fields']:<10} {result['data_points']:<15,} {result['mean_latency']:<12.2f} {result['median_latency']:<12.2f} {result['p95_latency']:<12.2f} {result['p99_latency']:<12.2f}")
        
        print("\nPerformance Scaling Analysis:")
        print("-"*70)
        
        # Calculate scaling metrics
        base_latency = results[0]['median_latency']
        base_points = results[0]['data_points']
        
        for i, result in enumerate(results):
            if i == 0:
                print(f"{result['fields']} field:  Baseline")
            else:
                if base_points > 0:
                    latency_increase = (result['median_latency'] / base_latency) if base_latency > 0 else 0
                    data_increase = (result['data_points'] / base_points)
                    efficiency = data_increase / latency_increase if latency_increase > 0 else 0
                    print(f"{result['fields']} fields: {data_increase:.1f}x data, {latency_increase:.2f}x latency, {efficiency:.2f} efficiency ratio")
                else:
                    print(f"{result['fields']} fields: No baseline data to compare")
        
        print("\nKey Metrics:")
        print("-"*70)
        print(f"Total data points in database: {num_minutes * len(self.fields):,}")
        print(f"Points per field: {num_minutes:,}")
        if base_points > 0 and results[0]['mean_latency'] > 0:
            print(f"Query throughput (1 field): {base_points / (results[0]['mean_latency']/1000):,.0f} points/sec")
        if results[-1]['data_points'] > 0 and results[-1]['mean_latency'] > 0:
            print(f"Query throughput (5 fields): {results[-1]['data_points'] / (results[-1]['mean_latency']/1000):,.0f} points/sec")
        
        print("\n" + "="*70)
        print("Benchmark completed successfully!")
        print("="*70)

def main():
    parser = argparse.ArgumentParser(description='TSDB Scaled Benchmark')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8086, help='Server port')
    
    args = parser.parse_args()
    
    benchmark = ScaledTSDBBenchmark(args.host, args.port)
    benchmark.run_scaled_benchmark()

if __name__ == '__main__':
    main()