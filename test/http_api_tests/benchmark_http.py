#!/usr/bin/env python3
"""
HTTP Benchmark Test for TSDB
Measures insert and query performance over 1000 operations
"""

import requests
import json
import time
import random
import statistics
import argparse
from datetime import datetime

class TSDBBenchmark:
    def __init__(self, host='localhost', port=8086):
        self.base_url = f'http://{host}:{port}'
        self.write_url = f'{self.base_url}/write'
        self.query_url = f'{self.base_url}/query'
        self.series_keys = []
        
    def generate_insert_data(self, base_timestamp, num_points=10):
        """Generate insert data with multiple points"""
        measurement = random.choice(['cpu', 'memory', 'disk', 'network', 'temperature'])
        host = f'server{random.randint(1, 5):02d}'
        region = random.choice(['us-east', 'us-west', 'eu-central', 'ap-south'])
        field = random.choice(['usage', 'value', 'rate', 'total'])
        
        # Store series key for queries
        series_key = f"{measurement},host={host},region={region}.{field}"
        self.series_keys.append(series_key)
        
        # Generate batch of points
        writes = []
        for i in range(num_points):
            writes.append({
                'measurement': measurement,
                'tags': {
                    'host': host,
                    'region': region
                },
                'fields': {
                    field: 100.0 + random.random() * 50
                },
                'timestamp': base_timestamp + i * 1000000000  # 1 second apart
            })
        
        return {'writes': writes}
    
    def run_insert_benchmark(self, num_operations=1000, points_per_op=10):
        """Run insert benchmark"""
        print(f"Running insert benchmark: {num_operations} operations, {points_per_op} points each")
        
        latencies = []
        base_timestamp = int(time.time() * 1e9)
        
        for i in range(num_operations):
            data = self.generate_insert_data(base_timestamp + i * 10000000000, points_per_op)
            
            start_time = time.perf_counter()
            try:
                response = requests.post(self.write_url, json=data)
                response.raise_for_status()
            except Exception as e:
                print(f"Insert failed: {e}")
                continue
            end_time = time.perf_counter()
            
            latency_ms = (end_time - start_time) * 1000
            latencies.append(latency_ms)
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"  Inserts: {i + 1}/{num_operations}", end='\r')
        
        print(f"  Inserts: {num_operations}/{num_operations} - Complete!")
        return latencies
    
    def run_query_benchmark(self, num_operations=1000):
        """Run query benchmark"""
        print(f"\nRunning query benchmark: {num_operations} operations")
        
        if not self.series_keys:
            print("No series keys available for queries!")
            return []
        
        latencies = []
        
        # Time range for queries
        end_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        start_time = "01-01-2024 00:00:00"
        
        for i in range(num_operations):
            # Pick random series to query
            measurement, tags_field = random.choice(self.series_keys).split(',', 1)
            tags_part, field = tags_field.rsplit('.', 1)
            
            # Parse tags
            tag_pairs = tags_part.split(',')
            tag_dict = {}
            for pair in tag_pairs:
                k, v = pair.split('=')
                tag_dict[k] = v
            
            # Build query string
            scope_parts = [f"{k}:{v}" for k, v in tag_dict.items()]
            scope = '{' + ','.join(scope_parts) + '}'
            query_string = f"avg:{measurement}({field}){scope}"
            
            query_data = {
                'query': query_string,
                'startTime': start_time,
                'endTime': end_time
            }
            
            start = time.perf_counter()
            try:
                response = requests.post(self.query_url, json=query_data)
                response.raise_for_status()
            except Exception as e:
                print(f"Query failed: {e}")
                continue
            end = time.perf_counter()
            
            latency_ms = (end - start) * 1000
            latencies.append(latency_ms)
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"  Queries: {i + 1}/{num_operations}", end='\r')
        
        print(f"  Queries: {num_operations}/{num_operations} - Complete!")
        return latencies
    
    def calculate_stats(self, latencies):
        """Calculate statistics from latency data"""
        if not latencies:
            return None
        
        sorted_latencies = sorted(latencies)
        
        return {
            'mean': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0,
            'min': min(latencies),
            'max': max(latencies),
            'p95': sorted_latencies[int(len(sorted_latencies) * 0.95)],
            'p99': sorted_latencies[int(len(sorted_latencies) * 0.99)]
        }
    
    def print_stats(self, stats, operation_type):
        """Print statistics in a formatted way"""
        if not stats:
            print(f"No statistics available for {operation_type}")
            return
        
        print(f"\n{operation_type} Statistics (ms):")
        print(f"  Mean:   {stats['mean']:.3f} ms")
        print(f"  Median: {stats['median']:.3f} ms")
        print(f"  StdDev: {stats['stdev']:.3f} ms")
        print(f"  Min:    {stats['min']:.3f} ms")
        print(f"  Max:    {stats['max']:.3f} ms")
        print(f"  P95:    {stats['p95']:.3f} ms")
        print(f"  P99:    {stats['p99']:.3f} ms")
    
    def run_benchmark(self):
        """Run complete benchmark"""
        print("\n" + "="*50)
        print("TSDB HTTP Performance Benchmark")
        print("="*50)
        
        # Check server health
        try:
            response = requests.get(f'{self.base_url}/health')
            response.raise_for_status()
            print(f"Server is healthy at {self.base_url}")
        except Exception as e:
            print(f"Failed to connect to server: {e}")
            return
        
        print("\nConfiguration:")
        print(f"  Server URL:        {self.base_url}")
        print(f"  Insert operations: 1000")
        print(f"  Points per insert: 10")
        print(f"  Total data points: 10000")
        print(f"  Query operations:  1000")
        print("="*50 + "\n")
        
        # Run insert benchmark
        start_time = time.perf_counter()
        insert_latencies = self.run_insert_benchmark(1000, 10)
        insert_duration = time.perf_counter() - start_time
        
        # Run query benchmark
        start_time = time.perf_counter()
        query_latencies = self.run_query_benchmark(1000)
        query_duration = time.perf_counter() - start_time
        
        # Calculate and print results
        print("\n" + "="*50)
        print("Performance Results")
        print("="*50)
        
        # Insert statistics
        insert_stats = self.calculate_stats(insert_latencies)
        self.print_stats(insert_stats, "Insert")
        
        if insert_latencies:
            insert_throughput = len(insert_latencies) / insert_duration
            point_throughput = (len(insert_latencies) * 10) / insert_duration
            print(f"\nInsert Throughput:")
            print(f"  Operations/sec: {insert_throughput:.1f}")
            print(f"  Points/sec:     {point_throughput:.1f}")
            print(f"  Total time:     {insert_duration*1000:.0f} ms")
        
        # Query statistics
        query_stats = self.calculate_stats(query_latencies)
        self.print_stats(query_stats, "Query")
        
        if query_latencies:
            query_throughput = len(query_latencies) / query_duration
            print(f"\nQuery Throughput:")
            print(f"  Operations/sec: {query_throughput:.1f}")
            print(f"  Total time:     {query_duration*1000:.0f} ms")
        
        print("\nBenchmark completed successfully!")
        print("="*50)

def main():
    parser = argparse.ArgumentParser(description='TSDB HTTP Benchmark')
    parser.add_argument('--host', default='localhost', help='Server host')
    parser.add_argument('--port', type=int, default=8086, help='Server port')
    
    args = parser.parse_args()
    
    benchmark = TSDBBenchmark(args.host, args.port)
    benchmark.run_benchmark()

if __name__ == '__main__':
    main()