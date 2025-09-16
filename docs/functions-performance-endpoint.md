# GET /functions/performance - Get Function Execution Statistics

## Overview
The function performance endpoint provides detailed execution statistics for time series functions, including performance metrics, resource usage, execution times, and optimization recommendations. This data is essential for query optimization, capacity planning, and troubleshooting performance issues.

## Endpoint Details
- **URL**: `/functions/performance`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Request
```bash
curl -X GET http://localhost:8086/functions/performance
```

### Query Parameters
- **function**: Filter statistics for a specific function name
- **timeframe**: Time period for statistics (1h, 24h, 7d, 30d) (default: 24h)
- **metric**: Specific metric to retrieve (execution_time, memory_usage, error_rate, etc.)
- **sort**: Sort results by metric (execution_time, call_count, error_rate) (default: call_count)
- **limit**: Maximum number of results to return (default: 100)

### Request Examples
```bash
# Get all function performance statistics
curl "http://localhost:8086/functions/performance"

# Get performance for specific function
curl "http://localhost:8086/functions/performance?function=sma"

# Get performance over last 7 days
curl "http://localhost:8086/functions/performance?timeframe=7d"

# Get only execution time metrics
curl "http://localhost:8086/functions/performance?metric=execution_time"

# Get top 10 slowest functions
curl "http://localhost:8086/functions/performance?sort=execution_time&limit=10"
```

## Response Format

### Complete Performance Statistics
```json
{
  "status": "success",
  "timeframe": "24h",
  "collected_at": 1709337600000000000,
  "functions": [
    {
      "name": "sma",
      "statistics": {
        "execution_metrics": {
          "total_executions": 15420,
          "successful_executions": 15398,
          "failed_executions": 22,
          "success_rate": 0.9986,
          "error_rate": 0.0014
        },
        "timing_metrics": {
          "avg_execution_time_ms": 12.5,
          "min_execution_time_ms": 2.1,
          "max_execution_time_ms": 145.8,
          "p50_execution_time_ms": 8.9,
          "p90_execution_time_ms": 23.4,
          "p95_execution_time_ms": 31.2,
          "p99_execution_time_ms": 89.3,
          "total_execution_time_ms": 192750.0
        },
        "resource_metrics": {
          "avg_memory_usage_mb": 2.1,
          "peak_memory_usage_mb": 15.7,
          "total_memory_allocated_mb": 32361.2,
          "avg_cpu_usage_percent": 0.8,
          "peak_cpu_usage_percent": 4.2
        },
        "data_metrics": {
          "avg_input_points": 850,
          "max_input_points": 10000,
          "total_points_processed": 13107000,
          "avg_output_points": 830,
          "data_reduction_ratio": 0.976
        },
        "parameter_analysis": {
          "most_common_parameters": {
            "window": {
              "5": 4521,
              "10": 3892,
              "20": 2156,
              "50": 1843
            }
          },
          "parameter_performance_impact": {
            "window": {
              "correlation_with_execution_time": 0.73,
              "optimal_range": "5-20"
            }
          }
        }
      },
      "performance_tier": "fast",
      "optimization_recommendations": [
        "Consider using window sizes between 5-20 for optimal performance",
        "Large datasets (>5000 points) may benefit from downsampling first"
      ],
      "recent_trends": {
        "execution_time_trend": "stable",
        "memory_usage_trend": "increasing",
        "error_rate_trend": "decreasing"
      }
    }
  ],
  "summary": {
    "total_function_executions": 89450,
    "overall_success_rate": 0.9912,
    "total_execution_time_ms": 1247890.5,
    "average_execution_time_ms": 13.9,
    "most_used_functions": ["sma", "scale", "derivative", "ema", "offset"],
    "slowest_functions": ["derivative", "stddev", "interpolate", "fft", "correlation"]
  }
}
```

### Single Function Performance
```json
{
  "status": "success",
  "function": "derivative",
  "timeframe": "24h",
  "statistics": {
    "execution_metrics": {
      "total_executions": 3204,
      "successful_executions": 3198,
      "failed_executions": 6,
      "success_rate": 0.9981,
      "error_rate": 0.0019,
      "common_errors": [
        {
          "error": "insufficient_data_points",
          "count": 4,
          "percentage": 66.7
        },
        {
          "error": "invalid_time_intervals",
          "count": 2,
          "percentage": 33.3
        }
      ]
    },
    "timing_metrics": {
      "avg_execution_time_ms": 28.7,
      "min_execution_time_ms": 8.2,
      "max_execution_time_ms": 234.6,
      "p50_execution_time_ms": 24.1,
      "p90_execution_time_ms": 45.3,
      "p95_execution_time_ms": 67.8,
      "p99_execution_time_ms": 156.2,
      "execution_time_distribution": {
        "0-10ms": 152,
        "10-25ms": 1856,
        "25-50ms": 1024,
        "50-100ms": 145,
        "100ms+": 27
      }
    },
    "performance_by_input_size": [
      {
        "input_range": "1-100 points",
        "avg_execution_time_ms": 12.3,
        "execution_count": 1024
      },
      {
        "input_range": "100-1000 points",
        "avg_execution_time_ms": 28.7,
        "execution_count": 1856
      },
      {
        "input_range": "1000-10000 points",
        "avg_execution_time_ms": 89.4,
        "execution_count": 324
      }
    ],
    "concurrency_metrics": {
      "max_concurrent_executions": 12,
      "avg_concurrent_executions": 2.3,
      "thread_contention_events": 15,
      "lock_wait_time_ms": 0.8
    }
  },
  "optimization_analysis": {
    "performance_bottlenecks": [
      "Large input datasets cause exponential time increase",
      "Frequent memory allocations for intermediate calculations"
    ],
    "recommended_optimizations": [
      "Use chunked processing for datasets > 1000 points",
      "Consider pre-filtering noisy data before derivative calculation",
      "Use 'forward' method instead of 'central' for better performance"
    ],
    "estimated_improvement": "30-50% faster execution with chunked processing"
  }
}
```

### Performance Trends
```json
{
  "status": "success",
  "function": "sma",
  "timeframe": "7d",
  "trends": {
    "daily_statistics": [
      {
        "date": "2024-03-01",
        "executions": 2156,
        "avg_execution_time_ms": 11.8,
        "success_rate": 0.9989,
        "peak_memory_mb": 12.4
      },
      {
        "date": "2024-03-02", 
        "executions": 2340,
        "avg_execution_time_ms": 12.1,
        "success_rate": 0.9985,
        "peak_memory_mb": 13.2
      }
    ],
    "trend_analysis": {
      "execution_time": {
        "direction": "stable",
        "change_percentage": 1.2,
        "significance": "low"
      },
      "memory_usage": {
        "direction": "increasing",
        "change_percentage": 8.7,
        "significance": "medium"
      },
      "error_rate": {
        "direction": "decreasing",
        "change_percentage": -15.3,
        "significance": "high"
      }
    }
  }
}
```

### Comparative Performance
```json
{
  "status": "success",
  "comparison": [
    {
      "function": "sma",
      "avg_execution_time_ms": 12.5,
      "throughput_ops_per_sec": 4250,
      "memory_efficiency_score": 8.7,
      "performance_tier": "fast"
    },
    {
      "function": "ema",
      "avg_execution_time_ms": 8.9,
      "throughput_ops_per_sec": 5680,
      "memory_efficiency_score": 9.2,
      "performance_tier": "fast"
    },
    {
      "function": "derivative",
      "avg_execution_time_ms": 28.7,
      "throughput_ops_per_sec": 1820,
      "memory_efficiency_score": 6.8,
      "performance_tier": "medium"
    }
  ],
  "ranking": {
    "fastest_execution": ["ema", "offset", "scale", "sma", "abs"],
    "highest_throughput": ["ema", "scale", "sma", "offset", "abs"],
    "most_memory_efficient": ["ema", "sma", "scale", "offset", "derivative"]
  }
}
```

### System-wide Performance Summary
```json
{
  "status": "success",
  "system_performance": {
    "overview": {
      "total_function_calls_24h": 89450,
      "unique_functions_used": 15,
      "overall_success_rate": 0.9912,
      "total_processing_time_hours": 20.7,
      "average_system_load": 2.3
    },
    "resource_utilization": {
      "cpu_usage": {
        "average_percent": 12.5,
        "peak_percent": 78.3,
        "functions_contribution_percent": 8.9
      },
      "memory_usage": {
        "average_mb": 156.8,
        "peak_mb": 892.4,
        "functions_contribution_mb": 89.2
      },
      "cache_statistics": {
        "hit_rate": 0.847,
        "miss_rate": 0.153,
        "cache_size_mb": 64.0,
        "evictions_24h": 234
      }
    },
    "performance_alerts": [
      {
        "severity": "warning",
        "message": "Derivative function showing 15% increase in execution time",
        "recommendation": "Monitor input data complexity"
      },
      {
        "severity": "info",
        "message": "SMA function performance stable within expected range",
        "recommendation": "No action needed"
      }
    ]
  }
}
```

## Usage Examples

### Get Overall Performance Statistics
```bash
# All function performance over last 24 hours
curl -s "http://localhost:8086/functions/performance" | jq

# Performance summary only
curl -s "http://localhost:8086/functions/performance" | jq '.summary'
```

### Get Specific Function Performance
```bash
# SMA function performance
curl -s "http://localhost:8086/functions/performance?function=sma" | jq

# Derivative function with 7-day trends
curl -s "http://localhost:8086/functions/performance?function=derivative&timeframe=7d" | jq
```

### Performance Analysis Queries
```bash
# Get slowest functions
curl -s "http://localhost:8086/functions/performance?sort=execution_time&limit=5" | jq '.functions[].name'

# Get functions with highest error rates
curl -s "http://localhost:8086/functions/performance?metric=error_rate&sort=error_rate" | jq

# Get memory usage statistics
curl -s "http://localhost:8086/functions/performance?metric=memory_usage" | jq
```

## Python Client Example

```python
import requests
import json
import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

class TSDBFunctionPerformanceClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_performance_stats(self, function: Optional[str] = None,
                             timeframe: str = "24h", metric: Optional[str] = None,
                             sort: str = "call_count", limit: int = 100) -> Dict[str, Any]:
        """Get function performance statistics."""
        params = {}
        if function:
            params['function'] = function
        if timeframe != "24h":
            params['timeframe'] = timeframe
        if metric:
            params['metric'] = metric
        if sort != "call_count":
            params['sort'] = sort
        if limit != 100:
            params['limit'] = limit
        
        response = requests.get(f"{self.base_url}/functions/performance", params=params)
        return response.json()
    
    def get_function_performance(self, function_name: str, timeframe: str = "24h") -> Dict[str, Any]:
        """Get performance statistics for a specific function."""
        result = self.get_performance_stats(function=function_name, timeframe=timeframe)
        
        if result.get("status") != "success":
            return {}
        
        if "functions" in result and result["functions"]:
            return result["functions"][0].get("statistics", {})
        elif "statistics" in result:
            return result["statistics"]
        
        return {}
    
    def get_slowest_functions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the slowest performing functions."""
        result = self.get_performance_stats(sort="execution_time", limit=limit)
        
        if result.get("status") != "success":
            return []
        
        functions = result.get("functions", [])
        slowest = []
        
        for func in functions:
            timing_metrics = func.get("statistics", {}).get("timing_metrics", {})
            slowest.append({
                "name": func.get("name", "unknown"),
                "avg_execution_time_ms": timing_metrics.get("avg_execution_time_ms", 0),
                "total_executions": func.get("statistics", {}).get("execution_metrics", {}).get("total_executions", 0)
            })
        
        return slowest
    
    def get_error_prone_functions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get functions with highest error rates."""
        result = self.get_performance_stats(sort="error_rate", limit=limit)
        
        if result.get("status") != "success":
            return []
        
        functions = result.get("functions", [])
        error_prone = []
        
        for func in functions:
            execution_metrics = func.get("statistics", {}).get("execution_metrics", {})
            error_prone.append({
                "name": func.get("name", "unknown"),
                "error_rate": execution_metrics.get("error_rate", 0),
                "failed_executions": execution_metrics.get("failed_executions", 0),
                "total_executions": execution_metrics.get("total_executions", 0)
            })
        
        return error_prone
    
    def get_resource_usage_summary(self) -> Dict[str, Any]:
        """Get summary of resource usage across all functions."""
        result = self.get_performance_stats()
        
        if result.get("status") != "success":
            return {}
        
        functions = result.get("functions", [])
        total_memory = 0
        total_cpu = 0
        total_executions = 0
        
        for func in functions:
            resource_metrics = func.get("statistics", {}).get("resource_metrics", {})
            execution_metrics = func.get("statistics", {}).get("execution_metrics", {})
            
            executions = execution_metrics.get("total_executions", 0)
            memory = resource_metrics.get("total_memory_allocated_mb", 0)
            cpu = resource_metrics.get("avg_cpu_usage_percent", 0) * executions
            
            total_executions += executions
            total_memory += memory
            total_cpu += cpu
        
        return {
            "total_memory_allocated_mb": total_memory,
            "average_cpu_usage_percent": total_cpu / total_executions if total_executions > 0 else 0,
            "total_function_executions": total_executions
        }
    
    def generate_performance_report(self, timeframe: str = "24h") -> str:
        """Generate a comprehensive performance report."""
        result = self.get_performance_stats(timeframe=timeframe)
        
        if result.get("status") != "success":
            return f"Failed to generate report: {result.get('error', 'Unknown error')}"
        
        summary = result.get("summary", {})
        
        report_lines = [
            f"# TSDB Function Performance Report ({timeframe})",
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Overview",
            f"- Total Function Executions: {summary.get('total_function_executions', 0):,}",
            f"- Overall Success Rate: {summary.get('overall_success_rate', 0):.2%}",
            f"- Average Execution Time: {summary.get('average_execution_time_ms', 0):.1f}ms",
            f"- Total Processing Time: {summary.get('total_execution_time_ms', 0) / 1000:.1f} seconds",
            "",
            "## Most Used Functions",
        ]
        
        most_used = summary.get("most_used_functions", [])
        for i, func_name in enumerate(most_used[:5], 1):
            report_lines.append(f"{i}. {func_name}")
        
        report_lines.extend([
            "",
            "## Slowest Functions",
        ])
        
        slowest = summary.get("slowest_functions", [])
        for i, func_name in enumerate(slowest[:5], 1):
            report_lines.append(f"{i}. {func_name}")
        
        # Add individual function details
        functions = result.get("functions", [])
        if functions:
            report_lines.extend([
                "",
                "## Function Details",
            ])
            
            for func in functions[:10]:  # Top 10 functions
                name = func.get("name", "unknown")
                stats = func.get("statistics", {})
                timing = stats.get("timing_metrics", {})
                execution = stats.get("execution_metrics", {})
                
                report_lines.extend([
                    f"### {name}",
                    f"- Executions: {execution.get('total_executions', 0):,}",
                    f"- Success Rate: {execution.get('success_rate', 0):.2%}",
                    f"- Avg Execution Time: {timing.get('avg_execution_time_ms', 0):.1f}ms",
                    f"- P95 Execution Time: {timing.get('p95_execution_time_ms', 0):.1f}ms",
                    ""
                ])
        
        return "\n".join(report_lines)
    
    def plot_performance_trends(self, function_name: str, timeframe: str = "7d"):
        """Plot performance trends for a function (requires matplotlib)."""
        result = self.get_performance_stats(function=function_name, timeframe=timeframe)
        
        if result.get("status") != "success" or "trends" not in result:
            print(f"No trend data available for {function_name}")
            return
        
        daily_stats = result["trends"]["daily_statistics"]
        
        dates = [datetime.strptime(stat["date"], "%Y-%m-%d") for stat in daily_stats]
        exec_times = [stat["avg_execution_time_ms"] for stat in daily_stats]
        success_rates = [stat["success_rate"] * 100 for stat in daily_stats]  # Convert to percentage
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        
        # Execution time trend
        ax1.plot(dates, exec_times, 'b-o', linewidth=2, markersize=4)
        ax1.set_ylabel('Avg Execution Time (ms)')
        ax1.set_title(f'{function_name} Performance Trends')
        ax1.grid(True, alpha=0.3)
        
        # Success rate trend
        ax2.plot(dates, success_rates, 'g-o', linewidth=2, markersize=4)
        ax2.set_ylabel('Success Rate (%)')
        ax2.set_xlabel('Date')
        ax2.grid(True, alpha=0.3)
        ax2.set_ylim([95, 100])  # Focus on the relevant range
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
    
    def monitor_performance_alerts(self) -> List[Dict[str, Any]]:
        """Check for performance alerts and anomalies."""
        result = self.get_performance_stats()
        
        if result.get("status") != "success":
            return []
        
        # Check for system-wide alerts if available
        system_performance = result.get("system_performance", {})
        alerts = system_performance.get("performance_alerts", [])
        
        # Generate custom alerts based on thresholds
        custom_alerts = []
        functions = result.get("functions", [])
        
        for func in functions:
            name = func.get("name", "unknown")
            stats = func.get("statistics", {})
            timing = stats.get("timing_metrics", {})
            execution = stats.get("execution_metrics", {})
            
            avg_time = timing.get("avg_execution_time_ms", 0)
            error_rate = execution.get("error_rate", 0)
            
            # Alert on slow execution times
            if avg_time > 100:  # More than 100ms average
                custom_alerts.append({
                    "severity": "warning",
                    "function": name,
                    "message": f"High execution time: {avg_time:.1f}ms average",
                    "metric": "execution_time",
                    "value": avg_time
                })
            
            # Alert on high error rates
            if error_rate > 0.01:  # More than 1% error rate
                custom_alerts.append({
                    "severity": "error",
                    "function": name,
                    "message": f"High error rate: {error_rate:.2%}",
                    "metric": "error_rate",
                    "value": error_rate
                })
        
        return alerts + custom_alerts

# Usage Examples
client = TSDBFunctionPerformanceClient()

# Get overall performance statistics
overall_stats = client.get_performance_stats()
print("Overall Function Performance:")
print(json.dumps(overall_stats.get("summary", {}), indent=2))

# Get performance for specific function
sma_performance = client.get_function_performance("sma")
if sma_performance:
    timing = sma_performance.get("timing_metrics", {})
    print(f"\nSMA Performance:")
    print(f"  Average execution time: {timing.get('avg_execution_time_ms', 0):.1f}ms")
    print(f"  P95 execution time: {timing.get('p95_execution_time_ms', 0):.1f}ms")
    print(f"  Total executions: {sma_performance.get('execution_metrics', {}).get('total_executions', 0):,}")

# Get slowest functions
slowest = client.get_slowest_functions(limit=5)
print(f"\nSlowest Functions:")
for i, func in enumerate(slowest, 1):
    print(f"  {i}. {func['name']}: {func['avg_execution_time_ms']:.1f}ms ({func['total_executions']:,} calls)")

# Get functions with errors
error_prone = client.get_error_prone_functions(limit=5)
print(f"\nFunctions with Errors:")
for func in error_prone:
    if func['error_rate'] > 0:
        print(f"  {func['name']}: {func['error_rate']:.2%} error rate ({func['failed_executions']} failures)")

# Get resource usage summary
resources = client.get_resource_usage_summary()
print(f"\nResource Usage Summary:")
print(f"  Total memory allocated: {resources.get('total_memory_allocated_mb', 0):.1f}MB")
print(f"  Average CPU usage: {resources.get('average_cpu_usage_percent', 0):.1f}%")
print(f"  Total function executions: {resources.get('total_function_executions', 0):,}")

# Generate comprehensive report
report = client.generate_performance_report("24h")
print(f"\n{report}")

# Check for performance alerts
alerts = client.monitor_performance_alerts()
if alerts:
    print(f"\nPerformance Alerts:")
    for alert in alerts:
        severity = alert.get("severity", "info").upper()
        message = alert.get("message", "No message")
        print(f"  [{severity}] {message}")
else:
    print(f"\n✓ No performance alerts")

# Plot trends (if matplotlib is available)
try:
    client.plot_performance_trends("sma", "7d")
except ImportError:
    print("Matplotlib not available for plotting")
except Exception as e:
    print(f"Could not plot trends: {e}")
```

## Performance Optimization

### Identifying Performance Bottlenecks
```python
def analyze_performance_bottlenecks(client):
    """Identify and analyze performance bottlenecks."""
    stats = client.get_performance_stats()
    
    if stats.get("status") != "success":
        return
    
    functions = stats.get("functions", [])
    
    print("Performance Bottleneck Analysis:")
    print("=" * 50)
    
    # Analyze execution time distribution
    slow_functions = []
    for func in functions:
        name = func.get("name", "unknown")
        timing = func.get("statistics", {}).get("timing_metrics", {})
        execution = func.get("statistics", {}).get("execution_metrics", {})
        
        avg_time = timing.get("avg_execution_time_ms", 0)
        p95_time = timing.get("p95_execution_time_ms", 0)
        total_executions = execution.get("total_executions", 0)
        
        # Functions with high average or high variability
        if avg_time > 50 or (p95_time / avg_time > 3 if avg_time > 0 else False):
            slow_functions.append({
                "name": name,
                "avg_time": avg_time,
                "p95_time": p95_time,
                "variability": p95_time / avg_time if avg_time > 0 else 0,
                "total_executions": total_executions
            })
    
    # Sort by impact (execution time * frequency)
    slow_functions.sort(key=lambda x: x["avg_time"] * x["total_executions"], reverse=True)
    
    print("High-Impact Slow Functions:")
    for func in slow_functions[:5]:
        impact_score = func["avg_time"] * func["total_executions"]
        print(f"  {func['name']}:")
        print(f"    Average time: {func['avg_time']:.1f}ms")
        print(f"    P95 time: {func['p95_time']:.1f}ms")
        print(f"    Variability: {func['variability']:.1f}x")
        print(f"    Impact score: {impact_score:.0f}")
        print()

# Usage
analyze_performance_bottlenecks(client)
```

### Performance Monitoring Dashboard
```python
def create_performance_dashboard(client):
    """Create a simple performance monitoring dashboard."""
    import time
    
    while True:
        # Clear screen (works on most terminals)
        print("\033[2J\033[H")
        
        print("TSDB Function Performance Dashboard")
        print("=" * 60)
        print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Get current statistics
        stats = client.get_performance_stats()
        
        if stats.get("status") == "success":
            summary = stats.get("summary", {})
            
            print("System Overview:")
            print(f"  Total function calls (24h): {summary.get('total_function_executions', 0):,}")
            print(f"  Overall success rate: {summary.get('overall_success_rate', 0):.2%}")
            print(f"  Average execution time: {summary.get('average_execution_time_ms', 0):.1f}ms")
            print()
            
            # Top functions
            most_used = summary.get('most_used_functions', [])[:5]
            print("Most Used Functions:")
            for i, func_name in enumerate(most_used, 1):
                print(f"  {i}. {func_name}")
            print()
            
            # Slowest functions
            slowest = summary.get('slowest_functions', [])[:5]
            print("Slowest Functions:")
            for i, func_name in enumerate(slowest, 1):
                print(f"  {i}. {func_name}")
            print()
            
            # Check for alerts
            alerts = client.monitor_performance_alerts()
            if alerts:
                print("⚠ Performance Alerts:")
                for alert in alerts[:3]:  # Show top 3 alerts
                    print(f"  [{alert.get('severity', 'info').upper()}] {alert.get('message', 'No message')}")
            else:
                print("✓ No performance alerts")
        else:
            print("✗ Unable to fetch performance data")
        
        print()
        print("Press Ctrl+C to exit")
        
        try:
            time.sleep(30)  # Update every 30 seconds
        except KeyboardInterrupt:
            print("\nDashboard stopped.")
            break

# Usage (run in interactive environment)
# create_performance_dashboard(client)
```

## Error Handling

### Common Error Scenarios
- **Function Not Found**: Specified function doesn't exist
- **Invalid Timeframe**: Unsupported time period format
- **No Data Available**: No performance data for specified period
- **Invalid Metric**: Unsupported performance metric

### Robust Client Implementation
```python
def get_performance_safely(client, function_name=None):
    """Get performance data with comprehensive error handling."""
    try:
        result = client.get_performance_stats(function=function_name)
        
        if result.get("status") != "success":
            print(f"Failed to get performance data: {result.get('error', 'Unknown error')}")
            return None
        
        return result
        
    except requests.exceptions.ConnectionError:
        print("Failed to connect to TSDB server")
        return None
    except requests.exceptions.Timeout:
        print("Performance request timed out")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
```

## Related Endpoints

- [`GET /functions`](functions-endpoint.md) - List functions to analyze performance
- [`GET /functions/{name}`](functions-name-endpoint.md) - Get function details for performance context
- [`GET /functions/cache`](functions-cache-endpoint.md) - Get cache performance statistics
- [`POST /functions/validate`](functions-validate-endpoint.md) - Validate before execution for performance
- [`POST /query/functions`](query-functions-endpoint.md) - Execute function queries with performance tracking