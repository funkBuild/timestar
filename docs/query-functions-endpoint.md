# POST /query/functions - Execute Function Queries on Time Series Data

## Overview
The function query endpoint executes time series analysis functions on stored data, providing advanced analytical capabilities including statistical functions, mathematical transformations, and complex data processing pipelines. This endpoint is optimized for function-specific operations and provides detailed execution metadata.

## Endpoint Details
- **URL**: `/query/functions`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Function Query
```json
{
  "function": "sma",
  "measurement": "temperature",
  "field": "value",
  "tags": {
    "location": "datacenter",
    "sensor": "outdoor"
  },
  "parameters": {
    "window": 10
  },
  "startTime": 1704067200000000000,
  "endTime": 1706745599000000000
}
```

### Function Pipeline Query
```json
{
  "pipeline": [
    {
      "function": "sma",
      "parameters": {
        "window": 5
      }
    },
    {
      "function": "derivative", 
      "parameters": {
        "method": "central"
      }
    }
  ],
  "measurement": "cpu",
  "field": "usage_percent",
  "tags": {
    "host": "server-01"
  },
  "startTime": 1704067200000000000,
  "endTime": 1706745599000000000
}
```

### Batch Function Queries
```json
{
  "queries": [
    {
      "function": "sma",
      "measurement": "temperature",
      "field": "value",
      "parameters": {"window": 5},
      "tags": {"location": "datacenter-1"}
    },
    {
      "function": "scale",
      "measurement": "temperature", 
      "field": "value",
      "parameters": {"factor": 1.8},
      "tags": {"location": "datacenter-2"}
    }
  ],
  "startTime": 1704067200000000000,
  "endTime": 1706745599000000000
}
```

### Advanced Function Query
```json
{
  "function": "derivative",
  "measurement": "network",
  "field": "bytes_sent",
  "tags": {
    "interface": "eth0",
    "host": "gateway-01"
  },
  "parameters": {
    "method": "central",
    "time_unit": "seconds"
  },
  "startTime": 1704067200000000000,
  "endTime": 1706745599000000000,
  "options": {
    "include_metadata": true,
    "cache_result": true,
    "max_points": 10000,
    "interpolate_missing": true
  }
}
```

## Request Fields

### Single Function Query
- **function**: Function name to execute (required)
- **measurement**: Measurement name (required)
- **field**: Field name (required)
- **tags**: Tag filters (optional)
- **parameters**: Function parameters (required for most functions)
- **startTime**: Start time in nanoseconds (required)
- **endTime**: End time in nanoseconds (required)
- **options**: Query execution options (optional)

### Pipeline Query
- **pipeline**: Array of functions to execute in sequence (required)
- **measurement**: Measurement name (required)
- **field**: Field name (required)
- **tags**: Tag filters (optional)
- **startTime**: Start time in nanoseconds (required)
- **endTime**: End time in nanoseconds (required)

### Batch Query
- **queries**: Array of function queries (required)
- **startTime**: Global start time (optional, can be per-query)
- **endTime**: Global end time (optional, can be per-query)

## Response Format

### Successful Function Query
```json
{
  "status": "success",
  "result": {
    "function": "sma",
    "measurement": "temperature",
    "field": "value",
    "tags": {
      "location": "datacenter",
      "sensor": "outdoor"
    },
    "parameters": {
      "window": 10
    },
    "data": {
      "timestamps": [
        1704067500000000000,
        1704067800000000000,
        1704068100000000000
      ],
      "values": [
        23.45,
        23.67,
        23.52
      ]
    },
    "input_stats": {
      "points_read": 1250,
      "time_range_ms": 3600000,
      "data_gaps": 2,
      "input_data_type": "float"
    },
    "output_stats": {
      "points_generated": 1240,
      "data_reduction_ratio": 0.992,
      "output_data_type": "float",
      "value_range": {
        "min": 22.1,
        "max": 24.8
      }
    }
  },
  "execution_metadata": {
    "execution_time_ms": 12.3,
    "cache_used": true,
    "cache_hit": false,
    "memory_usage_mb": 2.1,
    "data_source": "tsm_files",
    "function_version": "1.2.0"
  },
  "performance_stats": {
    "data_retrieval_ms": 8.1,
    "function_execution_ms": 4.2,
    "total_execution_ms": 12.3,
    "cpu_usage_percent": 1.2,
    "memory_peak_mb": 2.1
  }
}
```

### Function Pipeline Result
```json
{
  "status": "success",
  "result": {
    "pipeline": [
      {
        "function": "sma",
        "parameters": {"window": 5},
        "execution_time_ms": 6.7,
        "output_points": 1245
      },
      {
        "function": "derivative",
        "parameters": {"method": "central"},
        "execution_time_ms": 8.9,
        "output_points": 1243
      }
    ],
    "measurement": "cpu",
    "field": "usage_percent",
    "tags": {
      "host": "server-01"
    },
    "data": {
      "timestamps": [
        1704067500000000000,
        1704067800000000000,
        1704068100000000000
      ],
      "values": [
        0.05,
        -0.02,
        0.08
      ]
    },
    "pipeline_stats": {
      "total_stages": 2,
      "input_points": 1250,
      "output_points": 1243,
      "total_execution_time_ms": 15.6,
      "data_flow": [
        {"stage": 0, "input": 1250, "output": 1245},
        {"stage": 1, "input": 1245, "output": 1243}
      ]
    }
  },
  "execution_metadata": {
    "execution_time_ms": 15.6,
    "cache_used": true,
    "memory_usage_mb": 3.4,
    "parallel_execution": false
  }
}
```

### Batch Query Results
```json
{
  "status": "success",
  "results": [
    {
      "query_index": 0,
      "status": "success",
      "function": "sma",
      "data": {
        "timestamps": [1704067500000000000, 1704067800000000000],
        "values": [23.45, 23.67]
      },
      "execution_time_ms": 8.2
    },
    {
      "query_index": 1,
      "status": "success", 
      "function": "scale",
      "data": {
        "timestamps": [1704067500000000000, 1704067800000000000],
        "values": [42.21, 42.61]
      },
      "execution_time_ms": 3.1
    }
  ],
  "batch_metadata": {
    "total_queries": 2,
    "successful_queries": 2,
    "failed_queries": 0,
    "total_execution_time_ms": 11.3,
    "parallel_execution": true,
    "cache_hits": 1,
    "cache_misses": 1
  }
}
```

### Function Query Error
```json
{
  "status": "error",
  "error": {
    "code": "FUNCTION_EXECUTION_ERROR",
    "message": "Insufficient data points for SMA calculation with window=10",
    "details": {
      "function": "sma",
      "measurement": "temperature",
      "field": "value",
      "data_points_found": 3,
      "minimum_required": 10,
      "suggestion": "Reduce window size or expand time range"
    },
    "execution_context": {
      "execution_time_ms": 5.2,
      "data_retrieval_successful": true,
      "function_parameters": {
        "window": 10
      }
    }
  }
}
```

### Advanced Analytics Result
```json
{
  "status": "success",
  "result": {
    "function": "statistical_analysis",
    "measurement": "system_metrics",
    "field": "cpu_usage",
    "analytics": {
      "basic_stats": {
        "count": 1440,
        "mean": 45.67,
        "median": 44.2,
        "std_dev": 12.34,
        "min": 18.5,
        "max": 89.2
      },
      "trend_analysis": {
        "trend_direction": "increasing",
        "trend_strength": 0.68,
        "seasonal_components": {
          "hourly_pattern": true,
          "daily_pattern": false
        }
      },
      "anomaly_detection": {
        "anomalies_found": 7,
        "anomaly_timestamps": [
          1704067500000000000,
          1704071100000000000
        ],
        "anomaly_scores": [2.1, 2.8]
      },
      "forecasting": {
        "next_24h_prediction": {
          "timestamps": [1704153600000000000, 1704157200000000000],
          "predicted_values": [47.2, 48.1],
          "confidence_intervals": [
            {"lower": 42.1, "upper": 52.3},
            {"lower": 43.0, "upper": 53.2}
          ]
        }
      }
    }
  },
  "execution_metadata": {
    "execution_time_ms": 156.7,
    "cache_used": false,
    "memory_usage_mb": 15.6,
    "complex_analysis": true
  }
}
```

## Usage Examples

### Execute Simple Function Query
```bash
curl -X POST http://localhost:8086/query/functions \
  -H "Content-Type: application/json" \
  -d '{
    "function": "sma",
    "measurement": "temperature",
    "field": "value",
    "parameters": {"window": 10},
    "tags": {"location": "datacenter"},
    "startTime": 1704067200000000000,
    "endTime": 1706745599000000000
  }'
```

### Execute Function Pipeline
```bash
curl -X POST http://localhost:8086/query/functions \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline": [
      {
        "function": "sma",
        "parameters": {"window": 5}
      },
      {
        "function": "derivative",
        "parameters": {"method": "central"}
      }
    ],
    "measurement": "cpu",
    "field": "usage_percent",
    "tags": {"host": "server-01"},
    "startTime": 1704067200000000000,
    "endTime": 1706745599000000000
  }'
```

### Execute Batch Queries
```bash
curl -X POST http://localhost:8086/query/functions \
  -H "Content-Type: application/json" \
  -d '{
    "queries": [
      {
        "function": "sma",
        "measurement": "temperature",
        "field": "value", 
        "parameters": {"window": 5},
        "tags": {"location": "datacenter-1"}
      },
      {
        "function": "scale",
        "measurement": "temperature",
        "field": "value",
        "parameters": {"factor": 1.8},
        "tags": {"location": "datacenter-2"}
      }
    ],
    "startTime": 1704067200000000000,
    "endTime": 1706745599000000000
  }'
```

### Execute with Advanced Options
```bash
curl -X POST http://localhost:8086/query/functions \
  -H "Content-Type: application/json" \
  -d '{
    "function": "derivative",
    "measurement": "network",
    "field": "bytes_sent",
    "parameters": {
      "method": "central",
      "time_unit": "seconds"
    },
    "tags": {"interface": "eth0"},
    "startTime": 1704067200000000000,
    "endTime": 1706745599000000000,
    "options": {
      "include_metadata": true,
      "cache_result": true,
      "max_points": 10000
    }
  }'
```

## Python Client Example

```python
import requests
import json
import time
import numpy as np
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone

class TSDBFunctionQueryClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def execute_function(self, function: str, measurement: str, field: str,
                        parameters: Dict[str, Any], tags: Optional[Dict[str, str]] = None,
                        start_time: Union[int, datetime] = None, 
                        end_time: Union[int, datetime] = None,
                        options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a single function query."""
        data = {
            "function": function,
            "measurement": measurement,
            "field": field,
            "parameters": parameters
        }
        
        if tags:
            data["tags"] = tags
        if start_time:
            data["startTime"] = self._to_nanoseconds(start_time)
        if end_time:
            data["endTime"] = self._to_nanoseconds(end_time)
        if options:
            data["options"] = options
        
        response = requests.post(f"{self.base_url}/query/functions", json=data)
        return response.json()
    
    def execute_pipeline(self, pipeline: List[Dict[str, Any]], measurement: str, field: str,
                        tags: Optional[Dict[str, str]] = None,
                        start_time: Union[int, datetime] = None,
                        end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Execute a function pipeline."""
        data = {
            "pipeline": pipeline,
            "measurement": measurement,
            "field": field
        }
        
        if tags:
            data["tags"] = tags
        if start_time:
            data["startTime"] = self._to_nanoseconds(start_time)
        if end_time:
            data["endTime"] = self._to_nanoseconds(end_time)
        
        response = requests.post(f"{self.base_url}/query/functions", json=data)
        return response.json()
    
    def execute_batch(self, queries: List[Dict[str, Any]], 
                     global_start_time: Union[int, datetime] = None,
                     global_end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Execute multiple function queries in batch."""
        data = {"queries": queries}
        
        if global_start_time:
            data["startTime"] = self._to_nanoseconds(global_start_time)
        if global_end_time:
            data["endTime"] = self._to_nanoseconds(global_end_time)
        
        response = requests.post(f"{self.base_url}/query/functions", json=data)
        return response.json()
    
    def _to_nanoseconds(self, timestamp: Union[int, datetime]) -> int:
        """Convert timestamp to nanoseconds."""
        if isinstance(timestamp, datetime):
            return int(timestamp.timestamp() * 1_000_000_000)
        elif isinstance(timestamp, (int, float)):
            # Auto-detect precision and convert to nanoseconds
            if timestamp < 4102444800000:  # Year 2100 in milliseconds
                return int(timestamp * 1_000_000_000)
            elif timestamp < 4102444800000000:  # Year 2100 in microseconds
                return int(timestamp * 1_000_000)
            elif timestamp < 4102444800000000000:  # Year 2100 in nanoseconds
                return int(timestamp * 1_000)
            else:
                return int(timestamp)
        return int(timestamp)
    
    def get_function_data_as_dataframe(self, result: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Convert function query result to pandas DataFrame."""
        if result.get("status") != "success":
            return None
        
        result_data = result.get("result", {})
        data = result_data.get("data", {})
        
        timestamps = data.get("timestamps", [])
        values = data.get("values", [])
        
        if not timestamps or not values:
            return None
        
        # Convert nanoseconds to datetime
        dt_timestamps = [datetime.fromtimestamp(ts / 1e9, tz=timezone.utc) for ts in timestamps]
        
        df = pd.DataFrame({
            'timestamp': dt_timestamps,
            'value': values
        })
        
        # Add metadata as columns
        function_name = result_data.get("function", "unknown")
        measurement = result_data.get("measurement", "unknown")
        field = result_data.get("field", "unknown")
        
        df['function'] = function_name
        df['measurement'] = measurement
        df['field'] = field
        
        return df
    
    def calculate_moving_average(self, measurement: str, field: str, window: int,
                               tags: Optional[Dict[str, str]] = None,
                               start_time: Union[int, datetime] = None,
                               end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Calculate simple moving average."""
        return self.execute_function(
            function="sma",
            measurement=measurement,
            field=field,
            parameters={"window": window},
            tags=tags,
            start_time=start_time,
            end_time=end_time
        )
    
    def calculate_derivative(self, measurement: str, field: str,
                           method: str = "forward", time_unit: str = "seconds",
                           tags: Optional[Dict[str, str]] = None,
                           start_time: Union[int, datetime] = None,
                           end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Calculate derivative (rate of change)."""
        return self.execute_function(
            function="derivative",
            measurement=measurement,
            field=field,
            parameters={"method": method, "time_unit": time_unit},
            tags=tags,
            start_time=start_time,
            end_time=end_time
        )
    
    def scale_values(self, measurement: str, field: str, factor: float,
                    tags: Optional[Dict[str, str]] = None,
                    start_time: Union[int, datetime] = None,
                    end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Scale values by a multiplication factor."""
        return self.execute_function(
            function="scale",
            measurement=measurement,
            field=field,
            parameters={"factor": factor},
            tags=tags,
            start_time=start_time,
            end_time=end_time
        )
    
    def smooth_and_differentiate(self, measurement: str, field: str, window: int,
                                tags: Optional[Dict[str, str]] = None,
                                start_time: Union[int, datetime] = None,
                                end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Apply smoothing followed by differentiation."""
        pipeline = [
            {
                "function": "sma",
                "parameters": {"window": window}
            },
            {
                "function": "derivative", 
                "parameters": {"method": "central"}
            }
        ]
        
        return self.execute_pipeline(
            pipeline=pipeline,
            measurement=measurement,
            field=field,
            tags=tags,
            start_time=start_time,
            end_time=end_time
        )
    
    def compare_functions(self, measurement: str, field: str,
                         functions: List[Dict[str, Any]],
                         tags: Optional[Dict[str, str]] = None,
                         start_time: Union[int, datetime] = None,
                         end_time: Union[int, datetime] = None) -> Dict[str, Any]:
        """Compare results from multiple functions on the same data."""
        queries = []
        
        for func_config in functions:
            query = {
                "function": func_config["function"],
                "measurement": measurement,
                "field": field,
                "parameters": func_config.get("parameters", {})
            }
            
            if tags:
                query["tags"] = tags
            
            queries.append(query)
        
        return self.execute_batch(queries, start_time, end_time)
    
    def get_execution_statistics(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Extract execution statistics from query result."""
        if result.get("status") != "success":
            return {"error": "Query failed"}
        
        execution_metadata = result.get("execution_metadata", {})
        performance_stats = result.get("performance_stats", {})
        result_data = result.get("result", {})
        
        stats = {
            "execution_time_ms": execution_metadata.get("execution_time_ms", 0),
            "memory_usage_mb": execution_metadata.get("memory_usage_mb", 0),
            "cache_used": execution_metadata.get("cache_used", False),
            "cache_hit": execution_metadata.get("cache_hit", False),
            "function": result_data.get("function", "unknown")
        }
        
        # Add input/output statistics if available
        input_stats = result_data.get("input_stats", {})
        output_stats = result_data.get("output_stats", {})
        
        if input_stats:
            stats["input_points"] = input_stats.get("points_read", 0)
            stats["data_gaps"] = input_stats.get("data_gaps", 0)
        
        if output_stats:
            stats["output_points"] = output_stats.get("points_generated", 0)
            stats["data_reduction_ratio"] = output_stats.get("data_reduction_ratio", 1.0)
        
        return stats
    
    def analyze_performance(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance across multiple query results."""
        if not results:
            return {"error": "No results to analyze"}
        
        performance_data = []
        
        for result in results:
            stats = self.get_execution_statistics(result)
            if "error" not in stats:
                performance_data.append(stats)
        
        if not performance_data:
            return {"error": "No valid results to analyze"}
        
        # Calculate summary statistics
        execution_times = [stats["execution_time_ms"] for stats in performance_data]
        memory_usages = [stats["memory_usage_mb"] for stats in performance_data]
        cache_hits = sum(1 for stats in performance_data if stats["cache_hit"])
        
        return {
            "query_count": len(performance_data),
            "avg_execution_time_ms": np.mean(execution_times),
            "max_execution_time_ms": np.max(execution_times),
            "min_execution_time_ms": np.min(execution_times),
            "avg_memory_usage_mb": np.mean(memory_usages),
            "max_memory_usage_mb": np.max(memory_usages),
            "cache_hit_rate": cache_hits / len(performance_data),
            "total_execution_time_ms": sum(execution_times)
        }

# Usage Examples
client = TSDBFunctionQueryClient()

# Execute simple moving average
start_time = datetime.now() - pd.Timedelta(hours=1)
end_time = datetime.now()

sma_result = client.calculate_moving_average(
    measurement="temperature",
    field="value",
    window=10,
    tags={"location": "datacenter"},
    start_time=start_time,
    end_time=end_time
)

print("SMA Query Result:")
if sma_result.get("status") == "success":
    result_data = sma_result.get("result", {})
    data = result_data.get("data", {})
    print(f"  Function: {result_data.get('function', 'unknown')}")
    print(f"  Data points: {len(data.get('values', []))}")
    
    # Get execution statistics
    stats = client.get_execution_statistics(sma_result)
    print(f"  Execution time: {stats.get('execution_time_ms', 0):.1f}ms")
    print(f"  Memory usage: {stats.get('memory_usage_mb', 0):.1f}MB")
    print(f"  Cache hit: {stats.get('cache_hit', False)}")
else:
    error_info = sma_result.get("error", {})
    print(f"  Error: {error_info.get('message', 'Unknown error')}")

# Execute function pipeline
pipeline_result = client.smooth_and_differentiate(
    measurement="cpu",
    field="usage_percent",
    window=5,
    tags={"host": "server-01"},
    start_time=start_time,
    end_time=end_time
)

print(f"\nPipeline Query Result:")
if pipeline_result.get("status") == "success":
    result_data = pipeline_result.get("result", {})
    pipeline_stats = result_data.get("pipeline_stats", {})
    print(f"  Pipeline stages: {pipeline_stats.get('total_stages', 0)}")
    print(f"  Input points: {pipeline_stats.get('input_points', 0)}")
    print(f"  Output points: {pipeline_stats.get('output_points', 0)}")
    print(f"  Total execution time: {pipeline_stats.get('total_execution_time_ms', 0):.1f}ms")

# Compare multiple functions
function_configs = [
    {"function": "sma", "parameters": {"window": 5}},
    {"function": "sma", "parameters": {"window": 10}},
    {"function": "sma", "parameters": {"window": 20}}
]

comparison_result = client.compare_functions(
    measurement="temperature",
    field="value",
    functions=function_configs,
    tags={"location": "datacenter"},
    start_time=start_time,
    end_time=end_time
)

print(f"\nFunction Comparison Result:")
if comparison_result.get("status") == "success":
    results = comparison_result.get("results", [])
    batch_metadata = comparison_result.get("batch_metadata", {})
    
    print(f"  Total queries: {batch_metadata.get('total_queries', 0)}")
    print(f"  Successful: {batch_metadata.get('successful_queries', 0)}")
    print(f"  Total execution time: {batch_metadata.get('total_execution_time_ms', 0):.1f}ms")
    print(f"  Parallel execution: {batch_metadata.get('parallel_execution', False)}")
    
    # Analyze performance across all results
    performance_analysis = client.analyze_performance([comparison_result])
    print(f"  Average execution time: {performance_analysis.get('avg_execution_time_ms', 0):.1f}ms")

# Convert result to DataFrame for analysis
if sma_result.get("status") == "success":
    df = client.get_function_data_as_dataframe(sma_result)
    if df is not None:
        print(f"\nDataFrame created with {len(df)} rows:")
        print(df.head())
        
        # Basic analysis
        print(f"\nBasic Statistics:")
        print(f"  Mean: {df['value'].mean():.2f}")
        print(f"  Std: {df['value'].std():.2f}")
        print(f"  Min: {df['value'].min():.2f}")
        print(f"  Max: {df['value'].max():.2f}")

# Execute derivative calculation
derivative_result = client.calculate_derivative(
    measurement="network",
    field="bytes_sent",
    method="central",
    time_unit="seconds",
    tags={"interface": "eth0"},
    start_time=start_time,
    end_time=end_time
)

print(f"\nDerivative Query Result:")
if derivative_result.get("status") == "success":
    result_data = derivative_result.get("result", {})
    data = result_data.get("data", {})
    print(f"  Data points: {len(data.get('values', []))}")
    
    # Show some sample values
    values = data.get("values", [])
    if values:
        print(f"  Sample values: {values[:5]}")
        print(f"  Value range: {min(values):.3f} to {max(values):.3f}")

# Scale temperature from Celsius to Fahrenheit
scale_result = client.scale_values(
    measurement="temperature", 
    field="celsius",
    factor=1.8,  # C to F conversion factor (first step)
    tags={"sensor": "outdoor"},
    start_time=start_time,
    end_time=end_time
)

print(f"\nScale Query Result:")
if scale_result.get("status") == "success":
    result_data = scale_result.get("result", {})
    stats = client.get_execution_statistics(scale_result)
    print(f"  Execution time: {stats.get('execution_time_ms', 0):.1f}ms")
    print(f"  Data reduction ratio: {stats.get('data_reduction_ratio', 1.0):.3f}")
```

## Advanced Analytics Integration

### Statistical Analysis Pipeline
```python
def perform_statistical_analysis(client, measurement, field, tags=None):
    """Perform comprehensive statistical analysis using function pipeline."""
    
    # Define analysis pipeline
    analyses = [
        # Basic smoothing
        {
            "name": "smoothed_data",
            "function": "sma",
            "parameters": {"window": 10}
        },
        
        # Trend analysis
        {
            "name": "trend",
            "function": "derivative",
            "parameters": {"method": "central", "time_unit": "minutes"}
        },
        
        # Volatility analysis
        {
            "name": "volatility", 
            "function": "stddev",
            "parameters": {"window": 20}
        }
    ]
    
    results = {}
    
    for analysis in analyses:
        result = client.execute_function(
            function=analysis["function"],
            measurement=measurement,
            field=field,
            parameters=analysis["parameters"],
            tags=tags,
            start_time=datetime.now() - pd.Timedelta(hours=24),
            end_time=datetime.now()
        )
        
        if result.get("status") == "success":
            results[analysis["name"]] = result
            
            # Convert to DataFrame for analysis
            df = client.get_function_data_as_dataframe(result)
            if df is not None:
                print(f"{analysis['name']} - Points: {len(df)}, "
                      f"Mean: {df['value'].mean():.3f}, "
                      f"Std: {df['value'].std():.3f}")
    
    return results

# Usage
statistical_results = perform_statistical_analysis(
    client, "cpu", "usage_percent", {"host": "server-01"}
)
```

### Real-time Monitoring Dashboard
```python
def create_function_monitoring_dashboard(client):
    """Create a real-time monitoring dashboard using function queries."""
    
    while True:
        print("\033[2J\033[H")  # Clear screen
        print("TSDB Function Query Dashboard")
        print("=" * 50)
        print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Define monitoring queries
        monitoring_queries = [
            {
                "name": "CPU Average (5min)",
                "function": "sma",
                "measurement": "cpu",
                "field": "usage_percent",
                "parameters": {"window": 5}
            },
            {
                "name": "Memory Trend",
                "function": "derivative",
                "measurement": "memory",
                "field": "used_percent", 
                "parameters": {"method": "forward", "time_unit": "minutes"}
            },
            {
                "name": "Network Rate",
                "function": "derivative",
                "measurement": "network",
                "field": "bytes_sent",
                "parameters": {"method": "central", "time_unit": "seconds"}
            }
        ]
        
        # Execute monitoring queries
        current_time = datetime.now()
        start_time = current_time - pd.Timedelta(minutes=30)
        
        for query_config in monitoring_queries:
            result = client.execute_function(
                function=query_config["function"],
                measurement=query_config["measurement"],
                field=query_config["field"],
                parameters=query_config["parameters"],
                start_time=start_time,
                end_time=current_time
            )
            
            print(f"{query_config['name']}:")
            if result.get("status") == "success":
                result_data = result.get("result", {})
                data = result_data.get("data", {})
                values = data.get("values", [])
                
                if values:
                    latest_value = values[-1]
                    avg_value = np.mean(values)
                    print(f"  Current: {latest_value:.2f}")
                    print(f"  Average: {avg_value:.2f}")
                    print(f"  Points: {len(values)}")
                else:
                    print("  No data available")
            else:
                error = result.get("error", {})
                print(f"  Error: {error.get('message', 'Unknown error')}")
            
            print()
        
        try:
            time.sleep(10)  # Update every 10 seconds
        except KeyboardInterrupt:
            print("Dashboard stopped.")
            break

# Usage (run in interactive environment)
# create_function_monitoring_dashboard(client)
```

### Performance Benchmarking
```python
def benchmark_functions(client, measurement, field, tags=None):
    """Benchmark different functions for performance comparison."""
    
    benchmark_configs = [
        {"function": "sma", "parameters": {"window": 5}},
        {"function": "sma", "parameters": {"window": 10}},
        {"function": "sma", "parameters": {"window": 20}},
        {"function": "ema", "parameters": {"alpha": 0.3}},
        {"function": "derivative", "parameters": {"method": "forward"}},
        {"function": "derivative", "parameters": {"method": "central"}},
        {"function": "scale", "parameters": {"factor": 1.0}}
    ]
    
    benchmark_results = []
    
    start_time = datetime.now() - pd.Timedelta(hours=1)
    end_time = datetime.now()
    
    print("Function Performance Benchmark")
    print("=" * 40)
    
    for config in benchmark_configs:
        # Run multiple iterations for accuracy
        execution_times = []
        
        for iteration in range(3):
            result = client.execute_function(
                function=config["function"],
                measurement=measurement,
                field=field,
                parameters=config["parameters"],
                tags=tags,
                start_time=start_time,
                end_time=end_time
            )
            
            if result.get("status") == "success":
                execution_metadata = result.get("execution_metadata", {})
                exec_time = execution_metadata.get("execution_time_ms", 0)
                execution_times.append(exec_time)
        
        if execution_times:
            avg_time = np.mean(execution_times)
            benchmark_results.append({
                "function": config["function"],
                "parameters": config["parameters"],
                "avg_execution_time_ms": avg_time,
                "min_time_ms": min(execution_times),
                "max_time_ms": max(execution_times)
            })
            
            param_str = ", ".join([f"{k}={v}" for k, v in config["parameters"].items()])
            print(f"{config['function']}({param_str}): {avg_time:.1f}ms avg")
    
    # Sort by performance
    benchmark_results.sort(key=lambda x: x["avg_execution_time_ms"])
    
    print("\nRanked by Performance:")
    for i, result in enumerate(benchmark_results, 1):
        func_name = result["function"]
        param_str = ", ".join([f"{k}={v}" for k, v in result["parameters"].items()])
        avg_time = result["avg_execution_time_ms"]
        print(f"  {i}. {func_name}({param_str}) - {avg_time:.1f}ms")
    
    return benchmark_results

# Usage
benchmark_results = benchmark_functions(
    client, "temperature", "value", {"location": "datacenter"}
)
```

## Error Handling

### Common Error Scenarios
- **Function Execution Errors**: Insufficient data, invalid parameters, numerical errors
- **Data Access Errors**: Missing measurements/fields, permission issues
- **Resource Limits**: Memory exhaustion, timeout errors, query complexity limits
- **Pipeline Errors**: Type mismatches between pipeline stages, function incompatibilities

### Robust Error Handling
```python
def execute_function_safely(client, function, measurement, field, parameters, **kwargs):
    """Execute function query with comprehensive error handling."""
    try:
        result = client.execute_function(
            function=function,
            measurement=measurement,
            field=field,
            parameters=parameters,
            **kwargs
        )
        
        if result.get("status") == "success":
            print(f"✓ {function} executed successfully")
            
            # Get execution statistics
            stats = client.get_execution_statistics(result)
            print(f"  Execution time: {stats.get('execution_time_ms', 0):.1f}ms")
            print(f"  Output points: {stats.get('output_points', 0)}")
            
            return result
        else:
            error_info = result.get("error", {})
            error_code = error_info.get("code", "UNKNOWN_ERROR")
            error_message = error_info.get("message", "Unknown error occurred")
            
            print(f"✗ Function execution failed: {error_code}")
            print(f"  Message: {error_message}")
            
            # Show suggestions if available
            details = error_info.get("details", {})
            suggestion = details.get("suggestion", "")
            if suggestion:
                print(f"  Suggestion: {suggestion}")
            
            return result
            
    except requests.exceptions.ConnectionError:
        print("✗ Failed to connect to TSDB server")
        return {"status": "error", "error": "Connection failed"}
    except requests.exceptions.Timeout:
        print("✗ Function query timed out")
        return {"status": "error", "error": "Request timeout"}
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return {"status": "error", "error": str(e)}

# Usage
result = execute_function_safely(
    client, "sma", "temperature", "value", {"window": 10},
    tags={"location": "datacenter"}
)
```

## Related Endpoints

- [`POST /query/parse`](query-parse-endpoint.md) - Parse and validate function queries before execution
- [`POST /functions/validate`](functions-validate-endpoint.md) - Validate function parameters
- [`GET /functions/{name}`](functions-name-endpoint.md) - Get detailed function information
- [`GET /functions/performance`](functions-performance-endpoint.md) - Monitor function execution performance
- [`GET /functions/cache`](functions-cache-endpoint.md) - Manage function result caching
- [`POST /query`](query-endpoint.md) - Execute general queries with function support