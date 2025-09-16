# POST /write - Write Time Series Data

## Overview
The write endpoint allows you to store time series data in the TSDB. It supports both single point writes and batch operations, with automatic type detection for field values and optional timestamp generation.

## Endpoint Details
- **URL**: `/write`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Formats

### Single Point Write
```json
{
  "measurement": "temperature",
  "tags": {
    "location": "us-midwest",
    "host": "server-01"
  },
  "fields": {
    "value": 82.5,
    "humidity": 65.0
  },
  "timestamp": 1465839830100400200
}
```

### Batch Write
```json
{
  "writes": [
    {
      "measurement": "temperature", 
      "tags": {"location": "us-west"},
      "fields": {"value": 75.0},
      "timestamp": 1465839830100400200
    },
    {
      "measurement": "cpu",
      "tags": {"host": "server-02"},
      "fields": {"usage_percent": 85.2, "idle_time": 14.8}
    }
  ]
}
```

## Request Fields

### Required Fields
- **measurement**: String identifying the measurement name
- **fields**: Object with field names and values to store

### Optional Fields
- **tags**: Object with tag key-value pairs for indexing and grouping
- **timestamp**: Nanosecond timestamp (auto-generated if not provided)

### Field Types
The system automatically detects field types:
- **Float**: `82.5`, `3.14159`
- **Integer**: `42`, `1234` 
- **Boolean**: `true`, `false`
- **String**: `"hello world"`, `"error message"`

## Response

### Success Response (200 OK)
```json
{
  "status": "success",
  "points_written": 1
}
```

### Batch Success Response  
```json
{
  "status": "success", 
  "points_written": 5,
  "failed_points": 0
}
```

### Error Response (400 Bad Request)
```json
{
  "status": "error",
  "error": "Invalid JSON format"
}
```

## Usage Examples

### Single Temperature Reading
```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"sensor": "outdoor", "location": "patio"},
    "fields": {"celsius": 23.5, "fahrenheit": 74.3},
    "timestamp": 1609459200000000000
  }'
```

### System Metrics Collection
```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "writes": [
      {
        "measurement": "cpu",
        "tags": {"host": "web-01", "cpu": "cpu0"},
        "fields": {"usage_percent": 45.2, "idle_percent": 54.8}
      },
      {
        "measurement": "memory", 
        "tags": {"host": "web-01"},
        "fields": {"used_bytes": 8589934592, "free_bytes": 4294967296}
      },
      {
        "measurement": "disk",
        "tags": {"host": "web-01", "device": "/dev/sda1"},
        "fields": {"used_percent": 78.5, "free_space": 107374182400}
      }
    ]
  }'
```

### Application Logs
```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "app_logs",
    "tags": {"service": "api", "level": "error"},
    "fields": {
      "message": "Database connection failed",
      "count": 1,
      "response_time": 5420.3
    }
  }'
```

## Python Client Example
```python
import requests
import time
import json

class TSDBClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def write_point(self, measurement, tags=None, fields=None, timestamp=None):
        """Write a single data point."""
        if timestamp is None:
            timestamp = int(time.time() * 1_000_000_000)  # nanoseconds
        
        data = {
            "measurement": measurement,
            "tags": tags or {},
            "fields": fields or {},
            "timestamp": timestamp
        }
        
        response = requests.post(f"{self.base_url}/write", json=data)
        return response.json()
    
    def write_batch(self, points):
        """Write multiple data points."""
        data = {"writes": points}
        response = requests.post(f"{self.base_url}/write", json=data)
        return response.json()

# Usage
client = TSDBClient()

# Single point
result = client.write_point(
    measurement="temperature",
    tags={"location": "office", "sensor": "ds18b20"},
    fields={"celsius": 22.5, "humidity": 45.2}
)
print(result)

# Batch write
points = [
    {
        "measurement": "cpu",
        "tags": {"host": "server1"},
        "fields": {"usage": 67.3}
    },
    {
        "measurement": "memory", 
        "tags": {"host": "server1"},
        "fields": {"used_gb": 12.8, "available_gb": 3.2}
    }
]
result = client.write_batch(points)
print(result)
```

## Data Modeling Best Practices

### Measurement Naming
- Use descriptive names: `cpu_usage`, `network_traffic`, `sensor_readings`
- Avoid special characters and spaces
- Use consistent naming conventions

### Tag Strategy
- Tags are indexed and used for grouping/filtering
- Use tags for dimensions: `host`, `region`, `service`, `environment`
- Keep tag cardinality reasonable (< 100k unique combinations)
- Tag values should be strings

### Field Strategy  
- Fields contain the actual metric values
- Use multiple fields for related metrics: `{cpu_user: 45.2, cpu_system: 12.3}`
- Fields can be any supported data type

### Timestamp Guidelines
- Use nanosecond precision for best compatibility
- Omit timestamp to use server time
- Ensure timestamps are monotonically increasing for best performance

## Error Handling

### Common Errors
- **400 Bad Request**: Invalid JSON format or missing required fields
- **500 Internal Server Error**: Database write failure or server error

### Retry Strategy
```python
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def create_resilient_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
```

## Performance Considerations
1. **Batch Writes**: Use batch writes for better throughput
2. **Connection Pooling**: Reuse HTTP connections
3. **Compression**: Consider gzip compression for large payloads
4. **Async Writes**: Use async clients for high-throughput applications

## Related Endpoints
- [`POST /query`](query-endpoint.md) - Query stored time series data
- [`POST /delete`](delete-endpoint.md) - Delete time series data
- [`GET /measurements`](measurements-endpoint.md) - List stored measurements