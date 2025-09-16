# POST /query - Query Time Series Data

## Overview
The query endpoint allows you to retrieve and analyze time series data stored in the TSDB. It supports time range filtering, aggregation, grouping, and advanced function-based analysis with a simplified string-based query format.

## Endpoint Details
- **URL**: `/query`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Query Structure
```json
{
  "query": "aggregationMethod:measurement(fields){scopes} by {aggregationTagKeys}",
  "startTime": 1704067200000000000,
  "endTime": 1706745599000000000,
  "aggregationInterval": "5m"
}
```

### Request Fields

#### Required Fields
- **query**: String defining what data to retrieve and how to process it
- **startTime**: Start time as nanoseconds since Unix epoch
- **endTime**: End time as nanoseconds since Unix epoch

#### Optional Fields
- **aggregationInterval**: Time bucket interval (string with unit or nanoseconds)

## Query String Format

The query string follows this pattern:
```
aggregationMethod:measurement(fields){scopes} by {aggregationTagKeys}
```

### Components

#### 1. Aggregation Methods (required)
- **`avg`** - Average of values (default)
- **`min`** - Minimum value
- **`max`** - Maximum value  
- **`sum`** - Sum of values
- **`latest`** - Most recent value

#### 2. Measurement (required)
- The measurement name to query
- Must match existing measurement in database
- Case-sensitive

#### 3. Fields (optional)
- Comma-separated list within parentheses
- Empty parentheses `()` returns all fields
- Example: `(value,humidity)` or `()`

#### 4. Scopes (optional)
- Filter conditions in `key:value` format within braces
- Multiple scopes separated by commas (AND condition)
- Must use exact values (no wildcards currently)
- Empty braces `{}` for no filtering
- Example: `{location:us-west,sensor:temp-01}`

#### 5. Group By (optional)
- Tag keys for grouping results after `by` keyword
- Multiple keys separated by commas
- Omit entire `by {}` clause if not grouping
- Example: `by {location,sensor}`

### Aggregation Interval

Time intervals can be specified as:

**Numeric Format** (nanoseconds):
```json
"aggregationInterval": 300000000000
```

**String Format** (with units):
```json
"aggregationInterval": "5m"
```

**Supported Units:**
- `ns` - nanoseconds
- `us`, `µs` - microseconds
- `ms` - milliseconds  
- `s` - seconds
- `m` - minutes
- `h` - hours
- `d` - days

**Decimal Values Supported:**
- `"1.5s"` - 1.5 seconds
- `"0.5m"` - 30 seconds

## Time Format

**Timestamp Format:**
- All timestamps are nanoseconds since Unix epoch (January 1, 1970)
- The API accepts any numeric precision:
  - Seconds: `1704067200` → multiply by 10^9
  - Milliseconds: `1704067200000` → multiply by 10^6
  - Microseconds: `1704067200000000` → multiply by 10^3
  - Nanoseconds: `1704067200000000000` (recommended)

**Quick Conversion:**
```python
import time
current_time_ns = int(time.time() * 1_000_000_000)
```

## Response Format

### Success Response (200 OK)
```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "tags": {
        "location": "us-west",
        "sensor": "temp-01"
      },
      "fields": {
        "value": {
          "timestamps": [1638202821000000000, 1638202822000000000],
          "values": [23.5, 23.6]
        }
      }
    }
  ],
  "statistics": {
    "series_count": 1,
    "point_count": 2,
    "execution_time_ms": 12.5
  }
}
```

### Time-Bucketed Response
When using `aggregationInterval`, results are grouped into time buckets:

```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "tags": {
        "location": "us-west"
      },
      "fields": {
        "value": {
          "timestamps": [
            1709251200000000000,
            1709251500000000000,
            1709251800000000000
          ],
          "values": [23.5, 24.2, 23.8]
        }
      }
    }
  ],
  "statistics": {
    "series_count": 1,
    "point_count": 3,
    "execution_time_ms": 15.3
  }
}
```

### Error Response (400 Bad Request)
```json
{
  "status": "error",
  "error": {
    "code": "INVALID_QUERY",
    "message": "Invalid query format: missing measurement"
  }
}
```

## Usage Examples

### Basic Temperature Query
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature()",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000
  }'
```

### Filtered Query with Specific Fields
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "max:cpu(usage_percent){host:server-01,datacenter:dc1}",
    "startTime": 1707998400000000000,
    "endTime": 1708020000000000000
  }'
```

### Grouped Aggregation
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value,humidity){location:us-west} by {sensor}",
    "startTime": 1709251200000000000,
    "endTime": 1709254800000000000
  }'
```

### Time-Bucketed Query (5-minute intervals)
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value){location:us-west}",
    "startTime": 1709251200000000000,
    "endTime": 1709337600000000000,
    "aggregationInterval": "5m"
  }'
```

### Latest Values Query
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "latest:system.metrics(cpu,memory){server:prod-01}",
    "startTime": 1709251200000000000,
    "endTime": 1709254800000000000
  }'
```

## Python Client Example

```python
import requests
import time
import json
from datetime import datetime, timezone

class TSDBQueryClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def to_nanoseconds(self, timestamp):
        """Convert various timestamp formats to nanoseconds."""
        if isinstance(timestamp, datetime):
            return int(timestamp.timestamp() * 1_000_000_000)
        elif isinstance(timestamp, (int, float)):
            # Assume seconds if less than year 2100 in milliseconds
            if timestamp < 4102444800000:
                return int(timestamp * 1_000_000_000)
            # Assume milliseconds if less than year 2100 in microseconds
            elif timestamp < 4102444800000000:
                return int(timestamp * 1_000_000)
            # Assume microseconds if less than year 2100 in nanoseconds
            elif timestamp < 4102444800000000000:
                return int(timestamp * 1_000)
            else:
                return int(timestamp)
        return int(timestamp)
    
    def query(self, query_string, start_time, end_time, aggregation_interval=None):
        """Execute a query against the TSDB."""
        data = {
            "query": query_string,
            "startTime": self.to_nanoseconds(start_time),
            "endTime": self.to_nanoseconds(end_time)
        }
        
        if aggregation_interval:
            data["aggregationInterval"] = aggregation_interval
        
        response = requests.post(f"{self.base_url}/query", json=data)
        return response.json()
    
    def simple_query(self, measurement, fields=None, tags=None, 
                     start_time=None, end_time=None, aggregation="avg"):
        """Build and execute a simple query."""
        # Default to last hour if no time range specified
        if end_time is None:
            end_time = time.time()
        if start_time is None:
            start_time = end_time - 3600  # 1 hour ago
            
        # Build query string
        field_str = f"({','.join(fields)})" if fields else "()"
        tag_str = ""
        if tags:
            tag_conditions = [f"{k}:{v}" for k, v in tags.items()]
            tag_str = "{" + ",".join(tag_conditions) + "}"
        else:
            tag_str = "{}"
            
        query_string = f"{aggregation}:{measurement}{field_str}{tag_str}"
        
        return self.query(query_string, start_time, end_time)

# Usage Examples
client = TSDBQueryClient()

# Simple temperature query for the last hour
result = client.simple_query(
    measurement="temperature",
    fields=["value", "humidity"],
    tags={"location": "office"}
)
print("Temperature data:", json.dumps(result, indent=2))

# Custom query with time buckets
start = datetime(2024, 3, 1, tzinfo=timezone.utc)
end = datetime(2024, 3, 2, tzinfo=timezone.utc)

result = client.query(
    query_string="max:cpu(usage_percent){datacenter:dc1}",
    start_time=start,
    end_time=end,
    aggregation_interval="1h"
)
print("CPU usage:", json.dumps(result, indent=2))

# Latest system metrics
result = client.query(
    query_string="latest:system(cpu,memory,disk){}",
    start_time=time.time() - 300,  # Last 5 minutes
    end_time=time.time()
)
print("Latest metrics:", json.dumps(result, indent=2))

# Grouped query
result = client.query(
    query_string="avg:temperature(value){location:datacenter} by {rack,server}",
    start_time=time.time() - 1800,  # Last 30 minutes
    end_time=time.time(),
    aggregation_interval="5m"
)
print("Temperature by server:", json.dumps(result, indent=2))
```

## Query Performance Optimization

### Best Practices

1. **Time Range Optimization**
   - Use the narrowest time range possible
   - Prefer recent data for faster queries
   - Consider data retention policies

2. **Tag Filtering**
   - Always include relevant tag filters in scopes
   - More specific scopes = faster queries
   - Use indexed tag keys when available

3. **Field Selection**
   - Specify only needed fields instead of using `()`
   - Reduces data transfer and processing time

4. **Aggregation Strategy**
   - Use aggregation intervals for large time ranges
   - Pre-aggregate data with sum/avg instead of transferring raw points
   - Group by only necessary tag keys

### Query Complexity Guidelines

**Fast Queries:**
```json
{
  "query": "avg:cpu(usage){host:web-01}",
  "startTime": "recent",
  "endTime": "now"
}
```

**Medium Performance:**
```json
{
  "query": "avg:metrics(){datacenter:us-west} by {service}",
  "aggregationInterval": "5m"
}
```

**Slower Queries:**
```json
{
  "query": "avg:logs(){} by {host,service,environment}",
  "startTime": "months_ago",
  "endTime": "now"
}
```

## Error Handling

### Common Error Codes

- **`INVALID_QUERY`**: Malformed query string or missing required components
- **`INVALID_TIME_RANGE`**: Invalid start/end times or end before start
- **`MEASUREMENT_NOT_FOUND`**: Specified measurement doesn't exist
- **`FIELD_NOT_FOUND`**: Requested field doesn't exist in measurement
- **`TIMEOUT`**: Query execution exceeded timeout limit

### Error Response Examples

```json
{
  "status": "error",
  "error": {
    "code": "INVALID_TIME_RANGE",
    "message": "End time must be after start time"
  }
}
```

```json
{
  "status": "error",
  "error": {
    "code": "MEASUREMENT_NOT_FOUND",
    "message": "Measurement 'invalid_measurement' not found"
  }
}
```

### Retry Strategy

```python
import time
import random

def query_with_retry(client, query_data, max_retries=3):
    """Execute query with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            response = requests.post(f"{client.base_url}/query", json=query_data)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Rate limited
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
                continue
            else:
                # For other errors, return immediately
                return response.json()
                
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    
    raise Exception("Max retries exceeded")
```

## Advanced Query Features

### Function Integration

The query endpoint supports integration with the TSDB function system for advanced time series analysis:

```json
{
  "query": "function:sma(temperature(value){location:office}, window=5)",
  "startTime": 1709251200000000000,
  "endTime": 1709337600000000000
}
```

### Multi-Measurement Queries

Query multiple measurements in a single request:

```json
{
  "query": "avg:cpu,memory,disk(usage_percent){host:web-01}",
  "startTime": 1709251200000000000,
  "endTime": 1709254800000000000,
  "aggregationInterval": "1m"
}
```

### Calculated Fields

Perform calculations on query results:

```json
{
  "query": "sum:network(bytes_in,bytes_out){interface:eth0}",
  "startTime": 1709251200000000000,
  "endTime": 1709254800000000000
}
```

## Related Endpoints

- [`POST /write`](write-endpoint.md) - Write time series data
- [`POST /delete`](delete-endpoint.md) - Delete time series data
- [`GET /measurements`](measurements-endpoint.md) - List available measurements
- [`POST /query/functions`](query-functions-endpoint.md) - Execute function queries
- [`POST /query/parse`](query-parse-endpoint.md) - Validate query syntax