# POST /delete - Delete Time Series Data

## Overview
The delete endpoint allows you to remove time series data from the TSDB. It supports selective deletion by measurement, tags, fields, and time ranges, enabling precise data management and cleanup operations.

## Endpoint Details
- **URL**: `/delete`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Formats

### Single Series Delete
```json
{
  "measurement": "temperature",
  "tags": {
    "location": "us-midwest",
    "host": "server-01"
  },
  "fields": ["value", "humidity"],
  "startTime": 1465839830100400200,
  "endTime": 1465839830200400200
}
```

### Batch Delete
```json
{
  "deletes": [
    {
      "measurement": "temperature",
      "tags": {"location": "us-west"},
      "startTime": 1465839830100400200,
      "endTime": 1465839830200400200
    },
    {
      "measurement": "cpu",
      "tags": {"host": "server-02"},
      "fields": ["usage_percent"]
    }
  ]
}
```

### Delete All Data for Measurement
```json
{
  "measurement": "old_metrics"
}
```

## Request Fields

### Required Fields
- **measurement**: String identifying the measurement name to delete from

### Optional Fields
- **tags**: Object with tag key-value pairs to match for deletion (exact match)
- **fields**: Array of specific field names to delete (omit to delete all fields)
- **startTime**: Start time boundary for deletion (nanosecond timestamp)
- **endTime**: End time boundary for deletion (nanosecond timestamp)

### Time Range Behavior
- **No time range specified**: Deletes all matching data
- **Only startTime**: Deletes data from startTime onwards
- **Only endTime**: Deletes data up to endTime
- **Both times**: Deletes data within the specified time window

## Response Format

### Success Response (200 OK)
```json
{
  "status": "success",
  "deleted_series": 1,
  "deleted_points": 150
}
```

### Batch Success Response
```json
{
  "status": "success",
  "deleted_series": 5,
  "deleted_points": 1205,
  "failed_deletes": 0,
  "details": [
    {
      "measurement": "temperature",
      "deleted_points": 800
    },
    {
      "measurement": "cpu", 
      "deleted_points": 405
    }
  ]
}
```

### Error Response (400 Bad Request)
```json
{
  "status": "error",
  "error": "Invalid time range: end time must be after start time"
}
```

## Usage Examples

### Delete Specific Sensor Data
```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"sensor": "outdoor", "location": "patio"},
    "startTime": 1609459200000000000,
    "endTime": 1609545600000000000
  }'
```

### Delete Old CPU Metrics
```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "cpu",
    "endTime": 1609459200000000000
  }'
```

### Delete Specific Fields
```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "system_metrics",
    "fields": ["deprecated_field", "old_metric"],
    "tags": {"host": "legacy-server"}
  }'
```

### Batch Delete Multiple Measurements
```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "deletes": [
      {
        "measurement": "temp_logs",
        "endTime": 1609459200000000000
      },
      {
        "measurement": "debug_metrics",
        "tags": {"environment": "test"}
      },
      {
        "measurement": "cache_stats",
        "fields": ["hit_ratio_old", "miss_count_v1"]
      }
    ]
  }'
```

### Delete Entire Measurement
```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "obsolete_measurement"
  }'
```

## Python Client Example

```python
import requests
import time
import json
from datetime import datetime, timezone, timedelta

class TSDBDeleteClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def to_nanoseconds(self, timestamp):
        """Convert various timestamp formats to nanoseconds."""
        if isinstance(timestamp, datetime):
            return int(timestamp.timestamp() * 1_000_000_000)
        elif isinstance(timestamp, (int, float)):
            # Auto-detect timestamp precision
            if timestamp < 4102444800000:  # Year 2100 in milliseconds
                return int(timestamp * 1_000_000_000)
            elif timestamp < 4102444800000000:  # Year 2100 in microseconds
                return int(timestamp * 1_000_000)
            elif timestamp < 4102444800000000000:  # Year 2100 in nanoseconds
                return int(timestamp * 1_000)
            else:
                return int(timestamp)
        return int(timestamp)
    
    def delete_data(self, measurement, tags=None, fields=None, 
                   start_time=None, end_time=None):
        """Delete data matching the specified criteria."""
        data = {"measurement": measurement}
        
        if tags:
            data["tags"] = tags
        if fields:
            data["fields"] = fields
        if start_time:
            data["startTime"] = self.to_nanoseconds(start_time)
        if end_time:
            data["endTime"] = self.to_nanoseconds(end_time)
        
        response = requests.post(f"{self.base_url}/delete", json=data)
        return response.json()
    
    def delete_batch(self, delete_operations):
        """Execute multiple delete operations in a single request."""
        # Convert timestamps in each operation
        processed_ops = []
        for op in delete_operations:
            processed_op = op.copy()
            if "startTime" in op:
                processed_op["startTime"] = self.to_nanoseconds(op["startTime"])
            if "endTime" in op:
                processed_op["endTime"] = self.to_nanoseconds(op["endTime"])
            processed_ops.append(processed_op)
        
        data = {"deletes": processed_ops}
        response = requests.post(f"{self.base_url}/delete", json=data)
        return response.json()
    
    def delete_old_data(self, measurement, older_than_days, tags=None):
        """Delete data older than specified number of days."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        
        return self.delete_data(
            measurement=measurement,
            tags=tags,
            end_time=cutoff_time
        )
    
    def delete_time_range(self, measurement, start_time, end_time, tags=None, fields=None):
        """Delete data within a specific time range."""
        return self.delete_data(
            measurement=measurement,
            tags=tags,
            fields=fields,
            start_time=start_time,
            end_time=end_time
        )
    
    def cleanup_measurement(self, measurement, keep_recent_hours=24):
        """Keep only recent data, delete the rest."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=keep_recent_hours)
        
        return self.delete_data(
            measurement=measurement,
            end_time=cutoff_time
        )

# Usage Examples
client = TSDBDeleteClient()

# Delete specific sensor data from last week
result = client.delete_data(
    measurement="temperature",
    tags={"location": "office", "sensor": "ds18b20"},
    start_time=datetime.now() - timedelta(days=7),
    end_time=datetime.now() - timedelta(days=6)
)
print("Delete result:", json.dumps(result, indent=2))

# Delete old data (older than 30 days)
result = client.delete_old_data(
    measurement="debug_logs",
    older_than_days=30,
    tags={"level": "debug"}
)
print("Cleanup result:", json.dumps(result, indent=2))

# Batch delete multiple data sets
batch_operations = [
    {
        "measurement": "temp_metrics",
        "tags": {"environment": "staging"}
    },
    {
        "measurement": "cpu_stats", 
        "fields": ["deprecated_metric"],
        "endTime": datetime.now() - timedelta(days=90)
    },
    {
        "measurement": "network_logs",
        "tags": {"interface": "eth1"},
        "startTime": datetime(2024, 1, 1),
        "endTime": datetime(2024, 2, 1)
    }
]

result = client.delete_batch(batch_operations)
print("Batch delete result:", json.dumps(result, indent=2))

# Cleanup keeping only recent data
result = client.cleanup_measurement(
    measurement="high_frequency_metrics", 
    keep_recent_hours=6
)
print("Cleanup result:", json.dumps(result, indent=2))

# Delete specific time range
start = datetime(2024, 3, 1, tzinfo=timezone.utc)
end = datetime(2024, 3, 2, tzinfo=timezone.utc)

result = client.delete_time_range(
    measurement="test_data",
    start_time=start,
    end_time=end,
    tags={"test_run": "experiment_01"}
)
print("Time range delete:", json.dumps(result, indent=2))
```

## Data Retention Management

### Automated Cleanup Script
```python
#!/usr/bin/env python3
"""
Automated TSDB data retention management script.
Run this script periodically (via cron) to maintain data retention policies.
"""

import sys
import logging
from datetime import datetime, timezone, timedelta
from tsdb_delete_client import TSDBDeleteClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_retention_policies():
    """Define retention policies for different measurement types."""
    return {
        # High-frequency metrics - keep 7 days
        "high_freq_metrics": {"days": 7},
        "sensor_readings": {"days": 7},
        
        # Application metrics - keep 30 days  
        "app_metrics": {"days": 30},
        "performance_stats": {"days": 30},
        
        # Log data - keep 90 days
        "application_logs": {"days": 90},
        "error_logs": {"days": 90},
        
        # Debug data - keep 3 days only
        "debug_traces": {"days": 3},
        "test_metrics": {"days": 3},
    }

def apply_retention_policy(client, measurement, policy):
    """Apply retention policy to a measurement."""
    try:
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=policy["days"])
        
        result = client.delete_data(
            measurement=measurement,
            end_time=cutoff_date
        )
        
        if result.get("status") == "success":
            deleted_points = result.get("deleted_points", 0)
            logging.info(f"✓ {measurement}: Deleted {deleted_points} old data points")
        else:
            logging.error(f"✗ {measurement}: Delete failed - {result}")
            
    except Exception as e:
        logging.error(f"✗ {measurement}: Exception during cleanup - {e}")

def main():
    """Main retention management function."""
    client = TSDBDeleteClient()
    policies = setup_retention_policies()
    
    logging.info("Starting TSDB data retention cleanup...")
    
    total_cleaned = 0
    for measurement, policy in policies.items():
        apply_retention_policy(client, measurement, policy)
        total_cleaned += 1
    
    logging.info(f"Retention cleanup completed for {total_cleaned} measurements")

if __name__ == "__main__":
    main()
```

### Cron Job Setup
```bash
# Add to crontab for daily cleanup at 2 AM
# crontab -e
0 2 * * * /usr/bin/python3 /path/to/tsdb_retention_cleanup.py >> /var/log/tsdb_cleanup.log 2>&1
```

## Safety and Recovery

### Backup Before Delete
```python
def safe_delete_with_backup(client, query_client, delete_request):
    """Perform backup before deletion for safety."""
    # First, query the data that will be deleted
    backup_query = {
        "query": f"avg:{delete_request['measurement']}()",
        "startTime": delete_request.get("startTime", 0),
        "endTime": delete_request.get("endTime", int(time.time() * 1e9))
    }
    
    # Create backup
    backup_data = query_client.query(**backup_query)
    backup_file = f"backup_{delete_request['measurement']}_{int(time.time())}.json"
    
    with open(backup_file, 'w') as f:
        json.dump(backup_data, f, indent=2)
    
    print(f"Backup created: {backup_file}")
    
    # Confirm deletion
    confirm = input("Proceed with deletion? (yes/no): ")
    if confirm.lower() == "yes":
        result = client.delete_data(**delete_request)
        print("Delete result:", json.dumps(result, indent=2))
        return result
    else:
        print("Deletion cancelled")
        return None
```

### Dry Run Mode
```python
def dry_run_delete(client, delete_request):
    """Simulate deletion to show what would be deleted."""
    # Convert delete request to query to see affected data
    query_request = {
        "query": f"avg:{delete_request['measurement']}()",
        "startTime": delete_request.get("startTime", 0),
        "endTime": delete_request.get("endTime", int(time.time() * 1e9))
    }
    
    # Add tag filters if present
    if "tags" in delete_request:
        tag_filters = ",".join([f"{k}:{v}" for k, v in delete_request["tags"].items()])
        query_request["query"] += f"{{{tag_filters}}}"
    
    result = query_client.query(**query_request)
    
    if result.get("status") == "success":
        point_count = result["statistics"]["point_count"]
        series_count = result["statistics"]["series_count"]
        
        print(f"DRY RUN: Would delete {point_count} points from {series_count} series")
        print(f"Measurement: {delete_request['measurement']}")
        if "tags" in delete_request:
            print(f"Tags: {delete_request['tags']}")
        if "fields" in delete_request:
            print(f"Fields: {delete_request['fields']}")
            
        return {"would_delete_points": point_count, "would_delete_series": series_count}
    
    return result
```

## Performance Considerations

### Deletion Efficiency

1. **Tag Specificity**: More specific tag filters make deletions faster
2. **Time Ranges**: Narrow time ranges are more efficient than full measurement deletes
3. **Batch Operations**: Use batch deletes for multiple related operations
4. **Field Selection**: Deleting specific fields is faster than deleting entire series

### Resource Usage

- **Memory**: Large deletions may require significant memory for indexing
- **Disk I/O**: TSM file compaction occurs after deletions
- **Background Tasks**: Deletions trigger background cleanup processes

### Best Practices

```python
# Good: Specific deletion with time bounds
client.delete_data(
    measurement="metrics",
    tags={"host": "server-01", "environment": "test"},
    fields=["deprecated_metric"],
    end_time=cutoff_time
)

# Avoid: Overly broad deletions
client.delete_data(measurement="metrics")  # Deletes everything!
```

## Error Handling

### Common Error Scenarios

- **Invalid Time Range**: End time before start time
- **Measurement Not Found**: Attempting to delete non-existent measurement
- **Tag Mismatch**: No data matches the specified tag combination
- **Permission Denied**: Insufficient rights for deletion operations

### Retry Strategy
```python
def delete_with_retry(client, delete_request, max_retries=3):
    """Delete with exponential backoff retry."""
    for attempt in range(max_retries):
        try:
            result = client.delete_data(**delete_request)
            
            if result.get("status") == "success":
                return result
            elif "timeout" in str(result).lower():
                wait_time = 2 ** attempt
                time.sleep(wait_time)
                continue
            else:
                return result
                
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    
    raise Exception("Max retries exceeded for deletion")
```

## Related Endpoints

- [`POST /write`](write-endpoint.md) - Write time series data
- [`POST /query`](query-endpoint.md) - Query time series data before deletion
- [`GET /measurements`](measurements-endpoint.md) - List measurements to delete
- [`GET /tags`](tags-endpoint.md) - Discover tag combinations for targeted deletion
- [`GET /fields`](fields-endpoint.md) - Identify fields for selective deletion