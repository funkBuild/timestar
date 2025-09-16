# GET /tags - List Available Tag Keys and Values

## Overview
The tags endpoint provides discovery of tag keys and their associated values across measurements in the TSDB. Tags are used for indexing, filtering, and grouping time series data, making this endpoint essential for building queries and understanding data dimensions.

## Endpoint Details
- **URL**: `/tags`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Request
```bash
curl -X GET http://localhost:8086/tags
```

### Query Parameters
- **measurement**: Filter tags for a specific measurement
- **key**: Get values for a specific tag key
- **limit**: Maximum number of results to return (default: 1000)
- **offset**: Number of results to skip for pagination (default: 0)

### Request Examples
```bash
# Get all tag keys across all measurements
curl "http://localhost:8086/tags"

# Get tag keys for specific measurement
curl "http://localhost:8086/tags?measurement=cpu"

# Get values for a specific tag key
curl "http://localhost:8086/tags?key=host"

# Get host values for cpu measurement
curl "http://localhost:8086/tags?measurement=cpu&key=host"

# Paginated request
curl "http://localhost:8086/tags?limit=50&offset=10"
```

## Response Format

### All Tag Keys Response
```json
{
  "status": "success",
  "tag_keys": [
    "host",
    "region",
    "datacenter",
    "environment",
    "service",
    "instance",
    "location",
    "sensor_type"
  ],
  "total_count": 8
}
```

### Tag Keys for Specific Measurement
```json
{
  "status": "success",
  "measurement": "cpu",
  "tag_keys": [
    "host",
    "cpu_core",
    "datacenter",
    "environment"
  ],
  "total_count": 4
}
```

### Tag Values for Specific Key
```json
{
  "status": "success",
  "tag_key": "host",
  "tag_values": [
    "web-server-01",
    "web-server-02", 
    "db-server-01",
    "cache-server-01",
    "load-balancer-01"
  ],
  "total_count": 5,
  "measurements": ["cpu", "memory", "disk", "network"]
}
```

### Tag Values for Measurement and Key
```json
{
  "status": "success",
  "measurement": "temperature",
  "tag_key": "location",
  "tag_values": [
    "datacenter-rack-01",
    "datacenter-rack-02",
    "office-floor-1",
    "office-floor-2",
    "outdoor-sensor"
  ],
  "total_count": 5
}
```

### Detailed Tag Information
```json
{
  "status": "success",
  "tags": [
    {
      "key": "host",
      "values": ["server-01", "server-02", "server-03"],
      "value_count": 3,
      "measurements": ["cpu", "memory", "disk"],
      "series_count": 45
    },
    {
      "key": "region",
      "values": ["us-west", "us-east", "eu-central"],
      "value_count": 3,
      "measurements": ["cpu", "network"],
      "series_count": 18
    }
  ],
  "total_count": 2
}
```

### Empty Response
```json
{
  "status": "success",
  "tag_keys": [],
  "total_count": 0
}
```

### Error Response (400 Bad Request)
```json
{
  "status": "error",
  "error": "Invalid measurement name provided"
}
```

## Usage Examples

### Basic Tag Discovery
```bash
# Discover all tag keys
curl -s http://localhost:8086/tags | jq '.tag_keys[]'

# Get tags for specific measurement
curl -s "http://localhost:8086/tags?measurement=system_metrics" | jq

# Get all possible values for 'environment' tag
curl -s "http://localhost:8086/tags?key=environment" | jq '.tag_values[]'
```

### Advanced Filtering
```bash
# Get datacenter values for CPU measurement
curl -s "http://localhost:8086/tags?measurement=cpu&key=datacenter" \
  | jq '.tag_values[]'

# Get first 10 host values
curl -s "http://localhost:8086/tags?key=host&limit=10" \
  | jq '.tag_values[]'
```

## Python Client Example

```python
import requests
import json
from typing import List, Dict, Any, Optional

class TSDBTagsClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_tag_keys(self, measurement: Optional[str] = None, 
                    limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get all tag keys, optionally filtered by measurement."""
        params = {}
        if measurement:
            params['measurement'] = measurement
        if limit != 1000:
            params['limit'] = limit
        if offset > 0:
            params['offset'] = offset
        
        response = requests.get(f"{self.base_url}/tags", params=params)
        return response.json()
    
    def get_tag_values(self, tag_key: str, measurement: Optional[str] = None,
                      limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get all values for a specific tag key."""
        params = {'key': tag_key}
        if measurement:
            params['measurement'] = measurement
        if limit != 1000:
            params['limit'] = limit
        if offset > 0:
            params['offset'] = offset
        
        response = requests.get(f"{self.base_url}/tags", params=params)
        return response.json()
    
    def get_all_tag_keys(self, measurement: Optional[str] = None) -> List[str]:
        """Get all tag keys with automatic pagination."""
        all_keys = []
        offset = 0
        limit = 100
        
        while True:
            result = self.get_tag_keys(measurement=measurement, limit=limit, offset=offset)
            
            if result.get("status") != "success":
                raise Exception(f"Failed to fetch tag keys: {result}")
            
            keys = result.get("tag_keys", [])
            all_keys.extend(keys)
            
            if len(keys) < limit:  # No more data
                break
                
            offset += limit
        
        return list(set(all_keys))  # Remove duplicates
    
    def get_all_tag_values(self, tag_key: str, measurement: Optional[str] = None) -> List[str]:
        """Get all values for a tag key with automatic pagination."""
        all_values = []
        offset = 0
        limit = 100
        
        while True:
            result = self.get_tag_values(tag_key, measurement=measurement, 
                                       limit=limit, offset=offset)
            
            if result.get("status") != "success":
                raise Exception(f"Failed to fetch tag values: {result}")
            
            values = result.get("tag_values", [])
            all_values.extend(values)
            
            if len(values) < limit:  # No more data
                break
                
            offset += limit
        
        return list(set(all_values))  # Remove duplicates
    
    def get_tag_hierarchy(self) -> Dict[str, Dict[str, List[str]]]:
        """Build a hierarchical view of measurements -> tag keys -> values."""
        from ..measurements_client import TSDBMeasurementsClient
        
        measurements_client = TSDBMeasurementsClient(
            host=self.base_url.split("://")[1].split(":")[0],
            port=int(self.base_url.split(":")[-1])
        )
        
        measurements = measurements_client.get_all_measurements()
        hierarchy = {}
        
        for measurement in measurements:
            # Get tag keys for this measurement
            tag_keys = self.get_all_tag_keys(measurement=measurement)
            hierarchy[measurement] = {}
            
            for tag_key in tag_keys:
                # Get values for this tag key in this measurement
                tag_values = self.get_all_tag_values(tag_key, measurement=measurement)
                hierarchy[measurement][tag_key] = tag_values
        
        return hierarchy
    
    def find_common_tags(self, measurements: List[str]) -> Dict[str, List[str]]:
        """Find tag keys that are common across multiple measurements."""
        if not measurements:
            return {}
        
        # Get tag keys for each measurement
        measurement_tags = {}
        for measurement in measurements:
            measurement_tags[measurement] = set(self.get_all_tag_keys(measurement=measurement))
        
        # Find intersection of all tag keys
        common_keys = set.intersection(*measurement_tags.values())
        
        # For each common key, get all possible values across measurements
        common_tags = {}
        for tag_key in common_keys:
            all_values = set()
            for measurement in measurements:
                values = self.get_all_tag_values(tag_key, measurement=measurement)
                all_values.update(values)
            common_tags[tag_key] = list(all_values)
        
        return common_tags
    
    def validate_tag_combination(self, measurement: str, tags: Dict[str, str]) -> bool:
        """Validate that a tag combination exists for a measurement."""
        # Get all available tag keys for the measurement
        available_keys = self.get_all_tag_keys(measurement=measurement)
        
        for tag_key, tag_value in tags.items():
            # Check if tag key exists
            if tag_key not in available_keys:
                return False
            
            # Check if tag value exists for this key
            available_values = self.get_all_tag_values(tag_key, measurement=measurement)
            if tag_value not in available_values:
                return False
        
        return True

# Usage Examples
client = TSDBTagsClient()

# Get all tag keys
all_keys = client.get_all_tag_keys()
print(f"Available tag keys: {all_keys}")

# Get tag keys for CPU measurement
cpu_keys = client.get_all_tag_keys(measurement="cpu")
print(f"CPU measurement tag keys: {cpu_keys}")

# Get all host values
hosts = client.get_all_tag_values("host")
print(f"Available hosts: {hosts}")

# Get datacenter values for temperature measurement
datacenters = client.get_all_tag_values("datacenter", measurement="temperature")
print(f"Temperature measurement datacenters: {datacenters}")

# Build complete tag hierarchy
hierarchy = client.get_tag_hierarchy()
print("Tag hierarchy:")
print(json.dumps(hierarchy, indent=2))

# Find common tags across system measurements
common_tags = client.find_common_tags(["cpu", "memory", "disk"])
print(f"Common tags across system measurements: {common_tags}")

# Validate tag combination
is_valid = client.validate_tag_combination("cpu", {"host": "web-01", "environment": "production"})
print(f"Tag combination is valid: {is_valid}")
```

## Query Building Integration

### Dynamic Query Builder
```python
class QueryBuilder:
    def __init__(self, tags_client):
        self.tags_client = tags_client
    
    def build_interactive_query(self):
        """Interactively build a query using available tags."""
        # Get available measurements
        measurements = self.get_available_measurements()
        print("Available measurements:")
        for i, m in enumerate(measurements):
            print(f"  {i+1}. {m}")
        
        # Select measurement
        choice = int(input("Select measurement: ")) - 1
        measurement = measurements[choice]
        
        # Get available tag keys for measurement
        tag_keys = self.tags_client.get_all_tag_keys(measurement=measurement)
        
        # Build filter conditions
        filters = {}
        for tag_key in tag_keys:
            values = self.tags_client.get_all_tag_values(tag_key, measurement=measurement)
            print(f"\nAvailable values for '{tag_key}':")
            for i, v in enumerate(values[:10]):  # Show first 10
                print(f"  {i+1}. {v}")
            
            if len(values) > 10:
                print(f"  ... and {len(values) - 10} more")
            
            choice = input(f"Select value for {tag_key} (or press Enter to skip): ")
            if choice:
                try:
                    idx = int(choice) - 1
                    filters[tag_key] = values[idx]
                except (ValueError, IndexError):
                    pass
        
        # Build query string
        filter_str = ",".join([f"{k}:{v}" for k, v in filters.items()])
        query = f"avg:{measurement}(){{{filter_str}}}"
        
        print(f"\nGenerated query: {query}")
        return query
```

### Tag-based Dashboard Generation
```python
def generate_dashboard_config(tags_client):
    """Generate dashboard configuration based on available tags."""
    hierarchy = tags_client.get_tag_hierarchy()
    
    dashboard = {
        "name": "Auto-generated TSDB Dashboard",
        "panels": []
    }
    
    for measurement, tag_data in hierarchy.items():
        # Create panel for each measurement
        panel = {
            "title": f"{measurement.title()} Metrics",
            "measurement": measurement,
            "filters": []
        }
        
        # Add common tag filters
        for tag_key, tag_values in tag_data.items():
            if len(tag_values) <= 10:  # Only add tags with reasonable cardinality
                panel["filters"].append({
                    "tag": tag_key,
                    "values": tag_values
                })
        
        dashboard["panels"].append(panel)
    
    return dashboard
```

## Advanced Use Cases

### Tag Cardinality Analysis
```python
def analyze_tag_cardinality(tags_client, measurement=None):
    """Analyze tag cardinality to identify high-cardinality tags."""
    tag_keys = tags_client.get_all_tag_keys(measurement=measurement)
    
    cardinality_report = []
    
    for tag_key in tag_keys:
        values = tags_client.get_all_tag_values(tag_key, measurement=measurement)
        cardinality = len(values)
        
        # Classify cardinality
        if cardinality < 10:
            category = "low"
        elif cardinality < 100:
            category = "medium"
        elif cardinality < 1000:
            category = "high"
        else:
            category = "very_high"
        
        cardinality_report.append({
            "tag_key": tag_key,
            "cardinality": cardinality,
            "category": category,
            "sample_values": values[:5]  # First 5 values as sample
        })
    
    # Sort by cardinality
    cardinality_report.sort(key=lambda x: x["cardinality"], reverse=True)
    
    return cardinality_report

# Usage
report = analyze_tag_cardinality(client, measurement="application_metrics")
print("Tag cardinality analysis:")
for item in report:
    print(f"  {item['tag_key']}: {item['cardinality']} values ({item['category']})")
```

### Tag Value Migration
```python
def migrate_tag_values(tags_client, tag_key, value_mapping):
    """Generate migration script for renaming tag values."""
    # This would integrate with write/delete endpoints
    # to rename tag values across the database
    
    print(f"Migration plan for tag '{tag_key}':")
    for old_value, new_value in value_mapping.items():
        print(f"  {old_value} -> {new_value}")
    
    # Get all measurements using this tag
    all_measurements = []  # Would get from measurements client
    
    migration_steps = []
    for measurement in all_measurements:
        values = tags_client.get_all_tag_values(tag_key, measurement=measurement)
        for old_value, new_value in value_mapping.items():
            if old_value in values:
                migration_steps.append({
                    "measurement": measurement,
                    "tag_key": tag_key,
                    "old_value": old_value,
                    "new_value": new_value
                })
    
    return migration_steps
```

## Monitoring and Alerting

### Tag Monitoring Script
```python
#!/usr/bin/env python3
"""
Monitor tag keys and values for changes.
"""

import time
import json
import logging
from datetime import datetime

def monitor_tag_changes():
    """Monitor for changes in tag structure."""
    client = TSDBTagsClient()
    previous_state = {}
    
    logging.info("Starting tag monitoring...")
    
    while True:
        try:
            current_state = {}
            
            # Get current tag structure
            tag_keys = client.get_all_tag_keys()
            for tag_key in tag_keys:
                values = client.get_all_tag_values(tag_key)
                current_state[tag_key] = set(values)
            
            # Compare with previous state
            if previous_state:
                # Check for new tag keys
                new_keys = set(current_state.keys()) - set(previous_state.keys())
                for key in new_keys:
                    logging.info(f"New tag key detected: {key}")
                
                # Check for removed tag keys
                removed_keys = set(previous_state.keys()) - set(current_state.keys())
                for key in removed_keys:
                    logging.warning(f"Tag key removed: {key}")
                
                # Check for new tag values
                for key in current_state.keys():
                    if key in previous_state:
                        new_values = current_state[key] - previous_state[key]
                        for value in new_values:
                            logging.info(f"New value for tag '{key}': {value}")
                        
                        removed_values = previous_state[key] - current_state[key]
                        for value in removed_values:
                            logging.warning(f"Value removed from tag '{key}': {value}")
            
            previous_state = current_state
            time.sleep(300)  # Check every 5 minutes
            
        except Exception as e:
            logging.error(f"Error monitoring tags: {e}")
            time.sleep(60)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor_tag_changes()
```

## Performance Optimization

### Tag Caching Strategy
```python
import time
from functools import lru_cache
from threading import Lock

class CachedTagsClient(TSDBTagsClient):
    def __init__(self, host="localhost", port=8086, cache_ttl=300):
        super().__init__(host, port)
        self.cache_ttl = cache_ttl
        self._cache = {}
        self._cache_timestamps = {}
        self._cache_lock = Lock()
    
    def _get_cache_key(self, measurement=None, tag_key=None):
        """Generate cache key."""
        return f"{measurement}:{tag_key}"
    
    def _is_cache_valid(self, cache_key):
        """Check if cache entry is still valid."""
        if cache_key not in self._cache_timestamps:
            return False
        
        age = time.time() - self._cache_timestamps[cache_key]
        return age < self.cache_ttl
    
    def get_tag_keys(self, measurement=None, limit=1000, offset=0):
        """Get tag keys with caching."""
        # Only cache unlimited requests
        if limit != 1000 or offset != 0:
            return super().get_tag_keys(measurement, limit, offset)
        
        cache_key = self._get_cache_key(measurement=measurement)
        
        with self._cache_lock:
            if self._is_cache_valid(cache_key):
                return self._cache[cache_key]
        
        # Fetch fresh data
        result = super().get_tag_keys(measurement, limit, offset)
        
        with self._cache_lock:
            self._cache[cache_key] = result
            self._cache_timestamps[cache_key] = time.time()
        
        return result
```

## Error Handling and Validation

### Robust Tag Operations
```python
def get_tags_with_fallback(tags_client, measurement=None):
    """Get tags with comprehensive error handling."""
    try:
        result = tags_client.get_tag_keys(measurement=measurement)
        
        if result.get("status") != "success":
            logging.error(f"API returned error: {result.get('error', 'Unknown error')}")
            return []
        
        return result.get("tag_keys", [])
        
    except requests.exceptions.ConnectionError:
        logging.error("Failed to connect to TSDB server")
        return []
    except requests.exceptions.Timeout:
        logging.error("Request timed out")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return []

def validate_tag_structure(tags_client, expected_structure):
    """Validate that expected tag structure exists."""
    for measurement, expected_tags in expected_structure.items():
        actual_tags = get_tags_with_fallback(tags_client, measurement)
        
        missing_tags = set(expected_tags) - set(actual_tags)
        if missing_tags:
            logging.warning(f"Missing tags in {measurement}: {missing_tags}")
        
        extra_tags = set(actual_tags) - set(expected_tags)
        if extra_tags:
            logging.info(f"Extra tags in {measurement}: {extra_tags}")
```

## Related Endpoints

- [`GET /measurements`](measurements-endpoint.md) - List measurements that contain these tags
- [`GET /fields`](fields-endpoint.md) - List field names that work with tag combinations
- [`POST /query`](query-endpoint.md) - Use tags to filter and group query results
- [`POST /write`](write-endpoint.md) - Write data with tag key-value pairs
- [`POST /delete`](delete-endpoint.md) - Delete data filtered by tag combinations