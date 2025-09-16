# GET /measurements - List Available Measurements

## Overview
The measurements endpoint provides discovery of all measurement names stored in the TSDB. This is essential for exploring your data structure, building queries, and understanding what metrics are available for analysis.

## Endpoint Details
- **URL**: `/measurements`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Request
```bash
curl -X GET http://localhost:8086/measurements
```

### Optional Query Parameters
- **prefix**: Filter measurements that start with a specific prefix
- **limit**: Maximum number of measurements to return (default: 1000)
- **offset**: Number of measurements to skip for pagination (default: 0)

### Request with Parameters
```bash
curl -X GET "http://localhost:8086/measurements?prefix=system&limit=50&offset=10"
```

## Response Format

### Success Response (200 OK)
```json
{
  "status": "success",
  "measurements": [
    "cpu",
    "memory", 
    "disk",
    "network",
    "temperature",
    "application_metrics",
    "system_logs"
  ],
  "total_count": 7,
  "has_more": false
}
```

### Detailed Response with Metadata
```json
{
  "status": "success",
  "measurements": [
    {
      "name": "cpu",
      "field_count": 4,
      "series_count": 15,
      "first_timestamp": 1704067200000000000,
      "last_timestamp": 1709337600000000000
    },
    {
      "name": "memory",
      "field_count": 3,
      "series_count": 8,
      "first_timestamp": 1704067200000000000,
      "last_timestamp": 1709337600000000000
    }
  ],
  "total_count": 2,
  "has_more": true,
  "pagination": {
    "limit": 50,
    "offset": 0,
    "next_offset": 50
  }
}
```

### Empty Response
```json
{
  "status": "success",
  "measurements": [],
  "total_count": 0,
  "has_more": false
}
```

### Error Response (500 Internal Server Error)
```json
{
  "status": "error",
  "error": "Failed to retrieve measurements from database"
}
```

## Usage Examples

### List All Measurements
```bash
# Basic measurements list
curl http://localhost:8086/measurements

# Pretty print JSON
curl -s http://localhost:8086/measurements | jq
```

### Filter by Prefix
```bash
# Get all system-related measurements
curl "http://localhost:8086/measurements?prefix=system"

# Get application metrics
curl "http://localhost:8086/measurements?prefix=app"
```

### Paginated Requests
```bash
# First page (50 measurements)
curl "http://localhost:8086/measurements?limit=50&offset=0"

# Second page
curl "http://localhost:8086/measurements?limit=50&offset=50"

# Third page  
curl "http://localhost:8086/measurements?limit=50&offset=100"
```

## Python Client Example

```python
import requests
import json
from typing import List, Optional, Dict, Any

class TSDBMeasurementsClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_measurements(self, prefix: Optional[str] = None, 
                        limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get list of measurements with optional filtering and pagination."""
        params = {}
        if prefix:
            params['prefix'] = prefix
        if limit != 1000:
            params['limit'] = limit
        if offset > 0:
            params['offset'] = offset
        
        response = requests.get(f"{self.base_url}/measurements", params=params)
        return response.json()
    
    def get_all_measurements(self, prefix: Optional[str] = None) -> List[str]:
        """Get all measurements, handling pagination automatically."""
        all_measurements = []
        offset = 0
        limit = 100
        
        while True:
            result = self.get_measurements(prefix=prefix, limit=limit, offset=offset)
            
            if result.get("status") != "success":
                raise Exception(f"Failed to fetch measurements: {result}")
            
            measurements = result.get("measurements", [])
            all_measurements.extend(measurements)
            
            if not result.get("has_more", False):
                break
                
            offset += limit
        
        return all_measurements
    
    def search_measurements(self, pattern: str) -> List[str]:
        """Search for measurements containing a pattern."""
        all_measurements = self.get_all_measurements()
        return [m for m in all_measurements if pattern.lower() in m.lower()]
    
    def get_measurements_by_category(self) -> Dict[str, List[str]]:
        """Group measurements by common prefixes/categories."""
        all_measurements = self.get_all_measurements()
        categories = {}
        
        for measurement in all_measurements:
            # Determine category based on common prefixes
            if measurement.startswith(('cpu', 'memory', 'disk', 'network')):
                category = 'system'
            elif measurement.startswith(('app', 'application')):
                category = 'application'
            elif measurement.startswith(('log', 'error', 'debug')):
                category = 'logs'
            elif measurement.startswith(('sensor', 'temp', 'humidity')):
                category = 'sensors'
            elif measurement.startswith(('http', 'api', 'web')):
                category = 'web'
            else:
                category = 'other'
            
            if category not in categories:
                categories[category] = []
            categories[category].append(measurement)
        
        return categories
    
    def validate_measurement_exists(self, measurement_name: str) -> bool:
        """Check if a specific measurement exists."""
        all_measurements = self.get_all_measurements()
        return measurement_name in all_measurements

# Usage Examples
client = TSDBMeasurementsClient()

# Get all measurements
measurements = client.get_all_measurements()
print(f"Found {len(measurements)} measurements:")
for measurement in sorted(measurements):
    print(f"  - {measurement}")

# Search for specific measurements
cpu_measurements = client.search_measurements("cpu")
print(f"\nCPU-related measurements: {cpu_measurements}")

# Get measurements by category
categories = client.get_measurements_by_category()
print(f"\nMeasurements by category:")
for category, measurements in categories.items():
    print(f"  {category}: {len(measurements)} measurements")
    for m in sorted(measurements)[:3]:  # Show first 3
        print(f"    - {m}")
    if len(measurements) > 3:
        print(f"    ... and {len(measurements) - 3} more")

# Get system measurements with pagination
result = client.get_measurements(prefix="system", limit=10)
print(f"\nSystem measurements (first 10):")
print(json.dumps(result, indent=2))

# Validate specific measurement
if client.validate_measurement_exists("temperature"):
    print("\n✓ Temperature measurement exists")
else:
    print("\n✗ Temperature measurement not found")
```

## Integration Examples

### Grafana Data Source Discovery
```python
def get_grafana_measurements():
    """Format measurements for Grafana data source."""
    client = TSDBMeasurementsClient()
    measurements = client.get_all_measurements()
    
    # Format for Grafana dropdown
    grafana_format = [
        {"text": measurement, "value": measurement}
        for measurement in sorted(measurements)
    ]
    
    return grafana_format
```

### Monitoring Dashboard
```python
def create_dashboard_config():
    """Create monitoring dashboard configuration."""
    client = TSDBMeasurementsClient()
    categories = client.get_measurements_by_category()
    
    dashboard_panels = []
    
    for category, measurements in categories.items():
        panel = {
            "title": category.title(),
            "measurements": measurements,
            "panel_type": "graph"
        }
        dashboard_panels.append(panel)
    
    return {
        "dashboard": "TSDB Monitoring",
        "panels": dashboard_panels
    }
```

### Data Validation Script
```python
#!/usr/bin/env python3
"""
Validate expected measurements exist in TSDB.
"""

def validate_expected_measurements():
    """Validate that all expected measurements are present."""
    client = TSDBMeasurementsClient()
    
    expected_measurements = [
        "cpu",
        "memory", 
        "disk",
        "network",
        "application_metrics"
    ]
    
    existing_measurements = client.get_all_measurements()
    
    print("Measurement validation:")
    all_present = True
    
    for expected in expected_measurements:
        if expected in existing_measurements:
            print(f"✓ {expected}")
        else:
            print(f"✗ {expected} - MISSING")
            all_present = False
    
    # Check for unexpected measurements
    unexpected = set(existing_measurements) - set(expected_measurements)
    if unexpected:
        print(f"\nUnexpected measurements found:")
        for measurement in sorted(unexpected):
            print(f"! {measurement}")
    
    return all_present

if __name__ == "__main__":
    validate_expected_measurements()
```

## Automation and Scripting

### Backup Script Integration
```bash
#!/bin/bash
# Backup all measurements

TSDB_HOST="localhost"
TSDB_PORT="8086"
BACKUP_DIR="/backups/tsdb/$(date +%Y%m%d)"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Get list of measurements
measurements=$(curl -s "http://${TSDB_HOST}:${TSDB_PORT}/measurements" | jq -r '.measurements[]')

# Backup each measurement
for measurement in $measurements; do
    echo "Backing up measurement: $measurement"
    
    # Query and save measurement data
    curl -s -X POST "http://${TSDB_HOST}:${TSDB_PORT}/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"query\": \"avg:${measurement}()\",
            \"startTime\": $(date -d '30 days ago' +%s)000000000,
            \"endTime\": $(date +%s)000000000
        }" > "${BACKUP_DIR}/${measurement}.json"
done

echo "Backup completed in $BACKUP_DIR"
```

### Measurement Monitoring
```python
import time
import logging
from datetime import datetime

def monitor_measurements():
    """Monitor for new measurements being added."""
    client = TSDBMeasurementsClient()
    known_measurements = set()
    
    while True:
        try:
            current_measurements = set(client.get_all_measurements())
            
            # Check for new measurements
            new_measurements = current_measurements - known_measurements
            if new_measurements:
                for measurement in new_measurements:
                    logging.info(f"New measurement detected: {measurement}")
                
                known_measurements = current_measurements
            
            # Check for removed measurements
            removed_measurements = known_measurements - current_measurements
            if removed_measurements:
                for measurement in removed_measurements:
                    logging.warning(f"Measurement removed: {measurement}")
                
                known_measurements = current_measurements
            
            time.sleep(60)  # Check every minute
            
        except Exception as e:
            logging.error(f"Error monitoring measurements: {e}")
            time.sleep(60)
```

## Performance Considerations

### Caching Strategy
```python
import time
from functools import lru_cache

class CachedMeasurementsClient(TSDBMeasurementsClient):
    def __init__(self, host="localhost", port=8086, cache_ttl=300):
        super().__init__(host, port)
        self.cache_ttl = cache_ttl
        self._cache_timestamp = 0
        self._cached_measurements = []
    
    def get_all_measurements(self, use_cache=True):
        """Get measurements with optional caching."""
        current_time = time.time()
        
        if (use_cache and 
            self._cached_measurements and 
            current_time - self._cache_timestamp < self.cache_ttl):
            return self._cached_measurements
        
        # Refresh cache
        measurements = super().get_all_measurements()
        self._cached_measurements = measurements
        self._cache_timestamp = current_time
        
        return measurements
```

### Batch Processing
```python
def process_measurements_in_batches(measurements, batch_size=10):
    """Process measurements in batches to avoid overwhelming the system."""
    for i in range(0, len(measurements), batch_size):
        batch = measurements[i:i + batch_size]
        
        # Process this batch
        for measurement in batch:
            # Do something with each measurement
            process_single_measurement(measurement)
        
        # Small delay between batches
        time.sleep(0.1)
```

## Error Handling

### Robust Client Implementation
```python
def get_measurements_safely():
    """Get measurements with comprehensive error handling."""
    client = TSDBMeasurementsClient()
    
    try:
        result = client.get_measurements()
        
        if result.get("status") != "success":
            logging.error(f"API returned error: {result.get('error', 'Unknown error')}")
            return []
        
        measurements = result.get("measurements", [])
        logging.info(f"Successfully retrieved {len(measurements)} measurements")
        return measurements
        
    except requests.exceptions.ConnectionError:
        logging.error("Failed to connect to TSDB server")
        return []
    except requests.exceptions.Timeout:
        logging.error("Request timed out")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return []
```

## Related Endpoints

- [`GET /tags`](tags-endpoint.md) - List tag keys and values for measurements
- [`GET /fields`](fields-endpoint.md) - List field names and types for measurements
- [`POST /query`](query-endpoint.md) - Query data from specific measurements
- [`POST /write`](write-endpoint.md) - Write data to measurements
- [`POST /delete`](delete-endpoint.md) - Delete entire measurements