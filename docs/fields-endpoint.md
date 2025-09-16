# GET /fields - List Available Field Keys and Types

## Overview
The fields endpoint provides discovery of field names and their data types across measurements in the TSDB. Fields contain the actual metric values being stored, and understanding their types is essential for proper query construction and data analysis.

## Endpoint Details
- **URL**: `/fields`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Request
```bash
curl -X GET http://localhost:8086/fields
```

### Query Parameters
- **measurement**: Filter fields for a specific measurement
- **type**: Filter fields by data type (float, integer, boolean, string)
- **limit**: Maximum number of results to return (default: 1000)
- **offset**: Number of results to skip for pagination (default: 0)

### Request Examples
```bash
# Get all field keys across all measurements
curl "http://localhost:8086/fields"

# Get fields for specific measurement
curl "http://localhost:8086/fields?measurement=cpu"

# Get only float fields
curl "http://localhost:8086/fields?type=float"

# Get string fields for temperature measurement
curl "http://localhost:8086/fields?measurement=temperature&type=string"

# Paginated request
curl "http://localhost:8086/fields?limit=50&offset=10"
```

## Response Format

### All Fields Response
```json
{
  "status": "success",
  "fields": [
    {
      "key": "usage_percent",
      "type": "float",
      "measurements": ["cpu", "memory", "disk"]
    },
    {
      "key": "bytes_total",
      "type": "integer",
      "measurements": ["memory", "disk", "network"]
    },
    {
      "key": "is_healthy",
      "type": "boolean",
      "measurements": ["system_status", "service_health"]
    },
    {
      "key": "error_message",
      "type": "string",
      "measurements": ["application_logs", "error_logs"]
    }
  ],
  "total_count": 4
}
```

### Fields for Specific Measurement
```json
{
  "status": "success",
  "measurement": "cpu",
  "fields": [
    {
      "key": "usage_percent",
      "type": "float",
      "range": {"min": 0.0, "max": 100.0},
      "sample_count": 15420
    },
    {
      "key": "idle_percent",
      "type": "float",
      "range": {"min": 0.0, "max": 100.0},
      "sample_count": 15420
    },
    {
      "key": "core_count",
      "type": "integer",
      "range": {"min": 1, "max": 64},
      "sample_count": 2540
    },
    {
      "key": "throttled",
      "type": "boolean",
      "sample_count": 8930
    }
  ],
  "total_count": 4
}
```

### Fields by Type Response
```json
{
  "status": "success",
  "field_type": "float",
  "fields": [
    {
      "key": "temperature",
      "measurements": ["sensors", "weather"],
      "sample_count": 45230,
      "range": {"min": -40.5, "max": 125.8}
    },
    {
      "key": "cpu_usage",
      "measurements": ["system_metrics", "host_stats"],
      "sample_count": 98442,
      "range": {"min": 0.0, "max": 100.0}
    }
  ],
  "total_count": 2
}
```

### Detailed Fields Information
```json
{
  "status": "success",
  "fields": [
    {
      "key": "response_time",
      "type": "float",
      "measurements": ["http_requests", "api_calls"],
      "statistics": {
        "min": 0.05,
        "max": 5420.33,
        "avg": 245.67,
        "sample_count": 1847293
      },
      "first_seen": 1704067200000000000,
      "last_seen": 1709337600000000000
    },
    {
      "key": "status_code",
      "type": "integer",
      "measurements": ["http_requests"],
      "statistics": {
        "min": 200,
        "max": 504,
        "sample_count": 1847293,
        "distinct_values": 12
      },
      "common_values": [200, 404, 500, 301, 403]
    }
  ],
  "total_count": 2
}
```

### Empty Response
```json
{
  "status": "success",
  "fields": [],
  "total_count": 0
}
```

### Error Response (400 Bad Request)
```json
{
  "status": "error",
  "error": "Invalid field type specified. Supported types: float, integer, boolean, string"
}
```

## Field Data Types

### Supported Types

1. **float** - Floating-point numbers
   - Examples: `23.5`, `3.14159`, `-40.2`
   - Use for: temperatures, percentages, ratios, measurements

2. **integer** - Whole numbers
   - Examples: `42`, `-15`, `1000000`
   - Use for: counts, IDs, status codes, memory sizes

3. **boolean** - True/false values
   - Examples: `true`, `false`
   - Use for: flags, status indicators, binary states

4. **string** - Text values
   - Examples: `"error message"`, `"user-agent-string"`
   - Use for: logs, error messages, descriptive text

## Usage Examples

### Basic Field Discovery
```bash
# Discover all field keys and types
curl -s http://localhost:8086/fields | jq '.fields[] | {key: .key, type: .type}'

# Get fields for CPU measurement
curl -s "http://localhost:8086/fields?measurement=cpu" | jq

# Get all float fields
curl -s "http://localhost:8086/fields?type=float" | jq '.fields[].key'
```

### Type-specific Queries
```bash
# Get all boolean fields (flags/status indicators)
curl -s "http://localhost:8086/fields?type=boolean" | jq

# Get string fields for log analysis
curl -s "http://localhost:8086/fields?type=string" | jq

# Get integer fields for counting metrics
curl -s "http://localhost:8086/fields?type=integer" | jq
```

## Python Client Example

```python
import requests
import json
from typing import List, Dict, Any, Optional, Union
from collections import defaultdict

class TSDBFieldsClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_fields(self, measurement: Optional[str] = None, 
                  field_type: Optional[str] = None,
                  limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        """Get field information with optional filtering."""
        params = {}
        if measurement:
            params['measurement'] = measurement
        if field_type:
            params['type'] = field_type
        if limit != 1000:
            params['limit'] = limit
        if offset > 0:
            params['offset'] = offset
        
        response = requests.get(f"{self.base_url}/fields", params=params)
        return response.json()
    
    def get_all_fields(self, measurement: Optional[str] = None, 
                      field_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all fields with automatic pagination."""
        all_fields = []
        offset = 0
        limit = 100
        
        while True:
            result = self.get_fields(measurement=measurement, field_type=field_type,
                                   limit=limit, offset=offset)
            
            if result.get("status") != "success":
                raise Exception(f"Failed to fetch fields: {result}")
            
            fields = result.get("fields", [])
            all_fields.extend(fields)
            
            if len(fields) < limit:  # No more data
                break
                
            offset += limit
        
        return all_fields
    
    def get_field_keys_by_type(self, measurement: Optional[str] = None) -> Dict[str, List[str]]:
        """Group field keys by their data types."""
        fields = self.get_all_fields(measurement=measurement)
        
        fields_by_type = defaultdict(list)
        for field in fields:
            field_type = field.get("type", "unknown")
            field_key = field.get("key", "")
            fields_by_type[field_type].append(field_key)
        
        return dict(fields_by_type)
    
    def get_numeric_fields(self, measurement: Optional[str] = None) -> List[str]:
        """Get fields that contain numeric data (float or integer)."""
        fields = self.get_all_fields(measurement=measurement)
        
        numeric_fields = []
        for field in fields:
            if field.get("type") in ["float", "integer"]:
                numeric_fields.append(field.get("key", ""))
        
        return numeric_fields
    
    def get_measurement_schema(self, measurement: str) -> Dict[str, Any]:
        """Get complete field schema for a measurement."""
        fields = self.get_all_fields(measurement=measurement)
        
        schema = {
            "measurement": measurement,
            "field_count": len(fields),
            "fields": {},
            "type_summary": defaultdict(int)
        }
        
        for field in fields:
            field_key = field.get("key", "")
            field_type = field.get("type", "unknown")
            
            schema["fields"][field_key] = {
                "type": field_type,
                "sample_count": field.get("sample_count", 0),
                "statistics": field.get("statistics", {}),
                "range": field.get("range", {})
            }
            
            schema["type_summary"][field_type] += 1
        
        schema["type_summary"] = dict(schema["type_summary"])
        return schema
    
    def validate_field_types(self, measurement: str, 
                           expected_fields: Dict[str, str]) -> Dict[str, Any]:
        """Validate that fields in a measurement have expected types."""
        actual_fields = {f["key"]: f["type"] for f in self.get_all_fields(measurement)}
        
        validation_result = {
            "measurement": measurement,
            "valid": True,
            "issues": []
        }
        
        for field_name, expected_type in expected_fields.items():
            if field_name not in actual_fields:
                validation_result["issues"].append({
                    "type": "missing_field",
                    "field": field_name,
                    "expected_type": expected_type
                })
                validation_result["valid"] = False
            elif actual_fields[field_name] != expected_type:
                validation_result["issues"].append({
                    "type": "type_mismatch",
                    "field": field_name,
                    "expected_type": expected_type,
                    "actual_type": actual_fields[field_name]
                })
                validation_result["valid"] = False
        
        # Check for unexpected fields
        unexpected_fields = set(actual_fields.keys()) - set(expected_fields.keys())
        if unexpected_fields:
            for field_name in unexpected_fields:
                validation_result["issues"].append({
                    "type": "unexpected_field",
                    "field": field_name,
                    "actual_type": actual_fields[field_name]
                })
        
        return validation_result
    
    def find_compatible_fields(self, field_types: List[str]) -> Dict[str, List[str]]:
        """Find measurements that have fields of specified types."""
        compatible_measurements = defaultdict(list)
        
        for field_type in field_types:
            fields = self.get_all_fields(field_type=field_type)
            
            for field in fields:
                field_key = field.get("key", "")
                measurements = field.get("measurements", [])
                
                for measurement in measurements:
                    if field_key not in compatible_measurements[measurement]:
                        compatible_measurements[measurement].append(field_key)
        
        return dict(compatible_measurements)

# Usage Examples
client = TSDBFieldsClient()

# Get all fields
all_fields = client.get_all_fields()
print(f"Found {len(all_fields)} fields:")
for field in all_fields[:5]:  # Show first 5
    print(f"  - {field['key']} ({field['type']})")

# Get fields by type
fields_by_type = client.get_field_keys_by_type()
print(f"\nFields by type:")
for field_type, keys in fields_by_type.items():
    print(f"  {field_type}: {len(keys)} fields")
    print(f"    {', '.join(keys[:3])}{'...' if len(keys) > 3 else ''}")

# Get numeric fields for mathematical operations
numeric_fields = client.get_numeric_fields(measurement="cpu")
print(f"\nNumeric fields in CPU measurement: {numeric_fields}")

# Get complete schema for a measurement
schema = client.get_measurement_schema("system_metrics")
print(f"\nSystem metrics schema:")
print(json.dumps(schema, indent=2))

# Validate field types
expected_cpu_fields = {
    "usage_percent": "float",
    "idle_percent": "float", 
    "core_count": "integer",
    "throttled": "boolean"
}

validation = client.validate_field_types("cpu", expected_cpu_fields)
if validation["valid"]:
    print("✓ CPU measurement schema is valid")
else:
    print("✗ CPU measurement schema issues:")
    for issue in validation["issues"]:
        print(f"  - {issue}")

# Find measurements with float and integer fields
compatible = client.find_compatible_fields(["float", "integer"])
print(f"\nMeasurements with numeric fields:")
for measurement, field_list in compatible.items():
    print(f"  {measurement}: {', '.join(field_list[:3])}{'...' if len(field_list) > 3 else ''}")
```

## Advanced Use Cases

### Data Type Analysis
```python
def analyze_field_types(fields_client):
    """Analyze data type distribution across the TSDB."""
    all_fields = fields_client.get_all_fields()
    
    type_analysis = {
        "total_fields": len(all_fields),
        "type_distribution": defaultdict(int),
        "type_details": defaultdict(list)
    }
    
    for field in all_fields:
        field_type = field.get("type", "unknown")
        field_key = field.get("key", "")
        measurements = field.get("measurements", [])
        
        type_analysis["type_distribution"][field_type] += 1
        type_analysis["type_details"][field_type].append({
            "key": field_key,
            "measurement_count": len(measurements),
            "measurements": measurements
        })
    
    return type_analysis

# Usage
analysis = analyze_field_types(client)
print("Field type analysis:")
for field_type, count in analysis["type_distribution"].items():
    percentage = (count / analysis["total_fields"]) * 100
    print(f"  {field_type}: {count} fields ({percentage:.1f}%)")
```

### Query Optimization Helper
```python
def suggest_query_fields(fields_client, measurement: str, operation: str):
    """Suggest appropriate fields for different query operations."""
    fields = fields_client.get_all_fields(measurement=measurement)
    
    suggestions = {
        "aggregation": [],  # Numeric fields for avg, sum, etc.
        "counting": [],     # Integer fields for count operations  
        "filtering": [],    # Boolean fields for filtering
        "grouping": []      # Fields with limited distinct values
    }
    
    for field in fields:
        field_key = field.get("key", "")
        field_type = field.get("type", "")
        statistics = field.get("statistics", {})
        
        # Suggest numeric fields for aggregation
        if field_type in ["float", "integer"]:
            suggestions["aggregation"].append(field_key)
        
        # Suggest integer fields for counting
        if field_type == "integer":
            suggestions["counting"].append(field_key)
        
        # Suggest boolean fields for filtering
        if field_type == "boolean":
            suggestions["filtering"].append(field_key)
        
        # Suggest fields with low cardinality for grouping
        distinct_values = statistics.get("distinct_values", float('inf'))
        if distinct_values < 50:  # Reasonable for grouping
            suggestions["grouping"].append(field_key)
    
    if operation in suggestions:
        return suggestions[operation]
    
    return suggestions

# Usage
suggestions = suggest_query_fields(client, "http_requests", "aggregation")
print(f"Fields suitable for aggregation: {suggestions}")
```

### Schema Migration Helper
```python
def generate_migration_plan(fields_client, old_schema: Dict[str, str], 
                          new_schema: Dict[str, str], measurement: str):
    """Generate a plan for migrating field schemas."""
    current_fields = {f["key"]: f["type"] for f in fields_client.get_all_fields(measurement)}
    
    migration_plan = {
        "measurement": measurement,
        "actions": []
    }
    
    # Fields to add
    for field_name, field_type in new_schema.items():
        if field_name not in current_fields:
            migration_plan["actions"].append({
                "action": "add_field",
                "field": field_name,
                "type": field_type
            })
    
    # Fields to remove
    for field_name in current_fields:
        if field_name not in new_schema:
            migration_plan["actions"].append({
                "action": "remove_field", 
                "field": field_name,
                "current_type": current_fields[field_name]
            })
    
    # Type changes
    for field_name, new_type in new_schema.items():
        if (field_name in current_fields and 
            current_fields[field_name] != new_type):
            migration_plan["actions"].append({
                "action": "change_type",
                "field": field_name,
                "from_type": current_fields[field_name],
                "to_type": new_type
            })
    
    return migration_plan
```

## Monitoring and Validation

### Field Schema Monitoring
```python
#!/usr/bin/env python3
"""
Monitor field schemas for changes.
"""

import time
import json
import logging
from datetime import datetime

def monitor_field_schemas():
    """Monitor for changes in field schemas."""
    client = TSDBFieldsClient()
    previous_schemas = {}
    
    logging.info("Starting field schema monitoring...")
    
    while True:
        try:
            # Get current schemas for all measurements
            measurements = get_all_measurements()  # From measurements client
            current_schemas = {}
            
            for measurement in measurements:
                schema = client.get_measurement_schema(measurement)
                current_schemas[measurement] = schema
            
            # Compare with previous schemas
            for measurement, schema in current_schemas.items():
                if measurement in previous_schemas:
                    prev_schema = previous_schemas[measurement]
                    
                    # Check for new fields
                    new_fields = set(schema["fields"].keys()) - set(prev_schema["fields"].keys())
                    for field in new_fields:
                        field_type = schema["fields"][field]["type"]
                        logging.info(f"New field in {measurement}: {field} ({field_type})")
                    
                    # Check for removed fields
                    removed_fields = set(prev_schema["fields"].keys()) - set(schema["fields"].keys())
                    for field in removed_fields:
                        logging.warning(f"Field removed from {measurement}: {field}")
                    
                    # Check for type changes
                    for field in schema["fields"]:
                        if field in prev_schema["fields"]:
                            old_type = prev_schema["fields"][field]["type"]
                            new_type = schema["fields"][field]["type"]
                            if old_type != new_type:
                                logging.warning(f"Type change in {measurement}.{field}: {old_type} -> {new_type}")
                else:
                    logging.info(f"New measurement schema: {measurement} ({len(schema['fields'])} fields)")
            
            previous_schemas = current_schemas
            time.sleep(300)  # Check every 5 minutes
            
        except Exception as e:
            logging.error(f"Error monitoring field schemas: {e}")
            time.sleep(60)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    monitor_field_schemas()
```

### Data Quality Validation
```python
def validate_data_quality(fields_client, measurement: str):
    """Validate data quality based on field statistics."""
    schema = fields_client.get_measurement_schema(measurement)
    quality_issues = []
    
    for field_name, field_info in schema["fields"].items():
        field_type = field_info["type"]
        statistics = field_info.get("statistics", {})
        sample_count = field_info.get("sample_count", 0)
        
        # Check for fields with no data
        if sample_count == 0:
            quality_issues.append({
                "issue": "no_data",
                "field": field_name,
                "message": f"Field {field_name} has no data points"
            })
        
        # Check for suspicious ranges in numeric fields
        if field_type == "float" and "range" in field_info:
            range_info = field_info["range"]
            min_val = range_info.get("min", 0)
            max_val = range_info.get("max", 0)
            
            # Check for percentage fields outside 0-100 range
            if "percent" in field_name.lower():
                if min_val < 0 or max_val > 100:
                    quality_issues.append({
                        "issue": "invalid_percentage",
                        "field": field_name,
                        "message": f"Percentage field has values outside 0-100: {min_val}-{max_val}"
                    })
        
        # Check for fields with very low sample counts
        total_samples = sum(f.get("sample_count", 0) for f in schema["fields"].values())
        if total_samples > 0:
            field_ratio = sample_count / total_samples
            if field_ratio < 0.1:  # Less than 10% of total samples
                quality_issues.append({
                    "issue": "sparse_data",
                    "field": field_name,
                    "message": f"Field {field_name} is sparse ({field_ratio:.1%} of samples)"
                })
    
    return quality_issues

# Usage
issues = validate_data_quality(client, "system_metrics")
if issues:
    print("Data quality issues found:")
    for issue in issues:
        print(f"  - {issue['message']}")
else:
    print("✓ No data quality issues detected")
```

## Performance Optimization

### Field Query Optimization
```python
def optimize_field_queries(fields_client, measurement: str):
    """Suggest query optimizations based on field characteristics."""
    schema = fields_client.get_measurement_schema(measurement)
    recommendations = []
    
    numeric_fields = []
    sparse_fields = []
    high_cardinality_fields = []
    
    for field_name, field_info in schema["fields"].items():
        field_type = field_info["type"]
        statistics = field_info.get("statistics", {})
        sample_count = field_info.get("sample_count", 0)
        
        # Collect numeric fields
        if field_type in ["float", "integer"]:
            numeric_fields.append(field_name)
        
        # Identify sparse fields
        total_samples = sum(f.get("sample_count", 0) for f in schema["fields"].values())
        if total_samples > 0 and sample_count / total_samples < 0.5:
            sparse_fields.append(field_name)
        
        # Identify high-cardinality fields
        distinct_values = statistics.get("distinct_values", 0)
        if distinct_values > 1000:
            high_cardinality_fields.append(field_name)
    
    # Generate recommendations
    if numeric_fields:
        recommendations.append({
            "type": "aggregation",
            "message": f"Use aggregation functions (avg, sum, max) with: {', '.join(numeric_fields[:3])}"
        })
    
    if sparse_fields:
        recommendations.append({
            "type": "filtering",
            "message": f"Consider filtering sparse fields to improve query performance: {', '.join(sparse_fields)}"
        })
    
    if high_cardinality_fields:
        recommendations.append({
            "type": "grouping",
            "message": f"Avoid grouping by high-cardinality fields: {', '.join(high_cardinality_fields)}"
        })
    
    return recommendations
```

## Related Endpoints

- [`GET /measurements`](measurements-endpoint.md) - List measurements that contain these fields
- [`GET /tags`](tags-endpoint.md) - List tag keys that work with field combinations  
- [`POST /query`](query-endpoint.md) - Query specific fields with type-appropriate operations
- [`POST /write`](write-endpoint.md) - Write data with proper field types
- [`GET /functions`](functions-endpoint.md) - Find functions compatible with field types