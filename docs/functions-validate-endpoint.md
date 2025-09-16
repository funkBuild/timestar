# POST /functions/validate - Validate Function Parameters

## Overview
The function validation endpoint allows you to validate function parameters and usage before executing queries. This helps catch errors early, provides parameter suggestions, and ensures optimal query performance by validating function calls without executing them.

## Endpoint Details
- **URL**: `/functions/validate`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Validation Request
```json
{
  "function": "sma",
  "parameters": {
    "window": 10
  },
  "input_type": "float"
}
```

### Multiple Function Validation
```json
{
  "functions": [
    {
      "function": "sma", 
      "parameters": {
        "window": 5
      },
      "input_type": "float"
    },
    {
      "function": "scale",
      "parameters": {
        "factor": 1.8
      },
      "input_type": "float"
    }
  ]
}
```

### Pipeline Validation
```json
{
  "pipeline": [
    {
      "function": "sma",
      "parameters": {
        "window": 10
      },
      "input_type": "float"
    },
    {
      "function": "derivative",
      "parameters": {
        "method": "central"
      },
      "input_type": "float"
    }
  ]
}
```

### Query String Validation
```json
{
  "query": "sma(temperature(value){location:datacenter}, window=10)"
}
```

## Request Fields

### Single Function Validation
- **function**: Function name to validate (required)
- **parameters**: Object with parameter values to validate (required)
- **input_type**: Expected input data type (optional, default: "float")

### Multiple Function Validation
- **functions**: Array of function validation objects (required)

### Pipeline Validation
- **pipeline**: Array of functions in execution order (required)

### Query String Validation
- **query**: Complete function query string to validate (required)

## Response Format

### Successful Single Function Validation
```json
{
  "status": "success",
  "validation": {
    "function": "sma",
    "valid": true,
    "parameters": {
      "window": {
        "value": 10,
        "type": "integer",
        "valid": true,
        "within_constraints": true
      }
    },
    "input_compatibility": {
      "requested_type": "float",
      "supported_types": ["float", "integer"],
      "compatible": true
    },
    "output_type": "float",
    "performance_estimate": {
      "complexity": "O(n)",
      "execution_time_estimate_ms": 15.2,
      "memory_usage_estimate": "low"
    }
  }
}
```

### Validation with Warnings
```json
{
  "status": "success",
  "validation": {
    "function": "sma",
    "valid": true,
    "parameters": {
      "window": {
        "value": 1000,
        "type": "integer",
        "valid": true,
        "within_constraints": true,
        "warnings": [
          "Large window size may impact performance"
        ]
      }
    },
    "input_compatibility": {
      "requested_type": "float",
      "supported_types": ["float", "integer"],
      "compatible": true
    },
    "output_type": "float",
    "warnings": [
      "Consider using smaller window sizes for better performance"
    ],
    "suggestions": [
      "For smoothing, window sizes between 5-50 are typically most effective"
    ]
  }
}
```

### Validation Failure
```json
{
  "status": "error",
  "validation": {
    "function": "sma",
    "valid": false,
    "errors": [
      "Missing required parameter: window"
    ],
    "parameters": {},
    "input_compatibility": {
      "requested_type": "string",
      "supported_types": ["float", "integer"],
      "compatible": false,
      "error": "Function 'sma' does not support input type 'string'"
    }
  }
}
```

### Multiple Function Validation
```json
{
  "status": "success",
  "validations": [
    {
      "function": "sma",
      "valid": true,
      "parameters": {
        "window": {
          "value": 5,
          "valid": true
        }
      },
      "output_type": "float"
    },
    {
      "function": "scale", 
      "valid": false,
      "errors": [
        "Parameter 'factor' exceeds maximum value of 1000000"
      ],
      "parameters": {
        "factor": {
          "value": 5000000,
          "valid": false,
          "constraint_violation": "exceeds_maximum"
        }
      }
    }
  ],
  "overall_valid": false
}
```

### Pipeline Validation
```json
{
  "status": "success",
  "pipeline_validation": {
    "valid": true,
    "steps": [
      {
        "step": 0,
        "function": "sma",
        "valid": true,
        "input_type": "float",
        "output_type": "float"
      },
      {
        "step": 1,
        "function": "derivative",
        "valid": true,
        "input_type": "float",
        "output_type": "float"
      }
    ],
    "type_chain": ["float", "float", "float"],
    "performance_estimate": {
      "total_complexity": "O(n)",
      "estimated_execution_time_ms": 23.8,
      "memory_usage": "medium"
    }
  }
}
```

### Query String Validation
```json
{
  "status": "success",
  "query_validation": {
    "query": "sma(temperature(value){location:datacenter}, window=10)",
    "valid": true,
    "parsed_components": {
      "function": "sma",
      "measurement": "temperature",
      "field": "value",
      "tags": {
        "location": "datacenter"
      },
      "parameters": {
        "window": 10
      }
    },
    "function_validation": {
      "function": "sma",
      "valid": true,
      "parameters": {
        "window": {
          "value": 10,
          "valid": true
        }
      }
    },
    "estimated_result_type": "float"
  }
}
```

## Usage Examples

### Validate Single Function
```bash
curl -X POST http://localhost:8086/functions/validate \
  -H "Content-Type: application/json" \
  -d '{
    "function": "sma",
    "parameters": {
      "window": 10
    },
    "input_type": "float"
  }'
```

### Validate Multiple Functions
```bash
curl -X POST http://localhost:8086/functions/validate \
  -H "Content-Type: application/json" \
  -d '{
    "functions": [
      {
        "function": "sma",
        "parameters": {"window": 5},
        "input_type": "float"
      },
      {
        "function": "scale", 
        "parameters": {"factor": 2.0},
        "input_type": "float"
      }
    ]
  }'
```

### Validate Function Pipeline
```bash
curl -X POST http://localhost:8086/functions/validate \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline": [
      {
        "function": "sma",
        "parameters": {"window": 10},
        "input_type": "float"
      },
      {
        "function": "derivative",
        "parameters": {"method": "central"},
        "input_type": "float"
      }
    ]
  }'
```

### Validate Query String
```bash
curl -X POST http://localhost:8086/functions/validate \
  -H "Content-Type: application/json" \
  -d '{
    "query": "scale(temperature(celsius), factor=1.8)"
  }'
```

## Python Client Example

```python
import requests
import json
from typing import Dict, Any, List, Optional, Union

class TSDBFunctionValidator:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def validate_function(self, function_name: str, parameters: Dict[str, Any],
                         input_type: str = "float") -> Dict[str, Any]:
        """Validate a single function with parameters."""
        data = {
            "function": function_name,
            "parameters": parameters,
            "input_type": input_type
        }
        
        response = requests.post(f"{self.base_url}/functions/validate", json=data)
        return response.json()
    
    def validate_multiple_functions(self, functions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate multiple functions."""
        data = {"functions": functions}
        response = requests.post(f"{self.base_url}/functions/validate", json=data)
        return response.json()
    
    def validate_pipeline(self, pipeline: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate a function pipeline."""
        data = {"pipeline": pipeline}
        response = requests.post(f"{self.base_url}/functions/validate", json=data)
        return response.json()
    
    def validate_query(self, query: str) -> Dict[str, Any]:
        """Validate a complete function query string."""
        data = {"query": query}
        response = requests.post(f"{self.base_url}/functions/validate", json=data)
        return response.json()
    
    def is_function_call_valid(self, function_name: str, parameters: Dict[str, Any],
                              input_type: str = "float") -> bool:
        """Simple boolean check if function call is valid."""
        result = self.validate_function(function_name, parameters, input_type)
        return (result.get("status") == "success" and 
                result.get("validation", {}).get("valid", False))
    
    def get_validation_errors(self, function_name: str, parameters: Dict[str, Any],
                             input_type: str = "float") -> List[str]:
        """Get list of validation errors."""
        result = self.validate_function(function_name, parameters, input_type)
        
        if result.get("status") != "success":
            return [result.get("error", "Unknown error")]
        
        validation = result.get("validation", {})
        return validation.get("errors", [])
    
    def get_validation_warnings(self, function_name: str, parameters: Dict[str, Any],
                               input_type: str = "float") -> List[str]:
        """Get list of validation warnings."""
        result = self.validate_function(function_name, parameters, input_type)
        
        if result.get("status") != "success":
            return []
        
        validation = result.get("validation", {})
        warnings = validation.get("warnings", [])
        
        # Also collect parameter-specific warnings
        for param_name, param_info in validation.get("parameters", {}).items():
            if isinstance(param_info, dict):
                param_warnings = param_info.get("warnings", [])
                warnings.extend(param_warnings)
        
        return warnings
    
    def get_validation_suggestions(self, function_name: str, parameters: Dict[str, Any],
                                  input_type: str = "float") -> List[str]:
        """Get validation suggestions for improvement."""
        result = self.validate_function(function_name, parameters, input_type)
        
        if result.get("status") != "success":
            return []
        
        validation = result.get("validation", {})
        return validation.get("suggestions", [])
    
    def validate_before_query(self, measurement: str, field: str, function_name: str,
                             parameters: Dict[str, Any], tags: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Validate function before building a query."""
        # Build query string
        tag_str = ""
        if tags:
            tag_conditions = [f"{k}:{v}" for k, v in tags.items()]
            tag_str = "{" + ",".join(tag_conditions) + "}"
        else:
            tag_str = "{}"
        
        # Build parameter string
        param_parts = [f"{k}={v}" for k, v in parameters.items()]
        param_str = ", ".join(param_parts)
        
        query = f"{function_name}({measurement}({field}){tag_str}"
        if param_str:
            query += f", {param_str}"
        query += ")"
        
        # Validate the constructed query
        return self.validate_query(query)
    
    def suggest_parameter_fixes(self, function_name: str, parameters: Dict[str, Any],
                               input_type: str = "float") -> Dict[str, Any]:
        """Suggest fixes for invalid parameters."""
        result = self.validate_function(function_name, parameters, input_type)
        
        suggestions = {
            "original_parameters": parameters,
            "suggested_fixes": {},
            "issues_found": []
        }
        
        if result.get("status") != "success":
            suggestions["issues_found"].append("Function validation failed")
            return suggestions
        
        validation = result.get("validation", {})
        
        for param_name, param_info in validation.get("parameters", {}).items():
            if isinstance(param_info, dict) and not param_info.get("valid", True):
                constraint_violation = param_info.get("constraint_violation")
                original_value = param_info.get("value")
                
                if constraint_violation == "exceeds_maximum":
                    # Suggest maximum allowed value
                    suggestions["suggested_fixes"][param_name] = "Use maximum allowed value"
                elif constraint_violation == "below_minimum":
                    # Suggest minimum allowed value  
                    suggestions["suggested_fixes"][param_name] = "Use minimum allowed value"
                elif constraint_violation == "invalid_type":
                    suggestions["suggested_fixes"][param_name] = f"Convert to correct type"
                
                suggestions["issues_found"].append(f"Parameter '{param_name}' is invalid")
        
        return suggestions

# Usage Examples
validator = TSDBFunctionValidator()

# Validate simple moving average with window=10
result = validator.validate_function("sma", {"window": 10})
if result.get("status") == "success" and result["validation"]["valid"]:
    print("✓ SMA function call is valid")
    
    # Check for warnings
    warnings = validator.get_validation_warnings("sma", {"window": 10})
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  ⚠ {warning}")
    
    # Check for suggestions
    suggestions = validator.get_validation_suggestions("sma", {"window": 10})
    if suggestions:
        print("Suggestions:")
        for suggestion in suggestions:
            print(f"  💡 {suggestion}")
else:
    errors = validator.get_validation_errors("sma", {"window": 10})
    print("✗ Validation failed:")
    for error in errors:
        print(f"  - {error}")

# Validate multiple functions
functions = [
    {
        "function": "sma",
        "parameters": {"window": 5},
        "input_type": "float"
    },
    {
        "function": "scale",
        "parameters": {"factor": 1.8},
        "input_type": "float"
    }
]

multi_result = validator.validate_multiple_functions(functions)
print(f"\nMultiple function validation:")
if multi_result.get("status") == "success":
    overall_valid = multi_result.get("overall_valid", False)
    print(f"Overall valid: {overall_valid}")
    
    for i, validation in enumerate(multi_result.get("validations", [])):
        func_name = validation.get("function", f"function_{i}")
        valid = validation.get("valid", False)
        print(f"  {func_name}: {'✓' if valid else '✗'}")

# Validate function pipeline
pipeline = [
    {
        "function": "sma",
        "parameters": {"window": 10},
        "input_type": "float"
    },
    {
        "function": "derivative",
        "parameters": {"method": "central"},
        "input_type": "float"
    }
]

pipeline_result = validator.validate_pipeline(pipeline)
print(f"\nPipeline validation:")
if pipeline_result.get("status") == "success":
    pipeline_valid = pipeline_result["pipeline_validation"]["valid"]
    print(f"Pipeline valid: {'✓' if pipeline_valid else '✗'}")
    
    steps = pipeline_result["pipeline_validation"]["steps"]
    for step in steps:
        step_num = step["step"]
        func_name = step["function"]
        valid = step["valid"]
        print(f"  Step {step_num} ({func_name}): {'✓' if valid else '✗'}")

# Validate complete query string
query = "sma(temperature(value){location:datacenter}, window=10)"
query_result = validator.validate_query(query)
print(f"\nQuery validation:")
if query_result.get("status") == "success":
    query_valid = query_result["query_validation"]["valid"]
    print(f"Query valid: {'✓' if query_valid else '✗'}")
    
    parsed = query_result["query_validation"]["parsed_components"]
    print(f"  Function: {parsed['function']}")
    print(f"  Measurement: {parsed['measurement']}")
    print(f"  Field: {parsed['field']}")
    print(f"  Parameters: {parsed['parameters']}")

# Validate before building a query
pre_query_result = validator.validate_before_query(
    measurement="cpu",
    field="usage_percent",
    function_name="sma",
    parameters={"window": 5},
    tags={"host": "server-01"}
)
print(f"\nPre-query validation:")
if pre_query_result.get("status") == "success":
    print("✓ Query construction is valid")
    constructed_query = pre_query_result["query_validation"]["query"]
    print(f"  Constructed query: {constructed_query}")

# Get parameter fix suggestions
invalid_params = {"window": -5}  # Invalid negative window
suggestions = validator.suggest_parameter_fixes("sma", invalid_params)
print(f"\nParameter fix suggestions:")
print(f"Issues found: {suggestions['issues_found']}")
for param, suggestion in suggestions["suggested_fixes"].items():
    print(f"  {param}: {suggestion}")
```

## Advanced Validation Features

### Custom Validation Rules
```python
def validate_with_custom_rules(validator, function_name: str, parameters: Dict[str, Any],
                              custom_rules: Dict[str, Any]) -> Dict[str, Any]:
    """Apply custom validation rules on top of standard validation."""
    # First run standard validation
    standard_result = validator.validate_function(function_name, parameters)
    
    if standard_result.get("status") != "success":
        return standard_result
    
    custom_issues = []
    
    # Apply custom rules
    for param_name, param_value in parameters.items():
        if param_name in custom_rules:
            rule = custom_rules[param_name]
            
            # Custom range checks
            if "custom_min" in rule and param_value < rule["custom_min"]:
                custom_issues.append(f"Parameter {param_name} below custom minimum {rule['custom_min']}")
            
            if "custom_max" in rule and param_value > rule["custom_max"]:
                custom_issues.append(f"Parameter {param_name} exceeds custom maximum {rule['custom_max']}")
            
            # Custom value checks
            if "preferred_values" in rule and param_value not in rule["preferred_values"]:
                custom_issues.append(f"Parameter {param_name} not in preferred values: {rule['preferred_values']}")
    
    # Add custom issues to result
    validation = standard_result.get("validation", {})
    if custom_issues:
        existing_warnings = validation.get("warnings", [])
        validation["warnings"] = existing_warnings + custom_issues
    
    return standard_result

# Usage
custom_rules = {
    "window": {
        "custom_min": 3,
        "custom_max": 100,
        "preferred_values": [5, 10, 20, 50]
    }
}

result = validate_with_custom_rules(validator, "sma", {"window": 150}, custom_rules)
```

### Batch Query Validation
```python
def validate_query_batch(validator, queries: List[str]) -> Dict[str, Any]:
    """Validate multiple queries efficiently."""
    results = {
        "total_queries": len(queries),
        "valid_queries": 0,
        "invalid_queries": 0,
        "results": []
    }
    
    for i, query in enumerate(queries):
        result = validator.validate_query(query)
        
        query_result = {
            "index": i,
            "query": query,
            "valid": False,
            "errors": [],
            "warnings": []
        }
        
        if result.get("status") == "success":
            query_valid = result["query_validation"]["valid"]
            query_result["valid"] = query_valid
            
            if query_valid:
                results["valid_queries"] += 1
            else:
                results["invalid_queries"] += 1
                query_result["errors"] = result["query_validation"].get("errors", [])
        else:
            results["invalid_queries"] += 1
            query_result["errors"] = [result.get("error", "Unknown error")]
        
        results["results"].append(query_result)
    
    return results

# Usage
queries = [
    "sma(temperature(value), window=10)",
    "scale(cpu(usage), factor=1.8)",
    "invalid_function(data, param=value)"
]

batch_result = validate_query_batch(validator, queries)
print(f"Batch validation: {batch_result['valid_queries']}/{batch_result['total_queries']} valid")
```

## Performance Optimization

### Validation Caching
```python
import time
from functools import lru_cache

class CachedFunctionValidator(TSDBFunctionValidator):
    def __init__(self, host="localhost", port=8086, cache_size=128):
        super().__init__(host, port)
        self._cache_size = cache_size
    
    @lru_cache(maxsize=128)
    def _cached_validate_function(self, function_name: str, parameters_hash: str,
                                 input_type: str) -> str:
        """Cached validation with string-based parameters."""
        parameters = json.loads(parameters_hash)
        result = super().validate_function(function_name, parameters, input_type)
        return json.dumps(result)
    
    def validate_function(self, function_name: str, parameters: Dict[str, Any],
                         input_type: str = "float") -> Dict[str, Any]:
        """Validate with caching."""
        # Convert parameters to deterministic hash
        parameters_hash = json.dumps(parameters, sort_keys=True)
        
        # Get cached result
        cached_result = self._cached_validate_function(function_name, parameters_hash, input_type)
        return json.loads(cached_result)

# Usage
cached_validator = CachedFunctionValidator()

# First call - hits the API
result1 = cached_validator.validate_function("sma", {"window": 10})

# Second call with same parameters - uses cache
result2 = cached_validator.validate_function("sma", {"window": 10})
```

## Error Handling

### Common Error Scenarios
- **Invalid Function Name**: Function doesn't exist
- **Missing Required Parameters**: Required parameters not provided
- **Parameter Constraint Violations**: Values outside allowed ranges
- **Type Incompatibility**: Input type not supported by function
- **Invalid Query Syntax**: Malformed query strings

### Robust Error Handling
```python
def validate_safely(validator, function_name: str, parameters: Dict[str, Any]):
    """Validate with comprehensive error handling."""
    try:
        result = validator.validate_function(function_name, parameters)
        
        if result.get("status") == "success":
            validation = result.get("validation", {})
            if validation.get("valid", False):
                print(f"✓ {function_name} validation passed")
                
                # Show warnings if any
                warnings = validation.get("warnings", [])
                for warning in warnings:
                    print(f"  ⚠ {warning}")
                
                return True
            else:
                print(f"✗ {function_name} validation failed:")
                errors = validation.get("errors", ["Unknown validation error"])
                for error in errors:
                    print(f"  - {error}")
                return False
        else:
            print(f"✗ Validation request failed: {result.get('error', 'Unknown error')}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("✗ Failed to connect to TSDB server")
        return False
    except requests.exceptions.Timeout:
        print("✗ Validation request timed out")
        return False
    except Exception as e:
        print(f"✗ Unexpected error during validation: {e}")
        return False

# Usage
validate_safely(validator, "sma", {"window": 10})
```

## Related Endpoints

- [`GET /functions`](functions-endpoint.md) - List all available functions for validation
- [`GET /functions/{name}`](functions-name-endpoint.md) - Get detailed function information including constraints
- [`POST /query/parse`](query-parse-endpoint.md) - Parse query syntax for validation
- [`POST /query/functions`](query-functions-endpoint.md) - Execute validated function queries
- [`GET /functions/performance`](functions-performance-endpoint.md) - Performance data for validation decisions