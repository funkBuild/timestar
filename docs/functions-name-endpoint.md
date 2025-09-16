# GET /functions/{name} - Get Specific Function Details

## Overview
This endpoint provides detailed information about a specific time series function, including its parameters, usage examples, performance characteristics, and implementation details. This is essential for understanding how to properly use functions in queries.

## Endpoint Details
- **URL**: `/functions/{name}`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Path Parameters
- **name**: The name of the function to retrieve details for (required)

### Request Examples
```bash
# Get details for the Simple Moving Average function
curl "http://localhost:8086/functions/sma"

# Get details for the Scale function
curl "http://localhost:8086/functions/scale"

# Get details for the Exponential Moving Average function
curl "http://localhost:8086/functions/ema"
```

## Response Format

### Detailed Function Information
```json
{
  "status": "success",
  "function": {
    "name": "sma",
    "display_name": "Simple Moving Average",
    "category": "statistical",
    "description": "Calculates the simple moving average of values over a specified window size. The moving average smooths out short-term fluctuations and highlights longer-term trends in time series data.",
    "input_types": ["float", "integer"],
    "output_type": "float",
    "parameters": [
      {
        "name": "window",
        "type": "integer",
        "required": true,
        "default": null,
        "description": "Size of the moving window (number of data points to average)",
        "constraints": {
          "min": 1,
          "max": 1000
        },
        "examples": [5, 10, 20, 50]
      }
    ],
    "examples": [
      {
        "description": "5-point moving average of temperature data",
        "query": "sma(temperature(value){location:datacenter}, window=5)",
        "explanation": "Calculates the average of every 5 consecutive temperature readings"
      },
      {
        "description": "10-point moving average of CPU usage",
        "query": "sma(cpu(usage_percent){host:server-01}, window=10)",
        "explanation": "Smooths CPU usage data using a 10-point moving window"
      }
    ],
    "performance": {
      "complexity": "O(n)",
      "memory_usage": "O(w)",
      "performance_tier": "fast",
      "typical_execution_time_ms": 12.5,
      "max_input_size": 1000000
    },
    "implementation": {
      "algorithm": "Sliding window with running sum",
      "precision": "double",
      "handles_missing_values": true,
      "missing_value_strategy": "skip",
      "thread_safe": true
    },
    "related_functions": [
      {
        "name": "ema",
        "relation": "alternative",
        "description": "Exponential moving average - gives more weight to recent values"
      },
      {
        "name": "median",
        "relation": "alternative", 
        "description": "Moving median - more robust to outliers"
      }
    ],
    "version": "1.2.0",
    "added_in": "1.0.0",
    "last_modified": "2024-02-15"
  }
}
```

### Mathematical Function Example
```json
{
  "status": "success",
  "function": {
    "name": "scale",
    "display_name": "Scale Values",
    "category": "mathematical",
    "description": "Multiplies all input values by a specified scaling factor. Useful for unit conversions, normalization, or applying linear transformations to time series data.",
    "input_types": ["float", "integer"],
    "output_type": "float",
    "parameters": [
      {
        "name": "factor",
        "type": "float",
        "required": true,
        "default": null,
        "description": "The scaling factor to multiply all values by",
        "constraints": {
          "min": -1000000,
          "max": 1000000
        },
        "examples": [1.8, 0.001, 100, -1]
      }
    ],
    "examples": [
      {
        "description": "Convert Celsius to Fahrenheit",
        "query": "scale(temperature(celsius), factor=1.8)",
        "explanation": "Multiplies Celsius values by 1.8 (first step of C to F conversion)"
      },
      {
        "description": "Convert bytes to megabytes",
        "query": "scale(memory(bytes_used), factor=0.000001)",
        "explanation": "Converts byte values to megabytes by dividing by 1,000,000"
      },
      {
        "description": "Invert signal",
        "query": "scale(signal(amplitude), factor=-1)",
        "explanation": "Inverts the signal by multiplying by -1"
      }
    ],
    "performance": {
      "complexity": "O(n)",
      "memory_usage": "O(1)",
      "performance_tier": "fast",
      "typical_execution_time_ms": 2.1,
      "max_input_size": 10000000
    },
    "implementation": {
      "algorithm": "Direct multiplication",
      "precision": "double",
      "handles_missing_values": true,
      "missing_value_strategy": "preserve",
      "thread_safe": true,
      "special_cases": {
        "infinity": "Preserves infinity values",
        "nan": "Preserves NaN values",
        "zero_factor": "Returns zero for all values"
      }
    },
    "related_functions": [
      {
        "name": "offset",
        "relation": "complementary",
        "description": "Adds a constant value - often used together with scale"
      },
      {
        "name": "normalize",
        "relation": "alternative",
        "description": "Scales to 0-1 range automatically"
      }
    ],
    "version": "1.0.0",
    "added_in": "1.0.0",
    "last_modified": "2024-01-10"
  }
}
```

### Complex Function Example
```json
{
  "status": "success",
  "function": {
    "name": "derivative",
    "display_name": "Derivative (Rate of Change)",
    "category": "transformation",
    "description": "Calculates the derivative (rate of change) of time series data. Useful for detecting trends, acceleration, and changes in behavior over time.",
    "input_types": ["float", "integer"],
    "output_type": "float",
    "parameters": [
      {
        "name": "method",
        "type": "string",
        "required": false,
        "default": "forward",
        "description": "Differentiation method to use",
        "constraints": {
          "enum": ["forward", "backward", "central"]
        },
        "examples": ["forward", "central"]
      },
      {
        "name": "time_unit",
        "type": "string", 
        "required": false,
        "default": "seconds",
        "description": "Time unit for rate calculation",
        "constraints": {
          "enum": ["nanoseconds", "microseconds", "milliseconds", "seconds", "minutes", "hours"]
        },
        "examples": ["seconds", "minutes"]
      }
    ],
    "examples": [
      {
        "description": "Rate of temperature change per minute",
        "query": "derivative(temperature(value), method=forward, time_unit=minutes)",
        "explanation": "Calculates how fast temperature is changing per minute"
      },
      {
        "description": "CPU usage acceleration",
        "query": "derivative(cpu(usage_percent), method=central)",
        "explanation": "Shows rate of change in CPU usage using central difference"
      }
    ],
    "performance": {
      "complexity": "O(n)",
      "memory_usage": "O(1)",
      "performance_tier": "fast",
      "typical_execution_time_ms": 8.3,
      "max_input_size": 5000000
    },
    "implementation": {
      "algorithm": "Finite difference methods",
      "precision": "double",
      "handles_missing_values": true,
      "missing_value_strategy": "interpolate",
      "thread_safe": true,
      "numerical_stability": "Uses robust finite difference formulas to minimize numerical errors"
    },
    "related_functions": [
      {
        "name": "integral",
        "relation": "inverse",
        "description": "Opposite operation - integrates to get cumulative values"
      },
      {
        "name": "diff",
        "relation": "similar",
        "description": "Simple difference between consecutive points"
      }
    ],
    "mathematical_formula": "f'(x) ≈ (f(x+h) - f(x)) / h",
    "version": "1.1.0",
    "added_in": "1.0.0", 
    "last_modified": "2024-03-01"
  }
}
```

### Function Not Found Response
```json
{
  "status": "error",
  "error": "Function 'unknown_function' not found",
  "available_functions": [
    "sma", "ema", "scale", "offset", "derivative", "integral"
  ]
}
```

## Usage Examples

### Get Function Details
```bash
# Get SMA function details
curl -s "http://localhost:8086/functions/sma" | jq

# Get Scale function details  
curl -s "http://localhost:8086/functions/scale" | jq '.function | {name, description, parameters}'

# Get Derivative function with examples
curl -s "http://localhost:8086/functions/derivative" | jq '.function.examples'
```

### Extract Specific Information
```bash
# Get function parameters only
curl -s "http://localhost:8086/functions/sma" | jq '.function.parameters'

# Get performance characteristics
curl -s "http://localhost:8086/functions/derivative" | jq '.function.performance'

# Get usage examples
curl -s "http://localhost:8086/functions/scale" | jq '.function.examples[].query'
```

## Python Client Example

```python
import requests
import json
from typing import Dict, Any, Optional, List

class TSDBFunctionDetailsClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_function_details(self, function_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific function."""
        response = requests.get(f"{self.base_url}/functions/{function_name}")
        return response.json()
    
    def get_function_parameters(self, function_name: str) -> List[Dict[str, Any]]:
        """Get parameter information for a function."""
        result = self.get_function_details(function_name)
        
        if result.get("status") != "success":
            return []
        
        return result.get("function", {}).get("parameters", [])
    
    def get_function_examples(self, function_name: str) -> List[Dict[str, Any]]:
        """Get usage examples for a function."""
        result = self.get_function_details(function_name)
        
        if result.get("status") != "success":
            return []
        
        return result.get("function", {}).get("examples", [])
    
    def get_function_performance(self, function_name: str) -> Dict[str, Any]:
        """Get performance characteristics of a function."""
        result = self.get_function_details(function_name)
        
        if result.get("status") != "success":
            return {}
        
        return result.get("function", {}).get("performance", {})
    
    def validate_function_exists(self, function_name: str) -> bool:
        """Check if a function exists."""
        result = self.get_function_details(function_name)
        return result.get("status") == "success"
    
    def get_related_functions(self, function_name: str) -> List[Dict[str, Any]]:
        """Get functions related to the specified function."""
        result = self.get_function_details(function_name)
        
        if result.get("status") != "success":
            return []
        
        return result.get("function", {}).get("related_functions", [])
    
    def get_parameter_constraints(self, function_name: str, parameter_name: str) -> Dict[str, Any]:
        """Get constraints for a specific parameter."""
        parameters = self.get_function_parameters(function_name)
        
        for param in parameters:
            if param.get("name") == parameter_name:
                return param.get("constraints", {})
        
        return {}
    
    def build_function_signature(self, function_name: str) -> str:
        """Build a function signature string showing parameter types."""
        result = self.get_function_details(function_name)
        
        if result.get("status") != "success":
            return f"{function_name}(?)"
        
        func_info = result["function"]
        parameters = func_info.get("parameters", [])
        
        param_parts = []
        for param in parameters:
            param_name = param.get("name", "")
            param_type = param.get("type", "unknown")
            required = param.get("required", False)
            default = param.get("default")
            
            param_str = f"{param_name}: {param_type}"
            if not required:
                if default is not None:
                    param_str += f" = {default}"
                else:
                    param_str = f"[{param_str}]"
            
            param_parts.append(param_str)
        
        input_types = "/".join(func_info.get("input_types", ["unknown"]))
        output_type = func_info.get("output_type", "unknown")
        
        signature = f"{function_name}(data: {input_types}"
        if param_parts:
            signature += f", {', '.join(param_parts)}"
        signature += f") -> {output_type}"
        
        return signature
    
    def generate_function_documentation(self, function_name: str) -> str:
        """Generate formatted documentation for a function."""
        result = self.get_function_details(function_name)
        
        if result.get("status") != "success":
            return f"Function '{function_name}' not found."
        
        func_info = result["function"]
        
        doc_lines = [
            f"# {func_info.get('display_name', function_name)}",
            "",
            f"**Category**: {func_info.get('category', 'unknown')}",
            f"**Input Types**: {', '.join(func_info.get('input_types', []))}",
            f"**Output Type**: {func_info.get('output_type', 'unknown')}",
            "",
            "## Description",
            func_info.get('description', 'No description available.'),
            "",
            "## Parameters"
        ]
        
        parameters = func_info.get("parameters", [])
        if parameters:
            for param in parameters:
                param_name = param.get("name", "")
                param_type = param.get("type", "unknown")
                required = "**Required**" if param.get("required", False) else "Optional"
                param_desc = param.get("description", "No description")
                
                doc_lines.extend([
                    f"- **{param_name}** ({param_type}) - {required}",
                    f"  {param_desc}",
                    ""
                ])
        else:
            doc_lines.append("No parameters required.")
        
        # Add examples
        examples = func_info.get("examples", [])
        if examples:
            doc_lines.extend(["", "## Examples"])
            for i, example in enumerate(examples):
                doc_lines.extend([
                    f"### Example {i+1}: {example.get('description', 'Usage')}",
                    f"```",
                    f"{example.get('query', 'No query provided')}",
                    f"```",
                    f"{example.get('explanation', '')}",
                    ""
                ])
        
        # Add performance info
        performance = func_info.get("performance", {})
        if performance:
            doc_lines.extend([
                "## Performance",
                f"- **Complexity**: {performance.get('complexity', 'Unknown')}",
                f"- **Memory Usage**: {performance.get('memory_usage', 'Unknown')}",
                f"- **Performance Tier**: {performance.get('performance_tier', 'Unknown')}",
                ""
            ])
        
        return "\n".join(doc_lines)

# Usage Examples
client = TSDBFunctionDetailsClient()

# Get detailed information about SMA function
sma_details = client.get_function_details("sma")
if sma_details.get("status") == "success":
    func = sma_details["function"]
    print(f"Function: {func['display_name']}")
    print(f"Description: {func['description']}")
    print(f"Category: {func['category']}")
    print(f"Input types: {', '.join(func['input_types'])}")
    print(f"Output type: {func['output_type']}")
else:
    print(f"Error: {sma_details.get('error', 'Unknown error')}")

# Get function parameters
params = client.get_function_parameters("sma")
print(f"\nSMA Parameters:")
for param in params:
    required = " (required)" if param.get("required") else " (optional)"
    print(f"  - {param['name']}: {param['type']}{required}")
    print(f"    {param.get('description', 'No description')}")

# Get usage examples
examples = client.get_function_examples("scale")
print(f"\nScale Function Examples:")
for i, example in enumerate(examples):
    print(f"  {i+1}. {example.get('description', 'Example')}")
    print(f"     Query: {example.get('query', 'No query')}")

# Get performance information
performance = client.get_function_performance("derivative")
print(f"\nDerivative Performance:")
for key, value in performance.items():
    print(f"  {key}: {value}")

# Check if function exists
if client.validate_function_exists("sma"):
    print("\n✓ SMA function is available")
else:
    print("\n✗ SMA function not found")

# Get related functions
related = client.get_related_functions("sma")
print(f"\nFunctions related to SMA:")
for func in related:
    print(f"  - {func.get('name', 'unknown')}: {func.get('description', 'No description')}")

# Build function signature
signature = client.build_function_signature("sma")
print(f"\nSMA Function Signature:")
print(f"  {signature}")

# Generate complete documentation
documentation = client.generate_function_documentation("scale")
print(f"\nGenerated Documentation for Scale Function:")
print(documentation)
```

## Advanced Use Cases

### Function Comparison Tool
```python
def compare_functions(client, function_names: List[str]) -> Dict[str, Any]:
    """Compare multiple functions side by side."""
    comparison = {
        "functions": {},
        "comparison_matrix": {}
    }
    
    for func_name in function_names:
        details = client.get_function_details(func_name)
        if details.get("status") == "success":
            comparison["functions"][func_name] = details["function"]
    
    # Create comparison matrix
    comparison_aspects = ["category", "input_types", "output_type", "complexity", "performance_tier"]
    
    for aspect in comparison_aspects:
        comparison["comparison_matrix"][aspect] = {}
        for func_name, func_info in comparison["functions"].items():
            if aspect in ["complexity", "performance_tier"]:
                value = func_info.get("performance", {}).get(aspect, "unknown")
            else:
                value = func_info.get(aspect, "unknown")
            comparison["comparison_matrix"][aspect][func_name] = value
    
    return comparison

# Usage
comparison = compare_functions(client, ["sma", "ema", "median"])
print("Function Comparison:")
for aspect, values in comparison["comparison_matrix"].items():
    print(f"\n{aspect.title()}:")
    for func_name, value in values.items():
        print(f"  {func_name}: {value}")
```

### Parameter Validation Helper
```python
def validate_function_call(client, function_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a function call before execution."""
    func_details = client.get_function_details(function_name)
    
    if func_details.get("status") != "success":
        return {
            "valid": False,
            "error": f"Function '{function_name}' not found"
        }
    
    func_params = {p["name"]: p for p in func_details["function"].get("parameters", [])}
    validation_result = {"valid": True, "warnings": [], "suggestions": []}
    
    # Check required parameters
    for param_name, param_info in func_params.items():
        if param_info.get("required", False) and param_name not in parameters:
            validation_result["valid"] = False
            validation_result["error"] = f"Missing required parameter: {param_name}"
            return validation_result
    
    # Validate provided parameters
    for param_name, param_value in parameters.items():
        if param_name not in func_params:
            validation_result["warnings"].append(f"Unknown parameter: {param_name}")
            continue
        
        param_info = func_params[param_name]
        constraints = param_info.get("constraints", {})
        
        # Check type
        expected_type = param_info.get("type", "")
        if expected_type == "integer" and not isinstance(param_value, int):
            validation_result["valid"] = False
            validation_result["error"] = f"Parameter {param_name} must be integer, got {type(param_value).__name__}"
            return validation_result
        
        # Check constraints
        if "min" in constraints and param_value < constraints["min"]:
            validation_result["valid"] = False
            validation_result["error"] = f"Parameter {param_name} below minimum {constraints['min']}"
            return validation_result
        
        if "max" in constraints and param_value > constraints["max"]:
            validation_result["valid"] = False
            validation_result["error"] = f"Parameter {param_name} exceeds maximum {constraints['max']}"
            return validation_result
        
        if "enum" in constraints and param_value not in constraints["enum"]:
            validation_result["valid"] = False
            validation_result["error"] = f"Parameter {param_name} must be one of: {constraints['enum']}"
            return validation_result
        
        # Add suggestions based on examples
        examples = param_info.get("examples", [])
        if examples and param_value not in examples:
            validation_result["suggestions"].append(f"Consider using common values for {param_name}: {examples}")
    
    return validation_result

# Usage
validation = validate_function_call(client, "sma", {"window": 5})
if validation["valid"]:
    print("✓ Function call is valid")
    for suggestion in validation.get("suggestions", []):
        print(f"💡 {suggestion}")
else:
    print(f"✗ Invalid function call: {validation['error']}")
```

### Function Documentation Generator
```python
def generate_api_docs(client, output_file: str):
    """Generate complete API documentation for all functions."""
    # This would integrate with the functions list endpoint
    # to get all function names, then generate docs for each
    
    function_names = ["sma", "ema", "scale", "offset", "derivative"]  # Example list
    
    with open(output_file, 'w') as f:
        f.write("# TSDB Functions API Documentation\n\n")
        
        for func_name in function_names:
            documentation = client.generate_function_documentation(func_name)
            f.write(documentation)
            f.write("\n---\n\n")
    
    print(f"Documentation generated: {output_file}")

# Usage
generate_api_docs(client, "tsdb_functions_reference.md")
```

## Error Handling

### Common Error Scenarios
- **Function Not Found**: The specified function name doesn't exist
- **Invalid Function Name**: Malformed or empty function name in the URL

### Error Response Examples
```json
{
  "status": "error",
  "error": "Function 'nonexistent' not found",
  "available_functions": ["sma", "ema", "scale", "offset"]
}
```

### Robust Client Implementation
```python
def get_function_safely(client, function_name: str):
    """Get function details with comprehensive error handling."""
    try:
        result = client.get_function_details(function_name)
        
        if result.get("status") != "success":
            print(f"Function '{function_name}' not found")
            available = result.get("available_functions", [])
            if available:
                print(f"Available functions: {', '.join(available)}")
            return None
        
        return result["function"]
        
    except requests.exceptions.ConnectionError:
        print("Failed to connect to TSDB server")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
```

## Related Endpoints

- [`GET /functions`](functions-endpoint.md) - List all available functions
- [`POST /functions/validate`](functions-validate-endpoint.md) - Validate function parameters
- [`GET /functions/performance`](functions-performance-endpoint.md) - Get function execution statistics
- [`GET /functions/cache`](functions-cache-endpoint.md) - Get function cache statistics
- [`POST /query/functions`](query-functions-endpoint.md) - Execute function queries
- [`POST /query/parse`](query-parse-endpoint.md) - Parse function queries