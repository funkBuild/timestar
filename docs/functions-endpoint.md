# GET /functions - List Available Time Series Functions

## Overview
The functions endpoint provides discovery of all available time series analysis functions in the TSDB. These functions enable advanced data processing, mathematical operations, and analytical transformations on time series data during queries.

## Endpoint Details
- **URL**: `/functions`
- **Method**: `GET`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Request
```bash
curl -X GET http://localhost:8086/functions
```

### Query Parameters
- **category**: Filter functions by category (mathematical, statistical, transformation, etc.)
- **type**: Filter by input data type compatibility (float, integer, boolean, string)
- **search**: Search functions by name or description
- **limit**: Maximum number of results to return (default: 100)
- **offset**: Number of results to skip for pagination (default: 0)

### Request Examples
```bash
# Get all available functions
curl "http://localhost:8086/functions"

# Get mathematical functions only
curl "http://localhost:8086/functions?category=mathematical"

# Get functions compatible with float data
curl "http://localhost:8086/functions?type=float"

# Search for moving average functions
curl "http://localhost:8086/functions?search=average"

# Paginated request
curl "http://localhost:8086/functions?limit=10&offset=20"
```

## Response Format

### Complete Functions List
```json
{
  "status": "success",
  "functions": [
    {
      "name": "sma",
      "display_name": "Simple Moving Average",
      "category": "statistical",
      "description": "Calculates simple moving average over a specified window",
      "input_types": ["float", "integer"],
      "output_type": "float",
      "parameters": [
        {
          "name": "window",
          "type": "integer",
          "required": true,
          "default": null,
          "description": "Size of the moving window",
          "constraints": {"min": 1, "max": 1000}
        }
      ],
      "examples": [
        "sma(temperature(value), window=5)",
        "sma(cpu(usage_percent){host:server-01}, window=10)"
      ]
    },
    {
      "name": "scale",
      "display_name": "Scale Values",
      "category": "mathematical",
      "description": "Multiplies all values by a scaling factor",
      "input_types": ["float", "integer"],
      "output_type": "float",
      "parameters": [
        {
          "name": "factor",
          "type": "float",
          "required": true,
          "default": null,
          "description": "Scaling multiplication factor",
          "constraints": {"min": -1000000, "max": 1000000}
        }
      ],
      "examples": [
        "scale(temperature(celsius), factor=1.8)",
        "scale(memory(bytes), factor=0.000001)"
      ]
    }
  ],
  "total_count": 2,
  "categories": ["mathematical", "statistical", "transformation", "time"],
  "supported_types": ["float", "integer", "boolean", "string"]
}
```

### Functions by Category
```json
{
  "status": "success",
  "category": "statistical",
  "functions": [
    {
      "name": "sma",
      "display_name": "Simple Moving Average",
      "description": "Calculates simple moving average over a specified window",
      "input_types": ["float", "integer"],
      "output_type": "float",
      "complexity": "O(n)",
      "performance_tier": "fast"
    },
    {
      "name": "ema", 
      "display_name": "Exponential Moving Average",
      "description": "Calculates exponential moving average with smoothing factor",
      "input_types": ["float", "integer"],
      "output_type": "float",
      "complexity": "O(n)",
      "performance_tier": "fast"
    },
    {
      "name": "stddev",
      "display_name": "Standard Deviation",
      "description": "Calculates rolling standard deviation",
      "input_types": ["float", "integer"],
      "output_type": "float",
      "complexity": "O(n*w)",
      "performance_tier": "medium"
    }
  ],
  "total_count": 3
}
```

### Function Categories Summary
```json
{
  "status": "success",
  "categories": {
    "mathematical": {
      "count": 8,
      "functions": ["scale", "offset", "abs", "log", "exp", "power", "sqrt", "round"],
      "description": "Basic mathematical operations and transformations"
    },
    "statistical": {
      "count": 12,
      "functions": ["sma", "ema", "stddev", "variance", "percentile", "median"],
      "description": "Statistical analysis and smoothing functions"
    },
    "transformation": {
      "count": 6,
      "functions": ["derivative", "integral", "cumsum", "diff", "normalize", "interpolate"],
      "description": "Data transformation and signal processing"
    },
    "time": {
      "count": 4,
      "functions": ["time_shift", "resample", "downsample", "align"],
      "description": "Time-based operations and resampling"
    },
    "aggregation": {
      "count": 5,
      "functions": ["group_avg", "group_sum", "group_max", "group_min", "group_count"],
      "description": "Cross-series aggregation operations"
    }
  },
  "total_functions": 35
}
```

### Search Results
```json
{
  "status": "success",
  "search_term": "average",
  "functions": [
    {
      "name": "sma",
      "display_name": "Simple Moving Average",
      "relevance_score": 0.95,
      "match_type": "display_name"
    },
    {
      "name": "ema",
      "display_name": "Exponential Moving Average", 
      "relevance_score": 0.92,
      "match_type": "display_name"
    },
    {
      "name": "group_avg",
      "display_name": "Group Average",
      "relevance_score": 0.88,
      "match_type": "display_name"
    }
  ],
  "total_count": 3
}
```

### Empty Response
```json
{
  "status": "success",
  "functions": [],
  "total_count": 0,
  "message": "No functions found matching the specified criteria"
}
```

### Error Response (400 Bad Request)
```json
{
  "status": "error",
  "error": "Invalid category specified. Available categories: mathematical, statistical, transformation, time, aggregation"
}
```

## Function Categories

### Mathematical Functions
Basic mathematical operations and transformations:
- **scale**: Multiply values by a factor
- **offset**: Add/subtract a constant value
- **abs**: Absolute value
- **log**: Logarithmic transformation
- **exp**: Exponential function
- **power**: Raise to a power
- **sqrt**: Square root
- **round**: Round to specified decimal places

### Statistical Functions
Statistical analysis and smoothing:
- **sma**: Simple moving average
- **ema**: Exponential moving average
- **stddev**: Rolling standard deviation
- **variance**: Rolling variance
- **percentile**: Rolling percentile calculation
- **median**: Rolling median
- **zscore**: Z-score normalization
- **outliers**: Outlier detection

### Transformation Functions
Data transformation and signal processing:
- **derivative**: Calculate rate of change
- **integral**: Cumulative integration
- **cumsum**: Cumulative sum
- **diff**: Difference between consecutive points
- **normalize**: Normalize to 0-1 range
- **interpolate**: Fill missing values

### Time Functions
Time-based operations:
- **time_shift**: Shift timestamps
- **resample**: Change sampling frequency
- **downsample**: Reduce data density
- **align**: Align multiple series timestamps

### Aggregation Functions
Cross-series operations:
- **group_avg**: Average across series
- **group_sum**: Sum across series
- **group_max**: Maximum across series
- **group_min**: Minimum across series
- **group_count**: Count non-null values

## Usage Examples

### Discover All Functions
```bash
# List all available functions
curl -s http://localhost:8086/functions | jq '.functions[] | .name'

# Get functions with details
curl -s http://localhost:8086/functions | jq '.functions[] | {name: .name, category: .category, description: .description}'
```

### Category-based Discovery
```bash
# Get mathematical functions
curl -s "http://localhost:8086/functions?category=mathematical" | jq

# Get statistical functions
curl -s "http://localhost:8086/functions?category=statistical" | jq
```

### Type Compatibility
```bash
# Get functions that work with float data
curl -s "http://localhost:8086/functions?type=float" | jq '.functions[].name'

# Get functions that work with integer data  
curl -s "http://localhost:8086/functions?type=integer" | jq '.functions[].name'
```

### Search Functions
```bash
# Search for moving average functions
curl -s "http://localhost:8086/functions?search=moving" | jq

# Search for statistical functions
curl -s "http://localhost:8086/functions?search=deviation" | jq
```

## Python Client Example

```python
import requests
import json
from typing import List, Dict, Any, Optional
from collections import defaultdict

class TSDBFunctionsClient:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def get_functions(self, category: Optional[str] = None,
                     data_type: Optional[str] = None,
                     search: Optional[str] = None,
                     limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """Get available functions with optional filtering."""
        params = {}
        if category:
            params['category'] = category
        if data_type:
            params['type'] = data_type
        if search:
            params['search'] = search
        if limit != 100:
            params['limit'] = limit
        if offset > 0:
            params['offset'] = offset
        
        response = requests.get(f"{self.base_url}/functions", params=params)
        return response.json()
    
    def get_all_functions(self) -> List[Dict[str, Any]]:
        """Get all functions with automatic pagination."""
        all_functions = []
        offset = 0
        limit = 50
        
        while True:
            result = self.get_functions(limit=limit, offset=offset)
            
            if result.get("status") != "success":
                raise Exception(f"Failed to fetch functions: {result}")
            
            functions = result.get("functions", [])
            all_functions.extend(functions)
            
            if len(functions) < limit:  # No more data
                break
                
            offset += limit
        
        return all_functions
    
    def get_function_categories(self) -> Dict[str, List[str]]:
        """Get functions organized by category."""
        all_functions = self.get_all_functions()
        
        categories = defaultdict(list)
        for func in all_functions:
            category = func.get("category", "uncategorized")
            function_name = func.get("name", "")
            categories[category].append(function_name)
        
        return dict(categories)
    
    def find_compatible_functions(self, data_type: str) -> List[Dict[str, Any]]:
        """Find functions compatible with a specific data type."""
        all_functions = self.get_all_functions()
        
        compatible = []
        for func in all_functions:
            input_types = func.get("input_types", [])
            if data_type in input_types:
                compatible.append(func)
        
        return compatible
    
    def search_functions(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Search functions by name, description, or category."""
        result = self.get_functions(search=query, limit=limit)
        
        if result.get("status") != "success":
            return []
        
        return result.get("functions", [])
    
    def get_function_by_category(self, category: str) -> List[Dict[str, Any]]:
        """Get all functions in a specific category."""
        result = self.get_functions(category=category)
        
        if result.get("status") != "success":
            return []
        
        return result.get("functions", [])
    
    def build_function_index(self) -> Dict[str, Any]:
        """Build a searchable index of all functions."""
        all_functions = self.get_all_functions()
        
        index = {
            "by_name": {},
            "by_category": defaultdict(list),
            "by_input_type": defaultdict(list),
            "by_output_type": defaultdict(list),
            "search_terms": defaultdict(list)
        }
        
        for func in all_functions:
            name = func.get("name", "")
            category = func.get("category", "")
            input_types = func.get("input_types", [])
            output_type = func.get("output_type", "")
            description = func.get("description", "").lower()
            
            # Index by name
            index["by_name"][name] = func
            
            # Index by category
            index["by_category"][category].append(name)
            
            # Index by input types
            for input_type in input_types:
                index["by_input_type"][input_type].append(name)
            
            # Index by output type
            if output_type:
                index["by_output_type"][output_type].append(name)
            
            # Build search terms index
            search_terms = [name, category] + description.split()
            for term in search_terms:
                if len(term) > 2:  # Skip very short terms
                    index["search_terms"][term].append(name)
        
        # Convert defaultdicts to regular dicts
        for key in ["by_category", "by_input_type", "by_output_type", "search_terms"]:
            index[key] = dict(index[key])
        
        return index
    
    def suggest_functions_for_analysis(self, analysis_type: str) -> List[Dict[str, Any]]:
        """Suggest functions for common analysis patterns."""
        suggestions = {
            "smoothing": ["sma", "ema", "median"],
            "trend_analysis": ["derivative", "sma", "linear_regression"],
            "anomaly_detection": ["zscore", "outliers", "stddev"],
            "aggregation": ["group_avg", "group_sum", "group_max", "group_min"],
            "transformation": ["scale", "offset", "normalize", "log"],
            "resampling": ["resample", "downsample", "interpolate"]
        }
        
        if analysis_type not in suggestions:
            return []
        
        suggested_names = suggestions[analysis_type]
        all_functions = self.get_all_functions()
        
        return [func for func in all_functions if func.get("name") in suggested_names]

# Usage Examples
client = TSDBFunctionsClient()

# Get all available functions
all_functions = client.get_all_functions()
print(f"Found {len(all_functions)} functions:")
for func in all_functions[:5]:  # Show first 5
    print(f"  - {func['name']}: {func.get('description', 'No description')}")

# Get functions by category
categories = client.get_function_categories()
print(f"\nFunctions by category:")
for category, function_names in categories.items():
    print(f"  {category}: {len(function_names)} functions")
    print(f"    {', '.join(function_names[:3])}{'...' if len(function_names) > 3 else ''}")

# Find functions compatible with float data
float_functions = client.find_compatible_functions("float")
print(f"\nFunctions compatible with float data:")
for func in float_functions[:5]:
    print(f"  - {func['name']} ({func['category']})")

# Search for moving average functions
search_results = client.search_functions("average")
print(f"\nFunctions matching 'average':")
for func in search_results:
    print(f"  - {func['name']}: {func.get('display_name', func['name'])}")

# Get statistical functions
statistical_functions = client.get_function_by_category("statistical")
print(f"\nStatistical functions:")
for func in statistical_functions:
    params = [p['name'] for p in func.get('parameters', [])]
    print(f"  - {func['name']}({', '.join(params)}): {func.get('description', '')}")

# Build function index for advanced searching
index = client.build_function_index()
print(f"\nFunction index created:")
print(f"  - {len(index['by_name'])} functions indexed by name")
print(f"  - {len(index['by_category'])} categories")
print(f"  - {len(index['by_input_type'])} input types")
print(f"  - {len(index['search_terms'])} search terms")

# Get suggestions for smoothing analysis
smoothing_functions = client.suggest_functions_for_analysis("smoothing")
print(f"\nSuggested functions for smoothing:")
for func in smoothing_functions:
    print(f"  - {func['name']}: {func.get('description', '')}")
```

## Function Usage Integration

### Query Builder Integration
```python
def build_function_query(functions_client, measurement: str, field: str, 
                        analysis_type: str) -> str:
    """Build a function query for common analysis patterns."""
    # Get suggested functions
    suggested_functions = functions_client.suggest_functions_for_analysis(analysis_type)
    
    if not suggested_functions:
        return f"avg:{measurement}({field})"
    
    # Use the first suggested function
    func = suggested_functions[0]
    func_name = func["name"]
    
    # Build parameter string based on common defaults
    params = func.get("parameters", [])
    param_str = ""
    
    if params:
        param_parts = []
        for param in params:
            if param.get("required", False):
                # Use reasonable defaults for common parameters
                if param["name"] == "window":
                    param_parts.append("window=10")
                elif param["name"] == "factor":
                    param_parts.append("factor=1.0")
                elif param["name"] == "threshold":
                    param_parts.append("threshold=2.0")
        
        if param_parts:
            param_str = ", " + ", ".join(param_parts)
    
    # Build the complete query
    query = f"{func_name}({measurement}({field}){param_str})"
    return query

# Usage
query = build_function_query(client, "cpu", "usage_percent", "smoothing")
print(f"Generated query: {query}")
```

### Pipeline Builder
```python
def build_analysis_pipeline(functions_client, steps: List[str]) -> List[Dict[str, Any]]:
    """Build a multi-step analysis pipeline using available functions."""
    pipeline = []
    all_functions = {f["name"]: f for f in functions_client.get_all_functions()}
    
    for step in steps:
        if step in all_functions:
            func_info = all_functions[step]
            pipeline_step = {
                "function": step,
                "parameters": {},
                "input_type": func_info.get("input_types", [])[0] if func_info.get("input_types") else "float",
                "output_type": func_info.get("output_type", "float")
            }
            
            # Add default parameters for common functions
            for param in func_info.get("parameters", []):
                if param.get("default") is not None:
                    pipeline_step["parameters"][param["name"]] = param["default"]
                elif param.get("required", False):
                    # Set reasonable defaults
                    if param["name"] == "window":
                        pipeline_step["parameters"]["window"] = 10
                    elif param["name"] == "factor":
                        pipeline_step["parameters"]["factor"] = 1.0
            
            pipeline.append(pipeline_step)
    
    return pipeline

# Usage
pipeline = build_analysis_pipeline(client, ["sma", "derivative", "abs"])
print("Analysis pipeline:")
for i, step in enumerate(pipeline):
    print(f"  {i+1}. {step['function']} -> {step['output_type']}")
```

## Advanced Use Cases

### Function Compatibility Analysis
```python
def analyze_function_compatibility(functions_client):
    """Analyze type compatibility across functions."""
    all_functions = functions_client.get_all_functions()
    
    compatibility_matrix = defaultdict(lambda: defaultdict(list))
    
    for func in all_functions:
        name = func.get("name", "")
        input_types = func.get("input_types", [])
        output_type = func.get("output_type", "")
        
        for input_type in input_types:
            compatibility_matrix[input_type][output_type].append(name)
    
    return dict(compatibility_matrix)

# Usage
compatibility = analyze_function_compatibility(client)
print("Function type compatibility:")
for input_type, outputs in compatibility.items():
    print(f"  {input_type} input:")
    for output_type, functions in outputs.items():
        print(f"    -> {output_type}: {len(functions)} functions")
```

### Performance Analysis
```python
def analyze_function_performance(functions_client):
    """Analyze function performance characteristics."""
    all_functions = functions_client.get_all_functions()
    
    performance_analysis = {
        "fast": [],
        "medium": [],
        "slow": [],
        "complexity": defaultdict(list)
    }
    
    for func in all_functions:
        name = func.get("name", "")
        performance_tier = func.get("performance_tier", "medium")
        complexity = func.get("complexity", "O(n)")
        
        performance_analysis[performance_tier].append(name)
        performance_analysis["complexity"][complexity].append(name)
    
    return performance_analysis

# Usage
perf_analysis = analyze_function_performance(client)
print("Function performance analysis:")
for tier, functions in perf_analysis.items():
    if tier != "complexity":
        print(f"  {tier}: {len(functions)} functions")
```

## Error Handling and Validation

### Function Validation
```python
def validate_function_usage(functions_client, function_name: str, 
                          parameters: Dict[str, Any], input_type: str) -> Dict[str, Any]:
    """Validate function usage before query execution."""
    all_functions = {f["name"]: f for f in functions_client.get_all_functions()}
    
    if function_name not in all_functions:
        return {
            "valid": False,
            "error": f"Function '{function_name}' not found"
        }
    
    func_info = all_functions[function_name]
    validation_result = {"valid": True, "warnings": []}
    
    # Check input type compatibility
    input_types = func_info.get("input_types", [])
    if input_type not in input_types:
        return {
            "valid": False,
            "error": f"Function '{function_name}' doesn't support input type '{input_type}'. Supported: {input_types}"
        }
    
    # Validate parameters
    func_params = {p["name"]: p for p in func_info.get("parameters", [])}
    
    # Check for missing required parameters
    for param_name, param_info in func_params.items():
        if param_info.get("required", False) and param_name not in parameters:
            return {
                "valid": False,
                "error": f"Required parameter '{param_name}' missing for function '{function_name}'"
            }
    
    # Check parameter constraints
    for param_name, param_value in parameters.items():
        if param_name in func_params:
            param_info = func_params[param_name]
            constraints = param_info.get("constraints", {})
            
            # Check min/max constraints
            if "min" in constraints and param_value < constraints["min"]:
                return {
                    "valid": False,
                    "error": f"Parameter '{param_name}' value {param_value} below minimum {constraints['min']}"
                }
            
            if "max" in constraints and param_value > constraints["max"]:
                return {
                    "valid": False,
                    "error": f"Parameter '{param_name}' value {param_value} exceeds maximum {constraints['max']}"
                }
        else:
            validation_result["warnings"].append(f"Unknown parameter '{param_name}' for function '{function_name}'")
    
    return validation_result

# Usage
validation = validate_function_usage(client, "sma", {"window": 5}, "float")
if validation["valid"]:
    print("✓ Function usage is valid")
    if validation["warnings"]:
        print("Warnings:", validation["warnings"])
else:
    print("✗ Function usage invalid:", validation["error"])
```

## Related Endpoints

- [`GET /functions/{name}`](functions-name-endpoint.md) - Get detailed information about a specific function
- [`POST /functions/validate`](functions-validate-endpoint.md) - Validate function parameters before execution
- [`GET /functions/performance`](functions-performance-endpoint.md) - Get function execution performance statistics
- [`GET /functions/cache`](functions-cache-endpoint.md) - Get function cache statistics and management
- [`POST /query/functions`](query-functions-endpoint.md) - Execute function queries on time series data
- [`POST /query/parse`](query-parse-endpoint.md) - Parse and validate function query syntax