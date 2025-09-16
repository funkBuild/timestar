# POST /query/parse - Parse and Validate Function Query Syntax

## Overview
The query parse endpoint validates and analyzes query syntax before execution, providing detailed parsing information, syntax validation, optimization suggestions, and performance estimates. This is essential for query builders, debugging malformed queries, and optimizing query performance.

## Endpoint Details
- **URL**: `/query/parse`
- **Method**: `POST`
- **Content-Type**: `application/json`
- **Authentication**: None required

## Request Format

### Basic Query Parsing
```json
{
  "query": "sma(temperature(value){location:datacenter}, window=10)"
}
```

### Multiple Query Parsing
```json
{
  "queries": [
    "sma(temperature(value){location:datacenter}, window=10)",
    "scale(cpu(usage_percent){host:server-01}, factor=1.0)",
    "derivative(network(bytes_sent), method=central)"
  ]
}
```

### Query Parsing with Context
```json
{
  "query": "sma(temperature(value){location:datacenter}, window=10)",
  "context": {
    "measurement_exists": true,
    "field_exists": true,
    "expected_data_type": "float",
    "estimated_data_points": 1000
  }
}
```

### Query Validation Options
```json
{
  "query": "sma(temperature(value){location:datacenter}, window=10)",
  "validation_options": {
    "check_function_exists": true,
    "validate_parameters": true,
    "check_data_compatibility": true,
    "estimate_performance": true,
    "suggest_optimizations": true
  }
}
```

## Request Fields

### Single Query Parsing
- **query**: Query string to parse and validate (required)
- **context**: Additional context for validation (optional)
- **validation_options**: Parsing and validation options (optional)

### Multiple Query Parsing
- **queries**: Array of query strings to parse (required)
- **validation_options**: Parsing options applied to all queries (optional)

## Response Format

### Successful Query Parse
```json
{
  "status": "success",
  "query": "sma(temperature(value){location:datacenter}, window=10)",
  "parse_result": {
    "valid": true,
    "syntax_valid": true,
    "semantics_valid": true,
    "parsed_components": {
      "function": "sma",
      "measurement": "temperature",
      "field": "value",
      "tags": {
        "location": "datacenter"
      },
      "parameters": {
        "window": 10
      },
      "parameter_types": {
        "window": "integer"
      }
    },
    "function_chain": [
      {
        "function": "sma",
        "input_type": "float",
        "output_type": "float",
        "parameters": {
          "window": 10
        }
      }
    ],
    "query_type": "single_function",
    "complexity_level": "simple"
  },
  "validation_results": {
    "function_validation": {
      "function_exists": true,
      "parameters_valid": true,
      "parameter_constraints_met": true,
      "input_type_compatible": true
    },
    "data_validation": {
      "measurement_accessible": true,
      "field_accessible": true,
      "tags_valid": true,
      "estimated_series_count": 1
    }
  },
  "performance_estimate": {
    "estimated_execution_time_ms": 12.5,
    "memory_usage_estimate": "low",
    "cpu_intensity": "low",
    "complexity_score": 2.1,
    "cache_likelihood": 0.87
  },
  "optimization_suggestions": [
    "Query is well-optimized for the given parameters",
    "Consider caching if this query will be repeated"
  ],
  "execution_plan": {
    "steps": [
      {
        "step": 1,
        "operation": "data_retrieval",
        "description": "Fetch temperature.value data with location:datacenter filter",
        "estimated_time_ms": 8.2
      },
      {
        "step": 2,
        "operation": "function_execution",
        "description": "Apply SMA function with window=10",
        "estimated_time_ms": 4.3
      }
    ],
    "parallel_execution_possible": false,
    "bottleneck": "data_retrieval"
  }
}
```

### Complex Query Parse
```json
{
  "status": "success",
  "query": "derivative(sma(cpu(usage_percent){host:server-01}, window=5), method=central)",
  "parse_result": {
    "valid": true,
    "syntax_valid": true,
    "semantics_valid": true,
    "parsed_components": {
      "function": "derivative",
      "nested_functions": [
        {
          "function": "sma",
          "measurement": "cpu",
          "field": "usage_percent",
          "tags": {
            "host": "server-01"
          },
          "parameters": {
            "window": 5
          }
        }
      ],
      "parameters": {
        "method": "central"
      }
    },
    "function_chain": [
      {
        "function": "sma",
        "input_type": "float",
        "output_type": "float",
        "parameters": {"window": 5}
      },
      {
        "function": "derivative",
        "input_type": "float", 
        "output_type": "float",
        "parameters": {"method": "central"}
      }
    ],
    "query_type": "function_pipeline",
    "complexity_level": "medium",
    "nesting_depth": 2
  },
  "validation_results": {
    "function_validation": {
      "all_functions_exist": true,
      "parameter_compatibility": true,
      "type_chain_valid": true,
      "function_order_optimal": true
    },
    "data_validation": {
      "measurement_accessible": true,
      "field_accessible": true,
      "tags_valid": true,
      "estimated_series_count": 1
    }
  },
  "performance_estimate": {
    "estimated_execution_time_ms": 34.7,
    "memory_usage_estimate": "medium",
    "cpu_intensity": "medium",
    "complexity_score": 5.8,
    "cache_likelihood": 0.62
  },
  "optimization_suggestions": [
    "Function pipeline is well-structured",
    "Consider larger SMA window for smoother derivative calculation",
    "Pipeline can benefit from result caching"
  ]
}
```

### Query Parse Error
```json
{
  "status": "error",
  "query": "invalid_function(temperature(value), badparam=xyz)",
  "parse_result": {
    "valid": false,
    "syntax_valid": true,
    "semantics_valid": false,
    "errors": [
      {
        "type": "function_not_found",
        "message": "Function 'invalid_function' does not exist",
        "position": {
          "start": 0,
          "end": 16
        },
        "suggestions": ["sma", "ema", "derivative", "scale"]
      },
      {
        "type": "invalid_parameter",
        "message": "Parameter 'badparam' is not valid",
        "position": {
          "start": 40,
          "end": 52
        }
      }
    ],
    "partially_parsed": {
      "measurement": "temperature",
      "field": "value",
      "attempted_function": "invalid_function",
      "attempted_parameters": {
        "badparam": "xyz"
      }
    }
  },
  "suggestions": {
    "corrected_query_options": [
      "sma(temperature(value), window=10)",
      "ema(temperature(value), alpha=0.3)",
      "scale(temperature(value), factor=1.0)"
    ],
    "similar_functions": [
      {
        "name": "sma",
        "similarity": 0.4,
        "description": "Simple moving average function"
      }
    ]
  }
}
```

### Multiple Query Parse Results
```json
{
  "status": "success",
  "queries": [
    {
      "query": "sma(temperature(value), window=10)",
      "valid": true,
      "complexity_level": "simple",
      "estimated_execution_time_ms": 12.5
    },
    {
      "query": "invalid_syntax_here",
      "valid": false,
      "errors": [
        {
          "type": "syntax_error", 
          "message": "Invalid query syntax",
          "position": {"start": 0, "end": 18}
        }
      ]
    }
  ],
  "summary": {
    "total_queries": 2,
    "valid_queries": 1,
    "invalid_queries": 1,
    "avg_complexity": 2.5,
    "total_estimated_time_ms": 12.5
  }
}
```

### Query Optimization Analysis
```json
{
  "status": "success",
  "query": "sma(temperature(value){location:datacenter,sensor:outdoor,building:north}, window=50)",
  "parse_result": {
    "valid": true,
    "complexity_level": "simple"
  },
  "optimization_analysis": {
    "query_efficiency": "medium",
    "bottlenecks": [
      {
        "type": "large_window_size",
        "description": "Window size of 50 may cause performance issues",
        "impact": "medium",
        "suggestion": "Consider smaller window or use exponential moving average"
      },
      {
        "type": "multiple_tag_filters",
        "description": "Three tag filters may increase scan time",
        "impact": "low",
        "suggestion": "Ensure tags are indexed for optimal performance"
      }
    ],
    "optimizations": [
      {
        "type": "parameter_optimization",
        "original": "window=50",
        "suggested": "window=20",
        "expected_improvement": "40% faster execution",
        "trade_offs": "Slightly less smoothing"
      },
      {
        "type": "alternative_function",
        "original": "sma",
        "suggested": "ema",
        "expected_improvement": "60% faster execution",
        "trade_offs": "Different smoothing characteristics"
      }
    ]
  }
}
```

## Usage Examples

### Parse Single Query
```bash
curl -X POST http://localhost:8086/query/parse \
  -H "Content-Type: application/json" \
  -d '{
    "query": "sma(temperature(value){location:datacenter}, window=10)"
  }'
```

### Parse Multiple Queries
```bash
curl -X POST http://localhost:8086/query/parse \
  -H "Content-Type: application/json" \
  -d '{
    "queries": [
      "sma(temperature(value), window=5)",
      "scale(cpu(usage_percent), factor=1.0)",
      "derivative(network(bytes_sent), method=forward)"
    ]
  }'
```

### Parse with Validation Options
```bash
curl -X POST http://localhost:8086/query/parse \
  -H "Content-Type: application/json" \
  -d '{
    "query": "sma(temperature(value), window=10)",
    "validation_options": {
      "check_function_exists": true,
      "validate_parameters": true,
      "estimate_performance": true,
      "suggest_optimizations": true
    }
  }'
```

### Parse Complex Nested Query
```bash
curl -X POST http://localhost:8086/query/parse \
  -H "Content-Type: application/json" \
  -d '{
    "query": "derivative(sma(cpu(usage_percent){host:server-01}, window=5), method=central)"
  }'
```

## Python Client Example

```python
import requests
import json
from typing import Dict, Any, List, Optional, Union

class TSDBQueryParser:
    def __init__(self, host="localhost", port=8086):
        self.base_url = f"http://{host}:{port}"
    
    def parse_query(self, query: str, validation_options: Optional[Dict[str, bool]] = None,
                   context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Parse and validate a single query."""
        data = {"query": query}
        
        if validation_options:
            data["validation_options"] = validation_options
        if context:
            data["context"] = context
        
        response = requests.post(f"{self.base_url}/query/parse", json=data)
        return response.json()
    
    def parse_multiple_queries(self, queries: List[str], 
                              validation_options: Optional[Dict[str, bool]] = None) -> Dict[str, Any]:
        """Parse and validate multiple queries."""
        data = {"queries": queries}
        
        if validation_options:
            data["validation_options"] = validation_options
        
        response = requests.post(f"{self.base_url}/query/parse", json=data)
        return response.json()
    
    def is_query_valid(self, query: str) -> bool:
        """Simple boolean check if query is valid."""
        result = self.parse_query(query)
        
        if result.get("status") != "success":
            return False
        
        parse_result = result.get("parse_result", {})
        return parse_result.get("valid", False)
    
    def get_query_errors(self, query: str) -> List[Dict[str, Any]]:
        """Get parsing errors for a query."""
        result = self.parse_query(query)
        
        if result.get("status") != "success":
            return [{"type": "request_error", "message": result.get("error", "Unknown error")}]
        
        parse_result = result.get("parse_result", {})
        return parse_result.get("errors", [])
    
    def get_parsed_components(self, query: str) -> Dict[str, Any]:
        """Extract parsed components from a query."""
        result = self.parse_query(query)
        
        if result.get("status") != "success" or not result.get("parse_result", {}).get("valid"):
            return {}
        
        return result.get("parse_result", {}).get("parsed_components", {})
    
    def get_performance_estimate(self, query: str) -> Dict[str, Any]:
        """Get performance estimate for a query."""
        validation_options = {"estimate_performance": True}
        result = self.parse_query(query, validation_options=validation_options)
        
        if result.get("status") != "success":
            return {}
        
        return result.get("performance_estimate", {})
    
    def get_optimization_suggestions(self, query: str) -> List[str]:
        """Get optimization suggestions for a query."""
        validation_options = {"suggest_optimizations": True}
        result = self.parse_query(query, validation_options=validation_options)
        
        if result.get("status") != "success":
            return []
        
        suggestions = result.get("optimization_suggestions", [])
        
        # Also include optimization analysis if available
        optimization_analysis = result.get("optimization_analysis", {})
        optimizations = optimization_analysis.get("optimizations", [])
        
        for opt in optimizations:
            opt_type = opt.get("type", "optimization")
            suggestion = opt.get("suggested", "")
            improvement = opt.get("expected_improvement", "")
            suggestions.append(f"{opt_type}: {suggestion} - {improvement}")
        
        return suggestions
    
    def validate_query_syntax(self, query: str) -> Dict[str, Any]:
        """Comprehensive query syntax validation."""
        result = self.parse_query(query, validation_options={
            "check_function_exists": True,
            "validate_parameters": True,
            "check_data_compatibility": True
        })
        
        if result.get("status") != "success":
            return {
                "valid": False,
                "syntax_valid": False,
                "errors": [{"type": "request_error", "message": result.get("error", "Unknown error")}]
            }
        
        parse_result = result.get("parse_result", {})
        validation_results = result.get("validation_results", {})
        
        return {
            "valid": parse_result.get("valid", False),
            "syntax_valid": parse_result.get("syntax_valid", False),
            "semantics_valid": parse_result.get("semantics_valid", False),
            "errors": parse_result.get("errors", []),
            "function_validation": validation_results.get("function_validation", {}),
            "data_validation": validation_results.get("data_validation", {})
        }
    
    def analyze_query_complexity(self, query: str) -> Dict[str, Any]:
        """Analyze query complexity and structure."""
        result = self.parse_query(query)
        
        if result.get("status") != "success":
            return {"error": "Failed to parse query"}
        
        parse_result = result.get("parse_result", {})
        
        return {
            "query_type": parse_result.get("query_type", "unknown"),
            "complexity_level": parse_result.get("complexity_level", "unknown"),
            "nesting_depth": parse_result.get("nesting_depth", 0),
            "function_chain": parse_result.get("function_chain", []),
            "complexity_score": result.get("performance_estimate", {}).get("complexity_score", 0)
        }
    
    def suggest_query_corrections(self, invalid_query: str) -> List[str]:
        """Get suggestions for correcting an invalid query."""
        result = self.parse_query(invalid_query)
        
        if result.get("status") != "success":
            return []
        
        suggestions_data = result.get("suggestions", {})
        corrected_options = suggestions_data.get("corrected_query_options", [])
        
        return corrected_options
    
    def compare_query_performance(self, queries: List[str]) -> Dict[str, Any]:
        """Compare performance estimates of multiple queries."""
        validation_options = {"estimate_performance": True}
        result = self.parse_multiple_queries(queries, validation_options=validation_options)
        
        if result.get("status") != "success":
            return {"error": "Failed to parse queries"}
        
        query_results = result.get("queries", [])
        comparison = {
            "queries": [],
            "fastest_query": None,
            "slowest_query": None,
            "avg_execution_time": 0
        }
        
        total_time = 0
        valid_queries = 0
        
        for i, query_result in enumerate(query_results):
            if query_result.get("valid", False):
                exec_time = query_result.get("estimated_execution_time_ms", 0)
                complexity = query_result.get("complexity_level", "unknown")
                
                query_info = {
                    "index": i,
                    "query": queries[i] if i < len(queries) else "unknown",
                    "execution_time_ms": exec_time,
                    "complexity": complexity
                }
                
                comparison["queries"].append(query_info)
                total_time += exec_time
                valid_queries += 1
                
                # Track fastest and slowest
                if (comparison["fastest_query"] is None or 
                    exec_time < comparison["fastest_query"]["execution_time_ms"]):
                    comparison["fastest_query"] = query_info
                
                if (comparison["slowest_query"] is None or 
                    exec_time > comparison["slowest_query"]["execution_time_ms"]):
                    comparison["slowest_query"] = query_info
        
        if valid_queries > 0:
            comparison["avg_execution_time"] = total_time / valid_queries
        
        return comparison
    
    def build_execution_plan(self, query: str) -> Dict[str, Any]:
        """Get detailed execution plan for a query."""
        validation_options = {"estimate_performance": True}
        result = self.parse_query(query, validation_options=validation_options)
        
        if result.get("status") != "success":
            return {}
        
        return result.get("execution_plan", {})

# Usage Examples
parser = TSDBQueryParser()

# Parse a simple query
query = "sma(temperature(value){location:datacenter}, window=10)"
result = parser.parse_query(query)

if result.get("status") == "success":
    parse_result = result.get("parse_result", {})
    print(f"Query is valid: {parse_result.get('valid', False)}")
    print(f"Complexity: {parse_result.get('complexity_level', 'unknown')}")
    
    components = parse_result.get("parsed_components", {})
    print(f"Function: {components.get('function', 'unknown')}")
    print(f"Measurement: {components.get('measurement', 'unknown')}")
    print(f"Parameters: {components.get('parameters', {})}")
else:
    print(f"Parse failed: {result.get('error', 'Unknown error')}")

# Check if query is valid
is_valid = parser.is_query_valid("sma(temperature(value), window=5)")
print(f"\nQuery validity: {is_valid}")

# Get query errors
invalid_query = "invalid_function(data, bad_param=xyz)"
errors = parser.get_query_errors(invalid_query)
print(f"\nQuery errors:")
for error in errors:
    print(f"  {error.get('type', 'unknown')}: {error.get('message', 'no message')}")

# Get parsed components
components = parser.get_parsed_components(query)
print(f"\nParsed components:")
print(f"  Function: {components.get('function', 'unknown')}")
print(f"  Measurement: {components.get('measurement', 'unknown')}")
print(f"  Field: {components.get('field', 'unknown')}")
print(f"  Tags: {components.get('tags', {})}")
print(f"  Parameters: {components.get('parameters', {})}")

# Get performance estimate
performance = parser.get_performance_estimate(query)
print(f"\nPerformance estimate:")
print(f"  Execution time: {performance.get('estimated_execution_time_ms', 0):.1f}ms")
print(f"  Memory usage: {performance.get('memory_usage_estimate', 'unknown')}")
print(f"  Complexity score: {performance.get('complexity_score', 0):.1f}")

# Get optimization suggestions
suggestions = parser.get_optimization_suggestions(query)
print(f"\nOptimization suggestions:")
for suggestion in suggestions:
    print(f"  • {suggestion}")

# Validate query syntax comprehensively
validation = parser.validate_query_syntax(query)
print(f"\nSyntax validation:")
print(f"  Valid: {validation.get('valid', False)}")
print(f"  Syntax valid: {validation.get('syntax_valid', False)}")
print(f"  Semantics valid: {validation.get('semantics_valid', False)}")

if validation.get("errors"):
    print("  Errors:")
    for error in validation["errors"]:
        print(f"    {error.get('type', 'unknown')}: {error.get('message', 'no message')}")

# Analyze query complexity
complexity = parser.analyze_query_complexity("derivative(sma(cpu(usage), window=5), method=central)")
print(f"\nComplexity analysis:")
print(f"  Query type: {complexity.get('query_type', 'unknown')}")
print(f"  Complexity level: {complexity.get('complexity_level', 'unknown')}")
print(f"  Nesting depth: {complexity.get('nesting_depth', 0)}")
print(f"  Function chain length: {len(complexity.get('function_chain', []))}")

# Parse multiple queries
queries = [
    "sma(temperature(value), window=5)",
    "scale(cpu(usage_percent), factor=1.0)",
    "invalid_query_syntax"
]

multi_result = parser.parse_multiple_queries(queries)
print(f"\nMultiple query parsing:")
if multi_result.get("status") == "success":
    summary = multi_result.get("summary", {})
    print(f"  Valid queries: {summary.get('valid_queries', 0)}/{summary.get('total_queries', 0)}")
    print(f"  Average complexity: {summary.get('avg_complexity', 0):.1f}")
    print(f"  Total estimated time: {summary.get('total_estimated_time_ms', 0):.1f}ms")

# Compare query performance
performance_comparison = parser.compare_query_performance([
    "sma(temperature(value), window=5)",
    "sma(temperature(value), window=20)",
    "derivative(temperature(value), method=central)"
])

if "error" not in performance_comparison:
    print(f"\nPerformance comparison:")
    fastest = performance_comparison.get("fastest_query")
    slowest = performance_comparison.get("slowest_query")
    
    if fastest:
        print(f"  Fastest: {fastest['execution_time_ms']:.1f}ms - {fastest['complexity']}")
    if slowest:
        print(f"  Slowest: {slowest['execution_time_ms']:.1f}ms - {slowest['complexity']}")
    print(f"  Average: {performance_comparison.get('avg_execution_time', 0):.1f}ms")

# Get execution plan
execution_plan = parser.build_execution_plan(query)
if execution_plan:
    print(f"\nExecution plan:")
    steps = execution_plan.get("steps", [])
    for step in steps:
        step_num = step.get("step", 0)
        operation = step.get("operation", "unknown")
        description = step.get("description", "no description")
        time_ms = step.get("estimated_time_ms", 0)
        print(f"  Step {step_num}: {operation} - {description} ({time_ms:.1f}ms)")
    
    bottleneck = execution_plan.get("bottleneck", "none")
    print(f"  Bottleneck: {bottleneck}")

# Suggest corrections for invalid query
invalid_query = "invalid_func(temperature(value), badparam=xyz)"
corrections = parser.suggest_query_corrections(invalid_query)
print(f"\nSuggested corrections for invalid query:")
for correction in corrections:
    print(f"  • {correction}")
```

## Advanced Use Cases

### Query Builder Integration
```python
class QueryBuilder:
    def __init__(self, parser):
        self.parser = parser
        self.query_parts = {}
    
    def measurement(self, name):
        self.query_parts["measurement"] = name
        return self
    
    def field(self, name):
        self.query_parts["field"] = name
        return self
    
    def tags(self, **kwargs):
        self.query_parts["tags"] = kwargs
        return self
    
    def function(self, name, **params):
        self.query_parts["function"] = name
        self.query_parts["parameters"] = params
        return self
    
    def build(self):
        """Build and validate query string."""
        # Build query string from parts
        measurement = self.query_parts.get("measurement", "")
        field = self.query_parts.get("field", "")
        tags = self.query_parts.get("tags", {})
        function = self.query_parts.get("function", "")
        parameters = self.query_parts.get("parameters", {})
        
        # Build tag string
        tag_str = ""
        if tags:
            tag_conditions = [f"{k}:{v}" for k, v in tags.items()]
            tag_str = "{" + ",".join(tag_conditions) + "}"
        else:
            tag_str = "{}"
        
        # Build parameter string
        param_parts = [f"{k}={v}" for k, v in parameters.items()]
        param_str = ", ".join(param_parts)
        
        # Build complete query
        query = f"{function}({measurement}({field}){tag_str}"
        if param_str:
            query += f", {param_str}"
        query += ")"
        
        # Validate before returning
        validation = self.parser.validate_query_syntax(query)
        if not validation.get("valid", False):
            errors = validation.get("errors", [])
            error_messages = [error.get("message", "Unknown error") for error in errors]
            raise ValueError(f"Invalid query: {', '.join(error_messages)}")
        
        return query
    
    def build_with_suggestions(self):
        """Build query with optimization suggestions."""
        query = self.build()
        suggestions = self.parser.get_optimization_suggestions(query)
        
        return {
            "query": query,
            "suggestions": suggestions,
            "performance": self.parser.get_performance_estimate(query)
        }

# Usage
builder = QueryBuilder(parser)
query_info = (builder
              .measurement("temperature")
              .field("value")
              .tags(location="datacenter", sensor="outdoor")
              .function("sma", window=10)
              .build_with_suggestions())

print(f"Built query: {query_info['query']}")
print(f"Suggestions: {query_info['suggestions']}")
```

### Query Optimization Engine
```python
def optimize_query_automatically(parser, original_query):
    """Automatically optimize a query based on analysis."""
    # Parse original query
    result = parser.parse_query(original_query, validation_options={
        "estimate_performance": True,
        "suggest_optimizations": True
    })
    
    if result.get("status") != "success":
        return {"error": "Failed to parse original query"}
    
    # Extract optimization opportunities
    optimization_analysis = result.get("optimization_analysis", {})
    optimizations = optimization_analysis.get("optimizations", [])
    
    if not optimizations:
        return {
            "original_query": original_query,
            "optimized_query": original_query,
            "improvements": "No optimizations available"
        }
    
    # Apply optimizations (simplified approach)
    components = result["parse_result"]["parsed_components"]
    optimized_components = components.copy()
    
    improvements = []
    for opt in optimizations:
        if opt.get("type") == "parameter_optimization":
            param_name = opt.get("original", "").split("=")[0]
            new_value = opt.get("suggested", "").split("=")[1]
            optimized_components["parameters"][param_name] = new_value
            improvements.append(opt.get("expected_improvement", "Unknown improvement"))
    
    # Rebuild optimized query (simplified)
    # In practice, this would use a proper query builder
    optimized_query = original_query  # Placeholder for demonstration
    
    return {
        "original_query": original_query,
        "optimized_query": optimized_query,
        "improvements": improvements,
        "optimization_analysis": optimization_analysis
    }

# Usage
optimization_result = optimize_query_automatically(parser, 
    "sma(temperature(value){location:datacenter}, window=50)")
print("Query optimization result:", json.dumps(optimization_result, indent=2))
```

## Error Handling

### Common Error Scenarios
- **Syntax Errors**: Malformed query strings, missing brackets, invalid operators
- **Function Errors**: Non-existent functions, invalid parameters, type mismatches
- **Data Errors**: Invalid measurement/field names, malformed tag syntax
- **Semantic Errors**: Valid syntax but logically incorrect operations

### Robust Error Handling
```python
def parse_query_safely(parser, query):
    """Parse query with comprehensive error handling."""
    try:
        result = parser.parse_query(query)
        
        if result.get("status") == "success":
            parse_result = result.get("parse_result", {})
            
            if parse_result.get("valid", False):
                print(f"✓ Query parsed successfully")
                complexity = parse_result.get("complexity_level", "unknown")
                print(f"  Complexity: {complexity}")
                return result
            else:
                print(f"✗ Query is invalid:")
                errors = parse_result.get("errors", [])
                for error in errors:
                    print(f"  - {error.get('message', 'Unknown error')}")
                
                # Show suggestions if available
                suggestions = result.get("suggestions", {})
                corrected_options = suggestions.get("corrected_query_options", [])
                if corrected_options:
                    print("  Suggested corrections:")
                    for correction in corrected_options[:3]:  # Show top 3
                        print(f"    • {correction}")
                return result
        else:
            print(f"✗ Parse request failed: {result.get('error', 'Unknown error')}")
            return None
            
    except requests.exceptions.ConnectionError:
        print("✗ Failed to connect to TSDB server")
        return None
    except requests.exceptions.Timeout:
        print("✗ Parse request timed out")
        return None
    except Exception as e:
        print(f"✗ Unexpected error during parsing: {e}")
        return None

# Usage
parse_query_safely(parser, "sma(temperature(value), window=10)")
parse_query_safely(parser, "invalid_function(bad_syntax")
```

## Related Endpoints

- [`POST /functions/validate`](functions-validate-endpoint.md) - Validate function parameters in parsed queries
- [`GET /functions/{name}`](functions-name-endpoint.md) - Get details about functions used in queries
- [`POST /query`](query-endpoint.md) - Execute validated queries
- [`POST /query/functions`](query-functions-endpoint.md) - Execute function-specific queries
- [`GET /functions`](functions-endpoint.md) - List available functions for query building