# Anomaly Detection Implementation Plan

## Overview

Implement Datadog-style anomaly detection as a formula function in the derived query system. The `anomalies()` function will return multiple series pieces (raw, upper, lower, scores) that can be used to visualize anomaly bounds and detect outliers.

## API Design

### Request Format

```json
POST /derived
{
  "queries": {
    "cpu": "avg:system.cpu.user(){host:server01}"
  },
  "formula": "anomalies(cpu, 'basic', 2)",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000,
  "aggregationInterval": 60000000000
}
```

### Function Signature

```
anomalies(query_name, 'algorithm', bounds[, 'seasonality'])
```

**Parameters:**
- `query_name` - Reference to a defined query (e.g., `cpu`)
- `algorithm` - One of: `'basic'`, `'agile'`, `'robust'`
- `bounds` - Number 1-4 (standard deviations for normal range)
- `seasonality` (optional) - One of: `'hourly'`, `'daily'`, `'weekly'`

### Response Format

```json
{
  "status": "success",
  "times": [1704067200000000000, 1704067260000000000, ...],
  "series": [
    {
      "piece": "raw",
      "group_tags": ["host=server01"],
      "values": [45.2, 47.1, 52.3, ...]
    },
    {
      "piece": "upper",
      "group_tags": ["host=server01"],
      "values": [55.0, 55.5, 56.0, ...]
    },
    {
      "piece": "lower",
      "group_tags": ["host=server01"],
      "values": [35.0, 35.5, 36.0, ...]
    },
    {
      "piece": "scores",
      "group_tags": ["host=server01"],
      "values": [0.0, 0.0, 0.85, ...],
      "alert_value": 0.85
    }
  ],
  "statistics": {
    "algorithm": "basic",
    "bounds": 2,
    "anomaly_count": 3,
    "execution_time_ms": 45.2
  }
}
```

## Algorithm Descriptions

### 1. Basic Algorithm
- **Method:** Rolling quantile/standard deviation computation
- **Use case:** Metrics without seasonal patterns, fast response needed
- **Implementation:** Rolling window statistics (mean ± bounds × stddev)

### 2. Agile Algorithm (SARIMA-based)
- **Method:** Seasonal ARIMA prediction
- **Use case:** Metrics with seasonal patterns that may shift
- **Implementation:** Uses recent values + same time in previous periods

### 3. Robust Algorithm (STL-based)
- **Method:** Seasonal-Trend decomposition using LOESS
- **Use case:** Stable metrics with consistent seasonal patterns
- **Implementation:** Decompose into Trend + Seasonal + Residual, detect anomalies in residual

## Implementation Steps

### Phase 1: Core Infrastructure

1. **Create anomaly result types** (`lib/query/anomaly_result.hpp`)
   - `AnomalySeriesPiece` struct
   - `AnomalyQueryResult` struct
   - Statistics struct

2. **Extend expression AST** (`lib/query/expression_ast.hpp`)
   - Add `AnomalyFunctionNode` class
   - Add `BuiltinFunction::ANOMALIES` enum value

3. **Update expression parser** (`lib/query/expression_parser.cpp`)
   - Parse `anomalies(query, 'algorithm', bounds, 'seasonality')` syntax
   - Validate algorithm and seasonality values

### Phase 2: Algorithm Implementation

4. **Create anomaly detector base** (`lib/query/anomaly/anomaly_detector.hpp`)
   - Abstract base class
   - Common utilities

5. **Implement Basic algorithm** (`lib/query/anomaly/basic_detector.cpp`)
   - Rolling window statistics
   - Bound calculation
   - Score computation

6. **Implement STL decomposition** (`lib/query/anomaly/stl_decomposition.cpp`)
   - LOESS smoothing
   - Trend extraction
   - Seasonal extraction
   - Residual computation

7. **Implement Robust algorithm** (`lib/query/anomaly/robust_detector.cpp`)
   - Use STL decomposition
   - Detect anomalies in residuals

8. **Implement Agile algorithm** (`lib/query/anomaly/agile_detector.cpp`)
   - Seasonal prediction using historical data
   - Adaptive baseline

### Phase 3: Integration

9. **Create anomaly executor** (`lib/query/anomaly_executor.cpp`)
   - Route to appropriate algorithm
   - Handle seasonality periods
   - Aggregate results

10. **Update derived query executor** (`lib/query/derived_query_executor.cpp`)
    - Handle `AnomalyFunctionNode`
    - Execute anomaly detection
    - Format multi-piece results

11. **Update HTTP handler** (`lib/http/http_derived_query_handler.cpp`)
    - Handle anomaly response format
    - JSON serialization for pieces

### Phase 4: Testing

12. **Unit tests for Basic algorithm**
13. **Unit tests for STL decomposition**
14. **Unit tests for Robust algorithm**
15. **Unit tests for Agile algorithm**
16. **Integration tests for anomaly API**

## File Structure

```
lib/query/anomaly/
├── anomaly_result.hpp       # Result types
├── anomaly_detector.hpp     # Base interface
├── anomaly_executor.hpp     # Main executor
├── anomaly_executor.cpp
├── basic_detector.hpp       # Basic algorithm
├── basic_detector.cpp
├── stl_decomposition.hpp    # STL for Robust
├── stl_decomposition.cpp
├── robust_detector.hpp      # Robust algorithm
├── robust_detector.cpp
├── agile_detector.hpp       # Agile algorithm
└── agile_detector.cpp
```

## References

- [Datadog Anomaly Detection Blog](https://www.datadoghq.com/blog/introducing-anomaly-detection-datadog/)
- [STL Decomposition - Forecasting Principles](https://otexts.com/fpp3/stl.html)
- [statsmodels STL](https://www.statsmodels.org/dev/examples/notebooks/generated/stl_decomposition.html)
