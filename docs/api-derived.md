# Derived Query API

**Endpoint:** `POST /derived`
**Content-Type:** `application/json`

Execute multi-query expressions, anomaly detection, and forecasting. Sub-queries are executed in parallel, aligned to common timestamps, and combined via a formula.

## Basic Derived Query

Combine two queries with a formula:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:system(bytes_sent){host:server-01}",
      "b": "avg:system(bytes_recv){host:server-01}"
    },
    "formula": "a + b",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Single Query with Formula

Apply a transform to a single query:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:temperature(value){location:us-west}"
    },
    "formula": "a * 1.8 + 32",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Anomaly Detection

Use the `anomalies()` function in the formula:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:cpu(usage){host:server-01}"
    },
    "formula": "anomalies(a, '\''basic'\'', 2)",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

See [Anomaly Detection](anomaly-detection.md) for algorithm details.

## Forecasting

Use the `forecast()` function in the formula:

```bash
curl -X POST http://localhost:8086/derived \
  -H "Content-Type: application/json" \
  -d '{
    "queries": {
      "a": "avg:temperature(value){location:us-west}"
    },
    "formula": "forecast(a, '\''linear'\'', 2)",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

See [Forecasting](forecasting.md) for algorithm details.

## Formula Syntax

Formulas reference sub-queries by name and support:

- **Arithmetic:** `a + b`, `a - b`, `a * b`, `a / b`
- **Constants:** `a * 1.8 + 32`
- **Functions:** `abs(a)`, `rate(a)`, `rolling_avg(a, 10)`
- **Nesting:** `abs(a - b) / max(a, b) * 100`

See [Expression Functions](expression-functions.md) for the full function list.

## Request Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `queries` | object | yes | Map of name to query string (e.g., `{"a": "avg:cpu(usage){}"}`) |
| `formula` | string | yes | Expression combining sub-query results |
| `startTime` | uint64 | yes | Start time in nanoseconds |
| `endTime` | uint64 | yes | End time in nanoseconds |
| `aggregationInterval` | string/uint64 | no | Time bucket interval |

## Response

**Derived query (200):**
```json
{
  "status": "success",
  "timestamps": [1704067200000000000, 1704067500000000000],
  "values": [45.2, 46.8],
  "formula": "a + b",
  "statistics": {
    "point_count": 2,
    "execution_time_ms": 45.2,
    "sub_queries_executed": 2,
    "points_dropped_due_to_alignment": 0
  }
}
```

**Anomaly detection (200):**
```json
{
  "status": "success",
  "times": [1704067200000000000, 1704067500000000000],
  "series": [
    {"piece": "raw", "group_tags": {}, "values": [23.5, 24.2]},
    {"piece": "upper", "group_tags": {}, "values": [26.0, 26.1]},
    {"piece": "lower", "group_tags": {}, "values": [21.0, 21.1]},
    {"piece": "score", "group_tags": {}, "values": [0.0, 0.0], "alert_value": 0.0}
  ],
  "statistics": {
    "algorithm": "basic",
    "bounds": 2.0,
    "anomaly_count": 0,
    "total_points": 2,
    "execution_time_ms": 32.1
  }
}
```

**Forecast (200):**
```json
{
  "status": "success",
  "times": [1704067200000000000, 1704067500000000000, 1704067800000000000],
  "forecast_start_index": 2,
  "series": [
    {"piece": "past", "group_tags": {}, "values": [23.5, 24.2, null]},
    {"piece": "forecast", "group_tags": {}, "values": [null, null, 24.8]},
    {"piece": "upper", "group_tags": {}, "values": [null, null, 27.0]},
    {"piece": "lower", "group_tags": {}, "values": [null, null, 22.6]}
  ],
  "statistics": {
    "algorithm": "linear",
    "deviations": 2.0,
    "slope": 0.15,
    "intercept": 23.0,
    "r_squared": 0.89,
    "historical_points": 2,
    "forecast_points": 1,
    "execution_time_ms": 28.5
  }
}
```

**Error (400/500):**
```json
{
  "status": "error",
  "error": {"code": "QUERY_ERROR", "message": "Error description"}
}
```

## Limits

| Limit | Default |
|-------|---------|
| Max body size | 1 MB |
