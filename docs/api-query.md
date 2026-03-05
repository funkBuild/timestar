# Query API

**Endpoint:** `POST /query`
**Content-Type:** `application/json`

Query time series data with aggregation, filtering, grouping, and time bucketing.

## Request

```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value){location:us-west} by {sensor}",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000,
    "aggregationInterval": "5m"
  }'
```

## Query String Syntax

```
method:measurement(fields){scopes} by {groupKeys}
```

| Component | Required | Description | Example |
|-----------|----------|-------------|---------|
| method | yes | Aggregation method | `avg`, `max`, `sum` |
| measurement | yes | Measurement name | `temperature` |
| fields | no | Comma-separated field list; `()` = all | `(value,humidity)` |
| scopes | no | Tag filters, comma-separated AND | `{location:us-west,host:server-01}` |
| group by | no | Group results by tag keys | `by {location,sensor}` |

## Aggregation Methods

| Method | Description |
|--------|-------------|
| `avg` | Mean of values |
| `min` | Minimum value |
| `max` | Maximum value |
| `sum` | Sum of values |
| `count` | Count of non-NaN points |
| `latest` | Most recent value |
| `first` | Earliest value |
| `median` | 50th percentile |
| `stddev` | Population standard deviation |
| `stdvar` | Population variance |
| `spread` | Max minus min |

## Field Type Handling

Different field types interact with the aggregation pipeline differently. Numeric types (float, integer, boolean) are aggregated normally, while string fields bypass aggregation entirely.

| Type | Behavior |
|------|----------|
| **float** | Primary numeric type; aggregated directly |
| **integer** | Aggregated as numeric values |
| **boolean** | Converted to doubles (`true` = `1.0`, `false` = `0.0`), then aggregated |
| **string** | Returned raw; aggregation method is ignored |

## Scope Filtering

Scopes filter series by tag values. Three match types are supported:

| Type | Syntax | Example |
|------|--------|---------|
| Exact | `key:value` | `{host:server-01}` |
| Wildcard | `key:pattern` | `{host:server-*}` |
| Regex | `key:~pattern` or `key:/pattern/` | `{host:~server-[0-9]+}` |

Wildcards: `*` matches any sequence, `?` matches one character.

## Time Parameters

| Field | Type | Description |
|-------|------|-------------|
| `startTime` | uint64 | Start time in nanoseconds since epoch |
| `endTime` | uint64 | End time in nanoseconds since epoch |
| `aggregationInterval` | string or uint64 | Time bucket interval |

Time values accept any precision (seconds, milliseconds, microseconds, nanoseconds) and are normalized internally.

### Interval Units

| Unit | Meaning |
|------|---------|
| `ns` | nanoseconds |
| `us`, `µs` | microseconds |
| `ms` | milliseconds |
| `s` | seconds |
| `m` | minutes |
| `h` | hours |
| `d` | days |

Decimal values are supported: `"1.5s"`, `"0.5m"`.

## Examples

**Simple query:**
```json
{
  "query": "avg:temperature()",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000
}
```

**Filtered with time buckets:**
```json
{
  "query": "max:cpu(usage_percent){host:server-01}",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000,
  "aggregationInterval": "1h"
}
```

**Grouped aggregation:**
```json
{
  "query": "avg:temperature(value){location:us-west} by {sensor}",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000,
  "aggregationInterval": "5m"
}
```

## Response

**Success (200):**
```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "groupTags": ["location=us-west", "sensor=temp-01"],
      "fields": {
        "value": {
          "timestamps": [1704067200000000000, 1704067500000000000],
          "values": [23.5, 24.2]
        }
      }
    }
  ],
  "statistics": {
    "series_count": 1,
    "point_count": 2,
    "execution_time_ms": 12.5
  }
}
```

**Error (400/413/500):**
```json
{
  "status": "error",
  "error": "INVALID_QUERY",
  "message": "Invalid query format: missing measurement"
}
```

## Limits

| Limit | Default | Config Key |
|-------|---------|------------|
| Max body size | 1 MB | `http.max_query_body_size` |
| Max series | 10,000 | `http.max_series_count` |
| Max points | 10,000,000 | `http.max_total_points` |
| Timeout | 30s | `http.query_timeout_seconds` |
