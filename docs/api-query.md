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

`latest`/`first` do **not** collapse a time range. They select across *series*
at each timestamp, so without an `aggregationInterval` they keep every
timestamp (for a single series they are raw passthrough); with one they select
a point per bucket. Use an `aggregationInterval` covering the range to get a
single most-recent value.

## Field Type Handling

Different field types interact with the aggregation pipeline differently. Numeric types (float, integer) are aggregated normally, while string and boolean fields bypass aggregation entirely.

| Type | Behavior |
|------|----------|
| **float** | Primary numeric type; aggregated directly |
| **integer** | Aggregated as numeric values |
| **boolean** | Non-numeric; returned as `true`/`false`, aggregation method is ignored |
| **string** | Non-numeric; returned raw, aggregation method is ignored |

Non-numeric fields (string, boolean) are returned in the type they were written in and keep their series tags. Without an `aggregationInterval` they pass through raw; with one they reduce to LATEST-per-bucket (one value per epoch-aligned bucket, stamped with the bucket start). See CLAUDE.md "Non-Numeric Fields in Queries" for the canonical rule.

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

### Response framing and number text

**Large JSON results are streamed.** A successful JSON result with at least
`http.stream_response_min_points` points (default 65,536) is sent to an
HTTP/1.1 client with `Transfer-Encoding: chunked`, written to the connection as
it is formatted. Smaller results, every HTTP/1.0 client, error responses and
protobuf responses carry a `Content-Length` as before. The body bytes are
identical either way; any HTTP/1.1 client library handles this transparently.
What it buys: the server never holds (or copies) the whole body, so a
1M-point result costs it ~46 MB less at peak, and the first byte arrives as
soon as the query has executed (5 ms instead of 32 ms for a 30 MB response)
rather than after the whole body has been built. A slow reader applies
backpressure to formatting instead of growing a buffer. Set the key to `0` to
turn streaming off. Because the status line is sent before the body, a failure
*while streaming* (the connection dropping) cannot become an error response —
it ends the connection, which the client sees as a truncated chunked body.

**Numbers are shortest round-trip decimals.** Every `double` is written with
the fewest digits that parse back to exactly the same value. Magnitudes from
`1e-4` up to (but excluding) `1e16` are written in fixed notation (`0.00025`,
`1234567.5`); smaller and larger ones in scientific notation with an uppercase
`E` and no `+` (`2.5E-5`, `1E16`, `1.2345E21`). `NaN` and `±Infinity` are
`null`. Parse values as numbers; do not match on their text. (Before 1.5 the
fixed-notation range was wider — `1E16` was written `10000000000000000` and
`2.5E-5` as `0.000025`. The parsed values are unchanged.)

## Limits

| Limit | Default | Config Key |
|-------|---------|------------|
| Max body size | 1 MB | `http.max_query_body_size` |
| Max series | 10,000 | `http.max_series_count` |
| Max points | 10,000,000 | `http.max_total_points` |
| Timeout | 30s | `http.query_timeout_seconds` |
| Streamed-response threshold | 65,536 points (`0` = never stream) | `http.stream_response_min_points` |

A request body larger than the limit is refused with `413` before it is read:
the server takes the request headers, sees the declared length, and answers
without buffering the body (it then closes the connection rather than read the
rest).
