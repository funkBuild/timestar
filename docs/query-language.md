# Query Language

TimeStar uses a concise query string format for time series retrieval with aggregation, filtering, and grouping.

## Syntax

```
method:measurement(fields){scopes} by {groupKeys}
```

## Aggregation Methods

| Method | Description |
|--------|-------------|
| `avg` | Mean of values |
| `min` | Minimum value |
| `max` | Maximum value |
| `sum` | Sum of values |
| `count` | Count of non-NaN data points |
| `latest` | Most recent value |
| `first` | Earliest value |
| `median` | 50th percentile |
| `stddev` | Population standard deviation |
| `stdvar` | Population variance |
| `spread` | Range (max - min) |

Aggregation uses a two-phase distributed approach: each shard computes partial `AggregationState` objects that are merged centrally, reducing cross-shard data transfer.

## Fields

Specify which fields to retrieve inside parentheses:

| Syntax | Meaning |
|--------|---------|
| `()` | All fields |
| `(value)` | Single field |
| `(value,humidity)` | Multiple fields |

## Scopes (Tag Filters)

Filter series by tag values inside braces. All conditions are ANDed.

### Match Types

| Type | Syntax | Example |
|------|--------|---------|
| Exact | `key:value` | `{host:server-01}` |
| Wildcard | `key:pattern` | `{host:server-*}`, `{host:server-0?}` |
| Regex (tilde) | `key:~pattern` | `{host:~server-[0-9]+}` |
| Regex (slashes) | `key:/pattern/` | `{host:/server-[0-9]+/}` |

Wildcard characters: `*` matches any sequence, `?` matches one character.

Regex uses C++ `std::regex` (ECMAScript) syntax. An optimization extracts literal prefixes from patterns (e.g., `server-*` has prefix `server-`) to narrow the search before regex evaluation.

**Invalid pattern handling:** If a `~regex` or `/regex/` pattern fails to compile, the match returns `false` (no series matched). If a wildcard pattern fails to compile as regex, it falls back to an exact string comparison against the raw pattern.

## Group By

Group results by tag keys:

```
avg:temperature(value) by {location}
avg:temperature(value) by {location,sensor}
```

Omit the `by {}` clause entirely to aggregate all matching series together.

## Time Parameters

These are JSON fields in the `POST /query` request body, separate from the query string itself.

| Field | Type | Description |
|-------|------|-------------|
| `startTime` | uint64 or string | Start time |
| `endTime` | uint64 or string | End time |
| `aggregationInterval` | string or uint64 | Time bucket interval (optional) |

`startTime` and `endTime` must satisfy `startTime < endTime`.

### Numeric Timestamps

When provided as a JSON number (uint64), the value is used as-is in nanoseconds since epoch. No automatic precision normalization is performed, so callers should provide nanosecond values (e.g., multiply seconds by 10^9).

### String Timestamps

When provided as a JSON string, the value is first tried as a numeric string (via `stoull`). If that fails, it is parsed as a date string in the format:

```
dd-mm-yyyy hh:mm:ss
```

All times are interpreted as **UTC** (uses `timegm`). The result is converted to nanoseconds since epoch (second precision).

**Example:** `"15-03-2024 14:30:00"` represents March 15, 2024 at 14:30:00 UTC.

**Validation:** Day (1-31), month (1-12), year (>= 1970), hour (0-23), minute (0-59), second (0-59) are range-checked. However, per-month day limits are not enforced by the parser -- invalid dates like `31-02-2024` (Feb 31) are passed to `timegm`, which silently rolls them forward (e.g., to early March).

### Aggregation Interval

`aggregationInterval` is a JSON request parameter, not part of the query string syntax. It controls time-bucketed aggregation and can be:

- A JSON number (uint64) interpreted as nanoseconds.
- A JSON string with a duration suffix (see table below). A bare numeric string without a unit suffix is treated as nanoseconds.

### Interval Duration Strings

| Unit | Meaning | Example |
|------|---------|---------|
| `ns` | nanoseconds | `"500ns"` |
| `us`, `µs` | microseconds | `"100us"` |
| `ms` | milliseconds | `"500ms"` |
| `s` | seconds | `"30s"` |
| `m` | minutes | `"5m"` |
| `h` | hours | `"1h"` |
| `d` | days | `"1d"` |

Decimal values: `"1.5s"`, `"0.5m"`, `"2.5h"`.

## Examples

**Average temperature, all fields, no filter:**
```
avg:temperature()
```

**Max CPU for a specific host:**
```
max:cpu(usage_percent){host:server-01}
```

**Average with wildcard filter, grouped by datacenter:**
```
avg:cpu(usage_percent){host:server-*} by {datacenter}
```

**Standard deviation with regex filter:**
```
stddev:latency(p99){service:~api-v[0-9]+} by {region}
```

**Count of data points per location:**
```
count:temperature(value) by {location}
```
