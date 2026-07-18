# Write API

**Endpoint:** `POST /write`
**Content-Type:** `application/json`

Ingest time series data. Supports single points, multi-point arrays, and batch writes.

## Single Point

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"location": "us-west", "host": "server-01"},
    "fields": {"value": 23.5, "humidity": 65.0},
    "timestamp": 1704067200000000000
  }'
```

## Multi-Point Array

Send multiple timestamps for the same series in one request. Each field contains an array of values aligned with the `timestamps` array.

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"location": "us-west"},
    "fields": {
      "value": [23.5, 24.0, 23.8, 24.2],
      "humidity": [65.0, 64.5, 65.5, 64.0]
    },
    "timestamps": [1704067200000000000, 1704067260000000000, 1704067320000000000, 1704067380000000000]
  }'
```

## Batch Write

Send multiple independent points in one request.

```bash
curl -X POST http://localhost:8086/write \
  -H "Content-Type: application/json" \
  -d '{
    "writes": [
      {
        "measurement": "temperature",
        "tags": {"location": "us-west"},
        "fields": {"value": 23.5},
        "timestamp": 1704067200000000000
      },
      {
        "measurement": "cpu",
        "tags": {"host": "server-01"},
        "fields": {"usage": 45.2},
        "timestamp": 1704067200000000000
      }
    ]
  }'
```

## Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `measurement` | string | yes | Measurement name |
| `tags` | object | no | Key-value tag pairs for series identification |
| `fields` | object | yes | Field name to value mapping |
| `timestamp` | uint64 | no | Nanoseconds since epoch (auto-generated if omitted) |
| `timestamps` | uint64[] | no | Array of timestamps for multi-point format |
| `writes` | array | no | Array of points for batch format |

## Timestamp Auto-Generation

When `timestamp` or `timestamps` is omitted, the server generates timestamps automatically using the current system time (nanoseconds since epoch):

- **Single point:** A single nanosecond-precision timestamp is generated from the current time.
- **Multi-point array (no timestamp):** Timestamps are generated with 1ms (1,000,000 ns) spacing starting from the current time. For example, a 4-element array write produces timestamps at `now`, `now + 1ms`, `now + 2ms`, `now + 3ms`.
- **Multi-point array with single `timestamp`:** The provided timestamp is replicated for all points in the array (all points share the same timestamp).

## Field Types

Field values are auto-detected from JSON:

| JSON Type | TimeStar Type |
|-----------|-----------|
| number (has decimal) | float64 |
| number (integer) | int64 |
| boolean | bool |
| string | string |

### Explicit field types (`field_types`)

Auto-detection depends on how the number is *written*, which is fragile for
serializers that drop trailing decimals — JavaScript's `JSON.stringify(10.0)`
emits `10`, silently registering the field as int64. A write point (single or
batch entry) may carry an optional `field_types` object to pin the type
explicitly:

```json
{
  "measurement": "temperature",
  "tags": {"location": "us-west"},
  "fields": {"value": 10},
  "field_types": {"value": "float"},
  "timestamp": 1465839830100400200
}
```

- **Type names:** `float` (alias `double`), `int` (aliases `integer`,
  `int64`), `bool` (alias `boolean`), `string`.
- **Scope:** applies per write point; in batch writes each entry carries its
  own `field_types`. Fields not named in the map keep auto-detection.
- **Coercion:** a declared `float` accepts any JSON number (integer tokens
  widen to float64). A declared `int` accepts integer tokens and
  float-shaped values that are exactly integral (`1000.0` → `1000`);
  fractional values are rejected. `bool` and `string` require exact JSON
  types — no coercion.
- **Validation (400):** unknown type names, value/type mismatches, and
  `field_types` entries naming a field absent from `fields` are rejected
  (a typo silently falling back to detection would defeat the feature).
  In batch writes an invalid entry is skipped and reported in
  `failed_writes`, matching other per-entry validation errors.
- The declared type participates in the normal field-type registry: the
  first write for a field still registers its type, and later writes of a
  *different* type still land in a separate typed series. Declare types from
  the first write onward for full protection.

## Naming Restrictions

Measurement names, tag keys, tag values, and field names must not contain:
- Null bytes
- Commas (`,`)
- Equals signs (`=`)
- Spaces (in measurement names, tag keys, and field names -- spaces are allowed in tag values)

## Response

**Success (200):**
```json
{"status": "success", "points_written": 5}
```

**Error (400/413/500):**
```json
{"status": "error", "message": "Missing required field: measurement"}
```

## Behavior Notes

- **Empty fields rejected:** A `fields` object with no keys (`"fields": {}`) or a field whose value is an empty array (`"value": []`) returns `400` with an error message.
- **503 on shutdown:** If the server is shutting down, write requests return `503 Service Unavailable` with `{"status":"error","message":"Server is shutting down"}`. Clients should retry against another node or after restart.
- **Batch coalescing:** In batch writes (`"writes": [...]`), points that share the same series key (measurement + tags + field) are automatically coalesced into multi-point arrays. Each coalesced array is capped at 10,000 values per field; excess points remain as separate entries.
- **Metadata indexing is asynchronous:** Series ID creation and tag indexing are dispatched as fire-and-forget background tasks after the data is durable in the WAL. The write response is returned before metadata indexing completes.
- **Duplicate points overwrite — last write wins:** Writing the same measurement + tags + field + timestamp again REPLACES the earlier point (InfluxDB-compatible last-write-wins). Queries only ever see the most recent write for a given timestamp: raw reads return one point, and aggregations count it exactly once (`count` = 1 no matter how many times the point was rewritten). This holds regardless of where the copies live — within one batch (the last entry in request order wins), in the in-memory store, across a flush boundary (a rewrite in memory shadows the flushed copy), across TSM files, and permanently after compaction. Idempotent retries of a write are therefore always safe.

## Limits

| Limit | Default | Config Key |
|-------|---------|------------|
| Max body size | 64 MB | `http.max_write_body_size` |
