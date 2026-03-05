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

| JSON Type | TSDB Type |
|-----------|-----------|
| number (has decimal) | float64 |
| number (integer) | int64 |
| boolean | bool |
| string | string |

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

## Limits

| Limit | Default | Config Key |
|-------|---------|------------|
| Max body size | 64 MB | `http.max_write_body_size` |
