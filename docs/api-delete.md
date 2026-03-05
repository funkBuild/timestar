# Delete API

**Endpoint:** `POST /delete`
**Content-Type:** `application/json`

Delete time series data by series key, structured query, pattern match, or batch.

## Delete by Structured Query

```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"location": "us-west", "host": "server-01"},
    "field": "value",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000
  }'
```

## Delete by Pattern

Omit tags or fields to match broadly. Specify `fields` as an array to delete multiple fields.

```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "tags": {"location": "us-west"},
    "fields": ["value", "humidity"],
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000
  }'
```

## Delete All Data for a Measurement

```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "startTime": 0,
    "endTime": 9223372036854775807
  }'
```

## Delete by Series Key

Use a pre-formatted series key string:

```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "series": "temperature,location=us-west.value",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000
  }'
```

## Batch Delete

```bash
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -d '{
    "deletes": [
      {
        "measurement": "temperature",
        "tags": {"location": "us-west"},
        "fields": ["value"],
        "startTime": 1704067200000000000,
        "endTime": 1704153600000000000
      },
      {
        "measurement": "cpu",
        "field": "usage",
        "startTime": 0,
        "endTime": 9223372036854775807
      }
    ]
  }'
```

## Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `series` | string | no | Pre-formatted series key |
| `measurement` | string | no | Measurement name (used with structured delete) |
| `tags` | object | no | Tag filters; omit to match all tags |
| `field` | string | no | Single field to delete |
| `fields` | string[] | no | Multiple fields to delete |
| `startTime` | uint64 | no | Start of time range (default: 0) |
| `endTime` | uint64 | no | End of time range (default: max uint64) |
| `deletes` | array | no | Array of delete operations for batch |

Either `series` or `measurement` must be provided.

## Response

**Success (200):**

The response always includes `seriesDeleted` (number of series affected) and `totalRequests` (number of delete requests processed, i.e. 1 for a single delete, or the length of the `deletes` array for a batch).

When 100 or fewer series are deleted, the response includes a `deletedSeries` array listing each affected series key:

```json
{
  "status": "success",
  "seriesDeleted": 3,
  "totalRequests": 1,
  "deletedSeries": [
    "temperature,location=us-west value",
    "temperature,location=us-west humidity",
    "temperature,location=us-west dewpoint"
  ]
}
```

When more than 100 series are deleted, the individual list is omitted and replaced with a count and a note:

```json
{
  "status": "success",
  "seriesDeleted": 250,
  "totalRequests": 1,
  "deletedSeriesCount": 250,
  "note": "Series list omitted due to size"
}
```

When no series match, `seriesDeleted` is 0 and neither `deletedSeries` nor `deletedSeriesCount` is present:

```json
{
  "status": "success",
  "seriesDeleted": 0,
  "totalRequests": 1
}
```

**Error (400/500):**
```json
{"status": "error", "error": "Missing required field: measurement or series"}
```
