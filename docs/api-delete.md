# Delete API

**Endpoint:** `POST /delete`
**Content-Type:** `application/json`

Delete time series data by series key, structured query, pattern match, or batch.

## Clustered Mode

Partitioned RF&gt;1 mode currently accepts only exact targets: either `series`, or
`measurement` plus one `field` and the complete tag set. RF=1 delete is
unavailable. Requests using `fields`, omitting a field, or mixing an exact target
with a pattern return `501` before discovery or mutation. Pattern deletes remain
available in non-partitioned mode.

Every RF&gt;1 request must include both of these headers:

- `Idempotency-Key`: exactly 32 hexadecimal characters and not all zeroes.
- `Idempotency-Key-Timestamp`: the request's Unix epoch time in milliseconds. It
  must be less than one hour old and no more than five minutes in the future.

Retry an uncertain request with the same body and both original headers. The
request body, key, and timestamp together identify the operation; changing any
of them creates a different delete.

```bash
DELETE_TS_MS="$(date +%s)000"
curl -X POST http://localhost:8086/delete \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 9f1d73046ce64e719adbc8a11f431b52" \
  -H "Idempotency-Key-Timestamp: ${DELETE_TS_MS}" \
  -d '{
    "series": "temperature,location=us-west.value",
    "startTime": 1704067200000000000,
    "endTime": 1704153600000000000
  }'
```

Each VShard retains at most the latest 1,024 operation receipts for at most one
hour. Capacity can therefore retire a receipt sooner. A retry below the
replicated capacity floor returns `409`, the JSON code
`DELETE_IDEMPOTENCY_EXPIRED`, and `X-TimeStar-Idempotency-Window: expired`. A
timestamp already outside the one-hour HTTP window returns `400`. In either
case, reconcile the current data state; do not merely generate a new timestamp,
because that would authorize a new delete which can erase intervening writes.

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

Use `fields`, or omit both `field` and `fields`, to select more than one exact
series. Tags act as filters. This form is currently rejected in partitioned
cluster mode.

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

The response always includes `seriesDeleted` and `totalRequests` (1 for a single
delete, or the length of the `deletes` array for a batch). In non-partitioned
mode, `seriesDeleted` is the number of series the local operation affected. In
RF&gt;1 mode, it is the number of unique exact targets covered by committed and
applied VShard batches; it does not assert that points existed before apply.

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

**Error:**
```json
{"status": "error", "error": "Missing required field: measurement or series"}
```

Relevant status codes are:

| Status | Meaning |
|--------|---------|
| `400` | Invalid body, idempotency header, timestamp, range, or safety limit |
| `409` | The replicated idempotency receipt has been retired; outcome reconciliation is required |
| `413` | HTTP body or encoded per-VShard Raft entry is too large |
| `501` | Delete form is unsupported in the configured cluster mode |
| `503` | Retryable pre-proposal cluster condition; honor `Retry-After` |
| `504` | Mutation outcome is unknown; preserve the original retry identity |
| `500` | Internal error or invalid cluster placement/state |
