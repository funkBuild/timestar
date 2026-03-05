# Streaming API

Real-time data streaming via Server-Sent Events (SSE).

## Subscribe

**Endpoint:** `POST /subscribe`
**Content-Type:** `application/json`
**Response:** `text/event-stream`

### Single Query

```bash
curl -N -X POST http://localhost:8086/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "query": "avg:temperature(value){location:us-west}",
    "aggregationInterval": "1m",
    "backfill": true,
    "startTime": "1h"
  }'
```

### Multi-Query with Formula

```bash
curl -N -X POST http://localhost:8086/subscribe \
  -H "Content-Type: application/json" \
  -d '{
    "queries": [
      {"query": "avg:system(bytes_sent){host:server-01}", "label": "sent"},
      {"query": "avg:system(bytes_recv){host:server-01}", "label": "recv"}
    ],
    "formula": "sent + recv",
    "aggregationInterval": "1m",
    "backfill": true,
    "startTime": "1h"
  }'
```

## Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | no* | Single query string |
| `queries` | array | no* | Array of `{query, label}` objects |
| `formula` | string | no | Expression formula (requires `aggregationInterval`) |
| `aggregationInterval` | string/uint64 | no | Time bucket interval (required with `formula`) |
| `backfill` | bool | no | Send historical data on connect (default: false) |
| `startTime` | uint64/string | no | Start time as nanosecond timestamp (uint64) or relative duration offset string (e.g., `"1h"`, `"30m"`, `"5s"`) |

*Either `query` or `queries` must be provided.

**Note on `startTime`:** String values that look like bare integers (e.g., `"1704067200"`) are
silently ignored -- no error is returned, but no backfill will occur. To pass an absolute
nanosecond timestamp, use a JSON number without quotes (e.g., `"startTime": 1704067200000000000`).
String values are only interpreted as relative duration offsets with a unit suffix
(e.g., `"1h"`, `"30m"`, `"7d"`).

For multi-query mode, each query object has:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `query` | string | yes | Query string |
| `label` | string | no | Label for formula reference (auto-generated if omitted) |

## Response Headers

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Subscription-Ids: 1,2,3
```

## SSE Event Format

**Retry directive** (sent once at connection start):
```
retry: 5000
```
Sets the client reconnection interval to 5 seconds.

**Backfill event** (sent once on connect if `backfill: true`):
```
id: 1704067200000000000
event: backfill
data: {"series":[{"measurement":"temperature","tags":{"location":"us-west"},"fields":{"value":{"timestamps":[...],"values":[...]}}}]}
```

**Data event** (sent periodically with new data):
```
id: 1704067260000000000
event: data
data: {"series":[{"measurement":"temperature","tags":{"location":"us-west"},"fields":{"value":{"timestamps":[...],"values":[...]}}}]}
```

**Multi-query with label:**
```
id: 1704067260000000000
event: data
data: {"label":"sent","series":[...]}
```

**Drop event** (emitted when the output queue overflows):
```
id: 1704067260000000000
event: drop
data: {"droppedPoints":150}
```
For multi-query subscriptions, the `label` field identifies which query's points were dropped:
```
id: 1704067260000000000
event: drop
data: {"droppedPoints":150,"label":"sent"}
```

**Heartbeat** (keep-alive):
```
: heartbeat
```

## Error Responses

If the request is invalid, the server returns a JSON error response (not an SSE stream):

**400 Bad Request** -- malformed JSON body:
```json
{"status":"error","error":{"code":"INVALID_JSON","message":"Failed to parse subscribe request"}}
```

**400 Bad Request** -- invalid or missing query:
```json
{"status":"error","error":{"code":"INVALID_QUERY","message":"Either 'query' or 'queries' must be provided"}}
```

**429 Too Many Requests** -- subscription limit reached:
```json
{"status":"error","error":{"code":"TOO_MANY_SUBSCRIPTIONS","message":"..."}}
```

## Restrictions

- `anomalies()` and `forecast()` functions are not supported in streaming formulas
- If `formula` is set, `aggregationInterval` is required
- Single-query formulas must reference `a` (the implicit query label)
- Multi-query mode auto-generates labels `q0`, `q1`, `q2`, ... for queries that omit `label`
- Formula references must match query labels (user-provided or auto-generated)

## List Subscriptions

**Endpoint:** `GET /subscriptions`

```bash
curl http://localhost:8086/subscriptions
```

Returns active subscription metadata across all shards.

**Response:**
```json
{
  "subscriptions": [
    {
      "id": 123,
      "label": "temp",
      "measurement": "temperature",
      "fields": ["value"],
      "scopes": {"location": "us-west"},
      "handler_shard": 0,
      "queue_depth": 10,
      "queue_capacity": 1024,
      "dropped_points": 0,
      "events_sent": 42
    }
  ],
  "total_subscriptions": 1
}
```

The `label` field is omitted for single-query subscriptions.

## Limits

| Limit | Default | Config Key |
|-------|---------|------------|
| Max subscriptions/shard | 100 | `streaming.max_subscriptions_per_shard` |
| Output queue size | 1024 | `streaming.output_queue_size` |
| Heartbeat interval | 15s | `streaming.heartbeat_interval_seconds` |
