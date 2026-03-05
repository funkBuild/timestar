# Retention API

Manage data retention and downsampling policies per measurement.

## Set Retention Policy

**Endpoint:** `PUT /retention`
**Content-Type:** `application/json`

```bash
curl -X PUT http://localhost:8086/retention \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "temperature",
    "ttl": "30d",
    "downsample": {
      "after": "7d",
      "interval": "1h",
      "method": "avg"
    }
  }'
```

### Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `measurement` | string | yes | Measurement name |
| `ttl` | string | no* | Time-to-live duration string (e.g., `"30d"`, `"720h"`) |
| `downsample` | object | no* | Downsampling policy |
| `downsample.after` | string | yes | When to start downsampling (e.g., `"7d"`) |
| `downsample.interval` | string | yes | Bucket interval for downsampled data (e.g., `"1h"`) |
| `downsample.method` | string | yes | Aggregation method: `avg`, `min`, `max`, `sum`, `latest` |

*At least one of `ttl` or `downsample` is required.

Duration units: `d` (days), `h` (hours), `m` (minutes), `s` (seconds), `ms`, `us`, `ns`. Decimal values supported (e.g., `"1.5d"`).

### Validation

- If both `ttl` and `downsample` are set, TTL must be greater than `downsample.after`
- Method must be one of: `avg`, `min`, `max`, `sum`, `latest`

**Response (200):**
```json
{
  "status": "success",
  "policy": {
    "measurement": "temperature",
    "ttl": "30d",
    "ttlNanos": 2592000000000000,
    "downsample": {
      "after": "7d",
      "afterNanos": 604800000000000,
      "interval": "1h",
      "intervalNanos": 3600000000000,
      "method": "avg"
    }
  }
}
```

The response includes server-computed `ttlNanos`, `afterNanos`, and `intervalNanos` fields (durations converted to nanoseconds). These fields are read-only and ignored on input.

## Get Retention Policies

**Endpoint:** `GET /retention`

```bash
# All policies
curl http://localhost:8086/retention

# Single measurement
curl "http://localhost:8086/retention?measurement=temperature"
```

**Response (200) - all:**
```json
{
  "status": "success",
  "policies": [
    {"measurement": "temperature", "ttl": "30d", "ttlNanos": 2592000000000000},
    {
      "measurement": "cpu",
      "ttl": "90d",
      "ttlNanos": 7776000000000000,
      "downsample": {
        "after": "7d",
        "afterNanos": 604800000000000,
        "interval": "1h",
        "intervalNanos": 3600000000000,
        "method": "avg"
      }
    }
  ]
}
```

**Response (200) - single:**
```json
{
  "status": "success",
  "policy": {"measurement": "temperature", "ttl": "30d", "ttlNanos": 2592000000000000}
}
```

**Response (404):**
```json
{"status": "error", "error": "No retention policy found for measurement: unknown"}
```

## Delete Retention Policy

**Endpoint:** `DELETE /retention`

```bash
curl -X DELETE "http://localhost:8086/retention?measurement=temperature"
```

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `measurement` | string | yes | Measurement to remove policy for |

**Response (200):**
```json
{"status": "success", "message": "Retention policy deleted for measurement: temperature"}
```

**Response (404):**
```json
{"status": "error", "error": "No retention policy found for measurement: temperature"}
```
