# Metadata API

Discover measurements, tags, and fields stored in the database.

## List Measurements

**Endpoint:** `GET /measurements`

```bash
curl http://localhost:8086/measurements

# With prefix filter and pagination
curl "http://localhost:8086/measurements?prefix=temp&offset=0&limit=10"
```

**Parameters:**

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `prefix` | string | none | Filter by measurement name prefix |
| `offset` | int | 0 | Pagination offset |
| `limit` | int | unlimited | Max results to return |

**Response (200):**
```json
{
  "measurements": ["temperature", "humidity", "cpu"],
  "total": 3
}
```

## Get Tags

**Endpoint:** `GET /tags`

```bash
# All tags for a measurement
curl "http://localhost:8086/tags?measurement=temperature"

# Values for a specific tag key
curl "http://localhost:8086/tags?measurement=temperature&tag=location"
```

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `measurement` | string | yes | Measurement name |
| `tag` | string | no | Specific tag key to get values for |

**Response (200) - all tags:**
```json
{
  "measurement": "temperature",
  "tags": {
    "location": ["us-west", "us-east"],
    "sensor": ["temp-01", "temp-02"]
  }
}
```

**Response (200) - specific tag:**
```json
{
  "measurement": "temperature",
  "tag": "location",
  "values": ["us-west", "us-east"]
}
```

## Get Fields

**Endpoint:** `GET /fields`

```bash
# All fields for a measurement
curl "http://localhost:8086/fields?measurement=temperature"
```

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `measurement` | string | yes | Measurement name |
| `tags` | string | no | Accepted but **not used for filtering**. The `tags` parameter is parsed (`key1:value1,key2:value2` format) and echoed back in a `filtered_by` response field, but all fields for the measurement are returned regardless of any tag values provided. Entries without a colon separator are silently dropped -- the parser stops processing the remainder of the `tags` string at the first token that lacks a `:`, so any entries after it are also lost. No error is returned. |

**Response (200):**
```json
{
  "measurement": "temperature",
  "fields": {
    "value": {"name": "value", "type": "float"},
    "humidity": {"name": "humidity", "type": "float"}
  }
}
```

> **Note:** If the measurement does not exist, the endpoint returns 200 with an empty `fields` object rather than a 404 error.

## Errors

**Missing parameter (400):**
```json
{
  "status": "error",
  "error": {"code": "MISSING_PARAMETER", "message": "measurement parameter is required"}
}
```
