# Retention API

Manage one retention policy per measurement with `PUT`, `GET`, and `DELETE
/retention`.

Partitioned cluster mode and standalone mode have different policy scope:

- Cluster v1 supports TTL only. Group 0 replicates policy versions and a
  controller fans one explicit cutoff through every VShard; replicas never
  derive a cutoff from their local clocks. Downsampling is rejected.
- Standalone mode retains the local NativeIndex policy store and supports TTL,
  downsampling, or both. Its policy `version` is zero.

When authentication is enabled, send the normal `Authorization: Bearer ...`
header on every request.

## Set a policy

`PUT /retention` with `Content-Type: application/json`:

```bash
curl -X PUT http://localhost:8086/retention \
  -H 'Content-Type: application/json' \
  -d '{
    "measurement": "temperature",
    "expectedVersion": 0,
    "ttl": "30d"
  }'
```

| Field | Type | Required | Description |
|---|---|---:|---|
| `measurement` | string | yes | Measurement name; cluster v1 allows 1–1,024 bytes and no control characters |
| `expectedVersion` | uint64 | cluster | Exact CAS version; use `0` only for a name that has never existed |
| `ttl` | string | yes in cluster | Positive finite duration such as `30d`, `720h`, or `1.5d` |
| `downsample` | object | standalone only | Local downsampling policy |
| `downsample.after` | string | with downsample | Age at which downsampling starts |
| `downsample.interval` | string | with downsample | Bucket duration |
| `downsample.method` | string | with downsample | `avg`, `min`, `max`, `sum`, or `latest` |

Duration units are `d`, `h`, `m`, `s`, `ms`, `us`, and `ns`. Cluster TTL text
is limited to 64 bytes. A TTL whose nanosecond conversion saturates the uint64
range is rejected rather than treated as an accidental never-expire policy.

A successful clustered create returns version 1:

```json
{
  "status": "success",
  "policy": {
    "measurement": "temperature",
    "version": 1,
    "ttl": "30d",
    "ttlNanos": 2592000000000000,
    "downsample": null
  }
}
```

An exact retry with the original `expectedVersion` and identical value is
idempotently successful at the same committed version. A different value or a
stale version returns HTTP 409:

```json
{
  "status": "error",
  "error": "retention policy CAS conflict",
  "leader": 3,
  "currentVersion": 4
}
```

`leader` is zero when no Group-0 leader is known. No known leader produces HTTP
503; a request sent to a known follower produces HTTP 409 with its leader hint.

## Read policies

```bash
# All live policies
curl http://localhost:8086/retention

# One measurement
curl 'http://localhost:8086/retention?measurement=temperature'
```

The list response contains only live policies:

```json
{
  "status": "success",
  "policies": [
    {
      "measurement": "temperature",
      "version": 1,
      "ttl": "30d",
      "ttlNanos": 2592000000000000,
      "downsample": null
    }
  ]
}
```

A missing or deleted single policy returns HTTP 404. In cluster mode the
response still exposes the current tombstone version:

```json
{
  "status": "error",
  "error": "No retention policy found for measurement: temperature",
  "currentVersion": 2
}
```

Use that version as `expectedVersion` to recreate a deleted policy. Requiring
the tombstone version prevents a stale client from silently resurrecting a
policy that another client deleted.

Cluster GET reads this node's applied Group-0 state. It is not a quorum read and
may briefly lag the leader, but mutation CAS is authoritative: a stale version
cannot overwrite or resurrect a newer value. Mutation and conflict responses
carry the version observed after the Group-0 proposal.

## Delete a policy

Standalone delete needs only the measurement. Cluster delete also requires its
exact current version:

```bash
curl -X DELETE \
  'http://localhost:8086/retention?measurement=temperature&expected_version=1'
```

```json
{
  "status": "success",
  "message": "Retention policy deleted for measurement: temperature",
  "version": 2
}
```

The new version identifies the durable tombstone. Retrying the same delete with
the pre-delete expected version is idempotently successful. A stale version
returns HTTP 409; an already deleted or never-created name returns HTTP 404 with
`currentVersion`.

## Protobuf

The same routes accept and return protobuf through content negotiation.
`RetentionPutRequest.expected_version` and `RetentionPolicy.version` carry the
CAS values. Generic `StatusResponse` errors/successes carry `current_version`
and `leader` hints where applicable, including delete success, conflicts, and
deleted-policy 404 responses. Cluster downsampling is rejected for protobuf and
JSON identically.

## Enforcement and observability

At most one cluster sweep is active. Group 0 stores its exact policy version,
leader-derived cutoff, global sweep ID, and first unacknowledged VShard. The
controller advances in batches of 32 only after every cutoff command in the
batch commits and applies. A controller crash therefore repeats at most one
idempotent batch.

`GET /cluster/status` exposes:

- `control_retention_policies` and `control_retention_cutoff_records`;
- `control_retention_sweep_active`, `control_retention_next_vshard`, and the
  durable `control_retention_last_sweep_id`;
- node-local controller pass/failure counters and successful cutoff
  acknowledgements.

The same measurement is not swept more often than every 15 minutes. Policy
mutation for a measurement with an active sweep conflicts until that sweep
finishes. Deleting a policy does not restore data already removed by a completed
cutoff.
