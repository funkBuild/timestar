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

### Downsampling cascade

`downsample` also accepts an **array** of tiers, applied in order as data ages.
Each tier names the age at which its resolution takes over:

```bash
curl -X PUT http://localhost:8086/retention \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "scada",
    "ttl": "730d",
    "downsample": [
      { "after": "7d",  "interval": "1m",  "method": "avg" },
      { "after": "90d", "interval": "15m", "method": "avg" }
    ]
  }'
```

Data younger than 7 days stays at its written resolution; data between 7 and
90 days old is stored as one point per minute; data older than 90 days is
stored as one point per 15 minutes; everything older than 730 days is deleted.

A single object is exactly equivalent to a one-element array, so existing
single-tier policies are unaffected.

### Per-field methods

One method per measurement destroys data: a flow totalizer averaged over a
15-minute bucket is meaningless, and a status word needs its last value, not its
mean. `fieldMethods` overrides the tier's `method` for named fields:

```bash
curl -X PUT http://localhost:8086/retention \
  -H "Content-Type: application/json" \
  -d '{
    "measurement": "scada",
    "ttl": "730d",
    "downsample": [
      { "after": "7d",  "interval": "1m",  "method": "avg",
        "fieldMethods": { "flow_total": "max", "status": "latest" } },
      { "after": "90d", "interval": "15m", "method": "avg",
        "fieldMethods": { "flow_total": "max", "status": "latest" } }
    ]
  }'
```

Every other field of `scada` still folds with `avg`. A field named in one tier
must be named in **every** tier with the same method — see Validation.

This is also what makes the `avg` approximation below tolerable: `sum`, `min`,
`max` and `latest` compose *exactly* across stages, so routing counters and
status words to them confines the inexactness to analog averages.

Omitting `fieldMethods` changes nothing: a policy without it behaves — and is
stored and returned — exactly as it was before the field existed.

#### Boolean and string fields

Boolean and string fields **are not downsampled by default**. They pass through
at their written resolution (TTL still applies). This is deliberate: folding is
destructive, and for a status word the transition history is usually the reason
it is retained at all. Note this differs from *query-time* behaviour, where an
`aggregationInterval` reduces non-numeric fields to LATEST-per-bucket — that
reduction is computed per request and discards nothing.

To fold one anyway, name it in `fieldMethods` with `latest`:

```json
"fieldMethods": { "status": "latest" }
```

Each bucket then keeps the value with the greatest timestamp inside it, stored
at the bucket start — matching the query-time rule exactly. Values stay in the
type they were written in: a boolean comes back as `true`/`false`, never `1`/`0`.

Any method other than `latest` on a boolean or string field is rejected. A
measurement-wide `"method": "latest"` does **not** enable non-numeric folding;
only an explicit `fieldMethods` entry does.

### Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `measurement` | string | yes | Measurement name |
| `ttl` | string | no* | Time-to-live duration string (e.g., `"30d"`, `"720h"`) |
| `downsample` | object **or array of objects** | no* | Downsampling policy, or an ordered cascade of up to 4 tiers |
| `downsample[].after` | string | yes | When this tier's resolution takes over (e.g., `"7d"`) |
| `downsample[].interval` | string | yes | Bucket interval for this tier (e.g., `"1h"`) |
| `downsample[].method` | string | yes | Aggregation method: `avg`, `min`, `max`, `sum`, `latest` |
| `downsample[].fieldMethods` | object | no | Per-field method overrides, `{"field": "method"}`. Absent means every field uses `method` |

*At least one of `ttl` or `downsample` is required.

Duration units: `d` (days), `h` (hours), `m` (minutes), `s` (seconds), `ms`, `us`, `ns`. Decimal values supported (e.g., `"1.5d"`).

### Validation

- 1 to 4 tiers
- `after` strictly increasing across tiers
- `interval` strictly increasing across tiers
- **each `interval` must be an exact multiple of the previous one** (`15m` = 15 × `1m`).
  Buckets are epoch-aligned, so divisibility is what guarantees that a tier-*k*
  bucket lies wholly inside one tier-*k+1* bucket rather than straddling a boundary
- `after` must be at least that tier's own `interval`
- all tiers must use the same method (this restriction may be relaxed later)
- If `ttl` is set it must be greater than the **last** tier's `after`
- Method must be one of: `avg`, `min`, `max`, `sum`, `latest`.
  `count` is deliberately not offered: a count of counts is not a count, so it
  cannot survive a second folding stage
- Every `fieldMethods` value must be from that same set
- A field must resolve to the **same** method in every tier. Because the tier
  methods already have to agree, that means a field named in one tier's
  `fieldMethods` must be named in all of them, with the same method — otherwise
  it would silently fall back to the tier method in the tiers that omit it
- Only `latest` may be given to a boolean or string field. Any other method is
  rejected rather than coerced, in line with how derived queries refuse
  non-numeric operands. (A field the index has not yet seen has no known type,
  so the policy is accepted; the compactor still refuses the fold at merge time,
  which is where the series' real type is known)

### Accuracy of cascaded aggregation

Compacted TSM V4 points retain their aggregation method, sample count,
original latest timestamp, and numeric accumulator. Each later stage combines
that state rather than treating an earlier bucket as one raw sample.

| Method | Cascaded result |
|---|---|
| `min`, `max` | Preserve extrema, including every bit of integer values. |
| `sum` | Combines sums; integer overflow fails the compaction and retains its inputs. |
| `latest` | Compares original observation timestamps, including delayed observations. |
| `avg` | Weights earlier buckets by their actual sample counts, including gaps and outages. |

Floating-point sums and averages retain normal floating-point rounding limits.
Integer averages remain integer-valued, truncating toward zero, but a persisted
128-bit sum and sample count prevent that truncation from compounding across
stages. For fractional displayed averages, ingest the field as floating point.

Separate files can hold contributions to the same bucket. Queries and later
compactions combine those contributions; equal bucket labels do not invoke raw
last-write-wins deduplication. Raw samples still deduplicate by timestamp before
folding. An aggregate NaN (for example, `sum(+Inf, -Inf)`) retains its nonzero
sample count and survives subsequent compaction; a missing raw NaN is skipped.

A delayed **additional** observation combines with the existing bucket. For
`latest`, an older delayed status cannot displace a newer observation. This does
not reconstruct deleted raw history: replaying or correcting observations that
were already incorporated into a rollup cannot generally be undone exactly.
In particular, sums and counts are not deduplicated against the erased raw
samples. Such corrections require rebuilding the affected history from its
original source. Changing the aggregation method of an existing rollup is
rejected by compaction, preserving its input files.

Query-time aggregates across different stored resolutions still operate on the
stored representatives; a mixed-resolution query is not automatically a
raw-sample-weighted average. The retained counts guarantee weighted **storage
cascades**.

**Format compatibility:** raw flushes still write V3; compaction writes V4 with
checksummed rollup metadata and input-file ancestry for crash recovery. This
binary reads V2, V3, and V4. Older binaries cannot read V4 files. Already-folded
V2/V3 data has no recoverable counts or original latest timestamps; upgrading
cannot repair information discarded by earlier downsampling. The exact-state
guarantees apply to data first folded by the V4 implementation.

### TTL against folded points

The TTL cutoff is `now - ttl`, and it is **not** aligned to any bucket grid —
unlike a downsample threshold, which is always aligned down to its own interval
so that only complete buckets fold.

A folded point is stored at its **bucket start** but summarises the whole
bucket. When the cutoff falls inside that bucket the point is dropped whole, so
up to one bucket interval of still-in-retention data disappears with it. At the
coarsest tier of the SCADA example that is up to 15 minutes, at the moment the
data turns 730 days old.

The alternative — keeping a point whose timestamp is already past the cutoff —
would leave expired data readable, so the early drop is the deliberate choice.
If the boundary matters for a measurement, set `ttl` a bucket or two beyond the
real requirement.

### When the fold actually runs

Two things trigger it.

1. **Compaction.** Every tier merge applies the cascade to its inputs. This is
   the normal path for a measurement that keeps receiving writes.
2. **The retention sweep** (`engine.retention_sweep_interval_minutes`, default
   15). A merge only happens once `files_per_merge` files accumulate, so a
   series that *stops* receiving writes — a decommissioned RTU — would otherwise
   never be re-compacted and would stay at raw resolution forever. The sweep
   finds files holding aged, still-too-dense data and rewrites them in place.

Eligible data is considered each sweep, subject to the rewrite cap and available
compaction capacity. A backlog can require multiple sweep periods.

The sweep is deliberately conservative, and these limits are worth knowing:

- It uses the resident density estimate for wholly aged series. For a series
  straddling a tier boundary, it reads block timestamps and counts occupied
  target buckets in the affected region. Already-folded singleton buckets do
  not dilute the reduction in the older region. This path costs additional I/O
  but allows quiet files to cascade while retaining their newer data.
- It requires the estimated fold to reduce the point count by at least
  `engine.downsample_rewrite_min_reduction_factor` (default 1.25x), so an
  already-folded file is not rewritten again as the threshold creeps forward.
  An already-folded series estimates at exactly 1.0x or below, so any value
  above 1 suppresses re-churn; the default is deliberately well under 2 because
  the estimate counts *buckets*, so even a 2x tier step (the smallest the
  validator admits) produces a ratio strictly below 2.
- It rewrites at most `engine.max_downsample_rewrites_per_sweep` files per shard
  per sweep (default 2; `0` disables the sweep entirely). A backlog is worked
  off over successive sweeps rather than in one burst.
- Its estimate uses each block's stored point count, which for float series
  counts only non-NaN values. A NaN-heavy series therefore looks sparser than it
  is and may not trigger — accepted, because folding NaN points changes no
  aggregate (NaN is missing, see `docs/nan_policy.md`).
- Boolean and string series trigger it only when their field carries an explicit
  `fieldMethods: {"<field>": "latest"}` entry; without one they are not folded at
  all, so a rewrite would return the same file and the sweep would propose it
  again forever. Resolving which non-numeric series opted in needs a per-series
  index lookup, so it is bounded per sweep — a large opted-in measurement warms
  up over several sweeps.
- A policy that fails validation never triggers it, for the same reason the
  compactor refuses to fold one.
- It needs to enumerate a measurement's series, and the index refuses to
  enumerate more than 10,000,000 of them. Past that the sweep **skips that
  measurement** with a warning and age-driven folding stops for it until its
  cardinality drops; merge-driven folding is unaffected. (The compaction path
  treats the same limit as an error and fails the merge, so retention is never
  silently skipped there.)
- A rewrite that fails is logged and retried on the next sweep, with no backoff.
  A file that fails deterministically is therefore re-attempted every sweep
  period; it consumes one of the per-sweep slots but cannot corrupt anything,
  since a failed rewrite leaves the original file in place.

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
    },
    "downsampleTiers": [
      {
        "after": "7d",
        "afterNanos": 604800000000000,
        "interval": "1h",
        "intervalNanos": 3600000000000,
        "method": "avg"
      }
    ]
  }
}
```

The response includes server-computed `ttlNanos`, `afterNanos`, and `intervalNanos` fields (durations converted to nanoseconds). These fields are read-only and are **not accepted on input**: `PUT` rejects any field it does not define, so a response body cannot be echoed back unmodified. A `PUT` body carries `measurement`, `ttl` and `downsample` and nothing else — in particular there is no `downsampleTiers` input field, because `downsample` already accepts the array form.

`downsampleTiers` is the canonical form. `downsample` is always returned as well
and always mirrors **tier 0** (the finest), so a client written before the
cascade existed keeps working: it reads the finest tier and simply does not see
the coarser ones. The same mirroring applies to the stored record and to the
protobuf `RetentionPolicy` message (`downsample_tiers`, field 5).

**Downgrade caveat.** Over protobuf the mirror degrades cleanly in both
directions — proto3 ignores unknown fields, so an old client reading a cascade
sees tier 0. The stored JSON record is different: a reader that rejects unknown
keys fails the *whole* record on `downsampleTiers` and drops the policy, TTL
included, rather than falling back to the mirror. The server now reads its own
retention records leniently, so a downgrade to any build from this version
forward gets the intended tier-0 behaviour; a downgrade to a build that predates
the cascade does not — it logs "Failed to parse retention policy JSON" and
behaves as if no policy were set. Nothing stored is lost (re-upgrading restores
the policy), but retention stops being applied until then.

`fieldMethods` follows the same rules and is covered by the same lenient read:
a build one version behind loads the record intact — TTL, tiers and all — and
simply folds every field with the tier `method`, so a totalizer it would have
kept as `max` is averaged until the newer binary is back. It is also omitted
from the record entirely when no overrides are named, so a policy that does not
use the feature is byte-identical to one written before it existed.

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
    {
      "measurement": "temperature",
      "ttl": "30d",
      "ttlNanos": 2592000000000000,
      "downsampleTiers": []
    },
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
      },
      "downsampleTiers": [
        {
          "after": "7d",
          "afterNanos": 604800000000000,
          "interval": "1h",
          "intervalNanos": 3600000000000,
          "method": "avg"
        }
      ]
    }
  ]
}
```

**Response (200) - single:**
```json
{
  "status": "success",
  "policy": {
    "measurement": "temperature",
    "ttl": "30d",
    "ttlNanos": 2592000000000000,
    "downsampleTiers": []
  }
}
```

`downsampleTiers` is **always** present, including as `[]` on a TTL-only policy.
`downsample` is present only when there is at least one tier (it is a disengaged
optional otherwise, which glaze omits), and `fieldMethods` likewise appears only
on a tier that names overrides.

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
