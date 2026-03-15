# Proposal: Co-located Distributed Index

## Problem

All metadata operations serialize through shard 0's single reactor thread. This creates:

- **Write bottleneck**: Every insert from shards 1..N-1 pays a cross-shard RPC to index metadata on shard 0
- **Query bottleneck**: TAG_INDEX scans for wildcard/regex queries run on one core while N-1 cores idle
- **Redundant work**: Query discovery returns full metadata from shard 0, then each data shard re-fetches the same metadata from shard 0 again
- **Cache fragility**: Discovery cache for a measurement is flushed entirely whenever any new series is added
- **No time pruning**: Queries for "last 5 minutes" scan the same TAG_INDEX as queries for "last 90 days"

## Design Principles

Every production TSDB studied — InfluxDB, VictoriaMetrics, Prometheus, M3DB, Elasticsearch, Datadog — follows the same core principles. This design adopts all of them:

1. **Co-locate index with data** — No system centralizes its tag index. Each shard/node maintains the index for the series it owns. (All systems)
2. **Time-scope the index** — Queries should only scan index entries relevant to the queried time range. (VictoriaMetrics per-day index)
3. **Use postings lists with fast set operations** — Multi-tag queries reduce to set intersections. Roaring bitmaps or sorted arrays with smallest-first ordering. (InfluxDB, M3DB, Prometheus, Elasticsearch)
4. **Aggregate at the shard, not the coordinator** — Ship partial aggregation states, not raw data. (ClickHouse, Elasticsearch)
5. **Prune before scanning** — Use bloom filters and time bounds to skip irrelevant shards/blocks before touching the index. (M3DB, Thanos, ClickHouse)

## Storage Engine: NativeIndex

The metadata index backend is **NativeIndex** — a Seastar-native LSM tree that replaced LevelDB. Understanding its architecture is essential because this proposal builds directly on it.

### NativeIndex Components

```
NativeIndex (per-shard instance)
├── MemTable          — in-memory std::map (sorted write buffer, ~16MB)
├── Immutable MemTable — previous MemTable being flushed (brief window)
├── IndexWAL          — DMA-aligned write-ahead log (CRC32C checksums)
├── SSTableReader[]   — immutable on-disk sorted tables, FULLY CACHED IN RAM
│   ├── BlockReader   — prefix-compressed blocks with restart points
│   └── BloomFilter   — double-hashing for point-lookup acceleration
├── Manifest          — append-only file tracking (levels, file numbers)
└── CompactionEngine  — background level-based compaction (async coroutines)
```

### Key Properties for Distribution

1. **All reads are synchronous** — `kvGet()` and `kvPrefixScan()` never `co_await`. Every SSTable is fully cached in memory after `open()`. This means index reads have **zero I/O overhead** — they're pure in-memory lookups. Distributing the index across shards doesn't add I/O; it adds memory parallelism.

2. **Writes are async (WAL + flush)** — `kvPut()` appends to the IndexWAL via Seastar DMA, then inserts into MemTable. When the MemTable exceeds `write_buffer_size`, it's swapped to an immutable MemTable, a new SSTable is written, and the WAL is rotated. All async, non-blocking.

3. **No thread-pool crossings** — Unlike LevelDB (which required `seastar::async()` for every operation), NativeIndex uses Seastar coroutines and DMA natively. No external threads, no mutex contention.

4. **Batch writes** — `kvWriteBatch()` applies multiple keys atomically in a single WAL record. Currently used by `getOrCreateSeriesId()` to batch 5+ metadata entries per new series.

5. **SSTable format** — Custom "TSIX" format with Snappy compression, prefix-compressed blocks, bloom filters, and a footer index. Files are stored at `shard_N/native_index/idx_NNNNNN.sst`.

6. **Compaction** — Level-based (L0→L1→L2...) via async coroutines. Triggered when L0 exceeds 4 files. Uses MergeIterator to merge sorted streams. No external threads — runs on the Seastar reactor.

### Implications for This Proposal

The NativeIndex architecture means the "LevelDB compaction thread contention" concern from the original analysis **does not apply**. NativeIndex compaction runs as Seastar coroutines on the reactor thread — no external threads compete for CPU. This removes a major risk of per-shard index instances.

Additionally, since SSTables are fully cached in RAM, the per-shard memory cost is proportional to the total index size, not the number of instances. With N shards, each shard's NativeIndex holds ~1/N of the total data — total memory usage is approximately the same as a single centralized instance.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        EVERY SHARD (0..N-1)                     │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              NativeIndex (per-shard LSM tree)            │   │
│  │                                                         │   │
│  │  MemTable:   active writes (std::map, synchronous get)  │   │
│  │  SSTables:   immutable, fully cached in RAM             │   │
│  │  IndexWAL:   DMA-based durability                       │   │
│  │  Compaction: async coroutines (no external threads)     │   │
│  │                                                         │   │
│  │  Stores: TAG_INDEX, SERIES_METADATA, MEASUREMENT_SERIES │   │
│  │          TIME_SCOPED_POSTINGS, ID_MAP                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Postings Acceleration (in-memory)           │   │
│  │                                                         │   │
│  │  Mutable:  (meas, tagK, tagV) → sorted vec<LocalId>    │   │
│  │  Cached:   (meas, tagK, tagV) → roaring bitmap          │   │
│  │  Bloom:    per-measurement bloom filter                  │   │
│  │  HLL:      per-measurement cardinality sketches          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Schema Registry (replicated)                │   │
│  │  measurements, fields, tags, tagValues, fieldTypes       │   │
│  │  In-memory, populated via broadcast, rebuilt on startup  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Data (unchanged)                            │   │
│  │  WAL → MemoryStore → TSM files                          │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

No shard has a special role. Every shard is identical.

## Index Structure

### Shard-Local Series IDs

SeriesId128 is a 128-bit hash — too wide for efficient postings lists. Following InfluxDB and Prometheus, each shard assigns a **shard-local uint32 ID** (called `LocalId`) to each series it owns:

```
Shard 3:
  SeriesId128(0xABCD...) → LocalId 0
  SeriesId128(0x1234...) → LocalId 1
  SeriesId128(0x5678...) → LocalId 2
  ...
```

LocalIds are auto-incrementing per shard. With uint32, each shard supports up to 4 billion series — far more than any single core would hold. Sequential IDs compress extremely well in roaring bitmaps (runs of consecutive integers stored as ranges, not individual bits).

**Bidirectional mapping** stored in NativeIndex:
```
[0x10] + LocalId(4 bytes)            → SeriesId128(16 bytes)    // resolve results
[0x11] + SeriesId128(16 bytes)       → LocalId(4 bytes)         // write path lookup
```

### Postings Lists (TAG_INDEX)

The TAG_INDEX maps `(measurement, tagKey, tagValue)` → set of LocalIds. Two representations:

**Mutable layer (in-memory)**: Sorted `std::vector<uint32_t>` per postings entry. Writes append + re-sort. Intersection uses two-pointer merge.

```cpp
// Per-shard, single-threaded (no locks needed in Seastar)
tsl::robin_map<PostingsKey, std::vector<uint32_t>> mutablePostings_;
```

**Immutable segments (NativeIndex SSTables)**: Roaring bitmaps serialized into NativeIndex's SSTable files. Created when the mutable postings layer is flushed (aligned with NativeIndex's MemTable flush / SSTable compaction cycle). Since NativeIndex caches all SSTables in RAM, reading immutable bitmaps is a synchronous in-memory lookup — zero I/O.

```
[0x06] + measurement + \0 + tagKey + \0 + tagValue → roaring_bitmap(LocalIds)
```

**Query** merges results from the mutable layer and immutable segments (both in memory). Immutable segment bitmaps are intersected/unioned using CRoaring's native operations. Mutable layer vectors are converted to temporary bitmaps for intersection if large enough, or intersected directly if small.

### Time-Scoped Postings (from VictoriaMetrics)

Alongside global postings, maintain **per-day postings** that record which series were active on each day:

```
[0x0D] + day(4 bytes) + measurement + \0 + tagKey + \0 + tagValue → roaring_bitmap(LocalIds)
[0x0E] + day(4 bytes) + measurement                              → roaring_bitmap(LocalIds)
```

**Write path**: When inserting data for series X on day D, mark X as active on day D in the time-scoped postings. This is a set-add operation — idempotent, cheap.

**Query path**: For a query with time range [startDay, endDay]:
1. Union the time-scoped bitmaps across all days in range → `activeSeries`
2. For each tag scope filter, retrieve the global postings bitmap → `tagMatches`
3. Intersect: `result = activeSeries AND tagMatch1 AND tagMatch2 ...`

This prunes series that exist globally but had no data during the query range. For a database with 90-day retention queried for "last 1 hour", this eliminates series that were only active days or weeks ago.

**Storage overhead**: One bitmap per (day, measurement, tagKey, tagValue) combination. With 90-day retention and 100 unique tag combinations, this is ~9K additional NativeIndex entries per shard — small relative to the query time saved.

**Pre-population** (from VictoriaMetrics): During the last hour of each day, progressively pre-populate the next day's entries for currently-active series. This prevents a write spike at midnight.

### Series Metadata

```
[0x05] + LocalId(4 bytes) → SeriesMetadata(measurement, tags, field)
```

Stored in the shard-local NativeIndex. No cross-shard lookup needed — the shard that owns a series also owns its metadata.

### Bloom Filters (from M3DB)

Each shard maintains an in-memory bloom filter of its active LocalIds, keyed by measurement:

```cpp
tsl::robin_map<std::string, BloomFilter> measurementBloom_;  // measurement → bloom
```

**Write path**: Add LocalId to the measurement's bloom filter on insert.
**Query path**: Before scanning TAG_INDEX for a measurement, check the bloom filter. If `bloomFilter.mightContain(measurement)` returns false, skip this shard entirely — it has no data for this measurement. Cost: ~10 bits per series, checked in nanoseconds.

TimeStar already has SIMD-accelerated bloom filters (Highway-based). Reuse the existing implementation.

### HyperLogLog Sketches (from InfluxDB)

Per-measurement HLL sketches provide O(1) cardinality estimates:

```cpp
tsl::robin_map<std::string, HyperLogLog> measurementHLL_;     // measurement → HLL
tsl::robin_map<PostingsKey, HyperLogLog> tagCardinalityHLL_;   // (meas, tagK, tagV) → HLL
```

**Uses**:
- Query planning: Choose intersection order (smallest-first) without reading full postings lists
- `SHOW SERIES CARDINALITY` equivalent queries: Return approximate count in O(1)
- HLL sketches are mergeable across shards: coordinator unions per-shard HLLs for global estimate

### Schema Registry (replicated in-memory)

Small, per-measurement metadata replicated to all shards via broadcast:

| Data | Characteristics |
|------|-----------------|
| MEASUREMENT_FIELDS | Append-only set, bounded by unique fields |
| MEASUREMENT_TAGS | Append-only set, bounded by unique tag keys |
| TAG_VALUES | Append-only set per tag key |
| FIELD_TYPE | Write-once per measurement+field |
| RETENTION_POLICY | Rarely changes |

These are already cached in-memory on shard 0. The change: replicate them to **all** shards so metadata API reads are always local.

**Broadcast protocol**: When a shard discovers a new field/tag/value during insert:
```cpp
if (hasNewSchema) {
    SchemaUpdate update{measurement, newFields, newTags, newTagValues, newFieldTypes};
    co_await shardedRef->invoke_on_all([update](Engine& e) {
        e.applySchemaUpdate(update);  // idempotent set merge
    });
}
```

Schema broadcasts occur only on **first occurrence** of a new field/tag/value. In steady state: zero broadcasts.

**Durability**: Schema is reconstructed from local SERIES_METADATA on startup — each shard scans its series to rebuild field/tag/value sets. No separate persistence needed.

## Write Path

### Current

```
HTTP Write → parse JSON → route data to shard X by hash
  Shard X: WAL + MemoryStore (local)
  Shard X: invoke_on(0, indexMetadataSync(metaOps))     ← CROSS-SHARD RPC
    Shard 0: indexMetadataBatch() → NativeIndex kvWriteBatch
    Shard 0: invalidateDiscoveryCache(measurement)
  Shard X: return HTTP 200
```

### Proposed

```
HTTP Write → parse JSON → route data to shard X by hash
  Shard X:
    1. WAL + MemoryStore                                  ← unchanged
    2. getOrAssign LocalId for SeriesId128                ← local hash map lookup
    3. Add LocalId to mutable postings for each tag       ← local, O(1) amortized
    4. Add LocalId to time-scoped postings for today      ← local, O(1) amortized
    5. Update bloom filter with LocalId                   ← local, O(1)
    6. Update HLL sketch for measurement                  ← local, O(1)
    7. If new series: write SeriesMetadata to NativeIndex  ← local
    8. If new schema: broadcast to all shards             ← rare
  Shard X: return HTTP 200
```

**Zero cross-shard RPCs on the write path.** All index operations are local. Schema broadcasts are fire-and-forget for already-known fields/tags (the common case).

## Query Path

### Current

```
HTTP Query → parse
  invoke_on(0, findSeriesWithMetadataCached())            ← 1 RPC TO SHARD 0
    Shard 0: scan TAG_INDEX (single-threaded)
    Shard 0: batch getSeriesMetadata
    Shard 0: return SeriesWithMetadata[]
  Hash series → data shards
  Per data shard S:
    invoke_on(S, executeLocalQuery)
      invoke_on(0, getSeriesMetadataBatch(seriesIds))     ← 1 MORE RPC PER SHARD
      run query on local TSM/MemoryStore
  Merge results across shards

RPCs per query: 1 + S (S = shards with data). Metadata fetched twice.
```

### Proposed: Four-Stage Pipeline

```
HTTP Query → parse → invoke_on_all(localDiscoverAndQuery)

  ══════════════════════════════════════════════════════════
  Each shard runs stages 1-4 locally, in parallel:
  ══════════════════════════════════════════════════════════

  Stage 1 — PRUNE
    a. Check measurement bloom filter → skip shard if miss
    b. Check time-scoped postings for [startDay..endDay]
       → build activeSeries bitmap (series active in range)
    c. If activeSeries is empty → skip shard

  Stage 2 — MATCH
    a. For each tag scope filter:
       Retrieve postings bitmap (merge mutable + immutable segments)
    b. Sort postings lists by estimated cardinality (HLL, smallest first)
    c. Intersect bitmaps: result = activeSeries ∩ tag1 ∩ tag2 ∩ ...
    d. Apply field filter if specified
    e. Resolve LocalIds → SeriesMetadata (local NativeIndex, sync)

  Stage 3 — EXECUTE
    a. For each matched series:
       Query local TSM files + MemoryStore
    b. Apply aggregation pushdown where possible
       (fold directly into BlockAggregator, skip materialization)

  Stage 4 — AGGREGATE
    a. Compute partial AggregationState per group
    b. Return PartialAggregationResult to coordinator

  ══════════════════════════════════════════════════════════
  Coordinator shard: merge partial aggregations from all shards
  ══════════════════════════════════════════════════════════

RPCs per query: 0 (single server). All work is shard-local.
```

### Stage 2 Detail: Smallest-First Intersection (from Prometheus)

When a query has multiple tag constraints (e.g., `{location:us-west, host:server-*}`):

```
1. Retrieve postings for each constraint:
     location=us-west  → bitmap A (500 series)
     host=server-*     → bitmap B (10,000 series)

2. Sort by cardinality: A (500) < B (10,000)

3. Intersect smallest first:
     result = A ∩ B
     // Iterate 500 entries in A, seek in B for each
     // Much faster than iterating 10,000 entries in B
```

Prometheus measured **63-96% faster** multi-tag queries with this ordering. With roaring bitmaps, the intersection is even faster — CRoaring's `roaring_bitmap_and()` operates on compressed containers directly.

### Performance Improvement

| Query Type | Current | Proposed | Speedup |
|------------|---------|----------|---------|
| Exact tag, 1h range | 1 core, full TAG_INDEX scan | N cores, time-pruned, bloom-filtered | **N× + time pruning** |
| Wildcard, 10K series | 10K entries on 1 core | ~10K/N per core, bitmap intersection | **N×** |
| Multi-tag intersection | Sorted merge on 1 core | Roaring AND, smallest-first, N cores | **N× + 2-10× from bitmaps** |
| No filters, all series | Full scan on 1 core | Bloom prune + 1/N scan per core | **N×** |
| Narrow time range (5m) | Same cost as 90d query | Time-scoped pruning skips inactive series | **10-100×** |

## Delete Path

### Current

```
HTTP DELETE → parse
  invoke_on(0, findSeries(measurement, tags))              ← RPC TO SHARD 0
    Shard 0: scan TAG_INDEX → return matching SeriesId128[]
  invoke_on(0, getSeriesMetadata(id) per series)           ← SEQUENTIAL RPCs TO SHARD 0
    Shard 0: resolve each id → SeriesMetadata
    Filter by field if specified
    Reconstruct seriesKey strings
  For each matching series:
    invoke_on(dataShard, deleteRangeImpl(seriesKey))        ← RPC TO DATA SHARD
    Create TSM tombstones
```

Two problems: discovery serializes on shard 0, and metadata lookups are sequential (not batched).

### Proposed: Scatter-Gather Delete

```
HTTP DELETE → parse
  invoke_on_all(localDeleteByPattern(measurement, tags, fields, timeRange))
    Each shard:
      1. Scan LOCAL TAG_INDEX for matching series             ← PARALLEL
      2. Resolve LocalIds → SeriesMetadata locally            ← LOCAL
      3. Apply field filter if specified
      4. Create TSM tombstones for matched series             ← LOCAL, no hop
      5. Mark series as deleted in local index (soft delete)
      6. Return count of deleted series
  Coordinator: sum deletion counts from all shards
```

**RPCs per delete: 0 (single server).** Each shard discovers and deletes its own series. No second hop needed because index and data are co-located.

### `deleteRangeBySeries` (Targeted Delete)

Current: `getSeriesId(measurement, tags, field)` on shard 0 to check existence. With distributed index, the target shard is deterministic:

```cpp
SeriesId128 seriesId = SeriesId128::fromSeriesKey(buildSeriesKey(measurement, tags, field));
unsigned targetShard = SeriesId128::Hash{}(seriesId) % smp::count;

co_await shardedRef->invoke_on(targetShard, [seriesId, startTime, endTime](Engine& e) {
    // Check existence in LOCAL index, create tombstone if found
    return e.deleteRangeLocal(seriesId, startTime, endTime);
});
```

**1 RPC to the known target shard** instead of 1 RPC to shard 0 + 1 RPC to data shard. When the request already lands on the target shard: zero RPCs.

### Index Cleanup on Delete

Current design: deleted series remain in the index permanently (tombstones in TSM, no index update). This carries forward — deletes create TSM tombstones, not index removals. Index entries for deleted series are harmless: queries find the series in the index but read zero data points (tombstones filter them out).

For long-term cleanup, a background **index garbage collection** sweep can remove entries for series with no remaining data across any TSM file. This is a Phase 6 optimization — not required for correctness.

## Retention Sweep

### Current

```
Timer (every 15 min, shard 0):
  For each measurement with TTL policy:
    invoke_on(0, getAllSeriesForMeasurement(measurement))    ← SHARD 0
    Build seriesMeasurementMap
    invoke_on_all(sweepExpiredFiles)                          ← ALL SHARDS
      Each shard: walk TSM files, delete fully expired files
```

### Proposed: Per-Shard Sweep

With co-located index, each shard knows its own series. The sweep becomes fully local:

```
Timer (every 15 min, each shard independently):
  For each measurement with TTL policy (from local schema registry):
    Scan local MEASUREMENT_SERIES index → get series for this shard
    Walk local TSM files, delete fully expired files
    Clean up time-scoped postings entries older than retention period
```

No cross-shard RPCs. Each shard sweeps independently. Retention policies are in the replicated schema registry, so every shard has them locally.

## Retention Policy CRUD

### Current

`setRetentionPolicy` / `getRetentionPolicy` / `deleteRetentionPolicy` are routed to shard 0's NativeIndex via `invoke_on(0, ...)`.

### Proposed

Retention policies are part of the schema registry (replicated). CRUD operations:

```
PUT  /retention → write to local NativeIndex + invoke_on_all(applySchemaUpdate)
GET  /retention → read from local schema registry cache (any shard)
DELETE /retention → delete from local NativeIndex + invoke_on_all(removeRetentionPolicy)
```

Writes go to the local shard's NativeIndex for persistence, then broadcast to all shards. Reads are served from the local in-memory cache. Same pattern as field/tag schema updates.

## Metadata API

```
GET /measurements → local schema registry (any shard)      ← NO RPC
GET /tags         → local schema registry (any shard)      ← NO RPC
GET /fields       → local schema registry (any shard)      ← NO RPC
```

Schema data is replicated, so any shard can answer these queries locally without cross-shard communication.

## Field Type Consistency

FIELD_TYPE is keyed by `(measurement, field)` — not per-series. It records whether a field stores float, boolean, string, or integer data. With centralized index, shard 0 is the single writer — no conflicts possible.

With distributed index, two shards could simultaneously receive first writes for the same `(measurement, field)` with different types. This is a correctness issue.

### Resolution: First-Writer-Wins via Schema Broadcast

```cpp
// In applySchemaUpdate(), receiving shard checks for conflict:
void Engine::applySchemaUpdate(const SchemaUpdate& update) {
    for (const auto& [field, newType] : update.newFieldTypes) {
        auto it = fieldTypeCache_.find({update.measurement, field});
        if (it != fieldTypeCache_.end() && it->second != newType) {
            // Conflict: field already registered with different type.
            // First-writer-wins: keep existing type, log warning.
            timestar::engine_log.warn(
                "Field type conflict: {}.{} is {} locally but {} from broadcast. Keeping existing.",
                update.measurement, field, it->second, newType);
            continue;
        }
        fieldTypeCache_[{update.measurement, field}] = newType;
    }
}
```

The write handler already validates type consistency within a batch (`http_write_handler.cpp`). Cross-shard conflicts are extremely rare — they require two shards to receive the very first write for the same `(measurement, field)` simultaneously with different types. If it happens, first-writer-wins is safe: subsequent writes with the wrong type will be rejected by the write handler's per-batch validation (it checks the local type cache before accepting data).

In practice, all fields for a measurement are created by the same client in the same batch, so they arrive on the same shard. Cross-shard type conflicts are a theoretical edge case, not a practical concern.

## Series Key Compatibility

The existing codebase uses string-based series keys (`"measurement,tag1=val1 field"`) throughout:
- `QueryRunner::runQuery(seriesKey, seriesId, startTime, endTime)`
- `TSMFileManager` / `MemoryStore` index by `SeriesId128`
- `deleteRangeImpl(seriesKey, startTime, endTime)`
- `buildSeriesKey()` called in 6+ locations

**LocalId is internal to the index layer only.** After discovery resolves LocalIds → SeriesId128 + SeriesMetadata, the rest of the pipeline uses SeriesId128 and string series keys unchanged:

```
Index layer:  LocalId → (SeriesId128, SeriesMetadata)
                              ↓
Data layer:   SeriesId128 → TSM files, MemoryStore, WAL  (unchanged)
              seriesKey → QueryRunner::runQuery()         (unchanged)
```

No changes needed to QueryRunner, TSMFileManager, WALFileManager, MemoryStore, or any data-path code. The LocalId mapping is encapsulated within NativeIndex (aliased as `LevelDBIndex`).

## Write Path `knownSeriesCache` Compatibility

The HTTP write handler maintains a `thread_local robin_set<SeriesId128>` (`knownSeriesCache`) that tracks which series have been indexed. Currently it guards against redundant `indexMetadataSync` RPCs to shard 0.

With distributed index, this cache remains valid and becomes more effective:
- **Before**: Guards against redundant cross-shard RPCs (saves ~5-15μs per hit)
- **After**: Guards against redundant local index writes (saves ~1-3μs per hit)

The cache is keyed by `SeriesId128`, which is deterministic. A cache hit means the series has been indexed on its owning shard — still correct in the distributed model. No changes needed.

## `getSeriesType` Compatibility

`getSeriesType()` determines a field's data type (float/bool/string/int) by checking TSM file headers and MemoryStore variants. It does NOT use the metadata index — it's purely data-local:

```
TSMFileManager::getSeriesType(seriesId)
  → for each TSM file: bloom filter check → sparse index lookup → return type
WALFileManager::getSeriesType(seriesId)
  → for each MemoryStore: variant index check → return type
```

This is already per-shard and requires no changes.

## Consistency Model

| Data Type | Consistency | Guarantee |
|-----------|-------------|-----------|
| Series-local index | **Strong per-shard** | Written before HTTP 200; immediately queryable on same shard |
| Schema registry | **Eventual** | Broadcast after local write; ~μs window where other shards don't see new field/tag |
| Query discovery | **Snapshot per-shard** | Each shard returns a consistent snapshot of its local series |

**Same-shard reads after writes**: Strongly consistent. The inserting shard writes index entries locally before returning HTTP 200.

**Cross-shard schema reads after writes**: Eventually consistent. In practice, not observable — Seastar's `invoke_on_all` completes within the same reactor loop iteration, and the HTTP response is sent after the broadcast returns.

## Multi-Server Extension

### Cluster Membership and Service Discovery

Every server in the cluster must know about every other server. Two options:

**Option A — Coordination service (recommended for initial deployment):**
- Use etcd (or ZooKeeper) to store the placement table and server list
- Each server registers itself on startup via a lease with TTL
- Other servers watch for membership changes
- etcd handles leader election if needed for coordinated operations
- Well-understood, battle-tested (used by M3DB, Kubernetes, CockroachDB)

**Option B — Gossip-based membership (for large clusters, 50+ servers):**
- Each server maintains a heartbeat with K random peers (SWIM protocol)
- Failure detection via suspicion + confirmation (avoids false positives)
- Membership converges in O(log S) rounds where S = server count
- No external dependency

**Recommendation**: Start with etcd (Option A) for clusters up to ~50 servers. Add gossip (Option B) as an optimization for larger deployments.

### Virtual Shard Placement (from M3DB)

Instead of mapping directly to physical cores, use a **virtual shard** indirection layer:

```
SeriesId128::Hash{} % V → virtual_shard    (V = 4096 or similar power of 2)
placement[virtual_shard] → (server, core)   (stored in etcd)
```

- **V is fixed** (e.g., 4096) — never changes, even as servers are added/removed
- **Placement** maps virtual shards to physical (server, core) pairs, stored in etcd
- **Placement changes** are watched by all servers — each server caches the full placement table locally and updates it on etcd change notifications
- **Rebalancing**: Move virtual shards between servers by updating placement in etcd, then migrating data/index for affected shards (see Rebalancing section)

**Single-server**: V virtual shards mapped round-robin to N cores. Multiple virtual shards per core. The shard-local index covers all virtual shards assigned to that core.

**Multi-server**: Same V virtual shards, but placement distributes them across servers:
```
Virtual shards 0-1023   → Server 0, cores 0-15
Virtual shards 1024-2047 → Server 1, cores 0-15
Virtual shards 2048-3071 → Server 2, cores 0-15
Virtual shards 3072-4095 → Server 3, cores 0-15
```

All routing in the cluster — writes, queries, deletes — uses virtual shard placement for addressing:
```cpp
virtual_shard = SeriesId128::Hash{}(seriesId) % V;
auto [server, core] = placement[virtual_shard];
```

This replaces the single-server `hash % smp::count` with a two-level scheme that extends naturally to multi-server.

### Write Path (Multi-Server)

```
HTTP Write → any server
  Compute virtual_shard = hash(seriesKey) % V
  Look up (server, core) = placement[virtual_shard]
  If local server:
    Route to local core → local write + local index       ← zero-RPC
  If remote server:
    Batch with other writes targeting same server          ← coalesce
    Forward batch to remote server via RPC                 ← one network hop
    Remote server routes each write to local core          ← zero-RPC
  Schema broadcast:
    Local: invoke_on_all                                   ← zero-cost
    Remote: gossip to peer servers                         ← rare, distributed
```

**Write forwarding failure handling:**
- If remote server is unreachable: return HTTP 503 (Service Unavailable) to client with `Retry-After` header. Do NOT buffer locally — the client must retry to the correct server. This preserves at-most-once semantics and avoids split-brain writes.
- Timeout: configurable per-write deadline (default 5s). If remote server doesn't ack within deadline, return 503.
- **Durability guarantee**: Write is acknowledged to the client only after the target server has written to its local WAL. The forwarding server does not persist the data — it is a stateless proxy for non-local writes.

**Write batching:**
- Writes arriving in the same HTTP batch targeting the same remote server are coalesced into a single network RPC. The HTTP write handler already groups inserts by shard — extend this grouping to group by (server, core).

### Replication

The initial multi-server design uses **replication factor 1** — each series lives on exactly one server. This is acceptable for use cases where:
- The storage layer is durable (local NVMe with RAID, or cloud block storage with replication)
- WAL provides crash recovery within a single server
- Backup/restore provides disaster recovery

For higher durability, add replication in a subsequent phase:

**Replication factor R (future):**
- Each virtual shard is assigned to R servers in the placement table
- Writes are forwarded to all R servers (quorum write: ack after W of R succeed, where W = ceil(R/2) + 1)
- Queries can be served by any of the R replicas (adaptive replica selection — route to fastest)
- Conflict resolution: last-write-wins by timestamp (TSDBs are append-mostly; updates are rare)
- Replica repair: anti-entropy sweep compares TSM file manifests between replicas, copies missing blocks

This follows the M3DB/Cassandra model. Not needed for Phase 5 — add in Phase 7.

### Query Path (Multi-Server)

```
HTTP Query → any server (coordinator)
  1. Look up measurement → server bitmap                    ← skip uninvolved servers
  2. Fan-out to relevant servers in parallel:               ← 1 RPC per remote server
       Each server: invoke_on_all locally                   ← zero-cost
         Each core: Stage 1-4 pipeline (prune → match → execute → aggregate)
       Each server: merge local partial results
       Each server: stream PartialAggregationResult back
  3. Coordinator: merge server-level partial results
       as they arrive (streaming, not blocking on slowest)
  4. Apply query deadline: if any server hasn't responded
       within timeout, return partial results + warning
  Return final response
```

**Timeout and partial results:**
- Query deadline propagated to all servers in the RPC metadata (e.g., `deadline_ns` field)
- Each remote server enforces the deadline locally — stops work and returns whatever it has
- Coordinator merges available results. If any server timed out, the response includes a `warnings` array: `["server-2 timed out after 5000ms, results may be incomplete"]`
- Client sees `"partial": true` flag in response when results are incomplete

**Backpressure:**
- Each server streams partial results to the coordinator (not one large response)
- Coordinator applies `maxTotalPoints` limit across all servers — once the limit is reached, cancel outstanding RPCs
- Use Seastar's `output_stream<char>` for streaming results over the network

For 4 servers × 16 cores = 64 shards: **up to 3 network RPCs** (to remote servers, reduced by measurement bitmap) + coordinator runs locally. Each server internally fans out to 16 cores at zero cost.

### Delete Path (Multi-Server)

```
HTTP DELETE → any server (coordinator)
  Scatter-gather to ALL servers:                            ← 1 RPC per remote server
    Each server: invoke_on_all(localDeleteByPattern)        ← zero-cost
      Each core: scan local TAG_INDEX, create tombstones
    Each server: return count
  Coordinator: sum counts, return total
```

For targeted `deleteRangeBySeries`:
```
  Compute virtual_shard = hash(seriesKey) % V
  Look up (server, core) = placement[virtual_shard]
  Forward delete to (server, core)                          ← 1 RPC if remote
```

### Fan-Out Optimization

**Measurement → server bitmap**: A small replicated structure tracking which servers have data for each measurement. Updated atomically with schema broadcasts — when a server broadcasts a new measurement, it includes its server ID. All receivers update their local bitmap.

```cpp
// Per-server, updated on schema broadcast receipt
tsl::robin_map<std::string, std::bitset<MAX_SERVERS>> measurementServerMap_;
```

For sparse measurements (data on 2 of 10 servers), this reduces network fan-out from 9 RPCs to 1. Staleness is bounded by schema broadcast latency (typically <100ms). A stale bitmap may cause a query to hit an extra server (returns empty results) — never causes missing results.

**Adaptive Replica Selection (future, with replication)**: When replicas exist, route to the replica most likely to respond fastest based on recent response times, service times, and queue depth. Reduces tail latency.

### Schema and Retention Broadcast (Multi-Server)

**Intra-server**: `invoke_on_all` (unchanged, zero-cost).

**Inter-server**: Gossip protocol with bounded convergence. Each server maintains persistent TCP connections to K random peers (default K=3, or all peers if cluster < 10 servers). Schema and retention policy updates propagate via epidemic broadcast:

1. Originating server sends update to K peers
2. Each peer forwards to K of its own peers (excluding sender)
3. Duplicate updates are idempotent (set merge) — no harm in receiving twice
4. Convergence: O(log S) rounds for S servers, each round ~1 RTT

**Why gossip instead of all-to-all broadcast:**
- All-to-all requires O(S²) connections (untenable at 100+ servers)
- Gossip requires O(K × S) messages total (K is constant, typically 3)
- Gossip tolerates server failures — messages route around dead nodes
- Gossip handles network partitions gracefully — converges when partition heals

**Schema payloads** (all are small, rare, and idempotent):
- New measurement fields: `{measurement, set<field>}`
- New measurement tags: `{measurement, set<tagKey>}`
- New tag values: `{measurement, tagKey, set<tagValue>}`
- New field types: `{measurement, field, type}`
- Retention policy changes: `{measurement, RetentionPolicy}`
- Server measurement bitmap update: `{serverId, measurement}`

**Startup and recovery**: On startup, a server requests a full schema snapshot from any peer. The peer scans its local SERIES_METADATA and sends the union of all known schema. This bootstraps the new server's schema registry in seconds.

### Field Type Consistency (Multi-Server)

On a single server, the race window for field type conflicts is ~microseconds (one reactor loop). Across servers, it is ~milliseconds (network RTT). This makes conflicts more likely.

**Resolution: Optimistic registration with conflict rejection.**

```
Write arrives on Server A for field "temperature.value" as float:
  1. Server A checks local fieldTypeCache → not found (first write)
  2. Server A registers type locally as "float"
  3. Server A broadcasts SchemaUpdate{temperature, value, float} via gossip

Meanwhile, write arrives on Server B for "temperature.value" as integer:
  1. Server B checks local fieldTypeCache → not found (broadcast hasn't arrived yet)
  2. Server B registers type locally as "integer"
  3. Server B broadcasts SchemaUpdate{temperature, value, integer} via gossip

Gossip convergence (within ~100ms):
  4. Server A receives B's broadcast: "temperature.value = integer"
     → Conflict detected: local says "float", broadcast says "integer"
     → First-writer-wins: A keeps "float", logs warning
  5. Server B receives A's broadcast: "temperature.value = float"
     → Conflict detected: local says "integer", broadcast says "float"
     → First-writer-wins: B keeps "integer", logs warning

Result: Server A thinks float, Server B thinks integer — DIVERGENCE.
```

**This is the fundamental problem.** First-writer-wins is non-deterministic across servers. To fix it, use **deterministic conflict resolution**:

```cpp
void Engine::applySchemaUpdate(const SchemaUpdate& update) {
    for (const auto& [field, newType] : update.newFieldTypes) {
        auto it = fieldTypeCache_.find({update.measurement, field});
        if (it != fieldTypeCache_.end() && it->second != newType) {
            // Conflict: deterministic resolution — lexicographically smaller type wins.
            // Both servers independently arrive at the same answer.
            if (newType < it->second) {
                timestar::engine_log.warn(
                    "Field type conflict: {}.{} changing from {} to {} (deterministic resolution)",
                    update.measurement, field, it->second, newType);
                it->second = newType;
                // Re-broadcast the winning type so all servers converge
            }
            // else: local type wins, no change needed
            continue;
        }
        fieldTypeCache_[{update.measurement, field}] = newType;
    }
}
```

With deterministic resolution (lexicographically smaller type wins: "boolean" < "float" < "integer" < "string"), all servers converge to the same answer regardless of message ordering. The "losing" server's already-written data may have the wrong type — a background repair sweep can detect and fix this (convert or quarantine mismatched points).

In practice, this conflict is extremely rare (requires two servers to receive the very first write for the same field simultaneously with different types). The deterministic resolution ensures correctness even when it does occur.

### Clock Synchronization and Time-Scoped Postings

Time-scoped postings are keyed by day. In a cluster, servers may have slightly different clocks. If server A's clock is 2 minutes ahead of server B's, near midnight they could write to different day buckets for the same wall-clock instant.

**Resolution**: Use the **data's timestamp** (from the write request), not the server's wall clock, to determine the day bucket. Since data timestamps are provided by the client (or generated once by the receiving server), they are consistent across the cluster. This also means historical backfill writes are correctly bucketed by the data's actual date, not the insertion date.

```cpp
uint32_t dayBucket = static_cast<uint32_t>(dataTimestampNs / NS_PER_DAY);
```

### Virtual Shard Rebalancing

When the cluster topology changes (server added, removed, or failed), virtual shards must be redistributed. This is the most operationally complex part of clustering.

**Rebalancing protocol (freeze-migrate-unfreeze):**

```
1. PLAN: Compute new placement (e.g., move vshards 512-767 from server 0 to server 4)
   Write new placement to etcd with status="migrating" for affected vshards

2. FREEZE: Affected vshards enter read-only mode
   - Writes to frozen vshards are rejected with HTTP 503 + Retry-After
   - Queries still served from old server (data is still there)
   - Duration: seconds (time to stream data)

3. MIGRATE: Stream data + index from old server to new server
   - TSM files: bulk copy via network (largest volume, but sequential I/O)
   - NativeIndex: snapshot SSTables + stream (immutable files, consistent)
   - WAL: replay entries for affected vshards
   - LocalId counters: transfer max counter value for each vshard

4. VERIFY: New server confirms data integrity
   - Compare TSM file checksums
   - Verify index entry counts match
   - Confirm LocalId counter continuity

5. ACTIVATE: Update placement in etcd to point affected vshards to new server
   - All servers observe the placement change via etcd watch
   - Writes to affected vshards now route to new server
   - Old server marks affected vshard data as "pending cleanup"

6. CLEANUP: Old server deletes migrated data after a grace period (e.g., 1 hour)
   - Grace period allows in-flight queries to complete
   - After grace period: delete TSM files, NativeIndex SSTables, WAL segments
```

**Failure during migration:**
- If migration fails at any step before ACTIVATE: abort, revert to old placement, unfreeze
- If new server crashes after ACTIVATE: old server still has data (grace period), revert placement
- If old server crashes during migration: new server has partial data. Either complete migration from another replica (if replication enabled) or accept data loss for the affected vshards

**Minimizing freeze duration:**
- Pre-copy: start streaming TSM files before freezing (they're immutable, so consistent)
- Only freeze for the WAL tail: the few seconds of writes between pre-copy completion and freeze
- For most vshards, freeze duration is <5 seconds

### Mutable Postings Durability

In-memory mutable postings are lost on server crash. After restart, the server must rebuild them by scanning local SERIES_METADATA via NativeIndex's `kvPrefixScan()` (synchronous once SSTables are loaded). During rebuild:

- The server is marked as "warming up" in etcd
- Queries that hit a warming server receive a header: `X-TimeStar-Warming: true`
- The coordinator can optionally skip warming servers and return partial results, or wait for rebuild to complete
- Rebuild time: proportional to series count. For 1M series per shard × 16 shards: ~5-15 seconds (NativeIndex `kvPrefixScan()` — all in RAM, no I/O)

For faster recovery, postings can be persisted as a dedicated SSTable on each MemoryStore rollover. On restart, load the postings SSTable directly instead of scanning all SERIES_METADATA entries.

### Network Protocol

Inter-server communication uses Seastar's built-in RPC framework (`seastar::rpc::protocol`). It provides:
- Connection multiplexing over a single TCP connection per peer
- Async request/response with futures
- Custom serialization via `seastar::rpc::serialize()` / `deserialize()`
- Timeout support per-request
- Compression (optional, LZ4)

**Serialization for key types:**
- `PartialAggregationResult`: Serialize `AggregationState` fields (sum, count, min, max, M2, firstTime, latestTime, firstValue, latestValue) as fixed-width doubles/uint64s. Bucket maps serialized as sorted `(timestamp, state)` pairs.
- `SchemaUpdate`: Serialize as length-prefixed strings (measurement, field names, tag keys/values, type strings). Small payloads (~100-500 bytes).
- `Roaring bitmaps`: Use CRoaring's `roaring_bitmap_portable_serialize()` — produces a portable byte array that can be embedded directly in RPC messages.
- `TSM data` (for rebalancing): Stream raw file bytes. TSM files are self-describing with checksums.

**Connection topology:**
- Each server maintains one persistent TCP connection to every other server (via Seastar RPC)
- For clusters up to ~50 servers, this is manageable (~50 connections per server)
- For larger clusters: connection pooling with on-demand connections to infrequent peers

## Performance Analysis

### Write Path

| Metric | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| Cross-shard RPCs per write batch | 1 (indexMetadataSync) | 0 (local write) | **Eliminated** |
| NativeIndex write contention | All shards compete for shard 0 | Each shard writes to own NativeIndex | **No contention** |
| Shard 0 reactor utilization | Handles all metadata from all shards | Handles only its own 1/N share | **N× less load** |
| Index update cost per insert | NativeIndex kvWriteBatch (multi-key) | In-memory postings append + bloom update | **~10× faster (memory vs WAL)** |

### Query Path

| Metric | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| Discovery RPCs | 1 to shard 0 | 0 (local per-shard) | **Eliminated** |
| Metadata re-fetch RPCs | S (one per data shard) | 0 (co-located) | **Eliminated** |
| TAG_INDEX scan parallelism | 1 core | N cores | **N×** |
| Time range pruning | None (scan all) | Per-day scoped postings | **10-100× for narrow queries** |
| Multi-tag intersection | Sorted vector merge | Roaring bitmap AND, smallest-first | **2-10× from bitmaps, 2-10× from ordering** |
| Shard skip rate | 0% (always query shard 0) | Bloom filter prunes empty shards | **Skip ~30-70% of shards** |

### Resource Usage

| Resource | Current | Proposed |
|----------|---------|----------|
| NativeIndex instances | 1 (shard 0) | N (one per shard) |
| Total index disk | X | ~1.3X (time-scoped entries add ~30%) |
| NativeIndex memory (SSTable cache) | Concentrated on shard 0 | Distributed evenly across shards (~same total) |
| In-memory postings | None | ~50-100 bytes per series per shard (mutable layer) |
| Bloom filters | None for index | ~10 bits per series per shard |
| HLL sketches | None | ~16KB per measurement per shard |
| Schema memory | Shard 0 only | Replicated N× (small: typically <1MB total) |

## Compaction in Distributed NativeIndex

Unlike LevelDB (which runs compaction in external threads outside Seastar's control), NativeIndex compaction runs as **Seastar async coroutines on the reactor thread**. This eliminates the thread contention concern entirely:

- No external threads — compaction is cooperative, yields to other coroutines
- No mutex contention — single-threaded per shard
- No reactor stalls from thread scheduling — compaction I/O is DMA-based

With N NativeIndex instances (one per shard), each instance is 1/N the size → compaction runs are shorter and less frequent per instance. Total compaction work across all shards is approximately the same as a single centralized instance, but spread across N reactor threads running independently.

**Tuning per-shard**: Scale `write_buffer_size` by 1/N to keep total MemTable memory constant. Each shard flushes smaller MemTables more frequently, producing smaller SSTables. The L0→L1 compaction threshold (default 4 files) applies independently per shard.

## Existing Infrastructure

The codebase already has scaffolding for this design:

- **Per-shard NativeIndex**: `Engine::index` member (type alias `LevelDBIndex = NativeIndex`), constructed with `shardId` (`engine.cpp:22`). Each shard already has an instance — only shard 0's is opened.
- **Per-shard index directory**: `shard_N/native_index/` path already computed in NativeIndex constructor
- **Seastar-native LSM tree**: NativeIndex provides `kvGet()` (sync), `kvPut()` (async), `kvPrefixScan()` (sync), `kvWriteBatch()` (async) — all Seastar-native, no thread-pool crossings
- **SSTable caching**: All SSTables cached in RAM after `open()`. Reads are pure in-memory lookups. Per-shard instances would each cache ~1/N of total data — same total memory.
- **In-memory schema caches**: `fieldsCache`, `tagsCache`, `tagValuesCache` — just need replication
- **SIMD bloom filters**: Highway-accelerated, already in `lib/storage/` — extend to index. NativeIndex also has its own per-SSTable bloom filters.
- **Partial aggregation**: `PartialAggregationResult` and `AggregationState` already exist and support cross-shard merging via `mergePartialAggregationsGrouped()`
- **Sorted intersection**: `findSeries()` already does two-pointer sorted-merge intersection — extend with cardinality-based ordering
- **SeriesId128**: Deterministic hash-based IDs — no centralized counter needed
- **Batch writes**: NativeIndex `kvWriteBatch()` applies multiple keys atomically in a single WAL record — already used by `getOrCreateSeriesId()` for 5+ entries per new series

## Implementation Phases

### Phase 1: Co-locate Index (eliminates shard-0 bottleneck)

The single highest-impact change. Enables all subsequent phases.

**Index distribution:**
1. Remove the `if (shardId != 0) return` gate in `NativeIndex::open()`. Each shard opens its own NativeIndex at `shard_N/native_index/`.
2. On insert, write TAG_INDEX + SERIES_METADATA + MEASUREMENT_SERIES entries to the **local** shard's NativeIndex (same key formats as today, just on a different shard). Use `kvWriteBatch()` for atomic multi-key writes.
3. Add schema broadcast via `invoke_on_all` for new fields/tags/values.
4. Add first-writer-wins field type conflict detection in `applySchemaUpdate()`.
5. Scale NativeIndex config per shard: `write_buffer_size /= N`.

**Write path:**
6. Remove `indexMetadataSync` cross-shard RPC from write path. Index locally instead.
7. Existing `knownSeriesCache` (thread_local in HTTP handler) carries over unchanged — now guards against redundant local writes instead of redundant RPCs.

**Query path:**
8. Change query handler: fan out `discoverAndQueryLocal()` to all shards instead of single RPC to shard 0. Each shard scans its local TAG_INDEX and queries its local data.
9. Remove `getSeriesMetadataBatch` re-fetch from `executeLocalQuery` — metadata is already local.

**Delete path:**
10. Change `deleteByPattern()`: scatter-gather to all shards. Each shard discovers matching series locally, creates tombstones locally, returns count.
11. Change `deleteRangeBySeries()`: compute target shard from hash, send delete directly to that shard. No shard 0 lookup.

**Retention:**
12. Change retention policy CRUD: write to local NativeIndex + broadcast via `invoke_on_all`.
13. Change `sweepExpiredFiles()`: each shard sweeps independently using its local index. No cross-shard series discovery.

**Metadata API:**
14. Change `/measurements`, `/tags`, `/fields` handlers: read from local schema registry cache instead of `invoke_on(0, ...)`.

**Validation:**
15. Dual-write to shard 0 during transition; run both old and new query paths and compare results.

**Complexity**: Medium-High. Touches write handler, query handler, delete handler, retention handler, metadata handler, and engine.
**Impact**: Eliminates all shard-0 bottlenecks across write, query, delete, and metadata paths.

### Phase 2: Shard-Local IDs + In-Memory Postings

Add in-memory postings acceleration on top of NativeIndex's kvPrefixScan-based TAG_INDEX.

1. Add shard-local uint32 ID assignment: `LocalIdMap` with bidirectional SeriesId128 ↔ LocalId mapping.
2. On insert, append LocalId to in-memory `mutablePostings_` sorted vectors.
3. On MemoryStore rollover, flush mutable postings to NativeIndex as serialized roaring bitmaps (via `kvWriteBatch()`).
4. On query, merge mutable (sorted vectors) + immutable (roaring bitmaps from NativeIndex's in-memory SSTable cache — synchronous, zero I/O).
5. Integrate CRoaring library for bitmap operations.

**Complexity**: Medium. New LocalId mapping layer + CRoaring integration.
**Impact**: Fast in-memory postings for recent data; roaring bitmaps for multi-tag intersection.

### Phase 3: Time-Scoped Postings

Add VictoriaMetrics-style per-day index entries.

1. On insert, mark series as active on the current day in time-scoped postings.
2. On query, build `activeSeries` bitmap from days in query range before tag intersection.
3. Add midnight pre-population: progressively populate next-day entries during last hour.
4. Add cleanup in retention sweep: each shard removes time-scoped entries with `day < now - retention_period` from its local NativeIndex (via `kvDelete()`) during the periodic sweep (Phase 1, step 13). This piggybacks on the existing 15-minute sweep timer.

**Complexity**: Low-Medium. Additional postings entries with day prefix.
**Impact**: 10-100× faster queries for narrow time ranges over long-retention data.

### Phase 4: Query Optimizations

1. **Smallest-first intersection**: Retrieve postings sizes (from HLL or bitmap cardinality), sort, intersect smallest first.
2. **Bloom filter pruning**: Add per-measurement bloom filter of active series. Check before TAG_INDEX scan.
3. **HLL sketches**: Add per-measurement and per-tag-key HLL. Use for intersection ordering and cardinality estimation API.

**Complexity**: Low. Additive optimizations, no architectural changes.
**Impact**: 2-10× faster multi-tag queries; fast cardinality estimation.

### Phase 5: Virtual Shard Abstraction (prepares for multi-server)

Introduce the virtual shard indirection layer **on a single server** first. This is a refactor, not a feature — the system behaves identically, but the addressing scheme is cluster-ready.

1. Allocate V=4096 virtual shards. Build default placement: round-robin across N local cores.
2. Replace all `SeriesId128::Hash{}(id) % smp::count` routing with `placement[hash % V]` lookup.
3. Each core manages multiple virtual shards (4096/N per core). LocalId counters are per-virtual-shard (not per-core) so they transfer cleanly during rebalancing.
4. Persist placement table to disk (simple JSON file). On startup, load placement and open NativeIndex/data directories per virtual shard.
5. Update `deleteRangeBySeries` routing to use placement lookup.

**Complexity**: Medium. Routing refactor, no new networking.
**Impact**: All addressing is cluster-ready. Multi-server becomes "just add networking."

### Phase 6: Multi-Server Networking (basic cluster)

Add inter-server communication. Single-server deployment still works (all virtual shards map to local cores).

1. **Cluster membership**: Integrate etcd client. Each server registers on startup, watches for membership changes. Store placement table in etcd.
2. **Seastar RPC layer**: Add `seastar::rpc::protocol` connections between servers. Define RPC verbs: `forward_write`, `query_shard`, `delete_pattern`, `schema_update`, `delete_range`.
3. **Write forwarding**: HTTP handler checks placement. If target is remote, batch writes by target server, forward via RPC. Return 503 if target unreachable. Durability: ack to client only after remote WAL write.
4. **Query fan-out**: Coordinator sends `query_shard` RPC to remote servers. Each server runs the four-stage pipeline locally, returns `PartialAggregationResult`. Coordinator merges. Propagate query deadline in RPC metadata.
5. **Query timeout and partial results**: If any server doesn't respond within deadline, coordinator returns available results with `"partial": true` and `warnings` array listing timed-out servers.
6. **Schema gossip**: Replace intra-server `invoke_on_all` with gossip to K peers for inter-server broadcast. Include retention policy changes and measurement → server bitmap updates in the gossip payload.
7. **Deterministic field type resolution**: Implement lexicographic conflict resolution in `applySchemaUpdate()` so all servers converge regardless of message ordering.
8. **Time-scoped postings**: Use data timestamp (not wall clock) for day bucket assignment. Eliminates clock skew issues.
9. **Delete scatter-gather**: Extend `deleteByPattern` to fan out to all servers. Extend `deleteRangeBySeries` to forward to the correct server via placement.
10. **Mutable postings recovery**: On startup, mark server as "warming" in etcd. Rebuild mutable postings from NativeIndex `kvPrefixScan()` (synchronous, all in RAM after `open()`). Remove "warming" flag when ready. Coordinator can skip or wait for warming servers.

**Complexity**: High. New networking layer, failure handling, gossip protocol.
**Impact**: Horizontal scaling beyond single server. Replication factor 1 (each series on exactly one server).

### Phase 7: Replication and Fault Tolerance

Add data replication for durability beyond single-server failure.

1. **Replication factor R**: Each virtual shard assigned to R servers in the placement table. Default R=1 (unchanged), configurable up to R=3.
2. **Quorum writes**: Write to all R replicas. Ack to client after W = ceil(R/2)+1 replicas succeed. (R=3, W=2: tolerate 1 server failure without write unavailability.)
3. **Read-any**: Queries served by any replica. Use adaptive replica selection (route to fastest based on response time history).
4. **Anti-entropy repair**: Background sweep compares TSM file manifests and NativeIndex SSTable snapshots between replicas. Copies missing data to repair divergence.
5. **Failure detection and failover**: When a server fails (detected via etcd lease expiry or heartbeat timeout), its virtual shards' remaining replicas continue serving reads and writes. No placement change needed — the remaining replicas are already authoritative.
6. **Server replacement**: New server joins, receives virtual shard assignments. Data streams from existing replicas. No freeze needed — existing replicas continue serving during streaming.

**Complexity**: Very high. Quorum protocol, anti-entropy, replica coordination.
**Impact**: Tolerates server failures without data loss or unavailability.

### Phase 8: Advanced (Future)

- **Index garbage collection**: Background sweep that removes TAG_INDEX, SERIES_METADATA, and MEASUREMENT_SERIES entries for series with no remaining data in any TSM file. Runs per-shard, checks each indexed series against local TSM file manifests. Not required for correctness (queries find the series but read zero points due to tombstones), but reclaims disk space and reduces index scan times.
- **FST-based term dictionary**: Replace NativeIndex prefix scan for tag values with FST (M3DB/Elasticsearch approach). Only needed at extreme cardinality (millions of unique tag values).
- **Custom routing**: Allow users to route writes by a tag value so queries filtering on that tag hit fewer shards (Elasticsearch routing).
- **Index compaction**: Merge multiple immutable postings segments into one (InfluxDB L0→L1→L2 compaction).
- **Gossip protocol upgrade**: Move from K-peer epidemic broadcast to SWIM protocol with suspicion mechanism for more accurate failure detection at scale.
- **Cross-datacenter replication**: Async replication between geographically distributed clusters for disaster recovery.

## Appendix: TAG_INDEX Fan-Out Mechanics

The TAG_INDEX is a reverse index: `(measurement, tagKey, tagValue)` → set of SeriesIds. When partitioned by SeriesId, each shard holds entries for the series it owns. Discovery queries fan out to all shards.

**Write**: Series X (hashed to shard 3) created with `{host:server-01}`. Shard 3 updates its local postings:
```
Shard 3 mutable postings:
  ("temp", "host", "server-01") → [..., LocalId_of_X]
```

**Query**: `avg:temp(){host:server-01}` fans out to all shards:
```
invoke_on_all: scan local postings for (temp, host, server-01)

  Shard 0: bloom hit → scan → 5 matches     (0.05ms)
  Shard 1: bloom miss → skip                 (0.001ms)
  Shard 2: bloom hit → scan → 3 matches      (0.03ms)
  Shard 3: bloom hit → scan → 7 matches      (0.06ms, includes X)
  ...
  Total: 15 matches across N shards, each already on the correct data shard
```

After discovery, each shard queries its local TSM/MemoryStore for the matched series and returns partial aggregation results. **Zero second hops.**

### Why Not Partition TAG_INDEX by Measurement or Tag?

Three alternatives were evaluated and rejected:

**Partition by measurement hash**: Hot measurements create hot shards. After discovery, still need RPCs to N data shards for actual data. Doesn't eliminate cross-shard hops.

**Partition by (measurement + tagKey + tagValue) hash**: Wildcard `host:server-*` must fan out anyway (different values hash differently). Multi-tag intersection `{host:a, dc:b}` requires cross-shard join. Index and data on different shards.

**Separate tag index shards**: Always 2 hops (tag shard → data shard). Double the sharding complexity. No co-location benefit.

All three separate the index from the data. Co-location is the only design that eliminates the second hop, and it's the approach used by every production system studied.
