# Index Architecture & Query Usage Report

## 1. Current Architecture

The TimeStar metadata index is **fully centralized on shard 0**. Only shard 0 opens a LevelDB database; all other shards have `db = nullptr`. Every metadata operation from a non-zero shard requires a cross-shard RPC via `invoke_on(0, ...)`.

```
                    ┌─────────────────────────────────────────┐
                    │              SHARD 0                     │
                    │  ┌───────────────────────────────────┐  │
                    │  │         LevelDB Instance           │  │
                    │  │  12 index types (0x01..0x0C)       │  │
                    │  │  Bloom filter, Snappy compression  │  │
                    │  └───────────────────────────────────┘  │
                    │  ┌───────────────────────────────────┐  │
                    │  │         8 In-Memory Caches         │  │
                    │  │  seriesCache (2-gen eviction)      │  │
                    │  │  fieldsCache, tagsCache            │  │
                    │  │  tagValuesCache, knownFieldTypes   │  │
                    │  │  measurementSeriesCache            │  │
                    │  │  seriesMetadataCache_ (LRU)        │  │
                    │  │  discoveryCache_ (LRU)             │  │
                    │  └───────────────────────────────────┘  │
                    │  + local TSM/WAL/MemoryStore (data)     │
                    └────────────────▲────────────────────────┘
                                     │ invoke_on(0, ...)
           ┌─────────────────────────┼─────────────────────────┐
           │                         │                         │
    ┌──────┴──────┐          ┌───────┴──────┐          ┌──────┴──────┐
    │   SHARD 1   │          │   SHARD 2    │          │   SHARD N   │
    │  db=nullptr │          │  db=nullptr  │          │  db=nullptr │
    │  TSM/WAL    │          │  TSM/WAL     │          │  TSM/WAL    │
    │  (data only)│          │  (data only) │          │  (data only)│
    └─────────────┘          └──────────────┘          └─────────────┘
```

## 2. Index Key Types (12 Prefixes)

| Prefix | Name | Key → Value | Write Frequency | Read Frequency |
|--------|------|-------------|-----------------|----------------|
| 0x01 | SERIES_INDEX | measurement+tags+field → series_id | Legacy, rarely written | Unused |
| 0x02 | MEASUREMENT_FIELDS | measurement → encoded set(fields) | Per new field | Per /fields, per query discovery miss |
| 0x03 | MEASUREMENT_TAGS | measurement → encoded set(tag_keys) | Per new tag key | Per /tags |
| 0x04 | TAG_VALUES | measurement+\0+tagKey → encoded set(values) | Per new tag value | Per /tags, per query filter |
| 0x05 | SERIES_METADATA | 16-byte SeriesId128 → measurement+field+tags | Per new series | Per query (batched) |
| 0x06 | TAG_INDEX | measurement+\0+tagKey+\0+tagValue+\0+SeriesId → SeriesId | Per new series × tags | Per query with tag filters |
| 0x07 | GROUP_BY_INDEX | (same key format as 0x06) | Per new series × tags | Per group-by query |
| 0x08 | FIELD_STATS | SeriesId+\0+field → type+minTime+maxTime+count | Per compaction | Rarely |
| 0x09 | FIELD_TYPE | measurement+\0+field → "float"/"boolean"/... | Per new measurement+field | Per /fields |
| 0x0A | MEASUREMENT_SERIES | measurement+\0+SeriesId → empty | Per new series | Per delete, per retention sweep |
| 0x0B | RETENTION_POLICY | measurement → JSON policy | Per PUT /retention | Startup + periodic |
| 0x0C | MEASUREMENT_FIELD_SERIES | measurement+\0+field+\0+SeriesId → empty | Per new series | Single-field queries (fast path) |

## 3. Every Cross-Shard RPC to Shard 0

### 3a. Write Path (per HTTP write request)

| Caller | Method | Data Copied Cross-Shard | Frequency |
|--------|--------|------------------------|-----------|
| `engine.cpp:289` | `indexMetadataSync()` | `vector<MetadataOp>` (moved) | **Once per write batch** |

The write handler collects all unique `MetadataOp` structs (one per new series), sends them in a single batched RPC. Inside shard 0, `indexMetadataBatch()` does at most 2 `co_await` calls (one LevelDB read batch for cache misses, one WriteBatch for all new entries).

### 3b. Query Path (per query request)

| Caller | Method | Data Copied Cross-Shard | Frequency |
|--------|--------|------------------------|-----------|
| `http_query_handler.cpp:423` | `findSeriesWithMetadataCached()` | measurement, scopes, fieldFilter (copied in), `SeriesDiscoveryResult` (moved out) | **Once per query** |
| `engine.cpp:411` | `getSeriesMetadataBatch()` | `vector<SeriesId128>` (copied in), `vector<pair<SeriesId128, optional<SeriesMetadata>>>` (moved out) | **Once per shard per query** |

The query handler does series discovery on shard 0, hashes results to data shards, then each data shard does a batched metadata lookup back to shard 0 via `getSeriesMetadataBatch()`.

**Total RPCs per query**: 1 (discovery) + S (one metadata batch per data shard with results) = **1 + S RPCs** where S = number of shards with matching data.

### 3c. Metadata API (per request)

| Caller | Method | Frequency |
|--------|--------|-----------|
| `http_metadata_handler.cpp:129` | `getAllMeasurements()` | Per GET /measurements |
| `http_metadata_handler.cpp:220` | `getMeasurementTags()` + parallel `getTagValues()` | Per GET /tags |
| `http_metadata_handler.cpp:327` | `getFields()` + loop `getFieldType()` | Per GET /fields |

### 3d. Delete Path (per delete request)

| Caller | Method | RPCs |
|--------|--------|------|
| `engine.cpp:565` | `getSeriesId()` | 1 per `deleteRangeBySeries` |
| `engine.cpp:611-623` | `getAllSeriesForMeasurement()` or `findSeries()` | 1 per `deleteByPattern` |
| `engine.cpp:656-672` | `getSeriesMetadata()` loop (NOT batched) | 1 per `deleteByPattern` |

### 3e. Background (periodic)

| Caller | Method | Frequency |
|--------|--------|-----------|
| Retention sweep | `getAllSeriesForMeasurement()` | Every 15 minutes per measurement with TTL |

## 4. Cache Hierarchy on Shard 0

```
Request arrives on shard 0
    │
    ├─ discoveryCache_ (LRU, ~16MB)
    │   Key: canonical(measurement + sorted scopes + sorted fields)
    │   Hit: return shared_ptr<vector<SeriesWithMetadata>>  ← FASTEST
    │   Miss: ↓
    │
    ├─ seriesCacheContains() (two-generation set, ~1M entries)
    │   Key: series key string
    │   Hit: skip LevelDB existence check on INSERT path
    │   Miss: ↓
    │
    ├─ fieldsCache / tagsCache / tagValuesCache (unbounded maps)
    │   Key: measurement (or measurement+tagKey)
    │   Hit: avoid read-modify-write on INSERT path
    │   Miss: ↓
    │
    ├─ seriesMetadataCache_ (LRU, ~64MB)
    │   Key: SeriesId128
    │   Hit: return SeriesMetadata without LevelDB Get
    │   Miss: ↓
    │
    └─ LevelDB (on disk, with bloom filter + block cache)
```

**Invalidation rules:**
- `discoveryCache_`: Flushed per-measurement whenever a new series is created (`invalidateDiscoveryCache(measurement)`)
- `seriesMetadataCache_`: Never invalidated (SeriesMetadata is immutable)
- `fieldsCache`, `tagsCache`, `tagValuesCache`: Never evicted (naturally bounded, merged-only)
- `seriesCache`: Two-generation eviction at 256 entries per insert when active exceeds limit

## 5. Bottleneck Analysis

### 5a. Shard 0 Becomes the Serialization Point

**All metadata reads AND writes serialize through shard 0's reactor thread.** Under load:

- N-1 data shards can be idle waiting for shard 0 to process discovery
- A heavy query (scanning thousands of TAG_INDEX entries) blocks insert metadata indexing on the same shard
- A burst of writes (many `indexMetadataBatch` calls) delays query discovery

The current mitigation is caching — but caches are cold on startup, after measurement changes, and for new query patterns.

### 5b. Discovery Cache Invalidation is Coarse-Grained

`invalidateDiscoveryCache(measurement)` clears **all** cached discovery results for a measurement when any single new series is added. For high-cardinality measurements with continuous new series (e.g., per-container metrics), the discovery cache is effectively useless — it's invalidated on every write batch.

### 5c. Query Path Does Redundant Metadata Lookups

The query path makes **1 + S** RPCs to shard 0:
1. Discovery RPC: `findSeriesWithMetadataCached()` — returns `SeriesWithMetadata` (includes full metadata)
2. Per-shard RPCs: `getSeriesMetadataBatch()` — fetches the **same metadata again**

At `http_query_handler.cpp:454-466`, the discovery result already contains `swm.metadata.measurement`, `swm.metadata.tags`, and `swm.metadata.field`. This is used to build `SeriesQueryContext` on shard 0, which is sent to data shards. But then at `engine.cpp:405-412`, each data shard calls `getSeriesMetadataBatch()` back to shard 0 to re-fetch the same metadata.

**This means every query fetches metadata twice for every matching series.**

### 5d. Delete Path Does Sequential Metadata Lookups

At `engine.cpp:640-651`, `deleteByPattern()` loops over series IDs calling `getSeriesMetadata()` one at a time inside shard 0 rather than using `getSeriesMetadataBatch()`.

### 5e. Tag Scans Are Single-Threaded

`findSeriesByTagPattern()` does a linear LevelDB scan + regex match on shard 0's single reactor thread. For high-cardinality tags (100K+ unique values), this scan cannot be parallelized because there's only one LevelDB instance on one core.

## 6. Data Volume Per RPC

| Operation | Typical Size Per RPC | Worst Case |
|-----------|---------------------|------------|
| `indexMetadataSync` (write) | 1-100 MetadataOps × ~200B = 200B-20KB | 10K ops × 200B = 2MB |
| `findSeriesWithMetadataCached` (query discovery) | 1-1000 SeriesWithMetadata × ~300B = 300B-300KB | 100K series × 300B = 30MB |
| `getSeriesMetadataBatch` (per shard) | series/N × ~300B (N=shard count) | 100K/N × 300B |
| `getAllMeasurements` | 10-1000 strings × ~20B | Small |
| `getTagValues` | 10-10K strings × ~20B | 200KB |

## 7. What Each Query Type Touches

| Query Pattern | Index Operations on Shard 0 | Scan Complexity |
|--------------|----------------------------|-----------------|
| `avg:temp()` (no filters) | `getAllSeriesForMeasurement` → MEASUREMENT_SERIES scan | O(M) where M = series in measurement |
| `avg:temp(){host:server-01}` (exact) | `findSeriesByTag` → TAG_INDEX point scan | O(1) seek + O(K) where K = matches |
| `avg:temp(){host:server-*}` (wildcard) | `findSeriesByTagPattern` → TAG_INDEX range scan + wildcard match | O(V) where V = all values for tag |
| `avg:temp(){host:~srv-[0-9]+}` (regex) | `findSeriesByTagPattern` → TAG_INDEX range scan + regex match | O(V) per entry |
| `avg:temp(){host:a,dc:b}` (multi-tag) | `findSeriesByTag` × 2 → sorted intersection | O(K1 + K2) intersection |
| `avg:temp(value)` (single field) | `MEASUREMENT_FIELD_SERIES` scan (fast path) | O(F) where F = series with that field |
| `GET /tags?measurement=temp` | `getTags` + parallel `getTagValues` per key | O(T) tags × O(V) values each |

## 8. Summary of Architectural Constraints

1. **Single-writer, single-reader**: All metadata I/O funnels through one core. This is correct for consistency but limits horizontal scaling.

2. **Cache-dependent performance**: Cold caches (startup, new measurements, high cardinality) expose full LevelDB latency on the critical path.

3. **Redundant metadata transfer**: Query discovery returns full metadata, but data shards re-fetch it. ~2x the cross-shard metadata bandwidth needed.

4. **Coarse cache invalidation**: Any new series for a measurement flushes all discovery cache entries for that measurement.

5. **No read replicas**: Non-zero shards cannot answer any metadata question locally — even immutable data (field types, tag schemas) that never changes after creation.

6. **Scan contention**: A TAG_INDEX scan for a wildcard query on shard 0 blocks insert metadata indexing running on the same reactor.
