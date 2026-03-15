# Distributed Index Implementation Plan

**Status**: Planning
**Branch**: TBD (off `feature/seastar-native-index`)
**Proposal**: [distributed_index_proposal.md](distributed_index_proposal.md)
**Research**: [distributed_index_research.md](distributed_index_research.md)

---

## Phase 1: Co-locate Index (eliminates shard-0 bottleneck)

**Status**: Not Started
**Estimated Complexity**: Medium-High

### Index Distribution
- [ ] Remove `if (shardId != 0) return` gate in `NativeIndex::open()` — each shard opens its own NativeIndex at `shard_N/native_index/`
- [ ] On insert, write TAG_INDEX + SERIES_METADATA + MEASUREMENT_SERIES to local shard's NativeIndex via `kvWriteBatch()`
- [ ] Add `SchemaUpdate` struct and `Engine::applySchemaUpdate()` (idempotent set merge)
- [ ] Add schema broadcast via `invoke_on_all` for new fields/tags/values
- [ ] Add first-writer-wins field type conflict detection in `applySchemaUpdate()`
- [ ] Scale NativeIndex config per shard: `write_buffer_size /= N`

### Write Path
- [ ] Remove `indexMetadataSync` cross-shard RPC from `http_write_handler.cpp` — index locally instead
- [ ] Verify `knownSeriesCache` (thread_local in HTTP handler) carries over correctly

### Query Path
- [ ] Add `discoverAndQueryLocal()` method to Engine — discovers matching series from local index, queries local data, returns `PartialAggregationResult`
- [ ] Change `http_query_handler.cpp`: fan out `discoverAndQueryLocal()` to all shards via `invoke_on_all` instead of single RPC to shard 0
- [ ] Remove `getSeriesMetadataBatch` re-fetch from `executeLocalQuery` — metadata already local
- [ ] Merge partial aggregations from all shards on coordinator (reuse existing `mergePartialAggregationsGrouped()`)

### Delete Path
- [ ] Add `localDeleteByPattern()` method to Engine — discovers + tombstones locally
- [ ] Change `deleteByPattern()`: scatter-gather to all shards, each shard discovers and deletes locally
- [ ] Change `deleteRangeBySeries()`: compute target shard from hash, send delete directly to that shard

### Retention
- [ ] Change retention policy CRUD: write to local NativeIndex + broadcast via `invoke_on_all`
- [ ] Change `sweepExpiredFiles()`: each shard sweeps independently using local index
- [ ] Update `loadAndBroadcastRetentionPolicies()`: each shard loads from local index

### Metadata API
- [ ] Change `/measurements` handler: read from local schema registry cache
- [ ] Change `/tags` handler: read from local schema registry cache
- [ ] Change `/fields` handler: read from local schema registry cache

### Validation
- [ ] Dual-write to shard 0 during transition
- [ ] Run both old and new query paths, compare results
- [ ] Run full test suite (2802+ tests)
- [ ] Benchmark: insert throughput, query latency, discovery scan time

---

## Phase 2: Shard-Local IDs + In-Memory Postings

**Status**: Not Started
**Estimated Complexity**: Medium

- [ ] Define `LocalIdMap` class: bidirectional `SeriesId128 ↔ uint32` mapping
- [ ] Add NativeIndex key prefixes `0x10` (LocalId → SeriesId128) and `0x11` (SeriesId128 → LocalId)
- [ ] Persist LocalId counter per shard in NativeIndex
- [ ] Add `mutablePostings_`: `robin_map<PostingsKey, vector<uint32_t>>` per shard
- [ ] On insert, append LocalId to mutable postings sorted vectors
- [ ] On MemoryStore rollover, flush mutable postings to NativeIndex as serialized roaring bitmaps via `kvWriteBatch()`
- [ ] Integrate CRoaring library (add to CMake, vendor or fetch)
- [ ] On query, merge mutable (sorted vectors) + immutable (roaring bitmaps from NativeIndex SSTable cache)
- [ ] Benchmark multi-tag intersection: roaring bitmap vs current sorted-merge

---

## Phase 3: Time-Scoped Postings

**Status**: Not Started
**Estimated Complexity**: Low-Medium

- [ ] Add NativeIndex key prefixes `0x0D` (day + tag → bitmap) and `0x0E` (day + measurement → bitmap)
- [ ] On insert, mark series as active on current day (using data timestamp, not wall clock)
- [ ] On query, build `activeSeries` bitmap from days in query range before tag intersection
- [ ] Add midnight pre-population: progressively populate next-day entries during last hour
- [ ] Add cleanup in retention sweep: remove time-scoped entries older than retention period
- [ ] Benchmark: narrow time range query (5min) over 90-day retention data

---

## Phase 4: Query Optimizations

**Status**: Not Started
**Estimated Complexity**: Low

- [ ] **Smallest-first intersection**: Retrieve postings sizes (HLL or bitmap cardinality), sort, intersect smallest first
- [ ] **Bloom filter pruning**: Add per-measurement bloom filter of active series, check before TAG_INDEX scan
- [ ] **HLL sketches**: Add per-measurement and per-tag-key HLL for cardinality estimation
- [ ] Add cardinality estimation API endpoint
- [ ] Benchmark: multi-tag query with selective vs broad filters

---

## Phase 5: Virtual Shard Abstraction

**Status**: Not Started
**Estimated Complexity**: Medium

- [ ] Allocate V=4096 virtual shards, build default placement (round-robin across N local cores)
- [ ] Add `PlacementTable` class: maps virtual shard → (server, core)
- [ ] Replace all `SeriesId128::Hash{}(id) % smp::count` routing with `placement[hash % V]` lookup
- [ ] Make LocalId counters per-virtual-shard (not per-core) for clean rebalancing
- [ ] Persist placement table to disk (JSON)
- [ ] Update `deleteRangeBySeries` routing to use placement lookup
- [ ] Verify single-server behavior is identical (all virtual shards map to local cores)

---

## Phase 6: Multi-Server Networking

**Status**: Not Started
**Estimated Complexity**: High

### Cluster Membership
- [ ] Integrate etcd client library
- [ ] Server self-registration on startup (lease with TTL)
- [ ] Watch for membership changes
- [ ] Store placement table in etcd

### RPC Layer
- [ ] Add `seastar::rpc::protocol` connections between servers
- [ ] Define RPC verbs: `forward_write`, `query_shard`, `delete_pattern`, `schema_update`, `delete_range`
- [ ] Serialization for `PartialAggregationResult`, `SchemaUpdate`, roaring bitmaps

### Write Forwarding
- [ ] HTTP handler checks placement — if target is remote, batch and forward via RPC
- [ ] Return 503 if target unreachable, with `Retry-After` header
- [ ] Ack to client only after remote WAL write (durability guarantee)

### Query Fan-Out
- [ ] Coordinator sends `query_shard` RPC to remote servers (1 per server, not per shard)
- [ ] Each server runs four-stage pipeline locally, returns `PartialAggregationResult`
- [ ] Propagate query deadline in RPC metadata
- [ ] Partial results on timeout: return available data + `"partial": true` + `warnings` array

### Schema Gossip
- [ ] Implement K-peer gossip for inter-server schema + retention broadcast
- [ ] Include measurement → server bitmap updates in gossip payload
- [ ] Deterministic field type conflict resolution (lexicographic ordering)

### Time-Scoped Postings
- [ ] Use data timestamp (not wall clock) for day bucket assignment

### Delete
- [ ] Extend `deleteByPattern` to fan out to all servers
- [ ] Extend `deleteRangeBySeries` to forward to correct server via placement

### Recovery
- [ ] Mark server as "warming" in etcd during postings rebuild
- [ ] Coordinator skips or waits for warming servers
- [ ] Full schema snapshot request from peer on startup

---

## Phase 7: Replication and Fault Tolerance

**Status**: Not Started
**Estimated Complexity**: Very High

- [ ] Configurable replication factor R (default R=1)
- [ ] Placement table assigns each virtual shard to R servers
- [ ] Quorum writes: ack after W = ceil(R/2)+1 replicas succeed
- [ ] Read-any: queries served by any replica
- [ ] Adaptive replica selection (route to fastest based on response time history)
- [ ] Anti-entropy repair: compare TSM manifests and NativeIndex SSTable snapshots between replicas
- [ ] Failure detection and failover via etcd lease expiry
- [ ] Server replacement: stream data from existing replicas

---

## Phase 8: Advanced (Future)

**Status**: Not Started

- [ ] Index garbage collection: remove entries for fully-deleted series
- [ ] FST-based term dictionary for extreme cardinality
- [ ] Custom routing: user-defined shard routing by tag value
- [ ] Index compaction: merge immutable postings segments (L0→L1→L2)
- [ ] SWIM gossip protocol upgrade for large clusters
- [ ] Cross-datacenter async replication

---

## Reference: Key Files to Modify

| Component | Files |
|-----------|-------|
| NativeIndex | `lib/index/native/native_index.hpp/cpp` |
| Engine | `lib/core/engine.hpp/cpp` |
| Write Handler | `lib/http/http_write_handler.cpp` |
| Query Handler | `lib/http/http_query_handler.cpp` |
| Delete Handler | `lib/http/http_delete_handler.cpp` |
| Metadata Handler | `lib/http/http_metadata_handler.cpp` |
| Retention Handler | `lib/http/http_retention_handler.cpp` |
| Query Planner | `lib/query/query_planner.cpp` |
| Config | `lib/config/timestar_config.hpp` |
| Key Encoding | `lib/index/key_encoding.hpp` |

## Reference: Key Metrics to Track

| Metric | Current Baseline | Phase 1 Target | Phase 4 Target |
|--------|-----------------|----------------|----------------|
| Insert throughput | 15.5M pts/sec | 16-17M pts/sec (no metadata RPC) | Same |
| Query discovery (1K series) | ~5ms | ~0.3ms (N=16 parallel) | ~0.1ms (+ bloom + bitmap) |
| Query discovery (10K series, wildcard) | ~50ms | ~3ms (N=16 parallel) | ~1ms (+ roaring + smallest-first) |
| Narrow time range (5min over 90d) | Same as full scan | Same as full scan | ~0.05ms (time-scoped pruning) |
| Multi-tag intersection (3 tags) | ~2ms | ~0.12ms (parallel) | ~0.03ms (roaring + smallest-first) |
| Metadata API (/measurements) | ~10μs (RPC to shard 0) | ~0.1μs (local cache) | Same |
| Delete by pattern | ~5ms (shard 0 serial) | ~0.3ms (parallel scatter-gather) | Same |
