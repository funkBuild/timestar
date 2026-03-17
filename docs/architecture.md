# Architecture

## Overview

TimeStar is a time series database built on C++23 and the Seastar framework. It uses a shard-per-core architecture where each CPU core independently manages its own storage and metadata, providing lock-free operation and linear scalability.

## Data Flow

### Write Path

```
HTTP POST /write
    → JSON parsing & validation
    → Route to target shard (hash of series key % core count)
    → WAL append (durability)
    → MemoryStore insert (fast reads)
    → NativeIndex metadata indexing (local to shard)
    → Schema broadcast to all shards (fields, tags, types)
    → [background] MemoryStore rollover → TSM file
    → [background] TSM compaction (low I/O priority)
```

### Query Path

```
HTTP POST /query
    → Parse query string
    → Scatter-gather: fan out to all shards
    → Each shard:
        → NativeIndex series discovery (time-scoped, bitmap-filtered)
        → TSM sparse index scan + block reads (high I/O priority)
        → MemoryStore scan for recent data
    → Merge partial AggregationState results
    → Apply aggregation, grouping, time bucketing
    → JSON response
```

## Storage Tiers

### Write-Ahead Log (WAL)

- Append-only log for crash recovery
- One WAL per shard in `shard_N/wal/`
- Rotated when size exceeds threshold (default 16 MB)
- Supports write, delete, and delete-range operations
- DMA-aligned writes for Seastar async I/O
- Adaptive compression ratio tracking for accurate capacity checks

### Memory Store

- In-memory buffer for recent writes
- Provides fast reads for the latest data
- Rolled over to TSM files when size threshold is reached

### TSM Files

Immutable, compressed data files in `shard_N/tsm/`:

- Indexed by SeriesId128 (XXH3 128-bit hash) with min/max time bounds
- Blocks contain compressed timestamps + values
- Tiered by compaction level (tier 0 = freshly flushed, higher = compacted)
- Sparse index for O(1) series lookup without scanning
- Bloom filters for fast negative lookups

### NativeIndex (Metadata)

Per-shard LSM-tree metadata index in `shard_N/native_index/`:

Each shard maintains its own NativeIndex co-located with its data — no shard-0 bottleneck. Schema metadata (fields, tags, types) is broadcast to all shards for local cache reads.

Key prefixes separate index types within the LSM-tree:

| Prefix | Name | Description |
|--------|------|-------------|
| `0x01` | `SERIES_INDEX` | Series key → series ID |
| `0x02` | `MEASUREMENT_FIELDS` | Measurement → set of field names |
| `0x03` | `MEASUREMENT_TAGS` | Measurement → set of tag keys |
| `0x04` | `TAG_VALUES` | Measurement + tag key → set of tag values |
| `0x05` | `SERIES_METADATA` | Series ID → metadata (measurement, tags, field) |
| `0x08` | `FIELD_STATS` | Series ID + field → stats |
| `0x09` | `FIELD_TYPE` | Measurement + field → field type |
| `0x0A` | `MEASUREMENT_SERIES` | Measurement + series ID → (empty) |
| `0x0B` | `RETENTION_POLICY` | Measurement → JSON retention policy |
| `0x0D` | `DAY_BITMAP` | Measurement + day → roaring bitmap of active series |
| `0x14` | `CARDINALITY_HLL` | Measurement → HyperLogLog sketch |
| `0x15` | `MEASUREMENT_BLOOM` | Measurement → bloom filter for tag combinations |

**Key features:**
- **Roaring bitmap postings** for tag-filtered series discovery (replaces prefix scans)
- **Time-scoped day bitmaps** to prune inactive series during queries
- **HyperLogLog cardinality estimation** (~0.8% error, O(1) per measurement)
- **Bloom filters** per measurement for early tag combination rejection
- **Block cache** (8 MB/shard LRU) for hot SSTable blocks
- **Streaming SSTable writes** (256 KB buffer, bounded memory)

## Compression

| Data Type | Algorithm | Notes |
|-----------|-----------|-------|
| Timestamps | FFOR (Frame-of-Reference) | Delta-of-delta + zigzag + bit-packing with Highway SIMD |
| Floats | ALP | Adaptive Lossless floating-Point compression with Highway SIMD |
| Integers | FFOR + exceptions | Bit-width optimized packing with SIMD |
| Booleans | Bit-packed + RLE | 1 bit/value (bit-pack) or run-length for biased data |
| Strings | zstd | Variable-length prefix + zstd block compression |

## Sharding

Data is distributed across CPU cores using Seastar's shard-per-core model:

- Each unique series (measurement + tags + field) is assigned to one shard
- Virtual shard abstraction: 4096 virtual shards mapped to physical cores via PlacementTable
- `routeToCore(seriesId)` determines the owning shard
- Different fields from the same measurement may be on different shards
- Metadata indexed locally on the owning shard (no cross-shard RPC for writes)

## I/O Scheduling

Seastar scheduling groups prioritize I/O under contention:

| Group | Shares | Use |
|-------|--------|-----|
| `ts_query` | 100 | TSM/index reads during queries |
| `ts_write` | 50 | WAL writes, memtable flushes |
| `ts_compact` | 10 | Background TSM/SSTable compaction |

When only one group has pending I/O, it gets full bandwidth. Shares only matter under contention — queries get ~10x the I/O bandwidth of compaction.

## Compaction

Background compaction merges smaller TSM files into larger ones:

- Tier-based with 4 tiers (0-3)
- Configurable thresholds per tier (default: 4 files triggers compaction)
- Concurrent compaction limit (default: 2)
- Runs under low-priority I/O scheduling group
- Tombstone cleanup during compaction (configurable dead fraction threshold)

## Caching Layers

Several in-memory caches avoid redundant I/O:

- **Series cache** (NativeIndex): two-generation LRU tracking indexed series. O(1) swap on overflow with incremental drain to avoid reactor stalls.
- **TSM sparse index cache**: per-file LRU mapping SeriesId128 to index entries.
- **Schema caches** (NativeIndex): in-memory maps for fields, tags, tag values, field types. Bounded eviction (256 tag values, 1000 HLL entries).
- **Metadata cache**: byte-budgeted LRU for series metadata (default 48 MB/shard).
- **Discovery cache**: byte-budgeted LRU for query discovery results (default 16 MB/shard).
- **Block cache**: byte-budgeted LRU for SSTable data blocks (default 8 MB/shard).

## Retention

- Per-measurement TTL and downsampling policies
- Sweep timer runs on shard 0 (default: every 15 minutes)
- Policies stored in NativeIndex and broadcast to all shards on startup
- Downsampling aggregates old data before deletion
- Day bitmap cleanup for expired time ranges

## Observability

- **Prometheus metrics** at `GET /metrics` — per-shard counters/gauges + Seastar built-ins
- **Slow query logging** — queries exceeding configurable threshold (default 500ms) auto-logged with full timing breakdown
- **Version endpoint** at `GET /version` — build version, git commit, compiler
- **Health endpoint** at `GET /health` — readiness-aware (returns "starting" during init)

## Configuration

TOML-based configuration with environment variable overrides (`TIMESTAR_*`):

| Section | Controls |
|---------|----------|
| `[server]` | Port, log level, data directory, shutdown timeout |
| `[storage]` | WAL threshold, block size, bloom filter, compaction |
| `[http]` | Body size limits, series limits, query timeout, slow query threshold |
| `[index]` | NativeIndex tuning (bloom bits, block size, cache bytes) |
| `[engine]` | Metadata retry, retention sweep, tombstone cleanup, I/O priority shares |
| `[streaming]` | Subscription limits, queue size, heartbeat interval |
| `[seastar]` | SMP count, memory, reactor backend, poll mode |

Generate a default config: `./timestar_http_server --dump-config`

Load a config: `./timestar_http_server --config /path/to/config.toml`
