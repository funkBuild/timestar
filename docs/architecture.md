# Architecture

## Overview

TSDB is a time series database built on C++23 and the Seastar framework. It uses a shard-per-core architecture where each CPU core independently manages its own storage, providing lock-free operation and linear scalability.

## Data Flow

### Write Path

```
HTTP POST /write
    -> JSON parsing & validation
    -> Series ID lookup/creation (shard 0 LevelDB)
    -> Route to target shard (hash of series ID % core count)
    -> WAL append (durability)
    -> MemoryStore insert (fast reads)
    -> [background] MemoryStore rollover -> TSM file
    -> [background] TSM compaction
```

### Query Path

```
HTTP POST /query
    -> Parse query string
    -> Fan out to all shards
    -> Each shard: LevelDB index lookup -> TSM + MemoryStore scan
    -> Merge partial AggregationState results
    -> Apply aggregation, grouping, time bucketing
    -> JSON response
```

## Storage Tiers

### Write-Ahead Log (WAL)

- Append-only log for crash recovery
- One WAL per shard in `shard_N/wal/`
- Rotated when size exceeds threshold (default 16 MB)
- Supports write, delete, and delete-range operations
- DMA alignment padding: writes are padded to `disk_write_dma_alignment` (typically 4096 bytes) before each flush to satisfy Seastar's DMA sink alignment requirement
- Adaptive compression ratio tracking (`CompressionStats`): per-type EMA (alpha=0.15) updated after each encode, used by `estimateInsertSize()` for accurate WAL capacity checks without trial encoding
- Immediate flush mode (`setImmediateFlush(true)`): forces `flush()` after every write/batch for lower-latency durability; default is buffered (64 KiB output stream)

### Memory Store

- In-memory buffer for recent writes
- Provides fast reads for the latest data
- Rolled over to TSM files periodically or when size threshold is reached

### TSM Files

Immutable, compressed data files in `shard_N/tsm/`:

- Indexed by series ID with min/max time bounds
- Blocks contain compressed timestamps + values
- Tiered by compaction level (tier 0 = freshly flushed, higher = compacted)
- Bloom filters for fast negative lookups

### LevelDB Index

Per-database metadata index on shard 0 in `shard_0/index/`:

Key prefixes separate index types (0x01-0x0B) within a single LevelDB instance:

| Prefix | Name | Description |
|--------|------|-------------|
| `0x01` | `SERIES_INDEX` | Series key -> series ID |
| `0x02` | `MEASUREMENT_FIELDS` | Measurement -> set of field names |
| `0x03` | `MEASUREMENT_TAGS` | Measurement -> set of tag keys |
| `0x04` | `TAG_VALUES` | Measurement + tag key -> set of tag values |
| `0x05` | `SERIES_METADATA` | Series ID -> metadata (measurement, tags, field) |
| `0x06` | `TAG_INDEX` | Measurement + tag key + tag value -> series IDs |
| `0x07` | `GROUP_BY_INDEX` | Measurement + tag key + tag value -> series IDs (for group-by) |
| `0x08` | `FIELD_STATS` | Series ID + field -> stats (data type, time bounds, point count) |
| `0x09` | `FIELD_TYPE` | Measurement + field -> field type (float, bool, string, integer) |
| `0x0A` | `MEASUREMENT_SERIES` | Measurement + series ID -> (empty) for fast measurement-to-series lookup |
| `0x0B` | `RETENTION_POLICY` | Measurement -> JSON retention policy |

## Compression

| Data Type | Algorithm | Notes |
|-----------|-----------|-------|
| Timestamps | XOR (Gorilla) | Delta-of-delta encoding |
| Floats | ALP | Adaptive Lossless floating-Point compression (compile-time selectable; Gorilla XOR available as alternative) |
| Integers | Simple8b | Packs small integers into 64-bit words |
| Booleans | Bit-packed | 1 bit per value |
| Strings | Snappy | Variable-length prefix + Snappy block compression |

ALP (Adaptive Lossless floating-Point) provides ~44% better compression than pure XOR for many workloads, with 2-3% overhead.

## Sharding

Data is distributed across CPU cores using Seastar's shard-per-core model:

- Each unique series (measurement + tags + field) is assigned to one shard
- Shard assignment: `SeriesId128::Hash{}(id) % smp::count`
- `SeriesId128::Hash` uses only the lower 8 bytes of the 16-byte XXH3 digest as a `size_t` for shard routing (the full 128-bit ID is still stored and used for equality/ordering)
- Different fields from the same measurement may be on different shards
- Metadata operations are centralized on shard 0 via async dispatch with bounded retry queue

## Compaction

Background compaction merges smaller TSM files into larger ones:

- Tier-based with 4 tiers (0-3): tier 0 (freshly flushed) -> tier 1 -> tier 2 -> tier 3; max tier capped at 3
- Per-tier size limits: 100 MB (tier 0), 1 GB (tier 1), 10 GB (tier 2), unlimited (tier 3)
- Configurable thresholds per tier (default: 4 files triggers compaction; tier 3 requires 8)
- Concurrent compaction limit (default: 2)
- Tombstone cleanup during compaction (configurable dead fraction threshold)

## Caching Layers

Several in-memory caches avoid redundant I/O on the hot path:

- **Two-generation series cache** (LevelDB index): tracks which series keys have already been indexed. Active generation receives inserts; when it exceeds `series_cache_size` (default 1M), it is O(1)-swapped to a retired slot that drains incrementally (256 entries per insert) to avoid reactor stalls.
- **TSM index cache**: per-TSM-file LRU cache (`tsm_cache_entries`, default 4096) mapping SeriesId128 to full `TSMIndexEntry`. Avoids re-reading the on-disk index for hot series.
- **Field/tag caches** (LevelDB index): in-memory maps for `fieldsCache`, `tagsCache`, `tagValuesCache`, and `knownFieldTypes`. Naturally bounded by schema cardinality, no eviction needed.
- **Measurement-series cache**: maps measurement name to vector of SeriesId128, populated lazily on first query and maintained incrementally during inserts.
- **LevelDB block cache**: 8 MB LRU cache for LevelDB's internal block reads.

## Retention

- Per-measurement TTL and downsampling policies
- Sweep timer runs on shard 0 (default: every 15 minutes)
- Policies stored in LevelDB and broadcast to all shards on startup
- Downsampling aggregates old data before deletion

## Configuration

TOML-based configuration file with sections:

| Section | Controls |
|---------|----------|
| `[server]` | Port, log level, data directory |
| `[storage]` | WAL threshold, block size, bloom filter, compaction |
| `[http]` | Body size limits, series limits, query timeout |
| `[index]` | LevelDB tuning (bloom bits, block size, cache) |
| `[engine]` | Metadata retry, retention sweep, tombstone cleanup |
| `[streaming]` | Subscription limits, queue size, heartbeat interval |
| `[seastar]` | SMP count, memory, reactor backend, poll mode |

Generate a default config: `./tsdb_http_server --dump-config`

Load a config: `./tsdb_http_server --config /path/to/config.toml`
