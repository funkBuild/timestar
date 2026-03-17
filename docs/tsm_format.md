# TSM File Format Specification

**Version**: 2 (TSM_VERSION)
**Minimum readable**: 1 (TSM_VERSION_MIN)

## Overview

TSM (Time-Structured Merge) is TimeStar's immutable columnar storage format for compressed time series data. Each TSM file contains data for one or more series, organized as compressed blocks with a trailing index for efficient lookups. Files are write-once; deletions use sidecar tombstone files.

## File Layout

```
+---------------------------+
| Header           (5 B)    |
+---------------------------+
| Data Block 0              |
| Data Block 1              |
| ...                       |
| Data Block N              |
+---------------------------+
| Index Section             |
+---------------------------+
| Index Offset     (8 B)    |  <-- footer (last 8 bytes of file)
+---------------------------+
```

All multi-byte integers are little-endian.

## Header (5 bytes)

| Offset | Size | Type    | Value    | Description          |
|--------|------|---------|----------|----------------------|
| 0      | 4    | char[4] | `"TASM"` | Magic number (ASCII) |
| 4      | 1    | uint8   | `2`      | Format version       |

Files with version outside `[TSM_VERSION_MIN, TSM_VERSION]` (currently `[1, 2]`) are rejected on open.

## Data Blocks

Each block stores up to `max_points_per_block` (default 1000) timestamp-value pairs for a single series.

### Block Header (9 bytes)

| Offset | Size | Type   | Description                            |
|--------|------|--------|----------------------------------------|
| 0      | 1    | uint8  | Value type (`TSMValueType` enum)       |
| 1      | 4    | uint32 | Number of timestamp-value pairs        |
| 5      | 4    | uint32 | Compressed timestamp section size (bytes) |

### TSMValueType Enum

| Value | Name    | C++ Type  | Encoder            |
|-------|---------|-----------|-------------------|
| 0     | Float   | `double`  | ALP               |
| 1     | Boolean | `bool`    | RLE bitpacking    |
| 2     | String  | `string`  | zstd + varint LEB |
| 3     | Integer | `int64_t` | ZigZag + Simple8b |

### Block Body

After the 9-byte header:

1. **Compressed timestamps** (size given by header field) -- Simple8b with delta-of-delta + ZigZag encoding.
2. **Compressed values** (remaining bytes to end of block) -- encoding depends on value type.

## Index Section

Immediately follows the last data block. Contains one entry per series, sorted by `SeriesId128`.

### Index Entry (per series)

| Offset | Size | Type     | Description                        |
|--------|------|----------|------------------------------------|
| 0      | 16   | byte[16] | SeriesId128 (XXH3_128bits hash)    |
| 16     | 1    | uint8    | Value type (`TSMValueType`)        |
| 17     | 2    | uint16   | Block count (N)                    |
| 19     | var  | Block[N] | N block metadata entries (below)   |

### Index Block -- Base Fields (28 bytes, all types)

| Offset | Size | Type   | Description                       |
|--------|------|--------|-----------------------------------|
| 0      | 8    | uint64 | minTime (earliest timestamp)      |
| 8      | 8    | uint64 | maxTime (latest timestamp)        |
| 16     | 8    | uint64 | Block byte offset from file start |
| 24     | 4    | uint32 | Block size in bytes               |

### Per-Type Extended Stats (V2)

V2 adds type-specific statistics after the 28-byte base. Total block entry sizes:

| Type    | Total bytes | Extra fields after base (28 B)                                   |
|---------|-------------|------------------------------------------------------------------|
| Float   | 80          | sum(8) + min(8) + max(8) + count(4) + M2(8) + first(8) + latest(8) |
| Integer | 72          | count(4) + sum(8) + min(8) + max(8) + first(8) + latest(8) as int64 |
| Boolean | 40          | count(4) + trueCount(4) + first(1) + latest(1) + pad(2)         |
| String  | 32          | count(4)                                                         |

#### Float Block Stats (offsets 28-79)

| Offset | Size | Type   | Field                                        |
|--------|------|--------|----------------------------------------------|
| 28     | 8    | double | blockSum                                     |
| 36     | 8    | double | blockMin                                     |
| 44     | 8    | double | blockMax                                     |
| 52     | 4    | uint32 | blockCount                                   |
| 56     | 8    | double | blockM2 (Welford accumulator for STDDEV)     |
| 64     | 8    | double | blockFirstValue (value at earliest timestamp)|
| 72     | 8    | double | blockLatestValue (value at latest timestamp) |

#### Integer Block Stats (offsets 28-71)

| Offset | Size | Type   | Field            |
|--------|------|--------|------------------|
| 28     | 4    | uint32 | blockCount       |
| 32     | 8    | int64  | sum              |
| 40     | 8    | int64  | min              |
| 48     | 8    | int64  | max              |
| 56     | 8    | int64  | firstValue       |
| 64     | 8    | int64  | latestValue      |

#### Boolean Block Stats (offsets 28-39)

| Offset | Size | Type   | Field            |
|--------|------|--------|------------------|
| 28     | 4    | uint32 | blockCount       |
| 32     | 4    | uint32 | boolTrueCount    |
| 36     | 1    | uint8  | boolFirstValue   |
| 37     | 1    | uint8  | boolLatestValue  |
| 38     | 2    | --     | padding (zero)   |

#### String Block Stats (offsets 28-31)

| Offset | Size | Type   | Field            |
|--------|------|--------|------------------|
| 28     | 4    | uint32 | blockCount       |

### V1 Backward Compatibility

V1 files have stats only for Float blocks (80 bytes). All other types use the 28-byte base only. The reader selects the per-block byte size via `indexBlockBytes(type, version)`.

## Footer (last 8 bytes)

| Offset         | Size | Type   | Description                   |
|----------------|------|--------|-------------------------------|
| fileSize - 8   | 8    | uint64 | Byte offset to index start    |

The reader reads this offset first, then reads the index section from `[indexOffset, fileSize - 8)`.

## SeriesId128

- **Algorithm**: XXH3_128bits
- **Input**: Series key string `"measurement,tag1=val1,tag2=val2 field"`
- **Storage**: 16 raw bytes
- **Zero value**: reserved as invalid/empty

## Tombstone Sidecar (`.tombstones`)

Stored at `{tsm_path}.tombstones`. Header: magic `0x54534D54` ("TSMT"), version, entry count, header CRC32 (16 bytes total). Each entry: SeriesId128 (16 B) + start/end timestamps (16 B) + CRC32 (4 B) = 36 bytes.

## In-Memory Structures

On open, only a sparse index is loaded (no block data):
- `tsl::robin_map<SeriesId128, SparseIndexEntry>` with per-series file offset, entry size, type, min/max time, and first/latest values (Float/Integer/Boolean).
- Bloom filter over series IDs (configurable FPR, default 0.1%).
- Byte-budgeted LRU cache for full index entries, loaded on demand via single DMA reads.

## File Naming

```
shard_{ID}/tsm/{TIER}_{SEQUENCE}.tsm
```

Ranking: `(tier << 60) | sequence`. Higher tiers and newer sequences take priority.

## Phase 4: Index Pagination (Deferred)

A paginated index layout was considered: writing the index as 64 KB pages with a trailing page directory (`"PIDX"` magic + per-page first-SeriesId + offset) to enable binary search without reading the entire index. After analysis, this was **deferred** for the following reasons:

1. **Low impact**: The existing `prefetchFullIndexEntries()` coalesces multiple series lookups into a single DMA read of the full index section. For typical query patterns this is already near-optimal.
2. **Sparse index eliminates full scans**: The sparse index loaded at file open already provides O(1) series lookup by hash. Full index entries are lazy-loaded per-series via single DMA reads.
3. **Risk/reward**: Changing the on-disk index format introduces migration complexity and backward compatibility burden for marginal improvement on files with 100K+ series -- a rare case given per-shard file counts.

The existing flat index with sparse-index + bloom-filter + LRU cache provides sub-millisecond lookups without format changes.
