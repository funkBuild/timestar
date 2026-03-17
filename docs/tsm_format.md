# TSM File Format Specification

**Version**: 1 (TSM_VERSION)
**Status**: Current production format

## Overview

TSM (Time-Structured Merge) is TimeStar's immutable columnar storage format for compressed time series data. Each TSM file contains data for one or more series, organized as compressed blocks with a trailing index for efficient lookups.

TSM files are write-once: data is written sequentially, the index is appended at the end, and the file is never modified after creation. Deletions are handled by sidecar tombstone files.

## File Layout

```
+---------------------------+
| File Header    (5 bytes)  |
+---------------------------+
| Data Block 0              |
| Data Block 1              |
| ...                       |
| Data Block N              |
+---------------------------+
| Index Section             |
+---------------------------+
| Index Offset   (8 bytes)  |
+---------------------------+
```

All multi-byte integers are little-endian unless noted otherwise.

## File Header

| Offset | Size | Type     | Value         | Description           |
|--------|------|----------|---------------|-----------------------|
| 0      | 4    | char[4]  | `"TASM"`      | Magic number (ASCII)  |
| 4      | 1    | uint8    | `1`           | Format version        |

Files with an unrecognized version byte are rejected at open time.

## Data Blocks

Each block stores a contiguous run of timestamp-value pairs for a single series, up to `max_points_per_block` points (default: 1000, configurable via TOML config).

### Block Structure

```
+-------------------------------------------+
| Block Header                    (9 bytes) |
|   - Value type                  (1 byte)  |
|   - Timestamp count             (4 bytes) |
|   - Compressed timestamp size   (4 bytes) |
+-------------------------------------------+
| Compressed Timestamps           (N bytes) |
+-------------------------------------------+
| Compressed Values               (M bytes) |
+-------------------------------------------+
```

### Block Header

| Offset | Size | Type   | Description                                  |
|--------|------|--------|----------------------------------------------|
| 0      | 1    | uint8  | Value type (see TSMValueType enum)            |
| 1      | 4    | uint32 | Number of timestamp-value pairs in this block |
| 5      | 4    | uint32 | Size of compressed timestamp section in bytes |

### TSMValueType Enum

| Value | Name    | C++ Type    | Encoder           |
|-------|---------|-------------|-------------------|
| 0     | Float   | `double`    | ALP               |
| 1     | Boolean | `bool`      | RLE bitpacking    |
| 2     | String  | `string`    | zstd + varint LEB |
| 3     | Integer | `int64_t`   | ZigZag + Simple8b |

### Compressed Timestamps

Timestamps are nanosecond-precision Unix epoch values (uint64). They are compressed using the **IntegerEncoder** (Simple8b packing):

1. Compute delta-of-delta: `DD[i] = (T[i] - T[i-1]) - (T[i-1] - T[i-2])`
2. ZigZag encode deltas to unsigned: `(x << 1) ^ (x >> 63)`
3. Pack with Simple8b (variable-width integer packing, 4-bit selector + 60-bit data per word)

The compressed size is recorded in the block header so the reader can locate the start of the value section.

### Compressed Values

Encoding depends on the value type:

#### Float Values (ALP)

Compressed using Adaptive Lossless floating-Point compression (ALP, SIGMOD 2024).

**Stream header** (16 bytes / 2 x uint64):

| Word | Bits    | Field                                |
|------|---------|--------------------------------------|
| 0    | [0:31]  | Magic: `0x414C5001` ("ALP\x01")     |
| 0    | [32:63] | Total value count                    |
| 1    | [0:15]  | Number of 1024-value blocks          |
| 1    | [16:31] | Tail count (remainder in final block)|
| 1    | [32:39] | Scheme (0=ALP, 1=ALP_RD, 2=ALP_DELTA)|

**Per-block header** (SCHEME_ALP / SCHEME_ALP_DELTA):

| Word | Bits    | Field                                       |
|------|---------|---------------------------------------------|
| 0    | [0:7]   | exp (exponent 0-18)                         |
| 0    | [8:15]  | fac (factor 0-18)                           |
| 0    | [16:22] | bit_width (FFOR bits per value, 0-64)       |
| 0    | [32:47] | exception_count                             |
| 0    | [48:63] | block_values (count in this block)          |
| 1    | [0:63]  | FFOR base (int64)                           |
| 2    | [0:63]  | First absolute value (SCHEME_ALP_DELTA only)|

**Encoding process:**
1. Sample 256 values to find optimal (exp, fac) pair
2. Scale: `encoded = round(value * 10^exp) / 10^fac` -> int64
3. Identify exceptions (values that don't round-trip losslessly, NaN, Inf, -0.0)
4. Compute FFOR base = min(non-exception values)
5. Bit-pack `(value - base)` at the computed bit_width
6. Store exception positions (uint16 array) and raw IEEE 754 bits

ALP processes values in fixed blocks of 1024. The final block may have fewer (stored in tail count).

#### Boolean Values (RLE Bitpack)

Booleans are packed as a raw bitstream, 1 bit per value:

- Processed in chunks: 64 bits (uint64), 32 bits (uint32), 16 bits (uint16), 8 bits (uint8)
- No header; the point count from the block header determines how many bits to read
- Decoding bulk-extracts 8 bytes (64 values) at a time

#### String Values (zstd + Varint)

```
+-------------------------------------+
| String Header              (16 bytes)|
|   - Magic: 0x53545247 "STRG" (4B)  |
|   - Uncompressed size       (4B)    |
|   - Compressed size         (4B)    |
|   - String count            (4B)    |
+-------------------------------------+
| zstd Compressed Payload    (N bytes)|
+-------------------------------------+
```

**Pre-compression payload format:**
```
[varint_len_0][string_bytes_0][varint_len_1][string_bytes_1]...
```

Lengths use LEB128 variable-length encoding (7 bits per byte, MSB continuation flag).

**Compression levels:**
- Level 1: Fresh writes (fast compression)
- Level 3: Compacted files (better ratio)

#### Integer Values (ZigZag + Simple8b)

1. ZigZag encode signed int64 to unsigned: `(x << 1) ^ (x >> 63)`
2. Compress with Simple8b (same algorithm as timestamps, without delta-of-delta)

## Index Section

The index section immediately follows the last data block. It contains one entry per series, storing metadata for all blocks belonging to that series.

Series are written in sorted order by SeriesId128.

### Index Entry (per series)

| Offset | Size | Type      | Description                          |
|--------|------|-----------|--------------------------------------|
| 0      | 16   | byte[16]  | SeriesId128 (XXH3_128bits hash)      |
| 16     | 1    | uint8     | Value type (TSMValueType)            |
| 17     | 2    | uint16    | Block count (N)                      |
| 19     | var  | Block[N]  | Block metadata entries (see below)   |

### Index Block Entry

**Base fields (all types) — 28 bytes:**

| Offset | Size | Type   | Description                         |
|--------|------|--------|-------------------------------------|
| 0      | 8    | uint64 | minTime (earliest timestamp)        |
| 8      | 8    | uint64 | maxTime (latest timestamp)          |
| 16     | 8    | uint64 | Block byte offset from file start   |
| 24     | 4    | uint32 | Block size in bytes                 |

**Additional Float fields — 52 bytes (total 80 bytes per Float block):**

| Offset | Size | Type   | Description                                    |
|--------|------|--------|------------------------------------------------|
| 28     | 8    | double | blockSum (sum of all values)                   |
| 36     | 8    | double | blockMin (minimum value)                       |
| 44     | 8    | double | blockMax (maximum value)                       |
| 52     | 4    | uint32 | blockCount (number of values)                  |
| 56     | 8    | double | blockM2 (Welford accumulator for variance)     |
| 64     | 8    | double | blockFirstValue (value at earliest timestamp)  |
| 72     | 8    | double | blockLatestValue (value at latest timestamp)   |

**Index block sizes:** 80 bytes (Float), 28 bytes (Boolean/String/Integer).

## Footer

The last 8 bytes of the file store the byte offset to the start of the index section:

| Offset       | Size | Type   | Description                    |
|--------------|------|--------|--------------------------------|
| fileSize - 8 | 8    | uint64 | Byte offset to index start     |

## SeriesId128

Each series is identified by a 128-bit hash:

- **Algorithm**: XXH3_128bits
- **Input**: Series key string: `"measurement,tag1=val1,tag2=val2 field"`
- **Storage**: 16 raw bytes (big-endian hash output)
- **Zero value**: All 16 bytes = 0x00 (reserved as invalid/empty)

## Tombstone File Format

Deletions are recorded in a sidecar file at `{tsm_path}.tombstones`.

### Tombstone Header (16 bytes)

| Offset | Size | Type   | Value        | Description       |
|--------|------|--------|--------------|-------------------|
| 0      | 4    | uint32 | `0x54534D54` | Magic ("TSMT")    |
| 4      | 4    | uint32 | `2`          | Version           |
| 8      | 4    | uint32 |              | Entry count       |
| 12     | 4    | uint32 |              | Header CRC32      |

### Tombstone Entry (36 bytes each)

| Offset | Size | Type     | Description                    |
|--------|------|----------|--------------------------------|
| 0      | 16   | byte[16] | SeriesId128                    |
| 16     | 8    | uint64   | Deletion range start (ns)      |
| 24     | 8    | uint64   | Deletion range end (ns)        |
| 32     | 4    | uint32   | Entry CRC32 checksum           |

**Legacy v1 entries** used 8-byte uint64 series IDs (28 bytes per entry). The reader detects v1 via the version field and upgrades on read.

## In-Memory Structures

### Sparse Index

When a TSM file is opened, only a lightweight sparse index is loaded into memory (no block data is read). Each entry contains:

- SeriesId128 (16 bytes)
- File offset + entry size (for lazy-loading the full index entry)
- Series value type
- Min/max time across all blocks (for time-range pruning)
- First/latest values (Float only, for zero-I/O LATEST/FIRST queries)

Stored in a `tsl::robin_map` keyed by SeriesId128.

### Bloom Filter

Each TSM file maintains an in-memory bloom filter over its series IDs for fast negative lookups. Configurable false positive rate (default: 0.1%).

### Full Index LRU Cache

Full index entries (with all block metadata) are lazy-loaded on demand and cached in a byte-budgeted LRU. Default budget: `tsm_cache_entries * 200 bytes` (~800 KB per file).

## File Naming Convention

```
shard_{SHARD_ID}/tsm/{TIER}_{SEQUENCE}.tsm
```

- **SHARD_ID**: Per-core shard number (0 to N-1)
- **TIER**: Compaction tier (0 = freshly flushed, higher = more compacted)
- **SEQUENCE**: Monotonically increasing per-shard sequence number

**Ranking**: `(tier << 60) | sequence` — higher tiers take priority during compaction. Newer files (higher sequence) are preferred for duplicate resolution.

## Compaction

Compaction merges multiple TSM files into a single new file:

- Input files are opened with merge iterators sorted by SeriesId128
- For duplicate series+timestamp, the value from the newest source file wins
- Block-level statistics are carried forward from source blocks when blocks are copied without re-encoding
- Tombstoned data is excluded from the output
- Output file uses the same format (potentially promoted to a higher tier)

## Configuration

| Parameter                        | Default | Description                           |
|----------------------------------|---------|---------------------------------------|
| `storage.max_points_per_block`   | 1000    | Max timestamp-value pairs per block   |
| `storage.tsm_bloom_fpr`         | 0.001   | Bloom filter false positive rate      |
| `storage.tsm_cache_entries`     | 4096    | Max entries in full index LRU cache   |

## Limits

| Constraint              | Limit      | Enforced by       |
|-------------------------|------------|-------------------|
| Block size              | ~4 GB      | uint32 in index   |
| Blocks per series       | 65,535     | uint16 in index   |
| Series per file         | Unbounded  | Limited by memory |
| Timestamp precision     | Nanosecond | uint64 epoch ns   |
