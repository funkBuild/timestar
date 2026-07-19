# TSM File Format Specification

**Version**: 3 (TSM_VERSION)
**Minimum readable**: 3 (TSM_VERSION_MIN)

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
| 4      | 1    | uint8   | `3`      | Format version       |

Files with version outside `[TSM_VERSION_MIN, TSM_VERSION]` (currently `[3, 3]`) are rejected on open.

V3 widened the per-series index block count from uint16 to uint32, shifting every
byte after offset 17 in an index entry. V1/V2 files cannot be parsed by the V3
reader and are rejected on open rather than silently misread. Under uint16 a
single series was capped at 65,535 blocks per file, which high-volume
single-series ingest reaches; compaction then threw at `writeIndex()` time and
the tier could never merge again.

## Data Blocks

Each block stores up to `max_points_per_block` (default 3000, configurable via `[storage]` in the TOML config) timestamp-value pairs for a single series.

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
| 3     | Integer | `int64_t` | ZigZag + FFOR     |

### Block Body

After the 9-byte header:

1. **Compressed timestamps** (size given by header field) -- FFOR (frame-of-reference
   bit-packing) with delta-of-delta + ZigZag encoding, in blocks of up to
   `kBlockSize` = 1024 values. A block whose deltas are all equal is emitted as a bare
   2-word (16-byte) header, so the densest possible encoding is 1024 values in 16 bytes
   = **64 values per byte**.

   The reader enforces this as a format constraint: a block whose declared timestamp
   COUNT exceeds `timestampBytes * 64 * 4` (the theoretical maximum plus a 4x safety
   margin) is rejected, so a corrupt count cannot drive a multi-GB allocation. Any
   encoder change that beats 64 values/byte must raise that bound first — a
   `static_assert` in `tsm.cpp` ties it to `kBlockSize`.
2. **Compressed values** (remaining bytes to end of block) -- encoding depends on value type.

## Index Section

Immediately follows the last data block. Contains one entry per series, sorted by `SeriesId128`.

### Index Entry (per series)

| Offset | Size | Type     | Description                        |
|--------|------|----------|------------------------------------|
| 0      | 16   | byte[16] | SeriesId128 (XXH3_128bits hash)    |
| 16     | 1    | uint8    | Value type (`TSMValueType`)        |
| 17     | 4    | uint32   | Block count (N)                    |
| 21     | var  | Block[N] | N block metadata entries (below)   |

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
| 52     | 4    | uint32 | blockCount (non-NaN count, see below)        |
| 56     | 8    | double | blockM2 (Welford accumulator for STDDEV)     |
| 64     | 8    | double | blockFirstValue (value at earliest timestamp)|
| 72     | 8    | double | blockLatestValue (value at latest timestamp) |

Float stats semantics (canonical NaN policy, docs/nan_policy.md): all stats
INCLUDING `blockCount` skip NaN values — `blockCount` is the number of
**non-NaN** points, so COUNT/AVG stats pushdown matches the scalar
NaN-skipping fold. The block header carries the true total point count for
decoding; a `blockCount` that differs from the header count marks a
NaN-carrying block (the COUNT-only read path uses this to force value
decode). Blocks containing NaN are written with NaN in
`blockM2`/`blockFirstValue`/`blockLatestValue` — NaN endpoints are the
on-disk sentinel from which the reader derives `hasExtendedStats = false`
(M2/first/latest withheld; the flag itself is not serialized). An all-NaN
block has `blockCount = 0` (stats pushdown disabled). Files written before
this rule stored the raw
total in `blockCount`; they never mismatch the header count and keep legacy
(NaN-counting) pushdown behaviour until rewritten by compaction.

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

#### String Dictionary (String series only, after the block entries)

Each String series index entry ends with `dictSize(4)` + `dictData(dictSize)`.
`dictSize == 0` means the series' blocks use raw `STRG` encoding; a non-zero
size means they use dictionary-ID `STR2` encoding, whose varint IDs are ONLY
meaningful with this dictionary. **Invariant: a file containing STR2 blocks
for a series MUST carry that series' dictionary in its index entry.**
Compaction enforces this by carrying the dictionary through the zero-copy
block path when the series comes from a single source file, and by decoding +
re-encoding (fresh dictionary) when merging dictionary-bearing blocks from
multiple sources.

Historical note (bug fixed Jul 2026): compaction's zero-copy path used to
carry STR2 blocks without their dictionary, writing `dictSize=0`. Files
written by an affected build have permanently undecodable string blocks
(the reader now reports "Dictionary-encoded (STR2) string block has no
dictionary in its TSM index entry"); the dictionary bytes were never written,
so no repair is possible — affected string data must be re-ingested. Numeric
series in the same files are unaffected.

### V1/V2 Compatibility

**V1 and V2 files are NOT readable by a V3 reader.** V3 widened the per-series index
block count from uint16 to uint32, which shifts every byte after offset 17 of an index
entry, so an older file cannot be parsed. `TSM_VERSION_MIN` is 3 and `TSM::open()`
rejects anything below it rather than misreading it.

Note that rejection happens per file, and `TSMFileManager::openTsmFile()` logs and skips
files that fail to open — so on an upgrade, pre-V3 files are silently dropped from query
results rather than surfacing an error. There is no migration tool.

`indexBlockBytes(type, version)` retains a `version < 2` branch and the reader retains
several `fileVersion >= 2` tests; with the accepted range at [3, 3] these are vestigial
and always take the modern path.

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
shard_{ID}/tsm/{TIER}_{SEQUENCE}.tsm          (flush-created, tier 0)
shard_{ID}/tsm/{TIER}_{SEQUENCE}_d{DATASEQ}.tsm   (compaction output)
```

Two distinct orderings are derived from the name:

- **File identity** (`rankAsInteger()`): `(tier << 60) | sequence`. Unique key
  for file-manager maps; NOT a duplicate-resolution priority.
- **Duplicate resolution** (`dataRank()`): `(dataSeq << 4) | tier`. On equal
  timestamps the file with the higher `dataRank` holds the newer write
  (last-write-wins). `dataSeq` is the newest write generation contained in the
  file: flush-created files use their own sequence (flush order == write order
  per shard); compaction outputs inherit `max(dataSeq)` of their inputs via
  the `_d{DATASEQ}` suffix. A freshly allocated sequence must never be used
  for dedup ranking — it would let an old point compacted into a higher tier
  outrank a tier-0 file holding a newer rewrite. Files without the suffix
  (legacy, or flush-created) fall back to `dataSeq = sequence`.

## Phase 4: Index Pagination (Deferred)

A paginated index layout was considered: writing the index as 64 KB pages with a trailing page directory (`"PIDX"` magic + per-page first-SeriesId + offset) to enable binary search without reading the entire index. After analysis, this was **deferred** for the following reasons:

1. **Low impact**: The existing `prefetchFullIndexEntries()` coalesces multiple series lookups into a single DMA read of the full index section. For typical query patterns this is already near-optimal.
2. **Sparse index eliminates full scans**: The sparse index loaded at file open already provides O(1) series lookup by hash. Full index entries are lazy-loaded per-series via single DMA reads.
3. **Risk/reward**: Changing the on-disk index format introduces migration complexity and backward compatibility burden for marginal improvement on files with 100K+ series -- a rare case given per-shard file counts.

The existing flat index with sparse-index + bloom-filter + LRU cache provides sub-millisecond lookups without format changes.
