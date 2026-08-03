# TSM file format

**Format version:** 1

TimeStar is greenfield. The current immutable columnar layout is the v1 layout;
there are no historical readers or migration branches. Writers update v1 in
place during development. A file with any other version is rejected.

## File layout

```text
+-------------------------------+
| header                 5 B    |
+-------------------------------+
| data blocks                   |
+-------------------------------+
| sorted series index           |
+-------------------------------+
| index CRC32             4 B    |
+-------------------------------+
| maximum revision       8 B    |
+-------------------------------+
| index offset           8 B    |
+-------------------------------+
| footer CRC32            4 B    |
+-------------------------------+
```

All multi-byte integers are little-endian.

## Header

| Offset | Size | Type | Value |
|---:|---:|---|---|
| 0 | 4 | `char[4]` | `TASM` |
| 4 | 1 | `uint8` | `1` |

The minimum valid file is 29 bytes: the header plus the 24-byte authenticated
footer. An empty sorted index is valid and has CRC32 value zero.

## Data blocks

Each block contains one series and up to `max_points_per_block` points. The
block header is:

| Offset | Size | Type | Description |
|---:|---:|---|---|
| 0 | 1 | `uint8` | `TSMValueType` |
| 1 | 4 | `uint32` | point count |
| 5 | 4 | `uint32` | compressed timestamp byte count |

The body contains compressed timestamps followed by compressed values.
The complete block header and body are covered by the CRC32 stored with that
block's authenticated index metadata. A data block is verified whenever its
bytes are read, including ordinary queries, batched queries, pushdown paths
that need data, and zero-copy compaction. Metadata-only answers use the
authenticated index and do not read the block.

| Value | Type | C++ representation | Encoding |
|---:|---|---|---|
| 0 | Float | `double` | ALP |
| 1 | Boolean | `bool` | RLE bit packing |
| 2 | String | `std::string` | raw or dictionary IDs |
| 3 | Integer | `int64_t` | ZigZag + FFOR |

String value payloads use a 16-byte v1 header: marker, uncompressed byte
count, compressed byte count, and value count. Raw zstd blocks use `STR1`;
dictionary-ID blocks use `STD1`. These are two encodings within TSM v1, not
separate format versions.

Timestamp FFOR encoding has a densest theoretical representation of 64 values
per byte. The reader applies a four-times safety margin when validating the
declared point count, then verifies decoded timestamp and value counts agree.

## Series index

Series entries are sorted by `SeriesId128`.

| Offset | Size | Type | Description |
|---:|---:|---|---|
| 0 | 16 | bytes | `SeriesId128` |
| 16 | 1 | `uint8` | value type |
| 17 | 4 | `uint32` | block count |
| 21 | variable | blocks | block metadata |

Every block begins with 28 common bytes:

| Offset | Size | Type | Description |
|---:|---:|---|---|
| 0 | 8 | `uint64` | minimum timestamp |
| 8 | 8 | `uint64` | maximum timestamp |
| 16 | 8 | `uint64` | byte offset |
| 24 | 4 | `uint32` | byte size |

Type-specific statistics follow the common fields:

| Type | Stats bytes | Fields |
|---|---:|---|
| Float | 52 | sum, min, max, non-NaN count, M2, first, latest |
| Integer | 44 | count, sum, min, max, first, latest |
| Boolean | 12 | count, true count, first, latest, padding |
| String | 4 | count |

Each type-specific section is followed by `minRevision:uint64`,
`maxRevision:uint64`, and a `blockCRC32:uint32`. Total serialized block
metadata sizes are therefore 100 bytes for Float, 92 for Integer, 60 for
Boolean, and 52 for String.

Float statistics follow the canonical NaN policy in `docs/nan_policy.md`:
statistics and `blockCount` exclude NaNs. A NaN endpoint sentinel disables
FIRST/LATEST/M2 pushdown so the values are decoded and folded canonically.

String series append `dictionarySize:uint32` and that many serialized
dictionary bytes after their block metadata. A zero size means raw string
blocks. A dictionary-ID block without its dictionary is corrupt and fails closed.

Every complete series entry ends with a CRC32 covering the series ID, type,
block count, all block metadata, and the optional string dictionary. The whole
sorted index has a second CRC32 in the footer. Open authenticates and
structurally parses the complete index before registering the file; lazy index
loads authenticate the individual entry again. Series IDs must be strictly
ascending and unique. Block time/revision ranges must be ordered and valid,
and every block byte range must remain between the header and index boundary.

## Footer

The final 24 bytes are:

| Offset | Size | Type | Description |
|---:|---:|---|---|
| file size - 24 | 4 | `uint32` | CRC32 of the complete sorted index |
| file size - 20 | 8 | `uint64` | maximum revision in the file |
| file size - 12 | 8 | `uint64` | index start offset |
| file size - 4 | 4 | `uint32` | CRC32 of the preceding 20 footer bytes |

The maximum revision lets recovery restore its revision allocator without
loading every full index entry. Open checks that it equals the maximum block
revision found while structurally validating the authenticated index.

## Tombstone sidecar

The `.tsm` suffix replaced by `.tombstone` names the separate v1 sidecar. Its
header contains `TSMT`, version 1, entry count, and a CRC32. Each entry contains a 16-byte
`SeriesId128`, inclusive start/end timestamps, and a CRC32. Any unsupported
version, malformed entry, or checksum failure fences startup for that immutable
generation.

## File names and ordering

```text
{TIER}_{SEQUENCE}.tsm
{TIER}_{SEQUENCE}_d{DATA_SEQUENCE}.tsm
```

Numeric fields use canonical decimal spelling: no sign or leading-zero alias is
accepted. Retired offline-rebalance names are unsupported.

`(tier << 60) | sequence` is the unique file identity. `dataSequence` is the
newest write generation represented by the file and determines last-write-wins
ranking across compaction. Flush outputs use their sequence as the data
sequence; compaction outputs inherit the maximum input data sequence.

## Compatibility policy

- Writers emit v1 only.
- Readers accept v1 only.
- There is no in-place upgrade or downgrade path.
- Changing the layout during greenfield development means updating v1 and
  recreating test/development data.
- A future post-release format change must introduce v2 deliberately, with a
  migration and release policy designed at that time.
