# Seastar-Native Index: Implementation Plan

**Branch:** `feature/seastar-native-index`
**Goal:** Replace LevelDB with a Seastar-native LSM-tree index to eliminate thread-pool crossings, enable per-shard metadata, and remove an external dependency.

---

## 1. Motivation

LevelDB is the metadata index for TimeStar, storing series-to-ID mappings, tag/field indexes, and measurement metadata on shard 0. It works, but conflicts with Seastar's architecture in several ways:

| Problem | Impact |
|---------|--------|
| **`seastar::async()` overhead** | Every LevelDB call crosses reactor -> thread pool -> reactor (~5-15us per call even for 1us operations) |
| **Shard 0 bottleneck** | All metadata centralized on shard 0; every insert requires cross-shard RPC via `invoke_on(0, ...)` |
| **No async I/O** | LevelDB uses `pread()`/`pwrite()` internally; Seastar's `file_dma_read()`/`file_dma_write()` would avoid the thread pool entirely |
| **Cache duplication** | LevelDB maintains its own block cache (8MB) + OS page cache, while TimeStar has 3 additional app-level caches (48MB metadata, 16MB discovery, 1M-entry series) |
| **Compaction interference** | LevelDB's background compaction threads can interfere with Seastar's reactor |
| **External dependency** | Requires `libleveldb-dev` system package |

### Expected Performance Gains

| Operation | Current (LevelDB) | Target (Native) |
|-----------|-------------------|-----------------|
| Point Get (cache miss) | ~15-50us (async + LevelDB) | ~2-5us (DMA read, no thread switch) |
| WriteBatch (new series) | ~50-100us (async + LevelDB) | ~5-15us (MemTable insert, no thread switch) |
| Prefix scan (1K series) | ~100-500us (async + iterator) | ~20-50us (in-memory or DMA) |
| Cross-shard insert metadata | ~10-30us RPC overhead | **Eliminated** (local to data shard) |

---

## 2. Current LevelDB Usage Analysis

### 2.1 Files Dependent on LevelDB

**Core implementation (6 files):**
- `lib/index/leveldb_index.hpp` (370 lines) — main interface
- `lib/index/leveldb_index.cpp` (2,728 lines) — full implementation
- `lib/index/metadata_index.hpp/cpp` — older metadata index
- `lib/index/metadata_index_sync.hpp/cpp` — synchronous metadata index

**Callers (~16 files):**
- `lib/core/engine.hpp/cpp` — orchestrator
- `lib/http/http_query_handler.hpp` — query endpoint
- `lib/http/http_retention_handler.cpp` — retention policies
- `lib/http/http_delete_handler.cpp` — delete operations
- `lib/http/http_derived_query_handler.hpp` — derived queries
- `lib/query/query_planner.hpp` — query planning
- `lib/query/derived_query_executor.cpp` — derived query execution
- `lib/storage/shard_rebalancer.cpp` — rebalancing

**Tests (21 files):**
- 11 specialized LevelDB index tests
- Integration, stress, and feature-specific tests

### 2.2 LevelDB APIs in Use

```
DB::Open()              — startup (once)
DB::Get()               — point lookups (high frequency on cache miss)
DB::Put()               — single key writes (low frequency)
DB::Write(WriteBatch)   — atomic batch writes (high frequency: every new series)
DB::NewIterator()       — prefix range scans (high frequency: every query)
DB::Delete()            — single key deletion (rare)
DB::CompactRange()      — manual compaction (rare)
DB::GetProperty()       — diagnostics (rare)
NewBloomFilterPolicy()  — bloom filter config (once)
NewLRUCache()           — block cache config (once)
```

### 2.3 Key Schema (12 Prefixes)

| Prefix | Name | Access Pattern | Key Structure |
|--------|------|---------------|---------------|
| `0x01` | SERIES_INDEX | Legacy, unused | — |
| `0x02` | MEASUREMENT_FIELDS | Point Get | `[0x02] + measurement` |
| `0x03` | MEASUREMENT_TAGS | Point Get | `[0x03] + measurement` |
| `0x04` | TAG_VALUES | Point Get | `[0x04] + measurement + \0 + tagKey` |
| `0x05` | SERIES_METADATA | Point Get | `[0x05] + SeriesId128 (16 bytes)` |
| `0x06` | TAG_INDEX | Prefix Scan | `[0x06] + measurement + \0 + tagKey + \0 + tagValue + \0 + SeriesId128` |
| `0x07` | GROUP_BY_INDEX | Prefix Scan | Same as 0x06 but prefix 0x07 |
| `0x08` | FIELD_STATS | Point Get | `[0x08] + SeriesId128 + \0 + field` |
| `0x09` | FIELD_TYPE | Point Get | `[0x09] + measurement + \0 + field` |
| `0x0A` | MEASUREMENT_SERIES | Prefix Scan | `[0x0A] + measurement + \0 + SeriesId128` |
| `0x0B` | RETENTION_POLICY | Point Get | `[0x0B] + measurement` |
| `0x0C` | MEASUREMENT_FIELD_SERIES | Prefix Scan | `[0x0C] + measurement + \0 + field + \0 + SeriesId128` |

### 2.4 Value Encoding Formats

- **String sets** (0x02-0x04): `[4-byte LE length][string bytes]...` repeated
- **Series metadata** (0x05): `measurement\0field\0tagCount\0tagKey1\0tagValue1\0...`
- **SeriesId128** (0x06-0x07 values): raw 16-byte binary
- **Field stats** (0x08): `dataType\0minTime\0maxTime\0pointCount` (ASCII decimal)
- **Field type** (0x09): plain string (`"float"`, `"boolean"`, `"string"`, `"integer"`)
- **Retention policy** (0x0B): JSON via Glaze

### 2.5 Concurrency Model

- Only shard 0 opens LevelDB
- All blocking calls wrapped in `co_await seastar::async([this, ...] { ... })`
- No locking: Seastar single-threaded-per-shard guarantee
- Three-tier application cache hierarchy:
  - `indexedSeriesCache_`: two-generation set with incremental eviction (1M entries)
  - `seriesMetadataCache_`: LRU (48MB) for SeriesId128 -> SeriesMetadata
  - `discoveryCache_`: LRU (16MB) for query results
  - `fieldsCache` / `tagsCache` / `tagValuesCache`: unbounded maps (bounded by measurement count)

---

## 3. Architecture Design

### 3.1 Component Overview

```
┌─────────────────────────────────────────────────────────┐
│                     NativeIndex                         │
│              (implements IndexBackend)                  │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐  │
│  │ MemTable │  │ WriteBatch│  │  Application Caches  │  │
│  │(skip list)│  │ (buffer) │  │  (series, metadata,  │  │
│  └────┬─────┘  └────┬─────┘  │   discovery, fields) │  │
│       │              │        └──────────────────────┘  │
│       │         ┌────▼─────┐                            │
│       │         │Index WAL │  (crash recovery)          │
│       │         └──────────┘                            │
│       │                                                 │
│  ┌────▼──────────────────────────────────────────────┐  │
│  │              Merge Iterator                       │  │
│  │   (MemTable + immutable MemTables + SSTables)     │  │
│  └───────────────────┬───────────────────────────────┘  │
│                      │                                  │
│  ┌───────────────────▼───────────────────────────────┐  │
│  │                SSTable Layer                       │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐│  │
│  │  │  Writer  │  │  Reader  │  │   Block Cache    ││  │
│  │  │(DMA write)│  │(DMA read)│  │  (shared LRU)   ││  │
│  │  └──────────┘  └──────────┘  └──────────────────┘│  │
│  │                                                   │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐│  │
│  │  │  Block   │  │  Bloom   │  │    Manifest      ││  │
│  │  │ Builder/ │  │  Filter  │  │  (version set)   ││  │
│  │  │ Reader   │  │          │  │                   ││  │
│  │  └──────────┘  └──────────┘  └──────────────────┘│  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │              Compaction Engine                     │  │
│  │        (background Seastar coroutine)              │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Sharding Strategy (Phase 5)

```
Current:                              Proposed:

  Shard 0: LevelDB (ALL metadata)      Shard 0: NativeIndex (its series + measurement metadata)
  Shard 1: invoke_on(0) ──►            Shard 1: NativeIndex (its series, local lookups)
  Shard 2: invoke_on(0) ──►            Shard 2: NativeIndex (its series, local lookups)
  Shard 3: invoke_on(0) ──►            Shard 3: NativeIndex (its series, local lookups)

Per-series metadata (0x05-0x08, 0x0A, 0x0C) → local to data shard
Measurement metadata (0x02-0x04, 0x09, 0x0B)  → shard 0 with broadcast
```

### 3.3 On-Disk File Format

#### SSTable Layout

```
┌─────────────────────────────────┐
│         Data Block 0            │  Sorted KV pairs, Snappy compressed
│         Data Block 1            │
│         ...                     │
│         Data Block N            │
├─────────────────────────────────┤
│         Bloom Filter Block      │  Per-SSTable bloom filter
├─────────────────────────────────┤
│         Index Block             │  First key + offset per data block
├─────────────────────────────────┤
│         Footer (48 bytes)       │  Offsets to index/bloom, magic, version
└─────────────────────────────────┘
```

#### Data Block Layout (prefix-compressed)

```
┌──────────────────────────────────────────────────┐
│ Entry 0: [shared_len][unshared_len][val_len][key_suffix][value] │
│ Entry 1: [shared_len][unshared_len][val_len][key_suffix][value] │
│ ...                                                              │
│ Restart point every 16 entries (for binary search)               │
├──────────────────────────────────────────────────┤
│ Restart offsets array (uint32_t each)            │
│ Restart count (uint32_t)                         │
└──────────────────────────────────────────────────┘
```

#### Footer Layout (48 bytes)

```
┌────────────────────────────┐
│ bloom_offset    (uint64_t) │
│ bloom_size      (uint64_t) │
│ index_offset    (uint64_t) │
│ index_size      (uint64_t) │
│ entry_count     (uint64_t) │
│ magic           (uint32_t) │  0x54534958 ("TSIX")
│ version         (uint32_t) │  1
└────────────────────────────┘
```

### 3.4 WAL Record Format

```
┌────────────────────────────────────┐
│ record_length   (uint32_t)         │  Length of payload
│ crc32           (uint32_t)         │  CRC32 of payload
│ sequence_number (uint64_t)         │  Monotonically increasing
│ payload         (variable)         │  Serialized WriteBatch
└────────────────────────────────────┘
```

WriteBatch serialization:
```
┌────────────────────────────────────┐
│ op_count        (uint32_t)         │
│ For each operation:                │
│   op_type       (uint8_t)          │  0=Put, 1=Delete
│   key_length    (uint32_t)         │
│   key           (variable)         │
│   value_length  (uint32_t)         │  (Put only)
│   value         (variable)         │  (Put only)
└────────────────────────────────────┘
```

### 3.5 Manifest Format

```
┌────────────────────────────────────┐
│ SNAPSHOT record (periodic):        │
│   file_count    (uint32_t)         │
│   For each file:                   │
│     file_number (uint64_t)         │
│     level       (uint32_t)         │
│     file_size   (uint64_t)         │
│     min_key_len (uint32_t)         │
│     min_key     (variable)         │
│     max_key_len (uint32_t)         │
│     max_key     (variable)         │
│     entry_count (uint64_t)         │
│                                    │
│ DELTA records (append):            │
│   op_type       (uint8_t)          │  0=AddFile, 1=RemoveFile
│   file_number   (uint64_t)         │
│   level         (uint32_t)         │  (AddFile only)
│   file_size     (uint64_t)         │  (AddFile only)
│   min_key/max_key (variable)       │  (AddFile only)
│   entry_count   (uint64_t)         │  (AddFile only)
└────────────────────────────────────┘
```

---

## 4. Detailed Phase Plan

### Phase 1: Extract IndexBackend Interface

**Risk:** Low (refactor only, no behavior change)
**Effort:** Small

Create `lib/index/index_backend.hpp` with a pure virtual interface extracted from `LevelDBIndex`'s public methods. Make `LevelDBIndex` inherit from it. Update all callers to use `IndexBackend*`.

```cpp
// lib/index/index_backend.hpp
class IndexBackend {
public:
    virtual ~IndexBackend() = default;

    virtual seastar::future<> open() = 0;
    virtual seastar::future<> close() = 0;

    // Series indexing
    virtual seastar::future<SeriesId128> getOrCreateSeriesId(
        std::string measurement,
        std::map<std::string, std::string> tags,
        std::string field) = 0;

    virtual seastar::future<std::optional<SeriesId128>> getSeriesId(
        const std::string& measurement,
        const std::map<std::string, std::string>& tags,
        const std::string& field) = 0;

    // ... all other public methods from LevelDBIndex
};
```

**Validation:** All 2833 tests pass, no functional change.

### Phase 2: Core LSM Components

These components are independent building blocks. Phases 2a-2f can be developed and tested in isolation.

#### Phase 2a: MemTable

In-memory sorted key-value store. Backed by `std::map<std::string, std::string>` initially (simpler than skip list, adequate performance for metadata workload). Can upgrade to skip list later if profiling shows map overhead.

```cpp
class MemTable {
public:
    void put(std::string_view key, std::string_view value);
    std::optional<std::string_view> get(std::string_view key) const;
    void remove(std::string_view key);  // insert tombstone

    size_t approximateMemoryUsage() const;
    bool empty() const;

    // Iteration
    class Iterator {
    public:
        void seek(std::string_view target);
        void seekToFirst();
        void next();
        bool valid() const;
        std::string_view key() const;
        std::string_view value() const;
        bool isTombstone() const;
    };

    Iterator newIterator() const;

    // Snapshot for flushing
    std::unique_ptr<MemTable> freeze();  // Returns this, caller creates new MemTable
};
```

#### Phase 2b: WriteBatch

Buffers Put/Delete operations for atomic application.

```cpp
class IndexWriteBatch {
public:
    void put(std::string_view key, std::string_view value);
    void remove(std::string_view key);
    void clear();
    size_t count() const;
    size_t approximateSize() const;

    // Apply all operations to a MemTable
    void applyTo(MemTable& memtable) const;

    // Serialization for WAL
    void serializeTo(std::string& output) const;
    static IndexWriteBatch deserializeFrom(std::string_view data);
};
```

#### Phase 2e: Bloom Filter

```cpp
class BloomFilter {
public:
    explicit BloomFilter(int bits_per_key);

    // Build phase
    void addKey(std::string_view key);
    void build();  // Finalize filter

    // Query phase
    bool mayContain(std::string_view key) const;

    // Serialization
    void serializeTo(std::string& output) const;
    static BloomFilter deserializeFrom(std::string_view data, int bits_per_key);

private:
    int bits_per_key_;
    std::vector<uint8_t> filter_;
    std::vector<std::string> keys_;  // Pre-build only
};
```

#### Phase 2f: Block Format

```cpp
class BlockBuilder {
public:
    explicit BlockBuilder(int restart_interval = 16);

    void add(std::string_view key, std::string_view value);
    std::string finish();  // Returns compressed block bytes
    size_t currentSize() const;
    bool empty() const;
    void reset();
};

class BlockReader {
public:
    explicit BlockReader(std::string_view data);  // Decompressed block

    class Iterator {
    public:
        void seek(std::string_view target);
        void seekToFirst();
        void next();
        bool valid() const;
        std::string_view key() const;
        std::string_view value() const;
    };

    Iterator newIterator() const;
};
```

#### Phase 2c: SSTable Writer

```cpp
class SSTableWriter {
public:
    // Opens a new SSTable file for writing
    static seastar::future<SSTableWriter> create(
        std::string filename,
        int block_size = 16384,
        int bloom_bits_per_key = 15);

    // Add entries in sorted order
    seastar::future<> add(std::string_view key, std::string_view value);

    // Finalize and close the file
    seastar::future<SSTableMetadata> finish();

private:
    seastar::file file_;
    BlockBuilder currentBlock_;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
    uint64_t offset_ = 0;
};
```

#### Phase 2d: SSTable Reader

```cpp
class SSTableReader {
public:
    // Open an existing SSTable
    static seastar::future<std::unique_ptr<SSTableReader>> open(
        std::string filename,
        std::shared_ptr<BlockCache> cache = nullptr);

    // Point lookup (uses bloom filter)
    seastar::future<std::optional<std::string>> get(std::string_view key);

    // Range iteration
    class Iterator {
    public:
        seastar::future<> seek(std::string_view target);
        seastar::future<> next();
        bool valid() const;
        std::string_view key() const;
        std::string_view value() const;
    };

    seastar::future<std::unique_ptr<Iterator>> newIterator();

    // Metadata
    const SSTableMetadata& metadata() const;
    uint64_t fileNumber() const;

private:
    seastar::file file_;
    BloomFilter bloom_;
    std::vector<IndexEntry> index_;
    std::shared_ptr<BlockCache> blockCache_;
};

// Shared block cache (per-shard, across all SSTables)
class BlockCache {
public:
    explicit BlockCache(size_t capacity_bytes);

    std::optional<std::string_view> get(uint64_t file_id, uint64_t block_offset);
    void insert(uint64_t file_id, uint64_t block_offset, std::string block_data);
    void evictFile(uint64_t file_id);
};
```

### Phase 3: Durability and Compaction

#### Phase 3a: Index WAL

```cpp
class IndexWAL {
public:
    static seastar::future<IndexWAL> open(std::string directory);

    // Append a write batch (returns after fsync)
    seastar::future<> append(const IndexWriteBatch& batch);

    // Replay all records into a MemTable
    seastar::future<> replay(MemTable& target);

    // Rotate: close current WAL, start new file
    seastar::future<> rotate();

    // Delete old WAL files after flush
    seastar::future<> deleteOldFiles(uint64_t flushed_sequence);

    seastar::future<> close();
};
```

#### Phase 3b: Merge Iterator

```cpp
class MergeIterator {
public:
    // Sources: MemTable iterator(s) + SSTable iterators
    // Newest source wins for duplicate keys; tombstones suppress older values
    explicit MergeIterator(std::vector<std::unique_ptr<IteratorSource>> sources);

    seastar::future<> seek(std::string_view target);
    seastar::future<> next();
    bool valid() const;
    std::string_view key() const;
    std::string_view value() const;
};
```

#### Phase 3c: Compaction Engine

```cpp
class CompactionEngine {
public:
    CompactionEngine(std::string data_dir, Manifest& manifest,
                     std::shared_ptr<BlockCache> cache);

    // Background compaction loop (runs as Seastar coroutine)
    seastar::future<> run();

    // Manual compaction trigger
    seastar::future<> compactRange(std::optional<std::string_view> begin,
                                    std::optional<std::string_view> end);

    // Stop background compaction
    seastar::future<> stop();

private:
    // Pick files for compaction based on level thresholds
    std::optional<CompactionJob> pickCompaction();

    // Execute a compaction job
    seastar::future<> doCompaction(CompactionJob job);
};
```

#### Phase 3d: Manifest

```cpp
class Manifest {
public:
    static seastar::future<Manifest> open(std::string directory);

    // Current version
    const std::vector<SSTableFileInfo>& filesAtLevel(int level) const;
    std::vector<SSTableFileInfo> allFiles() const;

    // Mutations
    seastar::future<> addFile(SSTableFileInfo info);
    seastar::future<> removeFiles(std::vector<uint64_t> file_numbers);

    // Recovery
    seastar::future<> recover();

    // Snapshot (periodic, to limit manifest replay time)
    seastar::future<> writeSnapshot();

    seastar::future<> close();

    // File number generator
    uint64_t nextFileNumber();
};
```

### Phase 4: Assembly

#### Phase 4a: NativeIndex

Wire all components into a complete IndexBackend implementation. This is the largest single task — it re-implements all ~50 methods from LevelDBIndex using the native LSM primitives.

Key method mapping:
- `DB::Get()` → `memtable.get()` then `sstables[i].get()` newest-to-oldest
- `DB::Put()` → `wal.append(batch)` then `memtable.put()`
- `DB::Write(batch)` → `wal.append(batch)` then `batch.applyTo(memtable)`
- `DB::NewIterator()` → `MergeIterator(memtable_iter, sstable_iters...)`
- `DB::Delete()` → `memtable.remove()` (insert tombstone)
- `DB::CompactRange()` → `compaction.compactRange()`
- Background: MemTable flush when size > threshold, compaction when L0 files > threshold

#### Phase 4b: Key Encoding Helpers

Extract all key encoding/decoding from `leveldb_index.cpp` into shared `lib/index/key_encoding.hpp/cpp`. Both `LevelDBIndex` and `NativeIndex` use the same key format — the data is identical, only the storage engine changes.

### Phase 5: Sharded Metadata

The biggest architectural change: eliminate the shard-0 bottleneck.

#### Phase 5a: Per-Shard Metadata

Each shard opens its own NativeIndex in `shard_N/index/`:

```
shard_0/index/   → NativeIndex (series metadata for shard 0's data + measurement-level metadata)
shard_1/index/   → NativeIndex (series metadata for shard 1's data)
shard_2/index/   → NativeIndex (series metadata for shard 2's data)
shard_3/index/   → NativeIndex (series metadata for shard 3's data)
```

**Per-series metadata** (prefixes 0x05-0x08, 0x0A, 0x0C) moves to the shard that owns the data for that series. Since data sharding is `SeriesId128::Hash{} % smp::count`, the metadata follows the same function.

**Measurement-level metadata** (prefixes 0x02-0x04, 0x09, 0x0B) stays on shard 0 because it's naturally small, rarely changes, and is heavily cached. New field/tag discovery broadcasts to shard 0.

#### Phase 5b: Caller Updates

**Insert path (hot path, biggest win):**
```
Before: HTTP shard N → invoke_on(0, getOrCreateSeriesId) → RPC back → local insert
After:  HTTP shard N → local getOrCreateSeriesId() → local insert
```

**Query path:**
```
Before: fan-out to all shards for data, shard 0 resolves all metadata
After:  fan-out to all shards, each shard resolves its own metadata locally
```

### Phase 6: Validation

Full regression + performance testing:

1. All 2833+ C++ unit tests pass
2. All 46 Jest API tests pass
3. All 8 standalone integration tests pass
4. Insert benchmark: target improvement over 15.55M pts/sec baseline
5. Query benchmark: target improvement over current (3.5-10.6x faster than InfluxDB)
6. Stress test: 1M+ unique series creation
7. Crash recovery: WAL replay after kill -9
8. Compaction correctness: no data loss after multiple cycles

### Phase 7: Cleanup

Remove LevelDB dependency entirely:
- Delete `leveldb_index.hpp/cpp`, `metadata_index*.hpp/cpp`
- Remove from CMakeLists.txt
- Update CLAUDE.md and documentation
- Verify clean build without LevelDB installed

---

## 5. File Layout

### New Files

```
lib/index/
├── index_backend.hpp                   # Phase 1: abstract interface
├── key_encoding.hpp                    # Phase 4b: shared key encoding
├── key_encoding.cpp                    # Phase 4b: shared key encoding
└── native/
    ├── memtable.hpp                    # Phase 2a
    ├── memtable.cpp                    # Phase 2a
    ├── write_batch.hpp                 # Phase 2b
    ├── write_batch.cpp                 # Phase 2b
    ├── sstable_writer.hpp              # Phase 2c
    ├── sstable_writer.cpp              # Phase 2c
    ├── sstable_reader.hpp              # Phase 2d
    ├── sstable_reader.cpp              # Phase 2d
    ├── bloom_filter.hpp                # Phase 2e
    ├── bloom_filter.cpp                # Phase 2e
    ├── block.hpp                       # Phase 2f
    ├── block.cpp                       # Phase 2f
    ├── block_cache.hpp                 # Phase 2d (part of reader)
    ├── block_cache.cpp                 # Phase 2d
    ├── index_wal.hpp                   # Phase 3a
    ├── index_wal.cpp                   # Phase 3a
    ├── merge_iterator.hpp              # Phase 3b
    ├── merge_iterator.cpp              # Phase 3b
    ├── compaction.hpp                  # Phase 3c
    ├── compaction.cpp                  # Phase 3c
    ├── manifest.hpp                    # Phase 3d
    ├── manifest.cpp                    # Phase 3d
    ├── native_index.hpp                # Phase 4a
    └── native_index.cpp                # Phase 4a

test/unit/index/
├── memtable_test.cpp                   # Phase 2a
├── write_batch_test.cpp                # Phase 2b
├── sstable_writer_test.cpp             # Phase 2c
├── sstable_reader_test.cpp             # Phase 2d
├── bloom_filter_test.cpp               # Phase 2e
├── block_test.cpp                      # Phase 2f
├── index_wal_test.cpp                  # Phase 3a
├── merge_iterator_test.cpp             # Phase 3b
├── compaction_test.cpp                 # Phase 3c
├── manifest_test.cpp                   # Phase 3d
├── native_index_test.cpp               # Phase 4a
└── key_encoding_test.cpp               # Phase 4b
```

### Files Modified

```
lib/index/leveldb_index.hpp             # Phase 1: inherit IndexBackend
lib/index/leveldb_index.cpp             # Phase 4b: use shared key encoding
lib/core/engine.hpp                     # Phase 1, 5: use IndexBackend, sharded metadata
lib/core/engine.cpp                     # Phase 1, 5: use IndexBackend, sharded metadata
lib/http/http_query_handler.hpp         # Phase 1: use IndexBackend
lib/http/http_retention_handler.cpp     # Phase 1: use IndexBackend
lib/http/http_delete_handler.cpp        # Phase 1: use IndexBackend
lib/http/http_derived_query_handler.hpp # Phase 1: use IndexBackend
lib/query/query_planner.hpp             # Phase 1: use IndexBackend
lib/query/derived_query_executor.cpp    # Phase 1: use IndexBackend
lib/storage/shard_rebalancer.cpp        # Phase 1: use IndexBackend
lib/CMakeLists.txt                      # Phase 2+: add new source files
CMakeLists.txt                          # Phase 7: remove LevelDB
```

---

## 6. Dependency Graph

```
Phase 1 (IndexBackend interface)
    │
    ├──► Phase 2a (MemTable)
    │        │
    ├──► Phase 2b (WriteBatch) ────────────────────┐
    │        │                                      │
    ├──► Phase 2e (Bloom Filter)                    │
    │        │                                      │
    ├──► Phase 2f (Block Format)                    │
    │        │                                      │
    │   Phase 2c (SSTable Writer) ◄─ 2e, 2f         │
    │        │                                      │
    │   Phase 2d (SSTable Reader) ◄─ 2e, 2f         │
    │        │                                      │
    │   Phase 3a (Index WAL) ◄─────── 2a, 2b ──────┘
    │        │
    │   Phase 3b (Merge Iterator) ◄── 2a, 2d
    │        │
    │   Phase 3c (Compaction) ◄────── 2c, 2d, 3b
    │        │
    │   Phase 3d (Manifest) ◄──────── 2c
    │        │
    ├──► Phase 4b (Key Encoding) ─────┐
    │                                  │
    │   Phase 4a (NativeIndex) ◄────── 3a, 3b, 3c, 3d, 4b
    │        │
    │   Phase 5a (Shard Metadata) ◄── 4a
    │        │
    │   Phase 5b (Caller Updates) ◄── 4a, 5a
    │        │
    │   Phase 6 (Validation) ◄──────── 4a, 5b
    │        │
    │   Phase 7 (Cleanup) ◄─────────── 6
```

---

## 7. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| SSTable format bugs cause data loss | High | Extensive fuzz testing, CRC checksums, compare with LevelDB output |
| Compaction correctness | High | Test with concurrent reads/writes, verify key counts before/after |
| WAL replay correctness | High | Crash-recovery tests, inject failures at every write point |
| Performance regression | Medium | Benchmark at each phase; Phase 4a can fall back to LevelDB |
| MemTable memory pressure | Medium | Same thresholds as LevelDB write_buffer_size (16MB) |
| DMA alignment issues | Medium | Seastar requires 4KB-aligned reads; pad blocks accordingly |
| Migration from existing LevelDB data | Low | Phase 4a can include a one-time migration tool |

### Fallback Plan

The IndexBackend interface (Phase 1) allows switching between LevelDB and NativeIndex at startup via configuration. If the native index has issues, revert to `--index-backend=leveldb` without code changes.

---

## 8. Alternatives Considered

| Alternative | Why Not |
|-------------|---------|
| **RocksDB** | Same blocking I/O problem as LevelDB; wrapping in `seastar::async` gives no improvement |
| **ScyllaDB internals** | Not available as standalone library |
| **LMDB** | Memory-mapped I/O conflicts with Seastar's DMA model; single-writer limitation |
| **In-memory only (no LSM)** | Metadata must survive restarts; WAL-only recovery too slow for 1M+ series |
| **SQLite** | Blocking I/O, no prefix scan optimization, overhead for simple KV patterns |

The custom implementation is justified because:
1. The key schema is simple (12 prefix types, two access patterns)
2. TimeStar already implements LSM-style storage (TSM files)
3. No existing library provides Seastar-native LSM with DMA I/O
4. The total code is estimated at ~3000-4000 lines (comparable to the existing LevelDB wrapper at ~2800 lines)
