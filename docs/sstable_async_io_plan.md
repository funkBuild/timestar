# SSTable Async I/O Migration Plan (Revised)

## Problem

`SSTableReader::decompressBlock()` at `lib/index/native/sstable.cpp:511` uses synchronous `::pread()` to read data blocks from disk. This blocks the Seastar reactor thread on every block cache miss, stalling all coroutines on that shard. On loaded systems or slow storage, this can cause reactor stalls of 1-10ms per read, triggering Seastar's blocked-reactor warnings and degrading latency for all concurrent operations.

---

## ScyllaDB Reference Architecture

ScyllaDB's SSTable implementation is the canonical reference for Seastar-native storage I/O. Key architectural lessons from studying their source (`sstables/` directory, ~15,000 lines):

### Core Principle: Zero Blocking I/O

ScyllaDB **never** uses POSIX `read()`/`pread()`/`write()`. Every byte of SSTable I/O goes through Seastar's async DMA API. Files are opened with `O_DIRECT`, bypassing the OS page cache entirely. ScyllaDB manages all caching itself.

### Read-Side Architecture: `file_random_access_reader`

ScyllaDB's key read abstraction (`sstables/random_access_reader.hh`):

```cpp
class file_random_access_reader : public random_access_reader {
    file _file;
    size_t _buffer_size;     // Default 8192
    unsigned _read_ahead;    // Default 4 concurrent buffers

    input_stream<char> open_at(uint64_t pos) override {
        file_input_stream_options options;
        options.buffer_size = _buffer_size;
        options.read_ahead = _read_ahead;
        return make_file_input_stream(_file, pos, _file_size - pos, options);
    }
};
```

**Critical design choice:** `seek()` doesn't reposition a stream — it **closes the old stream and creates a new one** at the target position. Each new stream benefits from independent read-ahead. Three separate `file_input_stream_history` objects track I/O patterns for point lookups, range scans, and index traversal independently, preventing one access pattern from polluting another's prefetch decisions.

### Write-Side Architecture: `file_writer`

ScyllaDB's write wrapper (`sstables/file_writer.hh`):

```cpp
class file_writer {
    output_stream<char> _out;
    writer_offset_tracker _offset;
    void write(const char* buf, size_t n) {
        _offset.offset += n;
        _out.write(buf, n).get();  // .get() is safe inside seastar::thread
    }
};
```

Write configuration:
- Buffer size: 128 KB (`sstable_buffer_size`)
- Write-behind: 10 outstanding async write chunks
- Extent allocation hint: 32 MB for large sequential writes
- Uses `open_file_dma()` with `O_DIRECT | O_WRONLY | O_CREAT | O_EXCL`

### Compression Layer

Compression is transparent, layered between the consumer and the raw DMA stream:

```
Consumer ← input_stream<char>
              ← compressed_file_data_source_impl
                  ← input_stream<char> (raw chunks)
                      ← make_file_input_stream (DMA)
```

For reads: chunks are read asynchronously, decompressed, and checksummed per-chunk. For writes: `make_compressed_file_m_format_output_stream()` wraps the output.

### Hierarchical Read Path (Summary → Index → Data)

ScyllaDB minimizes I/O via a hierarchical lookup with progressively finer granularity:

```
Bloom Filter (in-memory)
  → Summary Index (in-memory, downsampled keys)
    → Partition Index Pages (LRU-cached via partition_index_cache)
      → Promoted Index (per-partition clustering-key samples)
        → Data Blocks (read via file_random_access_reader)
```

The Summary is always in memory (small). Index pages are LRU-cached and shared across concurrent readers. Data blocks are read on demand via async DMA.

### Index Reader Design

The index reader (`sstables/index_reader.hh`) maintains **two independent cursors** for range queries:

```cpp
class index_reader {
    index_bound _lower_bound;  // Range start
    index_bound _upper_bound;  // Range end (for skip-ahead)
};
```

Each bound has its own stream position, cached page, and clustering cursor. The lower bound cursor navigates forward through partitions; the upper bound cursor can skip ahead to determine when to stop.

### Consumer State Machine Pattern

Data parsing uses `continuous_data_consumer<StateProcessor>` — a state machine driven by `input_stream::consume()`:

1. Consumer processes bytes from the current buffer
2. When buffer is exhausted, returns `continue_consuming` to the stream
3. Stream issues another async DMA read, calls consumer again with new data
4. Consumer can save/restore state across buffer boundaries
5. Returns `stop_consuming` when a complete result is available

This avoids manual buffer-boundary handling and naturally supports Seastar's async model.

### Reader Concurrency Semaphore

`reader_concurrency_semaphore` controls memory and concurrent readers:
- Each read requires a `reader_permit` (1 count + ~128KB memory budget)
- `make_tracked_file(f, permit)` wraps a file to account I/O against the permit
- Inactive readers can be evicted under memory pressure
- Prevents OOM from many concurrent cache-miss block reads

### Atomic Write Safety

- All component files written to temporary paths first
- `seal_sstable()` atomically renames `TOC.txt.tmp` → `TOC.txt`
- Implicit deletion mark set at creation, cleared on successful seal
- Crash mid-write → incomplete SSTable cleaned up on restart

### I/O Scheduling

Seastar's I/O scheduler sits between application requests and the kernel:
- Per-priority-class queues (query reads, compaction, streaming)
- Fair queuing ensures interactive reads get priority over background compaction
- Our TimeStar already uses Seastar scheduling groups for this

---

## Our Current Architecture

```
SSTableReader
├── readFd_ (POSIX fd, O_RDONLY)              ← BLOCKING
├── decompressBlock(idx) → pread() → zstd     ← BLOCKING
├── getDecompressedBlock(idx) → cache/decompress ← BLOCKING on miss
├── get(key) → findBlock → getDecompressedBlock  ← BLOCKING on miss
├── contains(key) → findBlock → getDecompressedBlock ← BLOCKING on miss
├── Iterator::loadBlock → decompressBlock      ← BLOCKING
└── blockCache_ (LRU, optional)               ← cache hit avoids I/O

SSTableWriter
├── file_ (seastar::file, DMA)                ← already async
├── flushPending() → co_await file_.dma_write  ← already async
└── finish() → co_await                       ← already async
```

**Note:** The writer already uses Seastar DMA correctly. Only the reader needs migration.

### Call Sites That Block the Reactor

| Caller | File | How it reaches `pread()` |
|--------|------|--------------------------|
| `NativeIndex::kvGet()` | `native_index.cpp` | `reader->get(key)` |
| `NativeIndex::kvExists()` | `native_index.cpp` | `reader->contains(key)` |
| `NativeIndex::kvPrefixScan()` | `native_index.cpp` | `Iterator::next()` → `loadBlock()` |
| `Compaction::compactFiles()` | `compaction.cpp` | `Iterator::next()` → `loadBlock()` |
| `MergeIterator` (via sources) | `merge_iterator.cpp` | delegates to SSTable iterator |

---

## Migration Plan

### Phase 1: Async Block Reads (Replace `pread` with `dma_read`)

**Goal:** Eliminate all reactor blocking. The block cache ensures cache hits remain zero-cost.

#### 1.1 Replace POSIX fd with `seastar::file`

```cpp
// sstable.hpp — SSTableReader private members
// Before:
int readFd_ = -1;

// After:
seastar::file readFile_;
```

In `open()`: replace `::open()` with `co_await seastar::open_file_dma()`.
In `close()`: replace `::close(readFd_)` with `co_await readFile_.close()`.
In destructor: remove the `::close(readFd_)` call (Seastar file handles are reference-counted).

#### 1.2 Make `decompressBlock()` a coroutine

```cpp
// Before:
std::string decompressBlock(size_t blockIndex) const;

// After:
seastar::future<std::string> decompressBlock(size_t blockIndex) const;
```

Replace the `::pread()` call with:
```cpp
auto buf = co_await readFile_.dma_read<uint8_t>(entry.offset, entry.size);
```

`seastar::file::dma_read<T>(pos, len)` handles DMA alignment internally — it reads the enclosing aligned region and returns only the requested bytes as a `temporary_buffer<T>`. No manual alignment code needed.

The rest of the function (CRC check, zstd decompress) remains synchronous within the coroutine.

#### 1.3 Make `getDecompressedBlock()` async

```cpp
// Before:
const std::string& getDecompressedBlock(size_t idx, std::string& fallback) const;

// After:
seastar::future<std::string> getDecompressedBlock(size_t idx) const;
```

On cache hit, return the cached block synchronously (wrapped in `make_ready_future` implicitly by `co_return`). On cache miss, `co_await decompressBlock()`, insert into cache, return.

#### 1.4 Make `get()` and `contains()` coroutines

```cpp
// Before:
std::optional<std::string> get(std::string_view key);
bool contains(std::string_view key);

// After:
seastar::future<std::optional<std::string>> get(std::string_view key);
seastar::future<bool> contains(std::string_view key);
```

The bloom filter check and `findBlock()` binary search remain synchronous (all data is in memory). Only the `getDecompressedBlock()` call becomes `co_await`.

#### 1.5 Make Iterator operations async

Following ScyllaDB's approach, the Iterator gets async `seek`/`seekToFirst`/`loadBlock`:

```cpp
class Iterator {
public:
    // Async: these may trigger block I/O
    seastar::future<> seek(std::string_view target);
    seastar::future<> seekToFirst();
    seastar::future<> nextBlock();  // advance to next block (async I/O)

    // Sync: operate within the current loaded block (no I/O)
    bool valid() const;
    std::string_view key() const;
    std::string_view value() const;
    void next();  // advance within current block

    // True when current block is exhausted and nextBlock() is needed
    bool blockExhausted() const;
};
```

Within a loaded block, `next()` remains synchronous (fast). When the block is exhausted, callers call `co_await nextBlock()` to load the next one.

#### 1.6 Update IteratorSource interface

The `MergeIterator` already has async source methods (`seek()`, `seekToFirst()`, `next()` returning `future<>`). The `SSTableIteratorSource` wrapper needs to wire through to the async Iterator:

```cpp
// In compaction.cpp and native_index.cpp:
class SSTableIteratorSource : public IteratorSource {
    seastar::future<> seek(std::string_view target) override {
        co_await iter_->seek(target);
    }
    seastar::future<> seekToFirst() override {
        co_await iter_->seekToFirst();
    }
    seastar::future<> next() override {
        iter_->next();  // sync within block
        if (iter_->blockExhausted()) {
            co_await iter_->nextBlock();  // async: loads next block
        }
    }
    // Sync methods (for backward compat where callers can't co_await):
    void nextSync() override {
        iter_->next();
        if (iter_->blockExhausted()) {
            iter_->nextBlock().get();  // ONLY safe inside seastar::async()
        }
    }
};
```

#### 1.7 Update NativeIndex callers

| Method | Change |
|--------|--------|
| `kvGet()` | Already a coroutine. Change `reader->get(key)` to `co_await reader->get(key)` |
| `kvExists()` | Already a coroutine. Change `reader->contains(key)` to `co_await reader->contains(key)` |
| `kvPrefixScan()` | Already runs inside `seastar::async()`. The sync `.get()` path via `nextSync()` continues to work. For optimal performance, migrate to async loop with `co_await`. |

#### 1.8 Update Compaction

Compaction's merge loop (`compaction.cpp`) currently uses `nextSync()`. Two options:
- **Quick fix:** Keep `nextSync()` which calls `.get()` — safe because compaction already runs in `seastar::async()` or with ready futures from the block cache.
- **Full fix:** Migrate the merge loop to use `co_await next()` on each source. This requires making `compactFiles()` a full coroutine instead of using `seastar::async()`.

Recommended: Quick fix first (keep sync compatibility), full fix in Phase 2.

---

### Phase 2: Read-Ahead for Sequential Scans

**Goal:** Pipeline I/O with computation for compaction and prefix scans.

Adopt ScyllaDB's pattern: when loading block N, pre-issue the DMA read for block N+1.

```cpp
seastar::future<> Iterator::nextBlock() {
    ++blockIndex_;
    if (blockIndex_ >= reader_->blockCount()) {
        valid_ = false;
        co_return;
    }

    // Use prefetched block if available
    if (prefetchFuture_) {
        blockData_ = co_await std::move(*prefetchFuture_);
        prefetchFuture_.reset();
    } else {
        blockData_ = co_await reader_->decompressBlock(blockIndex_);
    }

    // Start prefetching the next block
    if (blockIndex_ + 1 < reader_->blockCount()) {
        prefetchFuture_.emplace(reader_->decompressBlock(blockIndex_ + 1));
    }

    blockReader_ = std::make_unique<BlockReader>(blockData_);
    blockIter_ = blockReader_->begin();
    updateFromBlockIter();
}
```

This mirrors ScyllaDB's `read_ahead = 4` pattern but simplified to 1-block lookahead (sufficient for our 16KB blocks).

For deeper prefetching (matching ScyllaDB's 4-buffer read-ahead), use `make_file_input_stream()` with `read_ahead` option instead of individual `dma_read()` calls for sequential iteration.

---

### Phase 3: Reader Concurrency Semaphore

**Goal:** Prevent OOM from concurrent cache-miss block reads.

```cpp
// In NativeIndex (per-shard):
seastar::semaphore blockReadSemaphore_{16};  // max 16 concurrent block reads

// In SSTableReader::decompressBlock():
seastar::future<std::string> SSTableReader::decompressBlock(size_t blockIndex) const {
    auto units = co_await nativeIndex_->blockReadSemaphore_.wait(1);
    auto buf = co_await readFile_.dma_read<uint8_t>(entry.offset, entry.size);
    // ... CRC + decompress (sync, under semaphore) ...
    co_return result;
    // units released here
}
```

This matches ScyllaDB's `reader_concurrency_semaphore` pattern, adapted to our simpler model. The semaphore pointer could be passed through at open time or stored in a shared config.

---

### Phase 4: Eliminate Metadata Load Blocking (Optional)

Currently, `SSTableReader::open()` reads the entire metadata section (footer + index + bloom) using `seastar::async()` with `read_exactly()`. This is already non-blocking (runs in a Seastar thread). However, for consistency, the metadata load could be migrated to use coroutines directly:

```cpp
// Current (already non-blocking via seastar::async):
auto buf = co_await seastar::async([&] {
    // ... read metadata synchronously ...
});

// Could become (fully coroutine-native):
auto buf = co_await readFile_.dma_read<uint8_t>(metaOffset, metaSize);
// Parse footer, index, bloom from buf
```

This is low priority since the current approach is already non-blocking.

---

## Files to Modify

| File | Phase | Changes |
|------|-------|---------|
| `lib/index/native/sstable.hpp` | 1 | Replace `readFd_` with `seastar::file`; make `get`/`contains`/`decompressBlock`/`getDecompressedBlock` return futures; add async Iterator methods |
| `lib/index/native/sstable.cpp` | 1 | Replace `::pread()` with `co_await dma_read()`; make functions coroutines; update `open()`/`close()` for `seastar::file` |
| `lib/index/native/native_index.cpp` | 1 | Update `kvGet`/`kvExists` to `co_await`; update `kvPrefixScan` if migrating to async |
| `lib/index/native/compaction.cpp` | 1-2 | Update `SSTableIteratorSource` for async; optionally migrate merge loop to coroutine |
| `lib/index/native/merge_iterator.hpp` | 1 | Already has async interface; may need minor adjustments |
| `test/unit/index/sstable_test.cpp` | 1 | Update tests for async API (wrap in `seastar::async` or use coroutines) |

## Estimated Impact

| Metric | Before | After Phase 1 | After Phase 2 |
|--------|--------|---------------|---------------|
| Reactor stalls on cache miss | 1-10ms | 0 (async I/O) | 0 |
| Sequential scan throughput | Limited by single I/O | Same | ~2-4x (prefetch pipeline) |
| Concurrent reads | Serialized (blocks reactor) | Fully concurrent | Concurrent + bounded |
| API surface change | Sync methods | Async coroutines | Same |
| Memory per SSTable | Same | Same + ~64B for seastar::file | Same |

## Key ScyllaDB Lessons Applied

1. **DMA everything** — No POSIX I/O in the reactor. Use `open_file_dma()` + `dma_read()`.
2. **Cache makes the common path free** — Our block cache means cache hits are instant. Async only affects the cold/miss path.
3. **Seek = close + reopen** — For random access within a sequential stream abstraction, create a new stream at each position (ScyllaDB's `file_random_access_reader` pattern).
4. **Separate I/O histories** — Different access patterns (point lookup vs scan) should have independent read-ahead tracking.
5. **Semaphore for admission** — Bound concurrent cache-miss reads to prevent memory spikes.
6. **Write path is already correct** — Our `SSTableWriter` already uses `seastar::file` with DMA. Only the reader needs migration.
