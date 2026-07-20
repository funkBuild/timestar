#include "native_index.hpp"

#include "../key_encoding.hpp"
#include "series_key.hpp"           // for buildSeriesKey
#include "tsm.hpp"                  // for TSMValueType definition
#include "value_type_dispatch.hpp"  // for valueTypeName

#include <glaze/json.hpp>

#include <endian.h>
#include <xxhash.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <format>
#include <seastar/core/coroutine.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/log.hh>

// Use short namespace alias for key encoding
namespace ke = timestar::index::keys;

// Tombstone sentinel: a single null byte distinguishes SSTable tombstones from
// legitimate empty-value marker entries (e.g. MEASUREMENT_SERIES).
static constexpr std::string_view SSTABLE_TOMBSTONE{"\0", 1};

static bool isSstableTombstone(std::string_view value) {
    return value.size() == 1 && value[0] == '\0';
}

static seastar::logger native_index_log("timestar.native_index");

namespace timestar::index {

// Computes the exclusive upper bound for all keys starting with `prefix`:
// the prefix with its last byte incremented (trailing 0xFF bytes are dropped
// first). Returns an empty string when the bound is unbounded (empty or
// all-0xFF prefix). Computed once per scan and reused across all SSTables.
static std::string nextPrefixBound(std::string_view prefix) {
    std::string next(prefix);
    while (!next.empty() && static_cast<unsigned char>(next.back()) == 0xFF) {
        next.pop_back();
    }
    if (!next.empty()) {
        next.back() = static_cast<char>(static_cast<unsigned char>(next.back()) + 1);
    }
    return next;
}

// Returns true if an SSTable whose keys span [minKey, maxKey] may contain a
// key starting with `prefix`. Used to skip non-intersecting SSTables in
// prefix scans. maxKey is populated by SSTableReader::open(); an empty maxKey
// means the range is unknown and the file cannot be pruned.
// nextPrefix is the precomputed nextPrefixBound(prefix) (empty = unbounded).
static bool rangeMayContainPrefix(const SSTableMetadata& meta, std::string_view prefix, std::string_view nextPrefix) {
    if (prefix.empty() || meta.maxKey.empty()) {
        return true;
    }
    // The smallest possible key with this prefix is the prefix itself:
    // if even that is past maxKey, nothing in the file can match.
    if (prefix > meta.maxKey) {
        return false;
    }
    if (nextPrefix.empty()) {
        return true;
    }
    return nextPrefix > meta.minKey;  // skip when nextPrefix <= minKey (file entirely above range)
}

// ============================================================================
// Iterator adapters for MergeIterator (used by kvPrefixScan)
// ============================================================================

// Wraps MemTable::Iterator (synchronous) as IteratorSource.
// Owns the MemTable via shared_ptr: scans suspend on SSTable block reads, and
// the background flush destroys immutableMemtable_ when it completes — without
// ownership the map (and this iterator) would dangle mid-scan.
class MemTableIteratorSource : public IteratorSource {
public:
    explicit MemTableIteratorSource(std::shared_ptr<const MemTable> mt, int priority)
        : owner_(std::move(mt)), iter_(owner_->newIterator()), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override {
        iter_.seek(target);
        return seastar::make_ready_future<>();
    }
    seastar::future<> seekToFirst() override {
        iter_.seekToFirst();
        return seastar::make_ready_future<>();
    }
    seastar::future<> next() override {
        iter_.next();
        return seastar::make_ready_future<>();
    }

    bool valid() const override { return iter_.valid(); }
    std::string_view key() const override { return iter_.key(); }
    std::string_view value() const override { return iter_.value(); }
    bool isTombstone() const override { return iter_.isTombstone(); }
    int priority() const override { return priority_; }

private:
    std::shared_ptr<const MemTable> owner_;
    MemTable::Iterator iter_;
    int priority_;
};

// Non-owning SSTable iterator source — borrows reader, doesn't own it.
// (Unlike compaction.cpp's SSTableIteratorSource which takes ownership.)
class SSTableBorrowedIteratorSource : public IteratorSource {
public:
    SSTableBorrowedIteratorSource(SSTableReader* reader, int priority)
        : iter_(reader->newIterator()), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override { co_await iter_->seek(target); }
    seastar::future<> seekToFirst() override { co_await iter_->seekToFirst(); }
    seastar::future<> next() override { co_await iter_->next(); }

    bool valid() const override { return iter_->valid(); }
    std::string_view key() const override { return iter_->key(); }
    std::string_view value() const override { return iter_->value(); }
    bool isTombstone() const override { return isSstableTombstone(value()); }
    int priority() const override { return priority_; }

private:
    std::unique_ptr<SSTableReader::Iterator> iter_;
    int priority_;
};

// ============================================================================
// Constructor / Destructor
// ============================================================================

NativeIndex::NativeIndex(int shardId)
    : shardId_(shardId),
      blockCache_(timestar::config().index.block_cache_bytes / std::max(1u, seastar::smp::count)),
      seriesMetadataCache_(timestar::config().index.metadata_cache_bytes / std::max(1u, seastar::smp::count)),
      discoveryCache_(timestar::config().index.discovery_cache_bytes / std::max(1u, seastar::smp::count)) {}

NativeIndex::~NativeIndex() {
    // Stop the periodic WAL sync from firing after destruction (close() also
    // cancels it; this covers destruction-without-close paths).
    walSyncTimer_.cancel();

    // Guard against destroying the index while a background flush is still in flight.
    // The caller must co_await close() (which calls waitForFlush()) before destruction.
    // This check must fire in release builds too — the background coroutine holds a raw
    // `this` pointer that would dangle if we proceed with destruction.
    if (flushFuture_.has_value() && !flushFuture_->available()) {
        ::native_index_log.error("NativeIndex destroyed with in-flight flush on shard {} — aborting to prevent UB",
                                 shardId_);
        std::abort();
    }
}

// ============================================================================
// Lifecycle
// ============================================================================

seastar::future<> NativeIndex::open() {
    // Fail loudly at startup if the configured memory budgets cannot fit this
    // shard's arena. Previously the individual caches were fixed absolutes that
    // summed to ~690 MB per shard regardless of how much memory the shard
    // actually had, so an under-provisioned deployment did not find out until a
    // std::bad_alloc hours later, from whichever allocation happened to be
    // unlucky. A wrong number should be a config error, not a mystery crash.
    {
        const size_t shardTotal = seastar::memory::stats().total_memory();
        const size_t indexBudget = indexCacheBudgetBytes();
        const size_t memtables = timestar::config().index.write_buffer_size * 2;  // active + immutable
        // NOTE: storage.compaction.max_memory is deliberately NOT counted here.
        // It is currently dead config -- nothing reads it -- so counting it would
        // overstate what is actually committed. Compaction's real footprint is
        // bounded by its streaming writer, not by that setting.
        const size_t committed = indexBudget + memtables;

        if (shardTotal > 0 && committed > shardTotal / 2) {
            ::native_index_log.warn(
                "Shard {} memory budgets are large relative to its arena: index caches {} MB + memtables {} MB = "
                "{} MB of {} MB ({}%). Reduce index.write_buffer_size, or give the shard more memory.",
                shardId_, indexBudget >> 20, memtables >> 20, committed >> 20, shardTotal >> 20,
                committed * 100 / shardTotal);
        }
    }

    // Note: std::filesystem::absolute() depends on the process CWD at call time.
    // This is fine because open() is called during startup before any CWD change.
    indexPath_ = std::filesystem::absolute(timestar::shardDataPath(shardId_) + "/native_index").string();
    co_await seastar::async([this] { std::filesystem::create_directories(indexPath_); });

    // Open manifest
    manifest_ = std::make_unique<Manifest>(co_await Manifest::open(indexPath_));

    // Open WAL and replay into MemTable
    memtable_ = std::make_shared<MemTable>();
    wal_ = std::make_unique<IndexWAL>(co_await IndexWAL::open(indexPath_ + "/wal"));
    auto replayed = co_await wal_->replay(*memtable_);
    if (replayed > 0) {
        ::native_index_log.info("Replayed {} WAL records into MemTable", replayed);
    }

    // Open SSTable readers
    co_await refreshSSTables();

    // Configure per-shard concurrency limiter for SSTable block reads
    SSTableReader::setBlockReadSemaphore(&blockReadSemaphore_);

    // Create compaction engine
    CompactionConfig compCfg;
    compCfg.blockSize = timestar::config().index.block_size;
    compCfg.bloomBitsPerKey = timestar::config().index.bloom_filter_bits;
    compCfg.rateLimitMBps = timestar::config().index.compaction_rate_limit_mbps;
    compaction_ = std::make_unique<CompactionEngine>(indexPath_, *manifest_, compCfg);

    // Durability: replayed WAL records exist only in the volatile memtable at
    // this point. Make them durable in an SSTable BEFORE the current WAL can
    // be truncated (first append) or the older generations deleted — a crash
    // after either would otherwise lose everything just replayed.
    // flushMemTable() also rotates to a fresh WAL generation and deletes the
    // consumed one once the SSTable is durable.
    if (!memtable_->empty()) {
        co_await flushMemTable();
        ::native_index_log.info("Recovered MemTable flushed to SSTable before WAL reuse");
    }
    co_await wal_->purgeReplayedFiles();

    // Bound the WAL buffering loss window: append() only buffers in user space,
    // so periodically sync (flush + fsync) when there is unpersisted data.
    walSyncTimer_.set_callback([this] {
        if (walSyncGate_.is_closed()) {
            return;
        }
        (void)seastar::with_gate(walSyncGate_, [this] {
            return wal_->sync();
        }).handle_exception([](std::exception_ptr ep) {
            ::native_index_log.warn("Background WAL sync failed: {}", ep);
        });
    });
    walSyncTimer_.arm_periodic(kWalSyncInterval);

    // Phase 2: Load or migrate LocalIdMap for roaring bitmap postings
    auto counterKey = ke::encodeLocalIdCounterKey();
    auto counterVal = co_await kvGet(counterKey);
    if (counterVal.has_value()) {
        // Existing local ID mappings — stream directly into LocalIdMap (no intermediate vector)
        uint32_t nextId = ke::decodeLocalId(*counterVal);
        localIdMap_.restoreBegin(nextId, nextId);
        std::string fwdPrefix(1, static_cast<char>(LOCAL_ID_FORWARD));
        co_await kvPrefixScan(fwdPrefix, [&](std::string_view key, std::string_view value) {
            if (key.size() >= 5 && value.size() >= 16) {
                uint32_t localId = ke::decodeLocalId(key.substr(1));
                SeriesId128 globalId = SeriesId128::fromBytes(value.data(), 16);
                localIdMap_.restoreEntry(localId, globalId);
            }
            return true;
        });
        lastFlushedLocalId_ = localIdMap_.nextId();  // All restored IDs are already persisted
        ::native_index_log.info("Restored LocalIdMap: {} mappings", localIdMap_.size());
    } else {
        // Phase 1 → Phase 2 migration: build LocalIdMap + bitmaps from existing data
        IndexWriteBatch migrationBatch;
        co_await migrateToLocalIds(migrationBatch);
        if (!migrationBatch.empty()) {
            co_await kvWriteBatch(migrationBatch);
            lastFlushedLocalId_ = localIdMap_.nextId();  // Migration persisted all IDs
            ::native_index_log.info("Phase 2 migration complete: {} local IDs assigned", localIdMap_.size());
        }
    }

    // Crash-window postings repair: series created after the last bitmap flush
    // have durable metadata + local IDs (persisted in their creation batch) but
    // their postings-bitmap membership only lived in RAM. Re-add them from
    // metadata so tag-filtered queries see them again. Day bitmaps cannot be
    // reconstructed here (insert timestamps are unknown) — future inserts
    // re-record days; the time-scoped gap is bounded to the crash window.
    {
        auto wmVal = co_await kvGet(ke::encodePostingsWatermarkKey());
        // A missing watermark does NOT mean "nothing to repair": the watermark
        // is only written by flushDirtyBitmaps(), while local IDs are persisted
        // with each series-creation batch. A crash before the first bitmap
        // flush/clean close leaves assigned IDs with no watermark — the crash
        // window is then every assigned local ID, so default to 0. (Bitmap
        // adds are idempotent, so over-repairing is safe.)
        uint32_t watermark = wmVal.has_value() ? ke::decodeLocalId(*wmVal) : 0;
        uint32_t repaired = 0;
        std::string bitmapCacheKey;
        for (uint32_t id = watermark; id < localIdMap_.nextId(); ++id) {
            if (!localIdMap_.isValid(id))
                continue;
            SeriesId128 globalId = localIdMap_.getGlobalId(id);
            auto meta = co_await getSeriesMetadata(globalId);
            if (!meta.has_value())
                continue;
            for (const auto& [tagKey, tagValue] : meta->tags) {
                buildBitmapCacheKey(bitmapCacheKey, meta->measurement, tagKey, tagValue);
                auto* bitmap = co_await getOrLoadBitmapForInsert(bitmapCacheKey);
                bitmap->add(id);
            }
            dirtyMeasurementBlooms_.insert(meta->measurement);
            ++repaired;
        }
        if (repaired > 0) {
            ::native_index_log.info("Repaired postings for {} series in crash window [{}, {})", repaired, watermark,
                                    localIdMap_.nextId());
        }
    }

    // HLL sketches and measurement bloom filters are loaded lazily on first access
    // via getOrCreateSeriesId() / estimateMeasurementCardinality() / getPostingsBitmapByKey().
    // This avoids scanning all HLL/bloom KV entries at startup, which can stall for
    // 10K+ measurements.

    ::native_index_log.info("NativeIndex opened at: {} ({} SSTables, {} MemTable entries, {} local IDs)", indexPath_,
                            manifest_->files().size(), memtable_->size(), localIdMap_.size());
}

seastar::future<> NativeIndex::close() {
    // Stop the periodic WAL sync and drain any in-flight sync before touching
    // the WAL below (wal_->close() flushes everything anyway).
    walSyncTimer_.cancel();
    if (!walSyncGate_.is_closed()) {
        co_await walSyncGate_.close();
    }

    // Wait for any in-flight background flush, then flush remaining data
    try {
        co_await waitForFlush();
        if (memtable_ && !memtable_->empty()) {
            co_await flushMemTable();
        }
    } catch (const std::exception& e) {
        ::native_index_log.warn("Failed to flush MemTable on close: {} — data preserved in WAL", e.what());
    }

    // Close all SSTable readers (Step 4: map-keyed)
    for (auto& [fileNum, reader] : sstableReaders_) {
        co_await reader->close();
    }
    sstableReaders_.clear();

    // Force-close any deferred readers (no scans can be in flight at shutdown)
    co_await drainPendingCloseReaders(true);

    if (wal_)
        co_await wal_->close();
    if (manifest_) {
        co_await manifest_->writeSnapshot();
        co_await manifest_->close();
    }

    // Clear the per-shard semaphore pointer so stale references don't dangle
    // after this NativeIndex (which owns the semaphore) is destroyed.
    SSTableReader::setBlockReadSemaphore(nullptr);

    ::native_index_log.info("NativeIndex closed on shard {}", shardId_);
}

// ============================================================================
// Low-level KV operations
// ============================================================================

std::string NativeIndex::sstFilename(uint64_t fileNumber) {
    return std::format("{}/idx_{:06}.sst", indexPath_, fileNumber);
}

// Step 4: Incremental SSTable refresh — only opens new files and closes removed ones.
// Existing readers (with warm block caches) are preserved.
seastar::future<> NativeIndex::refreshSSTables() {
    // Snapshot the manifest file list: the open loop below suspends on DMA
    // I/O, and a concurrent manifest mutation could reallocate files_ under a
    // live range-for reference.
    auto manifestFileList = manifest_->files();

    // Build set of file numbers currently in the manifest
    std::set<uint64_t> manifestFiles;
    for (const auto& fileMeta : manifestFileList) {
        manifestFiles.insert(fileMeta.fileNumber);
    }

    // Remove readers for files no longer in the manifest. Do NOT close them
    // eagerly: in-flight scans snapshot the shared_ptr, which protects the
    // object but not the fd — closing here would break a suspended scan's next
    // block read. Defer the close until the last external reference is gone.
    for (auto it = sstableReaders_.begin(); it != sstableReaders_.end();) {
        if (manifestFiles.find(it->first) == manifestFiles.end()) {
            pendingCloseReaders_.push_back(std::move(it->second));
            it = sstableReaders_.erase(it);
        } else {
            ++it;
        }
    }

    // Open readers for new files not yet in the reader map
    for (const auto& fileMeta : manifestFileList) {
        if (sstableReaders_.find(fileMeta.fileNumber) != sstableReaders_.end()) {
            continue;  // Already open
        }
        auto path = sstFilename(fileMeta.fileNumber);
        if (co_await seastar::file_exists(path)) {
            // SSTableReader::open returns unique_ptr; convert to shared_ptr for lifetime safety
            auto reader = co_await SSTableReader::open(path, &blockCache_);
            sstableReaders_[fileMeta.fileNumber] = std::shared_ptr<SSTableReader>(std::move(reader));
        } else {
            // Crash edge: the manifest references a file that is missing on
            // disk. Skip it so startup can proceed, but say so LOUDLY — any
            // keys whose newest version lived only in this file are lost.
            ::native_index_log.error(
                "Manifest lists SSTable file {} ({}) but it does not exist on disk — skipping "
                "(possible data loss on shard {})",
                fileMeta.fileNumber, path, shardId_);
        }
    }

    // Close any deferred readers whose last scan has finished.
    co_await drainPendingCloseReaders(false);
}

// Close deferred readers once no in-flight scan holds them (use_count()==1
// means pendingCloseReaders_ holds the only reference). With force=true (used
// by close()) all remaining readers are closed unconditionally.
seastar::future<> NativeIndex::drainPendingCloseReaders(bool force) {
    std::vector<std::shared_ptr<SSTableReader>> toClose;
    for (auto it = pendingCloseReaders_.begin(); it != pendingCloseReaders_.end();) {
        if (force || it->use_count() == 1) {
            toClose.push_back(std::move(*it));
            it = pendingCloseReaders_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto& reader : toClose) {
        co_await reader->close();
    }
}

// Checks MemTable (sync) then SSTables (async DMA on cache miss).
seastar::future<std::optional<std::string>> NativeIndex::kvGet(std::string_view key) {
    // 1. Check active MemTable first (newest) — single-traversal probe
    std::string value;
    auto probe = memtable_->probe(key, &value);
    if (probe == MemTable::ProbeResult::Live) {
        co_return std::move(value);
    }
    if (probe == MemTable::ProbeResult::Tombstone) {
        co_return std::nullopt;
    }

    // 2. Check immutable MemTable (being flushed)
    if (immutableMemtable_) {
        probe = immutableMemtable_->probe(key, &value);
        if (probe == MemTable::ProbeResult::Live) {
            co_return std::move(value);
        }
        if (probe == MemTable::ProbeResult::Tombstone) {
            co_return std::nullopt;
        }
    }

    // 3. Check SSTables (newest to oldest = reverse map order by file number)
    // Snapshot readers into a local vector to prevent iterator invalidation
    // if refreshSSTables() modifies sstableReaders_ during co_await.
    {
        std::vector<std::shared_ptr<SSTableReader>> readers;
        readers.reserve(sstableReaders_.size());
        for (auto it = sstableReaders_.rbegin(); it != sstableReaders_.rend(); ++it) {
            readers.push_back(it->second);
        }
        // Hash the key once for all bloom probes across the reader loop
        const uint64_t keyHash = XXH3_64bits(key.data(), key.size());
        for (auto& reader : readers) {
            auto result = co_await reader->get(key, keyHash);
            if (result.has_value()) {
                if (isSstableTombstone(*result))
                    co_return std::nullopt;
                co_return result;
            }
        }
    }

    co_return std::nullopt;
}

// Step 8: Existence check without copying the value.
seastar::future<bool> NativeIndex::kvExists(std::string_view key) {
    // 1. Check active MemTable first (newest) — single-traversal probe
    auto probe = memtable_->probe(key);
    if (probe == MemTable::ProbeResult::Live)
        co_return true;
    if (probe == MemTable::ProbeResult::Tombstone)
        co_return false;

    // 2. Check immutable MemTable (being flushed)
    if (immutableMemtable_) {
        probe = immutableMemtable_->probe(key);
        if (probe == MemTable::ProbeResult::Live)
            co_return true;
        if (probe == MemTable::ProbeResult::Tombstone)
            co_return false;
    }

    // 3. Check SSTables (newest to oldest).
    // Snapshot readers to prevent iterator invalidation across co_await.
    {
        std::vector<std::shared_ptr<SSTableReader>> readers;
        readers.reserve(sstableReaders_.size());
        for (auto it = sstableReaders_.rbegin(); it != sstableReaders_.rend(); ++it) {
            readers.push_back(it->second);
        }
        // Hash the key once for all bloom probes across the reader loop
        const uint64_t keyHash = XXH3_64bits(key.data(), key.size());
        for (auto& reader : readers) {
            // get() returns nullopt on bloom-filter rejection, so the
            // fast-path is preserved.
            auto result = co_await reader->get(key, keyHash);
            if (result.has_value())
                co_return !isSstableTombstone(*result);
        }
    }

    co_return false;
}

// Write ordering: apply to the memtable BEFORE the WAL append. append() can
// suspend (threshold flush / write semaphore), and a concurrent memtable swap
// during that suspension would otherwise pair the WAL record with the OLD
// generation while the memtable apply landed in the NEW memtable — the old
// WAL is deleted after flushing the old memtable, silently orphaning the
// record. Applying first guarantees the record is covered by whichever
// memtable's flush runs; the WAL copy is then redundant-but-idempotent on
// replay. Callers are acked only after the append returns, so the earlier
// read visibility is not an early ack.
seastar::future<> NativeIndex::kvPut(const std::string& key, const std::string& value) {
    IndexWriteBatch batch;
    batch.put(key, value);
    memtable_->put(key, value);
    co_await wal_->append(batch);
    co_await maybeFlushMemTable();
}

seastar::future<> NativeIndex::kvDelete(const std::string& key) {
    IndexWriteBatch batch;
    batch.remove(key);
    memtable_->remove(key);
    co_await wal_->append(batch);
    co_await maybeFlushMemTable();
}

seastar::future<> NativeIndex::kvWriteBatch(const IndexWriteBatch& batch) {
    batch.applyTo(*memtable_);
    co_await wal_->append(batch);
    co_await maybeFlushMemTable();
}

// Async streaming kvPrefixScan using MergeIterator.
// SSTable block reads may require DMA I/O on cache miss.
seastar::future<> NativeIndex::kvPrefixScan(const std::string& prefix, ScanCallback fn) {
    // Count valid sources and track if we can use the single-source fast path
    bool memtableEmpty = memtable_->empty();
    bool immutableEmpty = !immutableMemtable_ || immutableMemtable_->empty();
    size_t sstCount = sstableReaders_.size();

    // Exclusive upper bound for the prefix — computed once for all SSTables.
    const std::string nextPrefix = nextPrefixBound(prefix);

    // Fast path: single SSTable, empty memtables — skip MergeIterator entirely.
    // This is the common case after compaction.
    if (memtableEmpty && immutableEmpty && sstCount == 1) {
        auto reader = sstableReaders_.begin()->second;  // shared_ptr copy
        if (!rangeMayContainPrefix(reader->metadata(), prefix, nextPrefix)) {
            co_return;  // key range cannot intersect the prefix
        }
        auto iter = reader->newIterator();
        co_await iter->seek(prefix);
        while (iter->valid()) {
            auto key = iter->key();
            if (key.size() < prefix.size() || std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
                break;
            }
            // Skip tombstones in single-source scan
            auto val = iter->value();
            if (!isSstableTombstone(val)) {
                if (!fn(key, val))
                    break;
            }
            co_await iter->next();
        }
        co_return;
    }

    // Fast path: single MemTable, no SSTables — skip MergeIterator entirely.
    if (!memtableEmpty && immutableEmpty && sstCount == 0) {
        auto iter = memtable_->newIterator();
        iter.seek(prefix);
        while (iter.valid()) {
            auto key = iter.key();
            if (key.size() < prefix.size() || std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
                break;
            }
            // Skip tombstones in single-source scan
            if (!iter.isTombstone()) {
                if (!fn(key, iter.value()))
                    break;
            }
            iter.next();
        }
        co_return;
    }

    // General path: multiple sources — use MergeIterator.
    // Sources take shared ownership of the memtables: the background flush may
    // reset immutableMemtable_ (and a concurrent swap may replace memtable_)
    // while this scan is suspended in an SSTable block read.
    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableIteratorSource>(memtable_, 0));

    int nextPriority = 1;
    if (immutableMemtable_) {
        sources.push_back(std::make_unique<MemTableIteratorSource>(immutableMemtable_, nextPriority));
        ++nextPriority;
    }

    // Step 4: Iterate map in ascending file number order (oldest first → highest priority).
    // Snapshot shared_ptrs to keep readers alive for the entire scan duration,
    // preventing use-after-free if refreshSSTables() runs during co_await.
    std::vector<std::shared_ptr<SSTableReader>> readerSnapshot;
    readerSnapshot.reserve(sstableReaders_.size());
    int sstPriority = nextPriority + static_cast<int>(sstableReaders_.size());
    for (auto& [fileNum, reader] : sstableReaders_) {
        // Range pruning: skip SSTables whose [minKey, maxKey] cannot intersect
        // the prefix — avoids a block read + heap entry per excluded file.
        if (!rangeMayContainPrefix(reader->metadata(), prefix, nextPrefix)) {
            --sstPriority;
            continue;
        }
        readerSnapshot.push_back(reader);
        sources.push_back(std::make_unique<SSTableBorrowedIteratorSource>(reader.get(), sstPriority));
        --sstPriority;
    }

    MergeIterator merger(std::move(sources));
    co_await merger.seek(prefix);

    while (merger.valid()) {
        auto key = merger.key();
        if (key.size() < prefix.size() || std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
            break;
        }
        if (!fn(key, merger.value()))
            break;
        co_await merger.next();
    }
}

seastar::future<> NativeIndex::waitForFlush() {
    // Reentrant: any number of coroutines may wait on the same in-flight
    // flush (shared_future is multi-consumer). Loop because a new flush may
    // have been scheduled while we were suspended.
    while (flushFuture_) {
        auto fut = flushFuture_->get_future();
        co_await std::move(fut);
        if (flushFuture_ && flushFuture_->available()) {
            flushFuture_.reset();
        }
    }
}

seastar::future<> NativeIndex::maybeFlushMemTable() {
    auto threshold = timestar::config().index.write_buffer_size;
    if (memtable_->approximateMemoryUsage() < threshold)
        co_return;

    // Serialize the entire check→swap→rotate→schedule region: without this,
    // two coroutines crossing the threshold both swapped memtable_ into
    // immutableMemtable_ — the second swap destroyed the first's unflushed
    // memtable and its WAL was deleted after flushing the wrong data.
    auto flushUnits = co_await seastar::get_units(flushMutex_, 1);

    // Re-check after acquiring — another coroutine may have flushed already.
    auto usage = memtable_->approximateMemoryUsage();
    if (usage < threshold)
        co_return;

    ::native_index_log.info("Flushing MemTable: {} bytes >= {} threshold", usage, threshold);

    // Phase 2+3+4: Flush dirty bitmaps, day bitmaps, HLLs, and blooms before memtable swap
    if (!bitmapCache_.empty() || !dayBitmapCache_.empty() || !hllCacheDirty_.empty() ||
        !dirtyMeasurementBlooms_.empty()) {
        IndexWriteBatch postingsBatch;
        flushDirtyBitmaps(postingsBatch);
        flushDirtyDayBitmaps(postingsBatch);
        flushDirtyHLLs(postingsBatch);
        co_await flushDirtyMeasurementBlooms(postingsBatch);
        if (!postingsBatch.empty()) {
            co_await wal_->append(postingsBatch);
            postingsBatch.applyTo(*memtable_);
        }
        // Evict non-dirty cache entries to bound memory growth
        trimBitmapCache();
        trimDayBitmapCache();
        trimHllCache();        // Step 7
        trimTagValuesCache();  // Step 6
        trimSchemaCaches();
        trimMeasurementBloomCache();
    }

    // If a previous flush is still in progress, wait for it
    co_await waitForFlush();

    // Swap: active memtable becomes immutable, create fresh active
    immutableMemtable_ = std::move(memtable_);
    memtable_ = std::make_shared<MemTable>();

    // Rotate WAL so new writes go to a fresh log
    auto oldWalPath = co_await wal_->rotate();

    // Schedule the flush asynchronously — writer is NOT blocked.
    // Use a lambda coroutine that owns the oldWalPath by value.
    flushFuture_.emplace([](NativeIndex* self, std::string walPath) -> seastar::future<> {
        co_await self->doFlushImmutableMemTable();
        co_await IndexWAL::deleteFile(walPath);
        self->immutableMemtable_.reset();
        co_await self->compaction_->maybeCompact();
        co_await self->refreshSSTables();
    }(this, std::move(oldWalPath)));
}

// Blocking flush — used by close() and compact() where we need synchronous completion.
seastar::future<> NativeIndex::flushMemTable() {
    // Same serialization as maybeFlushMemTable — the swap/rotate region must
    // never run concurrently with another flush.
    auto flushUnits = co_await seastar::get_units(flushMutex_, 1);

    // Phase 2+3+4: Flush dirty bitmaps, day bitmaps, HLLs, and blooms before checking empty
    if (!bitmapCache_.empty() || !dayBitmapCache_.empty() || !hllCacheDirty_.empty() ||
        !dirtyMeasurementBlooms_.empty()) {
        IndexWriteBatch postingsBatch;
        flushDirtyBitmaps(postingsBatch);
        flushDirtyDayBitmaps(postingsBatch);
        flushDirtyHLLs(postingsBatch);
        co_await flushDirtyMeasurementBlooms(postingsBatch);
        if (!postingsBatch.empty()) {
            co_await wal_->append(postingsBatch);
            postingsBatch.applyTo(*memtable_);
        }
        trimBitmapCache();
        trimDayBitmapCache();
        trimHllCache();        // Step 7
        trimTagValuesCache();  // Step 6
        trimSchemaCaches();
        trimMeasurementBloomCache();
    }

    if (memtable_->empty()) {
        co_await waitForFlush();
        co_return;
    }

    // Wait for any in-flight flush first
    co_await waitForFlush();

    // Swap to immutable
    immutableMemtable_ = std::move(memtable_);
    memtable_ = std::make_shared<MemTable>();

    auto oldWalPath = co_await wal_->rotate();

    // Flush synchronously (blocking)
    co_await doFlushImmutableMemTable();
    co_await IndexWAL::deleteFile(oldWalPath);
    immutableMemtable_.reset();

    co_await compaction_->maybeCompact();
    co_await refreshSSTables();
}

// Writes the immutable memtable to an SSTable. Does NOT touch active memtable.
seastar::future<> NativeIndex::doFlushImmutableMemTable() {
    if (!immutableMemtable_ || immutableMemtable_->empty())
        co_return;

    ::native_index_log.info("Flushing immutable MemTable: {} entries, {} approx bytes", immutableMemtable_->size(),
                            immutableMemtable_->approximateMemoryUsage());

    // Write immutable MemTable to a new SSTable. (No separate counting pass —
    // it walked the entire memtable synchronously just for a log line, and
    // its empty-guard was unreachable behind the empty() check above.)
    uint64_t fileNum = manifest_->nextFileNumber();
    auto path = sstFilename(fileNum);
    auto writer = co_await SSTableWriter::create(path, timestar::config().index.block_size,
                                                 timestar::config().index.bloom_filter_bits);

    auto it = immutableMemtable_->newIterator();
    it.seekToFirst();
    size_t addCount = 0;
    while (it.valid()) {
        // Tombstones are written with a sentinel value to suppress older
        // values in lower-level SSTables until compaction GC.
        if (it.isTombstone()) {
            writer.add(it.key(), SSTABLE_TOMBSTONE);
        } else {
            writer.add(it.key(), it.value());
        }
        if (++addCount % 1024 == 0) {
            co_await writer.flushPending();
        }
        it.next();
    }
    co_await writer.flushPending();

    ::native_index_log.info("SSTable write starting for file {}", fileNum);
    auto meta = co_await writer.finish();
    ::native_index_log.info("SSTable write complete: {} entries, {} bytes", meta.entryCount, meta.fileSize);
    meta.fileNumber = fileNum;
    meta.level = 0;
    co_await manifest_->addFile(meta);

    // Refresh SSTable readers so queries see the new file
    co_await refreshSSTables();
}

// ============================================================================
// Series cache (two-generation design)
// ============================================================================

bool NativeIndex::seriesCacheContains(const SeriesId128& id) const {
    return indexedSeriesCache_.count(id) || indexedSeriesCacheRetired_.count(id);
}

void NativeIndex::seriesCacheInsert(const SeriesId128& id) {
    indexedSeriesCache_.insert(id);
    seriesCacheEvictIncremental();
}

void NativeIndex::seriesCacheEvictIncremental() {
    // Drain retired cache incrementally
    auto retiredIt = indexedSeriesCacheRetired_.begin();
    for (size_t i = 0; i < EVICTION_BATCH_SIZE && retiredIt != indexedSeriesCacheRetired_.end(); ++i) {
        retiredIt = indexedSeriesCacheRetired_.erase(retiredIt);
    }
    // Swap generations if active exceeds limit
    if (indexedSeriesCache_.size() > maxSeriesCacheSize_) {
        indexedSeriesCacheRetired_.clear();
        indexedSeriesCache_.swap(indexedSeriesCacheRetired_);
    }
}

// ============================================================================
// Series indexing
// ============================================================================

// Stage 4: Coalesced insert — all metadata (series, fields, tags) written
// in a single WAL batch instead of 2-6 separate writes.
seastar::future<SeriesId128> NativeIndex::getOrCreateSeriesId(std::string measurement,
                                                              std::map<std::string, std::string> tags,
                                                              std::string field) {
    // Compute SeriesId128 from the canonical series key string.
    // This produces the same ID used by the data plane (TSM, WAL, MemoryStore),
    // ensuring index metadata lookups match data lookups without recomputation.
    std::string seriesKey = timestar::buildSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    co_return co_await getOrCreateSeriesId(seriesId, measurement, tags, field);
}

// Fast overload: caller supplies the pre-computed series ID, so the common
// already-indexed path does zero string building / hashing / map copying.
seastar::future<SeriesId128> NativeIndex::getOrCreateSeriesId(SeriesId128 seriesId, const std::string& measurement,
                                                              const std::map<std::string, std::string>& tags,
                                                              const std::string& field) {
    // Fast path: already indexed (no string allocation needed)
    if (seriesCacheContains(seriesId)) {
        co_return seriesId;
    }

    // Step 8: Check if series exists in storage (existence check only — no value copy)
    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    if (co_await kvExists(metaKey)) {
        seriesCacheInsert(seriesId);
        co_return seriesId;
    }

    // New series — create ALL index entries in a single batch (Stage 4).
    // Previously this was split across getOrCreateSeriesId + a separate
    // fields/tags indexing pass = 2-6 separate WAL writes. Now it's exactly 1.
    IndexWriteBatch batch;
    batch.reserve(4 + tags.size());  // metadata + 2 indexes + local ID forward + per-tag bitmap

    // Series metadata
    SeriesMetadata metadata{measurement, tags, field};
    batch.put(std::move(metaKey), ke::encodeSeriesMetadata(metadata));

    // Measurement series index
    batch.put(ke::encodeMeasurementSeriesKey(measurement, seriesId), "");

    // Phase 2: Assign local ID and persist the mapping ATOMICALLY with the
    // series metadata. Previously the forward mapping lived only in RAM until
    // the next bitmap flush — after a crash, the kvExists fast path above saw
    // the metadata and returned without re-assigning, leaving the series
    // permanently invisible to tag-filtered and time-scoped queries.
    uint32_t localId = localIdMap_.getOrAssign(seriesId);
    batch.put(ke::encodeLocalIdForwardKey(localId), seriesId.toBytes());
    batch.put(ke::encodeLocalIdCounterKey(), ke::encodeLocalId(localId + 1));

    // Phase 2: Add local ID to dirty postings bitmaps (TAG_INDEX/GROUP_BY_INDEX removed in Phase 3)
    std::string bitmapCacheKey;
    bitmapCacheKey.reserve(measurement.size() + 1 + 32 + 1 + 32);
    // Phase 4: Update cardinality HLLs.
    //
    // The per-tag-value HLL is maintained ONLY above a cardinality threshold.
    // Each sketch is 16 KB (16384 registers), so one per distinct tag value is
    // ruinous for a high-cardinality tag: 6,000 distinct values measured ~64 KB
    // each once the sketch, its serialised copy and its KV entry are counted,
    // and the shard hit std::bad_alloc at ~6,100 values. The same workload with
    // 200,000 series but only 300 distinct tag values was fine -- the cost
    // tracks distinct VALUES, not series.
    //
    // It is also poor value below the threshold: the exact roaring bitmap for
    // the same (measurement, tagKey, tagValue) is maintained here anyway, and
    // getTagValueCardinality() already falls back to bitmap->cardinality(),
    // which is EXACT. A 16 KB sketch with 0.8% error to count something in the
    // tens is strictly worse than the answer already on hand.
    for (const auto& [tagKey, tagValue] : tags) {
        buildBitmapCacheKey(bitmapCacheKey, measurement, tagKey, tagValue);
        auto* bitmap = co_await getOrLoadBitmapForInsert(bitmapCacheKey);
        bitmap->add(localId);

        if (bitmap->cardinality() >= kTagHllMinCardinality) {
            co_await updateTagHLL(measurement, tagKey, tagValue, localId, bitmap);
        }
    }

    co_await updateHLL(measurement, localId);
    // Mark measurement bloom for rebuild on next flush
    dirtyMeasurementBlooms_.insert(measurement);

    // Schema-cache access pattern below: NEVER hold a reference into
    // fieldsCache_/tagsCache_/tagValuesCache_ across a co_await. A concurrent
    // write can trigger trimSchemaCaches()/trimTagValuesCache() (destroying
    // the node → UAF) or populate the same entry (a blind assignment after
    // resume would clobber its additions → schema entries silently lost).
    // Re-find after every suspension and only fill still-empty sets.

    // --- Field metadata (was addField) ---
    // NOTE: empty() check means the cache is re-populated on every call if the set is
    // legitimately empty. This is acceptable since a measurement always has at least one field.
    auto* fieldCache = &fieldsCache_[measurement];
    if (fieldCache->empty()) {
        auto val = co_await kvGet(ke::encodeMeasurementFieldsKey(measurement));
        fieldCache = &fieldsCache_[measurement];  // re-find after co_await
        if (val.has_value() && fieldCache->empty())
            *fieldCache = ke::decodeStringSet(*val);
    }
    if (fieldCache->insert(field).second) {
        batch.put(ke::encodeMeasurementFieldsKey(measurement), ke::encodeStringSet(*fieldCache));
        pendingSchemaUpdate_.newFields[measurement].insert(field);
    }

    // --- Tag metadata (was addTag for each tag) ---
    if (tagsCache_[measurement].empty()) {
        auto val = co_await kvGet(ke::encodeMeasurementTagsKey(measurement));
        auto& tagKeysCache = tagsCache_[measurement];  // re-find after co_await
        if (val.has_value() && tagKeysCache.empty())
            tagKeysCache = ke::decodeStringSet(*val);
    }
    // Reusable buffer for tag-values cache key
    std::string tvCacheKey;
    tvCacheKey.reserve(measurement.size() + 1 + 32);
    bool tagKeysChanged = false;
    for (const auto& [tagKey, tagValue] : tags) {
        if (tagsCache_[measurement].insert(tagKey).second) {
            tagKeysChanged = true;
            pendingSchemaUpdate_.newTags[measurement].insert(tagKey);
        }

        tvCacheKey.clear();
        tvCacheKey += measurement;
        tvCacheKey.push_back('\0');
        tvCacheKey += tagKey;
        auto* tagVals = &tagValuesCache_[tvCacheKey];
        if (tagVals->empty()) {
            auto loaded = co_await loadTagValuesFromKv(measurement, tagKey);
            // Re-find after co_await; MERGE the loaded set (a blind assignment
            // would clobber values added concurrently during the suspension).
            tagVals = &tagValuesCache_[tvCacheKey];
            tagVals->insert(loaded.begin(), loaded.end());
        }
        if (tagVals->insert(tagValue).second) {
            // Per-value marker key: O(1) append instead of re-encoding the
            // measurement's entire value set (O(V²) write amplification).
            // The legacy TAG_VALUES blob is no longer written; reads union it.
            batch.put(ke::encodeTagValueMarkerKey(measurement, tagKey, tagValue), "");
            pendingSchemaUpdate_.newTagValues[tvCacheKey].insert(tagValue);
        }
    }
    // Write measurement-tags once after processing all tags (not per-tag)
    if (tagKeysChanged) {
        batch.put(ke::encodeMeasurementTagsKey(measurement), ke::encodeStringSet(tagsCache_[measurement]));
    }

    // Single WAL write for everything
    co_await kvWriteBatch(batch);
    seriesCacheInsert(seriesId);

    // The forward mapping was persisted in this batch — skip it in the next
    // flushDirtyBitmaps pass (avoids re-putting every mapping every flush).
    lastFlushedLocalId_ = std::max(lastFlushedLocalId_, localId + 1);

    // Pre-populate metadata cache — avoids decode cost on future lookups.
    // The metadata object is already constructed; move it into cache.
    seriesMetadataCache_.put(seriesId, std::move(metadata));

    // Invalidate discovery cache for this measurement
    invalidateDiscoveryCache(measurement);

    // Step 5: measurementSeriesCache_ removed — no update needed

    co_return seriesId;
}

seastar::future<std::optional<SeriesId128>> NativeIndex::getSeriesId(const std::string& measurement,
                                                                     const std::map<std::string, std::string>& tags,
                                                                     const std::string& field) {
    std::string seriesKey = timestar::buildSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Step 8: Existence check without copying the value
    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    if (co_await kvExists(metaKey)) {
        co_return seriesId;
    }
    co_return std::nullopt;
}

seastar::future<std::optional<SeriesMetadata>> NativeIndex::getSeriesMetadata(const SeriesId128& seriesId) {
    // Check metadata cache
    auto cached = seriesMetadataCache_.get(seriesId);
    if (cached)
        co_return *cached;

    auto key = ke::encodeSeriesMetadataKey(seriesId);
    auto val = co_await kvGet(key);
    if (!val.has_value())
        co_return std::nullopt;

    auto metadata = ke::decodeSeriesMetadata(*val);
    seriesMetadataCache_.put(seriesId, metadata);
    co_return metadata;
}

// Stage 3: Batch metadata resolution — inlines cache checks and avoids
// N sequential getSeriesMetadata coroutine calls.
seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>> NativeIndex::getSeriesMetadataBatch(
    const std::vector<SeriesId128>& seriesIds) {
    std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>> results;
    results.reserve(seriesIds.size());

    // Reusable key buffer — avoids re-allocating 17-byte string per ID
    std::string key;
    key.reserve(1 + 16);

    for (const auto& id : seriesIds) {
        // Inline cache check — avoids coroutine overhead of getSeriesMetadata()
        auto cached = seriesMetadataCache_.get(id);
        if (cached) {
            results.push_back({id, *cached});
            continue;
        }

        // Cache miss — build key in reusable buffer
        key.clear();
        key.push_back(static_cast<char>(SERIES_METADATA));
        id.appendTo(key);

        auto val = co_await kvGet(key);
        if (val.has_value()) {
            auto metadata = ke::decodeSeriesMetadata(*val);
            seriesMetadataCache_.put(id, metadata);
            results.push_back({id, std::move(metadata)});
        } else {
            results.push_back({id, std::nullopt});
        }
    }

    co_return results;
}

// ============================================================================
// Measurement metadata
// ============================================================================

seastar::future<> NativeIndex::setFieldType(const std::string& measurement, const std::string& field,
                                            const std::string& type) {
    std::string cacheKey;
    cacheKey.reserve(measurement.size() + 1 + field.size());
    cacheKey = measurement;
    cacheKey.push_back('\0');
    cacheKey += field;
    // Step 8: Check cache first, then use kvExists (no value copy needed)
    if (knownFieldTypes_.count(cacheKey))
        co_return;

    // After a cache clear, check KV before writing to avoid redundant WAL ops.
    auto kvKey = ke::encodeFieldTypeKey(measurement, field);
    if (co_await kvExists(kvKey)) {
        knownFieldTypes_.insert(cacheKey);
        co_return;
    }

    co_await kvPut(kvKey, type);
    knownFieldTypes_.insert(cacheKey);
    fieldTypeValues_[cacheKey] = type;
    pendingSchemaUpdate_.newFieldTypes[cacheKey] = type;
}

seastar::future<std::string> NativeIndex::getFieldType(const std::string& measurement, const std::string& field) {
    // Check in-memory cache first (populated by local indexing and schema broadcast)
    std::string cacheKey;
    cacheKey.reserve(measurement.size() + 1 + field.size());
    cacheKey = measurement;
    cacheKey.push_back('\0');
    cacheKey += field;
    auto it = fieldTypeValues_.find(cacheKey);
    if (it != fieldTypeValues_.end()) {
        co_return it->second;
    }
    // Fall back to KV store
    auto val = co_await kvGet(ke::encodeFieldTypeKey(measurement, field));
    if (val.has_value()) {
        fieldTypeValues_[cacheKey] = *val;
    }
    co_return val.value_or("");
}

seastar::future<std::set<std::string>> NativeIndex::getAllMeasurements() {
    std::set<std::string> result;

    // Include measurements from cache (populated by local indexing and schema
    // broadcast). Skip empty entries: getFields() negative-caches {} for
    // nonexistent measurements, and listing those here fabricated phantom
    // measurements out of mere lookup typos.
    for (const auto& [measurement, fields] : fieldsCache_) {
        if (!fields.empty()) {
            result.insert(measurement);
        }
    }

    // Also scan KV store for measurements not yet in cache
    std::string prefix(1, static_cast<char>(MEASUREMENT_FIELDS));
    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        result.insert(std::string(key.substr(1)));  // Skip prefix byte
        return true;
    });
    co_return result;
}

seastar::future<std::set<std::string>> NativeIndex::getFields(std::string measurement) {
    if (auto it = fieldsCache_.find(measurement); it != fieldsCache_.end())
        co_return it->second;

    auto val = co_await kvGet(ke::encodeMeasurementFieldsKey(measurement));
    if (val.has_value()) {
        auto decoded = ke::decodeStringSet(*val);
        fieldsCache_[measurement] = decoded;
        co_return decoded;
    }
    // Cache empty result to avoid repeated KV lookups for non-existent measurements
    fieldsCache_[measurement] = {};
    co_return std::set<std::string>{};
}

seastar::future<std::set<std::string>> NativeIndex::getTags(std::string measurement) {
    if (auto it = tagsCache_.find(measurement); it != tagsCache_.end())
        co_return it->second;

    auto val = co_await kvGet(ke::encodeMeasurementTagsKey(measurement));
    if (val.has_value()) {
        auto decoded = ke::decodeStringSet(*val);
        tagsCache_[measurement] = decoded;
        co_return decoded;
    }
    // Cache empty result to avoid repeated KV lookups
    tagsCache_[measurement] = {};
    co_return std::set<std::string>{};
}

seastar::future<std::set<std::string>> NativeIndex::getTagValues(std::string measurement, std::string tagKey) {
    std::string tvCacheKey;
    tvCacheKey.reserve(measurement.size() + 1 + tagKey.size());
    tvCacheKey = measurement;
    tvCacheKey.push_back('\0');
    tvCacheKey += tagKey;
    // Only trust non-empty cache entries: an empty set may be a transient
    // placeholder created by getOrCreateSeriesId's operator[] mid-load.
    if (auto it = tagValuesCache_.find(tvCacheKey); it != tagValuesCache_.end() && !it->second.empty())
        co_return it->second;

    auto loaded = co_await loadTagValuesFromKv(measurement, tagKey);
    if (!loaded.empty()) {
        // Re-find after co_await and MERGE — a concurrent insert may have
        // added values to this entry during the suspension.
        auto& entry = tagValuesCache_[tvCacheKey];
        entry.insert(loaded.begin(), loaded.end());
        co_return entry;
    }
    // Nothing persisted — return whatever concurrent inserts put in the cache
    // (usually nothing). Do NOT cache the empty result: a later full load must
    // still be able to populate the entry.
    if (auto it = tagValuesCache_.find(tvCacheKey); it != tagValuesCache_.end())
        co_return it->second;
    co_return std::set<std::string>{};
}

// Union of the legacy TAG_VALUES blob (pre-marker DBs keep working without
// migration) and the per-value TAG_VALUE_MARKER keys.
seastar::future<std::set<std::string>> NativeIndex::loadTagValuesFromKv(const std::string& measurement,
                                                                        const std::string& tagKey) {
    std::set<std::string> result;
    auto blob = co_await kvGet(ke::encodeTagValuesKey(measurement, tagKey));
    if (blob.has_value()) {
        result = ke::decodeStringSet(*blob);
    }
    std::string prefix = ke::encodeTagValueMarkerPrefix(measurement, tagKey);
    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        if (key.size() > prefix.size()) {
            result.emplace(key.substr(prefix.size()));
        }
        return true;
    });
    co_return result;
}

seastar::future<> NativeIndex::indexMetadataBatch(const std::vector<MetadataOp>& ops) {
    if (ops.empty())
        co_return;

    for (const auto& op : ops) {
        // Use the pre-computed series ID when the producer supplied one (write
        // handler always does) — skips buildSeriesKey + rehash + map copy.
        SeriesId128 seriesId = !op.seriesId.isZero()
                                   ? co_await getOrCreateSeriesId(op.seriesId, op.measurement, op.tags, op.fieldName)
                                   : co_await getOrCreateSeriesId(op.measurement, op.tags, op.fieldName);
        auto typeStr = timestar::valueTypeName(op.valueType);
        if (!typeStr.empty()) {
            co_await setFieldType(op.measurement, op.fieldName, std::string(typeStr));
        }
        // First-batch day coverage: the data-shard batch path can only record
        // days once the LocalId exists, which happens here. Recording the full
        // [minTs, maxTs] day span is a safe superset (extra days reduce pruning;
        // MISSING days would wrongly exclude the series from time-scoped queries).
        if (op.maxTs >= op.minTs && op.maxTs > 0) {
            co_await recordDaySpan(op.measurement, seriesId, op.minTs, op.maxTs);
        }
    }
}

seastar::future<> NativeIndex::recordDaySpan(const std::string& measurement, const SeriesId128& seriesId,
                                             uint64_t minTs, uint64_t maxTs) {
    auto localIdOpt = localIdMap_.getLocalId(seriesId);
    if (!localIdOpt.has_value())
        co_return;
    const uint32_t localId = *localIdOpt;
    const uint32_t firstDay = ke::dayBucketFromNs(minTs);
    const uint32_t lastDay = ke::dayBucketFromNs(maxTs);

    // Bound the span: min/max come from raw client timestamps, so one batch
    // mixing ts=0 (or a seconds-precision timestamp that decodes as 1970)
    // with a current nanosecond timestamp would otherwise create ~20,000
    // day bitmaps (one kvGet + a permanently persisted dirty bitmap each).
    // The query side caps day scans at 365 days; mirror that here. An
    // over-wide span only costs discovery precision, never correctness —
    // day bitmaps are a superset filter.
    constexpr uint32_t kMaxDaySpan = 366;
    if (lastDay - firstDay >= kMaxDaySpan) {
        ::native_index_log.warn(
            "recordDaySpan: day span [{}, {}] for measurement '{}' exceeds {} days — "
            "recording only the trailing window (check client timestamp precision)",
            firstDay, lastDay, measurement, kMaxDaySpan);
    }
    const uint32_t clampedFirst = (lastDay - firstDay >= kMaxDaySpan) ? lastDay - (kMaxDaySpan - 1) : firstDay;

    std::string dayCacheKey;
    bool newDayMembership = false;
    for (uint32_t day = clampedFirst; day <= lastDay; ++day) {
        buildDayBitmapCacheKey(dayCacheKey, measurement, day);
        // addChecked: day-scoped discovery results cached under the current
        // generation go stale when an EXISTING series first appears in a day.
        if ((co_await getOrLoadDayBitmapForInsert(dayCacheKey))->addChecked(localId)) {
            newDayMembership = true;
        }
    }
    if (newDayMembership) {
        invalidateDiscoveryCache(measurement);
    }
}

seastar::future<> NativeIndex::recordInsertDays(const std::string& measurement, const SeriesId128& seriesId,
                                                const std::vector<uint64_t>& timestamps) {
    // Exact per-day recording for the batch insert path (mirrors indexInsert's
    // day loop). Skips silently when the series has no LocalId yet — the
    // first-batch case is covered by the MetadataOp day-span path above.
    auto localIdOpt = localIdMap_.getLocalId(seriesId);
    if (!localIdOpt.has_value())
        co_return;
    const uint32_t localId = *localIdOpt;
    std::string dayCacheKey;
    uint32_t lastDay = UINT32_MAX;
    bool newDayMembership = false;
    for (uint64_t ts : timestamps) {
        uint32_t day = ke::dayBucketFromNs(ts);
        if (day != lastDay) {
            buildDayBitmapCacheKey(dayCacheKey, measurement, day);
            // addChecked: invalidate cached day-scoped discovery when an
            // existing series first appears in a day (see recordDaySpan).
            if ((co_await getOrLoadDayBitmapForInsert(dayCacheKey))->addChecked(localId)) {
                newDayMembership = true;
            }
            lastDay = day;
        }
    }
    if (newDayMembership) {
        invalidateDiscoveryCache(measurement);
    }
}

// ============================================================================
// Series discovery
// ============================================================================

seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> NativeIndex::findSeries(
    const std::string& measurement, const std::map<std::string, std::string>& tagFilters, size_t maxSeries) {
    if (tagFilters.empty()) {
        co_return co_await getAllSeriesForMeasurement(measurement, maxSeries);
    }

    // Single-tag fast path — no bitmap copy needed, delegate to findSeriesByTag
    if (tagFilters.size() == 1) {
        auto& [tagKey, tagValue] = *tagFilters.begin();
        auto res = co_await findSeriesByTag(measurement, tagKey, tagValue, maxSeries);
        if (maxSeries > 0 && res.size() > maxSeries) {
            co_return std::unexpected(SeriesLimitExceeded{res.size(), maxSeries});
        }
        co_return res;
    }

    // Multi-tag: incrementally intersect bitmaps.
    // IMPORTANT: We copy each bitmap immediately because getPostingsBitmapByKey() may
    // insert into bitmapCache_ (a robin_map), which can rehash and invalidate prior pointers.
    std::string cacheKey;
    roaring::Roaring result;
    bool first = true;
    for (const auto& [tagKey, tagValue] : tagFilters) {
        buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
        auto* bitmap = co_await getPostingsBitmapByKey(cacheKey);
        if (!bitmap)
            co_return std::vector<SeriesId128>{};
        if (first) {
            result = *bitmap;  // copy into owned accumulator
            first = false;
        } else {
            result &= *bitmap;
            if (result.isEmpty())
                co_return std::vector<SeriesId128>{};
        }
    }

    // Convert bitmap to SeriesId128 vector via reverse lookup.
    // `result` is an owned accumulator, so yielding mid-loop is safe.
    std::vector<SeriesId128> res;
    res.reserve(result.cardinality());
    size_t processed = 0;
    for (auto it = result.begin(); it != result.end(); ++it) {
        if ((++processed & 0x3FFF) == 0)
            co_await seastar::coroutine::maybe_yield();
        uint32_t localId = *it;
        if (localIdMap_.isValid(localId)) {
            res.push_back(localIdMap_.getGlobalId(localId));
        }
    }

    if (maxSeries > 0 && res.size() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{res.size(), maxSeries});
    }

    co_return res;
}

// Fused series discovery + metadata resolution.
// For tag-filtered queries, iterates bitmap directly and resolves metadata inline
// in a single pass — avoids materializing an intermediate vector<SeriesId128>.
seastar::future<std::expected<std::vector<IndexBackend::SeriesWithMetadata>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadata(const std::string& measurement,
                                    const std::map<std::string, std::string>& tagFilters,
                                    const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    // For no-tag queries, fall through to the vector-based path
    if (tagFilters.empty()) {
        auto findResult = co_await getAllSeriesForMeasurement(measurement, maxSeries);
        if (!findResult.has_value()) {
            co_return std::unexpected(findResult.error());
        }
        const auto& seriesIds = *findResult;
        std::vector<SeriesWithMetadata> results;
        results.reserve(seriesIds.size());
        std::string key;
        key.reserve(1 + 16);
        size_t processed = 0;
        for (const auto& id : seriesIds) {
            // kvGet below is a ready future on warm cache — yield explicitly
            if ((++processed & 0x3FFF) == 0)
                co_await seastar::coroutine::maybe_yield();
            auto cached = seriesMetadataCache_.get(id);
            const SeriesMetadata* meta = nullptr;
            std::optional<SeriesMetadata> decoded;
            if (cached) {
                meta = cached;
            } else {
                key.clear();
                key.push_back(static_cast<char>(SERIES_METADATA));
                id.appendTo(key);
                auto val = co_await kvGet(key);
                if (val.has_value()) {
                    decoded.emplace(ke::decodeSeriesMetadata(*val));
                    seriesMetadataCache_.put(id, *decoded);
                    meta = &*decoded;
                }
            }
            if (!meta)
                continue;
            if (!fieldFilter.empty() && !fieldFilter.count(meta->field))
                continue;
            if (decoded)
                results.push_back({id, std::move(*decoded)});
            else
                results.push_back({id, *meta});
        }
        co_return results;
    }

    // Tag-filtered: build bitmap intersection, then iterate bitmap directly
    // resolving metadata inline (single pass, no intermediate vector).
    std::string cacheKey;
    std::optional<roaring::Roaring> intersected;

    if (tagFilters.size() == 1) {
        // Single tag — copy bitmap into owned storage (pointer may be invalidated by future cache ops)
        auto& [tagKey, tagValue] = *tagFilters.begin();
        buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
        auto* bmp = co_await getPostingsBitmapByKey(cacheKey);
        if (!bmp)
            co_return std::vector<SeriesWithMetadata>{};
        intersected = *bmp;
    } else {
        // Multi-tag — incrementally intersect bitmaps.
        // IMPORTANT: Copy each bitmap immediately because getPostingsBitmapByKey() may
        // insert into bitmapCache_ (a robin_map), which can rehash and invalidate prior pointers.
        bool first = true;
        for (const auto& [tagKey, tagValue] : tagFilters) {
            buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
            auto* bmp = co_await getPostingsBitmapByKey(cacheKey);
            if (!bmp)
                co_return std::vector<SeriesWithMetadata>{};
            if (first) {
                intersected = *bmp;
                first = false;
            } else {
                *intersected &= *bmp;
                if (intersected->isEmpty())
                    co_return std::vector<SeriesWithMetadata>{};
            }
        }
    }

    const roaring::Roaring& bitmap = *intersected;

    if (maxSeries > 0 && bitmap.cardinality() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{bitmap.cardinality(), maxSeries});
    }

    // Single-pass: iterate bitmap → reverse lookup → metadata resolve → filter → emit
    std::vector<SeriesWithMetadata> results;
    results.reserve(bitmap.cardinality());
    std::string key;
    key.reserve(1 + 16);
    size_t processed = 0;

    for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        // kvGet below is a ready future on warm cache — yield explicitly.
        // `bitmap` is an owned local (intersected), so this is suspension-safe.
        if ((++processed & 0x3FFF) == 0)
            co_await seastar::coroutine::maybe_yield();
        uint32_t localId = *it;
        if (!localIdMap_.isValid(localId))
            continue;
        // COPY, not reference: this loop suspends at kvGet below, and a
        // concurrent insert's getOrAssign can realloc the underlying vector,
        // dangling any reference returned by getGlobalId.
        SeriesId128 id = localIdMap_.getGlobalId(localId);

        auto cached = seriesMetadataCache_.get(id);
        const SeriesMetadata* meta = nullptr;
        std::optional<SeriesMetadata> decoded;

        if (cached) {
            meta = cached;
        } else {
            key.clear();
            key.push_back(static_cast<char>(SERIES_METADATA));
            id.appendTo(key);
            auto val = co_await kvGet(key);
            if (val.has_value()) {
                decoded.emplace(ke::decodeSeriesMetadata(*val));
                seriesMetadataCache_.put(id, *decoded);
                meta = &*decoded;
            }
        }
        if (!meta)
            continue;
        if (!fieldFilter.empty() && !fieldFilter.count(meta->field))
            continue;
        if (decoded)
            results.push_back({id, std::move(*decoded)});
        else
            results.push_back({id, *meta});
    }

    co_return results;
}

// Build discovery cache key with generation counter to avoid O(N) prefix scan
// on invalidation. Shared by findSeriesWithMetadataCached and the time-scoped
// variant (which appends a "D:start-end" day-range suffix — non-scoped keys
// always end with "L:<num>", so the key spaces cannot collide).
std::string NativeIndex::buildDiscoveryCacheKey(const std::string& measurement,
                                                const std::map<std::string, std::string>& tagFilters,
                                                const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    // Absent means never invalidated — generation 0 (find() avoids the
    // default-insert of operator[] on this read path).
    uint64_t gen = 0;
    if (auto genIt = discoveryCacheGen_.find(measurement); genIt != discoveryCacheGen_.end()) {
        gen = genIt->second;
    }

    // Reserve the full key up front (20 digits covers any uint64/size_t)
    size_t estimate = measurement.size() + 1 + 20 + 1 + 2 + 2 + 20;
    for (const auto& [k, v] : tagFilters) {
        estimate += k.size() + v.size() + 2;
    }
    for (const auto& f : fieldFilter) {
        estimate += f.size() + 1;
    }

    char numBuf[20];
    std::string cacheKey;
    cacheKey.reserve(estimate);
    cacheKey += measurement;
    cacheKey += '\0';
    auto [genEnd, genEc] = std::to_chars(numBuf, numBuf + sizeof(numBuf), gen);
    cacheKey.append(numBuf, genEnd);
    cacheKey += '\0';
    for (const auto& [k, v] : tagFilters) {
        cacheKey += k;
        cacheKey += '=';
        cacheKey += v;
        cacheKey += '\0';
    }
    cacheKey += "F:";
    // Sort field names for deterministic cache key (unordered_set iteration is
    // non-deterministic). Sort pointers — avoids copying every field string.
    std::vector<const std::string*> sortedFields;
    sortedFields.reserve(fieldFilter.size());
    for (const auto& f : fieldFilter) {
        sortedFields.push_back(&f);
    }
    std::sort(sortedFields.begin(), sortedFields.end(),
              [](const std::string* a, const std::string* b) { return *a < *b; });
    for (const auto* f : sortedFields) {
        cacheKey += *f;
        cacheKey += ',';
    }
    // Include maxSeries in cache key: a cached result with maxSeries=100 must not
    // be returned for a query with maxSeries=10 (which should enforce a lower limit).
    cacheKey += "L:";
    auto [maxEnd, maxEc] = std::to_chars(numBuf, numBuf + sizeof(numBuf), maxSeries);
    cacheKey.append(numBuf, maxEnd);
    return cacheKey;
}

seastar::future<
    std::expected<std::shared_ptr<const std::vector<IndexBackend::SeriesWithMetadata>>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadataCached(const std::string& measurement,
                                          const std::map<std::string, std::string>& tagFilters,
                                          const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    std::string cacheKey = buildDiscoveryCacheKey(measurement, tagFilters, fieldFilter, maxSeries);

    auto cached = discoveryCache_.get(cacheKey);
    if (cached)
        co_return *cached;

    auto result = co_await findSeriesWithMetadata(measurement, tagFilters, fieldFilter, maxSeries);
    if (!result.has_value()) {
        co_return std::unexpected(result.error());
    }

    auto ptr = std::make_shared<const std::vector<SeriesWithMetadata>>(std::move(*result));
    discoveryCache_.put(cacheKey, ptr);
    co_return ptr;
}

void NativeIndex::invalidateDiscoveryCache(const std::string& measurement) {
    // Bump generation — stale entries become unreachable and are evicted by LRU naturally
    discoveryCacheGen_[measurement] = nextDiscoveryCacheGen_++;
}

size_t NativeIndex::getMetadataCacheSize() const {
    return seriesMetadataCache_.size();
}
size_t NativeIndex::getMetadataCacheBytes() const {
    return seriesMetadataCache_.currentBytes();
}
size_t NativeIndex::getDiscoveryCacheSize() const {
    return discoveryCache_.size();
}
size_t NativeIndex::getDiscoveryCacheBytes() const {
    return discoveryCache_.currentBytes();
}

// ============================================================================
// Tag queries
// ============================================================================

seastar::future<std::vector<SeriesId128>> NativeIndex::findSeriesByTag(const std::string& measurement,
                                                                       const std::string& tagKey,
                                                                       const std::string& tagValue, size_t maxSeries) {
    // Phase 2: Load roaring bitmap and reverse-lookup to global IDs
    std::string cacheKey;
    buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
    auto* bitmapPtr = co_await getPostingsBitmapByKey(cacheKey);
    if (!bitmapPtr)
        co_return std::vector<SeriesId128>{};
    // Owned copy: the loop below yields periodically, and a concurrent insert
    // can rehash bitmapCache_ (dangling bitmapPtr) or add to this bitmap
    // (invalidating its iterator) during the suspension.
    roaring::Roaring bitmap = *bitmapPtr;

    std::vector<SeriesId128> result;
    result.reserve(bitmap.cardinality());
    size_t processed = 0;
    for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        if ((++processed & 0x3FFF) == 0)
            co_await seastar::coroutine::maybe_yield();
        uint32_t localId = *it;
        if (localIdMap_.isValid(localId)) {
            result.push_back(localIdMap_.getGlobalId(localId));
        }
        if (maxSeries > 0 && result.size() >= maxSeries)
            break;
    }

    co_return result;
}

seastar::future<std::map<std::string, std::vector<SeriesId128>>> NativeIndex::getSeriesGroupedByTag(
    const std::string& measurement, const std::string& tagKey) {
    // Phase 2: Scan POSTINGS_BITMAP prefix in KV, overlay dirty cache entries.
    std::string bitmapPrefix = ke::encodePostingsBitmapPrefix(measurement, tagKey);

    // Cache key prefix (without the 0x13 byte): "measurement\0tagKey\0"
    std::string cachePrefix;
    cachePrefix.reserve(measurement.size() + 1 + tagKey.size() + 1);
    cachePrefix += measurement;
    cachePrefix.push_back('\0');
    cachePrefix += tagKey;
    cachePrefix.push_back('\0');

    // First: scan persisted KV for all bitmaps with this prefix.
    // Collect tag values for hits and raw data for misses. Do NOT store pointers
    // into bitmapCache_ during the scan — batch inserts below can rehash the
    // robin_map and invalidate all previously collected pointers.
    std::string fullCacheKey;  // reused buffer: cachePrefix + tagValue
    fullCacheKey.reserve(cachePrefix.size() + 32);
    std::vector<std::string> cacheHitValues;                  // tag values with existing cache entries
    std::vector<std::pair<std::string, std::string>> toLoad;  // (tagValue, serialized bitmap)
    co_await kvPrefixScan(bitmapPrefix, [&](std::string_view key, std::string_view value) {
        auto tagValue = key.substr(bitmapPrefix.size());
        if (tagValue.empty() || value.empty())
            return true;
        fullCacheKey.assign(cachePrefix);
        fullCacheKey.append(tagValue);
        if (bitmapCache_.find(fullCacheKey) != bitmapCache_.end()) {
            cacheHitValues.emplace_back(tagValue);
        } else {
            toLoad.emplace_back(std::string(tagValue), std::string(value));
        }
        return true;
    });
    // Batch-insert cache misses (may rehash bitmapCache_)
    for (auto& [tagValue, value] : toLoad) {
        fullCacheKey.assign(cachePrefix);
        fullCacheKey.append(tagValue);
        auto& entry = bitmapCache_[fullCacheKey];
        entry.bitmap = roaring::Roaring::readSafe(value.data(), value.size());
        entry.dirty = false;
        cacheHitValues.push_back(std::move(tagValue));
    }

    // Owned copies, not pointers: the reverse-lookup loop below yields
    // periodically, and a concurrent insert can rehash bitmapCache_ (dangling
    // any pointers) or grow a bitmap under its iterator.
    std::map<std::string, roaring::Roaring> bitmapsByValue;
    for (auto& tagValue : cacheHitValues) {
        fullCacheKey.assign(cachePrefix);
        fullCacheKey.append(tagValue);
        auto cacheIt = bitmapCache_.find(fullCacheKey);
        if (cacheIt != bitmapCache_.end()) {
            bitmapsByValue.emplace(std::move(tagValue), cacheIt->second.bitmap);
        }
    }

    // Add any dirty cache entries that don't have a KV counterpart
    // (new tag values inserted since last flush).
    for (auto cacheIt = bitmapCache_.begin(); cacheIt != bitmapCache_.end(); ++cacheIt) {
        if (!cacheIt->second.dirty)
            continue;
        auto& postingsKey = cacheIt->first;
        if (postingsKey.size() > cachePrefix.size() && postingsKey.compare(0, cachePrefix.size(), cachePrefix) == 0) {
            // try_emplace: single lookup; copies the bitmap only when inserted
            bitmapsByValue.try_emplace(postingsKey.substr(cachePrefix.size()), cacheIt->second.bitmap);
        }
    }

    // Reverse-lookup to global IDs
    std::map<std::string, std::vector<SeriesId128>> result;
    size_t processed = 0;
    for (auto& [tagValue, bitmap] : bitmapsByValue) {
        if (bitmap.isEmpty())
            continue;
        auto& vec = result[tagValue];
        vec.reserve(bitmap.cardinality());
        for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
            if ((++processed & 0x3FFF) == 0)
                co_await seastar::coroutine::maybe_yield();
            uint32_t localId = *it;
            if (localIdMap_.isValid(localId)) {
                vec.push_back(localIdMap_.getGlobalId(localId));
            }
        }
    }

    co_return result;
}

// ============================================================================
// Field stats
// ============================================================================

seastar::future<> NativeIndex::updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                                const IndexFieldStats& stats) {
    std::string key;
    key.push_back(FIELD_STATS);
    seriesId.appendTo(key);
    key.push_back('\0');
    key += field;

    std::string value;
    value += stats.dataType;
    value.push_back('\0');
    char numBuf[24];
    auto [p1, ec1] = std::to_chars(numBuf, numBuf + sizeof(numBuf), stats.minTime);
    value.append(numBuf, p1);
    value.push_back('\0');
    auto [p2, ec2] = std::to_chars(numBuf, numBuf + sizeof(numBuf), stats.maxTime);
    value.append(numBuf, p2);
    value.push_back('\0');
    auto [p3, ec3] = std::to_chars(numBuf, numBuf + sizeof(numBuf), stats.pointCount);
    value.append(numBuf, p3);

    co_await kvPut(key, value);
}

seastar::future<std::optional<IndexFieldStats>> NativeIndex::getFieldStats(const SeriesId128& seriesId,
                                                                           const std::string& field) {
    std::string key;
    key.push_back(FIELD_STATS);
    seriesId.appendTo(key);
    key.push_back('\0');
    key += field;

    auto val = co_await kvGet(key);
    if (!val.has_value())
        co_return std::nullopt;

    IndexFieldStats stats;
    std::string_view data(*val);
    size_t pos = 0;

    auto nextField = [&]() -> std::string_view {
        if (pos >= data.size())
            return {};
        auto end = data.find('\0', pos);
        if (end == std::string_view::npos) {
            auto f = data.substr(pos);
            pos = data.size();
            return f;
        }
        auto f = data.substr(pos, end - pos);
        pos = end + 1;
        return f;
    };

    stats.dataType = std::string(nextField());
    {
        auto sv = nextField();
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), stats.minTime);
        if (ec != std::errc{})
            co_return std::nullopt;
    }
    {
        auto sv = nextField();
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), stats.maxTime);
        if (ec != std::errc{})
            co_return std::nullopt;
    }
    {
        auto sv = nextField();
        auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), stats.pointCount);
        if (ec != std::errc{})
            co_return std::nullopt;
    }

    co_return stats;
}

// ============================================================================
// Measurement series
// ============================================================================

// Step 5: measurementSeriesCache_ removed — prefix scan is fast enough
// (in-memory memtable + SSTable index lookup, no unbounded cache growth).
seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> NativeIndex::getAllSeriesForMeasurement(
    const std::string& measurement, size_t maxSeries) {
    // Safety limit: if caller doesn't specify a max, use a generous default to prevent OOM.
    // 10M series × 16 bytes = 160MB — large but bounded.
    static constexpr size_t SAFETY_MAX_SERIES = 10'000'000;
    size_t effectiveMax = (maxSeries > 0) ? maxSeries : SAFETY_MAX_SERIES;

    std::string prefix = ke::encodeMeasurementSeriesPrefix(measurement);
    std::vector<SeriesId128> result;

    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        if (key.size() >= prefix.size() + 16) {
            result.push_back(SeriesId128::fromBytes(key.data() + prefix.size(), 16));
        }
        if (result.size() > effectiveMax)
            return false;
        return true;
    });

    if (result.size() > effectiveMax) {
        co_return std::unexpected(SeriesLimitExceeded{result.size(), effectiveMax});
    }

    co_return result;
}

// ============================================================================
// Cache management
// ============================================================================

size_t NativeIndex::getSeriesCacheSize() const {
    return indexedSeriesCache_.size();
}

// ============================================================================
// Retention policies
// ============================================================================

seastar::future<> NativeIndex::setRetentionPolicy(const RetentionPolicy& policy) {
    auto key = ke::encodeRetentionPolicyKey(policy.measurement);
    std::string value;
    auto ec = glz::write_json(policy, value);
    if (ec) {
        throw std::runtime_error("Failed to serialize retention policy");
    }
    co_await kvPut(key, value);
}

seastar::future<std::optional<RetentionPolicy>> NativeIndex::getRetentionPolicy(const std::string& measurement) {
    auto key = ke::encodeRetentionPolicyKey(measurement);
    auto val = co_await kvGet(key);
    if (!val.has_value())
        co_return std::nullopt;

    RetentionPolicy policy;
    auto ec = glz::read_json(policy, *val);
    if (ec) {
        ::native_index_log.warn("Failed to parse retention policy JSON for measurement '{}' — treating as no policy",
                                measurement);
        co_return std::nullopt;
    }
    co_return policy;
}

seastar::future<std::vector<RetentionPolicy>> NativeIndex::getAllRetentionPolicies() {
    std::string prefix(1, static_cast<char>(RETENTION_POLICY));
    std::vector<RetentionPolicy> result;

    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view value) {
        RetentionPolicy policy;
        std::string valStr(value);
        auto ec = glz::read_json(policy, valStr);
        if (!ec) {
            result.push_back(std::move(policy));
        } else {
            // Key layout: RETENTION_POLICY prefix byte + measurement name.
            ::native_index_log.warn("Skipping unparseable retention policy JSON for measurement '{}'",
                                    key.size() > 1 ? key.substr(1) : key);
        }
        return true;
    });

    co_return result;
}

seastar::future<bool> NativeIndex::deleteRetentionPolicy(const std::string& measurement) {
    auto key = ke::encodeRetentionPolicyKey(measurement);
    auto existing = co_await kvGet(key);
    if (!existing.has_value())
        co_return false;

    co_await kvDelete(key);
    co_return true;
}

// ============================================================================
// Debug/maintenance
// ============================================================================

size_t NativeIndex::getSeriesCountSync() const {
    // O(1) via LocalIdMap — no disk I/O, no .get() on futures, no reactor stall.
    return localIdMap_.size();
}

seastar::future<> NativeIndex::compact() {
    // Wait for any in-flight background flush, then flush active memtable
    co_await waitForFlush();
    if (memtable_ && !memtable_->empty()) {
        co_await flushMemTable();
    }

    // Retire all readers before compaction (compaction deletes files). Deferred
    // close: an in-flight scan may still hold a snapshot of these readers.
    for (auto& [fileNum, reader] : sstableReaders_) {
        pendingCloseReaders_.push_back(std::move(reader));
    }
    sstableReaders_.clear();

    co_await compaction_->compactAll();
    co_await refreshSSTables();

    // Step 5: measurementSeriesCache_ removed — no cache to clear
}

// ============================================================================
// Phase 2: Roaring bitmap postings
// ============================================================================

void NativeIndex::buildBitmapCacheKey(std::string& out, const std::string& measurement, const std::string& tagKey,
                                      const std::string& tagValue) {
    out.clear();
    out.reserve(measurement.size() + 1 + tagKey.size() + 1 + tagValue.size());
    out += measurement;
    out.push_back('\0');
    out += tagKey;
    out.push_back('\0');
    out += tagValue;
}

void NativeIndex::flushDirtyBitmaps(IndexWriteBatch& batch) {
    // Persist the local ID counter once per flush
    if (localIdMap_.size() > 0) {
        batch.put(ke::encodeLocalIdCounterKey(), ke::encodeLocalId(localIdMap_.nextId()));
    }

    // Batch-persist LOCAL_ID_FORWARD entries for IDs assigned since last flush
    // (normally none — series creation persists its own mapping — but this
    // covers IDs assigned by repair/migration paths).
    for (uint32_t id = lastFlushedLocalId_; id < localIdMap_.nextId(); ++id) {
        batch.put(ke::encodeLocalIdForwardKey(id), localIdMap_.getGlobalId(id).toBytes());
    }
    lastFlushedLocalId_ = localIdMap_.nextId();

    // Advance the postings watermark: every local ID below nextId() has its
    // bitmap membership included in this batch. On restart, open() re-adds
    // postings only for IDs at/after the persisted watermark (crash window).
    batch.put(ke::encodePostingsWatermarkKey(), ke::encodeLocalId(localIdMap_.nextId()));

    // Serialize dirty bitmaps — iterate the dirty-key set, not the whole
    // cache (up to 100K entries walked per flush previously).
    std::string bitmapKey;
    for (const auto& cacheKey : bitmapCacheDirtyKeys_) {
        auto it = bitmapCache_.find(cacheKey);
        if (it == bitmapCache_.end())
            continue;
        auto& entry = it.value();
        if (!entry.dirty)
            continue;

        auto& bitmap = entry.bitmap;
        if (bitmap.isEmpty()) {
            // Nothing to persist — clear the flag so the entry is evictable
            // (previously empty-but-dirty entries were pinned forever).
            entry.dirty = false;
            continue;
        }

        bitmap.runOptimize();
        bitmap.shrinkToFit();
        entry.dirty = false;

        bitmapKey.clear();
        bitmapKey.push_back(static_cast<char>(POSTINGS_BITMAP));
        bitmapKey.append(it->first);
        size_t serializedSize = bitmap.getSizeInBytes();
        entry.approxBytes = serializedSize;
        std::string serialized(serializedSize, '\0');
        bitmap.write(serialized.data());
        batch.put(bitmapKey, serialized);
    }
    bitmapCacheDirtyKeys_.clear();
}

seastar::future<const roaring::Roaring*> NativeIndex::getPostingsBitmapByKey(const std::string& cacheKey) {
    auto it = bitmapCache_.find(cacheKey);
    if (it != bitmapCache_.end()) {
        co_return &it->second.bitmap;
    }

    // Build KV key by prepending prefix byte to cache key (used for both bloom check and KV lookup)
    std::string bitmapKvKey;
    bitmapKvKey.reserve(1 + cacheKey.size());
    bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
    bitmapKvKey.append(cacheKey);

    // Phase 4: Check measurement bloom filter before KV lookup.
    // Cache key format: "measurement\0tagKey\0tagValue"
    auto nullPos = cacheKey.find('\0');
    if (nullPos != std::string::npos) {
        std::string measurement = cacheKey.substr(0, nullPos);
        auto bloomIt = measurementBloomCache_.find(measurement);
        if (bloomIt == measurementBloomCache_.end()) {
            // Lazy-load the bloom persisted by flushDirtyMeasurementBlooms in a
            // prior session. A missing/corrupt entry is cached as a null filter
            // (never short-circuits) so KV is probed at most once per measurement.
            auto serialized = co_await kvGet(ke::encodeMeasurementBloomKey(measurement));
            // Re-find after co_await: a concurrent lookup may have inserted, and
            // robin_map rehash invalidates any earlier iterator.
            bloomIt = measurementBloomCache_.find(measurement);
            if (bloomIt == measurementBloomCache_.end()) {
                bloomIt = measurementBloomCache_
                              .emplace(measurement, serialized.has_value() ? BloomFilter::deserializeFrom(*serialized)
                                                                           : BloomFilter::createNull())
                              .first;
            }
        }
        if (!bloomIt->second.isNull()) {
            if (!bloomIt->second.mayContain(bitmapKvKey)) {
                co_return nullptr;
            }
        }
    }

    auto val = co_await kvGet(bitmapKvKey);
    // Re-find after co_await: a concurrent insert may have created (and
    // dirtied) this entry during the suspension. A plain assignment would
    // discard its adds AND clear the dirty flag — the postings update would
    // never be flushed, permanently hiding the series. Merge instead and
    // leave the dirty flag alone (mirrors getOrLoadBitmapForInsert).
    auto post = bitmapCache_.find(cacheKey);
    if (post != bitmapCache_.end()) {
        if (val.has_value()) {
            post.value().bitmap |= roaring::Roaring::readSafe(val->data(), val->size());
            post.value().approxBytes = val->size();
        }
        co_return &post.value().bitmap;
    }
    if (val.has_value()) {
        auto& entry = bitmapCache_[cacheKey];
        entry.bitmap = roaring::Roaring::readSafe(val->data(), val->size());
        entry.dirty = false;
        entry.approxBytes = val->size();
        co_return &entry.bitmap;
    }

    co_return nullptr;
}

seastar::future<roaring::Roaring*> NativeIndex::getOrLoadBitmapForInsert(std::string& cacheKey) {
    auto it = bitmapCache_.find(cacheKey);
    if (it != bitmapCache_.end()) {
        it.value().dirty = true;
        bitmapCacheDirtyKeys_.insert(cacheKey);
        co_return &it.value().bitmap;
    }

    // Cache miss — cold load from KV store
    std::string bitmapKvKey;
    bitmapKvKey.reserve(1 + cacheKey.size());
    bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
    bitmapKvKey.append(cacheKey);

    // Pre-insert an empty bitmap before co_await so concurrent coroutines
    // can start adding local IDs immediately (cache hit path at line 1742).
    // Do NOT hold a reference across the suspension — robin_map rehash
    // would invalidate it.
    bitmapCache_[cacheKey].dirty = true;
    bitmapCacheDirtyKeys_.insert(cacheKey);
    auto existing = co_await kvGet(bitmapKvKey);
    // Re-find after co_await (rehash may have moved entries)
    auto& entry = bitmapCache_[cacheKey];
    if (existing.has_value()) {
        // IMPORTANT: Merge (OR) the KV-loaded bitmap into the existing entry
        // rather than replacing it. During the co_await above, concurrent
        // coroutines may have added local IDs to the pre-inserted empty bitmap.
        // A plain assignment would silently discard those concurrent adds.
        entry.bitmap |= roaring::Roaring::readSafe(existing->data(), existing->size());
        entry.approxBytes = existing->size();
    }
    co_return &entry.bitmap;
}

seastar::future<> NativeIndex::migrateToLocalIds(IndexWriteBatch& batch) {
    // Scan all SERIES_METADATA entries to assign local IDs
    std::string metaPrefix(1, static_cast<char>(SERIES_METADATA));
    co_await kvPrefixScan(metaPrefix, [&](std::string_view key, std::string_view) {
        if (key.size() >= 1 + 16) {
            SeriesId128 globalId = SeriesId128::fromBytes(key.data() + 1, 16);
            uint32_t localId = localIdMap_.getOrAssign(globalId);
            batch.put(ke::encodeLocalIdForwardKey(localId), globalId.toBytes());
        }
        return true;
    });

    if (localIdMap_.size() == 0)
        co_return;

    // Persist the counter
    batch.put(ke::encodeLocalIdCounterKey(), ke::encodeLocalId(localIdMap_.nextId()));

    // Scan TAG_INDEX entries to build initial POSTINGS_BITMAP entries.
    // Populate bitmapCache_ directly so they're immediately available for queries.
    std::string tagPrefix(1, static_cast<char>(TAG_INDEX));
    co_await kvPrefixScan(tagPrefix, [&](std::string_view key, std::string_view value) {
        if (value.size() < 16)
            return true;
        SeriesId128 globalId = SeriesId128::fromBytes(value.data(), 16);
        auto localOpt = localIdMap_.getLocalId(globalId);
        if (!localOpt.has_value())
            return true;

        // Extract measurement\0tagKey\0tagValue from key (skip prefix byte, strip trailing \0+seriesId)
        auto afterPrefix = key.substr(1);
        if (afterPrefix.size() < 17)
            return true;
        auto mtvPart = afterPrefix.substr(0, afterPrefix.size() - 16);
        if (!mtvPart.empty() && mtvPart.back() == '\0') {
            mtvPart = mtvPart.substr(0, mtvPart.size() - 1);
        }
        std::string postingsKey(mtvPart);
        auto& entry = bitmapCache_[postingsKey];
        entry.bitmap.add(*localOpt);
        entry.dirty = true;
        return true;
    });

    // Serialize dirty bitmaps into the migration batch
    std::string bitmapKey;
    size_t bitmapCount = 0;
    for (auto it = bitmapCache_.begin(); it != bitmapCache_.end(); ++it) {
        auto& entry = it.value();
        if (!entry.dirty)
            continue;
        entry.bitmap.runOptimize();
        entry.bitmap.shrinkToFit();
        entry.dirty = false;

        bitmapKey.clear();
        bitmapKey.push_back(static_cast<char>(POSTINGS_BITMAP));
        bitmapKey.append(it->first);
        size_t serializedSize = entry.bitmap.getSizeInBytes();
        std::string serialized(serializedSize, '\0');
        entry.bitmap.write(serialized.data());
        batch.put(bitmapKey, serialized);
        ++bitmapCount;
    }

    ::native_index_log.info("Migration: assigned {} local IDs, built {} postings bitmaps", localIdMap_.size(),
                            bitmapCount);
}

// ============================================================================
// Phase 3: Time-scoped per-day bitmaps
// ============================================================================

void NativeIndex::buildDayBitmapCacheKey(std::string& out, const std::string& measurement, uint32_t day) {
    out.clear();
    out.reserve(measurement.size() + 1 + 4);
    out += measurement;
    out.push_back('\0');
    // Big-endian encoding for correct lexicographic ordering of day bitmap keys
    // and consistency with decodeDayFromDayBitmapKey() which uses be32toh().
    uint32_t dayBE = htobe32(day);
    out.append(reinterpret_cast<const char*>(&dayBE), 4);
}

seastar::future<roaring::Roaring*> NativeIndex::getOrLoadDayBitmapForInsert(std::string& cacheKey) {
    auto it = dayBitmapCache_.find(cacheKey);
    if (it != dayBitmapCache_.end()) {
        it.value().dirty = true;
        dayBitmapCacheDirtyKeys_.insert(cacheKey);
        co_return &it.value().bitmap;
    }

    // Cache miss — cold load from KV store
    std::string kvKey;
    kvKey.reserve(1 + cacheKey.size());
    kvKey.push_back(static_cast<char>(TIME_SERIES_DAY));
    kvKey.append(cacheKey);

    // Pre-insert an empty bitmap before co_await so concurrent coroutines
    // can start adding local IDs immediately.
    dayBitmapCache_[cacheKey].dirty = true;
    dayBitmapCacheDirtyKeys_.insert(cacheKey);
    auto existing = co_await kvGet(kvKey);
    // Re-find after co_await (rehash may have moved entries)
    auto& entry = dayBitmapCache_[cacheKey];
    if (existing.has_value()) {
        // Merge (OR) to preserve concurrent adds during the co_await suspension.
        entry.bitmap |= roaring::Roaring::readSafe(existing->data(), existing->size());
        entry.approxBytes = existing->size();
    }
    co_return &entry.bitmap;
}

seastar::future<const roaring::Roaring*> NativeIndex::getDayBitmapByKey(const std::string& cacheKey) {
    auto it = dayBitmapCache_.find(cacheKey);
    if (it != dayBitmapCache_.end()) {
        co_return &it->second.bitmap;
    }

    // Cache miss — load from KV
    std::string kvKey;
    kvKey.reserve(1 + cacheKey.size());
    kvKey.push_back(static_cast<char>(TIME_SERIES_DAY));
    kvKey.append(cacheKey);

    auto val = co_await kvGet(kvKey);
    // Re-find after co_await and merge — same race as getPostingsBitmapByKey:
    // a concurrent insert may have created a dirty entry during the suspension;
    // assignment would drop its adds and clear the dirty flag.
    auto post = dayBitmapCache_.find(cacheKey);
    if (post != dayBitmapCache_.end()) {
        if (val.has_value()) {
            post.value().bitmap |= roaring::Roaring::readSafe(val->data(), val->size());
            post.value().approxBytes = val->size();
        }
        co_return &post.value().bitmap;
    }
    if (val.has_value()) {
        auto& entry = dayBitmapCache_[cacheKey];
        entry.bitmap = roaring::Roaring::readSafe(val->data(), val->size());
        entry.dirty = false;
        entry.approxBytes = val->size();
        co_return &entry.bitmap;
    }

    co_return nullptr;
}

void NativeIndex::flushDirtyDayBitmaps(IndexWriteBatch& batch) {
    // Iterate the dirty-key set, not the whole cache (see flushDirtyBitmaps).
    std::string kvKey;
    for (const auto& cacheKey : dayBitmapCacheDirtyKeys_) {
        auto it = dayBitmapCache_.find(cacheKey);
        if (it == dayBitmapCache_.end())
            continue;
        auto& entry = it.value();
        if (!entry.dirty)
            continue;

        auto& bitmap = entry.bitmap;
        if (bitmap.isEmpty()) {
            entry.dirty = false;
            continue;
        }

        bitmap.runOptimize();
        bitmap.shrinkToFit();
        entry.dirty = false;

        kvKey.clear();
        kvKey.push_back(static_cast<char>(TIME_SERIES_DAY));
        kvKey.append(it->first);
        size_t serializedSize = bitmap.getSizeInBytes();
        entry.approxBytes = serializedSize;
        std::string serialized(serializedSize, '\0');
        bitmap.write(serialized.data());
        batch.put(kvKey, serialized);
    }
    dayBitmapCacheDirtyKeys_.clear();
}

// Fraction of this shard's arena that ALL index caches together may occupy.
// 25% leaves room for the memtables, the query working set, WAL buffers, TSM
// sparse indexes and compaction, none of which draw from this budget.
static constexpr double kIndexCacheFractionOfShard = 0.25;

size_t NativeIndex::indexCacheBudgetBytes() {
    const size_t shardTotal = seastar::memory::stats().total_memory();
    const size_t budget = static_cast<size_t>(static_cast<double>(shardTotal) * kIndexCacheFractionOfShard);
    // Floor so tiny test/dev shards still cache usefully; ceiling so a very large
    // shard does not hand an unbounded budget to structures whose useful working
    // set is far smaller.
    constexpr size_t kMinBudget = 16u << 20;   // 16 MB
    constexpr size_t kMaxBudget = 512u << 20;  // 512 MB
    return std::clamp(budget, kMinBudget, kMaxBudget);
}

size_t NativeIndex::maxBitmapCacheBytes() {
    return indexCacheBudgetBytes() * 40 / 100;
}
size_t NativeIndex::maxDayBitmapCacheBytes() {
    return indexCacheBudgetBytes() * 20 / 100;
}
size_t NativeIndex::maxBloomCacheBytes() {
    return indexCacheBudgetBytes() * 15 / 100;
}
size_t NativeIndex::maxHllCacheBytes() {
    return indexCacheBudgetBytes() * 10 / 100;
}

void NativeIndex::trimBitmapCache() {
    // Check both entry count and byte budget
    bool overEntries = bitmapCache_.size() > MAX_BITMAP_CACHE_ENTRIES;
    size_t totalBytes = 0;
    bool overBytes = false;
    if (!overEntries) {
        // Only compute total bytes if entry count is OK (avoid O(n) scan when unnecessary)
        for (const auto& [key, entry] : bitmapCache_) {
            totalBytes += entry.approxBytes + key.size();
        }
        overBytes = totalBytes > maxBitmapCacheBytes();
    }
    if (!overEntries && !overBytes)
        return;

    // Evict non-dirty entries until under BOTH limits. The byte budget is
    // enforced independently of the entry count: a few thousand huge bitmaps
    // can blow the memory budget while staying far under the entry cap
    // (previously the loop broke on entry count alone, making the byte budget
    // dead code). Evicting a persisted postings entry also invalidates the
    // measurement's bloomFullyBuilt_ claim — the next bloom rebuild must
    // re-scan KV or it would persist a bloom missing the evicted keys
    // (false negatives = series silently invisible).
    const size_t entryTarget = MAX_BITMAP_CACHE_ENTRIES / 2;  // evict to 50% to avoid thrashing
    const size_t byteTarget = maxBitmapCacheBytes() / 2;
    for (auto it = bitmapCache_.begin(); it != bitmapCache_.end();) {
        bool doneEntries = !overEntries || bitmapCache_.size() <= entryTarget;
        bool doneBytes = !overBytes || totalBytes <= byteTarget;
        if (doneEntries && doneBytes)
            break;
        if (!it->second.dirty) {
            if (overBytes) {
                totalBytes -= it->second.approxBytes + it->first.size();
            }
            auto nullPos = it->first.find('\0');
            if (nullPos != std::string::npos) {
                bloomFullyBuilt_.erase(it->first.substr(0, nullPos));
            }
            it = bitmapCache_.erase(it);
        } else {
            ++it;
        }
    }
}

void NativeIndex::trimDayBitmapCache() {
    bool overEntries = dayBitmapCache_.size() > MAX_DAY_BITMAP_CACHE_ENTRIES;
    size_t totalBytes = 0;
    bool overBytes = false;
    if (!overEntries) {
        for (const auto& [key, entry] : dayBitmapCache_) {
            totalBytes += entry.approxBytes + key.size();
        }
        overBytes = totalBytes > maxDayBitmapCacheBytes();
    }
    if (!overEntries && !overBytes)
        return;

    // Enforce the byte budget independently of the entry cap (see trimBitmapCache).
    const size_t entryTarget = MAX_DAY_BITMAP_CACHE_ENTRIES / 2;
    const size_t byteTarget = maxDayBitmapCacheBytes() / 2;
    for (auto it = dayBitmapCache_.begin(); it != dayBitmapCache_.end();) {
        bool doneEntries = !overEntries || dayBitmapCache_.size() <= entryTarget;
        bool doneBytes = !overBytes || totalBytes <= byteTarget;
        if (doneEntries && doneBytes)
            break;
        if (!it->second.dirty) {
            if (overBytes) {
                totalBytes -= it->second.approxBytes + it->first.size();
            }
            it = dayBitmapCache_.erase(it);
        } else {
            ++it;
        }
    }
}

// Step 7: Trim HLL cache after flush — evict non-dirty entries when too large.
// Per-measurement HLLs (key = "measurement\0") stay warm; per-tag-value HLLs
// (key = "measurement\0tagKey\0tagValue") are numerous and rarely queried.
void NativeIndex::trimHllCache() {
    if (hllCache_.size() <= MAX_HLL_CACHE_ENTRIES)
        return;

    size_t toEvict = hllCache_.size() - MAX_HLL_CACHE_ENTRIES;
    for (auto it = hllCache_.begin(); it != hllCache_.end() && toEvict > 0;) {
        // Skip dirty entries (will be evicted after next flush)
        if (hllCacheDirty_.count(it->first)) {
            ++it;
            continue;
        }
        // Skip per-measurement HLLs (few entries, frequently queried by /cardinality API)
        // Per-measurement keys have format "measurement\0" (single null separator, no more)
        const auto& key = it->first;
        auto firstNull = key.find('\0');
        if (firstNull != std::string::npos && firstNull == key.size() - 1) {
            ++it;
            continue;
        }
        it = hllCache_.erase(it);
        --toEvict;
    }
}

// Step 6: Evict tag values cache entries when over limit.
void NativeIndex::trimTagValuesCache() {
    if (tagValuesCache_.size() <= MAX_TAG_VALUES_CACHE_ENTRIES)
        return;
    // Evict half the entries (iteration order ≈ random in unordered_map).
    // This preserves ~50% of hot entries vs clearing everything.
    size_t toEvict = tagValuesCache_.size() / 2;
    for (auto it = tagValuesCache_.begin(); it != tagValuesCache_.end() && toEvict > 0;) {
        it = tagValuesCache_.erase(it);
        --toEvict;
    }
}

void NativeIndex::trimSchemaCaches() {
    // fieldsCache_ and tagsCache_ are keyed by measurement name.
    // Clear entirely when over limit — they're repopulated on miss from KV store.
    if (fieldsCache_.size() > MAX_SCHEMA_CACHE_ENTRIES) {
        fieldsCache_.clear();
    }
    if (tagsCache_.size() > MAX_SCHEMA_CACHE_ENTRIES) {
        tagsCache_.clear();
    }
    // fieldTypeValues_ is keyed by "measurement\0field". With many fields per measurement
    // this grows faster than measurement count. Use a higher threshold.
    if (fieldTypeValues_.size() > MAX_SCHEMA_CACHE_ENTRIES * 10) {
        fieldTypeValues_.clear();
        knownFieldTypes_.clear();
    }
    // discoveryCacheGen_ is keyed by measurement name.
    if (discoveryCacheGen_.size() > MAX_SCHEMA_CACHE_ENTRIES) {
        discoveryCacheGen_.clear();
        // Must also clear the discovery LRU to prevent stale entries (with
        // generation 0 from before any invalidation) from aliasing with new
        // entries created after the generation counter reset.
        discoveryCache_.clear();
    }
}

void NativeIndex::trimMeasurementBloomCache() {
    // Bound by BYTES as well as entry count. A bloom is sized from the number of
    // distinct tag values in its measurement, so entries range from 8 KB to
    // megabytes -- an entry count alone says nothing about the memory held. The
    // byte budget is what actually protects the arena; the count cap remains as a
    // cheap guard against pathological numbers of tiny measurements.
    const size_t byteBudget = maxBloomCacheBytes();

    auto totalBytes = [this]() {
        size_t total = 0;
        for (const auto& [key, bloom] : measurementBloomCache_) {
            total += bloom.filterSize() + key.size();
        }
        return total;
    };

    size_t bytes = totalBytes();
    if (measurementBloomCache_.size() <= MAX_BLOOM_CACHE_ENTRIES && bytes <= byteBudget) {
        return;
    }

    // Evict non-dirty entries first; dirty ones still owe a flush.
    for (auto it = measurementBloomCache_.begin(); it != measurementBloomCache_.end();) {
        if (measurementBloomCache_.size() <= MAX_BLOOM_CACHE_ENTRIES && bytes <= byteBudget)
            break;
        if (dirtyMeasurementBlooms_.count(it->first) == 0) {
            bytes -= std::min(bytes, it->second.filterSize() + it->first.size());
            bloomFullyBuilt_.erase(it->first);
            it = measurementBloomCache_.erase(it);
        } else {
            ++it;
        }
    }
}

seastar::future<roaring::Roaring> NativeIndex::buildActiveSeriesBitmap(const std::string& measurement,
                                                                       uint32_t startDay, uint32_t endDay) {
    roaring::Roaring result;
    std::string cacheKey;
    for (uint32_t day = startDay; day <= endDay; ++day) {
        buildDayBitmapCacheKey(cacheKey, measurement, day);
        auto* bitmap = co_await getDayBitmapByKey(cacheKey);
        if (bitmap) {
            result |= *bitmap;
        }
    }
    co_return result;
}

seastar::future<> NativeIndex::removeExpiredDayBitmaps(const std::string& measurement, uint32_t cutoffDay) {
    std::string prefix = ke::encodeDayBitmapPrefix(measurement);
    IndexWriteBatch batch;

    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        uint32_t day = ke::decodeDayFromDayBitmapKey(key);
        if (day < cutoffDay) {
            batch.remove(std::string(key));
        }
        return true;
    });

    // Evict from cache
    std::string cacheKey;
    std::vector<std::string> toEvict;
    for (auto it = dayBitmapCache_.begin(); it != dayBitmapCache_.end(); ++it) {
        auto& k = it->first;
        // Cache key format: "measurement\0day(4B)"
        if (k.size() >= measurement.size() + 1 + 4 && k.compare(0, measurement.size(), measurement) == 0 &&
            k[measurement.size()] == '\0') {
            uint32_t dayBE;
            std::memcpy(&dayBE, k.data() + k.size() - 4, 4);
            uint32_t day = be32toh(dayBE);
            if (day < cutoffDay) {
                toEvict.push_back(k);
            }
        }
    }
    for (const auto& k : toEvict) {
        dayBitmapCache_.erase(k);
    }

    if (!batch.empty()) {
        co_await kvWriteBatch(batch);
    }
}

// Wide-range threshold shared by the time-scoped discovery paths: beyond this
// day span, fall back to non-time-scoped discovery to avoid O(days) bitmap
// lookups (e.g. 7000+ KV gets for 19 years of data).
static constexpr uint32_t MAX_DAY_SCAN = 365;

seastar::future<std::expected<std::vector<IndexBackend::SeriesWithMetadata>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadataTimeScoped(const std::string& measurement,
                                              const std::map<std::string, std::string>& tagFilters,
                                              const std::unordered_set<std::string>& fieldFilter, uint64_t startTimeNs,
                                              uint64_t endTimeNs, size_t maxSeries) {
    uint32_t startDay = ke::dayBucketFromNs(startTimeNs);
    uint32_t endDay = ke::dayBucketFromNs(endTimeNs);

    if (endDay < startDay || endDay - startDay > MAX_DAY_SCAN) {
        co_return co_await findSeriesWithMetadata(measurement, tagFilters, fieldFilter, maxSeries);
    }

    roaring::Roaring activeSeries = co_await buildActiveSeriesBitmap(measurement, startDay, endDay);

    if (activeSeries.isEmpty()) {
        // Check if any day bitmaps exist for this measurement (pre-Phase-3 fallback)
        std::string prefix = ke::encodeDayBitmapPrefix(measurement);
        bool hasDayBitmaps = false;
        co_await kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
            hasDayBitmaps = true;
            return false;  // Stop after first hit
        });
        // Also check dirty cache entries for this measurement
        if (!hasDayBitmaps) {
            std::string measPrefix = measurement;
            measPrefix.push_back('\0');
            for (auto it = dayBitmapCache_.begin(); it != dayBitmapCache_.end(); ++it) {
                if (it->first.size() >= measPrefix.size() && it->first.compare(0, measPrefix.size(), measPrefix) == 0 &&
                    !it->second.bitmap.isEmpty()) {
                    hasDayBitmaps = true;
                    break;
                }
            }
        }
        if (!hasDayBitmaps) {
            // Pre-Phase-3 data — fall back to non-time-scoped discovery
            co_return co_await findSeriesWithMetadata(measurement, tagFilters, fieldFilter, maxSeries);
        }
        // Day bitmaps exist but none in the queried range — no active series
        co_return std::vector<SeriesWithMetadata>{};
    }

    // If tag filters present, intersect with tag bitmap
    if (!tagFilters.empty()) {
        std::string cacheKey;
        if (tagFilters.size() == 1) {
            auto& [tagKey, tagValue] = *tagFilters.begin();
            buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
            auto* tagBitmap = co_await getPostingsBitmapByKey(cacheKey);
            if (!tagBitmap)
                co_return std::vector<SeriesWithMetadata>{};
            activeSeries &= *tagBitmap;
        } else {
            // Multi-tag — incrementally intersect into activeSeries.
            // IMPORTANT: Copy/AND each bitmap immediately because getPostingsBitmapByKey() may
            // insert into bitmapCache_ (a robin_map), which can rehash and invalidate prior pointers.
            roaring::Roaring tagIntersection;
            bool first = true;
            for (const auto& [tagKey, tagValue] : tagFilters) {
                buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
                auto* bmp = co_await getPostingsBitmapByKey(cacheKey);
                if (!bmp)
                    co_return std::vector<SeriesWithMetadata>{};
                if (first) {
                    tagIntersection = *bmp;
                    first = false;
                } else {
                    tagIntersection &= *bmp;
                    if (tagIntersection.isEmpty())
                        break;
                }
            }
            activeSeries &= tagIntersection;
        }
    }

    if (activeSeries.isEmpty()) {
        co_return std::vector<SeriesWithMetadata>{};
    }

    if (maxSeries > 0 && activeSeries.cardinality() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{activeSeries.cardinality(), maxSeries});
    }

    // Single-pass: iterate bitmap → reverse lookup → metadata resolve → filter → emit
    std::vector<SeriesWithMetadata> results;
    results.reserve(activeSeries.cardinality());
    std::string key;
    key.reserve(1 + 16);
    size_t processed = 0;

    for (auto it = activeSeries.begin(); it != activeSeries.end(); ++it) {
        // kvGet below is a ready future on warm cache — yield explicitly.
        // `activeSeries` is an owned local, so this is suspension-safe.
        if ((++processed & 0x3FFF) == 0)
            co_await seastar::coroutine::maybe_yield();
        uint32_t localId = *it;
        if (!localIdMap_.isValid(localId))
            continue;
        // COPY, not reference: this loop suspends at kvGet below, and a
        // concurrent insert's getOrAssign can realloc the underlying vector,
        // dangling any reference returned by getGlobalId.
        SeriesId128 id = localIdMap_.getGlobalId(localId);

        auto cached = seriesMetadataCache_.get(id);
        const SeriesMetadata* meta = nullptr;
        std::optional<SeriesMetadata> decoded;

        if (cached) {
            meta = cached;
        } else {
            key.clear();
            key.push_back(static_cast<char>(SERIES_METADATA));
            id.appendTo(key);
            auto val = co_await kvGet(key);
            if (val.has_value()) {
                decoded.emplace(ke::decodeSeriesMetadata(*val));
                seriesMetadataCache_.put(id, *decoded);
                meta = &*decoded;
            }
        }
        if (!meta)
            continue;
        if (!fieldFilter.empty() && !fieldFilter.count(meta->field))
            continue;
        if (decoded)
            results.push_back({id, std::move(*decoded)});
        else
            results.push_back({id, *meta});
    }

    co_return results;
}

seastar::future<
    std::expected<std::shared_ptr<const std::vector<IndexBackend::SeriesWithMetadata>>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadataTimeScopedCached(const std::string& measurement,
                                                    const std::map<std::string, std::string>& tagFilters,
                                                    const std::unordered_set<std::string>& fieldFilter,
                                                    uint64_t startTimeNs, uint64_t endTimeNs, size_t maxSeries) {
    uint32_t startDay = ke::dayBucketFromNs(startTimeNs);
    uint32_t endDay = ke::dayBucketFromNs(endTimeNs);

    // Wide range: the uncached path falls back to non-time-scoped discovery
    // anyway, so serve it from the (day-agnostic) discovery cache directly.
    if (endDay < startDay || endDay - startDay > MAX_DAY_SCAN) {
        co_return co_await findSeriesWithMetadataCached(measurement, tagFilters, fieldFilter, maxSeries);
    }

    // Day-scoped cache key. Invalidation: the generation embedded in the key
    // bumps on new series (invalidateDiscoveryCache) AND when an existing
    // series first writes into a new day (addChecked in the day-bitmap
    // recording paths) — so cached day-scoped results can never go stale.
    std::string cacheKey = buildDiscoveryCacheKey(measurement, tagFilters, fieldFilter, maxSeries);
    cacheKey += "D:";
    cacheKey += std::to_string(startDay);
    cacheKey += '-';
    cacheKey += std::to_string(endDay);

    auto cached = discoveryCache_.get(cacheKey);
    if (cached)
        co_return *cached;

    auto result = co_await findSeriesWithMetadataTimeScoped(measurement, tagFilters, fieldFilter, startTimeNs,
                                                            endTimeNs, maxSeries);
    if (!result.has_value()) {
        co_return std::unexpected(result.error());
    }

    auto ptr = std::make_shared<const std::vector<SeriesWithMetadata>>(std::move(*result));
    discoveryCache_.put(cacheKey, ptr);
    co_return ptr;
}

// ============================================================================
// Phase 4: Cardinality estimation (HLL + bloom)
// ============================================================================

seastar::future<> NativeIndex::updateHLL(const std::string& measurement, uint32_t localId) {
    std::string key;
    key.reserve(measurement.size() + 1);
    key += measurement;
    key.push_back('\0');

    auto it = hllCache_.find(key);
    if (it == hllCache_.end()) {
        // Try loading from KV
        auto kvKey = ke::encodeCardinalityHLLKey(measurement);
        auto val = co_await kvGet(kvKey);
        // Re-find after co_await: a concurrent update may have created this
        // entry (from the same persisted base) and already added IDs —
        // overwriting it would silently lose those adds.
        it = hllCache_.find(key);
        if (it == hllCache_.end()) {
            if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
                it = hllCache_.try_emplace(key, HyperLogLog::deserialize(*val)).first;
            } else {
                it = hllCache_.try_emplace(key).first;
            }
        }
    }
    it.value().add(localId);
    hllCacheDirty_.insert(key);
}

seastar::future<> NativeIndex::updateTagHLL(const std::string& measurement, const std::string& tagKey,
                                            const std::string& tagValue, uint32_t localId,
                                            const roaring::Roaring* seedFrom) {
    std::string key;
    key.reserve(measurement.size() + 1 + tagKey.size() + 1 + tagValue.size());
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    key.push_back('\0');
    key += tagValue;

    auto it = hllCache_.find(key);
    if (it == hllCache_.end()) {
        // Try loading from KV
        auto kvKey = ke::encodeCardinalityHLLKey(measurement, tagKey, tagValue);
        auto val = co_await kvGet(kvKey);
        // Re-find after co_await (see updateHLL).
        it = hllCache_.find(key);
        if (it == hllCache_.end()) {
            if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
                it = hllCache_.try_emplace(key, HyperLogLog::deserialize(*val)).first;
            } else {
                it = hllCache_.try_emplace(key).first;
                // This sketch is being created only now, because the tag value
                // just crossed the cardinality threshold. Seed it from the exact
                // bitmap: otherwise it would count only the IDs added from here
                // on and under-report by roughly the threshold.
                if (seedFrom != nullptr) {
                    for (uint32_t existingId : *seedFrom) {
                        it.value().add(existingId);
                    }
                }
            }
        }
    }
    it.value().add(localId);
    hllCacheDirty_.insert(key);
}

void NativeIndex::flushDirtyHLLs(IndexWriteBatch& batch) {
    for (const auto& cacheKey : hllCacheDirty_) {
        auto it = hllCache_.find(cacheKey);
        if (it == hllCache_.end() || it->second.empty())
            continue;

        std::string kvKey;
        kvKey.reserve(1 + cacheKey.size());
        kvKey.push_back(static_cast<char>(CARDINALITY_HLL));
        kvKey.append(cacheKey);

        std::string serialized;
        serialized.reserve(HyperLogLog::SERIALIZED_SIZE);
        it->second.serialize(serialized);
        batch.put(kvKey, serialized);
    }
    hllCacheDirty_.clear();
}

seastar::future<> NativeIndex::flushDirtyMeasurementBlooms(IndexWriteBatch& batch) {
    // Swap the dirty set into a local before iterating: the KV prefix scan
    // below suspends, and a concurrent insert doing dirtyMeasurementBlooms_.
    // insert() could rehash the set under our iterator (UAF). Marks added
    // during the suspension land in the (now empty) member and are handled by
    // the next flush — previously the trailing clear() silently dropped them,
    // leaving a stale persisted bloom that hid the new tag combination.
    auto pending = std::exchange(dirtyMeasurementBlooms_, {});
    if (pending.empty()) {
        co_return;
    }

    // Single pass over bitmapCache_ bucketing non-empty entries by dirty
    // measurement (previously O(dirty × cache): one full walk per measurement).
    // Bucket keys are views into `pending` (stable local set); bucket contents
    // are OWNED copies because the per-measurement KV scan below suspends and
    // a concurrent insert may rehash bitmapCache_.
    std::unordered_map<std::string_view, std::vector<std::string>> keysByMeasurement;
    keysByMeasurement.reserve(pending.size());
    for (const auto& measurement : pending) {
        keysByMeasurement.emplace(measurement, std::vector<std::string>{});
    }
    for (auto it = bitmapCache_.begin(); it != bitmapCache_.end(); ++it) {
        auto& key = it->first;
        // Cache key format: "measurement\0tagKey\0tagValue"
        size_t sep = key.find('\0');
        if (sep == std::string::npos || it->second.bitmap.isEmpty())
            continue;
        auto bucket = keysByMeasurement.find(std::string_view(key.data(), sep));
        if (bucket != keysByMeasurement.end()) {
            bucket->second.push_back(key);
        }
    }

    for (const auto& measurement : pending) {
        // Build bloom from all postings bitmap KV keys for this measurement.
        // This lets us short-circuit lookups for non-existent tag combinations.
        BloomFilter bloom(10);  // 10 bits/key → ~1% FPR

        std::string measPrefix = measurement;
        measPrefix.push_back('\0');

        std::string bitmapKvKey;
        for (const auto& key : keysByMeasurement.find(std::string_view(measurement))->second) {
            bitmapKvKey.clear();
            bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
            bitmapKvKey.append(key);
            bloom.addKey(bitmapKvKey);
        }

        // Only scan KV store if we haven't done a full scan for this measurement yet.
        // After getOrCreateSeriesId, the cache contains all bitmap entries for the measurement,
        // so the KV scan only adds entries from prior sessions (before this process opened).
        if (bloomFullyBuilt_.find(measurement) == bloomFullyBuilt_.end()) {
            std::string kvScanPrefix;
            kvScanPrefix.push_back(static_cast<char>(POSTINGS_BITMAP));
            kvScanPrefix.append(measPrefix);
            co_await kvPrefixScan(kvScanPrefix, [&](std::string_view key, std::string_view) {
                bloom.addKey(key);
                return true;
            });
            bloomFullyBuilt_.insert(measurement);
        }

        bloom.build();

        // Serialize and persist
        auto kvKey = ke::encodeMeasurementBloomKey(measurement);
        std::string serialized;
        bloom.serializeTo(serialized);
        batch.put(kvKey, serialized);

        // Update cache
        measurementBloomCache_[measurement] = std::move(bloom);
    }
}

seastar::future<double> NativeIndex::estimateMeasurementCardinality(const std::string& measurement) {
    std::string key;
    key.reserve(measurement.size() + 1);
    key += measurement;
    key.push_back('\0');

    auto it = hllCache_.find(key);
    if (it != hllCache_.end()) {
        co_return it->second.estimate();
    }

    // Try loading from KV
    auto kvKey = ke::encodeCardinalityHLLKey(measurement);
    auto val = co_await kvGet(kvKey);
    // Re-find after co_await: a concurrent updateHLL may have created (and
    // dirtied) this entry — overwriting it with the stale KV version would
    // lose its adds and then persist the stale sketch.
    if (auto post = hllCache_.find(key); post != hllCache_.end()) {
        co_return post->second.estimate();
    }
    if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
        auto hll = HyperLogLog::deserialize(*val);
        double est = hll.estimate();
        hllCache_[key] = std::move(hll);
        co_return est;
    }

    // No HLL — fallback to counting measurement series entries
    std::string prefix = ke::encodeMeasurementSeriesPrefix(measurement);
    size_t count = 0;
    co_await kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
        ++count;
        return true;
    });
    co_return static_cast<double>(count);
}

seastar::future<double> NativeIndex::estimateTagCardinality(const std::string& measurement, const std::string& tagKey,
                                                            const std::string& tagValue) {
    std::string key;
    key.reserve(measurement.size() + 1 + tagKey.size() + 1 + tagValue.size());
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    key.push_back('\0');
    key += tagValue;

    auto it = hllCache_.find(key);
    if (it != hllCache_.end()) {
        co_return it->second.estimate();
    }

    // Try loading HLL from KV
    auto kvKey = ke::encodeCardinalityHLLKey(measurement, tagKey, tagValue);
    auto val = co_await kvGet(kvKey);
    // Re-find after co_await (see estimateMeasurementCardinality).
    if (auto post = hllCache_.find(key); post != hllCache_.end()) {
        co_return post->second.estimate();
    }
    if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
        auto hll = HyperLogLog::deserialize(*val);
        double est = hll.estimate();
        hllCache_[key] = std::move(hll);
        co_return est;
    }

    // Fallback: check roaring bitmap cardinality (exact)
    std::string cacheKey;
    buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
    auto* bitmap = co_await getPostingsBitmapByKey(cacheKey);
    if (bitmap) {
        co_return static_cast<double>(bitmap->cardinality());
    }

    co_return 0.0;
}

// ============================================================================
// indexInsert template (non-virtual convenience method)
// ============================================================================

template <class T>
seastar::future<SeriesId128> NativeIndex::indexInsert(const TimeStarInsert<T>& insert) {
    // insert.seriesId128() is cached by the write handler, so this avoids the
    // buildSeriesKey + rehash and the tag-map copy of the by-value overload.
    SeriesId128 seriesId =
        co_await getOrCreateSeriesId(insert.seriesId128(), insert.measurement, insert.getTags(), insert.field);

    // Phase 3: Record day bitmaps for time-scoped discovery
    auto localIdOpt = localIdMap_.getLocalId(seriesId);
    if (localIdOpt.has_value()) {
        uint32_t localId = *localIdOpt;
        std::string dayCacheKey;
        uint32_t lastDay = UINT32_MAX;
        bool newDayMembership = false;
        for (uint64_t ts : insert.getTimestamps()) {
            uint32_t day = ke::dayBucketFromNs(ts);
            if (day != lastDay) {
                buildDayBitmapCacheKey(dayCacheKey, insert.measurement, day);
                // addChecked: invalidate cached day-scoped discovery when an
                // existing series first appears in a day (see recordDaySpan).
                if ((co_await getOrLoadDayBitmapForInsert(dayCacheKey))->addChecked(localId)) {
                    newDayMembership = true;
                }
                lastDay = day;
            }
        }
        if (newDayMembership) {
            invalidateDiscoveryCache(insert.measurement);
        }
    }

    std::string fieldTypeCacheKey;
    fieldTypeCacheKey.reserve(insert.measurement.size() + 1 + insert.field.size());
    fieldTypeCacheKey = insert.measurement;
    fieldTypeCacheKey.push_back('\0');
    fieldTypeCacheKey += insert.field;
    if (knownFieldTypes_.find(fieldTypeCacheKey) == knownFieldTypes_.end()) {
        // Single source of truth for the type name (matches indexMetadataBatch).
        co_await setFieldType(insert.measurement, insert.field,
                              std::string(timestar::valueTypeName(timestar::valueTypeOf<T>())));
        knownFieldTypes_.insert(fieldTypeCacheKey);
    }

    co_return seriesId;
}

// ============================================================================
// Schema broadcast support
// ============================================================================

seastar::future<SchemaUpdate> NativeIndex::indexMetadataBatchWithSchema(const std::vector<MetadataOp>& ops) {
    // Do NOT reset the accumulator here: concurrent calls interleave inside
    // indexMetadataBatch, and a reset would wipe another call's accumulated
    // schema changes before it moved them out — those deltas would never be
    // broadcast and other shards' caches would stay incomplete until restart.
    // The move below empties the member; whichever concurrent call moves last
    // simply carries the remainder (schema broadcasts are idempotent unions).
    co_await indexMetadataBatch(ops);
    co_return std::exchange(pendingSchemaUpdate_, SchemaUpdate{});
}

// Persist broadcast schema deltas into the LOCAL shard's KV store so every
// shard's KV becomes a complete schema replica. Without this, each shard's KV
// only held MEASUREMENT_FIELDS/TAGS/TAG_VALUES for series it owns; once
// trimSchemaCaches() cleared the broadcast deltas (or the process restarted),
// /fields//tags//tag-values served from a non-owning shard returned partial
// data. Broadcasts are idempotent unions, so the origin shard re-applying its
// own update is harmless.
seastar::future<> NativeIndex::applySchemaUpdate(SchemaUpdate update) {
    IndexWriteBatch batch;

    // --- Fields / tag keys: read-modify-write the schema blobs ---
    // Stage the merged sets and encode them synchronously right before
    // kvWriteBatch below, folding in the live caches at write time so a
    // concurrent update applied during one of our suspensions can't be
    // clobbered by a stale snapshot.
    std::vector<std::pair<std::string, std::set<std::string>>> fieldBlobs;
    fieldBlobs.reserve(update.newFields.size());
    for (const auto& [measurement, fields] : update.newFields) {
        auto val = co_await kvGet(ke::encodeMeasurementFieldsKey(measurement));
        std::set<std::string> merged = val.has_value() ? ke::decodeStringSet(*val) : std::set<std::string>{};
        const size_t persisted = merged.size();
        merged.insert(fields.begin(), fields.end());
        // Re-find the cache after co_await; merge both ways so the cache
        // entry becomes the complete union (fixes the partial-delta-masking
        // bug where a delta-only entry satisfied reads as if complete).
        auto& cached = fieldsCache_[measurement];
        merged.insert(cached.begin(), cached.end());
        cached = merged;
        if (merged.size() != persisted) {
            fieldBlobs.emplace_back(measurement, std::move(merged));
        }
    }

    std::vector<std::pair<std::string, std::set<std::string>>> tagBlobs;
    tagBlobs.reserve(update.newTags.size());
    for (const auto& [measurement, tags] : update.newTags) {
        auto val = co_await kvGet(ke::encodeMeasurementTagsKey(measurement));
        std::set<std::string> merged = val.has_value() ? ke::decodeStringSet(*val) : std::set<std::string>{};
        const size_t persisted = merged.size();
        merged.insert(tags.begin(), tags.end());
        auto& cached = tagsCache_[measurement];  // re-find after co_await
        merged.insert(cached.begin(), cached.end());
        cached = merged;
        if (merged.size() != persisted) {
            tagBlobs.emplace_back(measurement, std::move(merged));
        }
    }

    // --- Tag values: per-value marker keys — no blob RMW needed ---
    for (const auto& [cacheKey, values] : update.newTagValues) {
        auto sep = cacheKey.find('\0');
        if (sep == std::string::npos)
            continue;  // malformed key — skip
        std::string measurement = cacheKey.substr(0, sep);
        std::string tagKey = cacheKey.substr(sep + 1);
        for (const auto& value : values) {
            batch.put(ke::encodeTagValueMarkerKey(measurement, tagKey, value), "");
        }
        // Merge into the cache ONLY when a fully-loaded entry exists. Creating
        // a delta-only entry would satisfy getTagValues as if complete (the
        // partial-delta-masking bug); reads load the full union from KV on
        // demand, which now includes the markers persisted above.
        if (auto it = tagValuesCache_.find(cacheKey); it != tagValuesCache_.end() && !it->second.empty()) {
            it->second.insert(values.begin(), values.end());
        }
    }

    // --- Field types: in-memory caches (persisted by the owning shard) ---
    for (const auto& [cacheKey, type] : update.newFieldTypes) {
        knownFieldTypes_.insert(cacheKey);
        // Store the actual type value so getFieldType() can return it
        // First-write-wins: don't overwrite existing field types
        if (!fieldTypeValues_.contains(cacheKey)) {
            fieldTypeValues_[cacheKey] = type;
        }
    }

    // Encode blobs from the freshest state (staged union ∪ live cache) with no
    // suspension between here and the synchronous memtable apply inside
    // kvWriteBatch — later writers always persist a superset.
    for (auto& [measurement, merged] : fieldBlobs) {
        if (auto it = fieldsCache_.find(measurement); it != fieldsCache_.end()) {
            merged.insert(it->second.begin(), it->second.end());
        }
        batch.put(ke::encodeMeasurementFieldsKey(measurement), ke::encodeStringSet(merged));
    }
    for (auto& [measurement, merged] : tagBlobs) {
        if (auto it = tagsCache_.find(measurement); it != tagsCache_.end()) {
            merged.insert(it->second.begin(), it->second.end());
        }
        batch.put(ke::encodeMeasurementTagsKey(measurement), ke::encodeStringSet(merged));
    }

    if (!batch.empty()) {
        co_await kvWriteBatch(batch);
    }
}

// Explicit template instantiations
template seastar::future<SeriesId128> NativeIndex::indexInsert<double>(const TimeStarInsert<double>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<bool>(const TimeStarInsert<bool>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<std::string>(const TimeStarInsert<std::string>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<int64_t>(const TimeStarInsert<int64_t>& insert);

}  // namespace timestar::index
