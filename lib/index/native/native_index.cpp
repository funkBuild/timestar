#include "native_index.hpp"

#include "../key_encoding.hpp"
#include "tsm.hpp"  // for TSMValueType definition

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <glaze/glaze.hpp>

// Use short namespace alias for key encoding
namespace ke = timestar::index::keys;

static seastar::logger native_index_log("timestar.native_index");

namespace timestar::index {

// ============================================================================
// Iterator adapters for MergeIterator (used by kvPrefixScan)
// ============================================================================

// Wraps MemTable::Iterator (synchronous) as IteratorSource.
class MemTableIteratorSource : public IteratorSource {
public:
    explicit MemTableIteratorSource(MemTable::Iterator iter, int priority)
        : iter_(std::move(iter)), priority_(priority) {}

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

    // Synchronous fast path — no future wrapping
    void seekSync(std::string_view target) override { iter_.seek(target); }
    void seekToFirstSync() override { iter_.seekToFirst(); }
    void nextSync() override { iter_.next(); }

    bool valid() const override { return iter_.valid(); }
    std::string_view key() const override { return iter_.key(); }
    std::string_view value() const override { return iter_.value(); }
    bool isTombstone() const override { return iter_.isTombstone(); }
    int priority() const override { return priority_; }

private:
    MemTable::Iterator iter_;
    int priority_;
};

// Non-owning SSTable iterator source — borrows reader, doesn't own it.
// (Unlike compaction.cpp's SSTableIteratorSource which takes ownership.)
class SSTableBorrowedIteratorSource : public IteratorSource {
public:
    SSTableBorrowedIteratorSource(SSTableReader* reader, int priority)
        : iter_(reader->newIterator()), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override {
        iter_->seek(target);
        return seastar::make_ready_future<>();
    }
    seastar::future<> seekToFirst() override {
        iter_->seekToFirst();
        return seastar::make_ready_future<>();
    }
    seastar::future<> next() override {
        iter_->next();
        return seastar::make_ready_future<>();
    }

    // Synchronous fast path — no future wrapping
    void seekSync(std::string_view target) override { iter_->seek(target); }
    void seekToFirstSync() override { iter_->seekToFirst(); }
    void nextSync() override { iter_->next(); }

    bool valid() const override { return iter_->valid(); }
    std::string_view key() const override { return iter_->key(); }
    std::string_view value() const override { return iter_->value(); }
    bool isTombstone() const override { return false; }  // SSTables don't store tombstones
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

NativeIndex::~NativeIndex() = default;

// ============================================================================
// Lifecycle
// ============================================================================

seastar::future<> NativeIndex::open() {
    indexPath_ = std::filesystem::absolute("shard_" + std::to_string(shardId_) + "/native_index").string();
    std::filesystem::create_directories(indexPath_);

    // Open manifest
    manifest_ = std::make_unique<Manifest>(co_await Manifest::open(indexPath_));

    // Open WAL and replay into MemTable
    memtable_ = std::make_unique<MemTable>();
    wal_ = std::make_unique<IndexWAL>(co_await IndexWAL::open(indexPath_ + "/wal"));
    auto replayed = co_await wal_->replay(*memtable_);
    if (replayed > 0) {
        ::native_index_log.info("Replayed {} WAL records into MemTable", replayed);
    }

    // Open SSTable readers
    co_await refreshSSTables();

    // Create compaction engine
    CompactionConfig compCfg;
    compCfg.blockSize = timestar::config().index.block_size;
    compCfg.bloomBitsPerKey = timestar::config().index.bloom_filter_bits;
    compaction_ = std::make_unique<CompactionEngine>(indexPath_, *manifest_, compCfg);

    // Phase 2: Load or migrate LocalIdMap for roaring bitmap postings
    auto counterKey = ke::encodeLocalIdCounterKey();
    auto counterVal = kvGet(counterKey);
    if (counterVal.has_value()) {
        // Existing local ID mappings — stream directly into LocalIdMap (no intermediate vector)
        uint32_t nextId = ke::decodeLocalId(*counterVal);
        localIdMap_.restoreBegin(nextId, nextId);
        std::string fwdPrefix(1, static_cast<char>(LOCAL_ID_FORWARD));
        kvPrefixScan(fwdPrefix, [&](std::string_view key, std::string_view value) {
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
        migrateToLocalIds(migrationBatch);
        if (!migrationBatch.empty()) {
            co_await kvWriteBatch(migrationBatch);
            lastFlushedLocalId_ = localIdMap_.nextId();  // Migration persisted all IDs
            ::native_index_log.info("Phase 2 migration complete: {} local IDs assigned", localIdMap_.size());
        }
    }

    // HLL sketches and measurement bloom filters are loaded lazily on first access
    // via getOrCreateSeriesId() / estimateMeasurementCardinality() / getPostingsBitmapByKey().
    // This avoids scanning all HLL/bloom KV entries at startup, which can stall for
    // 10K+ measurements.

    ::native_index_log.info("NativeIndex opened at: {} ({} SSTables, {} MemTable entries, {} local IDs)",
                          indexPath_, manifest_->files().size(), memtable_->size(), localIdMap_.size());
}

seastar::future<> NativeIndex::close() {
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

    if (wal_) co_await wal_->close();
    if (manifest_) {
        co_await manifest_->writeSnapshot();
        co_await manifest_->close();
    }

    ::native_index_log.info("NativeIndex closed on shard {}", shardId_);
}

// ============================================================================
// Low-level KV operations
// ============================================================================

std::string NativeIndex::sstFilename(uint64_t fileNumber) {
    char buf[32];
    snprintf(buf, sizeof(buf), "idx_%06lu.sst", fileNumber);
    return indexPath_ + "/" + buf;
}

// Step 4: Incremental SSTable refresh — only opens new files and closes removed ones.
// Existing readers (with warm block caches) are preserved.
seastar::future<> NativeIndex::refreshSSTables() {
    // Build set of file numbers currently in the manifest
    std::set<uint64_t> manifestFiles;
    for (const auto& fileMeta : manifest_->files()) {
        manifestFiles.insert(fileMeta.fileNumber);
    }

    // Close and remove readers for files no longer in the manifest
    for (auto it = sstableReaders_.begin(); it != sstableReaders_.end();) {
        if (manifestFiles.find(it->first) == manifestFiles.end()) {
            co_await it->second->close();
            it = sstableReaders_.erase(it);
        } else {
            ++it;
        }
    }

    // Open readers for new files not yet in the reader map
    for (const auto& fileMeta : manifest_->files()) {
        if (sstableReaders_.find(fileMeta.fileNumber) != sstableReaders_.end()) {
            continue;  // Already open
        }
        auto path = sstFilename(fileMeta.fileNumber);
        if (std::filesystem::exists(path)) {
            auto reader = co_await SSTableReader::open(path, &blockCache_);
            sstableReaders_[fileMeta.fileNumber] = std::move(reader);
        }
    }
}

// Synchronous — all data is in memory (MemTable + cached SSTables).
// No coroutine frame overhead.
// Synchronous — all data is in memory (MemTable + cached SSTables).
std::optional<std::string> NativeIndex::kvGet(std::string_view key) {
    // 1. Check active MemTable first (newest)
    auto memResult = memtable_->get(key);
    if (memResult.has_value()) {
        return std::string(*memResult);
    }
    if (memtable_->isTombstone(key)) {
        return std::nullopt;
    }

    // 2. Check immutable MemTable (being flushed)
    if (immutableMemtable_) {
        auto immResult = immutableMemtable_->get(key);
        if (immResult.has_value()) {
            return std::string(*immResult);
        }
        if (immutableMemtable_->isTombstone(key)) {
            return std::nullopt;
        }
    }

    // 3. Check SSTables (newest to oldest = reverse map order by file number)
    for (auto it = sstableReaders_.rbegin(); it != sstableReaders_.rend(); ++it) {
        auto result = it->second->get(key);
        if (result.has_value()) {
            return result;
        }
    }

    return std::nullopt;
}

// Step 8: Existence check without copying the value.
bool NativeIndex::kvExists(std::string_view key) {
    // 1. Check active MemTable first (newest)
    auto memResult = memtable_->get(key);
    if (memResult.has_value()) return true;
    if (memtable_->isTombstone(key)) return false;

    // 2. Check immutable MemTable (being flushed)
    if (immutableMemtable_) {
        auto immResult = immutableMemtable_->get(key);
        if (immResult.has_value()) return true;
        if (immutableMemtable_->isTombstone(key)) return false;
    }

    // 3. Check SSTables (newest to oldest)
    for (auto it = sstableReaders_.rbegin(); it != sstableReaders_.rend(); ++it) {
        if (it->second->contains(key)) return true;
    }

    return false;
}

seastar::future<> NativeIndex::kvPut(const std::string& key, const std::string& value) {
    IndexWriteBatch batch;
    batch.put(key, value);
    co_await wal_->append(batch);
    memtable_->put(key, value);
    co_await maybeFlushMemTable();
}

seastar::future<> NativeIndex::kvDelete(const std::string& key) {
    IndexWriteBatch batch;
    batch.remove(key);
    co_await wal_->append(batch);
    memtable_->remove(key);
}

seastar::future<> NativeIndex::kvWriteBatch(const IndexWriteBatch& batch) {
    co_await wal_->append(batch);
    batch.applyTo(*memtable_);
    co_await maybeFlushMemTable();
}

// Synchronous streaming kvPrefixScan using MergeIterator.
// All data is in memory — no coroutine overhead.
void NativeIndex::kvPrefixScan(const std::string& prefix, ScanCallback fn) {
    // Count valid sources and track if we can use the single-source fast path
    bool memtableEmpty = memtable_->empty();
    bool immutableEmpty = !immutableMemtable_ || immutableMemtable_->empty();
    size_t sstCount = sstableReaders_.size();

    // Fast path: single SSTable, empty memtables — skip MergeIterator entirely.
    // This is the common case after compaction.
    if (memtableEmpty && immutableEmpty && sstCount == 1) {
        auto iter = sstableReaders_.begin()->second->newIterator();
        iter->seek(prefix);
        while (iter->valid()) {
            auto key = iter->key();
            if (key.size() < prefix.size() ||
                std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
                break;
            }
            if (!fn(key, iter->value())) break;
            iter->next();
        }
        return;
    }

    // Fast path: single MemTable, no SSTables — skip MergeIterator entirely.
    if (!memtableEmpty && immutableEmpty && sstCount == 0) {
        auto iter = memtable_->newIterator();
        iter.seek(prefix);
        while (iter.valid()) {
            auto key = iter.key();
            if (key.size() < prefix.size() ||
                std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
                break;
            }
            // Skip tombstones in single-source scan
            if (!iter.isTombstone()) {
                if (!fn(key, iter.value())) break;
            }
            iter.next();
        }
        return;
    }

    // General path: multiple sources — use MergeIterator
    std::vector<std::unique_ptr<IteratorSource>> sources;
    sources.push_back(std::make_unique<MemTableIteratorSource>(memtable_->newIterator(), 0));

    int nextPriority = 1;
    if (immutableMemtable_) {
        sources.push_back(std::make_unique<MemTableIteratorSource>(immutableMemtable_->newIterator(), nextPriority));
        ++nextPriority;
    }

    // Step 4: Iterate map in ascending file number order (oldest first → highest priority)
    int sstPriority = nextPriority + static_cast<int>(sstableReaders_.size());
    for (auto& [fileNum, reader] : sstableReaders_) {
        sources.push_back(std::make_unique<SSTableBorrowedIteratorSource>(reader.get(), sstPriority));
        --sstPriority;
    }

    MergeIterator merger(std::move(sources));
    merger.seekSync(prefix);

    while (merger.valid()) {
        auto key = merger.key();
        if (key.size() < prefix.size() ||
            std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
            break;
        }
        if (!fn(key, merger.value())) break;
        merger.nextSync();
    }
}

seastar::future<> NativeIndex::waitForFlush() {
    if (flushFuture_) {
        co_await std::move(*flushFuture_);
        flushFuture_.reset();
    }
}

seastar::future<> NativeIndex::maybeFlushMemTable() {
    auto usage = memtable_->approximateMemoryUsage();
    auto threshold = timestar::config().index.write_buffer_size;
    if (usage < threshold) co_return;

    ::native_index_log.info("Flushing MemTable: {} bytes >= {} threshold", usage, threshold);

    // Phase 2+3+4: Flush dirty bitmaps, day bitmaps, HLLs, and blooms before memtable swap
    if (!bitmapCache_.empty() || !dayBitmapCache_.empty() || !hllCacheDirty_.empty() || !dirtyMeasurementBlooms_.empty()) {
        IndexWriteBatch postingsBatch;
        flushDirtyBitmaps(postingsBatch);
        flushDirtyDayBitmaps(postingsBatch);
        flushDirtyHLLs(postingsBatch);
        flushDirtyMeasurementBlooms(postingsBatch);
        if (!postingsBatch.empty()) {
            co_await wal_->append(postingsBatch);
            postingsBatch.applyTo(*memtable_);
        }
        // Evict non-dirty cache entries to bound memory growth
        trimBitmapCache();
        trimDayBitmapCache();
        trimHllCache();           // Step 7
        trimTagValuesCache();     // Step 6
        trimSchemaCaches();
        trimMeasurementBloomCache();
    }

    // If a previous flush is still in progress, wait for it (like LevelDB's imm_ check)
    co_await waitForFlush();

    // Swap: active memtable becomes immutable, create fresh active
    immutableMemtable_ = std::move(memtable_);
    memtable_ = std::make_unique<MemTable>();

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
    // Phase 2+3+4: Flush dirty bitmaps, day bitmaps, HLLs, and blooms before checking empty
    if (!bitmapCache_.empty() || !dayBitmapCache_.empty() || !hllCacheDirty_.empty() || !dirtyMeasurementBlooms_.empty()) {
        IndexWriteBatch postingsBatch;
        flushDirtyBitmaps(postingsBatch);
        flushDirtyDayBitmaps(postingsBatch);
        flushDirtyHLLs(postingsBatch);
        flushDirtyMeasurementBlooms(postingsBatch);
        if (!postingsBatch.empty()) {
            co_await wal_->append(postingsBatch);
            postingsBatch.applyTo(*memtable_);
        }
        trimBitmapCache();
        trimDayBitmapCache();
        trimHllCache();           // Step 7
        trimTagValuesCache();     // Step 6
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
    memtable_ = std::make_unique<MemTable>();

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
    if (!immutableMemtable_ || immutableMemtable_->empty()) co_return;

    ::native_index_log.info("Flushing immutable MemTable: {} entries, {} approx bytes",
                            immutableMemtable_->size(), immutableMemtable_->approximateMemoryUsage());

    // Count live (non-tombstone) entries
    size_t liveCount = 0;
    {
        auto countIt = immutableMemtable_->newIterator();
        countIt.seekToFirst();
        while (countIt.valid()) {
            if (!countIt.isTombstone()) ++liveCount;
            countIt.next();
        }
    }
    ::native_index_log.info("Immutable MemTable has {} live entries (excluding tombstones)", liveCount);
    if (liveCount == 0) {
        ::native_index_log.info("Skipping SSTable flush — no live entries");
        co_return;
    }

    // Write immutable MemTable to a new SSTable
    uint64_t fileNum = manifest_->nextFileNumber();
    auto path = sstFilename(fileNum);
    auto writer = co_await SSTableWriter::create(path, timestar::config().index.block_size,
                                                  timestar::config().index.bloom_filter_bits);

    auto it = immutableMemtable_->newIterator();
    it.seekToFirst();
    size_t addCount = 0;
    while (it.valid()) {
        if (!it.isTombstone()) {
            writer.add(it.key(), it.value());
            // Periodically flush to disk to bound memory (every 1024 entries)
            if (++addCount % 1024 == 0) {
                co_await writer.flushPending();
            }
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
// Series cache (same two-generation design as LevelDBIndex)
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
    // Compute SeriesId128 directly from components — no encoded string needed.
    // This avoids 2+2M temporary string allocations from escapeKeyComponent().
    SeriesId128 seriesId = SeriesId128::fromComponents(measurement, tags, field);

    // Fast path: already indexed (no string allocation needed)
    if (seriesCacheContains(seriesId)) {
        co_return seriesId;
    }

    // Step 8: Check if series exists in storage (existence check only — no value copy)
    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    if (kvExists(metaKey)) {
        seriesCacheInsert(seriesId);
        co_return seriesId;
    }

    // New series — create ALL index entries in a single batch (Stage 4).
    // Previously this was split across getOrCreateSeriesId + addFieldsAndTags
    // = 2-6 separate WAL writes. Now it's exactly 1.
    IndexWriteBatch batch;
    batch.reserve(4 + tags.size());  // metadata + 2 indexes + local ID forward + per-tag bitmap

    // Series metadata
    SeriesMetadata metadata{measurement, tags, field};
    batch.put(metaKey, ke::encodeSeriesMetadata(metadata));

    // Measurement series index
    batch.put(ke::encodeMeasurementSeriesKey(measurement, seriesId), "");

    // Measurement field series index
    batch.put(ke::encodeMeasurementFieldSeriesKey(measurement, field, seriesId), "");

    // Phase 2: Assign local ID (forward mapping persisted in batch on flush)
    uint32_t localId = localIdMap_.getOrAssign(seriesId);

    // Phase 2: Add local ID to dirty postings bitmaps (TAG_INDEX/GROUP_BY_INDEX removed in Phase 3)
    std::string bitmapCacheKey;
    bitmapCacheKey.reserve(measurement.size() + 1 + 32 + 1 + 32);
    for (const auto& [tagKey, tagValue] : tags) {
        buildBitmapCacheKey(bitmapCacheKey, measurement, tagKey, tagValue);
        auto& bitmap = getOrLoadBitmapForInsert(bitmapCacheKey);
        bitmap.add(localId);
    }

    // Phase 4: Update cardinality HLLs
    updateHLL(measurement, localId);
    for (const auto& [tagKey, tagValue] : tags) {
        updateTagHLL(measurement, tagKey, tagValue, localId);
    }
    // Mark measurement bloom for rebuild on next flush
    dirtyMeasurementBlooms_.insert(measurement);

    // --- Field metadata (was addField) ---
    auto& fieldCache = fieldsCache_[measurement];
    if (fieldCache.empty()) {
        auto val = kvGet(ke::encodeMeasurementFieldsKey(measurement));
        if (val.has_value()) fieldCache = ke::decodeStringSet(*val);
    }
    if (fieldCache.insert(field).second) {
        batch.put(ke::encodeMeasurementFieldsKey(measurement), ke::encodeStringSet(fieldCache));
        pendingSchemaUpdate_.newFields[measurement].insert(field);
    }

    // --- Tag metadata (was addTag for each tag) ---
    auto& tagKeysCache = tagsCache_[measurement];
    if (tagKeysCache.empty()) {
        auto val = kvGet(ke::encodeMeasurementTagsKey(measurement));
        if (val.has_value()) tagKeysCache = ke::decodeStringSet(*val);
    }
    // Reusable buffer for tag-values cache key
    std::string tvCacheKey;
    tvCacheKey.reserve(measurement.size() + 1 + 32);
    for (const auto& [tagKey, tagValue] : tags) {
        if (tagKeysCache.insert(tagKey).second) {
            batch.put(ke::encodeMeasurementTagsKey(measurement), ke::encodeStringSet(tagKeysCache));
            pendingSchemaUpdate_.newTags[measurement].insert(tagKey);
        }

        tvCacheKey.clear();
        tvCacheKey += measurement;
        tvCacheKey.push_back('\0');
        tvCacheKey += tagKey;
        auto& tagVals = tagValuesCache_[tvCacheKey];
        // Build KV key once, reuse for both get and put
        std::string tagValuesKvKey = ke::encodeTagValuesKey(measurement, tagKey);
        if (tagVals.empty()) {
            auto val = kvGet(tagValuesKvKey);
            if (val.has_value()) tagVals = ke::decodeStringSet(*val);
        }
        if (tagVals.insert(tagValue).second) {
            batch.put(std::move(tagValuesKvKey), ke::encodeStringSet(tagVals));
            pendingSchemaUpdate_.newTagValues[tvCacheKey].insert(tagValue);
        }
    }

    // Single WAL write for everything
    co_await kvWriteBatch(batch);
    seriesCacheInsert(seriesId);

    // Pre-populate metadata cache — avoids decode cost on future lookups.
    // The metadata object is already constructed; move it into cache.
    seriesMetadataCache_.put(seriesId, std::move(metadata));

    // Invalidate discovery cache for this measurement
    invalidateDiscoveryCache(measurement);

    // Step 5: measurementSeriesCache_ removed — no update needed

    co_return seriesId;
}

seastar::future<std::optional<SeriesId128>> NativeIndex::getSeriesId(
    const std::string& measurement, const std::map<std::string, std::string>& tags, const std::string& field) {
    std::string seriesKeyStr = ke::encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

    // Step 8: Existence check without copying the value
    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    if (kvExists(metaKey)) {
        co_return seriesId;
    }
    co_return std::nullopt;
}

seastar::future<std::optional<SeriesMetadata>> NativeIndex::getSeriesMetadata(const SeriesId128& seriesId) {
    // Check metadata cache
    auto cached = seriesMetadataCache_.get(seriesId);
    if (cached) co_return *cached;

    auto key = ke::encodeSeriesMetadataKey(seriesId);
    auto val = kvGet(key);
    if (!val.has_value()) co_return std::nullopt;

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

        auto val = kvGet(key);
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

seastar::future<> NativeIndex::addField(const std::string& measurement, const std::string& field) {
    auto& cached = fieldsCache_[measurement];
    if (cached.count(field)) co_return;

    // Load from storage if cache is empty
    if (cached.empty()) {
        auto key = ke::encodeMeasurementFieldsKey(measurement);
        auto val = kvGet(key);
        if (val.has_value()) {
            cached = ke::decodeStringSet(*val);
        }
    }

    if (cached.insert(field).second) {
        co_await kvPut(ke::encodeMeasurementFieldsKey(measurement), ke::encodeStringSet(cached));
    }
}

seastar::future<> NativeIndex::addTag(const std::string& measurement, const std::string& tagKey,
                                       const std::string& tagValue) {
    // Add tag key
    auto& tagKeys = tagsCache_[measurement];
    if (tagKeys.empty()) {
        auto key = ke::encodeMeasurementTagsKey(measurement);
        auto val = kvGet(key);
        if (val.has_value()) {
            tagKeys = ke::decodeStringSet(*val);
        }
    }

    bool tagKeyNew = tagKeys.insert(tagKey).second;

    // Add tag value — build tvCacheKey without intermediate string(1,'\0') alloc
    std::string tvCacheKey;
    tvCacheKey.reserve(measurement.size() + 1 + tagKey.size());
    tvCacheKey += measurement;
    tvCacheKey.push_back('\0');
    tvCacheKey += tagKey;
    auto& tagVals = tagValuesCache_[tvCacheKey];
    std::string tagValuesKvKey = ke::encodeTagValuesKey(measurement, tagKey);
    if (tagVals.empty()) {
        auto val = kvGet(tagValuesKvKey);
        if (val.has_value()) {
            tagVals = ke::decodeStringSet(*val);
        }
    }

    bool tagValueNew = tagVals.insert(tagValue).second;

    if (tagKeyNew || tagValueNew) {
        IndexWriteBatch batch;
        if (tagKeyNew) {
            batch.put(ke::encodeMeasurementTagsKey(measurement), ke::encodeStringSet(tagKeys));
        }
        if (tagValueNew) {
            batch.put(std::move(tagValuesKvKey), ke::encodeStringSet(tagVals));
        }
        co_await kvWriteBatch(batch);
    }
}

seastar::future<> NativeIndex::addFieldsAndTags(const std::string& measurement, const std::string& field,
                                                  const std::map<std::string, std::string>& tags) {
    co_await addField(measurement, field);
    for (const auto& [tagKey, tagValue] : tags) {
        co_await addTag(measurement, tagKey, tagValue);
    }
}

seastar::future<> NativeIndex::setFieldType(const std::string& measurement, const std::string& field,
                                              const std::string& type) {
    std::string cacheKey = measurement + std::string(1, '\0') + field;
    // Step 8: Check cache first, then use kvExists (no value copy needed)
    if (knownFieldTypes_.count(cacheKey)) co_return;

    co_await kvPut(ke::encodeFieldTypeKey(measurement, field), type);
    knownFieldTypes_.insert(cacheKey);
    fieldTypeValues_[cacheKey] = type;
    pendingSchemaUpdate_.newFieldTypes[cacheKey] = type;
}

seastar::future<std::string> NativeIndex::getFieldType(const std::string& measurement, const std::string& field) {
    // Check in-memory cache first (populated by local indexing and schema broadcast)
    std::string cacheKey = measurement + std::string(1, '\0') + field;
    auto it = fieldTypeValues_.find(cacheKey);
    if (it != fieldTypeValues_.end()) {
        co_return it->second;
    }
    // Fall back to KV store
    auto val = kvGet(ke::encodeFieldTypeKey(measurement, field));
    if (val.has_value()) {
        fieldTypeValues_[cacheKey] = *val;
    }
    co_return val.value_or("");
}

seastar::future<std::set<std::string>> NativeIndex::getAllMeasurements() {
    std::set<std::string> result;

    // Include measurements from cache (populated by local indexing and schema broadcast)
    for (const auto& [measurement, _] : fieldsCache_) {
        result.insert(measurement);
    }

    // Also scan KV store for measurements not yet in cache
    std::string prefix(1, static_cast<char>(MEASUREMENT_FIELDS));
    kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        result.insert(std::string(key.substr(1)));  // Skip prefix byte
        return true;
    });
    co_return result;
}

seastar::future<std::set<std::string>> NativeIndex::getFields(const std::string& measurement) {
    auto& cached = fieldsCache_[measurement];
    if (!cached.empty()) co_return cached;

    auto val = kvGet(ke::encodeMeasurementFieldsKey(measurement));
    if (val.has_value()) {
        cached = ke::decodeStringSet(*val);
    }
    co_return cached;
}

seastar::future<std::set<std::string>> NativeIndex::getTags(const std::string& measurement) {
    auto& cached = tagsCache_[measurement];
    if (!cached.empty()) co_return cached;

    auto val = kvGet(ke::encodeMeasurementTagsKey(measurement));
    if (val.has_value()) {
        cached = ke::decodeStringSet(*val);
    }
    co_return cached;
}

seastar::future<std::set<std::string>> NativeIndex::getTagValues(const std::string& measurement,
                                                                   const std::string& tagKey) {
    std::string tvCacheKey = measurement + std::string(1, '\0') + tagKey;
    auto& cached = tagValuesCache_[tvCacheKey];
    if (!cached.empty()) co_return cached;

    auto val = kvGet(ke::encodeTagValuesKey(measurement, tagKey));
    if (val.has_value()) {
        cached = ke::decodeStringSet(*val);
    }
    co_return cached;
}

seastar::future<> NativeIndex::indexMetadataBatch(const std::vector<MetadataOp>& ops) {
    if (ops.empty()) co_return;

    for (const auto& op : ops) {
        std::string typeStr;
        switch (op.valueType) {
            case TSMValueType::Float: typeStr = "float"; break;
            case TSMValueType::Boolean: typeStr = "boolean"; break;
            case TSMValueType::String: typeStr = "string"; break;
            case TSMValueType::Integer: typeStr = "integer"; break;
            default: break;
        }

        co_await getOrCreateSeriesId(op.measurement, op.tags, op.fieldName);
        if (!typeStr.empty()) {
            co_await setFieldType(op.measurement, op.fieldName, typeStr);
        }
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

    // Multi-tag: load all bitmaps, start intersection with the smallest to minimize work.
    std::string cacheKey;
    struct TagBitmap {
        const roaring::Roaring* bmp;
    };
    std::vector<TagBitmap> bitmaps;
    bitmaps.reserve(tagFilters.size());

    for (const auto& [tagKey, tagValue] : tagFilters) {
        buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
        auto* bitmap = getPostingsBitmapByKey(cacheKey);
        if (!bitmap) co_return std::vector<SeriesId128>{};
        bitmaps.push_back({bitmap});
    }

    // Sort by cardinality ascending — smallest first minimizes AND work
    std::sort(bitmaps.begin(), bitmaps.end(),
              [](const TagBitmap& a, const TagBitmap& b) { return a.bmp->cardinality() < b.bmp->cardinality(); });

    // Copy smallest bitmap as accumulator, AND the rest
    roaring::Roaring result = *bitmaps[0].bmp;
    for (size_t i = 1; i < bitmaps.size() && !result.isEmpty(); ++i) {
        result &= *bitmaps[i].bmp;
    }

    // Convert bitmap to SeriesId128 vector via reverse lookup
    std::vector<SeriesId128> res;
    res.reserve(result.cardinality());
    for (auto it = result.begin(); it != result.end(); ++it) {
        uint32_t localId = *it;
        if (localId < localIdMap_.nextId()) {
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
        for (const auto& id : seriesIds) {
            auto cached = seriesMetadataCache_.get(id);
            const SeriesMetadata* meta = nullptr;
            std::optional<SeriesMetadata> decoded;
            if (cached) {
                meta = cached;
            } else {
                key.clear();
                key.push_back(static_cast<char>(SERIES_METADATA));
                id.appendTo(key);
                auto val = kvGet(key);
                if (val.has_value()) {
                    decoded.emplace(ke::decodeSeriesMetadata(*val));
                    seriesMetadataCache_.put(id, *decoded);
                    meta = &*decoded;
                }
            }
            if (!meta) continue;
            if (!fieldFilter.empty() && !fieldFilter.count(meta->field)) continue;
            if (decoded) results.push_back({id, std::move(*decoded)});
            else results.push_back({id, *meta});
        }
        co_return results;
    }

    // Tag-filtered: build bitmap intersection, then iterate bitmap directly
    // resolving metadata inline (single pass, no intermediate vector).
    std::string cacheKey;
    const roaring::Roaring* singleBitmap = nullptr;
    std::optional<roaring::Roaring> intersected;

    if (tagFilters.size() == 1) {
        // Single tag — iterate const bitmap directly, zero copies
        auto& [tagKey, tagValue] = *tagFilters.begin();
        buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
        singleBitmap = getPostingsBitmapByKey(cacheKey);
        if (!singleBitmap) co_return std::vector<SeriesWithMetadata>{};
    } else {
        // Multi-tag — build intersection (smallest-first)
        struct TagBitmap { const roaring::Roaring* bmp; };
        std::vector<TagBitmap> bitmaps;
        bitmaps.reserve(tagFilters.size());
        for (const auto& [tagKey, tagValue] : tagFilters) {
            buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
            auto* bmp = getPostingsBitmapByKey(cacheKey);
            if (!bmp) co_return std::vector<SeriesWithMetadata>{};
            bitmaps.push_back({bmp});
        }
        std::sort(bitmaps.begin(), bitmaps.end(),
                  [](const TagBitmap& a, const TagBitmap& b) { return a.bmp->cardinality() < b.bmp->cardinality(); });
        intersected = *bitmaps[0].bmp;
        for (size_t i = 1; i < bitmaps.size() && !intersected->isEmpty(); ++i) {
            *intersected &= *bitmaps[i].bmp;
        }
    }

    const roaring::Roaring& bitmap = singleBitmap ? *singleBitmap : *intersected;

    if (maxSeries > 0 && bitmap.cardinality() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{bitmap.cardinality(), maxSeries});
    }

    // Single-pass: iterate bitmap → reverse lookup → metadata resolve → filter → emit
    std::vector<SeriesWithMetadata> results;
    results.reserve(bitmap.cardinality());
    std::string key;
    key.reserve(1 + 16);

    for (auto it = bitmap.begin(); it != bitmap.end(); ++it) {
        uint32_t localId = *it;
        if (localId >= localIdMap_.nextId()) continue;
        const SeriesId128& id = localIdMap_.getGlobalId(localId);

        auto cached = seriesMetadataCache_.get(id);
        const SeriesMetadata* meta = nullptr;
        std::optional<SeriesMetadata> decoded;

        if (cached) {
            meta = cached;
        } else {
            key.clear();
            key.push_back(static_cast<char>(SERIES_METADATA));
            id.appendTo(key);
            auto val = kvGet(key);
            if (val.has_value()) {
                decoded.emplace(ke::decodeSeriesMetadata(*val));
                seriesMetadataCache_.put(id, *decoded);
                meta = &*decoded;
            }
        }
        if (!meta) continue;
        if (!fieldFilter.empty() && !fieldFilter.count(meta->field)) continue;
        if (decoded) results.push_back({id, std::move(*decoded)});
        else results.push_back({id, *meta});
    }

    co_return results;
}

seastar::future<std::expected<std::shared_ptr<const std::vector<IndexBackend::SeriesWithMetadata>>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadataCached(const std::string& measurement,
                                           const std::map<std::string, std::string>& tagFilters,
                                           const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    // Build cache key with generation counter to avoid O(N) prefix scan on invalidation
    uint64_t gen = discoveryCacheGen_[measurement];
    std::string cacheKey = measurement;
    cacheKey += '\0';
    cacheKey += std::to_string(gen);
    cacheKey += '\0';
    for (const auto& [k, v] : tagFilters) {
        cacheKey += k;
        cacheKey += '=';
        cacheKey += v;
        cacheKey += '\0';
    }
    cacheKey += "F:";
    for (const auto& f : fieldFilter) {
        cacheKey += f;
        cacheKey += ',';
    }

    auto cached = discoveryCache_.get(cacheKey);
    if (cached) co_return *cached;

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

size_t NativeIndex::getMetadataCacheSize() const { return seriesMetadataCache_.size(); }
size_t NativeIndex::getMetadataCacheBytes() const { return seriesMetadataCache_.currentBytes(); }
size_t NativeIndex::getDiscoveryCacheSize() const { return discoveryCache_.size(); }
size_t NativeIndex::getDiscoveryCacheBytes() const { return discoveryCache_.currentBytes(); }

// ============================================================================
// Tag queries
// ============================================================================

seastar::future<std::vector<SeriesId128>> NativeIndex::findSeriesByTag(const std::string& measurement,
                                                                        const std::string& tagKey,
                                                                        const std::string& tagValue,
                                                                        size_t maxSeries) {
    // Phase 2: Load roaring bitmap (const ref, no copy) and reverse-lookup to global IDs
    std::string cacheKey;
    buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
    auto* bitmap = getPostingsBitmapByKey(cacheKey);
    if (!bitmap) co_return std::vector<SeriesId128>{};

    std::vector<SeriesId128> result;
    result.reserve(bitmap->cardinality());
    for (auto it = bitmap->begin(); it != bitmap->end(); ++it) {
        uint32_t localId = *it;
        if (localId < localIdMap_.nextId()) {
            result.push_back(localIdMap_.getGlobalId(localId));
        }
        if (maxSeries > 0 && result.size() > maxSeries) break;
    }

    co_return result;
}

seastar::future<std::vector<SeriesId128>> NativeIndex::findSeriesByTagPattern(const std::string& measurement,
                                                                               const std::string& tagKey,
                                                                               const std::string& scopeValue,
                                                                               size_t maxSeries) {
    // For simplicity, treat patterns as exact match for now.
    // Full wildcard/regex support can be added later.
    co_return co_await findSeriesByTag(measurement, tagKey, scopeValue, maxSeries);
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

    // Collect bitmaps by tagValue — use const pointers to avoid copies
    std::map<std::string, const roaring::Roaring*> bitmapsByValue;

    // First: scan persisted KV for all bitmaps with this prefix.
    // For each, check if cache has a dirty override; if so, use the cache version.
    kvPrefixScan(bitmapPrefix, [&](std::string_view key, std::string_view value) {
        auto tagValue = key.substr(bitmapPrefix.size());
        if (tagValue.empty() || value.empty()) return true;
        std::string fullCacheKey = cachePrefix + std::string(tagValue);
        auto cacheIt = bitmapCache_.find(fullCacheKey);
        if (cacheIt != bitmapCache_.end()) {
            // Cache has this entry (dirty or previously loaded) — use it
            bitmapsByValue[std::string(tagValue)] = &cacheIt->second.bitmap;
        } else {
            // Not in cache — load from KV into cache
            auto& entry = bitmapCache_[std::move(fullCacheKey)];
            entry.bitmap = roaring::Roaring::readSafe(value.data(), value.size());
            entry.dirty = false;
            bitmapsByValue[std::string(tagValue)] = &entry.bitmap;
        }
        return true;
    });

    // Second: add any dirty cache entries that don't have a KV counterpart
    // (new tag values inserted since last flush). Only scan dirty entries.
    for (auto cacheIt = bitmapCache_.begin(); cacheIt != bitmapCache_.end(); ++cacheIt) {
        if (!cacheIt->second.dirty) continue;
        auto& postingsKey = cacheIt->first;
        if (postingsKey.size() > cachePrefix.size() &&
            postingsKey.compare(0, cachePrefix.size(), cachePrefix) == 0) {
            std::string tagValue = postingsKey.substr(cachePrefix.size());
            if (bitmapsByValue.find(tagValue) == bitmapsByValue.end()) {
                bitmapsByValue[tagValue] = &cacheIt->second.bitmap;
            }
        }
    }

    // Reverse-lookup to global IDs
    std::map<std::string, std::vector<SeriesId128>> result;
    for (auto& [tagValue, bitmapPtr] : bitmapsByValue) {
        if (!bitmapPtr || bitmapPtr->isEmpty()) continue;
        auto& vec = result[tagValue];
        vec.reserve(bitmapPtr->cardinality());
        for (auto it = bitmapPtr->begin(); it != bitmapPtr->end(); ++it) {
            uint32_t localId = *it;
            if (localId < localIdMap_.nextId()) {
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
    value += std::to_string(stats.minTime);
    value.push_back('\0');
    value += std::to_string(stats.maxTime);
    value.push_back('\0');
    value += std::to_string(stats.pointCount);

    co_await kvPut(key, value);
}

seastar::future<std::optional<IndexFieldStats>> NativeIndex::getFieldStats(const SeriesId128& seriesId,
                                                                            const std::string& field) {
    std::string key;
    key.push_back(FIELD_STATS);
    seriesId.appendTo(key);
    key.push_back('\0');
    key += field;

    auto val = kvGet(key);
    if (!val.has_value()) co_return std::nullopt;

    IndexFieldStats stats;
    std::string_view data(*val);
    size_t pos = 0;

    auto nextField = [&]() -> std::string_view {
        if (pos >= data.size()) return {};
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
    stats.minTime = std::stoll(std::string(nextField()));
    stats.maxTime = std::stoll(std::string(nextField()));
    stats.pointCount = std::stoull(std::string(nextField()));

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

    kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        if (key.size() >= prefix.size() + 16) {
            result.push_back(SeriesId128::fromBytes(key.data() + prefix.size(), 16));
        }
        if (result.size() > effectiveMax) return false;
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

void NativeIndex::setMaxSeriesCacheSize(size_t maxSize) { maxSeriesCacheSize_ = maxSize; }
size_t NativeIndex::getMaxSeriesCacheSize() const { return maxSeriesCacheSize_; }
size_t NativeIndex::getSeriesCacheSize() const { return indexedSeriesCache_.size(); }

seastar::future<> NativeIndex::rebuildMeasurementSeriesIndex() {
    // No-op for NativeIndex — the MEASUREMENT_SERIES index is always maintained.
    co_return;
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
    auto val = kvGet(key);
    if (!val.has_value()) co_return std::nullopt;

    RetentionPolicy policy;
    auto ec = glz::read_json(policy, *val);
    if (ec) co_return std::nullopt;
    co_return policy;
}

seastar::future<std::vector<RetentionPolicy>> NativeIndex::getAllRetentionPolicies() {
    std::string prefix(1, static_cast<char>(RETENTION_POLICY));
    std::vector<RetentionPolicy> result;

    kvPrefixScan(prefix, [&](std::string_view, std::string_view value) {
        RetentionPolicy policy;
        std::string valStr(value);
        auto ec = glz::read_json(policy, valStr);
        if (!ec) {
            result.push_back(std::move(policy));
        }
        return true;
    });

    co_return result;
}

seastar::future<bool> NativeIndex::deleteRetentionPolicy(const std::string& measurement) {
    auto key = ke::encodeRetentionPolicyKey(measurement);
    auto existing = kvGet(key);
    if (!existing.has_value()) co_return false;

    co_await kvDelete(key);
    co_return true;
}

// ============================================================================
// Debug/maintenance
// ============================================================================

seastar::future<size_t> NativeIndex::getSeriesCount() {
    size_t count = 0;
    std::string prefix(1, static_cast<char>(SERIES_METADATA));
    kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
        ++count;
        return true;
    });
    co_return count;
}

seastar::future<> NativeIndex::compact() {
    // Wait for any in-flight background flush, then flush active memtable
    co_await waitForFlush();
    if (memtable_ && !memtable_->empty()) {
        co_await flushMemTable();
    }

    // Close all SSTable readers before compaction (compaction deletes files)
    for (auto& [fileNum, reader] : sstableReaders_) {
        co_await reader->close();
    }
    sstableReaders_.clear();

    co_await compaction_->compactAll();
    co_await refreshSSTables();

    // Step 5: measurementSeriesCache_ removed — no cache to clear
}

// ============================================================================
// Phase 2: Roaring bitmap postings
// ============================================================================

void NativeIndex::buildBitmapCacheKey(std::string& out, const std::string& measurement,
                                       const std::string& tagKey, const std::string& tagValue) {
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
    for (uint32_t id = lastFlushedLocalId_; id < localIdMap_.nextId(); ++id) {
        batch.put(ke::encodeLocalIdForwardKey(id), localIdMap_.getGlobalId(id).toBytes());
    }
    lastFlushedLocalId_ = localIdMap_.nextId();

    // Serialize dirty bitmaps
    std::string bitmapKey;
    for (auto it = bitmapCache_.begin(); it != bitmapCache_.end(); ++it) {
        auto& entry = it.value();
        if (!entry.dirty) continue;

        auto& bitmap = entry.bitmap;
        if (bitmap.isEmpty()) continue;

        bitmap.runOptimize();
        bitmap.shrinkToFit();
        entry.dirty = false;

        bitmapKey.clear();
        bitmapKey.push_back(static_cast<char>(POSTINGS_BITMAP));
        bitmapKey.append(it->first);
        size_t serializedSize = bitmap.getSizeInBytes();
        std::string serialized(serializedSize, '\0');
        bitmap.write(serialized.data());
        batch.put(bitmapKey, serialized);
    }
}

const roaring::Roaring* NativeIndex::getPostingsBitmapByKey(const std::string& cacheKey) {
    auto it = bitmapCache_.find(cacheKey);
    if (it != bitmapCache_.end()) {
        return &it->second.bitmap;
    }

    // Phase 4: Check measurement bloom filter before KV lookup.
    // Cache key format: "measurement\0tagKey\0tagValue"
    auto nullPos = cacheKey.find('\0');
    if (nullPos != std::string::npos) {
        std::string measurement = cacheKey.substr(0, nullPos);
        auto bloomIt = measurementBloomCache_.find(measurement);
        if (bloomIt != measurementBloomCache_.end() && !bloomIt->second.isNull()) {
            // Build the KV key and check bloom for the entire bitmap key
            std::string bitmapKvKey;
            bitmapKvKey.reserve(1 + cacheKey.size());
            bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
            bitmapKvKey.append(cacheKey);
            if (!bloomIt->second.mayContain(bitmapKvKey)) {
                return nullptr;
            }
        }
    }

    // Cache miss — build KV key by prepending prefix byte to cache key
    std::string bitmapKvKey;
    bitmapKvKey.reserve(1 + cacheKey.size());
    bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
    bitmapKvKey.append(cacheKey);

    auto val = kvGet(bitmapKvKey);
    if (val.has_value()) {
        auto& entry = bitmapCache_[cacheKey];
        entry.bitmap = roaring::Roaring::readSafe(val->data(), val->size());
        entry.dirty = false;
        return &entry.bitmap;
    }

    return nullptr;
}

roaring::Roaring& NativeIndex::getOrLoadBitmapForInsert(std::string& cacheKey) {
    auto it = bitmapCache_.find(cacheKey);
    if (it != bitmapCache_.end()) {
        it.value().dirty = true;
        return it.value().bitmap;
    }

    // Cache miss — cold load from KV store
    std::string bitmapKvKey;
    bitmapKvKey.reserve(1 + cacheKey.size());
    bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
    bitmapKvKey.append(cacheKey);

    auto& entry = bitmapCache_[cacheKey];
    entry.dirty = true;
    auto existing = kvGet(bitmapKvKey);
    if (existing.has_value()) {
        entry.bitmap = roaring::Roaring::readSafe(existing->data(), existing->size());
    }
    return entry.bitmap;
}

void NativeIndex::migrateToLocalIds(IndexWriteBatch& batch) {
    // Scan all SERIES_METADATA entries to assign local IDs
    std::string metaPrefix(1, static_cast<char>(SERIES_METADATA));
    kvPrefixScan(metaPrefix, [&](std::string_view key, std::string_view) {
        if (key.size() >= 1 + 16) {
            SeriesId128 globalId = SeriesId128::fromBytes(key.data() + 1, 16);
            uint32_t localId = localIdMap_.getOrAssign(globalId);
            batch.put(ke::encodeLocalIdForwardKey(localId), globalId.toBytes());
        }
        return true;
    });

    if (localIdMap_.size() == 0) return;

    // Persist the counter
    batch.put(ke::encodeLocalIdCounterKey(), ke::encodeLocalId(localIdMap_.nextId()));

    // Scan TAG_INDEX entries to build initial POSTINGS_BITMAP entries.
    // Populate bitmapCache_ directly so they're immediately available for queries.
    std::string tagPrefix(1, static_cast<char>(TAG_INDEX));
    kvPrefixScan(tagPrefix, [&](std::string_view key, std::string_view value) {
        if (value.size() < 16) return true;
        SeriesId128 globalId = SeriesId128::fromBytes(value.data(), 16);
        auto localOpt = localIdMap_.getLocalId(globalId);
        if (!localOpt.has_value()) return true;

        // Extract measurement\0tagKey\0tagValue from key (skip prefix byte, strip trailing \0+seriesId)
        auto afterPrefix = key.substr(1);
        if (afterPrefix.size() < 17) return true;
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
        if (!entry.dirty) continue;
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

    ::native_index_log.info("Migration: assigned {} local IDs, built {} postings bitmaps",
                          localIdMap_.size(), bitmapCount);
}

// ============================================================================
// Phase 3: Time-scoped per-day bitmaps
// ============================================================================

void NativeIndex::buildDayBitmapCacheKey(std::string& out, const std::string& measurement, uint32_t day) {
    out.clear();
    out.reserve(measurement.size() + 1 + 4);
    out += measurement;
    out.push_back('\0');
    out.append(reinterpret_cast<const char*>(&day), 4);
}

roaring::Roaring& NativeIndex::getOrLoadDayBitmapForInsert(std::string& cacheKey) {
    auto it = dayBitmapCache_.find(cacheKey);
    if (it != dayBitmapCache_.end()) {
        it.value().dirty = true;
        return it.value().bitmap;
    }

    // Cache miss — cold load from KV store
    std::string kvKey;
    kvKey.reserve(1 + cacheKey.size());
    kvKey.push_back(static_cast<char>(TIME_SERIES_DAY));
    kvKey.append(cacheKey);

    auto& entry = dayBitmapCache_[cacheKey];
    entry.dirty = true;
    auto existing = kvGet(kvKey);
    if (existing.has_value()) {
        entry.bitmap = roaring::Roaring::readSafe(existing->data(), existing->size());
    }
    return entry.bitmap;
}

const roaring::Roaring* NativeIndex::getDayBitmapByKey(const std::string& cacheKey) {
    auto it = dayBitmapCache_.find(cacheKey);
    if (it != dayBitmapCache_.end()) {
        return &it->second.bitmap;
    }

    // Cache miss — load from KV
    std::string kvKey;
    kvKey.reserve(1 + cacheKey.size());
    kvKey.push_back(static_cast<char>(TIME_SERIES_DAY));
    kvKey.append(cacheKey);

    auto val = kvGet(kvKey);
    if (val.has_value()) {
        auto& entry = dayBitmapCache_[cacheKey];
        entry.bitmap = roaring::Roaring::readSafe(val->data(), val->size());
        entry.dirty = false;
        return &entry.bitmap;
    }

    return nullptr;
}

void NativeIndex::flushDirtyDayBitmaps(IndexWriteBatch& batch) {
    std::string kvKey;
    for (auto it = dayBitmapCache_.begin(); it != dayBitmapCache_.end(); ++it) {
        auto& entry = it.value();
        if (!entry.dirty) continue;

        auto& bitmap = entry.bitmap;
        if (bitmap.isEmpty()) continue;

        bitmap.runOptimize();
        bitmap.shrinkToFit();
        entry.dirty = false;

        kvKey.clear();
        kvKey.push_back(static_cast<char>(TIME_SERIES_DAY));
        kvKey.append(it->first);
        size_t serializedSize = bitmap.getSizeInBytes();
        std::string serialized(serializedSize, '\0');
        bitmap.write(serialized.data());
        batch.put(kvKey, serialized);
    }
}

void NativeIndex::trimBitmapCache() {
    // Check both entry count and byte budget
    bool overEntries = bitmapCache_.size() > MAX_BITMAP_CACHE_ENTRIES;
    bool overBytes = false;
    if (!overEntries) {
        // Only compute total bytes if entry count is OK (avoid O(n) scan when unnecessary)
        size_t totalBytes = 0;
        for (const auto& [key, entry] : bitmapCache_) {
            totalBytes += entry.bitmap.getSizeInBytes(false) + key.size();
            if (totalBytes > MAX_BITMAP_CACHE_BYTES) {
                overBytes = true;
                break;
            }
        }
    }
    if (!overEntries && !overBytes) return;

    // Evict non-dirty entries until under both limits
    for (auto it = bitmapCache_.begin(); it != bitmapCache_.end();) {
        if (bitmapCache_.size() <= MAX_BITMAP_CACHE_ENTRIES / 2) break;  // Evict to 50% to avoid thrashing
        if (!it->second.dirty) {
            it = bitmapCache_.erase(it);
        } else {
            ++it;
        }
    }
}

void NativeIndex::trimDayBitmapCache() {
    bool overEntries = dayBitmapCache_.size() > MAX_DAY_BITMAP_CACHE_ENTRIES;
    bool overBytes = false;
    if (!overEntries) {
        size_t totalBytes = 0;
        for (const auto& [key, entry] : dayBitmapCache_) {
            totalBytes += entry.bitmap.getSizeInBytes(false) + key.size();
            if (totalBytes > MAX_DAY_BITMAP_CACHE_BYTES) {
                overBytes = true;
                break;
            }
        }
    }
    if (!overEntries && !overBytes) return;

    for (auto it = dayBitmapCache_.begin(); it != dayBitmapCache_.end();) {
        if (dayBitmapCache_.size() <= MAX_DAY_BITMAP_CACHE_ENTRIES / 2) break;
        if (!it->second.dirty) {
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
    if (hllCache_.size() <= MAX_HLL_CACHE_ENTRIES) return;

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

// Step 6: Evict oldest tag values cache entries when over limit.
void NativeIndex::trimTagValuesCache() {
    if (tagValuesCache_.size() <= MAX_TAG_VALUES_CACHE_ENTRIES) return;
    // Clear entirely — entries are repopulated on demand from KV store.
    // This avoids the old FIFO eviction which could discard hot entries.
    tagValuesCache_.clear();
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
    }
}

void NativeIndex::trimMeasurementBloomCache() {
    if (measurementBloomCache_.size() <= MAX_BLOOM_CACHE_ENTRIES) return;
    // Evict non-dirty entries first. If still over limit, evict all non-dirty.
    for (auto it = measurementBloomCache_.begin(); it != measurementBloomCache_.end();) {
        if (measurementBloomCache_.size() <= MAX_BLOOM_CACHE_ENTRIES) break;
        if (dirtyMeasurementBlooms_.count(it->first) == 0) {
            bloomFullyBuilt_.erase(it->first);
            it = measurementBloomCache_.erase(it);
        } else {
            ++it;
        }
    }
}

roaring::Roaring NativeIndex::buildActiveSeriesBitmap(const std::string& measurement,
                                                       uint32_t startDay, uint32_t endDay) {
    roaring::Roaring result;
    std::string cacheKey;
    for (uint32_t day = startDay; day <= endDay; ++day) {
        buildDayBitmapCacheKey(cacheKey, measurement, day);
        auto* bitmap = getDayBitmapByKey(cacheKey);
        if (bitmap) {
            result |= *bitmap;
        }
    }
    return result;
}

seastar::future<> NativeIndex::removeExpiredDayBitmaps(const std::string& measurement, uint32_t cutoffDay) {
    std::string prefix = ke::encodeDayBitmapPrefix(measurement);
    IndexWriteBatch batch;

    kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
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
        if (k.size() >= measurement.size() + 1 + 4 &&
            k.compare(0, measurement.size(), measurement) == 0 &&
            k[measurement.size()] == '\0') {
            uint32_t day;
            std::memcpy(&day, k.data() + k.size() - 4, 4);
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

seastar::future<std::expected<std::vector<IndexBackend::SeriesWithMetadata>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadataTimeScoped(const std::string& measurement,
                                               const std::map<std::string, std::string>& tagFilters,
                                               const std::unordered_set<std::string>& fieldFilter,
                                               uint64_t startTimeNs, uint64_t endTimeNs,
                                               size_t maxSeries) {
    uint32_t startDay = ke::dayBucketFromNs(startTimeNs);
    uint32_t endDay = ke::dayBucketFromNs(endTimeNs);

    roaring::Roaring activeSeries = buildActiveSeriesBitmap(measurement, startDay, endDay);

    if (activeSeries.isEmpty()) {
        // Check if any day bitmaps exist for this measurement (pre-Phase-3 fallback)
        std::string prefix = ke::encodeDayBitmapPrefix(measurement);
        bool hasDayBitmaps = false;
        kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
            hasDayBitmaps = true;
            return false;  // Stop after first hit
        });
        // Also check dirty cache entries for this measurement
        if (!hasDayBitmaps) {
            std::string measPrefix = measurement;
            measPrefix.push_back('\0');
            for (auto it = dayBitmapCache_.begin(); it != dayBitmapCache_.end(); ++it) {
                if (it->first.size() >= measPrefix.size() &&
                    it->first.compare(0, measPrefix.size(), measPrefix) == 0 &&
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
            auto* tagBitmap = getPostingsBitmapByKey(cacheKey);
            if (!tagBitmap) co_return std::vector<SeriesWithMetadata>{};
            activeSeries &= *tagBitmap;
        } else {
            // Multi-tag intersection (smallest-first)
            struct TagBitmap { const roaring::Roaring* bmp; };
            std::vector<TagBitmap> bitmaps;
            bitmaps.reserve(tagFilters.size());
            for (const auto& [tagKey, tagValue] : tagFilters) {
                buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
                auto* bmp = getPostingsBitmapByKey(cacheKey);
                if (!bmp) co_return std::vector<SeriesWithMetadata>{};
                bitmaps.push_back({bmp});
            }
            std::sort(bitmaps.begin(), bitmaps.end(),
                      [](const TagBitmap& a, const TagBitmap& b) { return a.bmp->cardinality() < b.bmp->cardinality(); });
            roaring::Roaring tagIntersection = *bitmaps[0].bmp;
            for (size_t i = 1; i < bitmaps.size() && !tagIntersection.isEmpty(); ++i) {
                tagIntersection &= *bitmaps[i].bmp;
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

    for (auto it = activeSeries.begin(); it != activeSeries.end(); ++it) {
        uint32_t localId = *it;
        if (localId >= localIdMap_.nextId()) continue;
        const SeriesId128& id = localIdMap_.getGlobalId(localId);

        auto cached = seriesMetadataCache_.get(id);
        const SeriesMetadata* meta = nullptr;
        std::optional<SeriesMetadata> decoded;

        if (cached) {
            meta = cached;
        } else {
            key.clear();
            key.push_back(static_cast<char>(SERIES_METADATA));
            id.appendTo(key);
            auto val = kvGet(key);
            if (val.has_value()) {
                decoded.emplace(ke::decodeSeriesMetadata(*val));
                seriesMetadataCache_.put(id, *decoded);
                meta = &*decoded;
            }
        }
        if (!meta) continue;
        if (!fieldFilter.empty() && !fieldFilter.count(meta->field)) continue;
        if (decoded) results.push_back({id, std::move(*decoded)});
        else results.push_back({id, *meta});
    }

    co_return results;
}

// ============================================================================
// Phase 4: Cardinality estimation (HLL + bloom)
// ============================================================================

void NativeIndex::updateHLL(const std::string& measurement, uint32_t localId) {
    std::string key;
    key.reserve(measurement.size() + 1);
    key += measurement;
    key.push_back('\0');

    auto it = hllCache_.find(key);
    if (it == hllCache_.end()) {
        // Try loading from KV
        auto kvKey = ke::encodeCardinalityHLLKey(measurement);
        auto val = kvGet(kvKey);
        if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
            hllCache_[key] = HyperLogLog::deserialize(*val);
        } else {
            hllCache_[key] = HyperLogLog{};
        }
        it = hllCache_.find(key);
    }
    it.value().add(localId);
    hllCacheDirty_.insert(key);
}

void NativeIndex::updateTagHLL(const std::string& measurement, const std::string& tagKey,
                                const std::string& tagValue, uint32_t localId) {
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
        auto val = kvGet(kvKey);
        if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
            hllCache_[key] = HyperLogLog::deserialize(*val);
        } else {
            hllCache_[key] = HyperLogLog{};
        }
        it = hllCache_.find(key);
    }
    it.value().add(localId);
    hllCacheDirty_.insert(key);
}

void NativeIndex::flushDirtyHLLs(IndexWriteBatch& batch) {
    for (const auto& cacheKey : hllCacheDirty_) {
        auto it = hllCache_.find(cacheKey);
        if (it == hllCache_.end() || it->second.empty()) continue;

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

void NativeIndex::flushDirtyMeasurementBlooms(IndexWriteBatch& batch) {
    for (const auto& measurement : dirtyMeasurementBlooms_) {
        // Build bloom from all postings bitmap KV keys for this measurement.
        // This lets us short-circuit lookups for non-existent tag combinations.
        BloomFilter bloom(10);  // 10 bits/key → ~1% FPR

        std::string measPrefix = measurement;
        measPrefix.push_back('\0');

        std::string bitmapKvKey;
        for (auto it = bitmapCache_.begin(); it != bitmapCache_.end(); ++it) {
            auto& key = it->first;
            // Cache key format: "measurement\0tagKey\0tagValue"
            if (key.size() >= measPrefix.size() &&
                key.compare(0, measPrefix.size(), measPrefix) == 0 &&
                !it->second.bitmap.isEmpty()) {
                bitmapKvKey.clear();
                bitmapKvKey.push_back(static_cast<char>(POSTINGS_BITMAP));
                bitmapKvKey.append(key);
                bloom.addKey(bitmapKvKey);
            }
        }

        // Only scan KV store if we haven't done a full scan for this measurement yet.
        // After getOrCreateSeriesId, the cache contains all bitmap entries for the measurement,
        // so the KV scan only adds entries from prior sessions (before this process opened).
        if (bloomFullyBuilt_.find(measurement) == bloomFullyBuilt_.end()) {
            std::string kvScanPrefix;
            kvScanPrefix.push_back(static_cast<char>(POSTINGS_BITMAP));
            kvScanPrefix.append(measPrefix);
            kvPrefixScan(kvScanPrefix, [&](std::string_view key, std::string_view) {
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
    dirtyMeasurementBlooms_.clear();
}

double NativeIndex::estimateMeasurementCardinality(const std::string& measurement) {
    std::string key;
    key.reserve(measurement.size() + 1);
    key += measurement;
    key.push_back('\0');

    auto it = hllCache_.find(key);
    if (it != hllCache_.end()) {
        return it->second.estimate();
    }

    // Try loading from KV
    auto kvKey = ke::encodeCardinalityHLLKey(measurement);
    auto val = kvGet(kvKey);
    if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
        auto hll = HyperLogLog::deserialize(*val);
        double est = hll.estimate();
        hllCache_[key] = std::move(hll);
        return est;
    }

    // No HLL — fallback to counting measurement series entries
    std::string prefix = ke::encodeMeasurementSeriesPrefix(measurement);
    size_t count = 0;
    kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
        ++count;
        return true;
    });
    return static_cast<double>(count);
}

double NativeIndex::estimateTagCardinality(const std::string& measurement,
                                            const std::string& tagKey, const std::string& tagValue) {
    std::string key;
    key.reserve(measurement.size() + 1 + tagKey.size() + 1 + tagValue.size());
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    key.push_back('\0');
    key += tagValue;

    auto it = hllCache_.find(key);
    if (it != hllCache_.end()) {
        return it->second.estimate();
    }

    // Try loading HLL from KV
    auto kvKey = ke::encodeCardinalityHLLKey(measurement, tagKey, tagValue);
    auto val = kvGet(kvKey);
    if (val.has_value() && val->size() >= HyperLogLog::SERIALIZED_SIZE) {
        auto hll = HyperLogLog::deserialize(*val);
        double est = hll.estimate();
        hllCache_[key] = std::move(hll);
        return est;
    }

    // Fallback: check roaring bitmap cardinality (exact)
    std::string cacheKey;
    buildBitmapCacheKey(cacheKey, measurement, tagKey, tagValue);
    auto* bitmap = getPostingsBitmapByKey(cacheKey);
    if (bitmap) {
        return static_cast<double>(bitmap->cardinality());
    }

    return 0.0;
}

// ============================================================================
// indexInsert template (non-virtual convenience method)
// ============================================================================

template <class T>
seastar::future<SeriesId128> NativeIndex::indexInsert(const TimeStarInsert<T>& insert) {
    SeriesId128 seriesId = co_await getOrCreateSeriesId(insert.measurement, insert.getTags(), insert.field);

    // Phase 3: Record day bitmaps for time-scoped discovery
    auto localIdOpt = localIdMap_.getLocalId(seriesId);
    if (localIdOpt.has_value()) {
        uint32_t localId = *localIdOpt;
        std::string dayCacheKey;
        uint32_t lastDay = UINT32_MAX;
        for (uint64_t ts : insert.getTimestamps()) {
            uint32_t day = ke::dayBucketFromNs(ts);
            if (day != lastDay) {
                buildDayBitmapCacheKey(dayCacheKey, insert.measurement, day);
                getOrLoadDayBitmapForInsert(dayCacheKey).add(localId);
                lastDay = day;
            }
        }
    }

    std::string fieldTypeCacheKey = insert.measurement + std::string(1, '\0') + insert.field;
    if (knownFieldTypes_.find(fieldTypeCacheKey) == knownFieldTypes_.end()) {
        std::string typeStr;
        if constexpr (std::is_same_v<T, double>) {
            typeStr = "float";
        } else if constexpr (std::is_same_v<T, bool>) {
            typeStr = "boolean";
        } else if constexpr (std::is_same_v<T, std::string>) {
            typeStr = "string";
        } else if constexpr (std::is_same_v<T, int64_t>) {
            typeStr = "integer";
        }
        if (!typeStr.empty()) {
            co_await setFieldType(insert.measurement, insert.field, typeStr);
            knownFieldTypes_.insert(fieldTypeCacheKey);
        }
    }

    co_return seriesId;
}

// ============================================================================
// Schema broadcast support
// ============================================================================

seastar::future<SchemaUpdate> NativeIndex::indexMetadataBatchWithSchema(const std::vector<MetadataOp>& ops) {
    pendingSchemaUpdate_ = SchemaUpdate{};  // Clear accumulator
    co_await indexMetadataBatch(ops);
    co_return std::move(pendingSchemaUpdate_);
}

void NativeIndex::applySchemaUpdate(const SchemaUpdate& update) {
    for (const auto& [measurement, fields] : update.newFields) {
        fieldsCache_[measurement].insert(fields.begin(), fields.end());
    }
    for (const auto& [measurement, tags] : update.newTags) {
        tagsCache_[measurement].insert(tags.begin(), tags.end());
    }
    for (const auto& [cacheKey, values] : update.newTagValues) {
        tagValuesCache_[cacheKey].insert(values.begin(), values.end());
    }
    for (const auto& [cacheKey, type] : update.newFieldTypes) {
        knownFieldTypes_.insert(cacheKey);
        // Store the actual type value so getFieldType() can return it
        auto it = fieldTypeValues_.find(cacheKey);
        if (it == fieldTypeValues_.end() || type < it->second) {
            fieldTypeValues_[cacheKey] = type;
        }
    }
}

// Explicit template instantiations
template seastar::future<SeriesId128> NativeIndex::indexInsert<double>(const TimeStarInsert<double>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<bool>(const TimeStarInsert<bool>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<std::string>(const TimeStarInsert<std::string>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<int64_t>(const TimeStarInsert<int64_t>& insert);

}  // namespace timestar::index
