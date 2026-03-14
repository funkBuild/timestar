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
      seriesMetadataCache_(shardId == 0 ? timestar::config().index.metadata_cache_bytes : 0),
      discoveryCache_(shardId == 0 ? timestar::config().index.discovery_cache_bytes : 0) {}

NativeIndex::~NativeIndex() = default;

// ============================================================================
// Lifecycle
// ============================================================================

seastar::future<> NativeIndex::open() {
    if (shardId_ != 0) {
        ::native_index_log.info("Shard {} skipping NativeIndex — metadata centralized on shard 0", shardId_);
        co_return;
    }

    indexPath_ = std::filesystem::absolute("shard_0/native_index").string();
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

    ::native_index_log.info("NativeIndex opened at: {} ({} SSTables, {} MemTable entries)", indexPath_,
                          manifest_->files().size(), memtable_->size());
}

seastar::future<> NativeIndex::close() {
    if (shardId_ != 0) co_return;

    // Wait for any in-flight background flush, then flush remaining data
    try {
        co_await waitForFlush();
        if (memtable_ && !memtable_->empty()) {
            co_await flushMemTable();
        }
    } catch (const std::exception& e) {
        ::native_index_log.warn("Failed to flush MemTable on close: {} — data preserved in WAL", e.what());
    }

    // Close all SSTable readers
    for (auto& reader : sstableReaders_) {
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

seastar::future<> NativeIndex::refreshSSTables() {
    for (auto& reader : sstableReaders_) {
        co_await reader->close();
    }
    sstableReaders_.clear();

    for (const auto& fileMeta : manifest_->files()) {
        auto path = sstFilename(fileMeta.fileNumber);
        if (std::filesystem::exists(path)) {
            auto reader = co_await SSTableReader::open(path);
            sstableReaders_.push_back(std::move(reader));
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

    // 3. Check SSTables (newest to oldest)
    for (auto it = sstableReaders_.rbegin(); it != sstableReaders_.rend(); ++it) {
        auto result = (*it)->get(key);
        if (result.has_value()) {
            return result;
        }
    }

    return std::nullopt;
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
        auto iter = sstableReaders_[0]->newIterator();
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

    int sstPriority = nextPriority + static_cast<int>(sstableReaders_.size());
    for (auto& reader : sstableReaders_) {
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
    while (it.valid()) {
        if (!it.isTombstone()) {
            writer.add(it.key(), it.value());
        }
        it.next();
    }

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
    if (shardId_ != 0) {
        throw std::runtime_error("getOrCreateSeriesId must be called on shard 0");
    }

    // Compute SeriesId128 directly from components — no encoded string needed.
    // This avoids 2+2M temporary string allocations from escapeKeyComponent().
    SeriesId128 seriesId = SeriesId128::fromComponents(measurement, tags, field);

    // Fast path: already indexed (no string allocation needed)
    if (seriesCacheContains(seriesId)) {
        co_return seriesId;
    }

    // Check if series exists in storage
    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    auto existing = kvGet(metaKey);
    if (existing.has_value()) {
        seriesCacheInsert(seriesId);
        co_return seriesId;
    }

    // New series — create ALL index entries in a single batch (Stage 4).
    // Previously this was split across getOrCreateSeriesId + addFieldsAndTags
    // = 2-6 separate WAL writes. Now it's exactly 1.
    IndexWriteBatch batch;
    batch.reserve(5 + 2 * tags.size());  // metadata + indexes + per-tag TAG_INDEX + GROUP_BY_INDEX

    // Series metadata
    SeriesMetadata metadata{measurement, tags, field};
    batch.put(metaKey, ke::encodeSeriesMetadata(metadata));

    // Measurement series index
    batch.put(ke::encodeMeasurementSeriesKey(measurement, seriesId), "");

    // Measurement field series index
    batch.put(ke::encodeMeasurementFieldSeriesKey(measurement, field, seriesId), "");

    // Tag index and group-by index
    std::string encodedSeriesId = ke::encodeSeriesId(seriesId);
    // Reusable buffer for tag index keys — avoids per-tag allocation
    std::string tagIdxKey;
    tagIdxKey.reserve(1 + measurement.size() + 1 + 32 + 1 + 32 + 1 + 16);
    for (const auto& [tagKey, tagValue] : tags) {
        // TAG_INDEX
        tagIdxKey.clear();
        tagIdxKey.push_back(TAG_INDEX);
        tagIdxKey += measurement;
        tagIdxKey.push_back('\0');
        tagIdxKey += tagKey;
        tagIdxKey.push_back('\0');
        tagIdxKey += tagValue;
        tagIdxKey.push_back('\0');
        tagIdxKey += encodedSeriesId;
        batch.put(tagIdxKey, encodedSeriesId);

        // GROUP_BY_INDEX — same key, flip first byte in-place (no copy)
        tagIdxKey[0] = GROUP_BY_INDEX;
        batch.put(tagIdxKey, encodedSeriesId);
    }

    // --- Field metadata (was addField) ---
    auto& fieldCache = fieldsCache_[measurement];
    if (fieldCache.empty()) {
        auto val = kvGet(ke::encodeMeasurementFieldsKey(measurement));
        if (val.has_value()) fieldCache = ke::decodeStringSet(*val);
    }
    if (fieldCache.insert(field).second) {
        batch.put(ke::encodeMeasurementFieldsKey(measurement), ke::encodeStringSet(fieldCache));
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
        }

        tvCacheKey.clear();
        tvCacheKey += measurement;
        tvCacheKey.push_back('\0');
        tvCacheKey += tagKey;
        auto& tagVals = tagValuesCache_[tvCacheKey];
        if (tagVals.empty()) {
            auto val = kvGet(ke::encodeTagValuesKey(measurement, tagKey));
            if (val.has_value()) tagVals = ke::decodeStringSet(*val);
        }
        if (tagVals.insert(tagValue).second) {
            batch.put(ke::encodeTagValuesKey(measurement, tagKey), ke::encodeStringSet(tagVals));
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

    // Update measurement series cache
    if (measurementSeriesCache_.count(measurement)) {
        measurementSeriesCache_[measurement].push_back(seriesId);
    }

    co_return seriesId;
}

seastar::future<std::optional<SeriesId128>> NativeIndex::getSeriesId(
    const std::string& measurement, const std::map<std::string, std::string>& tags, const std::string& field) {
    if (shardId_ != 0) co_return std::nullopt;

    std::string seriesKeyStr = ke::encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    auto existing = kvGet(metaKey);
    if (existing.has_value()) {
        co_return seriesId;
    }
    co_return std::nullopt;
}

seastar::future<std::optional<SeriesMetadata>> NativeIndex::getSeriesMetadata(const SeriesId128& seriesId) {
    if (shardId_ != 0) co_return std::nullopt;

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
    if (shardId_ != 0) co_return;

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
    if (shardId_ != 0) co_return;

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

    // Add tag value
    std::string tvCacheKey = measurement + std::string(1, '\0') + tagKey;
    auto& tagVals = tagValuesCache_[tvCacheKey];
    if (tagVals.empty()) {
        auto key = ke::encodeTagValuesKey(measurement, tagKey);
        auto val = kvGet(key);
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
            batch.put(ke::encodeTagValuesKey(measurement, tagKey), ke::encodeStringSet(tagVals));
        }
        co_await kvWriteBatch(batch);
    }
}

seastar::future<> NativeIndex::addFieldsAndTags(const std::string& measurement, const std::string& field,
                                                  const std::map<std::string, std::string>& tags) {
    if (shardId_ != 0) co_return;

    co_await addField(measurement, field);
    for (const auto& [tagKey, tagValue] : tags) {
        co_await addTag(measurement, tagKey, tagValue);
    }
}

seastar::future<> NativeIndex::setFieldType(const std::string& measurement, const std::string& field,
                                              const std::string& type) {
    if (shardId_ != 0) co_return;

    std::string cacheKey = measurement + std::string(1, '\0') + field;
    if (knownFieldTypes_.count(cacheKey)) co_return;

    co_await kvPut(ke::encodeFieldTypeKey(measurement, field), type);
    knownFieldTypes_.insert(cacheKey);
}

seastar::future<std::string> NativeIndex::getFieldType(const std::string& measurement, const std::string& field) {
    if (shardId_ != 0) co_return "";

    auto val = kvGet(ke::encodeFieldTypeKey(measurement, field));
    co_return val.value_or("");
}

seastar::future<std::set<std::string>> NativeIndex::getAllMeasurements() {
    if (shardId_ != 0) co_return std::set<std::string>{};

    std::set<std::string> result;
    std::string prefix(1, static_cast<char>(MEASUREMENT_FIELDS));
    kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        result.insert(std::string(key.substr(1)));  // Skip prefix byte
        return true;
    });
    co_return result;
}

seastar::future<std::set<std::string>> NativeIndex::getFields(const std::string& measurement) {
    if (shardId_ != 0) co_return std::set<std::string>{};

    auto& cached = fieldsCache_[measurement];
    if (!cached.empty()) co_return cached;

    auto val = kvGet(ke::encodeMeasurementFieldsKey(measurement));
    if (val.has_value()) {
        cached = ke::decodeStringSet(*val);
    }
    co_return cached;
}

seastar::future<std::set<std::string>> NativeIndex::getTags(const std::string& measurement) {
    if (shardId_ != 0) co_return std::set<std::string>{};

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
    if (shardId_ != 0) co_return std::set<std::string>{};

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
    if (shardId_ != 0 || ops.empty()) co_return;

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
    if (shardId_ != 0) co_return std::vector<SeriesId128>{};

    if (tagFilters.empty()) {
        co_return co_await getAllSeriesForMeasurement(measurement, maxSeries);
    }

    // For each tag filter, find matching series and intersect
    std::optional<std::vector<SeriesId128>> result;

    for (const auto& [tagKey, tagValue] : tagFilters) {
        auto seriesIds = co_await findSeriesByTag(measurement, tagKey, tagValue);
        std::sort(seriesIds.begin(), seriesIds.end());

        if (!result) {
            result = std::move(seriesIds);
        } else {
            // Intersect
            std::vector<SeriesId128> intersection;
            std::set_intersection(result->begin(), result->end(), seriesIds.begin(), seriesIds.end(),
                                  std::back_inserter(intersection));
            result = std::move(intersection);
        }

        if (result->empty()) break;
    }

    auto& res = *result;
    if (maxSeries > 0 && res.size() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{res.size(), maxSeries});
    }

    co_return res;
}

// Fused series discovery + metadata resolution.
// Avoids the intermediate metaBatch vector and its 2000 SeriesMetadata deep copies.
seastar::future<std::expected<std::vector<IndexBackend::SeriesWithMetadata>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadata(const std::string& measurement,
                                     const std::map<std::string, std::string>& tagFilters,
                                     const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    if (shardId_ != 0) co_return std::vector<SeriesWithMetadata>{};

    auto findResult = co_await findSeries(measurement, tagFilters, maxSeries);
    if (!findResult.has_value()) {
        co_return std::unexpected(findResult.error());
    }

    // Fused metadata resolution + filtering — resolve each ID inline,
    // copy metadata only for results that pass the field filter.
    const auto& seriesIds = *findResult;
    std::vector<SeriesWithMetadata> results;
    results.reserve(seriesIds.size());

    std::string key;
    key.reserve(1 + 16);

    for (const auto& id : seriesIds) {
        // Check metadata cache (returns pointer, no copy)
        auto cached = seriesMetadataCache_.get(id);
        const SeriesMetadata* meta = nullptr;
        std::optional<SeriesMetadata> decoded;

        if (cached) {
            meta = cached;
        } else {
            // Cache miss — fetch and decode
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

        // Apply field filter BEFORE copying metadata
        if (!fieldFilter.empty() && !fieldFilter.count(meta->field)) {
            continue;
        }

        // Only copy metadata for results that pass the filter
        if (decoded) {
            results.push_back({id, std::move(*decoded)});
        } else {
            results.push_back({id, *meta});
        }
    }

    co_return results;
}

seastar::future<std::expected<std::shared_ptr<const std::vector<IndexBackend::SeriesWithMetadata>>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadataCached(const std::string& measurement,
                                           const std::map<std::string, std::string>& tagFilters,
                                           const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    if (shardId_ != 0) {
        co_return std::make_shared<const std::vector<SeriesWithMetadata>>();
    }

    // Build cache key
    std::string cacheKey = measurement;
    for (const auto& [k, v] : tagFilters) {
        cacheKey += '\0';
        cacheKey += k;
        cacheKey += '=';
        cacheKey += v;
    }
    cacheKey += "\0F:";
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
    discoveryCache_.clearByPrefix(measurement);
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
    if (shardId_ != 0) co_return std::vector<SeriesId128>{};

    std::string prefix;
    prefix.push_back(TAG_INDEX);
    prefix += measurement;
    prefix.push_back('\0');
    prefix += tagKey;
    prefix.push_back('\0');
    prefix += tagValue;
    prefix.push_back('\0');

    std::vector<SeriesId128> result;
    kvPrefixScan(prefix, [&](std::string_view key, std::string_view value) {
        if (value.size() >= 16) {
            result.push_back(SeriesId128::fromBytes(value.data(), 16));
        }
        if (maxSeries > 0 && result.size() > maxSeries) return false;
        return true;
    });

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
    if (shardId_ != 0) co_return std::map<std::string, std::vector<SeriesId128>>{};

    std::string prefix;
    prefix.push_back(GROUP_BY_INDEX);
    prefix += measurement;
    prefix.push_back('\0');
    prefix += tagKey;
    prefix.push_back('\0');

    std::map<std::string, std::vector<SeriesId128>> result;
    kvPrefixScan(prefix, [&](std::string_view key, std::string_view value) {
        // Extract tag value from key: prefix + tagValue + \0 + seriesId
        auto afterPrefix = key.substr(prefix.size());
        auto nullPos = afterPrefix.find('\0');
        if (nullPos != std::string_view::npos && value.size() >= 16) {
            std::string tagValue(afterPrefix.substr(0, nullPos));
            result[tagValue].push_back(SeriesId128::fromBytes(value.data(), 16));
        }
        return true;
    });

    co_return result;
}

// ============================================================================
// Field stats
// ============================================================================

seastar::future<> NativeIndex::updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                                  const IndexFieldStats& stats) {
    if (shardId_ != 0) co_return;

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
    if (shardId_ != 0) co_return std::nullopt;

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

seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> NativeIndex::getAllSeriesForMeasurement(
    const std::string& measurement, size_t maxSeries) {
    if (shardId_ != 0) co_return std::vector<SeriesId128>{};

    // Check cache
    if (measurementSeriesCache_.count(measurement)) {
        auto& cached = measurementSeriesCache_[measurement];
        if (maxSeries > 0 && cached.size() > maxSeries) {
            co_return std::unexpected(SeriesLimitExceeded{cached.size(), maxSeries});
        }
        co_return cached;
    }

    std::string prefix = ke::encodeMeasurementSeriesPrefix(measurement);
    std::vector<SeriesId128> result;

    kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        // SeriesId128 is the last 16 bytes of the key
        if (key.size() >= prefix.size() + 16) {
            result.push_back(SeriesId128::fromBytes(key.data() + prefix.size(), 16));
        }
        if (maxSeries > 0 && result.size() > maxSeries) return false;
        return true;
    });

    if (maxSeries > 0 && result.size() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{result.size(), maxSeries});
    }

    measurementSeriesCache_[measurement] = result;
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
    if (shardId_ != 0) co_return;

    auto key = ke::encodeRetentionPolicyKey(policy.measurement);
    std::string value;
    auto ec = glz::write_json(policy, value);
    if (ec) {
        throw std::runtime_error("Failed to serialize retention policy");
    }
    co_await kvPut(key, value);
}

seastar::future<std::optional<RetentionPolicy>> NativeIndex::getRetentionPolicy(const std::string& measurement) {
    if (shardId_ != 0) co_return std::nullopt;

    auto key = ke::encodeRetentionPolicyKey(measurement);
    auto val = kvGet(key);
    if (!val.has_value()) co_return std::nullopt;

    RetentionPolicy policy;
    auto ec = glz::read_json(policy, *val);
    if (ec) co_return std::nullopt;
    co_return policy;
}

seastar::future<std::vector<RetentionPolicy>> NativeIndex::getAllRetentionPolicies() {
    if (shardId_ != 0) co_return std::vector<RetentionPolicy>{};

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
    if (shardId_ != 0) co_return false;

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
    if (shardId_ != 0) co_return 0;

    size_t count = 0;
    std::string prefix(1, static_cast<char>(SERIES_METADATA));
    kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
        ++count;
        return true;
    });
    co_return count;
}

seastar::future<> NativeIndex::compact() {
    if (shardId_ != 0) co_return;

    // Wait for any in-flight background flush, then flush active memtable
    co_await waitForFlush();
    if (memtable_ && !memtable_->empty()) {
        co_await flushMemTable();
    }

    // Close all SSTable readers before compaction (compaction deletes files)
    for (auto& reader : sstableReaders_) {
        co_await reader->close();
    }
    sstableReaders_.clear();

    co_await compaction_->compactAll();
    co_await refreshSSTables();

    // Clear measurement series cache — will be repopulated on demand
    measurementSeriesCache_.clear();
}

// ============================================================================
// indexInsert template (non-virtual convenience method)
// ============================================================================

template <class T>
seastar::future<SeriesId128> NativeIndex::indexInsert(const TimeStarInsert<T>& insert) {
    SeriesId128 seriesId = co_await getOrCreateSeriesId(insert.measurement, insert.getTags(), insert.field);

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

// Explicit template instantiations
template seastar::future<SeriesId128> NativeIndex::indexInsert<double>(const TimeStarInsert<double>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<bool>(const TimeStarInsert<bool>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<std::string>(const TimeStarInsert<std::string>& insert);
template seastar::future<SeriesId128> NativeIndex::indexInsert<int64_t>(const TimeStarInsert<int64_t>& insert);

}  // namespace timestar::index
