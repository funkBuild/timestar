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

    // Flush MemTable to SSTable before closing (data also preserved in WAL)
    try {
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

seastar::future<std::optional<std::string>> NativeIndex::kvGet(const std::string& key) {
    // 1. Check MemTable first (newest)
    auto memResult = memtable_->get(key);
    if (memResult.has_value()) {
        co_return std::string(*memResult);
    }
    // Check if it's a tombstone
    if (memtable_->isTombstone(key)) {
        co_return std::nullopt;
    }

    // 2. Check SSTables (newest to oldest)
    for (auto it = sstableReaders_.rbegin(); it != sstableReaders_.rend(); ++it) {
        auto result = co_await (*it)->get(key);
        if (result.has_value()) {
            co_return result;
        }
    }

    co_return std::nullopt;
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

seastar::future<> NativeIndex::kvPrefixScan(const std::string& prefix, ScanCallback fn) {
    // For simplicity, iterate MemTable directly for prefix scans.
    // In a full implementation, this would merge MemTable + SSTable iterators.
    // For now, we check MemTable first, then fall through to SSTables.

    // Collect all results from MemTable
    std::map<std::string, std::optional<std::string>> merged;

    // MemTable entries
    auto memIt = memtable_->newIterator();
    memIt.seek(prefix);
    while (memIt.valid() && memIt.key().substr(0, prefix.size()) == prefix) {
        if (memIt.isTombstone()) {
            merged[std::string(memIt.key())] = std::nullopt;  // Tombstone
        } else {
            merged[std::string(memIt.key())] = std::string(memIt.value());
        }
        memIt.next();
    }

    // SSTable entries (oldest to newest, so newer overwrites older)
    for (auto& reader : sstableReaders_) {
        auto it = co_await reader->newIterator();
        co_await it->seek(prefix);
        while (it->valid() && it->key().substr(0, prefix.size()) == prefix) {
            auto key = std::string(it->key());
            // Only add if not already in merged (MemTable wins, then newer SSTables)
            if (merged.find(key) == merged.end()) {
                merged[key] = std::string(it->value());
            }
            co_await it->next();
        }
    }

    // Call the callback for all live entries
    for (const auto& [key, value] : merged) {
        if (value.has_value()) {
            if (!fn(key, *value)) break;
        }
    }
}

seastar::future<> NativeIndex::maybeFlushMemTable() {
    auto usage = memtable_->approximateMemoryUsage();
    auto threshold = timestar::config().index.write_buffer_size;
    if (usage >= threshold) {
        ::native_index_log.info("Flushing MemTable: {} bytes >= {} threshold", usage, threshold);
        co_await flushMemTable();
    }
}

seastar::future<> NativeIndex::flushMemTable() {
    if (memtable_->empty()) co_return;

    ::native_index_log.info("Flushing MemTable: {} entries, {} approx bytes", memtable_->size(),
                            memtable_->approximateMemoryUsage());

    // Count live (non-tombstone) entries
    size_t liveCount = 0;
    {
        auto countIt = memtable_->newIterator();
        countIt.seekToFirst();
        while (countIt.valid()) {
            if (!countIt.isTombstone()) ++liveCount;
            countIt.next();
        }
    }
    ::native_index_log.info("MemTable has {} live entries (excluding tombstones)", liveCount);
    if (liveCount == 0) {
        ::native_index_log.info("Skipping SSTable flush — no live entries");
        memtable_ = std::make_unique<MemTable>();
        co_return;
    }

    // Write MemTable to a new SSTable
    uint64_t fileNum = manifest_->nextFileNumber();
    auto path = sstFilename(fileNum);
    auto writer = co_await SSTableWriter::create(path, timestar::config().index.block_size,
                                                  timestar::config().index.bloom_filter_bits);

    auto it = memtable_->newIterator();
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
    ::native_index_log.info("Adding file to manifest");
    co_await manifest_->addFile(meta);

    // Rotate WAL
    auto oldWal = co_await wal_->rotate();
    co_await IndexWAL::deleteFile(oldWal);

    // Reset MemTable
    memtable_ = std::make_unique<MemTable>();

    // Refresh SSTable readers
    co_await refreshSSTables();

    // Maybe compact
    co_await compaction_->maybeCompact();
    co_await refreshSSTables();
}

// ============================================================================
// Series cache (same two-generation design as LevelDBIndex)
// ============================================================================

bool NativeIndex::seriesCacheContains(const std::string& key) const {
    return indexedSeriesCache_.count(key) || indexedSeriesCacheRetired_.count(key);
}

void NativeIndex::seriesCacheInsert(std::string key) {
    indexedSeriesCache_.insert(std::move(key));
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

seastar::future<SeriesId128> NativeIndex::getOrCreateSeriesId(std::string measurement,
                                                                std::map<std::string, std::string> tags,
                                                                std::string field) {
    if (shardId_ != 0) {
        throw std::runtime_error("getOrCreateSeriesId must be called on shard 0");
    }

    std::string seriesKeyStr = ke::encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

    // Fast path: already indexed
    if (seriesCacheContains(seriesKeyStr)) {
        co_return seriesId;
    }

    // Check if series exists in storage
    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    auto existing = co_await kvGet(metaKey);
    if (existing.has_value()) {
        seriesCacheInsert(std::move(seriesKeyStr));
        co_return seriesId;
    }

    // New series — create all index entries in a single batch
    IndexWriteBatch batch;

    // Series metadata
    SeriesMetadata metadata{measurement, tags, field};
    batch.put(metaKey, ke::encodeSeriesMetadata(metadata));

    // Measurement series index
    batch.put(ke::encodeMeasurementSeriesKey(measurement, seriesId), "");

    // Measurement field series index
    batch.put(ke::encodeMeasurementFieldSeriesKey(measurement, field, seriesId), "");

    // Tag index and group-by index
    std::string encodedSeriesId = ke::encodeSeriesId(seriesId);
    for (const auto& [tagKey, tagValue] : tags) {
        // TAG_INDEX
        std::string tagIdxKey;
        tagIdxKey.push_back(TAG_INDEX);
        tagIdxKey += measurement;
        tagIdxKey.push_back('\0');
        tagIdxKey += tagKey;
        tagIdxKey.push_back('\0');
        tagIdxKey += tagValue;
        tagIdxKey.push_back('\0');
        tagIdxKey += encodedSeriesId;
        batch.put(tagIdxKey, encodedSeriesId);

        // GROUP_BY_INDEX (same key with different prefix)
        std::string groupByKey = tagIdxKey;
        groupByKey[0] = GROUP_BY_INDEX;
        batch.put(groupByKey, encodedSeriesId);
    }

    co_await kvWriteBatch(batch);
    seriesCacheInsert(std::move(seriesKeyStr));

    // Invalidate discovery cache for this measurement
    invalidateDiscoveryCache(measurement);

    // Update measurement series cache
    if (measurementSeriesCache_.count(measurement)) {
        measurementSeriesCache_[measurement].push_back(seriesId);
    }

    // Also add field/tag metadata
    co_await addFieldsAndTags(measurement, field, tags);

    co_return seriesId;
}

seastar::future<std::optional<SeriesId128>> NativeIndex::getSeriesId(
    const std::string& measurement, const std::map<std::string, std::string>& tags, const std::string& field) {
    if (shardId_ != 0) co_return std::nullopt;

    std::string seriesKeyStr = ke::encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

    auto metaKey = ke::encodeSeriesMetadataKey(seriesId);
    auto existing = co_await kvGet(metaKey);
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
    auto val = co_await kvGet(key);
    if (!val.has_value()) co_return std::nullopt;

    auto metadata = ke::decodeSeriesMetadata(*val);
    seriesMetadataCache_.put(seriesId, metadata);
    co_return metadata;
}

seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>> NativeIndex::getSeriesMetadataBatch(
    const std::vector<SeriesId128>& seriesIds) {
    std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>> results;
    results.reserve(seriesIds.size());

    for (const auto& id : seriesIds) {
        auto meta = co_await getSeriesMetadata(id);
        results.push_back({id, std::move(meta)});
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
        auto val = co_await kvGet(key);
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
        auto val = co_await kvGet(key);
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
        auto val = co_await kvGet(key);
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

    auto val = co_await kvGet(ke::encodeFieldTypeKey(measurement, field));
    co_return val.value_or("");
}

seastar::future<std::set<std::string>> NativeIndex::getAllMeasurements() {
    if (shardId_ != 0) co_return std::set<std::string>{};

    std::set<std::string> result;
    std::string prefix(1, static_cast<char>(MEASUREMENT_FIELDS));
    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
        result.insert(std::string(key.substr(1)));  // Skip prefix byte
        return true;
    });
    co_return result;
}

seastar::future<std::set<std::string>> NativeIndex::getFields(const std::string& measurement) {
    if (shardId_ != 0) co_return std::set<std::string>{};

    auto& cached = fieldsCache_[measurement];
    if (!cached.empty()) co_return cached;

    auto val = co_await kvGet(ke::encodeMeasurementFieldsKey(measurement));
    if (val.has_value()) {
        cached = ke::decodeStringSet(*val);
    }
    co_return cached;
}

seastar::future<std::set<std::string>> NativeIndex::getTags(const std::string& measurement) {
    if (shardId_ != 0) co_return std::set<std::string>{};

    auto& cached = tagsCache_[measurement];
    if (!cached.empty()) co_return cached;

    auto val = co_await kvGet(ke::encodeMeasurementTagsKey(measurement));
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

    auto val = co_await kvGet(ke::encodeTagValuesKey(measurement, tagKey));
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

seastar::future<std::expected<std::vector<IndexBackend::SeriesWithMetadata>, SeriesLimitExceeded>>
NativeIndex::findSeriesWithMetadata(const std::string& measurement,
                                     const std::map<std::string, std::string>& tagFilters,
                                     const std::unordered_set<std::string>& fieldFilter, size_t maxSeries) {
    if (shardId_ != 0) co_return std::vector<SeriesWithMetadata>{};

    auto findResult = co_await findSeries(measurement, tagFilters, maxSeries);
    if (!findResult.has_value()) {
        co_return std::unexpected(findResult.error());
    }

    std::vector<SeriesWithMetadata> results;
    for (const auto& seriesId : *findResult) {
        auto meta = co_await getSeriesMetadata(seriesId);
        if (meta.has_value()) {
            if (!fieldFilter.empty() && !fieldFilter.count(meta->field)) {
                continue;
            }
            results.push_back({seriesId, std::move(*meta)});
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
    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view value) {
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
    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view value) {
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

    auto val = co_await kvGet(key);
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

    co_await kvPrefixScan(prefix, [&](std::string_view key, std::string_view) {
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
    auto val = co_await kvGet(key);
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

    co_await kvPrefixScan(prefix, [&](std::string_view, std::string_view value) {
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
    auto existing = co_await kvGet(key);
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
    co_await kvPrefixScan(prefix, [&](std::string_view, std::string_view) {
        ++count;
        return true;
    });
    co_return count;
}

seastar::future<> NativeIndex::compact() {
    if (shardId_ != 0) co_return;

    // Flush MemTable to SSTable if non-empty
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
