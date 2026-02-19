#include "leveldb_index.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_matcher.hpp"
#include "tsm.hpp"  // for TSMValueType definition

#include <algorithm>
#include <charconv>
#include <cstring>
#include <filesystem>
#include <regex>
#include <string_view>
#include <type_traits>

#include <leveldb/comparator.h>
#include <leveldb/filter_policy.h>
#include <seastar/core/thread.hh>

LevelDBIndex::LevelDBIndex(int _shardId) : shardId(_shardId) {
    indexPath = "shard_" + std::to_string(shardId) + "/index";
}

LevelDBIndex::~LevelDBIndex() {
    // DB must be destroyed before the filter policy since LevelDB may
    // reference the policy during shutdown.
    if (db) {
        delete db.release();
    }
    filterPolicy_.reset();
}

seastar::future<> LevelDBIndex::open() {
    // Only shard 0 opens LevelDB for metadata storage
    // Other shards will delegate metadata operations to shard 0
    if (shardId != 0) {
        tsdb::index_log.info("Shard {} skipping LevelDB index opening - metadata centralized on shard 0", shardId);
        co_return;
    }
    
    tsdb::index_log.info("Shard 0 opening centralized metadata LevelDB index");
    
    // Create index directory if it doesn't exist.
    // Wrap in seastar::async to avoid blocking the reactor thread.
    co_await seastar::async([this]() {
        std::filesystem::create_directories(indexPath);
    });
    
    const auto& idxCfg = tsdb::config().index;

    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.compression = leveldb::kSnappyCompression;
    options.block_size = idxCfg.block_size;
    options.write_buffer_size = idxCfg.write_buffer_size;
    options.max_open_files = idxCfg.max_open_files;
    options.max_file_size = idxCfg.max_file_size;

    // Use bloom filter for faster lookups (owned by filterPolicy_ unique_ptr for RAII cleanup)
    filterPolicy_.reset(leveldb::NewBloomFilterPolicy(idxCfg.bloom_filter_bits));
    options.filter_policy = filterPolicy_.get();
    
    leveldb::DB* dbPtr;
    leveldb::Status status = leveldb::DB::Open(options, indexPath, &dbPtr);
    
    if (!status.ok()) {
        tsdb::index_log.error("Failed to open LevelDB index: {}", status.ToString());
        throw std::runtime_error("Failed to open LevelDB index: " + status.ToString());
    }
    
    db.reset(dbPtr);
    tsdb::index_log.info("LevelDB index opened at: {}", indexPath);
    
    // SeriesId128 is generated deterministically from SeriesKey - no counter needed
}

seastar::future<> LevelDBIndex::close() {
    if (db) {
        delete db.release();
        db.reset();
    }
    // Free the bloom filter policy after the DB is closed
    filterPolicy_.reset();
    co_return;
}

// loadSeriesCounter method removed - no longer needed with deterministic SeriesId128

// --- Two-generation series cache helpers ---
//
// These methods implement an amortized incremental eviction strategy for the
// indexedSeriesCache. Instead of clearing all ~1M entries at once (which
// deallocates ~1M strings synchronously on the reactor thread), we:
//   1. Swap the full active cache into a retired slot (O(1) pointer swap)
//   2. Drain the retired cache in small batches (EVICTION_BATCH_SIZE per insert)
//
// No locks needed: Seastar's shard-per-core model guarantees single-threaded access.

bool LevelDBIndex::seriesCacheContains(const std::string& key) const {
    return indexedSeriesCache_.count(key) > 0
        || indexedSeriesCacheRetired_.count(key) > 0;
}

void LevelDBIndex::seriesCacheInsert(const std::string& key) {
    indexedSeriesCache_.insert(key);
    seriesCacheEvictIncremental();
}

void LevelDBIndex::seriesCacheInsert(std::string&& key) {
    indexedSeriesCache_.insert(std::move(key));
    seriesCacheEvictIncremental();
}

void LevelDBIndex::seriesCacheEvictIncremental() {
    // Phase 1: Drain a small batch from the retired cache (amortized cost).
    // Each insert pays at most EVICTION_BATCH_SIZE string deallocations (~5-10us).
    if (!indexedSeriesCacheRetired_.empty()) {
        auto it = indexedSeriesCacheRetired_.begin();
        for (size_t i = 0; i < EVICTION_BATCH_SIZE && it != indexedSeriesCacheRetired_.end(); ++i) {
            it = indexedSeriesCacheRetired_.erase(it);
        }
    }

    // Phase 2: If the active cache exceeds the threshold, retire it.
    // The swap is O(1) -- just pointer/size swaps, no element movement.
    // The previous retired cache should be empty or nearly empty by now
    // (it has been draining since the last swap). In the rare case it isn't
    // fully drained, we clear whatever remains (at most a small residual).
    if (indexedSeriesCache_.size() > maxSeriesCacheSize) {
        // Any residual retired entries are force-cleared. This should be a no-op
        // in practice: the retired cache drains at EVICTION_BATCH_SIZE per insert,
        // so it is fully drained after ceil((maxSeriesCacheSize+1)/EVICTION_BATCH_SIZE)
        // inserts (e.g., ~3907 inserts for 1M/256). The active cache takes
        // maxSeriesCacheSize+1 inserts to refill, which is always much larger.
        //
        // For maxSeriesCacheSize = 0 (cache disabled), retired has at most 1 entry,
        // so this clear is O(1).
        indexedSeriesCacheRetired_.clear();

        indexedSeriesCache_.swap(indexedSeriesCacheRetired_);
        // Active is now empty. Retired holds the old entries for incremental draining.
    }
}

std::string LevelDBIndex::encodeSeriesKey(const std::string& measurement,
                                         const std::map<std::string, std::string>& tags,
                                         const std::string& field) {
    std::string key;
    key.push_back(static_cast<char>(SERIES_INDEX));
    
    // measurement,tag1=value1,tag2=value2 field
    key += measurement;
    
    // Sort tags for consistent ordering
    for (const auto& tag : tags) {
        key += "," + tag.first + "=" + tag.second;
    }
    
    key += " " + field;
    return key;
}

std::string LevelDBIndex::encodeMeasurementFieldsKey(const std::string& measurement) {
    std::string key;
    key.push_back(static_cast<char>(MEASUREMENT_FIELDS));
    key += measurement;
    return key;
}

std::string LevelDBIndex::encodeMeasurementTagsKey(const std::string& measurement) {
    std::string key;
    key.push_back(static_cast<char>(MEASUREMENT_TAGS));
    key += measurement;
    return key;
}

std::string LevelDBIndex::encodeTagValuesKey(const std::string& measurement, const std::string& tagKey) {
    std::string key;
    key.push_back(static_cast<char>(TAG_VALUES));
    key += measurement + "\x00" + tagKey; // Use null separator
    return key;
}

std::string LevelDBIndex::encodeStringSet(const std::set<std::string>& strings) {
    // Pre-calculate total size to avoid repeated allocations
    size_t totalSize = 0;
    for (const auto& str : strings) {
        if (str.length() > UINT16_MAX) {
            throw std::runtime_error("String length " + std::to_string(str.length()) +
                                     " exceeds maximum encodable length of " + std::to_string(UINT16_MAX));
        }
        totalSize += sizeof(uint16_t) + str.length();
    }

    std::string result;
    result.reserve(totalSize);
    for (const auto& str : strings) {
        // Length-prefixed encoding: 2-byte little-endian length + string bytes
        uint16_t len = static_cast<uint16_t>(str.length());
        result.append(reinterpret_cast<const char*>(&len), sizeof(len));
        result.append(str.data(), len);
    }
    return result;
}

std::set<std::string> LevelDBIndex::decodeStringSet(const std::string& encoded) {
    std::set<std::string> result;
    const char* data = encoded.data();
    size_t size = encoded.size();
    size_t offset = 0;

    while (offset + sizeof(uint16_t) <= size) {
        uint16_t len;
        std::memcpy(&len, data + offset, sizeof(len));
        offset += sizeof(uint16_t);

        if (offset + len > size) break;  // truncated data

        result.emplace(data + offset, len);
        offset += len;
    }

    return result;
}

std::string LevelDBIndex::encodeSeriesId(const SeriesId128& seriesId) {
    return seriesId.toBytes();
}

SeriesId128 LevelDBIndex::decodeSeriesId(const std::string& encoded) {
    return SeriesId128::fromBytes(encoded);
}

SeriesId128 LevelDBIndex::decodeSeriesId(const char* data, size_t len) {
    return SeriesId128::fromBytes(data, len);
}

std::string LevelDBIndex::encodeSeriesMetadataKey(const SeriesId128& seriesId) {
    std::string key;
    key.push_back(static_cast<char>(SERIES_METADATA));
    key += encodeSeriesId(seriesId);
    return key;
}

std::string LevelDBIndex::encodeFieldTypeKey(const std::string& measurement, const std::string& field) {
    std::string key;
    key.push_back(static_cast<char>(FIELD_TYPE));
    key += measurement;
    key.push_back('\0');
    key += field;
    return key;
}

std::string LevelDBIndex::encodeMeasurementSeriesKey(const std::string& measurement, const SeriesId128& seriesId) {
    std::string key;
    key.reserve(1 + measurement.size() + 1 + 16);
    key.push_back(static_cast<char>(MEASUREMENT_SERIES));
    key += measurement;
    key.push_back('\0');
    key += encodeSeriesId(seriesId);
    return key;
}

std::string LevelDBIndex::encodeMeasurementSeriesPrefix(const std::string& measurement) {
    std::string prefix;
    prefix.reserve(1 + measurement.size() + 1);
    prefix.push_back(static_cast<char>(MEASUREMENT_SERIES));
    prefix += measurement;
    prefix.push_back('\0');
    return prefix;
}

std::string LevelDBIndex::encodeSeriesMetadata(const SeriesMetadata& metadata) {
    try {
        // Add defensive checks to prevent corruption-related crashes
        if (metadata.measurement.length() > 10000 || metadata.field.length() > 10000) {
            throw std::runtime_error("SeriesMetadata has suspiciously long strings - possible corruption");
        }

        if (metadata.tags.size() > 1000) {
            throw std::runtime_error("SeriesMetadata has too many tags - possible corruption");
        }

        std::string tagCountStr = std::to_string(metadata.tags.size());

        // Pre-calculate total size to avoid repeated allocations
        size_t totalSize = metadata.measurement.size() + 1
                         + metadata.field.size() + 1
                         + tagCountStr.size() + 1;

        for (const auto& [k, v] : metadata.tags) {
            if (k.length() > 1000 || v.length() > 1000) {
                throw std::runtime_error("SeriesMetadata tag key/value too long - possible corruption");
            }
            totalSize += k.size() + 1 + v.size() + 1;
        }

        std::string result;
        result.reserve(totalSize);

        result.append(metadata.measurement);
        result.push_back('\0');
        result.append(metadata.field);
        result.push_back('\0');
        result.append(tagCountStr);
        result.push_back('\0');

        for (const auto& [k, v] : metadata.tags) {
            result.append(k);
            result.push_back('\0');
            result.append(v);
            result.push_back('\0');
        }

        return result;
    } catch (const std::exception& e) {
        tsdb::index_log.error("encodeSeriesMetadata failed: {}", e.what());
        throw std::runtime_error(std::string("encodeSeriesMetadata failed: ") + e.what());
    }
}

SeriesMetadata LevelDBIndex::decodeSeriesMetadata(const std::string& encoded) {
    return decodeSeriesMetadata(encoded.data(), encoded.size());
}

SeriesMetadata LevelDBIndex::decodeSeriesMetadata(const char* rawData, size_t rawLen) {
    SeriesMetadata metadata;
    std::string_view data(rawData, rawLen);
    size_t pos = 0;

    // Helper: extract next null-delimited field as string_view
    auto nextField = [&]() -> std::string_view {
        if (pos >= data.size()) {
            return {};
        }
        size_t end = data.find('\0', pos);
        if (end == std::string_view::npos) {
            // No delimiter found - take remaining data
            std::string_view field = data.substr(pos);
            pos = data.size();
            return field;
        }
        std::string_view field = data.substr(pos, end - pos);
        pos = end + 1;
        return field;
    };

    metadata.measurement = std::string(nextField());
    metadata.field = std::string(nextField());

    std::string_view sizeStr = nextField();
    if (sizeStr.empty()) {
        return metadata;  // No tags
    }

    size_t tagCount = 0;
    auto [ptr, ec] = std::from_chars(sizeStr.data(), sizeStr.data() + sizeStr.size(), tagCount);
    if (ec != std::errc()) {
        throw std::runtime_error("Failed to parse tag count in series metadata");
    }

    if (tagCount > 1000) {
        throw std::runtime_error("tagCount " + std::to_string(tagCount) +
                                 " exceeds maximum of 1000 - possible data corruption");
    }

    for (size_t i = 0; i < tagCount; ++i) {
        std::string_view key = nextField();
        std::string_view value = nextField();
        metadata.tags[std::string(key)] = std::string(value);
    }

    return metadata;
}

seastar::future<SeriesId128> LevelDBIndex::getOrCreateSeriesId(std::string measurement,
                                                              std::map<std::string, std::string> tags,
                                                              std::string field) {
    // Only shard 0 handles series metadata indexing
    if (shardId != 0) {
        throw std::runtime_error("getOrCreateSeriesId called on non-zero shard " + std::to_string(shardId) + " - metadata operations only supported on shard 0");
    }

    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getOrCreateSeriesId");
    }

    // Generate SeriesId128 deterministically from the index-domain series key.
    // Note: encodeSeriesKey() produces a 0x01-prefixed key that differs from
    // TSDBInsert::seriesKey(), so external IDs cannot be reused here.
    std::string seriesKeyStr = encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

    // Fast path: Check in-memory cache (no I/O).
    // Checks both active and retired generations (see seriesCacheContains).
    // No mutex needed: Seastar's shard-per-core model guarantees single-threaded access.
    if (seriesCacheContains(seriesKeyStr)) {
        // Already indexed - just return the computed SeriesId
        LOG_INSERT_PATH(tsdb::index_log, trace, "[INDEX] Cache hit for series: '{}'", seriesKeyStr);
        co_return seriesId;
    }

    // Cache miss - need to check if it exists in LevelDB (handles restarts).
    // Check metadata directly using the already-computed seriesId instead of
    // calling getSeriesId() which would redundantly recompute encodeSeriesKey + XXH3.
    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Cache miss for series: '{}', checking LevelDB", seriesKeyStr);
    auto existingMetadata = co_await getSeriesMetadata(seriesId);
    if (existingMetadata.has_value()) {
        // Exists in LevelDB but not in cache (likely after restart) - add to cache.
        // seriesCacheInsert handles incremental eviction of the retired generation.
        seriesCacheInsert(seriesKeyStr);
        LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Series found in LevelDB, added to cache: '{}'", seriesKeyStr);
        co_return seriesId;
    }

    // Series doesn't exist - create it
    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Creating new series index: '{}'", seriesKeyStr);

    leveldb::WriteBatch batch;

    // Store series metadata (seriesId -> metadata)
    // Note: We don't store seriesKey->seriesId mapping because seriesId is deterministically
    // calculated from seriesKey via XXH3_128bits hash (see SeriesId128::fromSeriesKey)
    SeriesMetadata metadata;
    metadata.measurement = measurement;
    metadata.tags = tags;
    metadata.field = field;
    std::string metadataKey = encodeSeriesMetadataKey(seriesId);
    batch.Put(metadataKey, encodeSeriesMetadata(metadata));

    // MEASUREMENT_SERIES index entry for fast measurement -> series lookup
    batch.Put(encodeMeasurementSeriesKey(measurement, seriesId), "");

    // Add TAG_INDEX entries for efficient tag-based queries
    for (const auto& tag : tags) {
        const std::string& tagKey = tag.first;
        const std::string& tagValue = tag.second;

        // TAG_INDEX key includes series ID to make it unique per series
        std::string tagIndexKey;
        tagIndexKey.push_back(TAG_INDEX);
        tagIndexKey.append(measurement);
        tagIndexKey.push_back('\0');
        tagIndexKey.append(tagKey);
        tagIndexKey.push_back('\0');
        tagIndexKey.append(tagValue);
        tagIndexKey.push_back('\0');
        tagIndexKey.append(encodeSeriesId(seriesId));

        batch.Put(tagIndexKey, encodeSeriesId(seriesId));

        // GROUP_BY_INDEX also needs unique keys per series
        std::string groupByKey;
        groupByKey.push_back(GROUP_BY_INDEX);
        groupByKey.append(measurement);
        groupByKey.push_back('\0');
        groupByKey.append(tagKey);
        groupByKey.push_back('\0');
        groupByKey.append(tagValue);
        groupByKey.push_back('\0');
        groupByKey.append(encodeSeriesId(seriesId));

        batch.Put(groupByKey, encodeSeriesId(seriesId));
    }

    // Write the batch (offload blocking I/O to Seastar thread pool)
    auto status = co_await seastar::async([this, &batch] {
        return db->Write(leveldb::WriteOptions(), &batch);
    });
    if (!status.ok()) {
        throw std::runtime_error("Failed to write series index: " + status.ToString());
    }

    // Update measurement series cache if it has been populated for this measurement
    auto msIt = measurementSeriesCache.find(measurement);
    if (msIt != measurementSeriesCache.end()) {
        msIt->second.push_back(seriesId);
    }

    // Update metadata indexes (batched: at most 2 co_awaits instead of N+1)
    co_await addFieldsAndTags(measurement, field, tags);

    tsdb::index_log.debug("Created new series ID {} for key: {}", seriesId.toHex(), seriesKeyStr);

    // Add to cache now that it's written. Move is safe since seriesKeyStr is
    // not used after this point. seriesCacheInsert handles incremental eviction.
    seriesCacheInsert(std::move(seriesKeyStr));
    co_return seriesId;
}

seastar::future<std::optional<SeriesId128>> LevelDBIndex::getSeriesId(const std::string& measurement,
                                                                     const std::map<std::string, std::string>& tags,
                                                                     const std::string& field) {
    if (!db) {
        throw std::runtime_error("Database not opened before getSeriesId");
    }

    // Calculate seriesId deterministically from series key (no DB lookup needed!)
    std::string seriesKey = encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Check if metadata exists for this seriesId to determine if series exists
    auto metadata = co_await getSeriesMetadata(seriesId);

    if (metadata.has_value()) {
        co_return seriesId;
    } else {
        co_return std::nullopt;
    }
}

seastar::future<> LevelDBIndex::addField(const std::string& measurement, const std::string& field) {
    // Metadata operations only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("addField called on non-zero shard " + std::to_string(shardId) + " - metadata operations only supported on shard 0");
    }

    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for addField");
    }

    // Use in-memory cache to avoid read-modify-write race across co_await points.
    // All synchronous operations on the cache complete atomically (no interleaving).
    auto cacheIt = fieldsCache.find(measurement);
    if (cacheIt != fieldsCache.end()) {
        // Cache hit: check if field already present (common fast path)
        if (cacheIt->second.count(field) > 0) {
            co_return; // Already cached, nothing to do
        }
        cacheIt->second.insert(field);
    } else {
        // Cache miss: load from LevelDB and populate cache
        auto fields = co_await getFields(measurement);
        fields.insert(field);
        fieldsCache[measurement] = std::move(fields);
    }

    // Write the full set to LevelDB (offload blocking I/O to Seastar thread pool)
    std::string key = encodeMeasurementFieldsKey(measurement);
    std::string encodedFields = encodeStringSet(fieldsCache[measurement]);
    auto status = co_await seastar::async([this, &key, &encodedFields] {
        return db->Put(leveldb::WriteOptions(), key, encodedFields);
    });

    if (!status.ok()) {
        throw std::runtime_error("Failed to add field: " + status.ToString());
    }
}

seastar::future<> LevelDBIndex::addTag(const std::string& measurement, const std::string& tagKey, const std::string& tagValue) {
    // Metadata operations only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("addTag called on non-zero shard " + std::to_string(shardId) + " - metadata operations only supported on shard 0");
    }

    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for addTag");
    }

    // Use in-memory caches to avoid read-modify-write race across co_await points.
    // Check tags cache
    bool tagsNeedWrite = false;
    auto tagsCacheIt = tagsCache.find(measurement);
    if (tagsCacheIt != tagsCache.end()) {
        if (tagsCacheIt->second.count(tagKey) == 0) {
            tagsCacheIt->second.insert(tagKey);
            tagsNeedWrite = true;
        }
    } else {
        // Cache miss: load from LevelDB and populate cache
        auto tags = co_await getTags(measurement);
        tagsNeedWrite = (tags.count(tagKey) == 0);
        tags.insert(tagKey);
        tagsCache[measurement] = std::move(tags);
    }

    // Check tag values cache
    bool tagValuesNeedWrite = false;
    std::string tagValuesCacheKey = measurement + std::string(1, '\0') + tagKey;
    auto tvCacheIt = tagValuesCache.find(tagValuesCacheKey);
    if (tvCacheIt != tagValuesCache.end()) {
        if (tvCacheIt->second.count(tagValue) == 0) {
            tvCacheIt->second.insert(tagValue);
            tagValuesNeedWrite = true;
        }
    } else {
        // Cache miss: load from LevelDB and populate cache
        auto tagValues = co_await getTagValues(measurement, tagKey);
        tagValuesNeedWrite = (tagValues.count(tagValue) == 0);
        tagValues.insert(tagValue);
        tagValuesCache[tagValuesCacheKey] = std::move(tagValues);
    }

    // Only write to LevelDB if something changed
    if (!tagsNeedWrite && !tagValuesNeedWrite) {
        co_return;
    }

    leveldb::WriteBatch batch;

    if (tagsNeedWrite) {
        std::string tagsKey = encodeMeasurementTagsKey(measurement);
        batch.Put(tagsKey, encodeStringSet(tagsCache[measurement]));
    }

    if (tagValuesNeedWrite) {
        std::string tagValuesKey = encodeTagValuesKey(measurement, tagKey);
        batch.Put(tagValuesKey, encodeStringSet(tagValuesCache[tagValuesCacheKey]));
    }

    // Offload blocking LevelDB I/O to Seastar thread pool
    auto status = co_await seastar::async([this, &batch] {
        return db->Write(leveldb::WriteOptions(), &batch);
    });
    if (!status.ok()) {
        throw std::runtime_error("Failed to add tag: " + status.ToString());
    }
}

seastar::future<> LevelDBIndex::addFieldsAndTags(const std::string& measurement,
                                                  const std::string& field,
                                                  const std::map<std::string, std::string>& tags) {
    // Metadata operations only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("addFieldsAndTags called on non-zero shard " + std::to_string(shardId) + " - metadata operations only supported on shard 0");
    }
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for addFieldsAndTags");
    }

    // Phase 1: Check in-memory caches synchronously (no co_await needed).
    // Identify which caches need to be loaded from LevelDB (cache misses).
    bool fieldsCacheMiss = (fieldsCache.find(measurement) == fieldsCache.end());
    bool fieldAlreadyCached = false;
    if (!fieldsCacheMiss) {
        fieldAlreadyCached = (fieldsCache[measurement].count(field) > 0);
    }

    // For tags, track which tag keys and tag values need cache loading.
    // tagKey -> whether tagsCache has this measurement loaded
    bool tagsCacheMiss = (tagsCache.find(measurement) == tagsCache.end());

    // For each tag, check tagValuesCache
    struct TagCacheStatus {
        std::string tagKey;
        std::string tagValue;
        std::string tvCacheKey;   // measurement + '\0' + tagKey
        bool tvCacheMiss;         // tagValuesCache miss for this tagKey
        bool tvAlreadyCached;     // tagValue already in cache
    };
    std::vector<TagCacheStatus> tagStatuses;
    tagStatuses.reserve(tags.size());

    for (const auto& [tagKey, tagValue] : tags) {
        std::string tvCacheKey = measurement + std::string(1, '\0') + tagKey;
        bool tvMiss = (tagValuesCache.find(tvCacheKey) == tagValuesCache.end());
        bool tvCached = false;
        if (!tvMiss) {
            tvCached = (tagValuesCache[tvCacheKey].count(tagValue) > 0);
        }
        tagStatuses.push_back({tagKey, tagValue, std::move(tvCacheKey), tvMiss, tvCached});
    }

    // Quick check: if everything is already cached and present, nothing to do
    bool allCached = !fieldsCacheMiss && fieldAlreadyCached && !tagsCacheMiss;
    if (allCached) {
        bool allTagsCached = true;
        bool allTagKeysPresent = true;
        for (const auto& ts : tagStatuses) {
            if (ts.tvCacheMiss || !ts.tvAlreadyCached) {
                allTagsCached = false;
            }
            if (tagsCache[measurement].count(ts.tagKey) == 0) {
                allTagKeysPresent = false;
            }
        }
        if (allTagsCached && allTagKeysPresent) {
            co_return;  // Everything already indexed, nothing to do
        }
    }

    // Phase 2: Load any cache misses from LevelDB in a single async block.
    // Collect the unique keys we need to load.
    bool needFieldsLoad = fieldsCacheMiss;
    bool needTagsLoad = tagsCacheMiss;
    // Collect unique tagKeys that need tagValues loaded
    std::vector<std::pair<std::string, std::string>> tvLoadsNeeded; // (tagKey, tvCacheKey)
    for (const auto& ts : tagStatuses) {
        if (ts.tvCacheMiss) {
            tvLoadsNeeded.emplace_back(ts.tagKey, ts.tvCacheKey);
        }
    }

    // Deduplicate tvLoadsNeeded by tvCacheKey (same tagKey across multiple tags)
    // Since tags is a map, tagKeys are unique, so no dedup needed.

    if (needFieldsLoad || needTagsLoad || !tvLoadsNeeded.empty()) {
        // Load all cache misses in a single seastar::async block (one co_await)
        struct CacheMissResults {
            std::set<std::string> fields;           // loaded if needFieldsLoad
            std::set<std::string> tagKeys;          // loaded if needTagsLoad
            // tagKey -> loaded tag values
            std::vector<std::pair<std::string, std::set<std::string>>> tagValues;
        };

        auto loaded = co_await seastar::async([this, &measurement, needFieldsLoad, needTagsLoad, &tvLoadsNeeded] {
            CacheMissResults result;
            leveldb::ReadOptions readOpts;

            if (needFieldsLoad) {
                std::string key = encodeMeasurementFieldsKey(measurement);
                std::string val;
                auto status = db->Get(readOpts, key, &val);
                if (status.ok()) {
                    result.fields = decodeStringSet(val);
                }
                // If NotFound, fields stays empty (new measurement)
            }

            if (needTagsLoad) {
                std::string key = encodeMeasurementTagsKey(measurement);
                std::string val;
                auto status = db->Get(readOpts, key, &val);
                if (status.ok()) {
                    result.tagKeys = decodeStringSet(val);
                }
            }

            for (const auto& [tagKey, tvCacheKey] : tvLoadsNeeded) {
                std::string key = encodeTagValuesKey(measurement, tagKey);
                std::string val;
                auto status = db->Get(readOpts, key, &val);
                std::set<std::string> values;
                if (status.ok()) {
                    values = decodeStringSet(val);
                }
                result.tagValues.emplace_back(tvCacheKey, std::move(values));
            }

            return result;
        });

        // Populate caches back on the reactor thread (safe, single-threaded)
        if (needFieldsLoad) {
            fieldsCache[measurement] = std::move(loaded.fields);
        }
        if (needTagsLoad) {
            tagsCache[measurement] = std::move(loaded.tagKeys);
        }
        for (auto& [tvCacheKey, values] : loaded.tagValues) {
            tagValuesCache[tvCacheKey] = std::move(values);
        }
    }

    // Phase 3: Update caches and collect what needs to be written to LevelDB.
    leveldb::WriteBatch batch;
    bool batchHasData = false;

    // Check/add field
    bool fieldNeedsWrite = false;
    if (fieldsCache[measurement].insert(field).second) {
        fieldNeedsWrite = true;
    }

    // Check/add tag keys and tag values
    bool tagsNeedWrite = false;
    for (const auto& ts : tagStatuses) {
        // Check tag key
        if (tagsCache[measurement].insert(ts.tagKey).second) {
            tagsNeedWrite = true;
        }

        // Check tag value
        if (tagValuesCache[ts.tvCacheKey].insert(ts.tagValue).second) {
            // This specific tag value is new, write its set
            std::string tvKey = encodeTagValuesKey(measurement, ts.tagKey);
            batch.Put(tvKey, encodeStringSet(tagValuesCache[ts.tvCacheKey]));
            batchHasData = true;
        }
    }

    // Write fields set if changed
    if (fieldNeedsWrite) {
        std::string key = encodeMeasurementFieldsKey(measurement);
        batch.Put(key, encodeStringSet(fieldsCache[measurement]));
        batchHasData = true;
    }

    // Write tags set if changed
    if (tagsNeedWrite) {
        std::string key = encodeMeasurementTagsKey(measurement);
        batch.Put(key, encodeStringSet(tagsCache[measurement]));
        batchHasData = true;
    }

    // Phase 4: Single atomic write if anything changed (one co_await)
    if (batchHasData) {
        auto status = co_await seastar::async([this, &batch] {
            return db->Write(leveldb::WriteOptions(), &batch);
        });
        if (!status.ok()) {
            throw std::runtime_error("Failed to write batched fields/tags: " + status.ToString());
        }
    }

    co_return;
}

seastar::future<std::set<std::string>> LevelDBIndex::getAllMeasurements() {
    // Metadata queries only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("getAllMeasurements called on non-zero shard " + std::to_string(shardId) + " - metadata queries only supported on shard 0");
    }

    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getAllMeasurements");
    }

    // Offload blocking LevelDB iterator I/O to Seastar thread pool
    auto measurements = co_await seastar::async([this] {
        std::set<std::string> result;

        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));

        // Seek to start of MEASUREMENT_FIELDS keys
        std::string startKey;
        startKey.push_back(static_cast<char>(MEASUREMENT_FIELDS));
        it->Seek(startKey);

        while (it->Valid()) {
            leveldb::Slice key = it->key();

            // Check if still in MEASUREMENT_FIELDS range
            if (key.size() == 0 || static_cast<uint8_t>(key[0]) != MEASUREMENT_FIELDS) {
                break;
            }

            // Extract measurement name (skip the key type byte)
            std::string measurement(key.data() + 1, key.size() - 1);
            result.insert(measurement);

            it->Next();
        }

        if (!it->status().ok()) {
            throw std::runtime_error("Iterator error while getting measurements: " + it->status().ToString());
        }

        return result;
    });

    tsdb::index_log.debug("Found {} measurements in index", measurements.size());
    co_return measurements;
}

seastar::future<std::set<std::string>> LevelDBIndex::getFields(const std::string& measurement) {
    // For non-zero shards with centralized metadata, return empty
    // This is expected behavior - only shard 0 has metadata
    if (shardId != 0 || !db) {
        co_return std::set<std::string>{};
    }

    std::string key = encodeMeasurementFieldsKey(measurement);

    // Offload blocking LevelDB I/O to Seastar thread pool
    auto [status, value] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });

    if (status.ok()) {
        co_return decodeStringSet(value);
    } else if (status.IsNotFound()) {
        co_return std::set<std::string>{};
    } else {
        throw std::runtime_error("Failed to get fields: " + status.ToString());
    }
}

seastar::future<> LevelDBIndex::setFieldType(const std::string& measurement, const std::string& field, const std::string& type) {
    // Only shard 0 manages metadata
    if (shardId != 0) {
        throw std::runtime_error("setFieldType called on non-zero shard " + std::to_string(shardId) + " - metadata management only supported on shard 0");
    }
    
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for setFieldType");
    }
    
    std::string key = encodeFieldTypeKey(measurement, field);
    // Offload blocking LevelDB I/O to Seastar thread pool
    auto status = co_await seastar::async([this, &key, &type] {
        return db->Put(leveldb::WriteOptions(), key, type);
    });

    if (!status.ok()) {
        throw std::runtime_error("Failed to set field type: " + status.ToString());
    }

    co_return;
}

seastar::future<std::string> LevelDBIndex::getFieldType(const std::string& measurement, const std::string& field) {
    // Only shard 0 has metadata
    if (shardId != 0) {
        throw std::runtime_error("getFieldType called on non-zero shard " + std::to_string(shardId) + " - metadata queries only supported on shard 0");
    }
    
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getFieldType");
    }
    
    std::string key = encodeFieldTypeKey(measurement, field);

    // Offload blocking LevelDB I/O to Seastar thread pool
    auto [status, value] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });

    if (status.ok()) {
        co_return value;
    } else if (status.IsNotFound()) {
        co_return "";  // Return empty string if field type not found
    } else {
        throw std::runtime_error("Failed to get field type: " + status.ToString());
    }
}

seastar::future<std::set<std::string>> LevelDBIndex::getTags(const std::string& measurement) {
    // Metadata queries only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("getTags called on non-zero shard " + std::to_string(shardId) + " - metadata queries only supported on shard 0");
    }

    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getTags");
    }

    std::string key = encodeMeasurementTagsKey(measurement);

    // Offload blocking LevelDB I/O to Seastar thread pool
    auto [status, value] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });
    
    if (status.ok()) {
        co_return decodeStringSet(value);
    } else if (status.IsNotFound()) {
        co_return std::set<std::string>{};
    } else {
        throw std::runtime_error("Failed to get tags: " + status.ToString());
    }
}

seastar::future<std::set<std::string>> LevelDBIndex::getTagValues(const std::string& measurement, const std::string& tagKey) {
    // Metadata queries only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("getTagValues called on non-zero shard " + std::to_string(shardId) + " - metadata queries only supported on shard 0");
    }

    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getTagValues");
    }

    std::string key = encodeTagValuesKey(measurement, tagKey);

    // Offload blocking LevelDB I/O to Seastar thread pool
    auto [status, value] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });
    
    if (status.ok()) {
        co_return decodeStringSet(value);
    } else if (status.IsNotFound()) {
        co_return std::set<std::string>{};
    } else {
        throw std::runtime_error("Failed to get tag values: " + status.ToString());
    }
}

template<class T>
seastar::future<SeriesId128> LevelDBIndex::indexInsert(const TSDBInsert<T>& insert) {
    // seriesKey() is const (uses mutable cache internally) -- no copy needed
    const std::string& seriesKeyStr = insert.seriesKey();

    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexInsert called for measurement: '{}', field: '{}', series key: '{}'",
                    insert.measurement, insert.field, seriesKeyStr);

    SeriesId128 seriesId = co_await getOrCreateSeriesId(insert.measurement, insert.getTags(), insert.field);

    // Store the field type based on the template parameter T.
    // Use an in-memory cache to avoid redundant LevelDB puts on every insert.
    std::string fieldTypeCacheKey = insert.measurement + std::string(1, '\0') + insert.field;
    if (knownFieldTypes.find(fieldTypeCacheKey) == knownFieldTypes.end()) {
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
            knownFieldTypes.insert(fieldTypeCacheKey);
        }
    }

    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexInsert completed - series ID: {} for series: '{}'",
                    seriesId.toHex(), seriesKeyStr);

    co_return seriesId;
}

seastar::future<> LevelDBIndex::indexMetadataBatch(const std::vector<MetadataOp>& ops) {
    if (shardId != 0) {
        throw std::runtime_error("indexMetadataBatch must be called on shard 0");
    }
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for indexMetadataBatch");
    }
    if (ops.empty()) {
        co_return;
    }

    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexMetadataBatch called with {} ops", ops.size());

    // Phase 1: Identify which series are new (not in cache).
    // Collect cache misses that need a LevelDB existence check.
    struct PendingOp {
        size_t opIdx;
        std::string seriesKeyStr;
        SeriesId128 seriesId;
        bool isNew = false;  // true if series doesn't exist yet
    };
    std::vector<PendingOp> pending;

    for (size_t i = 0; i < ops.size(); ++i) {
        const auto& op = ops[i];
        std::string seriesKeyStr = encodeSeriesKey(op.measurement, op.tags, op.fieldName);
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

        if (seriesCacheContains(seriesKeyStr)) {
            // Already in cache - still need to check field type cache below
            LOG_INSERT_PATH(tsdb::index_log, trace, "[INDEX] Batch cache hit for series: '{}'", seriesKeyStr);
        } else {
            pending.push_back({i, std::move(seriesKeyStr), seriesId, false});
        }
    }

    // Phase 2: Check LevelDB for cache misses (handles restart scenarios).
    // Offload all blocking Gets to Seastar thread pool in a single async block.
    if (!pending.empty()) {
        co_await seastar::async([this, &pending] {
            for (auto& p : pending) {
                std::string metadataKey = encodeSeriesMetadataKey(p.seriesId);
                std::string value;
                leveldb::Status status = db->Get(leveldb::ReadOptions(), metadataKey, &value);

                if (status.ok()) {
                    p.isNew = false;  // exists in LevelDB
                } else if (status.IsNotFound()) {
                    p.isNew = true;
                } else {
                    throw std::runtime_error("Failed to check series existence: " + status.ToString());
                }
            }
        });

        // Update caches back on the reactor thread (safe, no I/O).
        // seriesCacheInsert handles incremental eviction.
        for (auto& p : pending) {
            if (!p.isNew) {
                seriesCacheInsert(p.seriesKeyStr);
                LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Batch: series found in LevelDB, added to cache: '{}'", p.seriesKeyStr);
            }
        }
    }

    // Phase 3: Build a single WriteBatch for all new series and metadata updates.
    leveldb::WriteBatch batch;
    bool batchHasData = false;

    // Track which measurements need field/tag/fieldType updates in this batch.
    // Use local sets to accumulate changes, then write once per measurement.
    // key: measurement, value: set of new fields to add
    std::unordered_map<std::string, std::unordered_set<std::string>> newFields;
    // key: measurement, value: set of new tag keys
    std::unordered_map<std::string, std::unordered_set<std::string>> newTagKeys;
    // key: measurement + '\0' + tagKey, value: set of new tag values
    std::unordered_map<std::string, std::unordered_set<std::string>> newTagValues;

    // Add new series entries to the batch
    for (auto& p : pending) {
        if (!p.isNew) continue;

        const auto& op = ops[p.opIdx];

        // Series metadata entry
        SeriesMetadata metadata;
        metadata.measurement = op.measurement;
        metadata.tags = op.tags;
        metadata.field = op.fieldName;
        std::string metadataKey = encodeSeriesMetadataKey(p.seriesId);
        batch.Put(metadataKey, encodeSeriesMetadata(metadata));

        // MEASUREMENT_SERIES index entry for fast measurement -> series lookup
        batch.Put(encodeMeasurementSeriesKey(op.measurement, p.seriesId), "");

        // Update measurement series cache if populated for this measurement
        auto msIt = measurementSeriesCache.find(op.measurement);
        if (msIt != measurementSeriesCache.end()) {
            msIt->second.push_back(p.seriesId);
        }

        // TAG_INDEX and GROUP_BY_INDEX entries
        for (const auto& [tagKey, tagValue] : op.tags) {
            std::string tagIndexKey;
            tagIndexKey.push_back(TAG_INDEX);
            tagIndexKey.append(op.measurement);
            tagIndexKey.push_back('\0');
            tagIndexKey.append(tagKey);
            tagIndexKey.push_back('\0');
            tagIndexKey.append(tagValue);
            tagIndexKey.push_back('\0');
            tagIndexKey.append(encodeSeriesId(p.seriesId));
            batch.Put(tagIndexKey, encodeSeriesId(p.seriesId));

            std::string groupByKey;
            groupByKey.push_back(GROUP_BY_INDEX);
            groupByKey.append(op.measurement);
            groupByKey.push_back('\0');
            groupByKey.append(tagKey);
            groupByKey.push_back('\0');
            groupByKey.append(tagValue);
            groupByKey.push_back('\0');
            groupByKey.append(encodeSeriesId(p.seriesId));
            batch.Put(groupByKey, encodeSeriesId(p.seriesId));
        }

        // Add to series cache with incremental eviction
        seriesCacheInsert(p.seriesKeyStr);

        batchHasData = true;
    }

    // Phase 4: Collect field, tag, and field-type updates for ALL ops (not just new series).
    // Even cached series need field type checks on first batch call after restart.
    for (const auto& op : ops) {
        // Track field additions via in-memory cache
        auto& fieldSet = fieldsCache[op.measurement];
        if (fieldSet.empty()) {
            // Cache miss: need to load from LevelDB
            auto fields = co_await getFields(op.measurement);
            fieldSet.insert(fields.begin(), fields.end());
        }
        if (fieldSet.insert(op.fieldName).second) {
            newFields[op.measurement].insert(op.fieldName);
        }

        // Track tag additions via in-memory caches
        for (const auto& [tagKey, tagValue] : op.tags) {
            auto& tagKeySet = tagsCache[op.measurement];
            if (tagKeySet.empty()) {
                // Note: empty could mean genuinely empty or cache miss.
                // Use a sentinel or check LevelDB. For safety, load on first access per measurement.
            }
            if (tagKeySet.insert(tagKey).second) {
                newTagKeys[op.measurement].insert(tagKey);
            }

            std::string tvCacheKey = op.measurement + std::string(1, '\0') + tagKey;
            auto& tvSet = tagValuesCache[tvCacheKey];
            if (tvSet.insert(tagValue).second) {
                newTagValues[tvCacheKey].insert(tagValue);
            }
        }

        // Track field type via in-memory cache
        std::string fieldTypeCacheKey = op.measurement + std::string(1, '\0') + op.fieldName;
        if (knownFieldTypes.find(fieldTypeCacheKey) == knownFieldTypes.end()) {
            std::string typeStr;
            switch (op.valueType) {
                case TSMValueType::Float:   typeStr = "float"; break;
                case TSMValueType::Boolean: typeStr = "boolean"; break;
                case TSMValueType::String:  typeStr = "string"; break;
                case TSMValueType::Integer: typeStr = "integer"; break;
            }
            if (!typeStr.empty()) {
                std::string key = encodeFieldTypeKey(op.measurement, op.fieldName);
                batch.Put(key, typeStr);
                knownFieldTypes.insert(fieldTypeCacheKey);
                batchHasData = true;
            }
        }
    }

    // Phase 5: Write accumulated field/tag metadata to the batch.
    for (const auto& [measurement, fields] : newFields) {
        std::string key = encodeMeasurementFieldsKey(measurement);
        batch.Put(key, encodeStringSet(fieldsCache[measurement]));
        batchHasData = true;
    }

    // For tag keys, we need the full set per measurement.
    // Load from LevelDB on cache miss, merge, and write.
    for (const auto& [measurement, tagKeys] : newTagKeys) {
        auto& tagKeySet = tagsCache[measurement];
        // If cache was empty before we inserted, load from LevelDB to ensure correctness
        if (tagKeySet.size() == tagKeys.size()) {
            // All entries are new (cache was empty) - might need LevelDB load
            auto existing = co_await getTags(measurement);
            tagKeySet.insert(existing.begin(), existing.end());
        }
        std::string key = encodeMeasurementTagsKey(measurement);
        batch.Put(key, encodeStringSet(tagKeySet));
        batchHasData = true;
    }

    for (const auto& [tvCacheKey, tagValues] : newTagValues) {
        auto& tvSet = tagValuesCache[tvCacheKey];
        // Extract measurement and tagKey from cache key (separated by '\0')
        auto nullPos = tvCacheKey.find('\0');
        std::string measurement = tvCacheKey.substr(0, nullPos);
        std::string tagKey = tvCacheKey.substr(nullPos + 1);
        // If cache was populated only by this batch, load existing from LevelDB
        if (tvSet.size() == tagValues.size()) {
            auto existing = co_await getTagValues(measurement, tagKey);
            tvSet.insert(existing.begin(), existing.end());
        }
        std::string key = encodeTagValuesKey(measurement, tagKey);
        batch.Put(key, encodeStringSet(tvSet));
        batchHasData = true;
    }

    // Phase 6: Single atomic write (offload blocking I/O to Seastar thread pool)
    if (batchHasData) {
        auto status = co_await seastar::async([this, &batch] {
            return db->Write(leveldb::WriteOptions(), &batch);
        });
        if (!status.ok()) {
            throw std::runtime_error("Failed to write metadata batch: " + status.ToString());
        }
        LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexMetadataBatch wrote batch for {} ops", ops.size());
    }

    co_return;
}

seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>>
LevelDBIndex::findSeries(const std::string& measurement,
                          const std::map<std::string, std::string>& tagFilters,
                          size_t maxSeries) {
    tsdb::index_log.debug("findSeries called for measurement: {}, with {} tag filters, maxSeries: {}",
                         measurement, tagFilters.size(), maxSeries);

    if (tagFilters.empty()) {
        // Return all series for measurement (pass limit through for early bailout)
        tsdb::index_log.debug("No tag filters, getting all series for measurement");
        co_return co_await getAllSeriesForMeasurement(measurement, maxSeries);
    }

    // Sorted-merge intersection: collect results from each tag filter (already
    // sorted by SeriesId128 since TAG_INDEX keys are ordered by the embedded
    // series ID suffix), then intersect using a two-pointer merge.
    // This is O(N+M) per pair with zero hash-table allocations, giving 2-5x
    // speedup over the previous unordered_set approach for 3+ tag filters.
    //
    // Note: Individual tag scans run WITHOUT limits because intersection reduces
    // the count. The limit is checked on the FINAL result after all intersections.

    // Phase 1: Collect all per-tag result vectors.
    std::vector<std::vector<SeriesId128>> tagResultSets;
    tagResultSets.reserve(tagFilters.size());

    for (const auto& [tagKey, tagValue] : tagFilters) {
        std::vector<SeriesId128> tagResults;
        if (tsdb::SeriesMatcher::classifyScope(tagValue) == tsdb::ScopeMatchType::EXACT) {
            tagResults = co_await findSeriesByTag(measurement, tagKey, tagValue);
        } else {
            tagResults = co_await findSeriesByTagPattern(measurement, tagKey, tagValue);
        }
        tsdb::index_log.debug("TAG_INDEX filter {}={} returned {} series",
                             tagKey, tagValue, tagResults.size());

        // If any tag filter returns empty, the intersection is empty — short-circuit.
        if (tagResults.empty()) {
            tsdb::index_log.info("findSeries returning 0 results for measurement {} (empty tag filter {}={})",
                                measurement, tagKey, tagValue);
            co_return std::vector<SeriesId128>{};
        }

        tagResultSets.push_back(std::move(tagResults));
    }

    // Phase 2: Sort by ascending size so we intersect smallest sets first,
    // minimizing the number of comparisons in later passes.
    std::ranges::sort(tagResultSets, {}, &std::vector<SeriesId128>::size);

    // Phase 3: Two-pointer sorted-merge intersection.
    // Start with the smallest result set as the accumulator. Each subsequent
    // intersection writes matching elements in-place into the accumulator,
    // then truncates it — zero extra vector allocations.
    //
    // LevelDB TAG_INDEX keys embed the 16-byte SeriesId128 as the final key
    // component after a fixed prefix, so iteration yields IDs in the same
    // byte-lexicographic order that SeriesId128::operator< defines. No sort needed.
    auto results = std::move(tagResultSets[0]);

    for (size_t i = 1; i < tagResultSets.size() && !results.empty(); ++i) {
        const auto& other = tagResultSets[i];

        // In-place two-pointer intersection: walk both sorted vectors,
        // writing matches into the front of `results`.
        size_t writeIdx = 0;
        size_t a = 0, b = 0;
        const size_t aEnd = results.size(), bEnd = other.size();

        while (a < aEnd && b < bEnd) {
            if (results[a] < other[b]) {
                ++a;
            } else if (other[b] < results[a]) {
                ++b;
            } else {
                // Match — write in place (writeIdx <= a, so this is safe)
                results[writeIdx] = results[a];
                ++writeIdx;
                ++a;
                ++b;
            }
        }

        results.resize(writeIdx);
    }

    // Check limit on the final intersected result
    if (maxSeries > 0 && results.size() > maxSeries) {
        tsdb::index_log.info("findSeries: {} results exceed limit of {} for measurement {}",
                            results.size(), maxSeries, measurement);
        co_return std::unexpected(SeriesLimitExceeded{results.size(), maxSeries});
    }

    tsdb::index_log.info("findSeries returning {} results for measurement {}",
                        results.size(), measurement);
    co_return results;
}

seastar::future<std::expected<std::vector<LevelDBIndex::SeriesWithMetadata>, SeriesLimitExceeded>>
LevelDBIndex::findSeriesWithMetadata(
    const std::string& measurement,
    const std::map<std::string, std::string>& tagFilters,
    const std::unordered_set<std::string>& fieldFilter,
    size_t maxSeries) {

    // Pass maxSeries to findSeries for early bailout before metadata Gets
    auto findResult = co_await findSeries(measurement, tagFilters, maxSeries);

    if (!findResult.has_value()) {
        // Limit was exceeded during series discovery -- propagate the error
        co_return std::unexpected(findResult.error());
    }

    auto& candidateIds = findResult.value();

    if (candidateIds.empty()) {
        co_return std::vector<SeriesWithMetadata>{};
    }

    std::vector<std::string> metadataKeys;
    metadataKeys.reserve(candidateIds.size());
    for (const auto& id : candidateIds) {
        metadataKeys.push_back(encodeSeriesMetadataKey(id));
    }

    auto rawResults = co_await seastar::async([this, &metadataKeys] {
        std::vector<std::pair<leveldb::Status, std::string>> results;
        results.reserve(metadataKeys.size());
        leveldb::ReadOptions readOpts;
        for (const auto& key : metadataKeys) {
            std::string val;
            auto status = db->Get(readOpts, key, &val);
            results.emplace_back(status, std::move(val));
        }
        return results;
    });

    std::vector<SeriesWithMetadata> output;
    output.reserve(candidateIds.size());
    for (size_t i = 0; i < candidateIds.size(); ++i) {
        if (!rawResults[i].first.ok()) {
            continue;
        }
        auto metadata = decodeSeriesMetadata(rawResults[i].second);
        if (!fieldFilter.empty() && fieldFilter.count(metadata.field) == 0) {
            continue;
        }
        output.push_back({candidateIds[i], std::move(metadata)});
    }

    // After field filtering, the actual output may be smaller than candidateIds.
    // Re-check against maxSeries on the final output (safety net, since field
    // filtering can only reduce the count).
    if (maxSeries > 0 && output.size() > maxSeries) {
        co_return std::unexpected(SeriesLimitExceeded{output.size(), maxSeries});
    }

    co_return output;
}

seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>>
LevelDBIndex::getAllSeriesForMeasurement(const std::string& measurement, size_t maxSeries) {
    if (!db) {
        // For non-zero shards with centralized metadata, return empty
        // This is expected behavior - only shard 0 has metadata
        co_return std::vector<SeriesId128>{};
    }

    // Fast path: check in-memory cache
    auto cacheIt = measurementSeriesCache.find(measurement);
    if (cacheIt != measurementSeriesCache.end()) {
        tsdb::index_log.debug("getAllSeriesForMeasurement cache hit for '{}': {} series",
                             measurement, cacheIt->second.size());
        // Check limit against cached result before returning
        if (maxSeries > 0 && cacheIt->second.size() > maxSeries) {
            co_return std::unexpected(SeriesLimitExceeded{cacheIt->second.size(), maxSeries});
        }
        co_return cacheIt->second;
    }

    // Try the MEASUREMENT_SERIES prefix scan (new index, O(M) where M = series in this measurement)
    // Pass maxSeries into the scan so we can bail out early on huge measurements.
    // We use maxSeries+1 as the scan limit: if we see exactly maxSeries+1 entries,
    // we know the limit is exceeded without scanning all entries.
    auto seriesIds = co_await seastar::async([this, &measurement, maxSeries]() {
        std::vector<SeriesId128> result;

        std::string prefix = encodeMeasurementSeriesPrefix(measurement);

        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        if (!it) {
            tsdb::index_log.error("Failed to create LevelDB iterator");
            return result;
        }

        for (it->Seek(prefix); it->Valid(); it->Next()) {
            leveldb::Slice key = it->key();
            // Check if key starts with our prefix
            if (key.size() < prefix.size() ||
                std::memcmp(key.data(), prefix.data(), prefix.size()) != 0) {
                break;
            }

            // Extract SeriesId128 from the last 16 bytes of the key
            // Key format: [0x0A] + measurement + [0x00] + [16-byte SeriesId128]
            size_t expectedSize = prefix.size() + 16;
            if (key.size() == expectedSize) {
                SeriesId128 seriesId = SeriesId128::fromBytes(key.data() + prefix.size(), 16);
                result.push_back(seriesId);

                // Early bailout: if we've found more than the limit, stop scanning.
                // We don't cache partial results (caching happens only for full scans).
                if (maxSeries > 0 && result.size() > maxSeries) {
                    break;
                }
            }
        }

        if (!it->status().ok()) {
            tsdb::index_log.error("MEASUREMENT_SERIES iterator error: {}", it->status().ToString());
        }

        return result;
    });

    // Check if the limit was exceeded during the MEASUREMENT_SERIES scan
    if (maxSeries > 0 && seriesIds.size() > maxSeries) {
        tsdb::index_log.info("getAllSeriesForMeasurement: {} series exceed limit of {} for '{}'",
                            seriesIds.size(), maxSeries, measurement);
        co_return std::unexpected(SeriesLimitExceeded{seriesIds.size(), maxSeries});
    }

    // If the new index returned results, cache and return them
    if (!seriesIds.empty()) {
        tsdb::index_log.debug("MEASUREMENT_SERIES index found {} series for '{}'",
                             seriesIds.size(), measurement);
        measurementSeriesCache[measurement] = seriesIds;
        co_return seriesIds;
    }

    // Fallback: scan SERIES_METADATA (old full-table-scan approach) for backward compatibility.
    // This handles databases created before the MEASUREMENT_SERIES index was added.
    tsdb::index_log.debug("MEASUREMENT_SERIES index empty for '{}', falling back to SERIES_METADATA scan",
                         measurement);

    seriesIds = co_await seastar::async([this, &measurement, maxSeries]() {
        std::vector<SeriesId128> result;

        std::string keyPrefix;
        keyPrefix.push_back(static_cast<char>(SERIES_METADATA));

        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        if (!it) {
            tsdb::index_log.error("Failed to create LevelDB iterator");
            return result;
        }

        it->Seek(keyPrefix);
        while (it->Valid()) {
            leveldb::Slice keySlice = it->key();

            // Stop if we've moved past SERIES_METADATA keys
            if (keySlice.empty() || static_cast<uint8_t>(keySlice[0]) != SERIES_METADATA) {
                break;
            }

            // Decode the metadata to check the measurement
            leveldb::Slice valueSlice = it->value();
            if (!valueSlice.empty()) {
                try {
                    SeriesMetadata metadata = decodeSeriesMetadata(valueSlice.data(), valueSlice.size());

                    if (metadata.measurement == measurement) {
                        if (keySlice.size() >= 17) {  // 1 byte prefix + 16 bytes seriesId
                            SeriesId128 seriesId = SeriesId128::fromBytes(keySlice.data() + 1, 16);
                            result.push_back(seriesId);

                            // Early bailout for the fallback scan too
                            if (maxSeries > 0 && result.size() > maxSeries) {
                                break;
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    tsdb::index_log.error("Failed to decode series metadata: {}", e.what());
                }
            }

            it->Next();
        }

        if (!it->status().ok()) {
            tsdb::index_log.error("SERIES_METADATA fallback iterator error: {}", it->status().ToString());
        }

        return result;
    });

    // Check limit on the fallback result
    if (maxSeries > 0 && seriesIds.size() > maxSeries) {
        tsdb::index_log.info("getAllSeriesForMeasurement fallback: {} series exceed limit of {} for '{}'",
                            seriesIds.size(), maxSeries, measurement);
        // Don't cache partial results from limit-exceeded scans
        co_return std::unexpected(SeriesLimitExceeded{seriesIds.size(), maxSeries});
    }

    tsdb::index_log.info("Fallback scan found {} series for measurement '{}'",
                        seriesIds.size(), measurement);

    // Cache even fallback results
    measurementSeriesCache[measurement] = seriesIds;
    co_return seriesIds;
}

seastar::future<> LevelDBIndex::rebuildMeasurementSeriesIndex() {
    if (!db) {
        co_return;
    }

    tsdb::index_log.info("Rebuilding MEASUREMENT_SERIES index from SERIES_METADATA entries...");

    auto count = co_await seastar::async([this]() {
        leveldb::WriteBatch batch;
        size_t cnt = 0;

        std::string keyPrefix;
        keyPrefix.push_back(static_cast<char>(SERIES_METADATA));

        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        if (!it) {
            tsdb::index_log.error("Failed to create LevelDB iterator for rebuild");
            return cnt;
        }

        it->Seek(keyPrefix);
        while (it->Valid()) {
            leveldb::Slice keySlice = it->key();

            if (keySlice.empty() || static_cast<uint8_t>(keySlice[0]) != SERIES_METADATA) {
                break;
            }

            if (keySlice.size() >= 17) {
                SeriesId128 seriesId = SeriesId128::fromBytes(keySlice.data() + 1, 16);

                leveldb::Slice valueSlice = it->value();
                if (!valueSlice.empty()) {
                    try {
                        SeriesMetadata metadata = decodeSeriesMetadata(valueSlice.data(), valueSlice.size());
                        batch.Put(encodeMeasurementSeriesKey(metadata.measurement, seriesId), "");
                        ++cnt;
                    } catch (const std::exception& e) {
                        tsdb::index_log.error("Failed to decode metadata during rebuild: {}", e.what());
                    }
                }
            }

            it->Next();
        }

        if (!it->status().ok()) {
            tsdb::index_log.error("Iterator error during rebuild: {}", it->status().ToString());
        }

        if (cnt > 0) {
            auto status = db->Write(leveldb::WriteOptions(), &batch);
            if (!status.ok()) {
                throw std::runtime_error("Failed to write MEASUREMENT_SERIES rebuild batch: " + status.ToString());
            }
        }

        return cnt;
    });

    // Clear the cache so it will be re-populated from the new index
    measurementSeriesCache.clear();

    tsdb::index_log.info("MEASUREMENT_SERIES index rebuilt: {} entries written", count);
    co_return;
}

seastar::future<size_t> LevelDBIndex::getSeriesCount() {
    // Count SERIES_METADATA entries (one per series)
    if (!db) {
        co_return 0;
    }

    // Offload blocking LevelDB iterator I/O to Seastar thread pool
    auto count = co_await seastar::async([this] {
        size_t cnt = 0;
        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));

        // Count SERIES_METADATA entries
        std::string startKey;
        startKey.push_back(static_cast<char>(SERIES_METADATA));
        it->Seek(startKey);

        while (it->Valid()) {
            leveldb::Slice key = it->key();
            if (key.size() == 0 || static_cast<uint8_t>(key[0]) != SERIES_METADATA) {
                break;
            }
            cnt++;
            it->Next();
        }

        return cnt;
    });

    co_return count;
}

seastar::future<> LevelDBIndex::compact() {
    if (!db) co_return;
    // LevelDB handles compaction automatically, but we can trigger manual compaction
    // Offload blocking I/O to Seastar thread pool
    co_await seastar::async([this] {
        db->CompactRange(nullptr, nullptr);
    });
    co_return;
}

// Explicit template instantiations
seastar::future<std::optional<SeriesMetadata>> LevelDBIndex::getSeriesMetadata(const SeriesId128& seriesId) {
    if (!db) {
        co_return std::nullopt;
    }

    std::string metadataKey = encodeSeriesMetadataKey(seriesId);

    // Offload blocking LevelDB I/O to Seastar thread pool
    auto [status, value] = co_await seastar::async([this, &metadataKey] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), metadataKey, &val);
        return std::make_pair(s, std::move(val));
    });

    if (status.ok()) {
        co_return decodeSeriesMetadata(value);
    } else if (status.IsNotFound()) {
        co_return std::nullopt;
    } else {
        throw std::runtime_error("Failed to get series metadata: " + status.ToString());
    }
}

seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>>
LevelDBIndex::getSeriesMetadataBatch(const std::vector<SeriesId128>& seriesIds) {
    if (!db) {
        // Non-zero shards have no LevelDB; return nullopt for every entry
        std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>> results;
        results.reserve(seriesIds.size());
        for (const auto& id : seriesIds) {
            results.emplace_back(id, std::nullopt);
        }
        co_return results;
    }

    if (seriesIds.empty()) {
        co_return std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>{};
    }

    // Pre-encode all keys on the reactor thread (cheap CPU work, no I/O)
    std::vector<std::string> metadataKeys;
    metadataKeys.reserve(seriesIds.size());
    for (const auto& seriesId : seriesIds) {
        metadataKeys.push_back(encodeSeriesMetadataKey(seriesId));
    }

    // Perform ALL LevelDB Gets in a single seastar::async block.
    // This turns N coroutine suspensions + N thread pool dispatches into 1 of each.
    auto rawResults = co_await seastar::async([this, &metadataKeys] {
        std::vector<std::pair<leveldb::Status, std::string>> results;
        results.reserve(metadataKeys.size());

        leveldb::ReadOptions readOpts;
        for (const auto& key : metadataKeys) {
            std::string val;
            auto status = db->Get(readOpts, key, &val);
            results.emplace_back(status, std::move(val));
        }
        return results;
    });

    // Decode results back on the reactor thread
    std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>> results;
    results.reserve(seriesIds.size());

    for (size_t i = 0; i < seriesIds.size(); ++i) {
        const auto& [status, value] = rawResults[i];
        if (status.ok()) {
            results.emplace_back(seriesIds[i], decodeSeriesMetadata(value));
        } else if (status.IsNotFound()) {
            results.emplace_back(seriesIds[i], std::nullopt);
        } else {
            throw std::runtime_error("Failed to get series metadata in batch: " + status.ToString());
        }
    }

    co_return results;
}

// Enhanced index methods for query support

seastar::future<std::vector<SeriesId128>> LevelDBIndex::findSeriesByTag(const std::string& measurement,
                                                                     const std::string& tagKey,
                                                                     const std::string& tagValue,
                                                                     size_t maxSeries) {
    if (!db) {
        co_return std::vector<SeriesId128>{};
    }

    // Offload blocking LevelDB iterator I/O to Seastar thread pool
    auto seriesIds = co_await seastar::async([this, &measurement, &tagKey, &tagValue, maxSeries] {
        std::vector<SeriesId128> result;

        // Create key prefix for TAG_INDEX: TAG_INDEX + measurement + tagKey + tagValue
        std::string keyPrefix;
        keyPrefix.push_back(TAG_INDEX);
        keyPrefix.append(measurement);
        keyPrefix.push_back('\0');
        keyPrefix.append(tagKey);
        keyPrefix.push_back('\0');
        keyPrefix.append(tagValue);
        keyPrefix.push_back('\0');

        // Scan all keys with this prefix
        leveldb::Slice prefixSlice(keyPrefix);
        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        for (it->Seek(prefixSlice); it->Valid(); it->Next()) {
            leveldb::Slice keySlice = it->key();
            // Check if key starts with our prefix
            if (keySlice.size() < prefixSlice.size() ||
                std::memcmp(keySlice.data(), prefixSlice.data(), prefixSlice.size()) != 0) {
                break;
            }
            // Extract series ID from the value (16 bytes)
            leveldb::Slice valueSlice = it->value();
            SeriesId128 seriesId = decodeSeriesId(valueSlice.data(), valueSlice.size());
            result.push_back(seriesId);

            // Early bailout when limit is specified (used for single-tag queries
            // where no intersection is needed). For multi-tag intersection queries,
            // this is called without a limit since intersection reduces the count.
            if (maxSeries > 0 && result.size() > maxSeries) {
                break;
            }
        }

        if (!it->status().ok()) {
            throw std::runtime_error("Failed to iterate tag index: " + it->status().ToString());
        }

        return result;
    });

    co_return seriesIds;
}

seastar::future<std::vector<SeriesId128>> LevelDBIndex::findSeriesByTagPattern(
    const std::string& measurement,
    const std::string& tagKey,
    const std::string& scopeValue,
    size_t maxSeries) {
    if (!db) {
        co_return std::vector<SeriesId128>{};
    }

    // Extract the literal prefix to narrow the LevelDB seek range
    std::string literalPrefix = tsdb::SeriesMatcher::extractLiteralPrefix(scopeValue);

    // Build a key prefix: TAG_INDEX + measurement + \0 + tagKey + \0
    // We do NOT include the tagValue — we scan all values for this tag key.
    std::string keyPrefix;
    keyPrefix.push_back(TAG_INDEX);
    keyPrefix.append(measurement);
    keyPrefix.push_back('\0');
    keyPrefix.append(tagKey);
    keyPrefix.push_back('\0');

    // Seek start: keyPrefix + literalPrefix (narrows scan for patterns like "server-*")
    std::string seekKey = keyPrefix + literalPrefix;

    auto seriesIds = co_await seastar::async(
        [this, &keyPrefix, &seekKey, &literalPrefix, &scopeValue, maxSeries] {
            std::vector<SeriesId128> result;

            // Pre-compile regex once if this is a regex pattern, to avoid per-entry compilation
            auto matchType = tsdb::SeriesMatcher::classifyScope(scopeValue);
            std::optional<std::regex> compiledRegex;
            std::string regexPattern;

            if (matchType == tsdb::ScopeMatchType::REGEX) {
                // Extract the raw regex pattern
                if (scopeValue[0] == '~') {
                    regexPattern = scopeValue.substr(1);
                } else {
                    // /pattern/
                    size_t endPos = scopeValue.rfind('/');
                    regexPattern = scopeValue.substr(1, endPos - 1);
                }
                try {
                    compiledRegex.emplace(regexPattern);
                } catch (const std::regex_error&) {
                    // Invalid regex — return empty
                    return result;
                }
            }

            std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
            leveldb::Slice prefixSlice(keyPrefix);

            for (it->Seek(seekKey); it->Valid(); it->Next()) {
                leveldb::Slice keySlice = it->key();

                // Check key still starts with our measurement+tagKey prefix
                if (keySlice.size() < prefixSlice.size() ||
                    std::memcmp(keySlice.data(), prefixSlice.data(), prefixSlice.size()) != 0) {
                    break;
                }

                // Extract tagValue from key: bytes after keyPrefix until next \0
                const char* remaining = keySlice.data() + prefixSlice.size();
                size_t remainingLen = keySlice.size() - prefixSlice.size();
                const char* nullPos = static_cast<const char*>(
                    std::memchr(remaining, '\0', remainingLen));
                if (!nullPos) continue;

                std::string_view tagValue(remaining, static_cast<size_t>(nullPos - remaining));

                // If we have a literal prefix, check if tagValue still starts with it.
                // LevelDB ordering guarantees once we pass the prefix, we're done.
                if (!literalPrefix.empty() &&
                    (tagValue.size() < literalPrefix.size() ||
                     tagValue.substr(0, literalPrefix.size()) != literalPrefix)) {
                    break;
                }

                // Match the tag value against the pattern
                bool matched = false;
                if (compiledRegex) {
                    // Use pre-compiled regex directly
                    matched = std::regex_match(tagValue.begin(), tagValue.end(), *compiledRegex);
                } else {
                    // Wildcard — delegate to SeriesMatcher
                    std::string tvStr(tagValue);
                    matched = tsdb::SeriesMatcher::matchesTag(tvStr, scopeValue);
                }

                if (matched) {
                    leveldb::Slice valueSlice = it->value();
                    SeriesId128 seriesId = decodeSeriesId(valueSlice.data(), valueSlice.size());
                    result.push_back(seriesId);

                    if (maxSeries > 0 && result.size() > maxSeries) {
                        break;
                    }
                }
            }

            if (!it->status().ok()) {
                throw std::runtime_error(
                    "Failed to iterate tag index for pattern: " + it->status().ToString());
            }

            return result;
        });

    co_return seriesIds;
}

seastar::future<std::map<std::string, std::vector<SeriesId128>>>
LevelDBIndex::getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey) {
    if (!db) {
        co_return std::map<std::string, std::vector<SeriesId128>>{};
    }

    // Offload blocking LevelDB iterator I/O to Seastar thread pool
    auto grouped = co_await seastar::async([this, &measurement, &tagKey] {
        std::map<std::string, std::vector<SeriesId128>> result;

        // Create key prefix for GROUP_BY_INDEX: GROUP_BY_INDEX + measurement + tagKey
        std::string keyPrefix;
        keyPrefix.push_back(GROUP_BY_INDEX);
        keyPrefix.append(measurement);
        keyPrefix.push_back('\0');
        keyPrefix.append(tagKey);
        keyPrefix.push_back('\0');

        // Scan all keys with this prefix
        leveldb::Slice prefixSlice(keyPrefix);
        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        for (it->Seek(prefixSlice); it->Valid(); it->Next()) {
            leveldb::Slice keySlice = it->key();
            // Check if key starts with our prefix
            if (keySlice.size() < prefixSlice.size() ||
                std::memcmp(keySlice.data(), prefixSlice.data(), prefixSlice.size()) != 0) {
                break;
            }

            // Extract tag value from the key (between prefix and next '\0')
            size_t valueStart = prefixSlice.size();
            const char* remaining = keySlice.data() + valueStart;
            size_t remainingLen = keySlice.size() - valueStart;
            const char* nullPos = static_cast<const char*>(std::memchr(remaining, '\0', remainingLen));
            if (!nullPos) continue;

            // Tag value must be copied into a string since it's stored in the result map
            std::string tagValue(remaining, static_cast<size_t>(nullPos - remaining));
            leveldb::Slice valueSlice = it->value();
            SeriesId128 seriesId = decodeSeriesId(valueSlice.data(), valueSlice.size());

            result[tagValue].push_back(seriesId);
        }

        if (!it->status().ok()) {
            throw std::runtime_error("Failed to iterate group-by index: " + it->status().ToString());
        }

        return result;
    });

    co_return grouped;
}

seastar::future<> LevelDBIndex::updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                                 const FieldStats& stats) {
    if (!db) {
        co_return;
    }

    // Create key: FIELD_STATS + seriesId + field
    std::string key;
    key.push_back(FIELD_STATS);
    key.append(encodeSeriesId(seriesId));
    key.push_back('\0');
    key.append(field);
    
    // Encode stats as JSON-like format
    std::string value;
    value.append(stats.dataType);
    value.push_back('\0');
    value.append(std::to_string(stats.minTime));
    value.push_back('\0');
    value.append(std::to_string(stats.maxTime));
    value.push_back('\0');
    value.append(std::to_string(stats.pointCount));
    
    // Offload blocking LevelDB I/O to Seastar thread pool
    auto status = co_await seastar::async([this, &key, &value] {
        return db->Put(leveldb::WriteOptions(), key, value);
    });
    if (!status.ok()) {
        throw std::runtime_error("Failed to update field stats: " + status.ToString());
    }

    co_return;
}

seastar::future<std::optional<LevelDBIndex::FieldStats>>
LevelDBIndex::getFieldStats(const SeriesId128& seriesId, const std::string& field) {
    if (!db) {
        co_return std::nullopt;
    }

    // Create key: FIELD_STATS + seriesId + field
    std::string key;
    key.push_back(FIELD_STATS);
    key.append(encodeSeriesId(seriesId));
    key.push_back('\0');
    key.append(field);
    
    // Offload blocking LevelDB I/O to Seastar thread pool
    auto [status, value] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });

    if (status.ok()) {
        // Decode stats
        FieldStats stats;
        size_t pos = 0;

        size_t nextPos = value.find('\0', pos);
        stats.dataType = value.substr(pos, nextPos - pos);
        pos = nextPos + 1;

        nextPos = value.find('\0', pos);
        stats.minTime = std::stoll(value.substr(pos, nextPos - pos));
        pos = nextPos + 1;

        nextPos = value.find('\0', pos);
        stats.maxTime = std::stoll(value.substr(pos, nextPos - pos));
        pos = nextPos + 1;

        stats.pointCount = std::stoull(value.substr(pos));

        co_return stats;
    } else if (status.IsNotFound()) {
        co_return std::nullopt;
    } else {
        throw std::runtime_error("Failed to get field stats: " + status.ToString());
    }
}

template seastar::future<SeriesId128> LevelDBIndex::indexInsert<double>(const TSDBInsert<double>& insert);
template seastar::future<SeriesId128> LevelDBIndex::indexInsert<bool>(const TSDBInsert<bool>& insert);
template seastar::future<SeriesId128> LevelDBIndex::indexInsert<std::string>(const TSDBInsert<std::string>& insert);
template seastar::future<SeriesId128> LevelDBIndex::indexInsert<int64_t>(const TSDBInsert<int64_t>& insert);

// --- Retention Policy CRUD ---

static std::string encodeRetentionPolicyKey(const std::string& measurement) {
    std::string key;
    key.push_back(static_cast<char>(RETENTION_POLICY));
    key += measurement;
    return key;
}

seastar::future<> LevelDBIndex::setRetentionPolicy(const RetentionPolicy& policy) {
    if (shardId != 0) {
        throw std::runtime_error("setRetentionPolicy must be called on shard 0");
    }
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for setRetentionPolicy");
    }

    std::string key = encodeRetentionPolicyKey(policy.measurement);
    std::string value = glz::write_json(policy).value_or("{}");

    auto status = co_await seastar::async([this, &key, &value] {
        return db->Put(leveldb::WriteOptions(), key, value);
    });
    if (!status.ok()) {
        throw std::runtime_error("Failed to set retention policy: " + status.ToString());
    }
}

seastar::future<std::optional<RetentionPolicy>> LevelDBIndex::getRetentionPolicy(const std::string& measurement) {
    if (shardId != 0) {
        throw std::runtime_error("getRetentionPolicy must be called on shard 0");
    }
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getRetentionPolicy");
    }

    std::string key = encodeRetentionPolicyKey(measurement);

    auto [status, value] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });

    if (status.ok()) {
        RetentionPolicy policy;
        auto err = glz::read_json(policy, value);
        if (err) {
            throw std::runtime_error("Failed to parse retention policy JSON: " + std::string(glz::format_error(err)));
        }
        co_return policy;
    } else if (status.IsNotFound()) {
        co_return std::nullopt;
    } else {
        throw std::runtime_error("Failed to get retention policy: " + status.ToString());
    }
}

seastar::future<std::vector<RetentionPolicy>> LevelDBIndex::getAllRetentionPolicies() {
    if (shardId != 0) {
        throw std::runtime_error("getAllRetentionPolicies must be called on shard 0");
    }
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getAllRetentionPolicies");
    }

    auto policies = co_await seastar::async([this] {
        std::vector<RetentionPolicy> result;

        std::string prefix;
        prefix.push_back(static_cast<char>(RETENTION_POLICY));

        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        it->Seek(prefix);

        while (it->Valid()) {
            leveldb::Slice keySlice = it->key();
            if (keySlice.empty() || static_cast<uint8_t>(keySlice[0]) != RETENTION_POLICY) {
                break;
            }

            leveldb::Slice valueSlice = it->value();
            std::string jsonStr(valueSlice.data(), valueSlice.size());
            RetentionPolicy policy;
            auto err = glz::read_json(policy, jsonStr);
            if (!err) {
                result.push_back(std::move(policy));
            }

            it->Next();
        }

        return result;
    });

    co_return policies;
}

seastar::future<bool> LevelDBIndex::deleteRetentionPolicy(const std::string& measurement) {
    if (shardId != 0) {
        throw std::runtime_error("deleteRetentionPolicy must be called on shard 0");
    }
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for deleteRetentionPolicy");
    }

    std::string key = encodeRetentionPolicyKey(measurement);

    // Check if it exists first
    auto [getStatus, existing] = co_await seastar::async([this, &key] {
        std::string val;
        auto s = db->Get(leveldb::ReadOptions(), key, &val);
        return std::make_pair(s, std::move(val));
    });

    if (getStatus.IsNotFound()) {
        co_return false;
    }

    auto deleteStatus = co_await seastar::async([this, &key] {
        return db->Delete(leveldb::WriteOptions(), key);
    });
    if (!deleteStatus.ok()) {
        throw std::runtime_error("Failed to delete retention policy: " + deleteStatus.ToString());
    }
    co_return true;
}