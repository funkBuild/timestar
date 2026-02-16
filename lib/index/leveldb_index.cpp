#include "leveldb_index.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "tsm.hpp"  // for TSMValueType definition

#include <filesystem>
#include <sstream>
#include <cstring>
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
    
    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.compression = leveldb::kSnappyCompression;
    options.block_size = 4096;
    options.write_buffer_size = 4 * 1024 * 1024; // 4MB
    options.max_open_files = 1000;
    
    // Use bloom filter for faster lookups (owned by filterPolicy_ unique_ptr for RAII cleanup)
    filterPolicy_.reset(leveldb::NewBloomFilterPolicy(10));
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
    std::ostringstream oss;
    for (const auto& str : strings) {
        if (str.length() > UINT16_MAX) {
            throw std::runtime_error("String length " + std::to_string(str.length()) +
                                     " exceeds maximum encodable length of " + std::to_string(UINT16_MAX));
        }
        // Length-prefixed encoding
        uint16_t len = static_cast<uint16_t>(str.length());
        oss.write(reinterpret_cast<const char*>(&len), sizeof(len));
        oss.write(str.c_str(), len);
    }
    return oss.str();
}

std::set<std::string> LevelDBIndex::decodeStringSet(const std::string& encoded) {
    std::set<std::string> result;
    std::istringstream iss(encoded);
    
    while (iss.good() && static_cast<size_t>(iss.tellg()) < encoded.length()) {
        uint16_t len;
        iss.read(reinterpret_cast<char*>(&len), sizeof(len));
        
        if (iss.gcount() != sizeof(len)) break;
        
        std::string str(len, '\0');
        iss.read(&str[0], len);
        
        if (iss.gcount() == len) {
            result.insert(str);
        }
    }
    
    return result;
}

std::string LevelDBIndex::encodeSeriesId(const SeriesId128& seriesId) {
    return seriesId.toBytes();
}

SeriesId128 LevelDBIndex::decodeSeriesId(const std::string& encoded) {
    return SeriesId128::fromBytes(encoded);
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

std::string LevelDBIndex::encodeSeriesMetadata(const SeriesMetadata& metadata) {
    try {
        // Add defensive checks to prevent corruption-related crashes
        if (metadata.measurement.length() > 10000 || metadata.field.length() > 10000) {
            throw std::runtime_error("SeriesMetadata has suspiciously long strings - possible corruption");
        }
        
        if (metadata.tags.size() > 1000) {
            throw std::runtime_error("SeriesMetadata has too many tags - possible corruption");
        }
        
        std::stringstream ss;
        ss << metadata.measurement << '\0';
        ss << metadata.field << '\0';
        ss << metadata.tags.size() << '\0';
        
        for (const auto& [k, v] : metadata.tags) {
            if (k.length() > 1000 || v.length() > 1000) {
                throw std::runtime_error("SeriesMetadata tag key/value too long - possible corruption");
            }
            ss << k << '\0' << v << '\0';
        }
        
        return ss.str();
    } catch (const std::exception& e) {
        tsdb::index_log.error("encodeSeriesMetadata failed: {}", e.what());
        throw std::runtime_error(std::string("encodeSeriesMetadata failed: ") + e.what());
    }
}

SeriesMetadata LevelDBIndex::decodeSeriesMetadata(const std::string& encoded) {
    SeriesMetadata metadata;
    std::stringstream ss(encoded);
    
    std::getline(ss, metadata.measurement, '\0');
    std::getline(ss, metadata.field, '\0');
    
    std::string sizeStr;
    std::getline(ss, sizeStr, '\0');
    size_t tagCount = std::stoull(sizeStr);

    if (tagCount > 1000) {
        throw std::runtime_error("tagCount " + std::to_string(tagCount) +
                                 " exceeds maximum of 1000 - possible data corruption");
    }

    for (size_t i = 0; i < tagCount; ++i) {
        std::string key, value;
        std::getline(ss, key, '\0');
        std::getline(ss, value, '\0');
        metadata.tags[key] = value;
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

    // Generate SeriesId128 deterministically from series key (always the same for given measurement+tags+field)
    std::string seriesKeyStr = encodeSeriesKey(measurement, tags, field);
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKeyStr);

    // Fast path: Check in-memory cache (no I/O)
    // No mutex needed: Seastar's shard-per-core model guarantees single-threaded access
    if (indexedSeriesCache.count(seriesKeyStr) > 0) {
        // Already indexed - just return the computed SeriesId
        LOG_INSERT_PATH(tsdb::index_log, trace, "[INDEX] Cache hit for series: '{}'", seriesKeyStr);
        co_return seriesId;
    }

    // Cache miss - need to check if it exists in LevelDB (handles restarts)
    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Cache miss for series: '{}', checking LevelDB", seriesKeyStr);
    auto existingId = co_await getSeriesId(measurement, tags, field);
    if (existingId.has_value()) {
        // Exists in LevelDB but not in cache (likely after restart) - add to cache
        indexedSeriesCache.insert(seriesKeyStr);
        if (indexedSeriesCache.size() > maxSeriesCacheSize) {
            indexedSeriesCache.clear();
            indexedSeriesCache.insert(seriesKeyStr);
        }
        LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Series found in LevelDB, added to cache: '{}'", seriesKeyStr);
        co_return existingId.value();
    }

    // Series doesn't exist - create it
    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Creating new series index: '{}'", seriesKeyStr);

    leveldb::WriteBatch batch;

    // Store series metadata (seriesId -> metadata)
    // Note: We don't store seriesKey->seriesId mapping because seriesId is deterministically
    // calculated from seriesKey via SHA1 hash (see SeriesId128::fromSeriesKey)
    SeriesMetadata metadata;
    metadata.measurement = measurement;
    metadata.tags = tags;
    metadata.field = field;
    std::string metadataKey = encodeSeriesMetadataKey(seriesId);
    batch.Put(metadataKey, encodeSeriesMetadata(metadata));

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

    // Write the batch
    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to write series index: " + status.ToString());
    }

    // Add to cache now that it's written
    indexedSeriesCache.insert(seriesKeyStr);
    if (indexedSeriesCache.size() > maxSeriesCacheSize) {
        indexedSeriesCache.clear();
        indexedSeriesCache.insert(seriesKeyStr);
    }

    // Update metadata indexes
    co_await addField(measurement, field);
    
    // Process tags
    for (const auto& tag : tags) {
        co_await addTag(measurement, tag.first, tag.second);
    }
    
    tsdb::index_log.debug("Created new series ID {} for key: {}", seriesId.toHex(), seriesKeyStr);
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
        // Convert std::set to std::unordered_set for cache
        std::unordered_set<std::string> fieldSet(fields.begin(), fields.end());
        fieldSet.insert(field);
        fieldsCache[measurement] = std::move(fieldSet);
    }

    // Write the full set to LevelDB
    std::string key = encodeMeasurementFieldsKey(measurement);
    std::set<std::string> orderedFields(fieldsCache[measurement].begin(), fieldsCache[measurement].end());
    std::string encodedFields = encodeStringSet(orderedFields);
    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, encodedFields);

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
        std::unordered_set<std::string> tagSet(tags.begin(), tags.end());
        tagsNeedWrite = (tagSet.count(tagKey) == 0);
        tagSet.insert(tagKey);
        tagsCache[measurement] = std::move(tagSet);
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
        std::unordered_set<std::string> tvSet(tagValues.begin(), tagValues.end());
        tagValuesNeedWrite = (tvSet.count(tagValue) == 0);
        tvSet.insert(tagValue);
        tagValuesCache[tagValuesCacheKey] = std::move(tvSet);
    }

    // Only write to LevelDB if something changed
    if (!tagsNeedWrite && !tagValuesNeedWrite) {
        co_return;
    }

    leveldb::WriteBatch batch;

    if (tagsNeedWrite) {
        std::string tagsKey = encodeMeasurementTagsKey(measurement);
        std::set<std::string> orderedTags(tagsCache[measurement].begin(), tagsCache[measurement].end());
        batch.Put(tagsKey, encodeStringSet(orderedTags));
    }

    if (tagValuesNeedWrite) {
        std::string tagValuesKey = encodeTagValuesKey(measurement, tagKey);
        std::set<std::string> orderedValues(tagValuesCache[tagValuesCacheKey].begin(), tagValuesCache[tagValuesCacheKey].end());
        batch.Put(tagValuesKey, encodeStringSet(orderedValues));
    }

    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to add tag: " + status.ToString());
    }
}

seastar::future<std::set<std::string>> LevelDBIndex::getAllMeasurements() {
    // Metadata queries only supported on shard 0
    if (shardId != 0) {
        throw std::runtime_error("getAllMeasurements called on non-zero shard " + std::to_string(shardId) + " - metadata queries only supported on shard 0");
    }
    
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getAllMeasurements");
    }
    
    std::set<std::string> measurements;
    
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
        measurements.insert(measurement);
        
        it->Next();
    }
    
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error while getting measurements: " + it->status().ToString());
    }
    
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
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    
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
    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, type);
    
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
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    
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
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    
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
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    
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
    // Create a mutable copy to call seriesKey() method
    TSDBInsert<T> mutableInsert = insert;
    std::string seriesKeyStr = mutableInsert.seriesKey();

    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexInsert called for measurement: '{}', field: '{}', series key: '{}'",
                    insert.measurement, insert.field, seriesKeyStr);

    SeriesId128 seriesId = co_await getOrCreateSeriesId(insert.measurement, insert.tags, insert.field);

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

        if (indexedSeriesCache.count(seriesKeyStr) > 0) {
            // Already in cache - still need to check field type cache below
            LOG_INSERT_PATH(tsdb::index_log, trace, "[INDEX] Batch cache hit for series: '{}'", seriesKeyStr);
        } else {
            pending.push_back({i, std::move(seriesKeyStr), seriesId, false});
        }
    }

    // Phase 2: Check LevelDB for cache misses (handles restart scenarios).
    // Each Get is synchronous in LevelDB but we must check existence.
    for (auto& p : pending) {
        std::string metadataKey = encodeSeriesMetadataKey(p.seriesId);
        std::string value;
        leveldb::Status status = db->Get(leveldb::ReadOptions(), metadataKey, &value);

        if (status.ok()) {
            // Exists in LevelDB but not in cache (after restart) - add to cache
            indexedSeriesCache.insert(p.seriesKeyStr);
            if (indexedSeriesCache.size() > maxSeriesCacheSize) {
                indexedSeriesCache.clear();
                indexedSeriesCache.insert(p.seriesKeyStr);
            }
            LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] Batch: series found in LevelDB, added to cache: '{}'", p.seriesKeyStr);
        } else if (status.IsNotFound()) {
            p.isNew = true;
        } else {
            throw std::runtime_error("Failed to check series existence: " + status.ToString());
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

        // Update series cache
        indexedSeriesCache.insert(p.seriesKeyStr);
        if (indexedSeriesCache.size() > maxSeriesCacheSize) {
            indexedSeriesCache.clear();
            indexedSeriesCache.insert(p.seriesKeyStr);
        }

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
        std::set<std::string> orderedFields(fieldsCache[measurement].begin(), fieldsCache[measurement].end());
        batch.Put(key, encodeStringSet(orderedFields));
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
        std::set<std::string> orderedTags(tagKeySet.begin(), tagKeySet.end());
        batch.Put(key, encodeStringSet(orderedTags));
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
        std::set<std::string> orderedValues(tvSet.begin(), tvSet.end());
        batch.Put(key, encodeStringSet(orderedValues));
        batchHasData = true;
    }

    // Phase 6: Single atomic write
    if (batchHasData) {
        leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write metadata batch: " + status.ToString());
        }
        LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexMetadataBatch wrote batch for {} ops", ops.size());
    }

    co_return;
}

seastar::future<std::vector<SeriesId128>> LevelDBIndex::findSeries(const std::string& measurement,
                                                                    const std::map<std::string, std::string>& tagFilters) {
    tsdb::index_log.debug("findSeries called for measurement: {}, with {} tag filters", 
                         measurement, tagFilters.size());
    
    if (tagFilters.empty()) {
        // Return all series for measurement
        tsdb::index_log.debug("No tag filters, getting all series for measurement");
        co_return co_await getAllSeriesForMeasurement(measurement);
    }
    
    // With tag filters - for now still use exact match
    // TODO: Implement more sophisticated tag filtering with iterator scanning
    std::vector<SeriesId128> results;
    
    // Get all fields for this measurement
    auto fields = co_await getFields(measurement);
    tsdb::index_log.debug("Found {} fields for measurement {}", fields.size(), measurement);
    
    for (const auto& field : fields) {
        // Try exact match with provided tags
        auto seriesId = co_await getSeriesId(measurement, tagFilters, field);
        if (seriesId.has_value()) {
            results.push_back(seriesId.value());
            tsdb::index_log.debug("Found series ID {} for field {} with exact tag match", 
                                 seriesId.value().toHex(), field);
        }
    }
    
    // If exact match didn't work, try scanning all series and filtering
    if (results.empty()) {
        tsdb::index_log.debug("No exact matches found, scanning all series and filtering");
        auto allSeries = co_await getAllSeriesForMeasurement(measurement);
        
        for (const SeriesId128& seriesId : allSeries) {
            auto metadata = co_await getSeriesMetadata(seriesId);
            if (metadata.has_value()) {
                // Check if all tag filters match
                bool matches = true;
                for (const auto& [filterKey, filterValue] : tagFilters) {
                    auto it = metadata->tags.find(filterKey);
                    if (it == metadata->tags.end() || it->second != filterValue) {
                        matches = false;
                        break;
                    }
                }
                
                if (matches) {
                    results.push_back(seriesId);
                    tsdb::index_log.debug("Series {} matches tag filters", seriesId.toHex());
                }
            }
        }
    }
    
    tsdb::index_log.info("findSeries returning {} results for measurement {}", 
                        results.size(), measurement);
    co_return results;
}

seastar::future<std::vector<SeriesId128>> LevelDBIndex::getAllSeriesForMeasurement(const std::string& measurement) {
    // Don't use coroutines with LevelDB iterators
    return seastar::async([this, measurement]() {
        std::vector<SeriesId128> seriesIds;

        if (!db) {
            // For non-zero shards with centralized metadata, return empty
            // This is expected behavior - only shard 0 has metadata
            return seriesIds;
        }

        // Scan SERIES_METADATA keys (prefix 0x02) and filter by measurement
        // Since we no longer store SERIES_INDEX mappings, we scan all metadata
        // and check which series belong to this measurement
        std::string keyPrefix;
        keyPrefix.push_back(SERIES_METADATA);

        tsdb::index_log.debug("Scanning metadata for series belonging to measurement: {}", measurement);

        std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
        if (!it) {
            tsdb::index_log.error("Failed to create LevelDB iterator");
            return seriesIds;
        }

        // Start from first SERIES_METADATA key
        it->Seek(keyPrefix);
        while (it->Valid()) {
            std::string key = it->key().ToString();

            // Stop if we've moved past SERIES_METADATA keys
            if (key.empty() || key[0] != SERIES_METADATA) {
                break;
            }

            // Decode the metadata to check the measurement
            std::string value = it->value().ToString();
            if (!value.empty()) {
                try {
                    SeriesMetadata metadata = decodeSeriesMetadata(value);

                    // Check if this series belongs to our measurement
                    if (metadata.measurement == measurement) {
                        // Extract seriesId from the key (key format: SERIES_METADATA + seriesId bytes)
                        if (key.size() >= 17) {  // 1 byte prefix + 16 bytes seriesId
                            std::string seriesIdBytes = key.substr(1, 16);
                            SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);
                            seriesIds.push_back(seriesId);
                            tsdb::index_log.debug("Found series ID {} for measurement {}",
                                                 seriesId.toHex(), measurement);
                        }
                    }
                } catch (const std::exception& e) {
                    tsdb::index_log.error("Failed to decode series metadata: {}", e.what());
                }
            }

            it->Next();
        }

        leveldb::Status status = it->status();
        if (!status.ok()) {
            tsdb::index_log.error("Iterator error: {}", status.ToString());
        }

        tsdb::index_log.info("Found {} series for measurement {}", seriesIds.size(), measurement);
        return seriesIds;
    });
}

seastar::future<size_t> LevelDBIndex::getSeriesCount() {
    // Count SERIES_METADATA entries (one per series)
    if (!db) {
        co_return 0;
    }

    size_t count = 0;
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
        count++;
        it->Next();
    }

    co_return count;
}

seastar::future<> LevelDBIndex::compact() {
    if (!db) co_return;
    // LevelDB handles compaction automatically, but we can trigger manual compaction
    db->CompactRange(nullptr, nullptr);
    co_return;
}

// Explicit template instantiations
seastar::future<std::optional<SeriesMetadata>> LevelDBIndex::getSeriesMetadata(const SeriesId128& seriesId) {
    if (!db) {
        co_return std::nullopt;
    }

    std::string metadataKey = encodeSeriesMetadataKey(seriesId);
    std::string value;

    leveldb::Status status = db->Get(leveldb::ReadOptions(), metadataKey, &value);
    
    if (status.ok()) {
        co_return decodeSeriesMetadata(value);
    } else if (status.IsNotFound()) {
        co_return std::nullopt;
    } else {
        throw std::runtime_error("Failed to get series metadata: " + status.ToString());
    }
}

// Enhanced index methods for query support

seastar::future<std::vector<SeriesId128>> LevelDBIndex::findSeriesByTag(const std::string& measurement,
                                                                     const std::string& tagKey,
                                                                     const std::string& tagValue) {
    if (!db) {
        co_return std::vector<SeriesId128>{};
    }

    std::vector<SeriesId128> seriesIds;

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
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(keyPrefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        // Check if key starts with our prefix
        if (key.substr(0, keyPrefix.length()) != keyPrefix) {
            break;
        }
        // Extract series ID from the value
        SeriesId128 seriesId = decodeSeriesId(it->value().ToString());
        seriesIds.push_back(seriesId);
    }

    if (!it->status().ok()) {
        throw std::runtime_error("Failed to iterate tag index: " + it->status().ToString());
    }

    co_return seriesIds;
}

seastar::future<std::map<std::string, std::vector<SeriesId128>>>
LevelDBIndex::getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey) {
    if (!db) {
        co_return std::map<std::string, std::vector<SeriesId128>>{};
    }

    std::map<std::string, std::vector<SeriesId128>> grouped;

    // Create key prefix for GROUP_BY_INDEX: GROUP_BY_INDEX + measurement + tagKey
    std::string keyPrefix;
    keyPrefix.push_back(GROUP_BY_INDEX);
    keyPrefix.append(measurement);
    keyPrefix.push_back('\0');
    keyPrefix.append(tagKey);
    keyPrefix.push_back('\0');
    
    // Scan all keys with this prefix
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(keyPrefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        // Check if key starts with our prefix
        if (key.substr(0, keyPrefix.length()) != keyPrefix) {
            break;
        }

        // Extract tag value from the key
        size_t valueStart = keyPrefix.length();
        size_t valueEnd = key.find('\0', valueStart);
        if (valueEnd == std::string::npos) continue;

        std::string tagValue = key.substr(valueStart, valueEnd - valueStart);
        SeriesId128 seriesId = decodeSeriesId(it->value().ToString());

        grouped[tagValue].push_back(seriesId);
    }

    if (!it->status().ok()) {
        throw std::runtime_error("Failed to iterate group-by index: " + it->status().ToString());
    }

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
    
    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
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
    
    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    
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