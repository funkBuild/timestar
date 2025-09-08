#include "leveldb_index.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <filesystem>
#include <sstream>
#include <cstring>

#include <leveldb/comparator.h>
#include <leveldb/filter_policy.h>
#include <seastar/core/thread.hh>

LevelDBIndex::LevelDBIndex(int _shardId) : shardId(_shardId) {
    indexPath = "shard_" + std::to_string(shardId) + "/index";
}

LevelDBIndex::~LevelDBIndex() {
    if (db) {
        delete db.release();
    }
}

seastar::future<> LevelDBIndex::open() {
    // Only shard 0 opens LevelDB for metadata storage
    // Other shards will delegate metadata operations to shard 0
    if (shardId != 0) {
        tsdb::index_log.info("Shard {} skipping LevelDB index opening - metadata centralized on shard 0", shardId);
        co_return;
    }
    
    tsdb::index_log.info("Shard 0 opening centralized metadata LevelDB index");
    
    // Create index directory if it doesn't exist
    std::filesystem::create_directories(indexPath);
    
    leveldb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    options.compression = leveldb::kSnappyCompression;
    options.block_size = 4096;
    options.write_buffer_size = 4 * 1024 * 1024; // 4MB
    options.max_open_files = 1000;
    
    // Use bloom filter for faster lookups
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    
    leveldb::DB* dbPtr;
    leveldb::Status status = leveldb::DB::Open(options, indexPath, &dbPtr);
    
    if (!status.ok()) {
        tsdb::index_log.error("Failed to open LevelDB index: {}", status.ToString());
        throw std::runtime_error("Failed to open LevelDB index: " + status.ToString());
    }
    
    db.reset(dbPtr);
    tsdb::index_log.info("LevelDB index opened at: {}", indexPath);
    
    co_await loadSeriesCounter();
}

seastar::future<> LevelDBIndex::close() {
    if (db) {
        delete db.release();
        db.reset();
    }
    co_return;
}

seastar::future<> LevelDBIndex::loadSeriesCounter() {
    if (!db) {
        throw std::runtime_error("Database not opened before loading series counter");
    }
    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), SERIES_COUNTER_KEY, &value);
    
    if (status.ok()) {
        nextSeriesId = decodeSeriesId(value);
    } else if (status.IsNotFound()) {
        // First time initialization
        nextSeriesId = 1;
    } else {
        throw std::runtime_error("Failed to load series counter: " + status.ToString());
    }
    
    tsdb::index_log.debug("Loaded series counter: {}", nextSeriesId);
    co_return;
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
    std::ostringstream oss;
    for (const auto& str : strings) {
        // Length-prefixed encoding
        uint16_t len = str.length();
        oss.write(reinterpret_cast<const char*>(&len), sizeof(len));
        oss.write(str.c_str(), len);
    }
    return oss.str();
}

std::set<std::string> LevelDBIndex::decodeStringSet(const std::string& encoded) {
    std::set<std::string> result;
    std::istringstream iss(encoded);
    
    while (iss.tellg() < encoded.length()) {
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

std::string LevelDBIndex::encodeSeriesId(uint64_t seriesId) {
    std::string encoded(sizeof(uint64_t), '\0');
    std::memcpy(&encoded[0], &seriesId, sizeof(uint64_t));
    return encoded;
}

uint64_t LevelDBIndex::decodeSeriesId(const std::string& encoded) {
    if (encoded.length() != sizeof(uint64_t)) {
        throw std::runtime_error("Invalid series ID encoding");
    }
    uint64_t seriesId;
    std::memcpy(&seriesId, encoded.data(), sizeof(uint64_t));
    return seriesId;
}

std::string LevelDBIndex::encodeSeriesMetadataKey(uint64_t seriesId) {
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
    std::stringstream ss;
    ss << metadata.measurement << '\0';
    ss << metadata.field << '\0';
    ss << metadata.tags.size() << '\0';
    for (const auto& [k, v] : metadata.tags) {
        ss << k << '\0' << v << '\0';
    }
    return ss.str();
}

SeriesMetadata LevelDBIndex::decodeSeriesMetadata(const std::string& encoded) {
    SeriesMetadata metadata;
    std::stringstream ss(encoded);
    
    std::getline(ss, metadata.measurement, '\0');
    std::getline(ss, metadata.field, '\0');
    
    std::string sizeStr;
    std::getline(ss, sizeStr, '\0');
    size_t tagCount = std::stoull(sizeStr);
    
    for (size_t i = 0; i < tagCount; ++i) {
        std::string key, value;
        std::getline(ss, key, '\0');
        std::getline(ss, value, '\0');
        metadata.tags[key] = value;
    }
    
    return metadata;
}

seastar::future<uint64_t> LevelDBIndex::getOrCreateSeriesId(std::string measurement,
                                                           std::map<std::string, std::string> tags,
                                                           std::string field) {
    // Only shard 0 handles series ID generation and metadata indexing
    if (shardId != 0) {
        throw std::runtime_error("getOrCreateSeriesId called on non-zero shard " + std::to_string(shardId) + " - series ID generation only supported on shard 0. This needs to be delegated from Engine level.");
    }
    
    if (!db) {
        throw std::runtime_error("Database not opened on shard 0 for getOrCreateSeriesId");
    }
    
    // First check if series already exists
    return getSeriesId(measurement, tags, field).then([this, measurement = std::move(measurement), 
                                                        tags = std::move(tags), 
                                                        field = std::move(field)](std::optional<uint64_t> existingId) mutable {
        if (existingId.has_value()) {
            return seastar::make_ready_future<uint64_t>(existingId.value());
        }
        
        // Create new series
        uint64_t newSeriesId = nextSeriesId++;
        
        leveldb::WriteBatch batch;
        
        // Add series mapping
        std::string seriesKey = encodeSeriesKey(measurement, tags, field);
        batch.Put(seriesKey, encodeSeriesId(newSeriesId));
        
        // Store series metadata for reverse lookup
        SeriesMetadata metadata;
        metadata.measurement = measurement;
        metadata.tags = tags;
        metadata.field = field;
        std::string metadataKey = encodeSeriesMetadataKey(newSeriesId);
        batch.Put(metadataKey, encodeSeriesMetadata(metadata));
        
        // Update series counter
        batch.Put(SERIES_COUNTER_KEY, encodeSeriesId(nextSeriesId));
        
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
            tagIndexKey.append(encodeSeriesId(newSeriesId));
            
            batch.Put(tagIndexKey, encodeSeriesId(newSeriesId));
            
            // GROUP_BY_INDEX also needs unique keys per series
            std::string groupByKey;
            groupByKey.push_back(GROUP_BY_INDEX);
            groupByKey.append(measurement);
            groupByKey.push_back('\0');
            groupByKey.append(tagKey);
            groupByKey.push_back('\0');
            groupByKey.append(tagValue);
            groupByKey.push_back('\0');
            groupByKey.append(encodeSeriesId(newSeriesId));
            
            batch.Put(groupByKey, encodeSeriesId(newSeriesId));
        }
        
        // Write the batch
        leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
        if (!status.ok()) {
            throw std::runtime_error("Failed to write series index: " + status.ToString());
        }
        
        // Chain the async operations
        return addField(measurement, field).then([this, measurement = std::move(measurement), 
                                                  tags = std::move(tags), newSeriesId]() mutable {
            // Process tags
            seastar::future<> tag_future = seastar::make_ready_future<>();
            for (const auto& tag : tags) {
                auto tagKey = tag.first;
                auto tagValue = tag.second;
                tag_future = tag_future.then([this, measurement, tagKey, tagValue]() {
                    return addTag(measurement, tagKey, tagValue);
                });
            }
            
            return tag_future.then([newSeriesId]() {
                tsdb::index_log.debug("Created new series ID {}", newSeriesId);
                return seastar::make_ready_future<uint64_t>(newSeriesId);
            });
        });
    });
}

seastar::future<std::optional<uint64_t>> LevelDBIndex::getSeriesId(const std::string& measurement,
                                                                  const std::map<std::string, std::string>& tags,
                                                                  const std::string& field) {
    if (!db) {
        throw std::runtime_error("Database not opened before getSeriesId");
    }
    std::string seriesKey = encodeSeriesKey(measurement, tags, field);
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), seriesKey, &value);
    
    if (status.ok()) {
        co_return decodeSeriesId(value);
    } else if (status.IsNotFound()) {
        co_return std::nullopt;
    } else {
        throw std::runtime_error("Failed to get series ID: " + status.ToString());
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
    
    std::string key = encodeMeasurementFieldsKey(measurement);
    
    // Get existing fields
    auto fields = co_await getFields(measurement);
    fields.insert(field);
    
    // Store updated set
    std::string encodedFields = encodeStringSet(fields);
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
    
    leveldb::WriteBatch batch;
    
    // Add to measurement tags
    auto tags = co_await getTags(measurement);
    tags.insert(tagKey);
    std::string tagsKey = encodeMeasurementTagsKey(measurement);
    batch.Put(tagsKey, encodeStringSet(tags));
    
    // Add to tag values
    auto tagValues = co_await getTagValues(measurement, tagKey);
    tagValues.insert(tagValue);
    std::string tagValuesKey = encodeTagValuesKey(measurement, tagKey);
    batch.Put(tagValuesKey, encodeStringSet(tagValues));
    
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
        co_return "unknown";  // Default to unknown if not set
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
seastar::future<uint64_t> LevelDBIndex::indexInsert(const TSDBInsert<T>& insert) {
    // Create a mutable copy to call seriesKey() method
    TSDBInsert<T> mutableInsert = insert;
    std::string seriesKeyStr = mutableInsert.seriesKey();
    
    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexInsert called for measurement: '{}', field: '{}', series key: '{}'", 
                    insert.measurement, insert.field, seriesKeyStr);
    
    uint64_t seriesId = co_await getOrCreateSeriesId(insert.measurement, insert.tags, insert.field);
    
    LOG_INSERT_PATH(tsdb::index_log, debug, "[INDEX] indexInsert completed - series ID: {} for series: '{}'", 
                    seriesId, seriesKeyStr);
    
    co_return seriesId;
}

seastar::future<std::vector<uint64_t>> LevelDBIndex::findSeries(const std::string& measurement,
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
    std::vector<uint64_t> results;
    
    // Get all fields for this measurement
    auto fields = co_await getFields(measurement);
    tsdb::index_log.debug("Found {} fields for measurement {}", fields.size(), measurement);
    
    for (const auto& field : fields) {
        // Try exact match with provided tags
        auto seriesId = co_await getSeriesId(measurement, tagFilters, field);
        if (seriesId.has_value()) {
            results.push_back(seriesId.value());
            tsdb::index_log.debug("Found series ID {} for field {} with exact tag match", 
                                 seriesId.value(), field);
        }
    }
    
    // If exact match didn't work, try scanning all series and filtering
    if (results.empty()) {
        tsdb::index_log.debug("No exact matches found, scanning all series and filtering");
        auto allSeries = co_await getAllSeriesForMeasurement(measurement);
        
        for (uint64_t seriesId : allSeries) {
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
                    tsdb::index_log.debug("Series {} matches tag filters", seriesId);
                }
            }
        }
    }
    
    tsdb::index_log.info("findSeries returning {} results for measurement {}", 
                        results.size(), measurement);
    co_return results;
}

seastar::future<std::vector<uint64_t>> LevelDBIndex::getAllSeriesForMeasurement(const std::string& measurement) {
    // Don't use coroutines with LevelDB iterators
    return seastar::async([this, measurement]() {
        std::vector<uint64_t> seriesIds;
        
        if (!db) {
            // For non-zero shards with centralized metadata, return empty
            // This is expected behavior - only shard 0 has metadata
            return seriesIds;
        }
        
        // Create key prefix for SERIES_INDEX with measurement
        // Keys are: SERIES_INDEX + measurement + ',' + tags + ',' + field (with tags)
        //   or: SERIES_INDEX + measurement + ' ' + field (without tags)
        // We need to scan for both patterns
        std::string keyPrefix;
        keyPrefix.push_back(SERIES_INDEX);
        keyPrefix.append(measurement);
        
        tsdb::index_log.debug("Scanning for series with prefix for measurement: {}", measurement);
        
        // Scan all keys with this prefix
        leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
        if (!it) {
            tsdb::index_log.error("Failed to create LevelDB iterator");
            return seriesIds;
        }
        
        it->Seek(keyPrefix);
        while (it->Valid()) {
            std::string key = it->key().ToString();
            
            // Check if key still starts with our prefix
            if (key.size() < keyPrefix.size() || 
                key.substr(0, keyPrefix.size()) != keyPrefix) {
                break;  // We've moved past our measurement
            }
            
            // Check that after the measurement we have either ',' (with tags) or ' ' (without tags)
            if (key.size() > keyPrefix.size()) {
                char nextChar = key[keyPrefix.size()];
                if (nextChar != ',' && nextChar != ' ') {
                    // This key is for a different measurement that starts with our prefix
                    it->Next();
                    continue;
                }
            }
            
            // Decode series ID from value
            std::string value = it->value().ToString();
            if (!value.empty()) {
                try {
                    uint64_t seriesId = decodeSeriesId(value);
                    seriesIds.push_back(seriesId);
                    tsdb::index_log.debug("Found series ID {} for measurement {}", seriesId, measurement);
                } catch (const std::exception& e) {
                    tsdb::index_log.error("Failed to decode series ID: {}", e.what());
                }
            }
            
            it->Next();
        }
        
        leveldb::Status status = it->status();
        if (!status.ok()) {
            tsdb::index_log.error("Iterator error: {}", status.ToString());
        }
        
        delete it;
        
        tsdb::index_log.info("Found {} series for measurement {}", seriesIds.size(), measurement);
        return seriesIds;
    });
}

seastar::future<size_t> LevelDBIndex::getSeriesCount() {
    return seastar::make_ready_future<size_t>(nextSeriesId - 1);
}

seastar::future<> LevelDBIndex::compact() {
    // LevelDB handles compaction automatically, but we can trigger manual compaction
    db->CompactRange(nullptr, nullptr);
    co_return;
}

// Explicit template instantiations
seastar::future<std::optional<SeriesMetadata>> LevelDBIndex::getSeriesMetadata(uint64_t seriesId) {
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

seastar::future<std::vector<uint64_t>> LevelDBIndex::findSeriesByTag(const std::string& measurement,
                                                                     const std::string& tagKey,
                                                                     const std::string& tagValue) {
    std::vector<uint64_t> seriesIds;
    
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
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(keyPrefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        // Check if key starts with our prefix
        if (key.substr(0, keyPrefix.length()) != keyPrefix) {
            break;
        }
        // Extract series ID from the value
        uint64_t seriesId = decodeSeriesId(it->value().ToString());
        seriesIds.push_back(seriesId);
    }
    
    if (!it->status().ok()) {
        delete it;
        throw std::runtime_error("Failed to iterate tag index: " + it->status().ToString());
    }
    delete it;
    
    co_return seriesIds;
}

seastar::future<std::map<std::string, std::vector<uint64_t>>> 
LevelDBIndex::getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey) {
    std::map<std::string, std::vector<uint64_t>> grouped;
    
    // Create key prefix for GROUP_BY_INDEX: GROUP_BY_INDEX + measurement + tagKey
    std::string keyPrefix;
    keyPrefix.push_back(GROUP_BY_INDEX);
    keyPrefix.append(measurement);
    keyPrefix.push_back('\0');
    keyPrefix.append(tagKey);
    keyPrefix.push_back('\0');
    
    // Scan all keys with this prefix
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
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
        uint64_t seriesId = decodeSeriesId(it->value().ToString());
        
        grouped[tagValue].push_back(seriesId);
    }
    
    if (!it->status().ok()) {
        delete it;
        throw std::runtime_error("Failed to iterate group-by index: " + it->status().ToString());
    }
    delete it;
    
    co_return grouped;
}

seastar::future<> LevelDBIndex::updateFieldStats(uint64_t seriesId, const std::string& field,
                                                 const FieldStats& stats) {
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
LevelDBIndex::getFieldStats(uint64_t seriesId, const std::string& field) {
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

template seastar::future<uint64_t> LevelDBIndex::indexInsert<double>(const TSDBInsert<double>& insert);
template seastar::future<uint64_t> LevelDBIndex::indexInsert<bool>(const TSDBInsert<bool>& insert);
template seastar::future<uint64_t> LevelDBIndex::indexInsert<std::string>(const TSDBInsert<std::string>& insert);