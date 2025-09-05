#include "metadata_index.hpp"
#include "logger.hpp"
#include <leveldb/options.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>
#include <glaze/glaze.hpp>
#include <seastar/core/smp.hh>
#include <algorithm>
#include <sstream>
#include <iomanip>

// Global instance
std::unique_ptr<MetadataIndex> globalMetadataIndex;

// Glaze template specialization for SeriesMetadata
template <>
struct glz::meta<SeriesMetadata> {
    using T = SeriesMetadata;
    static constexpr auto value = object(
        "seriesId", &T::seriesId,
        "measurement", &T::measurement,
        "minTime", &T::minTime,
        "maxTime", &T::maxTime,
        "shardId", &T::shardId,
        "tags", &T::tags,
        "fields", &T::fields
    );
};

// Glaze template specialization for FieldStats
template <>
struct glz::meta<FieldStats> {
    using T = FieldStats;
    static constexpr auto value = object(
        "dataType", &T::dataType,
        "minValue", &T::minValue,
        "maxValue", &T::maxValue,
        "pointCount", &T::pointCount
    );
};

// SeriesMetadata implementation
std::string SeriesMetadata::serialize() const {
    return glz::write_json(*this).value_or("{}");
}

SeriesMetadata SeriesMetadata::deserialize(const std::string& data) {
    SeriesMetadata metadata;
    auto error = glz::read_json(metadata, data);
    
    if (error) {
        throw std::runtime_error("Failed to parse series metadata: " + std::string(glz::format_error(error)));
    }
    
    return metadata;
}

std::string SeriesMetadata::generateSeriesKey(const std::string& measurement,
                                              const std::map<std::string, std::string>& tags,
                                              const std::string& field) {
    std::stringstream ss;
    ss << measurement;
    
    // Sort tags for consistent key generation
    for (const auto& [k, v] : tags) {
        ss << "," << k << "=" << v;
    }
    ss << "," << field;
    
    return ss.str();
}

// FieldStats implementation
std::string FieldStats::serialize() const {
    return glz::write_json(*this).value_or("{}");
}

FieldStats FieldStats::deserialize(const std::string& data) {
    FieldStats stats;
    auto error = glz::read_json(stats, data);
    
    if (error) {
        throw std::runtime_error("Failed to parse field stats: " + std::string(glz::format_error(error)));
    }
    
    return stats;
}

// MetadataIndex implementation
MetadataIndex::MetadataIndex(const std::string& path) : dbPath(path) {
    tsdb::metadata_log.info("Creating metadata index at: {}", path);
}

MetadataIndex::~MetadataIndex() {
    if (db) {
        delete db.release();
    }
}

seastar::future<> MetadataIndex::init() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kSnappyCompression;
    options.write_buffer_size = 4 * 1024 * 1024; // 4MB
    options.max_open_files = 100;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(8 * 1024 * 1024); // 8MB cache
    
    leveldb::DB* dbPtr;
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &dbPtr);
    
    if (!status.ok()) {
        tsdb::metadata_log.error("Failed to open metadata index: {}", status.ToString());
        throw std::runtime_error("Failed to open metadata index: " + status.ToString());
    }
    
    db.reset(dbPtr);
    
    // Load next series ID
    std::string value;
    status = db->Get(leveldb::ReadOptions(), "meta:nextSeriesId", &value);
    if (status.ok()) {
        nextSeriesId = std::stoull(value);
    }
    
    tsdb::metadata_log.info("Metadata index initialized, next series ID: {}", nextSeriesId.load());
    co_return;
}

seastar::future<> MetadataIndex::close() {
    if (db) {
        // Save next series ID
        db->Put(leveldb::WriteOptions(), "meta:nextSeriesId", std::to_string(nextSeriesId.load()));
        
        delete db.release();
        tsdb::metadata_log.info("Metadata index closed");
    }
    co_return;
}

// Key generation helpers
std::string MetadataIndex::seriesKey(uint64_t seriesId) {
    std::stringstream ss;
    ss << "series:" << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::measurementKey(const std::string& measurement, uint64_t seriesId) {
    std::stringstream ss;
    ss << "m:" << measurement << ":" << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::tagKey(const std::string& measurement, const std::string& tagKey,
                                  const std::string& tagValue, uint64_t seriesId) {
    std::stringstream ss;
    ss << "t:" << measurement << ":" << tagKey << ":" << tagValue << ":" 
       << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::buildSortedTagString(const std::map<std::string, std::string>& tags) {
    std::stringstream ss;
    bool first = true;
    for (const auto& [k, v] : tags) {  // std::map is already sorted
        if (!first) ss << ",";
        ss << k << "=" << v;
        first = false;
    }
    return ss.str();
}

std::string MetadataIndex::compositeTagKey(const std::string& measurement,
                                           const std::map<std::string, std::string>& tags,
                                           uint64_t seriesId) {
    std::stringstream ss;
    ss << "ct:" << measurement << ":" << buildSortedTagString(tags) << ":"
       << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::fieldKey(const std::string& measurement, const std::string& field,
                                    uint64_t seriesId) {
    std::stringstream ss;
    ss << "f:" << measurement << ":" << field << ":"
       << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::groupByKey(const std::string& measurement, const std::string& tagKey,
                                      const std::string& tagValue) {
    return "g:" + measurement + ":" + tagKey + ":" + tagValue;
}

std::string MetadataIndex::seriesLookupKey(const std::string& seriesKey) {
    return "lookup:" + seriesKey;
}

std::vector<std::string> MetadataIndex::generateTagSubsets(const std::map<std::string, std::string>& tags) {
    std::vector<std::string> subsets;
    
    // Generate all non-empty subsets
    size_t n = tags.size();
    for (size_t mask = 1; mask < (1 << n); mask++) {
        std::map<std::string, std::string> subset;
        size_t idx = 0;
        
        for (const auto& [k, v] : tags) {
            if (mask & (1 << idx)) {
                subset[k] = v;
            }
            idx++;
        }
        
        subsets.push_back(buildSortedTagString(subset));
    }
    
    return subsets;
}

seastar::future<uint64_t> MetadataIndex::getOrCreateSeriesId(const std::string& measurement,
                                                             const std::map<std::string, std::string>& tags,
                                                             const std::string& field) {
    // Generate series key
    std::string sKey = SeriesMetadata::generateSeriesKey(measurement, tags, field);
    std::string lookupKey = seriesLookupKey(sKey);
    
    // Check if series already exists
    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), lookupKey, &value);
    
    if (status.ok()) {
        // Series exists, return ID
        uint64_t seriesId = std::stoull(value);
        co_return seriesId;
    }
    
    // Create new series
    uint64_t seriesId = nextSeriesId.fetch_add(1);
    
    // Create metadata
    SeriesMetadata metadata;
    metadata.seriesId = seriesId;
    metadata.measurement = measurement;
    metadata.tags = tags;
    metadata.fields.push_back(field);
    metadata.minTime = std::numeric_limits<int64_t>::max();
    metadata.maxTime = std::numeric_limits<int64_t>::min();
    // Simple sharding - handle case where smp::count might be 0 in tests
    unsigned shard_count = seastar::smp::count;
    if (shard_count == 0) shard_count = 1;  // Default to 1 shard for tests
    metadata.shardId = seriesId % shard_count;
    
    // Update all indexes atomically
    leveldb::WriteBatch batch;
    
    // 1. Series lookup
    batch.Put(lookupKey, std::to_string(seriesId));
    
    // 2. Series metadata
    batch.Put(seriesKey(seriesId), metadata.serialize());
    
    // 3. Measurement index
    batch.Put(measurementKey(measurement, seriesId), "");
    
    // 4. Tag indexes
    for (const auto& [k, v] : tags) {
        batch.Put(tagKey(measurement, k, v, seriesId), "");
    }
    
    // 5. Composite tag indexes (all subsets)
    for (const auto& subset : generateTagSubsets(tags)) {
        std::map<std::string, std::string> subsetTags;
        // Parse subset back to map (simplified for now)
        batch.Put("ct:" + measurement + ":" + subset + ":" + 
                 std::to_string(seriesId), "");
    }
    
    // 6. Field index
    batch.Put(fieldKey(measurement, field, seriesId), "");
    
    // 7. Group-by indexes
    for (const auto& [k, v] : tags) {
        std::string gKey = groupByKey(measurement, k, v);
        // Append series ID to existing list (simplified for now)
        batch.Put(gKey + ":" + std::to_string(seriesId), "");
    }
    
    // Write batch
    status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create series: " + status.ToString());
    }
    
    tsdb::metadata_log.debug("Created new series: {} with ID {}", sKey, seriesId);
    co_return seriesId;
}

seastar::future<std::vector<uint64_t>> MetadataIndex::findSeriesByMeasurement(const std::string& measurement) {
    std::vector<uint64_t> seriesIds;
    std::string prefix = "m:" + measurement + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        // Extract series ID from key
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            uint64_t seriesId = std::stoull(idStr, nullptr, 16);
            seriesIds.push_back(seriesId);
        }
    }
    delete it;
    
    tsdb::metadata_log.debug("Found {} series for measurement {}", seriesIds.size(), measurement);
    co_return seriesIds;
}

seastar::future<std::vector<uint64_t>> MetadataIndex::findSeriesByTag(const std::string& measurement,
                                                                      const std::string& tagKey,
                                                                      const std::string& tagValue) {
    std::vector<uint64_t> seriesIds;
    std::string prefix = "t:" + measurement + ":" + tagKey + ":" + tagValue + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            uint64_t seriesId = std::stoull(idStr, nullptr, 16);
            seriesIds.push_back(seriesId);
        }
    }
    delete it;
    
    co_return seriesIds;
}

seastar::future<std::vector<uint64_t>> MetadataIndex::findSeriesByTags(const std::string& measurement,
                                                                       const std::map<std::string, std::string>& tags) {
    std::vector<uint64_t> seriesIds;
    std::string tagStr = buildSortedTagString(tags);
    std::string prefix = "ct:" + measurement + ":" + tagStr + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            uint64_t seriesId = std::stoull(idStr, nullptr, 16);
            seriesIds.push_back(seriesId);
        }
    }
    delete it;
    
    co_return seriesIds;
}

seastar::future<std::optional<SeriesMetadata>> MetadataIndex::getSeriesMetadata(uint64_t seriesId) {
    std::string key = seriesKey(seriesId);
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
        co_return SeriesMetadata::deserialize(value);
    }
    
    co_return std::nullopt;
}

std::string MetadataIndex::getStats() const {
    std::string stats;
    db->GetProperty("leveldb.stats", &stats);
    return stats;
}

// Global initialization
seastar::future<> initGlobalMetadataIndex(const std::string& basePath) {
    globalMetadataIndex = std::make_unique<MetadataIndex>(basePath + "/metadata");
    co_await globalMetadataIndex->init();
}

seastar::future<std::set<std::string>> MetadataIndex::getMeasurementFields(const std::string& measurement) {
    std::set<std::string> fields;
    std::string prefix = "f:" + measurement + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        // Extract field name from key: f:measurement:field:seriesId
        size_t secondColon = key.find(':', prefix.length());
        if (secondColon != std::string::npos) {
            size_t thirdColon = key.find(':', secondColon + 1);
            if (thirdColon != std::string::npos) {
                std::string field = key.substr(secondColon + 1, thirdColon - secondColon - 1);
                fields.insert(field);
            }
        }
    }
    delete it;
    
    tsdb::metadata_log.debug("Found {} fields for measurement {}", fields.size(), measurement);
    co_return fields;
}

seastar::future<std::set<std::string>> MetadataIndex::getMeasurementTagKeys(const std::string& measurement) {
    std::set<std::string> tagKeys;
    std::string prefix = "t:" + measurement + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        // Extract tag key from key: t:measurement:tagKey:tagValue:seriesId
        size_t secondColon = key.find(':', prefix.length());
        if (secondColon != std::string::npos) {
            size_t thirdColon = key.find(':', secondColon + 1);
            if (thirdColon != std::string::npos) {
                std::string tagKey = key.substr(prefix.length(), secondColon - prefix.length());
                tagKeys.insert(tagKey);
            }
        }
    }
    delete it;
    
    tsdb::metadata_log.debug("Found {} tag keys for measurement {}", tagKeys.size(), measurement);
    co_return tagKeys;
}

seastar::future<std::set<std::string>> MetadataIndex::getTagValues(const std::string& measurement,
                                                                   const std::string& tagKey) {
    std::set<std::string> tagValues;
    std::string prefix = "t:" + measurement + ":" + tagKey + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        // Extract tag value from key: t:measurement:tagKey:tagValue:seriesId
        size_t thirdColon = key.find(':', prefix.length());
        if (thirdColon != std::string::npos) {
            std::string tagValue = key.substr(prefix.length(), thirdColon - prefix.length());
            tagValues.insert(tagValue);
        }
    }
    delete it;
    
    tsdb::metadata_log.debug("Found {} values for tag {}:{}", tagValues.size(), measurement, tagKey);
    co_return tagValues;
}

seastar::future<> MetadataIndex::updateFieldStats(const std::string& measurement,
                                                  const std::string& field,
                                                  uint64_t seriesId,
                                                  const FieldStats& stats) {
    std::string key = "fstats:" + measurement + ":" + field + ":" + std::to_string(seriesId);
    std::string value = stats.serialize();
    
    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to update field stats: " + status.ToString());
    }
    
    co_return;
}

seastar::future<std::map<std::string, std::vector<uint64_t>>> 
MetadataIndex::getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey) {
    std::map<std::string, std::vector<uint64_t>> grouped;
    std::string prefix = "g:" + measurement + ":" + tagKey + ":";
    
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        // Parse key: g:measurement:tagKey:tagValue:seriesId
        size_t valueStart = prefix.length();
        size_t lastColon = key.rfind(':');
        
        if (lastColon != std::string::npos && lastColon > valueStart) {
            std::string tagValue = key.substr(valueStart, lastColon - valueStart);
            std::string idStr = key.substr(lastColon + 1);
            uint64_t seriesId = std::stoull(idStr);
            
            grouped[tagValue].push_back(seriesId);
        }
    }
    delete it;
    
    tsdb::metadata_log.debug("Found {} groups for {}:{}", grouped.size(), measurement, tagKey);
    co_return grouped;
}

seastar::future<> MetadataIndex::updateSeriesMetadata(const SeriesMetadata& metadata) {
    std::string key = seriesKey(metadata.seriesId);
    std::string value = metadata.serialize();
    
    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to update series metadata: " + status.ToString());
    }
    
    co_return;
}

seastar::future<> MetadataIndex::deleteSeries(uint64_t seriesId) {
    // Get metadata first to clean up all indexes
    auto metadata = co_await getSeriesMetadata(seriesId);
    if (!metadata.has_value()) {
        co_return;
    }
    
    leveldb::WriteBatch batch;
    
    // Delete series metadata
    batch.Delete(seriesKey(seriesId));
    
    // Delete measurement index entry
    batch.Delete(measurementKey(metadata->measurement, seriesId));
    
    // Delete tag indexes
    for (const auto& [k, v] : metadata->tags) {
        batch.Delete(tagKey(metadata->measurement, k, v, seriesId));
        batch.Delete(groupByKey(metadata->measurement, k, v) + ":" + std::to_string(seriesId));
    }
    
    // Delete field indexes
    for (const auto& field : metadata->fields) {
        batch.Delete(fieldKey(metadata->measurement, field, seriesId));
        batch.Delete("fstats:" + metadata->measurement + ":" + field + ":" + std::to_string(seriesId));
    }
    
    // Delete series lookup
    std::string sKey = SeriesMetadata::generateSeriesKey(metadata->measurement, metadata->tags, 
                                                         metadata->fields.empty() ? "" : metadata->fields[0]);
    batch.Delete(seriesLookupKey(sKey));
    
    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to delete series: " + status.ToString());
    }
    
    tsdb::metadata_log.info("Deleted series {}", seriesId);
    co_return;
}

seastar::future<> shutdownGlobalMetadataIndex() {
    if (globalMetadataIndex) {
        co_await globalMetadataIndex->close();
        globalMetadataIndex.reset();
    }
}