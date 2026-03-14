#include "metadata_index.hpp"

#include "logger.hpp"

#include <glaze/glaze.hpp>

#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>
#include <leveldb/options.h>

#include <algorithm>
#include <cassert>
#include <iomanip>
#include <memory>
#include <seastar/core/smp.hh>
#include <sstream>

// Global instance
std::unique_ptr<MetadataIndex> globalMetadataIndex;

// Glaze template specialization for MetadataSeriesInfo
template <>
struct glz::meta<MetadataSeriesInfo> {
    using T = MetadataSeriesInfo;
    static constexpr auto value =
        object("seriesId", &T::seriesId, "measurement", &T::measurement, "minTime", &T::minTime, "maxTime", &T::maxTime,
               "shardId", &T::shardId, "tags", &T::tags, "fields", &T::fields);
};

// Glaze template specialization for FieldStats
template <>
struct glz::meta<FieldStats> {
    using T = FieldStats;
    static constexpr auto value = object("dataType", &T::dataType, "minValue", &T::minValue, "maxValue", &T::maxValue,
                                         "pointCount", &T::pointCount);
};

// MetadataSeriesInfo implementation
std::string MetadataSeriesInfo::serialize() const {
    return glz::write_json(*this).value_or("{}");
}

MetadataSeriesInfo MetadataSeriesInfo::deserialize(const std::string& data) {
    MetadataSeriesInfo metadata;
    auto error = glz::read_json(metadata, data);

    if (error) {
        throw std::runtime_error("Failed to parse series metadata: " + std::string(glz::format_error(error)));
    }

    return metadata;
}

std::string MetadataSeriesInfo::generateSeriesKey(const std::string& measurement,
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
    timestar::metadata_log.info("Creating metadata index at: {}", path);
}

MetadataIndex::~MetadataIndex() {
    // DB must be destroyed before filter policy and block cache since LevelDB
    // may reference them during shutdown.
    db.reset();
    filterPolicy_.reset();
    blockCache_.reset();
}

seastar::future<> MetadataIndex::init() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kSnappyCompression;
    options.write_buffer_size = 4 * 1024 * 1024;  // 4MB
    options.max_open_files = 100;
    // Bloom filter and block cache owned by unique_ptrs for RAII cleanup
    filterPolicy_.reset(leveldb::NewBloomFilterPolicy(10));
    blockCache_.reset(leveldb::NewLRUCache(8 * 1024 * 1024));  // 8MB cache
    options.filter_policy = filterPolicy_.get();
    options.block_cache = blockCache_.get();

    leveldb::DB* dbPtr;
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &dbPtr);

    if (!status.ok()) {
        timestar::metadata_log.error("Failed to open metadata index: {}", status.ToString());
        throw std::runtime_error("Failed to open metadata index: " + status.ToString());
    }

    db.reset(dbPtr);

    // Load next series ID
    std::string value;
    status = db->Get(leveldb::ReadOptions(), "meta:nextSeriesId", &value);
    if (status.ok()) {
        try {
            nextSeriesId = std::stoull(value);
        } catch (const std::exception& e) {
            timestar::metadata_log.warn("Corrupt nextSeriesId '{}': {}, resetting to 0", value, e.what());
            nextSeriesId = 0;
        }
    }

    timestar::metadata_log.info("Metadata index initialized, next series ID: {}", nextSeriesId);
    co_return;
}

seastar::future<> MetadataIndex::close() {
    if (db) {
        // Save next series ID
        auto putStatus = db->Put(leveldb::WriteOptions(), "meta:nextSeriesId", std::to_string(nextSeriesId));
        if (!putStatus.ok()) {
            timestar::metadata_log.warn("Failed to persist nextSeriesId during close: {}", putStatus.ToString());
        }

        delete db.release();
        timestar::metadata_log.info("Metadata index closed");
    }
    // Free the bloom filter policy and block cache after the DB is closed
    filterPolicy_.reset();
    blockCache_.reset();
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
    ss << "t:" << measurement << ":" << tagKey << ":" << tagValue << ":" << std::setfill('0') << std::setw(16)
       << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::buildSortedTagString(const std::map<std::string, std::string>& tags) {
    std::stringstream ss;
    bool first = true;
    for (const auto& [k, v] : tags) {  // std::map is already sorted
        if (!first)
            ss << ",";
        ss << k << "=" << v;
        first = false;
    }
    return ss.str();
}

std::string MetadataIndex::compositeTagKey(const std::string& measurement,
                                           const std::map<std::string, std::string>& tags, uint64_t seriesId) {
    std::stringstream ss;
    ss << "ct:" << measurement << ":" << buildSortedTagString(tags) << ":" << std::setfill('0') << std::setw(16)
       << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndex::fieldKey(const std::string& measurement, const std::string& field, uint64_t seriesId) {
    std::stringstream ss;
    ss << "f:" << measurement << ":" << field << ":" << std::setfill('0') << std::setw(16) << std::hex << seriesId;
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
    // Maximum subset size for composite tag indexes. Only subsets of size 1
    // through MAX_COMPOSITE_SUBSET_SIZE are generated. This bounds the number
    // of composite entries per series to O(N^MAX_COMPOSITE_SUBSET_SIZE) instead
    // of the previous O(2^N) which was exponential.
    //
    // For N tags the entry count is:
    //   C(N,1) + C(N,2) + C(N,3) = N + N*(N-1)/2 + N*(N-1)*(N-2)/6
    //
    // Examples:  N=5 -> 25,  N=10 -> 175,  N=20 -> 1350
    // Compare:   old 2^N approach: N=10 -> 1023,  N=20 -> 1,048,575
    //
    // Multi-tag queries with >3 filter tags will fall back to intersecting
    // results from smaller composite lookups, which is still efficient.
    static constexpr size_t MAX_COMPOSITE_SUBSET_SIZE = 3;

    std::vector<std::string> subsets;

    // Flatten the sorted map into a vector for indexed access.
    std::vector<std::pair<std::string, std::string>> tagVec(tags.begin(), tags.end());
    const size_t n = tagVec.size();

    if (n == 0) {
        return subsets;
    }

    // Reserve an upper-bound estimate: C(n,1) + C(n,2) + C(n,3).
    size_t estimate = n;
    if (MAX_COMPOSITE_SUBSET_SIZE >= 2 && n >= 2)
        estimate += n * (n - 1) / 2;
    if (MAX_COMPOSITE_SUBSET_SIZE >= 3 && n >= 3)
        estimate += n * (n - 1) * (n - 2) / 6;
    subsets.reserve(estimate);

    // Generate subsets of size 1 through min(MAX_COMPOSITE_SUBSET_SIZE, n)
    // using iterative nested loops to avoid recursion overhead.
    const size_t maxK = std::min(MAX_COMPOSITE_SUBSET_SIZE, n);

    // Size 1 subsets
    for (size_t i = 0; i < n; ++i) {
        std::map<std::string, std::string> subset;
        subset[tagVec[i].first] = tagVec[i].second;
        subsets.push_back(buildSortedTagString(subset));
    }

    // Size 2 subsets
    if (maxK >= 2) {
        for (size_t i = 0; i < n; ++i) {
            for (size_t j = i + 1; j < n; ++j) {
                std::map<std::string, std::string> subset;
                subset[tagVec[i].first] = tagVec[i].second;
                subset[tagVec[j].first] = tagVec[j].second;
                subsets.push_back(buildSortedTagString(subset));
            }
        }
    }

    // Size 3 subsets
    if (maxK >= 3) {
        for (size_t i = 0; i < n; ++i) {
            for (size_t j = i + 1; j < n; ++j) {
                for (size_t k = j + 1; k < n; ++k) {
                    std::map<std::string, std::string> subset;
                    subset[tagVec[i].first] = tagVec[i].second;
                    subset[tagVec[j].first] = tagVec[j].second;
                    subset[tagVec[k].first] = tagVec[k].second;
                    subsets.push_back(buildSortedTagString(subset));
                }
            }
        }
    }

    return subsets;
}

seastar::future<uint64_t> MetadataIndex::getOrCreateSeriesId(const std::string& measurement,
                                                             const std::map<std::string, std::string>& tags,
                                                             const std::string& field) {
    // Generate series key
    std::string sKey = MetadataSeriesInfo::generateSeriesKey(measurement, tags, field);
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
    uint64_t seriesId = nextSeriesId++;

    // Create metadata
    MetadataSeriesInfo metadata;
    metadata.seriesId = seriesId;
    metadata.measurement = measurement;
    metadata.tags = tags;
    metadata.fields.push_back(field);
    metadata.minTime = std::numeric_limits<int64_t>::max();
    metadata.maxTime = std::numeric_limits<int64_t>::min();
    // Simple sharding - handle case where smp::count might be 0 in tests
    unsigned shard_count = seastar::smp::count;
    if (shard_count == 0)
        shard_count = 1;  // Default to 1 shard for tests
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
        std::stringstream ctss;
        ctss << "ct:" << measurement << ":" << subset << ":" << std::setfill('0') << std::setw(16) << std::hex
             << seriesId;
        batch.Put(ctss.str(), "");
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

    timestar::metadata_log.debug("Created new series: {} with ID {}", sKey, seriesId);
    co_return seriesId;
}

seastar::future<std::vector<uint64_t>> MetadataIndex::findSeriesByMeasurement(const std::string& measurement) {
    std::vector<uint64_t> seriesIds;
    std::string prefix = "m:" + measurement + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            try {
                uint64_t seriesId = std::stoull(idStr, nullptr, 16);
                seriesIds.push_back(seriesId);
            } catch (const std::exception& e) {
                timestar::metadata_log.warn("Corrupt series ID in key '{}': {}", key, e.what());
            }
        }
    }
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in findSeriesByMeasurement: " + it->status().ToString());
    }

    timestar::metadata_log.debug("Found {} series for measurement {}", seriesIds.size(), measurement);
    co_return seriesIds;
}

seastar::future<std::vector<uint64_t>> MetadataIndex::findSeriesByTag(const std::string& measurement,
                                                                      const std::string& tagKey,
                                                                      const std::string& tagValue) {
    std::vector<uint64_t> seriesIds;
    std::string prefix = "t:" + measurement + ":" + tagKey + ":" + tagValue + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            try {
                uint64_t seriesId = std::stoull(idStr, nullptr, 16);
                seriesIds.push_back(seriesId);
            } catch (const std::exception& e) {
                timestar::metadata_log.warn("Corrupt series ID in key '{}': {}", key, e.what());
            }
        }
    }
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in findSeriesByTag: " + it->status().ToString());
    }

    co_return seriesIds;
}

seastar::future<std::vector<uint64_t>> MetadataIndex::findSeriesByTags(const std::string& measurement,
                                                                       const std::map<std::string, std::string>& tags) {
    std::vector<uint64_t> seriesIds;
    std::string tagStr = buildSortedTagString(tags);
    std::string prefix = "ct:" + measurement + ":" + tagStr + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            try {
                uint64_t seriesId = std::stoull(idStr, nullptr, 16);
                seriesIds.push_back(seriesId);
            } catch (const std::exception& e) {
                timestar::metadata_log.warn("Corrupt series ID in key '{}': {}", key, e.what());
            }
        }
    }
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in findSeriesByTags: " + it->status().ToString());
    }

    co_return seriesIds;
}

seastar::future<std::optional<MetadataSeriesInfo>> MetadataIndex::getSeriesMetadata(uint64_t seriesId) {
    std::string key = seriesKey(seriesId);
    std::string value;

    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
        co_return MetadataSeriesInfo::deserialize(value);
    } else if (status.IsNotFound()) {
        co_return std::nullopt;
    } else {
        throw std::runtime_error("Failed to get series metadata: " + status.ToString());
    }
}

std::string MetadataIndex::getStats() const {
    std::string stats;
    db->GetProperty("leveldb.stats", &stats);
    return stats;
}

// Global initialization -- must be called from shard 0 only.
seastar::future<> initGlobalMetadataIndex(const std::string& basePath) {
    assert(seastar::this_shard_id() == 0 && "initGlobalMetadataIndex must be called from shard 0");
    globalMetadataIndex = std::make_unique<MetadataIndex>(basePath + "/metadata");
    co_await globalMetadataIndex->init();
}

seastar::future<std::set<std::string>> MetadataIndex::getMeasurementFields(const std::string& measurement) {
    std::set<std::string> fields;
    std::string prefix = "f:" + measurement + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
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
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in getMeasurementFields: " + it->status().ToString());
    }

    timestar::metadata_log.debug("Found {} fields for measurement {}", fields.size(), measurement);
    co_return fields;
}

seastar::future<std::set<std::string>> MetadataIndex::getMeasurementTagKeys(const std::string& measurement) {
    std::set<std::string> tagKeys;
    std::string prefix = "t:" + measurement + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
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
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in getMeasurementTagKeys: " + it->status().ToString());
    }

    timestar::metadata_log.debug("Found {} tag keys for measurement {}", tagKeys.size(), measurement);
    co_return tagKeys;
}

seastar::future<std::set<std::string>> MetadataIndex::getTagValues(const std::string& measurement,
                                                                   const std::string& tagKey) {
    std::set<std::string> tagValues;
    std::string prefix = "t:" + measurement + ":" + tagKey + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        // Extract tag value from key: t:measurement:tagKey:tagValue:seriesId
        size_t thirdColon = key.find(':', prefix.length());
        if (thirdColon != std::string::npos) {
            std::string tagValue = key.substr(prefix.length(), thirdColon - prefix.length());
            tagValues.insert(tagValue);
        }
    }
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in getTagValues: " + it->status().ToString());
    }

    timestar::metadata_log.debug("Found {} values for tag {}:{}", tagValues.size(), measurement, tagKey);
    co_return tagValues;
}

seastar::future<> MetadataIndex::updateFieldStats(const std::string& measurement, const std::string& field,
                                                  uint64_t seriesId, const FieldStats& stats) {
    std::string key = "fstats:" + measurement + ":" + field + ":" + std::to_string(seriesId);
    std::string value = stats.serialize();

    leveldb::Status status = db->Put(leveldb::WriteOptions(), key, value);
    if (!status.ok()) {
        throw std::runtime_error("Failed to update field stats: " + status.ToString());
    }

    co_return;
}

seastar::future<std::map<std::string, std::vector<uint64_t>>> MetadataIndex::getSeriesGroupedByTag(
    const std::string& measurement, const std::string& tagKey) {
    std::map<std::string, std::vector<uint64_t>> grouped;
    std::string prefix = "g:" + measurement + ":" + tagKey + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        // Parse key: g:measurement:tagKey:tagValue:seriesId
        size_t valueStart = prefix.length();
        size_t lastColon = key.rfind(':');

        if (lastColon != std::string::npos && lastColon > valueStart) {
            std::string tagValue = key.substr(valueStart, lastColon - valueStart);
            std::string idStr = key.substr(lastColon + 1);
            try {
                uint64_t seriesId = std::stoull(idStr);
                grouped[tagValue].push_back(seriesId);
            } catch (const std::exception& e) {
                timestar::metadata_log.warn("Corrupt series ID in key '{}': {}", key, e.what());
            }
        }
    }
    if (!it->status().ok()) {
        throw std::runtime_error("Iterator error in getSeriesGroupedByTag: " + it->status().ToString());
    }

    timestar::metadata_log.debug("Found {} groups for {}:{}", grouped.size(), measurement, tagKey);
    co_return grouped;
}

seastar::future<> MetadataIndex::updateSeriesMetadata(const MetadataSeriesInfo& metadata) {
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

    // Delete composite tag indexes (all subsets, mirrors getOrCreateSeriesId step 5)
    for (const auto& subset : generateTagSubsets(metadata->tags)) {
        std::stringstream ctss;
        ctss << "ct:" << metadata->measurement << ":" << subset << ":" << std::setfill('0') << std::setw(16) << std::hex
             << seriesId;
        batch.Delete(ctss.str());
    }

    // Delete field indexes
    for (const auto& field : metadata->fields) {
        batch.Delete(fieldKey(metadata->measurement, field, seriesId));
        batch.Delete("fstats:" + metadata->measurement + ":" + field + ":" + std::to_string(seriesId));
    }

    // Delete series lookup keys for ALL fields (each field has its own lookup entry)
    for (const auto& field : metadata->fields) {
        std::string sKey = MetadataSeriesInfo::generateSeriesKey(metadata->measurement, metadata->tags, field);
        batch.Delete(seriesLookupKey(sKey));
    }
    // Also handle the edge case where fields list is empty
    if (metadata->fields.empty()) {
        std::string sKey = MetadataSeriesInfo::generateSeriesKey(metadata->measurement, metadata->tags, "");
        batch.Delete(seriesLookupKey(sKey));
    }

    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to delete series: " + status.ToString());
    }

    timestar::metadata_log.info("Deleted series {}", seriesId);
    co_return;
}

// Shutdown -- must be called from shard 0 only.
seastar::future<> shutdownGlobalMetadataIndex() {
    assert(seastar::this_shard_id() == 0 && "shutdownGlobalMetadataIndex must be called from shard 0");
    if (globalMetadataIndex) {
        co_await globalMetadataIndex->close();
        globalMetadataIndex.reset();
    }
}