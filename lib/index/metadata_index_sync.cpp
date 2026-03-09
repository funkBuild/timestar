#include "metadata_index_sync.hpp"
#include <leveldb/options.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>
#include <glaze/glaze.hpp>
#include <algorithm>
#include <memory>
#include <sstream>
#include <iomanip>
#include <iostream>

// Glaze template specialization for SeriesMetadataSync
template <>
struct glz::meta<SeriesMetadataSync> {
    using T = SeriesMetadataSync;
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

// Glaze template specialization for FieldStatsSync
template <>
struct glz::meta<FieldStatsSync> {
    using T = FieldStatsSync;
    static constexpr auto value = object(
        "dataType", &T::dataType,
        "minValue", &T::minValue,
        "maxValue", &T::maxValue,
        "pointCount", &T::pointCount
    );
};

// SeriesMetadataSync implementation
std::string SeriesMetadataSync::serialize() const {
    return glz::write_json(*this).value_or("{}");
}

SeriesMetadataSync SeriesMetadataSync::deserialize(const std::string& data) {
    SeriesMetadataSync metadata;
    auto error = glz::read_json(metadata, data);
    
    if (error) {
        throw std::runtime_error("Failed to parse series metadata: " + std::string(glz::format_error(error)));
    }
    
    return metadata;
}

std::string SeriesMetadataSync::generateSeriesKey(const std::string& measurement,
                                                  const std::map<std::string, std::string>& tags,
                                                  const std::string& field) {
    std::stringstream ss;
    ss << measurement;
    
    for (const auto& [k, v] : tags) {
        ss << "," << k << "=" << v;
    }
    ss << "," << field;
    
    return ss.str();
}

// FieldStatsSync implementation
std::string FieldStatsSync::serialize() const {
    return glz::write_json(*this).value_or("{}");
}

FieldStatsSync FieldStatsSync::deserialize(const std::string& data) {
    FieldStatsSync stats;
    auto error = glz::read_json(stats, data);
    
    if (error) {
        throw std::runtime_error("Failed to parse field stats: " + std::string(glz::format_error(error)));
    }
    
    return stats;
}

// MetadataIndexSync implementation
MetadataIndexSync::MetadataIndexSync(const std::string& path) : dbPath(path) {}

MetadataIndexSync::~MetadataIndexSync() {
    db.reset();
    delete filterPolicy_;
    delete blockCache_;
}

void MetadataIndexSync::init() {
    filterPolicy_ = leveldb::NewBloomFilterPolicy(10);
    blockCache_ = leveldb::NewLRUCache(8 * 1024 * 1024);

    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kSnappyCompression;
    options.write_buffer_size = 4 * 1024 * 1024;
    options.max_open_files = 100;
    options.filter_policy = filterPolicy_;
    options.block_cache = blockCache_;

    leveldb::DB* dbPtr;
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &dbPtr);

    if (!status.ok()) {
        delete filterPolicy_;
        filterPolicy_ = nullptr;
        delete blockCache_;
        blockCache_ = nullptr;
        throw std::runtime_error("Failed to open metadata index: " + status.ToString());
    }

    db.reset(dbPtr);
    
    std::string value;
    status = db->Get(leveldb::ReadOptions(), "meta:nextSeriesId", &value);
    if (status.ok()) {
        nextSeriesId = std::stoull(value);
    }
}

void MetadataIndexSync::close() {
    if (db) {
        db->Put(leveldb::WriteOptions(), "meta:nextSeriesId", std::to_string(nextSeriesId));
        delete db.release();
    }
    delete filterPolicy_;
    filterPolicy_ = nullptr;
    delete blockCache_;
    blockCache_ = nullptr;
}

// Key generation helpers
std::string MetadataIndexSync::seriesKey(uint64_t seriesId) {
    std::stringstream ss;
    ss << "series:" << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndexSync::measurementKey(const std::string& measurement, uint64_t seriesId) {
    std::stringstream ss;
    ss << "m:" << measurement << ":" << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndexSync::tagKey(const std::string& measurement, const std::string& tagKey,
                                      const std::string& tagValue, uint64_t seriesId) {
    std::stringstream ss;
    ss << "t:" << measurement << ":" << tagKey << ":" << tagValue << ":" 
       << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndexSync::buildSortedTagString(const std::map<std::string, std::string>& tags) {
    std::stringstream ss;
    bool first = true;
    for (const auto& [k, v] : tags) {
        if (!first) ss << ",";
        ss << k << "=" << v;
        first = false;
    }
    return ss.str();
}

std::string MetadataIndexSync::compositeTagKey(const std::string& measurement,
                                               const std::map<std::string, std::string>& tags,
                                               uint64_t seriesId) {
    std::stringstream ss;
    ss << "ct:" << measurement << ":" << buildSortedTagString(tags) << ":"
       << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndexSync::fieldKey(const std::string& measurement, const std::string& field,
                                        uint64_t seriesId) {
    std::stringstream ss;
    ss << "f:" << measurement << ":" << field << ":"
       << std::setfill('0') << std::setw(16) << std::hex << seriesId;
    return ss.str();
}

std::string MetadataIndexSync::groupByKey(const std::string& measurement, const std::string& tagKey,
                                          const std::string& tagValue) {
    return "g:" + measurement + ":" + tagKey + ":" + tagValue;
}

std::string MetadataIndexSync::seriesLookupKey(const std::string& seriesKey) {
    return "lookup:" + seriesKey;
}

std::vector<std::string> MetadataIndexSync::generateTagSubsets(const std::map<std::string, std::string>& tags) {
    // Only generate subsets of size 1 through MAX_COMPOSITE_SUBSET_SIZE to avoid
    // O(2^N) explosion. See the comment in MetadataIndex::generateTagSubsets for
    // the full rationale. Queries with more filter tags than this limit will fall
    // back to intersecting results from smaller composite lookups.
    static constexpr size_t MAX_COMPOSITE_SUBSET_SIZE = 3;

    std::vector<std::string> subsets;

    std::vector<std::pair<std::string, std::string>> tagVec(tags.begin(), tags.end());
    const size_t n = tagVec.size();

    if (n == 0) {
        return subsets;
    }

    size_t estimate = n;
    if (MAX_COMPOSITE_SUBSET_SIZE >= 2 && n >= 2)
        estimate += n * (n - 1) / 2;
    if (MAX_COMPOSITE_SUBSET_SIZE >= 3 && n >= 3)
        estimate += n * (n - 1) * (n - 2) / 6;
    subsets.reserve(estimate);

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

uint64_t MetadataIndexSync::getOrCreateSeriesId(const std::string& measurement,
                                                const std::map<std::string, std::string>& tags,
                                                const std::string& field) {
    std::string sKey = SeriesMetadataSync::generateSeriesKey(measurement, tags, field);
    std::string lookupKey = seriesLookupKey(sKey);
    
    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), lookupKey, &value);
    
    if (status.ok()) {
        return std::stoull(value);
    }
    
    uint64_t seriesId = nextSeriesId++;
    
    SeriesMetadataSync metadata;
    metadata.seriesId = seriesId;
    metadata.measurement = measurement;
    metadata.tags = tags;
    metadata.fields.push_back(field);
    metadata.minTime = std::numeric_limits<int64_t>::max();
    metadata.maxTime = std::numeric_limits<int64_t>::min();
    metadata.shardId = seriesId % 32;  // Default to 32 shards
    
    leveldb::WriteBatch batch;
    
    batch.Put(lookupKey, std::to_string(seriesId));
    batch.Put(seriesKey(seriesId), metadata.serialize());
    batch.Put(measurementKey(measurement, seriesId), "");
    
    for (const auto& [k, v] : tags) {
        batch.Put(tagKey(measurement, k, v, seriesId), "");
    }
    
    for (const auto& subset : generateTagSubsets(tags)) {
        batch.Put("ct:" + measurement + ":" + subset + ":" + 
                 std::to_string(seriesId), "");
    }
    
    batch.Put(fieldKey(measurement, field, seriesId), "");
    
    for (const auto& [k, v] : tags) {
        std::string gKey = groupByKey(measurement, k, v);
        batch.Put(gKey + ":" + std::to_string(seriesId), "");
    }
    
    status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to create series: " + status.ToString());
    }
    
    return seriesId;
}

std::vector<uint64_t> MetadataIndexSync::findSeriesByMeasurement(const std::string& measurement) {
    std::vector<uint64_t> seriesIds;
    std::string prefix = "m:" + measurement + ":";

    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        std::string key = it->key().ToString();
        size_t lastColon = key.rfind(':');
        if (lastColon != std::string::npos) {
            std::string idStr = key.substr(lastColon + 1);
            uint64_t seriesId = std::stoull(idStr, nullptr, 16);
            seriesIds.push_back(seriesId);
        }
    }

    return seriesIds;
}

std::vector<uint64_t> MetadataIndexSync::findSeriesByTag(const std::string& measurement,
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
            uint64_t seriesId = std::stoull(idStr, nullptr, 16);
            seriesIds.push_back(seriesId);
        }
    }

    return seriesIds;
}

std::vector<uint64_t> MetadataIndexSync::findSeriesByTags(const std::string& measurement,
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
            uint64_t seriesId = std::stoull(idStr, nullptr, 16);
            seriesIds.push_back(seriesId);
        }
    }

    return seriesIds;
}

std::optional<SeriesMetadataSync> MetadataIndexSync::getSeriesMetadata(uint64_t seriesId) {
    std::string key = seriesKey(seriesId);
    std::string value;
    
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
        return SeriesMetadataSync::deserialize(value);
    }
    
    return std::nullopt;
}

void MetadataIndexSync::deleteSeries(uint64_t seriesId) {
    auto metadata = getSeriesMetadata(seriesId);
    if (!metadata.has_value()) {
        return;
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

    // Delete composite tag indexes (all subsets, mirrors getOrCreateSeriesId)
    for (const auto& subset : generateTagSubsets(metadata->tags)) {
        batch.Delete("ct:" + metadata->measurement + ":" + subset + ":" +
                     std::to_string(seriesId));
    }

    // Delete field indexes
    for (const auto& field : metadata->fields) {
        batch.Delete(fieldKey(metadata->measurement, field, seriesId));
        batch.Delete("fstats:" + metadata->measurement + ":" + field + ":" + std::to_string(seriesId));
    }

    // Delete series lookup keys for ALL fields
    for (const auto& field : metadata->fields) {
        std::string sKey = SeriesMetadataSync::generateSeriesKey(metadata->measurement, metadata->tags, field);
        batch.Delete(seriesLookupKey(sKey));
    }
    if (metadata->fields.empty()) {
        std::string sKey = SeriesMetadataSync::generateSeriesKey(metadata->measurement, metadata->tags, "");
        batch.Delete(seriesLookupKey(sKey));
    }

    leveldb::Status status = db->Write(leveldb::WriteOptions(), &batch);
    if (!status.ok()) {
        throw std::runtime_error("Failed to delete series: " + status.ToString());
    }
}

std::string MetadataIndexSync::getStats() const {
    std::string stats;
    db->GetProperty("leveldb.stats", &stats);
    return stats;
}