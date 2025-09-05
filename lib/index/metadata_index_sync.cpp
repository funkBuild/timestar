#include "metadata_index_sync.hpp"
#include <leveldb/options.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <iostream>

// SeriesMetadataSync implementation
std::string SeriesMetadataSync::serialize() const {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("seriesId", seriesId, allocator);
    doc.AddMember("measurement", rapidjson::Value(measurement.c_str(), allocator), allocator);
    doc.AddMember("minTime", minTime, allocator);
    doc.AddMember("maxTime", maxTime, allocator);
    doc.AddMember("shardId", shardId, allocator);
    
    rapidjson::Value tagsObj(rapidjson::kObjectType);
    for (const auto& [k, v] : tags) {
        tagsObj.AddMember(
            rapidjson::Value(k.c_str(), allocator),
            rapidjson::Value(v.c_str(), allocator),
            allocator
        );
    }
    doc.AddMember("tags", tagsObj, allocator);
    
    rapidjson::Value fieldsArr(rapidjson::kArrayType);
    for (const auto& field : fields) {
        fieldsArr.PushBack(rapidjson::Value(field.c_str(), allocator), allocator);
    }
    doc.AddMember("fields", fieldsArr, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
}

SeriesMetadataSync SeriesMetadataSync::deserialize(const std::string& data) {
    SeriesMetadataSync metadata;
    rapidjson::Document doc;
    doc.Parse(data.c_str());
    
    if (doc.HasParseError()) {
        throw std::runtime_error("Failed to parse series metadata");
    }
    
    metadata.seriesId = doc["seriesId"].GetUint64();
    metadata.measurement = doc["measurement"].GetString();
    metadata.minTime = doc["minTime"].GetInt64();
    metadata.maxTime = doc["maxTime"].GetInt64();
    metadata.shardId = doc["shardId"].GetUint();
    
    if (doc.HasMember("tags") && doc["tags"].IsObject()) {
        for (const auto& tag : doc["tags"].GetObject()) {
            metadata.tags[tag.name.GetString()] = tag.value.GetString();
        }
    }
    
    if (doc.HasMember("fields") && doc["fields"].IsArray()) {
        for (const auto& field : doc["fields"].GetArray()) {
            metadata.fields.push_back(field.GetString());
        }
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
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("dataType", rapidjson::Value(dataType.c_str(), allocator), allocator);
    doc.AddMember("minValue", minValue, allocator);
    doc.AddMember("maxValue", maxValue, allocator);
    doc.AddMember("pointCount", pointCount, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
}

FieldStatsSync FieldStatsSync::deserialize(const std::string& data) {
    FieldStatsSync stats;
    rapidjson::Document doc;
    doc.Parse(data.c_str());
    
    if (doc.HasParseError()) {
        throw std::runtime_error("Failed to parse field stats");
    }
    
    stats.dataType = doc["dataType"].GetString();
    stats.minValue = doc["minValue"].GetDouble();
    stats.maxValue = doc["maxValue"].GetDouble();
    stats.pointCount = doc["pointCount"].GetUint64();
    
    return stats;
}

// MetadataIndexSync implementation
MetadataIndexSync::MetadataIndexSync(const std::string& path) : dbPath(path) {}

MetadataIndexSync::~MetadataIndexSync() {
    if (db) {
        delete db.release();
    }
}

void MetadataIndexSync::init() {
    leveldb::Options options;
    options.create_if_missing = true;
    options.compression = leveldb::kSnappyCompression;
    options.write_buffer_size = 4 * 1024 * 1024;
    options.max_open_files = 100;
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.block_cache = leveldb::NewLRUCache(8 * 1024 * 1024);
    
    leveldb::DB* dbPtr;
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &dbPtr);
    
    if (!status.ok()) {
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
        db->Put(leveldb::WriteOptions(), "meta:nextSeriesId", std::to_string(nextSeriesId.load()));
        delete db.release();
    }
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
    std::vector<std::string> subsets;
    
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
    
    uint64_t seriesId = nextSeriesId.fetch_add(1);
    
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
    
    return seriesIds;
}

std::vector<uint64_t> MetadataIndexSync::findSeriesByTag(const std::string& measurement,
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
    
    return seriesIds;
}

std::vector<uint64_t> MetadataIndexSync::findSeriesByTags(const std::string& measurement,
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

std::string MetadataIndexSync::getStats() const {
    std::string stats;
    db->GetProperty("leveldb.stats", &stats);
    return stats;
}