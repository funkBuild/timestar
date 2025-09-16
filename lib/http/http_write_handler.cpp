#include "http_write_handler.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <sstream>
#include <chrono>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <numeric>
#include <algorithm>
#include <boost/iterator/counting_iterator.hpp>

using namespace seastar;
using namespace httpd;

// Glaze-compatible structures for JSON parsing
struct GlazeWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    glz::json_t fields;  // Can hold heterogeneous types
    std::optional<uint64_t> timestamp;
};

template <>
struct glz::meta<GlazeWritePoint> {
    using T = GlazeWritePoint;
    static constexpr auto value = object(
        "measurement", &T::measurement,
        "tags", &T::tags,
        "fields", &T::fields,
        "timestamp", &T::timestamp
    );
};

struct GlazeMultiWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    glz::json_t fields;  // Can hold arrays or scalars
    std::optional<std::variant<uint64_t, std::vector<uint64_t>>> timestamps;
    std::optional<uint64_t> timestamp;  // Single timestamp option
};

template <>
struct glz::meta<GlazeMultiWritePoint> {
    using T = GlazeMultiWritePoint;
    static constexpr auto value = object(
        "measurement", &T::measurement,
        "tags", &T::tags,
        "fields", &T::fields,
        "timestamps", &T::timestamps,
        "timestamp", &T::timestamp
    );
};

struct GlazeBatchWrite {
    std::vector<glz::json_t> writes;
};

template <>
struct glz::meta<GlazeBatchWrite> {
    using T = GlazeBatchWrite;
    static constexpr auto value = object(
        "writes", &T::writes
    );
};

HttpWriteHandler::HttpWriteHandler(seastar::sharded<Engine>* _engineSharded) 
    : engineSharded(_engineSharded) {
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const glz::json_t& point) {
    MultiWritePoint mwp;
    
    // Parse using Glaze structure
    GlazeMultiWritePoint glazePoint;
    std::string json_str;
    auto write_error = glz::write_json(point, json_str);
    if (write_error) {
        throw std::runtime_error("Failed to serialize point for parsing: " + std::string(glz::format_error(write_error)));
    }
    auto error = glz::read_json(glazePoint, json_str);
    if (error) {
        throw std::runtime_error("Failed to parse write point: " + std::string(glz::format_error(error)));
    }
    
    mwp.measurement = glazePoint.measurement;
    mwp.tags = glazePoint.tags;
    
    // Parse fields - handle both scalars and arrays
    if (glazePoint.fields.is_object()) {
        auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
        
        for (auto& [fieldName, fieldValue] : fields_obj) {
            FieldArrays fa;
            
            if (fieldValue.is_array()) {
                auto& arr = fieldValue.get<glz::json_t::array_t>();
                if (arr.empty()) {
                    throw std::runtime_error("Field array cannot be empty: " + fieldName);
                }
                
                // Determine type from first element
                if (arr[0].is_number()) {
                    fa.type = FieldArrays::DOUBLE;
                    for (auto& elem : arr) {
                        if (elem.is_number()) {
                            fa.doubles.push_back(elem.get<double>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                } else if (arr[0].is_boolean()) {
                    fa.type = FieldArrays::BOOL;
                    for (auto& elem : arr) {
                        if (elem.is_boolean()) {
                            fa.bools.push_back(elem.get<bool>() ? 1 : 0);
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                } else if (arr[0].is_string()) {
                    fa.type = FieldArrays::STRING;
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Detected string array field '{}' with {} elements", fieldName, arr.size());
                    for (auto& elem : arr) {
                        if (elem.is_string()) {
                            fa.strings.push_back(elem.get<std::string>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Added {} string values to field '{}'", fa.strings.size(), fieldName);
                } else {
                    throw std::runtime_error("Unsupported field array type: " + fieldName);
                }
            } else {
                // Single value - convert to array of size 1
                if (fieldValue.is_number()) {
                    fa.type = FieldArrays::DOUBLE;
                    fa.doubles.push_back(fieldValue.get<double>());
                } else if (fieldValue.is_boolean()) {
                    fa.type = FieldArrays::BOOL;
                    fa.bools.push_back(fieldValue.get<bool>() ? 1 : 0);
                } else if (fieldValue.is_string()) {
                    fa.type = FieldArrays::STRING;
                    auto str_value = fieldValue.get<std::string>();
                    fa.strings.push_back(str_value);
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Detected single string field '{}' with value: '{}'", fieldName, str_value);
                } else {
                    throw std::runtime_error("Unsupported field type: " + fieldName);
                }
            }
            
            mwp.fields[fieldName] = std::move(fa);
        }
    } else {
        throw std::runtime_error("Fields must be an object");
    }
    
    // Parse timestamps
    if (glazePoint.timestamps) {
        if (std::holds_alternative<uint64_t>(*glazePoint.timestamps)) {
            mwp.timestamps.push_back(std::get<uint64_t>(*glazePoint.timestamps));
        } else {
            mwp.timestamps = std::get<std::vector<uint64_t>>(*glazePoint.timestamps);
        }
    } else if (glazePoint.timestamp) {
        // Single timestamp - determine how many we need from fields
        size_t numPoints = 1;
        for (const auto& [fieldName, fieldArray] : mwp.fields) {
            size_t fieldSize = 0;
            switch (fieldArray.type) {
                case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
                case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
                case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
                case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
            }
            numPoints = std::max(numPoints, fieldSize);
        }
        mwp.timestamps.resize(numPoints, *glazePoint.timestamp);
    } else {
        // No timestamp - use current time
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        uint64_t currentTime = static_cast<uint64_t>(nanos);
        
        size_t numPoints = 1;
        for (const auto& [fieldName, fieldArray] : mwp.fields) {
            size_t fieldSize = 0;
            switch (fieldArray.type) {
                case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
                case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
                case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
                case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
            }
            numPoints = std::max(numPoints, fieldSize);
        }
        
        // Generate timestamps 1ms apart
        for (size_t i = 0; i < numPoints; i++) {
            mwp.timestamps.push_back(currentTime + i * 1000000);
        }
    }
    
    return mwp;
}

std::vector<HttpWriteHandler::MultiWritePoint> HttpWriteHandler::coalesceWrites(const glz::json_t::array_t& writes_array) {
    // Configuration constants
    static const size_t MAX_COALESCE_SIZE = 10000;  // Max values per field array
    static const size_t MIN_COALESCE_COUNT = 2;     // Min writes needed to coalesce
    
    auto start_coalesce = std::chrono::high_resolution_clock::now();
    
    // Fast direct parsing approach - avoid JSON string serialization
    std::unordered_map<std::string, CoalesceCandidate> candidates;
    size_t totalWritesProcessed = 0;
    
    LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Processing {} writes for coalescing", writes_array.size());
    
    // TEMPORARY: Disable coalescing to isolate crash cause  
    LOG_INSERT_PATH(tsdb::http_log, info, "[COALESCE] Coalescing disabled for debugging - returning empty result");
    std::vector<MultiWritePoint> empty_result;
    auto end_coalesce_debug = std::chrono::high_resolution_clock::now();
    auto coalesce_duration_debug = std::chrono::duration_cast<std::chrono::microseconds>(end_coalesce_debug - start_coalesce);
    
    LOG_INSERT_PATH(tsdb::http_log, info, 
        "[COALESCE] Processed {} writes -> {} array writes + {} individual writes ({}μs)", 
        writes_array.size(), 0, writes_array.size(), coalesce_duration_debug.count());
    
    return empty_result;  // Return empty result - no coalescing
    
    // Parse writes directly from JSON objects for better performance
    for (const auto& write : writes_array) {
        totalWritesProcessed++;
        
        if (!write.is_object()) continue;
        
        try {
            auto& writeObj = write.get<glz::json_t::object_t>();
            
            // Extract measurement
            auto measurementIt = writeObj.find("measurement");
            if (measurementIt == writeObj.end() || !measurementIt->second.is_string()) continue;
            std::string measurement = measurementIt->second.get<std::string>();
            
            // Extract timestamp 
            uint64_t timestamp = 0;
            auto timestampIt = writeObj.find("timestamp");
            if (timestampIt != writeObj.end() && timestampIt->second.is_number()) {
                timestamp = static_cast<uint64_t>(timestampIt->second.get<double>());
            } else {
                // Generate timestamp
                auto now = std::chrono::system_clock::now();
                auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
                timestamp = static_cast<uint64_t>(nanos);
            }
            
            // Extract tags 
            std::map<std::string, std::string> tags;
            auto tagsIt = writeObj.find("tags");
            if (tagsIt != writeObj.end() && tagsIt->second.is_object()) {
                auto& tagsObj = tagsIt->second.get<glz::json_t::object_t>();
                for (const auto& [tagKey, tagValue] : tagsObj) {
                    if (tagValue.is_string()) {
                        tags[tagKey] = tagValue.get<std::string>();
                    }
                }
            }
            
            // Extract fields and process each field
            auto fieldsIt = writeObj.find("fields");
            if (fieldsIt == writeObj.end() || !fieldsIt->second.is_object()) continue;
            
            auto& fieldsObj = fieldsIt->second.get<glz::json_t::object_t>();
            for (const auto& [fieldName, fieldValue] : fieldsObj) {
                // Build series key efficiently 
                std::string seriesKey;
                seriesKey.reserve(measurement.length() + fieldName.length() + 64); // Reserve space
                seriesKey = measurement;
                for (const auto& [tagKey, tagValue] : tags) {
                    seriesKey += ",";
                    seriesKey += tagKey;
                    seriesKey += "=";
                    seriesKey += tagValue;
                }
                seriesKey += " ";
                seriesKey += fieldName;
                
                // Determine value type quickly
                TSMValueType valueType;
                if (fieldValue.is_number()) {
                    valueType = TSMValueType::Float;
                } else if (fieldValue.is_boolean()) {
                    valueType = TSMValueType::Boolean;
                } else if (fieldValue.is_string()) {
                    valueType = TSMValueType::String;
                } else {
                    continue; // Skip unsupported types
                }
                
                // Find or create candidate
                auto it = candidates.find(seriesKey);
                if (it == candidates.end()) {
                    // Create new candidate
                    CoalesceCandidate candidate;
                    candidate.seriesKey = std::move(seriesKey);
                    candidate.measurement = measurement;
                    candidate.tags = tags;
                    candidate.fieldName = fieldName;
                    candidate.valueType = valueType;
                    
                    // Add the first value efficiently 
                    if (valueType == TSMValueType::Float) {
                        candidate.timestamps.push_back(timestamp);
                        candidate.doubleValues.push_back(fieldValue.get<double>());
                    } else if (valueType == TSMValueType::Boolean) {
                        candidate.timestamps.push_back(timestamp);
                        candidate.boolValues.push_back(fieldValue.get<bool>() ? 1 : 0);
                    } else if (valueType == TSMValueType::String) {
                        candidate.timestamps.push_back(timestamp);
                        candidate.stringValues.push_back(fieldValue.get<std::string>());
                    }
                    
                    std::string keyCopy = candidate.seriesKey;  // Copy the key before move
                    candidates.emplace(keyCopy, std::move(candidate));
                } else {
                    // Add to existing candidate
                    CoalesceCandidate& candidate = it->second;
                    
                    if (candidate.valueType != valueType || candidate.timestamps.size() >= MAX_COALESCE_SIZE) {
                        continue; // Skip incompatible or oversized
                    }
                    
                    // Add value efficiently
                    candidate.timestamps.push_back(timestamp);
                    if (valueType == TSMValueType::Float) {
                        candidate.doubleValues.push_back(fieldValue.get<double>());
                    } else if (valueType == TSMValueType::Boolean) {
                        candidate.boolValues.push_back(fieldValue.get<bool>() ? 1 : 0);
                    } else if (valueType == TSMValueType::String) {
                        candidate.stringValues.push_back(fieldValue.get<std::string>());
                    }
                }
            }
        } catch (const std::exception& e) {
            LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Failed to parse write: {}", e.what());
            continue;
        }
    }
    
    // Second pass: convert candidates to MultiWritePoint objects (simplified)
    std::vector<MultiWritePoint> result;
    
    // Group by measurement+tags efficiently
    std::unordered_map<std::string, std::vector<CoalesceCandidate*>> grouped;
    size_t individualWrites = 0;
    
    for (auto& [seriesKey, candidate] : candidates) {
        if (candidate.timestamps.size() < MIN_COALESCE_COUNT) {
            individualWrites++;
            continue;
        }
        
        // Build group key (measurement + tags, without field)
        std::string groupKey = candidate.measurement;
        for (const auto& [tagKey, tagValue] : candidate.tags) {
            groupKey += ",";
            groupKey += tagKey;
            groupKey += "=";
            groupKey += tagValue;
        }
        
        grouped[groupKey].push_back(&candidate);
    }
    
    // Build MultiWritePoint objects
    for (auto& [groupKey, candidateGroup] : grouped) {
        if (candidateGroup.empty()) continue;
        
        // Verify all candidates have the same timestamps to avoid data corruption
        CoalesceCandidate* firstCandidate = candidateGroup[0];
        bool timestampsCompatible = true;
        
        for (size_t i = 1; i < candidateGroup.size(); i++) {
            if (candidateGroup[i]->timestamps.size() != firstCandidate->timestamps.size()) {
                timestampsCompatible = false;
                break;
            }
            // Check if timestamps match (allowing for different ordering)
            std::vector<uint64_t> sortedFirst = firstCandidate->timestamps;
            std::vector<uint64_t> sortedCurrent = candidateGroup[i]->timestamps;
            std::sort(sortedFirst.begin(), sortedFirst.end());
            std::sort(sortedCurrent.begin(), sortedCurrent.end());
            
            if (sortedFirst != sortedCurrent) {
                timestampsCompatible = false;
                break;
            }
        }
        
        if (!timestampsCompatible) {
            LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Skipping group '{}' due to incompatible timestamps", groupKey);
            individualWrites += candidateGroup.size();
            continue;
        }
        
        MultiWritePoint mwp;
        mwp.measurement = firstCandidate->measurement;
        mwp.tags = firstCandidate->tags;
        mwp.timestamps = firstCandidate->timestamps; // Use first as template
        
        // NOTE: Don't sort timestamps here because field arrays are in the same original order
        // Sorting would misalign timestamps with field values and cause memory corruption
        
        for (CoalesceCandidate* candidate : candidateGroup) {
            FieldArrays fa;
            fa.type = (candidate->valueType == TSMValueType::Float) ? FieldArrays::DOUBLE :
                     (candidate->valueType == TSMValueType::Boolean) ? FieldArrays::BOOL :
                     FieldArrays::STRING;
            
            // Simple assignment without complex sorting
            if (candidate->valueType == TSMValueType::Float) {
                fa.doubles = std::move(candidate->doubleValues);
            } else if (candidate->valueType == TSMValueType::Boolean) {
                fa.bools = std::move(candidate->boolValues);
            } else {
                fa.strings = std::move(candidate->stringValues);
            }
            
            mwp.fields[candidate->fieldName] = std::move(fa);
        }
        
        result.push_back(std::move(mwp));
    }
    
    auto end_coalesce = std::chrono::high_resolution_clock::now();
    auto coalesce_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_coalesce - start_coalesce);
    
    LOG_INSERT_PATH(tsdb::http_log, info, 
        "[COALESCE] Processed {} writes -> {} array writes + {} individual writes ({}μs)", 
        totalWritesProcessed, result.size(), individualWrites, coalesce_duration.count());
    
    return result;
}

bool HttpWriteHandler::validateArraySizes(const MultiWritePoint& point, std::string& error) {
    size_t expectedSize = point.timestamps.size();
    
    for (const auto& [fieldName, fieldArray] : point.fields) {
        size_t fieldSize = 0;
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: fieldSize = fieldArray.doubles.size(); break;
            case FieldArrays::BOOL: fieldSize = fieldArray.bools.size(); break;
            case FieldArrays::STRING: fieldSize = fieldArray.strings.size(); break;
            case FieldArrays::INTEGER: fieldSize = fieldArray.integers.size(); break;
        }
        
        if (fieldSize != expectedSize && fieldSize != 1) {
            error = "Field '" + fieldName + "' has " + std::to_string(fieldSize) + 
                    " values but expected " + std::to_string(expectedSize) + " (or 1 for scalar)";
            return false;
        }
    }
    
    return true;
}

seastar::future<HttpWriteHandler::AggregatedTimingInfo> HttpWriteHandler::processMultiWritePoint(const MultiWritePoint& point) {
    auto start_total = std::chrono::high_resolution_clock::now();
    
    // Group inserts by shard to reduce cross-shard operations and process them in batches
    struct PendingInsert {
        TSMValueType valueType;
        std::string measurement;
        std::string fieldName;
        std::map<std::string, std::string> tags;
        uint64_t timestamp;
        
        // Value storage (only one will be used based on valueType)
        double doubleValue = 0.0;
        bool boolValue = false;
        std::string stringValue;
        
        bool needsMetadataIndex = false;
    };
    
    // Helper struct for metadata operations
    struct MetaOp {
        TSMValueType valueType;
        std::string measurement;
        std::string fieldName;
        std::map<std::string, std::string> tags;
    };
    
    // Group inserts by shard to minimize cross-shard operations
    std::map<size_t, std::vector<PendingInsert>> shardInserts;
    
    auto start_grouping = std::chrono::high_resolution_clock::now();
    
    // Flatten all operations and group by shard
    for (size_t i = 0; i < point.timestamps.size(); i++) {
        uint64_t timestamp = point.timestamps[i];
        
        for (const auto& [fieldName, fieldArray] : point.fields) {
            // Build the complete series key for sharding
            std::string seriesKey = point.measurement;
            for (const auto& [tagKey, tagValue] : point.tags) {
                seriesKey += "," + tagKey + "=" + tagValue;
            }
            seriesKey += " " + fieldName;  // Space separator before field, not comma!
            
            // Use SeriesId128 for consistent sharding
            SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
            size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;
            
            // Note: metadata indexing is now handled after shard grouping
            // using the actual insert types from the PendingInsert structures
            
            // Get the index - either use i if field has multiple values, or 0 if it's scalar
            size_t fieldIndex = 0;
            size_t fieldSize = 0;
            
            PendingInsert insert;
            insert.measurement = point.measurement;
            insert.fieldName = fieldName;
            insert.tags = point.tags;
            insert.timestamp = timestamp;
            insert.needsMetadataIndex = false;  // Metadata indexing handled separately
            
            switch (fieldArray.type) {
                case FieldArrays::DOUBLE:
                    fieldSize = fieldArray.doubles.size();
                    if (fieldSize == 0) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Empty doubles array for field '{}'", fieldName);
                        continue; // Skip this field entirely
                    }
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    if (fieldIndex >= fieldSize) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Index {} out of bounds for doubles array size {}", fieldIndex, fieldSize);
                        continue; // Skip this field entirely
                    }
                    insert.valueType = TSMValueType::Float;
                    insert.doubleValue = fieldArray.doubles[fieldIndex];
                    break;
                    
                case FieldArrays::BOOL:
                    fieldSize = fieldArray.bools.size();
                    if (fieldSize == 0) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Empty bools array for field '{}'", fieldName);
                        continue; // Skip this field entirely
                    }
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    if (fieldIndex >= fieldSize) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Index {} out of bounds for bools array size {}", fieldIndex, fieldSize);
                        continue; // Skip this field entirely
                    }
                    insert.valueType = TSMValueType::Boolean;
                    insert.boolValue = (fieldArray.bools[fieldIndex] != 0);
                    break;
                    
                case FieldArrays::STRING:
                    fieldSize = fieldArray.strings.size();
                    if (fieldSize == 0) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Empty strings array for field '{}'", fieldName);
                        continue; // Skip this field entirely
                    }
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    if (fieldIndex >= fieldSize) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Index {} out of bounds for strings array size {}", fieldIndex, fieldSize);
                        continue; // Skip this field entirely
                    }
                    insert.valueType = TSMValueType::String;
                    insert.stringValue = fieldArray.strings[fieldIndex];
                    LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Prepared array string insert - field: '{}', value: '{}', timestamp: {}, shard: {}", 
                                   fieldName, insert.stringValue, timestamp, shard);
                    break;
                    
                case FieldArrays::INTEGER:
                    fieldSize = fieldArray.integers.size();
                    if (fieldSize == 0) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Empty integers array for field '{}'", fieldName);
                        continue; // Skip this field entirely
                    }
                    fieldIndex = (fieldSize > 1) ? i : 0;
                    if (fieldIndex >= fieldSize) {
                        LOG_INSERT_PATH(tsdb::http_log, error, "[WRITE] Index {} out of bounds for integers array size {}", fieldIndex, fieldSize);
                        continue; // Skip this field entirely
                    }
                    insert.valueType = TSMValueType::Float;
                    insert.doubleValue = static_cast<double>(fieldArray.integers[fieldIndex]);
                    break;
            }
            
            shardInserts[shard].push_back(std::move(insert));
        }
    }
    
    auto end_grouping = std::chrono::high_resolution_clock::now();
    auto grouping_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_grouping - start_grouping);
    
    // Collect unique (measurement, field) with their true types
    auto start_metadata = std::chrono::high_resolution_clock::now();
    std::unordered_set<std::string> seenMF;
    std::vector<MetaOp> metaOps;
    metaOps.reserve(point.fields.size());

    for (const auto& [shard, vec] : shardInserts) {
        for (const auto& ins : vec) {
            std::string key = ins.measurement + '|' + ins.fieldName;
            if (seenMF.insert(key).second) {
                metaOps.push_back(MetaOp{
                    ins.valueType,
                    ins.measurement,
                    ins.fieldName,
                    ins.tags
                });
            }
        }
    }

    if (!metaOps.empty()) {
        co_await engineSharded->invoke_on(0, [metaOps = std::move(metaOps)](Engine& engine) -> seastar::future<> {
            co_await seastar::do_for_each(metaOps, [&engine](const MetaOp& m) -> seastar::future<> {
                switch (m.valueType) {
                    case TSMValueType::Float: {
                        TSDBInsert<double> idx(m.measurement, m.fieldName);
                        idx.tags = m.tags;
                        co_await engine.indexMetadata(idx);
                        break;
                    }
                    case TSMValueType::Boolean: {
                        TSDBInsert<bool> idx(m.measurement, m.fieldName);
                        idx.tags = m.tags;
                        co_await engine.indexMetadata(idx);
                        break;
                    }
                    case TSMValueType::String: {
                        TSDBInsert<std::string> idx(m.measurement, m.fieldName);
                        idx.tags = m.tags;
                        co_await engine.indexMetadata(idx);
                        break;
                    }
                }
                co_return;
            });
        });
    }
    
    auto end_metadata = std::chrono::high_resolution_clock::now();
    auto metadata_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_metadata - start_metadata);
    
    auto start_batch_ops = std::chrono::high_resolution_clock::now();
    
    // Collect timing from all shards
    AggregatedTimingInfo aggregatedTiming;
    
    // Process all inserts grouped by shard - this creates one cross-shard call per shard instead of per insert
    co_await seastar::do_for_each(shardInserts.begin(), shardInserts.end(),
        [this, &aggregatedTiming](const std::pair<const size_t, std::vector<PendingInsert>>& shardData) -> seastar::future<> {
        
        const size_t shard = shardData.first;
        const auto& inserts = shardData.second;
        
        auto shard_start = std::chrono::high_resolution_clock::now();
        
        // Process all inserts for this shard in a single cross-shard operation using batch inserts
        auto shardTiming = co_await engineSharded->invoke_on(shard, [inserts](Engine& engine) -> seastar::future<AggregatedTimingInfo> {
            
            // Group inserts by type for batch processing
            std::vector<TSDBInsert<double>> doubleInserts;
            std::vector<TSDBInsert<bool>> boolInserts;
            std::vector<TSDBInsert<std::string>> stringInserts;
            
            // Collect all inserts by type
            for (const auto& pendingInsert : inserts) {
                switch (pendingInsert.valueType) {
                    case TSMValueType::Float: {
                        TSDBInsert<double> insert(pendingInsert.measurement, pendingInsert.fieldName);
                        insert.tags = pendingInsert.tags;
                        insert.addValue(pendingInsert.timestamp, pendingInsert.doubleValue);
                        doubleInserts.push_back(std::move(insert));
                        break;
                    }
                    
                    case TSMValueType::Boolean: {
                        TSDBInsert<bool> insert(pendingInsert.measurement, pendingInsert.fieldName);
                        insert.tags = pendingInsert.tags;
                        insert.addValue(pendingInsert.timestamp, pendingInsert.boolValue);
                        boolInserts.push_back(std::move(insert));
                        break;
                    }
                    
                    case TSMValueType::String: {
                        TSDBInsert<std::string> insert(pendingInsert.measurement, pendingInsert.fieldName);
                        insert.tags = pendingInsert.tags;
                        insert.addValue(pendingInsert.timestamp, pendingInsert.stringValue);
                        
                        
                        stringInserts.push_back(std::move(insert));
                        break;
                    }
                }
            }
            
            // Now execute batch inserts by type and collect timing
            AggregatedTimingInfo batchTiming;
            
            if (!doubleInserts.empty()) {
                auto walTiming = co_await engine.insertBatch(doubleInserts);
                batchTiming.aggregate(walTiming);
            }
            
            if (!boolInserts.empty()) {
                auto walTiming = co_await engine.insertBatch(boolInserts);
                batchTiming.aggregate(walTiming);
            }
            
            if (!stringInserts.empty()) {
                auto walTiming = co_await engine.insertBatch(stringInserts);
                batchTiming.aggregate(walTiming);
            }
            
            co_return batchTiming;
        });
        
        // Aggregate timing from this shard
        aggregatedTiming.aggregate(shardTiming);
        
        auto shard_end = std::chrono::high_resolution_clock::now();
        auto shard_duration = std::chrono::duration_cast<std::chrono::microseconds>(shard_end - shard_start);
    });
    
    auto end_batch_ops = std::chrono::high_resolution_clock::now();
    auto batch_ops_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_batch_ops - start_batch_ops);
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);
    
    co_return aggregatedTiming;
}

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const std::string& json) {
    WritePoint wp;
    
    GlazeWritePoint glazePoint;
    auto error = glz::read_json(glazePoint, json);
    if (error) {
        throw std::runtime_error("Failed to parse write point: " + std::string(glz::format_error(error)));
    }
    
    wp.measurement = glazePoint.measurement;
    wp.tags = glazePoint.tags;
    
    // Parse fields
    if (!glazePoint.fields.is_object()) {
        throw std::runtime_error("'fields' must be an object");
    }
    
    auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
    if (fields_obj.empty()) {
        throw std::runtime_error("'fields' object cannot be empty");
    }
    
    for (auto& [fieldName, fieldValue] : fields_obj) {
        if (fieldValue.is_number()) {
            wp.fields[fieldName] = fieldValue.get<double>();
        } else if (fieldValue.is_boolean()) {
            wp.fields[fieldName] = fieldValue.get<bool>();
        } else if (fieldValue.is_string()) {
            auto str_value = fieldValue.get<std::string>();
            wp.fields[fieldName] = str_value;
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Single write: detected string field '{}' with value: '{}'", fieldName, str_value);
        } else {
            throw std::runtime_error("Unsupported field type for field: " + fieldName);
        }
    }
    
    // Parse timestamp
    if (glazePoint.timestamp) {
        wp.timestamp = *glazePoint.timestamp;
    } else {
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        wp.timestamp = static_cast<uint64_t>(nanos);
    }
    
    return wp;
}

seastar::future<> HttpWriteHandler::processWritePoint(const WritePoint& point) {
    // Process each field in the point sequentially using a simple loop
    for (const auto& field_pair : point.fields) {
        const auto& fieldName = field_pair.first;
        const auto& fieldValue = field_pair.second;
        
        // Build the complete series key for sharding
        std::string seriesKey = point.measurement;
        
        // Sort tags for consistent hashing (tags are already sorted in std::map)
        for (const auto& [tagKey, tagValue] : point.tags) {
            seriesKey += "," + tagKey + "=" + tagValue;
        }
        seriesKey += " " + fieldName;  // Space separator before field, not comma!
        
        // Use SeriesId128 for consistent sharding across the system
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;
        
        // Handle each variant type explicitly
        if (std::holds_alternative<double>(fieldValue)) {
            double value = std::get<double>(fieldValue);
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);
            
            // First index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            
            co_await engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            
        } else if (std::holds_alternative<bool>(fieldValue)) {
            bool value = std::get<bool>(fieldValue);
            TSDBInsert<bool> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);
            
            // First index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            
            co_await engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            
        } else if (std::holds_alternative<std::string>(fieldValue)) {
            const std::string& value = std::get<std::string>(fieldValue);
            TSDBInsert<std::string> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);
            
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing single string insert - field: '{}', value: '{}', timestamp: {}, shard: {}", 
                           fieldName, value, point.timestamp, shard);
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] String series key: '{}'", insert.seriesKey());
            
            // First index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            
            co_await engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            
        } else if (std::holds_alternative<int64_t>(fieldValue)) {
            int64_t value = std::get<int64_t>(fieldValue);
            // Convert integers to doubles for now
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, static_cast<double>(value));
            
            // First index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            
            co_await engineSharded->invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
        }
    }
    co_return;
}

std::string HttpWriteHandler::createErrorResponse(const std::string& error) {
    // Create JSON object directly
    auto response = glz::obj{"status", "error", "message", error};
    
    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","message":"Failed to serialize error response"})";
    }
    return buffer;
}

std::string HttpWriteHandler::createSuccessResponse(int pointsWritten) {
    // Create JSON object directly
    auto response = glz::obj{"status", "success", "points_written", pointsWritten};
    
    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","message":"Failed to serialize success response"})";
    }
    return buffer;
}

seastar::future<std::unique_ptr<seastar::http::reply>> 
HttpWriteHandler::handleWrite(std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    
    try {
        // Read the complete request body
        std::string body;
        if (!req->content.empty()) {
            body = req->content;
        }
        
        // Read from stream if available
        if (req->content_stream) {
            while (!req->content_stream->eof()) {
                auto data = co_await req->content_stream->read();
                body.append(data.get(), data.size());
            }
        }
        
        if (body.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Empty request body");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        LOG_INSERT_PATH(tsdb::http_log, debug, "Received write request: {} bytes", body.size());
        
        // Parse JSON using Glaze
        glz::json_t doc{};
        auto parse_error = glz::read_json(doc, body);
        
        if (parse_error) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(parse_error)));
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        int pointsWritten = 0;
        auto batchStartTime = std::chrono::steady_clock::now();
        
        // Check if it's a batch write or single write
        if (doc.is_object()) {
            auto& obj = doc.get<glz::json_t::object_t>();
            
            if (obj.contains("writes")) {
                // Batch write - coalesce individual writes into efficient array writes
                auto& writes = obj["writes"];
                if (writes.is_array()) {
                    auto& writes_array = writes.get<glz::json_t::array_t>();
                    
                    LOG_INSERT_PATH(tsdb::http_log, info, "[BATCH] Processing batch with {} writes", writes_array.size());
                    
                    // First, separate already-array writes from individual writes
                    std::vector<MultiWritePoint> arrayWrites;
                    std::vector<glz::json_t> individualWrites;
                    
                    for (auto& point : writes_array) {
                        try {
                            // Check if this point has arrays
                            bool hasArrays = false;
                            if (point.is_object()) {
                                auto& point_obj = point.get<glz::json_t::object_t>();
                                if (point_obj.contains("timestamps")) {
                                    hasArrays = true;
                                } else if (point_obj.contains("fields")) {
                                    auto& fields = point_obj["fields"];
                                    if (fields.is_object()) {
                                        auto& fields_obj = fields.get<glz::json_t::object_t>();
                                        for (auto& [name, value] : fields_obj) {
                                            if (value.is_array()) {
                                                hasArrays = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            
                            if (hasArrays) {
                                // Already an array write - process directly
                                MultiWritePoint mwp = parseMultiWritePoint(point);
                                std::string error;
                                if (!validateArraySizes(mwp, error)) {
                                    throw std::runtime_error(error);
                                }
                                arrayWrites.push_back(std::move(mwp));
                            } else {
                                // Individual write - collect for coalescing
                                individualWrites.push_back(point);
                            }
                        } catch (const std::exception& e) {
                            std::cerr << "Error processing point: " << e.what() << std::endl;
                            // Continue processing other points
                        }
                    }
                    
                    // Coalesce individual writes into array writes
                    auto coalesceStartTime = std::chrono::steady_clock::now();
                    std::vector<MultiWritePoint> coalescedWrites;
                    if (!individualWrites.empty()) {
                        glz::json_t::array_t individualArray(individualWrites.begin(), individualWrites.end());
                        coalescedWrites = coalesceWrites(individualArray);
                    }
                    auto coalesceEndTime = std::chrono::steady_clock::now();
                    auto coalesceDuration = std::chrono::duration_cast<std::chrono::microseconds>(coalesceEndTime - coalesceStartTime);
                    
                    LOG_INSERT_PATH(tsdb::http_log, info, "[BATCH] Coalesced {} individual writes into {} array writes, plus {} existing array writes", 
                                   individualWrites.size(), coalescedWrites.size(), arrayWrites.size());
                    
                    // Process all array writes (both existing and coalesced)
                    auto walStartTime = std::chrono::steady_clock::now();
                    int walWriteCount = 0;
                    AggregatedTimingInfo batchTiming;
                    
                    for (auto& mwp : arrayWrites) {
                        try {
                            auto timing = co_await processMultiWritePoint(mwp);
                            batchTiming.aggregate(timing);
                            pointsWritten += mwp.timestamps.size() * mwp.fields.size();
                            walWriteCount += mwp.fields.size(); // One WAL write per field type
                        } catch (const std::exception& e) {
                            std::cerr << "Error processing array write: " << e.what() << std::endl;
                        }
                    }
                    
                    for (auto& mwp : coalescedWrites) {
                        try {
                            auto timing = co_await processMultiWritePoint(mwp);
                            batchTiming.aggregate(timing);
                            pointsWritten += mwp.timestamps.size() * mwp.fields.size();
                            walWriteCount += mwp.fields.size(); // One WAL write per field type
                        } catch (const std::exception& e) {
                            std::cerr << "Error processing coalesced write: " << e.what() << std::endl;
                        }
                    }
                    
                    // Handle individual writes that couldn't be coalesced
                    // We need to check if any individual writes were not included in coalescing
                    // and process them separately to avoid data loss
                    size_t coalescedPoints = 0;
                    for (const auto& mwp : coalescedWrites) {
                        coalescedPoints += mwp.timestamps.size();
                    }
                    
                    // If we have fewer coalesced points than individual writes, some weren't coalesced
                    if (coalescedPoints < individualWrites.size()) {
                        LOG_INSERT_PATH(tsdb::http_log, debug, "[BATCH] Some individual writes couldn't be coalesced, processing {} individually", 
                                       individualWrites.size() - coalescedPoints);
                        
                        // For simplicity, we'll just process all individual writes that had < MIN_COALESCE_COUNT occurrences
                        // This is less efficient but ensures no data loss. In a production system, you might want
                        // to track exactly which writes were coalesced to avoid double-processing.
                        std::set<std::string> processedSeries;
                        for (const auto& mwp : coalescedWrites) {
                            for (const auto& [fieldName, fieldArray] : mwp.fields) {
                                std::string seriesKey = mwp.measurement;
                                for (const auto& [tagKey, tagValue] : mwp.tags) {
                                    seriesKey += "," + tagKey + "=" + tagValue;
                                }
                                seriesKey += " " + fieldName;
                                processedSeries.insert(seriesKey);
                            }
                        }
                        
                        // Process individual writes that weren't coalesced
                        for (auto& point : individualWrites) {
                            try {
                                auto json_result = glz::write_json(point);
                                if (!json_result) {
                                    continue;
                                }
                                WritePoint wp = parseWritePoint(json_result.value());
                                
                                bool alreadyProcessed = false;
                                for (const auto& [fieldName, fieldValue] : wp.fields) {
                                    std::string seriesKey = wp.measurement;
                                    for (const auto& [tagKey, tagValue] : wp.tags) {
                                        seriesKey += "," + tagKey + "=" + tagValue;
                                    }
                                    seriesKey += " " + fieldName;
                                    
                                    if (processedSeries.find(seriesKey) != processedSeries.end()) {
                                        alreadyProcessed = true;
                                        break;
                                    }
                                }
                                
                                if (!alreadyProcessed) {
                                    co_await processWritePoint(wp);
                                    pointsWritten += wp.fields.size();
                                    walWriteCount += wp.fields.size(); // Individual writes also count toward WAL writes
                                }
                            } catch (const std::exception& e) {
                                std::cerr << "Error processing individual write: " << e.what() << std::endl;
                            }
                        }
                    }
                    
                    auto walEndTime = std::chrono::steady_clock::now();
                    auto walDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - walStartTime);
                    auto batchEndTime = std::chrono::steady_clock::now();
                    auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(batchEndTime - batchStartTime);
                    
                    // Calculate actual WAL time excluding compression
                    auto actualWalTime = batchTiming.totalWalWriteTime;
                    auto compressionTime = batchTiming.totalCompressionTime;
                    
                    // Log detailed timing breakdown with compression time
                    tsdb::http_log.info("[WRITE_TIMING] Total: {}μs, Coalesce: {}μs, Compression: {}μs, WAL: {}μs, WAL_writes: {}, Points: {} (batch write)", 
                                       totalDuration.count(), coalesceDuration.count(), compressionTime.count(), actualWalTime.count(), batchTiming.totalWalWriteCount, pointsWritten);
                }
            } else {
                // Single write - check for arrays
                bool hasArrays = false;
                if (obj.contains("timestamps")) {
                    hasArrays = true;
                } else if (obj.contains("fields")) {
                    auto& fields = obj["fields"];
                    if (fields.is_object()) {
                        auto& fields_obj = fields.get<glz::json_t::object_t>();
                        for (auto& [name, value] : fields_obj) {
                            if (value.is_array()) {
                                hasArrays = true;
                                break;
                            }
                        }
                    }
                }
                
                if (hasArrays) {
                    auto walStartTime = std::chrono::steady_clock::now();
                    MultiWritePoint mwp = parseMultiWritePoint(doc);
                    std::string error;
                    if (!validateArraySizes(mwp, error)) {
                        throw std::runtime_error(error);
                    }
                    auto timing = co_await processMultiWritePoint(mwp);
                    auto walEndTime = std::chrono::steady_clock::now();
                    auto walDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - walStartTime);
                    pointsWritten = mwp.timestamps.size() * mwp.fields.size();  // Count each timestamp * each field
                    
                    // Log timing for single array write with compression details
                    auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - batchStartTime);
                    tsdb::http_log.info("[WRITE_TIMING] Total: {}μs, Compression: {}μs, WAL: {}μs, WAL_writes: {}, Points: {} (single array write)", 
                                       totalDuration.count(), timing.totalCompressionTime.count(), timing.totalWalWriteTime.count(), timing.totalWalWriteCount, pointsWritten);
                } else {
                    auto walStartTime = std::chrono::steady_clock::now();
                    WritePoint wp = parseWritePoint(body);
                    co_await processWritePoint(wp);
                    auto walEndTime = std::chrono::steady_clock::now();
                    auto walDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - walStartTime);
                    pointsWritten = wp.fields.size();  // Count each field as a point
                    
                    // Log timing for single individual write
                    auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - batchStartTime);
                    tsdb::http_log.info("[WRITE_TIMING] Total: {}μs, WAL: {}μs, WAL_writes: {}, Points: {} (single individual write)", 
                                       totalDuration.count(), walDuration.count(), wp.fields.size(), pointsWritten);
                }
            }
        }
        
        // Success response
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = createSuccessResponse(pointsWritten);
        rep->add_header("Content-Type", "application/json");
        
    } catch (const std::exception& e) {
        std::cerr << "Error handling write request: " << e.what() << std::endl;
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse(e.what());
        rep->add_header("Content-Type", "application/json");
    }
    
    co_return rep;
}

void HttpWriteHandler::registerRoutes(seastar::httpd::routes& r) {
    auto* handler = new seastar::httpd::function_handler(
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            return handleWrite(std::move(req));
        }, "json");
    
    r.add(seastar::httpd::operation_type::POST, 
          seastar::httpd::url("/write"), handler);
    
    tsdb::http_log.info("Registered HTTP write endpoint at /write");
}