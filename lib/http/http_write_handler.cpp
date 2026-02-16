#include "http_write_handler.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <chrono>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <numeric>
#include <algorithm>
#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/when_all.hh>

using namespace seastar;
using namespace httpd;

// Helper function to build series key for deduplication
// Format: measurement,tag1=val1,tag2=val2 field
static std::string buildSeriesKey(const std::string& measurement,
                                   const std::map<std::string, std::string>& tags,
                                   const std::string& field) {
    std::string key = measurement;
    for (const auto& [tagKey, tagValue] : tags) {
        key += "," + tagKey + "=" + tagValue;
    }
    key += " " + field;
    return key;
}

// IEEE 754 double can represent integers exactly up to 2^53.
// Beyond that, some integer values will silently lose precision
// when converted to double (e.g., 9007199254740993 -> 9007199254740992.0).
namespace {
    constexpr int64_t MAX_EXACT_DOUBLE_INT = 1LL << 53;  // 9007199254740992

    bool wouldLosePrecision(int64_t value) {
        return value > MAX_EXACT_DOUBLE_INT || value < -MAX_EXACT_DOUBLE_INT;
    }
}

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

std::string HttpWriteHandler::validateName(const std::string& name, const std::string& context) {
    if (name.empty()) {
        return context + " must not be empty";
    }
    // Reserved characters that corrupt key encoding:
    // \0 - used as separator in LevelDB tag value keys (encodeTagValuesKey)
    // ,  - used as tag separator in series keys (encodeSeriesKey)
    // =  - used as tag key=value separator in series keys
    // ' '- used as field separator in series keys
    for (char c : name) {
        if (c == '\0') return context + " must not contain null bytes";
        if (c == ',') return context + " must not contain commas";
        if (c == '=') return context + " must not contain equals signs";
        if (c == ' ') return context + " must not contain spaces";
    }
    return ""; // Valid
}

std::string HttpWriteHandler::validateTagValue(const std::string& value, const std::string& context) {
    if (value.empty()) {
        return context + " must not be empty";
    }
    // Same reserved characters as validateName, except spaces are allowed
    // in tag values since they don't participate in space-delimited key encoding.
    for (char c : value) {
        if (c == '\0') return context + " must not contain null bytes";
        if (c == ',') return context + " must not contain commas";
        if (c == '=') return context + " must not contain equals signs";
    }
    return ""; // Valid
}

void HttpWriteHandler::validateWritePointNames(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::map<std::string, FieldValue>& fields) {

    auto err = validateName(measurement, "Measurement name");
    if (!err.empty()) throw std::runtime_error(err);

    for (const auto& [key, value] : tags) {
        err = validateName(key, "Tag key '" + key + "'");
        if (!err.empty()) throw std::runtime_error(err);
        err = validateTagValue(value, "Tag value for '" + key + "'");
        if (!err.empty()) throw std::runtime_error(err);
    }

    for (const auto& [fieldName, fieldValue] : fields) {
        err = validateName(fieldName, "Field name '" + fieldName + "'");
        if (!err.empty()) throw std::runtime_error(err);
    }
}

void HttpWriteHandler::parseAndValidateWritePoint(const std::string& json) {
    GlazeWritePoint glazePoint;
    auto error = glz::read_json(glazePoint, json);
    if (error) {
        throw std::runtime_error("Failed to parse write point: " + std::string(glz::format_error(error)));
    }

    // Validate measurement name
    auto err = validateName(glazePoint.measurement, "Measurement name");
    if (!err.empty()) throw std::runtime_error(err);

    // Validate tags
    for (const auto& [key, value] : glazePoint.tags) {
        err = validateName(key, "Tag key '" + key + "'");
        if (!err.empty()) throw std::runtime_error(err);
        err = validateTagValue(value, "Tag value for '" + key + "'");
        if (!err.empty()) throw std::runtime_error(err);
    }

    // Validate field names
    if (glazePoint.fields.is_object()) {
        auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
        for (const auto& [fieldName, fieldValue] : fields_obj) {
            err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty()) throw std::runtime_error(err);
        }
    }
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const json_value_t& point) {
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

    // Validate measurement and tag names before any processing
    {
        auto err = validateName(mwp.measurement, "Measurement name");
        if (!err.empty()) throw std::runtime_error(err);
        for (const auto& [key, value] : mwp.tags) {
            err = validateName(key, "Tag key '" + key + "'");
            if (!err.empty()) throw std::runtime_error(err);
            err = validateTagValue(value, "Tag value for '" + key + "'");
            if (!err.empty()) throw std::runtime_error(err);
        }
    }

    // Parse fields - handle both scalars and arrays
    if (glazePoint.fields.is_object()) {
        auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
        
        for (auto& [fieldName, fieldValue] : fields_obj) {
            // Validate field name before processing
            {
                auto err = validateName(fieldName, "Field name '" + fieldName + "'");
                if (!err.empty()) throw std::runtime_error(err);
            }

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

std::vector<HttpWriteHandler::MultiWritePoint> HttpWriteHandler::coalesceWrites(const json_value_t::array_t& writes_array) {
    // Configuration constants
    static const size_t MAX_COALESCE_SIZE = 10000;  // Max values per field array
    static const size_t MIN_COALESCE_COUNT = 2;     // Min writes needed to coalesce
    
    auto start_coalesce = std::chrono::high_resolution_clock::now();
    
    // Fast direct parsing approach - avoid JSON string serialization
    std::unordered_map<std::string, CoalesceCandidate> candidates;
    size_t totalWritesProcessed = 0;
    
    LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Processing {} writes for coalescing", writes_array.size());

    // Parse writes directly from JSON objects for better performance
    for (const auto& write : writes_array) {
        totalWritesProcessed++;
        
        if (!write.is_object()) continue;
        
        try {
            auto& writeObj = write.get<json_value_t::object_t>();

            // Extract measurement
            auto measurementIt = writeObj.find("measurement");
            if (measurementIt == writeObj.end() || !measurementIt->second.is_string()) continue;
            std::string measurement = measurementIt->second.get<std::string>();

            // Validate measurement name
            {
                auto err = validateName(measurement, "Measurement name");
                if (!err.empty()) throw std::runtime_error(err);
            }

            // Extract timestamp - use as<uint64_t>() to preserve precision.
            // With json_value_t (generic_u64), integer timestamps are stored natively
            // as uint64_t, avoiding the ~512ns precision loss from double conversion.
            uint64_t timestamp = 0;
            auto timestampIt = writeObj.find("timestamp");
            if (timestampIt != writeObj.end() && timestampIt->second.is_number()) {
                timestamp = timestampIt->second.as<uint64_t>();
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
                auto& tagsObj = tagsIt->second.get<json_value_t::object_t>();
                for (const auto& [tagKey, tagValue] : tagsObj) {
                    if (tagValue.is_string()) {
                        tags[tagKey] = tagValue.get<std::string>();
                    }
                }
            }

            // Validate tag keys and values
            for (const auto& [tagKey, tagValue] : tags) {
                auto err = validateName(tagKey, "Tag key '" + tagKey + "'");
                if (!err.empty()) throw std::runtime_error(err);
                err = validateTagValue(tagValue, "Tag value for '" + tagKey + "'");
                if (!err.empty()) throw std::runtime_error(err);
            }

            // Extract fields and process each field
            auto fieldsIt = writeObj.find("fields");
            if (fieldsIt == writeObj.end() || !fieldsIt->second.is_object()) continue;
            
            auto& fieldsObj = fieldsIt->second.get<json_value_t::object_t>();
            for (const auto& [fieldName, fieldValue] : fieldsObj) {
                // Validate field name
                {
                    auto err = validateName(fieldName, "Field name '" + fieldName + "'");
                    if (!err.empty()) throw std::runtime_error(err);
                }

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
                    // Create new candidate in-place to avoid moves
                    CoalesceCandidate& candidate = candidates[seriesKey];
                    candidate.seriesKey = seriesKey;  // Copy, don't move
                    candidate.measurement = measurement;
                    candidate.tags = tags;
                    candidate.fieldName = fieldName;
                    candidate.valueType = valueType;

                    // Add the first value - explicitly copy strings from JSON
                    candidate.timestamps.push_back(timestamp);
                    if (valueType == TSMValueType::Float) {
                        candidate.doubleValues.push_back(fieldValue.as<double>());
                    } else if (valueType == TSMValueType::Boolean) {
                        candidate.boolValues.push_back(fieldValue.get<bool>() ? 1 : 0);
                    } else if (valueType == TSMValueType::String) {
                        // Explicitly copy string to avoid potential dangling reference
                        std::string strValue = fieldValue.get<std::string>();
                        candidate.stringValues.push_back(std::move(strValue));
                    }
                } else {
                    // Add to existing candidate
                    CoalesceCandidate& candidate = it->second;

                    if (candidate.valueType != valueType || candidate.timestamps.size() >= MAX_COALESCE_SIZE) {
                        // Type mismatch or size overflow: create a new candidate with a
                        // disambiguated key so the data is not silently dropped.
                        size_t suffix = 1;
                        std::string altKey;
                        do {
                            altKey = seriesKey + "#" + std::to_string(suffix++);
                        } while (candidates.count(altKey) > 0);

                        CoalesceCandidate& altCandidate = candidates[altKey];
                        altCandidate.seriesKey = altKey;
                        altCandidate.measurement = measurement;
                        altCandidate.tags = tags;
                        altCandidate.fieldName = fieldName;
                        altCandidate.valueType = valueType;
                        altCandidate.timestamps.push_back(timestamp);
                        if (valueType == TSMValueType::Float) {
                            altCandidate.doubleValues.push_back(fieldValue.as<double>());
                        } else if (valueType == TSMValueType::Boolean) {
                            altCandidate.boolValues.push_back(fieldValue.get<bool>() ? 1 : 0);
                        } else if (valueType == TSMValueType::String) {
                            std::string strValue = fieldValue.get<std::string>();
                            altCandidate.stringValues.push_back(std::move(strValue));
                        }
                    } else {
                        // Add value - explicitly copy strings from JSON
                        candidate.timestamps.push_back(timestamp);
                        if (valueType == TSMValueType::Float) {
                            candidate.doubleValues.push_back(fieldValue.as<double>());
                        } else if (valueType == TSMValueType::Boolean) {
                            candidate.boolValues.push_back(fieldValue.get<bool>() ? 1 : 0);
                        } else if (valueType == TSMValueType::String) {
                            // Explicitly copy string to avoid potential dangling reference
                            std::string strValue = fieldValue.get<std::string>();
                            candidate.stringValues.push_back(std::move(strValue));
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Failed to parse write: {}", e.what());
            continue;
        }
    }
    
    // Second pass: convert candidates to MultiWritePoint objects
    std::vector<MultiWritePoint> result;

    // Group by measurement+tags efficiently (only for coalescing)
    std::unordered_map<std::string, std::vector<std::string>> grouped; // groupKey -> seriesKeys
    std::unordered_set<std::string> processedSeriesKeys; // Track which candidates were coalesced
    size_t individualCount = 0;

    for (const auto& [seriesKey, candidate] : candidates) {
        if (candidate.timestamps.size() < MIN_COALESCE_COUNT) {
            individualCount++;
            // Will process these in a separate pass
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

        grouped[groupKey].push_back(seriesKey);
    }

    // IMPORTANT: Process coalesced groups FIRST before moving data from candidates
    // Build MultiWritePoint objects for coalesced groups
    for (auto& [groupKey, seriesKeys] : grouped) {
        if (seriesKeys.empty()) continue;

        // Get first candidate for validation
        const auto& firstSeriesKey = seriesKeys[0];
        auto& firstCandidate = candidates[firstSeriesKey];
        bool timestampsCompatible = true;

        // Create sorted version of first candidate's timestamps once
        std::vector<uint64_t> sortedFirst = firstCandidate.timestamps;
        std::sort(sortedFirst.begin(), sortedFirst.end());

        for (size_t i = 1; i < seriesKeys.size(); i++) {
            auto& candidate = candidates[seriesKeys[i]];
            if (candidate.timestamps.size() != firstCandidate.timestamps.size()) {
                timestampsCompatible = false;
                break;
            }
            // Check if timestamps match (allowing for different ordering)
            std::vector<uint64_t> sortedCurrent = candidate.timestamps;
            std::sort(sortedCurrent.begin(), sortedCurrent.end());

            if (sortedFirst != sortedCurrent) {
                timestampsCompatible = false;
                break;
            }
        }

        if (!timestampsCompatible) {
            LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Group '{}' has incompatible timestamps, emitting individual MultiWritePoints", groupKey);
            // Timestamps are incompatible across fields, so emit each candidate
            // as its own single-field MultiWritePoint to avoid data loss.
            for (const auto& seriesKey : seriesKeys) {
                auto& candidate = candidates[seriesKey];

                MultiWritePoint mwp;
                mwp.measurement = candidate.measurement;
                mwp.tags = candidate.tags;
                mwp.timestamps = std::move(candidate.timestamps);

                FieldArrays fa;
                fa.type = (candidate.valueType == TSMValueType::Float) ? FieldArrays::DOUBLE :
                         (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL :
                         FieldArrays::STRING;

                if (candidate.valueType == TSMValueType::Float) {
                    fa.doubles = std::move(candidate.doubleValues);
                } else if (candidate.valueType == TSMValueType::Boolean) {
                    fa.bools = std::move(candidate.boolValues);
                } else {
                    fa.strings = std::move(candidate.stringValues);
                }

                mwp.fields[candidate.fieldName] = std::move(fa);
                result.push_back(std::move(mwp));
                processedSeriesKeys.insert(seriesKey);
            }
            continue;
        }

        MultiWritePoint mwp;
        mwp.measurement = firstCandidate.measurement;
        mwp.tags = firstCandidate.tags;
        mwp.timestamps = std::move(firstCandidate.timestamps);

        // NOTE: Don't sort timestamps here because field arrays are in the same original order
        // Sorting would misalign timestamps with field values and cause memory corruption

        for (const auto& seriesKey : seriesKeys) {
            auto& candidate = candidates[seriesKey];

            FieldArrays fa;
            fa.type = (candidate.valueType == TSMValueType::Float) ? FieldArrays::DOUBLE :
                     (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL :
                     FieldArrays::STRING;

            // Simple assignment without complex sorting
            if (candidate.valueType == TSMValueType::Float) {
                fa.doubles = std::move(candidate.doubleValues);
            } else if (candidate.valueType == TSMValueType::Boolean) {
                fa.bools = std::move(candidate.boolValues);
            } else {
                fa.strings = std::move(candidate.stringValues);
            }

            mwp.fields[candidate.fieldName] = std::move(fa);
            processedSeriesKeys.insert(seriesKey); // Mark this series as processed
        }

        result.push_back(std::move(mwp));
    }

    // Finally, process individual writes (< MIN_COALESCE_COUNT) that weren't coalesced
    for (auto& [seriesKey, candidate] : candidates) {
        // Skip if already processed in coalesced groups or if it was a multi-count candidate
        if (processedSeriesKeys.count(seriesKey) > 0 || candidate.timestamps.size() >= MIN_COALESCE_COUNT) {
            continue;
        }

        MultiWritePoint mwp;
        mwp.measurement = candidate.measurement;
        mwp.tags = candidate.tags;
        mwp.timestamps = std::move(candidate.timestamps);

        FieldArrays fa;
        fa.type = (candidate.valueType == TSMValueType::Float) ? FieldArrays::DOUBLE :
                 (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL :
                 FieldArrays::STRING;

        if (candidate.valueType == TSMValueType::Float) {
            fa.doubles = std::move(candidate.doubleValues);
        } else if (candidate.valueType == TSMValueType::Boolean) {
            fa.bools = std::move(candidate.boolValues);
        } else {
            fa.strings = std::move(candidate.stringValues);
        }

        mwp.fields[candidate.fieldName] = std::move(fa);
        result.push_back(std::move(mwp));
    }

    auto end_coalesce = std::chrono::high_resolution_clock::now();
    auto coalesce_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_coalesce - start_coalesce);

    size_t coalescedCount = result.size() - individualCount;
    LOG_INSERT_PATH(tsdb::http_log, info,
        "[COALESCE] Processed {} writes -> {} coalesced arrays + {} individual writes = {} total ({}μs)",
        totalWritesProcessed, coalescedCount, individualCount, result.size(), coalesce_duration.count());

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

seastar::future<HttpWriteHandler::AggregatedTimingInfo> HttpWriteHandler::processMultiWritePoint(
    const MultiWritePoint& point,
    std::unordered_set<std::string>& seenMF,
    std::vector<MetaOp>& metaOps) {
    auto start_total = std::chrono::high_resolution_clock::now();

    // No artificial batch splitting -- the WAL enforces its own 16 MiB per-segment
    // limit (MAX_WAL_SIZE) and will signal rollover if a single insert exceeds it.
    // Splitting here only adds redundant copies and sequential cross-shard roundtrips.

    // Group inserts by shard to reduce cross-shard operations and process them in batches
    LOG_INSERT_PATH(tsdb::http_log, debug,
        "[WRITE] Processing MultiWritePoint: {} timestamps × {} fields",
        point.timestamps.size(), point.fields.size());

    auto start_grouping = std::chrono::high_resolution_clock::now();

    // NEW APPROACH: Keep arrays intact - create ONE TSDBInsert per field with ALL timestamps/values
    // Group by (shard, type) to batch properly
    std::map<size_t, std::vector<TSDBInsert<double>>> shardDoubleInserts;
    std::map<size_t, std::vector<TSDBInsert<bool>>> shardBoolInserts;
    std::map<size_t, std::vector<TSDBInsert<std::string>>> shardStringInserts;

    // Note: seenMF and metaOps are now passed by reference from caller for cross-batch deduplication

    // Process each field - create ONE insert with ALL timestamps and values
    for (const auto& [fieldName, fieldArray] : point.fields) {
        // Build the complete series key for sharding
        std::string seriesKey = point.measurement;
        for (const auto& [tagKey, tagValue] : point.tags) {
            seriesKey += "," + tagKey + "=" + tagValue;
        }
        seriesKey += " " + fieldName;

        // Determine target shard
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;

        // Create ONE TSDBInsert with ALL values for this field
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: {
                if (fieldArray.doubles.empty()) continue;

                TSDBInsert<double> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                insert.timestamps = point.timestamps;
                insert.values = fieldArray.doubles;

                shardDoubleInserts[shard].push_back(std::move(insert));

                // Track metadata - deduplicate by full series key (measurement+tags+field)
                std::string seriesKey = buildSeriesKey(point.measurement, point.tags, fieldName);
                if (seenMF.insert(seriesKey).second) {
                    metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, point.tags});
                }
                break;
            }

            case FieldArrays::BOOL: {
                if (fieldArray.bools.empty()) continue;

                TSDBInsert<bool> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                insert.timestamps = point.timestamps;
                // Convert uint8_t to bool
                insert.values.reserve(fieldArray.bools.size());
                for (uint8_t val : fieldArray.bools) {
                    insert.values.push_back(val != 0);
                }

                shardBoolInserts[shard].push_back(std::move(insert));

                // Track metadata - deduplicate by full series key (measurement+tags+field)
                std::string seriesKey = buildSeriesKey(point.measurement, point.tags, fieldName);
                if (seenMF.insert(seriesKey).second) {
                    metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, point.tags});
                }
                break;
            }

            case FieldArrays::STRING: {
                if (fieldArray.strings.empty()) continue;

                TSDBInsert<std::string> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                insert.timestamps = point.timestamps;
                insert.values = fieldArray.strings;

                shardStringInserts[shard].push_back(std::move(insert));

                // Track metadata - deduplicate by full series key (measurement+tags+field)
                std::string seriesKey = buildSeriesKey(point.measurement, point.tags, fieldName);
                if (seenMF.insert(seriesKey).second) {
                    metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, point.tags});
                }
                break;
            }

            case FieldArrays::INTEGER: {
                if (fieldArray.integers.empty()) continue;

                TSDBInsert<double> insert(point.measurement, fieldName);
                insert.tags = point.tags;
                insert.timestamps = point.timestamps;
                // Convert integers to doubles
                insert.values.reserve(fieldArray.integers.size());
                bool warnedPrecision = false;
                for (int64_t val : fieldArray.integers) {
                    if (!warnedPrecision && wouldLosePrecision(val)) {
                        tsdb::http_log.warn(
                            "Integer field '{}' contains values exceeding 2^53 that will lose precision when stored as double",
                            fieldName);
                        warnedPrecision = true;
                    }
                    insert.values.push_back(static_cast<double>(val));
                }

                shardDoubleInserts[shard].push_back(std::move(insert));

                // Track metadata - deduplicate by full series key (measurement+tags+field)
                std::string seriesKey = buildSeriesKey(point.measurement, point.tags, fieldName);
                if (seenMF.insert(seriesKey).second) {
                    metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, point.tags});
                }
                break;
            }
        }
    }
    
    auto end_grouping = std::chrono::high_resolution_clock::now();
    auto grouping_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_grouping - start_grouping);

    // Note: Metadata indexing now happens at batch level (after all points processed)
    // to ensure proper deduplication across the entire HTTP request

    auto start_batch_ops = std::chrono::high_resolution_clock::now();

    // Dispatch to all shards in parallel - inserts are already batched per field with all timestamps
    std::vector<seastar::future<AggregatedTimingInfo>> shardFutures;

    // Collect all unique shards that have work
    std::set<size_t> activeShards;
    for (const auto& [shard, _] : shardDoubleInserts) activeShards.insert(shard);
    for (const auto& [shard, _] : shardBoolInserts) activeShards.insert(shard);
    for (const auto& [shard, _] : shardStringInserts) activeShards.insert(shard);

    shardFutures.reserve(activeShards.size());

    for (size_t shard : activeShards) {
        // Extract this shard's inserts (using moves to avoid copying)
        std::vector<TSDBInsert<double>> doubles;
        std::vector<TSDBInsert<bool>> bools;
        std::vector<TSDBInsert<std::string>> strings;

        auto doubleIt = shardDoubleInserts.find(shard);
        if (doubleIt != shardDoubleInserts.end()) {
            doubles = std::move(doubleIt->second);
        }

        auto boolIt = shardBoolInserts.find(shard);
        if (boolIt != shardBoolInserts.end()) {
            bools = std::move(boolIt->second);
        }

        auto stringIt = shardStringInserts.find(shard);
        if (stringIt != shardStringInserts.end()) {
            strings = std::move(stringIt->second);
        }

        // Launch shard operation
        shardFutures.push_back(
            engineSharded->invoke_on(shard,
                [doubles = std::move(doubles), bools = std::move(bools), strings = std::move(strings)]
                (Engine& engine) mutable -> seastar::future<AggregatedTimingInfo> {
                    AggregatedTimingInfo batchTiming;

                    if (!doubles.empty()) {
                        auto walTiming = co_await engine.insertBatch(doubles);
                        batchTiming.aggregate(walTiming);
                    }

                    if (!bools.empty()) {
                        auto walTiming = co_await engine.insertBatch(bools);
                        batchTiming.aggregate(walTiming);
                    }

                    if (!strings.empty()) {
                        auto walTiming = co_await engine.insertBatch(strings);
                        batchTiming.aggregate(walTiming);
                    }

                    co_return batchTiming;
                })
        );
    }

    // Wait for all shard operations to complete in parallel
    auto shardTimings = co_await seastar::when_all_succeed(std::move(shardFutures));

    // Aggregate all timing results
    AggregatedTimingInfo aggregatedTiming;
    for (const auto& timing : shardTimings) {
        aggregatedTiming.aggregate(timing);
    }
    
    auto end_batch_ops = std::chrono::high_resolution_clock::now();
    auto batch_ops_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_batch_ops - start_batch_ops);
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);

    LOG_INSERT_PATH(tsdb::http_log, info,
        "[PERF] [HTTP] processMultiWritePoint breakdown - Total: {}μs, Grouping: {}μs, BatchOps: {}μs",
        total_duration.count(), grouping_duration.count(), batch_ops_duration.count());

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

    // Validate measurement and tag names before any processing
    {
        auto err = validateName(wp.measurement, "Measurement name");
        if (!err.empty()) throw std::runtime_error(err);
        for (const auto& [key, value] : wp.tags) {
            err = validateName(key, "Tag key '" + key + "'");
            if (!err.empty()) throw std::runtime_error(err);
            err = validateTagValue(value, "Tag value for '" + key + "'");
            if (!err.empty()) throw std::runtime_error(err);
        }
    }

    // Parse fields
    if (!glazePoint.fields.is_object()) {
        throw std::runtime_error("'fields' must be an object");
    }
    
    auto& fields_obj = glazePoint.fields.get<glz::json_t::object_t>();
    if (fields_obj.empty()) {
        throw std::runtime_error("'fields' object cannot be empty");
    }
    
    for (auto& [fieldName, fieldValue] : fields_obj) {
        // Validate field name before processing
        {
            auto err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty()) throw std::runtime_error(err);
        }

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
    auto start_total = std::chrono::high_resolution_clock::now();

    // Insert data before metadata for crash safety: if data insert succeeds but
    // metadata indexing fails, the data is still durable and discoverable on retry.
    // The reverse order (metadata first) would create phantom metadata entries
    // pointing to nonexistent data if the data insert fails.
    //
    // TODO: Each field does a sequential cross-shard call to insert followed by
    // indexMetadata. This could be batched like the multi-write path (collect all
    // MetaOps and dispatch once), but single-point writes are rare in practice.
    // Process each field in the point sequentially using a simple loop
    for (const auto& field_pair : point.fields) {
        auto start_field = std::chrono::high_resolution_clock::now();
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

            // Insert data first for crash safety (data before metadata)
            auto start_insert = std::chrono::high_resolution_clock::now();
            co_await engineSharded->invoke_on(shard, [insert](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });
            auto end_insert = std::chrono::high_resolution_clock::now();
            auto insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_insert - start_insert);

            // Then index metadata on shard 0
            auto start_metadata = std::chrono::high_resolution_clock::now();
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            auto end_metadata = std::chrono::high_resolution_clock::now();
            auto metadata_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_metadata - start_metadata);

            auto end_field = std::chrono::high_resolution_clock::now();
            auto field_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_field - start_field);
            LOG_INSERT_PATH(tsdb::http_log, info, "[INDIVIDUAL_WRITE_TIMING] Field: {}μs (insert: {}μs, metadata: {}μs)",
                field_duration.count(), insert_duration.count(), metadata_duration.count());
            
        } else if (std::holds_alternative<bool>(fieldValue)) {
            bool value = std::get<bool>(fieldValue);
            TSDBInsert<bool> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);

            // Insert data first for crash safety (data before metadata)
            co_await engineSharded->invoke_on(shard, [insert](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });

            // Then index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            
        } else if (std::holds_alternative<std::string>(fieldValue)) {
            const std::string& value = std::get<std::string>(fieldValue);
            TSDBInsert<std::string> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, value);

            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing single string insert - field: '{}', value: '{}', timestamp: {}, shard: {}",
                           fieldName, value, point.timestamp, shard);
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] String series key: '{}'", insert.seriesKey());

            // Insert data first for crash safety (data before metadata)
            co_await engineSharded->invoke_on(shard, [insert](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });

            // Then index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
            });
            
        } else if (std::holds_alternative<int64_t>(fieldValue)) {
            int64_t value = std::get<int64_t>(fieldValue);
            if (wouldLosePrecision(value)) {
                tsdb::http_log.warn(
                    "Integer field '{}' value {} exceeds 2^53 and will lose precision when stored as double",
                    fieldName, value);
            }
            // Convert integers to doubles for now
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.tags = point.tags;
            insert.addValue(point.timestamp, static_cast<double>(value));

            // Insert data first for crash safety (data before metadata)
            co_await engineSharded->invoke_on(shard, [insert](Engine& engine) mutable {
                return engine.insert(std::move(insert));
            });

            // Then index metadata on shard 0
            co_await engineSharded->invoke_on(0, [insert](Engine& engine) -> seastar::future<> {
                co_await engine.indexMetadata(insert);
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
        
        if (body.size() > MAX_WRITE_BODY_SIZE) {
            rep->set_status(seastar::http::reply::status_type::payload_too_large);
            rep->_content = createErrorResponse("Request body too large (max 64MB)");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }

        if (body.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Empty request body");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }
        
        LOG_INSERT_PATH(tsdb::http_log, debug, "Received write request: {} bytes", body.size());
        
        // Parse JSON using Glaze with u64 number mode to preserve integer precision.
        // This prevents nanosecond timestamps from losing precision through double conversion.
        json_value_t doc{};
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
            auto& obj = doc.get<json_value_t::object_t>();
            
            if (obj.contains("writes")) {
                // Batch write - coalesce individual writes into efficient array writes
                auto& writes = obj["writes"];
                if (writes.is_array()) {
                    auto& writes_array = writes.get<json_value_t::array_t>();
                    
                    LOG_INSERT_PATH(tsdb::http_log, info, "[BATCH] Processing batch with {} writes", writes_array.size());
                    
                    // First, separate already-array writes from individual writes
                    std::vector<MultiWritePoint> arrayWrites;
                    std::vector<json_value_t> individualWrites;
                    
                    for (auto& point : writes_array) {
                        try {
                            // Check if this point has arrays
                            bool hasArrays = false;
                            if (point.is_object()) {
                                auto& point_obj = point.get<json_value_t::object_t>();
                                if (point_obj.contains("timestamps")) {
                                    hasArrays = true;
                                } else if (point_obj.contains("fields")) {
                                    auto& fields = point_obj["fields"];
                                    if (fields.is_object()) {
                                        auto& fields_obj = fields.get<json_value_t::object_t>();
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
                                // Individual write - collect for coalescing.
                                // Move from writes_array since elements are not needed after this loop.
                                individualWrites.push_back(std::move(point));
                            }
                        } catch (const std::exception& e) {
                            tsdb::http_log.error("Error processing point: {}", e.what());
                            // Continue processing other points
                        }
                    }
                    
                    // Coalesce individual writes into array writes
                    auto coalesceStartTime = std::chrono::steady_clock::now();
                    std::vector<MultiWritePoint> coalescedWrites;
                    if (!individualWrites.empty()) {
                        json_value_t::array_t individualArray(
                            std::make_move_iterator(individualWrites.begin()),
                            std::make_move_iterator(individualWrites.end()));
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

                    // Create shared metadata tracking for entire batch (cross-request deduplication)
                    std::unordered_set<std::string> seenMF;
                    std::vector<MetaOp> metaOps;

                    for (auto& mwp : arrayWrites) {
                        try {
                            auto timing = co_await processMultiWritePoint(mwp, seenMF, metaOps);
                            batchTiming.aggregate(timing);
                            pointsWritten += mwp.timestamps.size() * mwp.fields.size();
                            walWriteCount += mwp.fields.size(); // One WAL write per field type
                        } catch (const std::exception& e) {
                            tsdb::http_log.error("Error processing array write: {}", e.what());
                        }
                    }

                    for (auto& mwp : coalescedWrites) {
                        try {
                            auto timing = co_await processMultiWritePoint(mwp, seenMF, metaOps);
                            batchTiming.aggregate(timing);
                            pointsWritten += mwp.timestamps.size() * mwp.fields.size();
                            walWriteCount += mwp.fields.size(); // One WAL write per field type
                        } catch (const std::exception& e) {
                            tsdb::http_log.error("Error processing coalesced write: {}", e.what());
                        }
                    }

                    // Process metadata once for entire batch using batched LevelDB write
                    auto start_metadata = std::chrono::high_resolution_clock::now();
                    size_t metaOpsCount = metaOps.size(); // Save count before move
                    if (!metaOps.empty()) {
                        co_await engineSharded->invoke_on(0, [metaOps = std::move(metaOps)](Engine& engine) -> seastar::future<> {
                            co_await engine.indexMetadataBatch(metaOps);
                        });
                    }
                    auto end_metadata = std::chrono::high_resolution_clock::now();
                    auto metadata_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_metadata - start_metadata);

                    LOG_INSERT_PATH(tsdb::http_log, info,
                        "[METADATA] Batch indexing: {} unique series indexed in {}μs",
                        metaOpsCount, metadata_duration.count());

                    // Note: All individual writes are now included in coalescedWrites as single-point arrays
                    // No fallback processing needed

                    // auto walEndTime = std::chrono::steady_clock::now();
                    // auto walDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - walStartTime);
                    // auto batchEndTime = std::chrono::steady_clock::now();
                    // auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(batchEndTime - batchStartTime);
                    //
                    // // Calculate actual WAL time excluding compression
                    // auto actualWalTime = batchTiming.totalWalWriteTime;
                    // auto compressionTime = batchTiming.totalCompressionTime;
                    //
                    // // Log detailed timing breakdown with compression time
                    // tsdb::http_log.info("[WRITE_TIMING] Total: {}μs, Coalesce: {}μs, Compression: {}μs, WAL: {}μs, WAL_writes: {}, Points: {} (batch write)",
                    //                    totalDuration.count(), coalesceDuration.count(), compressionTime.count(), actualWalTime.count(), batchTiming.totalWalWriteCount, pointsWritten);
                }
            } else {
                // Single write - check for arrays
                bool hasArrays = false;
                if (obj.contains("timestamps")) {
                    hasArrays = true;
                } else if (obj.contains("fields")) {
                    auto& fields = obj["fields"];
                    if (fields.is_object()) {
                        auto& fields_obj = fields.get<json_value_t::object_t>();
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

                    // Create metadata tracking for this single write
                    std::unordered_set<std::string> seenMF;
                    std::vector<MetaOp> metaOps;

                    auto timing = co_await processMultiWritePoint(mwp, seenMF, metaOps);

                    // Process metadata for this write using batched LevelDB write
                    auto start_metadata = std::chrono::high_resolution_clock::now();
                    size_t metaOpsCount = metaOps.size(); // Save count before move
                    if (!metaOps.empty()) {
                        co_await engineSharded->invoke_on(0, [metaOps = std::move(metaOps)](Engine& engine) -> seastar::future<> {
                            co_await engine.indexMetadataBatch(metaOps);
                        });
                    }
                    auto end_metadata = std::chrono::high_resolution_clock::now();
                    auto metadata_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_metadata - start_metadata);

                    LOG_INSERT_PATH(tsdb::http_log, info,
                        "[METADATA] Single write indexing: {} unique series indexed in {}μs",
                        metaOpsCount, metadata_duration.count());

                    // auto walEndTime = std::chrono::steady_clock::now();
                    // auto walDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - walStartTime);
                    pointsWritten = mwp.timestamps.size() * mwp.fields.size();  // Count each timestamp * each field

                    // // Log timing for single array write with compression details
                    // auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - batchStartTime);
                    // tsdb::http_log.info("[WRITE_TIMING] Total: {}μs, Compression: {}μs, WAL: {}μs, WAL_writes: {}, Points: {} (single array write)",
                    //                    totalDuration.count(), timing.totalCompressionTime.count(), timing.totalWalWriteTime.count(), timing.totalWalWriteCount, pointsWritten);
                } else {
                    // auto walStartTime = std::chrono::steady_clock::now();
                    WritePoint wp = parseWritePoint(body);
                    co_await processWritePoint(wp);
                    // auto walEndTime = std::chrono::steady_clock::now();
                    // auto walDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - walStartTime);
                    pointsWritten = wp.fields.size();  // Count each field as a point

                    // // Log timing for single individual write
                    // auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(walEndTime - batchStartTime);
                    // tsdb::http_log.info("[WRITE_TIMING] Total: {}μs, WAL: {}μs, WAL_writes: {}, Points: {} (single individual write)",
                    //                    totalDuration.count(), walDuration.count(), wp.fields.size(), pointsWritten);
                }
            }
        }
        
        // Success response
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = createSuccessResponse(pointsWritten);
        rep->add_header("Content-Type", "application/json");
        
    } catch (const std::exception& e) {
        tsdb::http_log.error("Error handling write request: {}", e.what());
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