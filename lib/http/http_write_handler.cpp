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

    // Extract fields directly from the json_value_t object, avoiding a
    // serialize-then-reparse round-trip that was previously done via
    // glz::write_json + glz::read_json(GlazeMultiWritePoint, ...).

    if (!point.is_object()) {
        throw std::runtime_error("Write point must be a JSON object");
    }
    auto& obj = point.get<json_value_t::object_t>();

    // Extract measurement
    auto measurementIt = obj.find("measurement");
    if (measurementIt == obj.end() || !measurementIt->second.is_string()) {
        throw std::runtime_error("Missing or invalid 'measurement' field");
    }
    mwp.measurement = measurementIt->second.get<std::string>();

    // Extract tags
    auto tagsIt = obj.find("tags");
    if (tagsIt != obj.end() && tagsIt->second.is_object()) {
        auto& tagsObj = tagsIt->second.get<json_value_t::object_t>();
        for (const auto& [tagKey, tagValue] : tagsObj) {
            if (tagValue.is_string()) {
                mwp.tags[tagKey] = tagValue.get<std::string>();
            }
        }
    }

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
    auto fieldsIt = obj.find("fields");
    if (fieldsIt == obj.end() || !fieldsIt->second.is_object()) {
        throw std::runtime_error("Fields must be an object");
    }
    auto& fields_obj = fieldsIt->second.get<json_value_t::object_t>();

    for (auto& [fieldName, fieldValue] : fields_obj) {
        // Validate field name before processing
        {
            auto err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty()) throw std::runtime_error(err);
        }

        FieldArrays fa;

        if (fieldValue.is_array()) {
            auto& arr = fieldValue.get<json_value_t::array_t>();
            if (arr.empty()) {
                throw std::runtime_error("Field array cannot be empty: " + fieldName);
            }

            // Determine type from first element
            if (arr[0].is_number()) {
                fa.type = FieldArrays::DOUBLE;
                for (auto& elem : arr) {
                    if (elem.is_number()) {
                        fa.doubles.push_back(elem.as<double>());
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
                fa.doubles.push_back(fieldValue.as<double>());
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

    // Parse timestamps - extract directly from json_value_t
    auto timestampsArrIt = obj.find("timestamps");
    auto singleTimestampIt = obj.find("timestamp");

    if (timestampsArrIt != obj.end()) {
        // "timestamps" can be a single number or an array of numbers
        if (timestampsArrIt->second.is_number()) {
            mwp.timestamps.push_back(timestampsArrIt->second.as<uint64_t>());
        } else if (timestampsArrIt->second.is_array()) {
            auto& tsArr = timestampsArrIt->second.get<json_value_t::array_t>();
            mwp.timestamps.reserve(tsArr.size());
            for (auto& elem : tsArr) {
                if (elem.is_number()) {
                    mwp.timestamps.push_back(elem.as<uint64_t>());
                }
            }
        }
    } else if (singleTimestampIt != obj.end() && singleTimestampIt->second.is_number()) {
        // Single timestamp - determine how many we need from fields
        uint64_t ts = singleTimestampIt->second.as<uint64_t>();
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
        mwp.timestamps.resize(numPoints, ts);
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

#if TSDB_LOG_INSERT_PATH
    auto start_coalesce = std::chrono::high_resolution_clock::now();
#endif

    // Fast direct parsing approach - avoid JSON string serialization.
    // Handles both scalar and array fields in a single pass, eliminating
    // the need for callers to pre-scan fields for array detection.
    std::unordered_map<std::string, CoalesceCandidate> candidates;
    [[maybe_unused]] size_t totalWritesProcessed = 0;

    LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Processing {} writes for coalescing", writes_array.size());

    // Helper lambda: add a single scalar value to a CoalesceCandidate.
    // Returns false if the value type is unsupported, true otherwise.
    auto addScalarToCandidate = [](CoalesceCandidate& candidate, TSMValueType valueType,
                                   uint64_t ts, const json_value_t& val) -> bool {
        candidate.timestamps.push_back(ts);
        if (valueType == TSMValueType::Float) {
            candidate.doubleValues.push_back(val.as<double>());
        } else if (valueType == TSMValueType::Boolean) {
            candidate.boolValues.push_back(val.get<bool>() ? 1 : 0);
        } else if (valueType == TSMValueType::String) {
            std::string strValue = val.get<std::string>();
            candidate.stringValues.push_back(std::move(strValue));
        } else {
            return false;
        }
        return true;
    };

    // Helper lambda: initialize a new CoalesceCandidate with metadata.
    auto initCandidate = [](CoalesceCandidate& candidate, const std::string& seriesKey,
                            const std::string& measurement, const std::map<std::string, std::string>& tags,
                            const std::string& fieldName, TSMValueType valueType) {
        candidate.seriesKey = seriesKey;
        candidate.measurement = measurement;
        candidate.tags = tags;
        candidate.fieldName = fieldName;
        candidate.valueType = valueType;
    };

    // Helper lambda: find or create a candidate, handling type mismatch and overflow
    // by creating disambiguated keys. Returns a reference to the target candidate.
    auto findOrCreateCandidate = [&](const std::string& seriesKey, const std::string& measurement,
                                     const std::map<std::string, std::string>& tags,
                                     const std::string& fieldName, TSMValueType valueType,
                                     size_t numValuesToAdd) -> CoalesceCandidate& {
        auto it = candidates.find(seriesKey);
        if (it == candidates.end()) {
            CoalesceCandidate& c = candidates[seriesKey];
            initCandidate(c, seriesKey, measurement, tags, fieldName, valueType);
            return c;
        }
        CoalesceCandidate& existing = it->second;
        if (existing.valueType == valueType &&
            existing.timestamps.size() + numValuesToAdd <= MAX_COALESCE_SIZE) {
            return existing;
        }
        // Type mismatch or size overflow: create a disambiguated candidate
        size_t suffix = 1;
        std::string altKey;
        do {
            altKey = seriesKey + "#" + std::to_string(suffix++);
        } while (candidates.count(altKey) > 0);
        CoalesceCandidate& c = candidates[altKey];
        initCandidate(c, altKey, measurement, tags, fieldName, valueType);
        return c;
    };

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

            // Extract timestamps - handle both "timestamp" (single) and "timestamps" (array).
            // For scalar fields, we use timestamps[0]. For array fields, timestamps must
            // match the array length. This unified extraction handles all write formats
            // in a single pass without requiring callers to pre-detect array vs scalar.
            std::vector<uint64_t> timestamps;
            auto timestampsArrIt = writeObj.find("timestamps");
            auto singleTimestampIt = writeObj.find("timestamp");

            if (timestampsArrIt != writeObj.end()) {
                if (timestampsArrIt->second.is_array()) {
                    auto& tsArr = timestampsArrIt->second.get<json_value_t::array_t>();
                    timestamps.reserve(tsArr.size());
                    for (auto& elem : tsArr) {
                        if (elem.is_number()) {
                            timestamps.push_back(elem.as<uint64_t>());
                        }
                    }
                } else if (timestampsArrIt->second.is_number()) {
                    timestamps.push_back(timestampsArrIt->second.as<uint64_t>());
                }
            } else if (singleTimestampIt != writeObj.end() && singleTimestampIt->second.is_number()) {
                timestamps.push_back(singleTimestampIt->second.as<uint64_t>());
            } else {
                // Generate timestamp
                auto now = std::chrono::system_clock::now();
                auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
                timestamps.push_back(static_cast<uint64_t>(nanos));
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

            // Pre-build measurement+tags prefix for series key construction (once per write point)
            std::string seriesKeyPrefix;
            seriesKeyPrefix.reserve(measurement.length() + 64);
            seriesKeyPrefix = measurement;
            for (const auto& [tagKey, tagValue] : tags) {
                seriesKeyPrefix += ",";
                seriesKeyPrefix += tagKey;
                seriesKeyPrefix += "=";
                seriesKeyPrefix += tagValue;
            }

            // Extract fields and process each field - handles both scalar and array values
            auto fieldsIt = writeObj.find("fields");
            if (fieldsIt == writeObj.end() || !fieldsIt->second.is_object()) continue;

            auto& fieldsObj = fieldsIt->second.get<json_value_t::object_t>();
            for (const auto& [fieldName, fieldValue] : fieldsObj) {
                // Validate field name
                {
                    auto err = validateName(fieldName, "Field name '" + fieldName + "'");
                    if (!err.empty()) throw std::runtime_error(err);
                }

                // Build series key
                std::string seriesKey;
                seriesKey.reserve(seriesKeyPrefix.length() + fieldName.length() + 1);
                seriesKey = seriesKeyPrefix;
                seriesKey += " ";
                seriesKey += fieldName;

                if (fieldValue.is_array()) {
                    // Array field - expand all elements into the candidate
                    auto& arr = fieldValue.get<json_value_t::array_t>();
                    if (arr.empty()) {
                        throw std::runtime_error("Field array cannot be empty: " + fieldName);
                    }

                    // Determine type from first element
                    TSMValueType valueType;
                    if (arr[0].is_number()) {
                        valueType = TSMValueType::Float;
                    } else if (arr[0].is_boolean()) {
                        valueType = TSMValueType::Boolean;
                    } else if (arr[0].is_string()) {
                        valueType = TSMValueType::String;
                    } else {
                        throw std::runtime_error("Unsupported field array type: " + fieldName);
                    }

                    // Resolve timestamps for this array: use the timestamps array if it
                    // matches, or replicate a single timestamp, or generate sequential ones
                    std::vector<uint64_t> fieldTimestamps;
                    if (timestamps.size() == arr.size()) {
                        fieldTimestamps = timestamps;
                    } else if (timestamps.size() == 1) {
                        // Single timestamp with array fields - replicate for each element
                        fieldTimestamps.resize(arr.size(), timestamps[0]);
                    } else {
                        // Generate timestamps 1ms apart from the first available
                        uint64_t baseTs = timestamps.empty() ? static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::system_clock::now().time_since_epoch()).count())
                            : timestamps[0];
                        fieldTimestamps.reserve(arr.size());
                        for (size_t i = 0; i < arr.size(); i++) {
                            fieldTimestamps.push_back(baseTs + i * 1000000);
                        }
                    }

                    CoalesceCandidate& candidate = findOrCreateCandidate(
                        seriesKey, measurement, tags, fieldName, valueType, arr.size());

                    // Add all array elements to candidate
                    candidate.timestamps.reserve(candidate.timestamps.size() + arr.size());
                    for (size_t i = 0; i < arr.size(); i++) {
                        auto& elem = arr[i];
                        candidate.timestamps.push_back(fieldTimestamps[i]);
                        if (valueType == TSMValueType::Float) {
                            if (!elem.is_number()) throw std::runtime_error("Mixed types in field array: " + fieldName);
                            candidate.doubleValues.push_back(elem.as<double>());
                        } else if (valueType == TSMValueType::Boolean) {
                            if (!elem.is_boolean()) throw std::runtime_error("Mixed types in field array: " + fieldName);
                            candidate.boolValues.push_back(elem.get<bool>() ? 1 : 0);
                        } else if (valueType == TSMValueType::String) {
                            if (!elem.is_string()) throw std::runtime_error("Mixed types in field array: " + fieldName);
                            std::string strValue = elem.get<std::string>();
                            candidate.stringValues.push_back(std::move(strValue));
                        }
                    }
                } else {
                    // Scalar field - determine type and add single value
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

                    CoalesceCandidate& candidate = findOrCreateCandidate(
                        seriesKey, measurement, tags, fieldName, valueType, 1);
                    addScalarToCandidate(candidate, valueType, timestamps[0], fieldValue);
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
    [[maybe_unused]] size_t individualCount = 0;

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

#if TSDB_LOG_INSERT_PATH
    size_t coalescedCount = result.size() - individualCount;
    auto end_coalesce = std::chrono::high_resolution_clock::now();
    auto coalesce_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_coalesce - start_coalesce);
    LOG_INSERT_PATH(tsdb::http_log, info,
        "[COALESCE] Processed {} writes -> {} coalesced arrays + {} individual writes = {} total ({}μs)",
        totalWritesProcessed, coalescedCount, individualCount, result.size(), coalesce_duration.count());
#endif

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
    MultiWritePoint& point,
    std::unordered_set<SeriesId128, SeriesId128::Hash>& seenMF,
    std::vector<MetaOp>& metaOps) {
#if TSDB_LOG_INSERT_PATH
    auto start_total = std::chrono::high_resolution_clock::now();
#endif

    // No artificial batch splitting -- the WAL enforces its own 16 MiB per-segment
    // limit (MAX_WAL_SIZE) and will signal rollover if a single insert exceeds it.
    // Splitting here only adds redundant copies and sequential cross-shard roundtrips.

    // Group inserts by shard to reduce cross-shard operations and process them in batches
    LOG_INSERT_PATH(tsdb::http_log, debug,
        "[WRITE] Processing MultiWritePoint: {} timestamps × {} fields",
        point.timestamps.size(), point.fields.size());

#if TSDB_LOG_INSERT_PATH
    auto start_grouping = std::chrono::high_resolution_clock::now();
#endif

    // NEW APPROACH: Keep arrays intact - create ONE TSDBInsert per field with ALL timestamps/values
    // Group by (shard, type) to batch properly
    // Use pre-sized vectors instead of std::map since shard IDs are small integers [0, smp::count)
    const size_t shardCount = seastar::smp::count;
    std::vector<std::vector<TSDBInsert<double>>> shardDoubleInserts(shardCount);
    std::vector<std::vector<TSDBInsert<bool>>> shardBoolInserts(shardCount);
    std::vector<std::vector<TSDBInsert<std::string>>> shardStringInserts(shardCount);

    // Note: seenMF and metaOps are now passed by reference from caller for cross-batch deduplication

    // Process each field - create ONE insert with ALL timestamps and values.
    // Use shared_ptr for tags and timestamps so that all field inserts from
    // the same multi-field point share a single allocation instead of making
    // N-1 copies (for N fields). This is safe across Seastar shard boundaries
    // because shared_ptr uses atomic refcounting.
    auto sharedTags = std::make_shared<const std::map<std::string, std::string>>(std::move(point.tags));
    auto sharedTimestamps = std::make_shared<const std::vector<uint64_t>>(std::move(point.timestamps));

    // Pre-build the measurement+tags prefix once for series key construction
    std::string seriesKeyPrefix = point.measurement;
    for (const auto& [tagKey, tagValue] : *sharedTags) {
        seriesKeyPrefix += "," + tagKey + "=" + tagValue;
    }

    for (const auto& [fieldName, fieldArray] : point.fields) {
        // Build the complete series key for sharding
        std::string seriesKey = seriesKeyPrefix + " " + fieldName;

        // Compute SeriesId128 ONCE per field for shard routing.
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;

        // Create ONE TSDBInsert with ALL values for this field
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: {
                if (fieldArray.doubles.empty()) continue;

                // Track metadata before potential move - deduplicate by SeriesId128 (16-byte key,
                // fast hash via first 8 bytes) instead of the raw 60-100 byte series key string.
                {
                    if (seenMF.insert(seriesId).second) {
                        metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});
                    }
                }

                TSDBInsert<double> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
                insert.values = fieldArray.doubles;

                shardDoubleInserts[shard].push_back(std::move(insert));
                break;
            }

            case FieldArrays::BOOL: {
                if (fieldArray.bools.empty()) continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, *sharedTags});
                    }
                }

                TSDBInsert<bool> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
                // Convert uint8_t to bool
                insert.values.reserve(fieldArray.bools.size());
                for (uint8_t val : fieldArray.bools) {
                    insert.values.push_back(val != 0);
                }

                shardBoolInserts[shard].push_back(std::move(insert));
                break;
            }

            case FieldArrays::STRING: {
                if (fieldArray.strings.empty()) continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, *sharedTags});
                    }
                }

                TSDBInsert<std::string> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
                insert.values = fieldArray.strings;

                shardStringInserts[shard].push_back(std::move(insert));
                break;
            }

            case FieldArrays::INTEGER: {
                if (fieldArray.integers.empty()) continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});
                    }
                }

                TSDBInsert<double> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
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
                break;
            }
        }
    }
    
#if TSDB_LOG_INSERT_PATH
    auto end_grouping = std::chrono::high_resolution_clock::now();
    auto grouping_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_grouping - start_grouping);
#endif

    // Note: Metadata indexing now happens at batch level (after all points processed)
    // to ensure proper deduplication across the entire HTTP request

#if TSDB_LOG_INSERT_PATH
    auto start_batch_ops = std::chrono::high_resolution_clock::now();
#endif

    // Dispatch to all shards in parallel - inserts are already batched per field with all timestamps
    std::vector<seastar::future<AggregatedTimingInfo>> shardFutures;
    shardFutures.reserve(shardCount);

    for (size_t shard = 0; shard < shardCount; ++shard) {
        // Skip shards with no work
        if (shardDoubleInserts[shard].empty() &&
            shardBoolInserts[shard].empty() &&
            shardStringInserts[shard].empty()) {
            continue;
        }

        // Move this shard's inserts directly from the pre-sized vectors
        auto doubles = std::move(shardDoubleInserts[shard]);
        auto bools = std::move(shardBoolInserts[shard]);
        auto strings = std::move(shardStringInserts[shard]);

        // Launch shard operation
        shardFutures.push_back(
            engineSharded->invoke_on(shard,
                [doubles = std::move(doubles), bools = std::move(bools), strings = std::move(strings)]
                (Engine& engine) mutable -> seastar::future<AggregatedTimingInfo> {
                    AggregatedTimingInfo batchTiming;

                    if (!doubles.empty()) {
                        auto walTiming = co_await engine.insertBatch(std::move(doubles));
                        batchTiming.aggregate(walTiming);
                    }

                    if (!bools.empty()) {
                        auto walTiming = co_await engine.insertBatch(std::move(bools));
                        batchTiming.aggregate(walTiming);
                    }

                    if (!strings.empty()) {
                        auto walTiming = co_await engine.insertBatch(std::move(strings));
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
    
#if TSDB_LOG_INSERT_PATH
    auto end_batch_ops = std::chrono::high_resolution_clock::now();
    auto batch_ops_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_batch_ops - start_batch_ops);
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);

    LOG_INSERT_PATH(tsdb::http_log, info,
        "[PERF] [HTTP] processMultiWritePoint breakdown - Total: {}μs, Grouping: {}μs, BatchOps: {}μs",
        total_duration.count(), grouping_duration.count(), batch_ops_duration.count());
#endif

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

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const json_value_t& doc) {
    WritePoint wp;

    // Extract fields directly from the already-parsed json_value_t, avoiding
    // a redundant re-parse of the raw JSON string.

    if (!doc.is_object()) {
        throw std::runtime_error("Write point must be a JSON object");
    }
    auto& obj = doc.get<json_value_t::object_t>();

    // Extract measurement
    auto measurementIt = obj.find("measurement");
    if (measurementIt == obj.end() || !measurementIt->second.is_string()) {
        throw std::runtime_error("Missing or invalid 'measurement' field");
    }
    wp.measurement = measurementIt->second.get<std::string>();

    // Extract tags
    auto tagsIt = obj.find("tags");
    if (tagsIt != obj.end() && tagsIt->second.is_object()) {
        auto& tagsObj = tagsIt->second.get<json_value_t::object_t>();
        for (const auto& [tagKey, tagValue] : tagsObj) {
            if (tagValue.is_string()) {
                wp.tags[tagKey] = tagValue.get<std::string>();
            }
        }
    }

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
    auto fieldsIt = obj.find("fields");
    if (fieldsIt == obj.end() || !fieldsIt->second.is_object()) {
        throw std::runtime_error("'fields' must be an object");
    }

    auto& fields_obj = fieldsIt->second.get<json_value_t::object_t>();
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
            wp.fields[fieldName] = fieldValue.as<double>();
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
    auto timestampIt = obj.find("timestamp");
    if (timestampIt != obj.end() && timestampIt->second.is_number()) {
        wp.timestamp = timestampIt->second.as<uint64_t>();
    } else {
        auto now = std::chrono::system_clock::now();
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        wp.timestamp = static_cast<uint64_t>(nanos);
    }

    return wp;
}

seastar::future<> HttpWriteHandler::processWritePoint(const WritePoint& point) {
#if TSDB_LOG_INSERT_PATH
    auto start_total = std::chrono::high_resolution_clock::now();
#endif

    // Insert data before metadata for crash safety: if data insert succeeds but
    // metadata indexing fails, the data is still durable and discoverable on retry.
    // The reverse order (metadata first) would create phantom metadata entries
    // pointing to nonexistent data if the data insert fails.

    // Group inserts by shard to dispatch all fields in parallel rather than
    // sequentially. For a point with N fields, this reduces up to 2*N sequential
    // cross-shard round trips down to one parallel fan-out + one metadata dispatch.
    std::map<size_t, std::vector<TSDBInsert<double>>> shardDoubleInserts;
    std::map<size_t, std::vector<TSDBInsert<bool>>> shardBoolInserts;
    std::map<size_t, std::vector<TSDBInsert<std::string>>> shardStringInserts;
    std::vector<MetaOp> metaOps;

    // Share tags across all field inserts to avoid N copies for N fields.
    // For single-point writes this is a minor win, but for multi-field points
    // it eliminates all tag copies (only shared_ptr refcount increments).
    auto sharedTags = std::make_shared<const std::map<std::string, std::string>>(point.tags);

    // Pre-build the measurement+tags prefix once for series key construction
    std::string seriesKeyPrefix = point.measurement;
    for (const auto& [tagKey, tagValue] : *sharedTags) {
        seriesKeyPrefix += "," + tagKey + "=" + tagValue;
    }

    for (const auto& [fieldName, fieldValue] : point.fields) {
        // Build the complete series key for sharding
        std::string seriesKey = seriesKeyPrefix + " " + fieldName;

        // Compute SeriesId128 ONCE per field for shard routing.
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;

        // Handle each variant type explicitly, accumulating into per-shard vectors
        if (std::holds_alternative<double>(fieldValue)) {
            double value = std::get<double>(fieldValue);
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.addValue(point.timestamp, value);
            shardDoubleInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<bool>(fieldValue)) {
            bool value = std::get<bool>(fieldValue);
            TSDBInsert<bool> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.addValue(point.timestamp, value);
            shardBoolInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<std::string>(fieldValue)) {
            const std::string& value = std::get<std::string>(fieldValue);
            TSDBInsert<std::string> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.addValue(point.timestamp, value);

            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing single string insert - field: '{}', value: '{}', timestamp: {}, shard: {}",
                           fieldName, value, point.timestamp, shard);
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] String series key: '{}'", insert.seriesKey());

            shardStringInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<int64_t>(fieldValue)) {
            int64_t value = std::get<int64_t>(fieldValue);
            if (wouldLosePrecision(value)) {
                tsdb::http_log.warn(
                    "Integer field '{}' value {} exceeds 2^53 and will lose precision when stored as double",
                    fieldName, value);
            }
            // Convert integers to doubles for now
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.addValue(point.timestamp, static_cast<double>(value));
            shardDoubleInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});
        }
    }

    // Dispatch all shards in parallel using insertBatch for efficiency
    std::set<size_t> activeShards;
    for (const auto& [shard, _] : shardDoubleInserts) activeShards.insert(shard);
    for (const auto& [shard, _] : shardBoolInserts) activeShards.insert(shard);
    for (const auto& [shard, _] : shardStringInserts) activeShards.insert(shard);

    std::vector<seastar::future<>> shardFutures;
    shardFutures.reserve(activeShards.size());

    for (size_t shard : activeShards) {
        std::vector<TSDBInsert<double>> doubles;
        std::vector<TSDBInsert<bool>> bools;
        std::vector<TSDBInsert<std::string>> strings;

        if (auto it = shardDoubleInserts.find(shard); it != shardDoubleInserts.end()) {
            doubles = std::move(it->second);
        }
        if (auto it = shardBoolInserts.find(shard); it != shardBoolInserts.end()) {
            bools = std::move(it->second);
        }
        if (auto it = shardStringInserts.find(shard); it != shardStringInserts.end()) {
            strings = std::move(it->second);
        }

        shardFutures.push_back(
            engineSharded->invoke_on(shard,
                [doubles = std::move(doubles), bools = std::move(bools), strings = std::move(strings)]
                (Engine& engine) mutable -> seastar::future<> {
                    // Use insertBatch when multiple inserts target the same shard,
                    // otherwise fall back to single insert for less overhead.
                    // skipMetadataIndexing=true because we index metadata separately below.
                    if (!doubles.empty()) {
                        if (doubles.size() == 1) {
                            co_await engine.insert(std::move(doubles[0]), true);
                        } else {
                            co_await engine.insertBatch(std::move(doubles));
                        }
                    }
                    if (!bools.empty()) {
                        if (bools.size() == 1) {
                            co_await engine.insert(std::move(bools[0]), true);
                        } else {
                            co_await engine.insertBatch(std::move(bools));
                        }
                    }
                    if (!strings.empty()) {
                        if (strings.size() == 1) {
                            co_await engine.insert(std::move(strings[0]), true);
                        } else {
                            co_await engine.insertBatch(std::move(strings));
                        }
                    }
                })
        );
    }

    // Wait for all shard inserts to complete in parallel
    co_await seastar::when_all_succeed(std::move(shardFutures));

    // Dispatch metadata indexing to shard 0 once for all fields
    if (!metaOps.empty()) {
        co_await engineSharded->invoke_on(0, [metaOps = std::move(metaOps)](Engine& engine) -> seastar::future<> {
            co_await engine.indexMetadataBatch(metaOps);
        });
    }

#if TSDB_LOG_INSERT_PATH
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);
    LOG_INSERT_PATH(tsdb::http_log, info,
        "[PERF] [HTTP] processWritePoint: {}us ({} fields, {} shards)",
        total_duration.count(), point.fields.size(), activeShards.size());
#endif

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
#if TSDB_LOG_INSERT_PATH
        auto batchStartTime = std::chrono::steady_clock::now();
#endif
        
        // Check if it's a batch write or single write
        if (doc.is_object()) {
            auto& obj = doc.get<json_value_t::object_t>();
            
            if (obj.contains("writes")) {
                // Batch write - coalesce ALL writes (both scalar and array) in a
                // single pass through coalesceWrites. No array-detection pre-scan
                // is needed because coalesceWrites now handles both scalar and array
                // field values, eliminating the redundant field iteration that
                // previously occurred when array writes were detected and then
                // re-parsed by parseMultiWritePoint.
                auto& writes = obj["writes"];
                if (writes.is_array()) {
                    auto& writes_array = writes.get<json_value_t::array_t>();

                    LOG_INSERT_PATH(tsdb::http_log, info, "[BATCH] Processing batch with {} writes", writes_array.size());

                    // Coalesce all writes (scalar and array) in a single pass
#if TSDB_LOG_INSERT_PATH
                    auto coalesceStartTime = std::chrono::steady_clock::now();
#endif
                    auto coalescedWrites = coalesceWrites(writes_array);
#if TSDB_LOG_INSERT_PATH
                    auto coalesceEndTime = std::chrono::steady_clock::now();
                    auto coalesceDuration = std::chrono::duration_cast<std::chrono::microseconds>(coalesceEndTime - coalesceStartTime);

                    LOG_INSERT_PATH(tsdb::http_log, info, "[BATCH] Coalesced {} writes into {} MultiWritePoints ({}μs)",
                                   writes_array.size(), coalescedWrites.size(), coalesceDuration.count());
#endif

                    // Process all coalesced writes in parallel
                    [[maybe_unused]] AggregatedTimingInfo batchTiming;

                    // Create shared metadata tracking for entire batch (cross-request deduplication)
                    // Safe to share across concurrent coroutines: seenMF/metaOps are only written
                    // during the synchronous grouping phase of processMultiWritePoint (before any co_await),
                    // and Seastar coroutines are cooperatively scheduled on a single thread.
                    // Use SeriesId128 (16-byte key, O(1) hash via first 8 bytes) instead of
                    // string keys (60-100 bytes with expensive hashing).
                    std::unordered_set<SeriesId128, SeriesId128::Hash> seenMF;
                    std::vector<MetaOp> metaOps;

                    // Compute point/field counts upfront since processMultiWritePoint may move from the MWPs
                    for (const auto& mwp : coalescedWrites) {
                        pointsWritten += mwp.timestamps.size() * mwp.fields.size();
                    }

                    // Validate array sizes before processing
                    for (const auto& mwp : coalescedWrites) {
                        std::string error;
                        if (!validateArraySizes(mwp, error)) {
                            throw std::runtime_error(error);
                        }
                    }

                    // Launch all MWP processing concurrently and await them together
                    std::vector<seastar::future<AggregatedTimingInfo>> mwpFutures;
                    mwpFutures.reserve(coalescedWrites.size());

                    for (auto& mwp : coalescedWrites) {
                        mwpFutures.push_back(processMultiWritePoint(mwp, seenMF, metaOps));
                    }

                    // Await all MWP futures in parallel
                    auto mwpResults = co_await seastar::when_all(mwpFutures.begin(), mwpFutures.end());
                    for (auto& result : mwpResults) {
                        try {
                            batchTiming.aggregate(result.get());
                        } catch (const std::exception& e) {
                            tsdb::http_log.error("Error processing write: {}", e.what());
                        }
                    }

                    // Process metadata once for entire batch using batched LevelDB write
#if TSDB_LOG_INSERT_PATH
                    auto start_metadata = std::chrono::high_resolution_clock::now();
                    size_t metaOpsCount = metaOps.size(); // Save count before move
#endif
                    if (!metaOps.empty()) {
                        co_await engineSharded->invoke_on(0, [metaOps = std::move(metaOps)](Engine& engine) -> seastar::future<> {
                            co_await engine.indexMetadataBatch(metaOps);
                        });
                    }
#if TSDB_LOG_INSERT_PATH
                    auto end_metadata = std::chrono::high_resolution_clock::now();
                    auto metadata_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_metadata - start_metadata);

                    LOG_INSERT_PATH(tsdb::http_log, info,
                        "[METADATA] Batch indexing: {} unique series indexed in {}μs",
                        metaOpsCount, metadata_duration.count());
#endif
                }
            } else {
                // Single write - parseMultiWritePoint handles both scalar and
                // array fields in a single pass (scalars are wrapped as size-1
                // arrays), so no detection pre-pass is needed.
                MultiWritePoint mwp = parseMultiWritePoint(doc);
                std::string error;
                if (!validateArraySizes(mwp, error)) {
                    throw std::runtime_error(error);
                }

                // Compute points count before processMultiWritePoint, which may move from mwp
                pointsWritten = mwp.timestamps.size() * mwp.fields.size();

                // Create metadata tracking for this single write.
                // Use SeriesId128 (16-byte key) instead of string keys for faster dedup.
                std::unordered_set<SeriesId128, SeriesId128::Hash> seenMF;
                std::vector<MetaOp> metaOps;

                [[maybe_unused]] auto timing = co_await processMultiWritePoint(mwp, seenMF, metaOps);

                // Process metadata for this write using batched LevelDB write
#if TSDB_LOG_INSERT_PATH
                auto start_metadata = std::chrono::high_resolution_clock::now();
                size_t metaOpsCount = metaOps.size(); // Save count before move
#endif
                if (!metaOps.empty()) {
                    co_await engineSharded->invoke_on(0, [metaOps = std::move(metaOps)](Engine& engine) -> seastar::future<> {
                        co_await engine.indexMetadataBatch(metaOps);
                    });
                }
#if TSDB_LOG_INSERT_PATH
                auto end_metadata = std::chrono::high_resolution_clock::now();
                auto metadata_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_metadata - start_metadata);

                LOG_INSERT_PATH(tsdb::http_log, info,
                    "[METADATA] Single write indexing: {} unique series indexed in {}μs",
                    metaOpsCount, metadata_duration.count());
#endif
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