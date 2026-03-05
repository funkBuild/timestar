#include "http_write_handler.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <chrono>
#include <unordered_set>
#include <numeric>
#include <algorithm>
#include <tsl/robin_map.h>
#include <tsl/robin_set.h>
#include <boost/iterator/counting_iterator.hpp>
#include <seastar/core/when_all.hh>

using namespace seastar;
using namespace httpd;

// Helper function to build series key for deduplication
// Format: measurement,tag1=val1,tag2=val2 field
static std::string buildSeriesKey(const std::string& measurement,
                                   const std::map<std::string, std::string>& tags,
                                   const std::string& field) {
    size_t totalSize = measurement.size() + 1 + field.size(); // +1 for space
    for (const auto& [tagKey, tagValue] : tags) {
        totalSize += 1 + tagKey.size() + 1 + tagValue.size(); // ,key=value
    }
    std::string key;
    key.reserve(totalSize);
    key = measurement;
    for (const auto& [tagKey, tagValue] : tags) {
        key += ',';
        key += tagKey;
        key += '=';
        key += tagValue;
    }
    key += ' ';
    key += field;
    return key;
}

// Glaze-compatible structures for JSON parsing
struct GlazeWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    json_value_t fields;  // Use u64 mode for integer precision
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
    if (!engineSharded) {
        throw std::invalid_argument("engineSharded must not be null");
    }
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
        auto& fields_obj = glazePoint.fields.get<json_value_t::object_t>();
        for (const auto& [fieldName, fieldValue] : fields_obj) {
            err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty()) throw std::runtime_error(err);
        }
    }
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const json_value_t& point, uint64_t defaultTimestampNs) {
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
                if (arr[0].holds<int64_t>() || arr[0].holds<uint64_t>()) {
                    fa.type = FieldArrays::INTEGER;
                    for (auto& elem : arr) {
                        if (elem.is_number()) {
                            fa.integers.push_back(elem.as<int64_t>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
                    }
                } else {
                    fa.type = FieldArrays::DOUBLE;
                    for (auto& elem : arr) {
                        if (elem.is_number()) {
                            fa.doubles.push_back(elem.as<double>());
                        } else {
                            throw std::runtime_error("Mixed types in field array: " + fieldName);
                        }
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
                if (fieldValue.holds<int64_t>() || fieldValue.holds<uint64_t>()) {
                    fa.type = FieldArrays::INTEGER;
                    fa.integers.push_back(fieldValue.as<int64_t>());
                } else {
                    fa.type = FieldArrays::DOUBLE;
                    fa.doubles.push_back(fieldValue.as<double>());
                }
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
        // No timestamp - use caller-supplied default (captured once per batch)
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

        // Generate timestamps 1ms apart from the pre-computed default
        for (size_t i = 0; i < numPoints; i++) {
            mwp.timestamps.push_back(defaultTimestampNs + i * 1000000);
        }
    }

    return mwp;
}

std::vector<HttpWriteHandler::MultiWritePoint> HttpWriteHandler::coalesceWrites(const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs) {
    // Configuration constants
    static const size_t MAX_COALESCE_SIZE = 10000;  // Max values per field array
    static const size_t MIN_COALESCE_COUNT = 2;     // Min writes needed to coalesce

#if TSDB_LOG_INSERT_PATH
    auto start_coalesce = std::chrono::high_resolution_clock::now();
#endif

    // Fast direct parsing approach - avoid JSON string serialization.
    // Handles both scalar and array fields in a single pass, eliminating
    // the need for callers to pre-scan fields for array detection.
    // Use robin_map for better cache locality on the hot lookup path --
    // open-addressing with Robin Hood probing keeps entries in a flat array,
    // avoiding per-bucket linked-list pointer chasing of std::unordered_map.
    tsl::robin_map<std::string, CoalesceCandidate> candidates;
    // Pre-allocate for the expected number of unique series keys.
    // A typical write point has 1-3 fields, so estimate writes * 2.
    candidates.reserve(writes_array.size() * 2);
    [[maybe_unused]] size_t totalWritesProcessed = 0;

    LOG_INSERT_PATH(tsdb::http_log, debug, "[COALESCE] Processing {} writes for coalescing", writes_array.size());

    // Helper lambda: add a single scalar value to a CoalesceCandidate.
    // Returns false if the value type is unsupported, true otherwise.
    auto addScalarToCandidate = [](CoalesceCandidate& candidate, TSMValueType valueType,
                                   uint64_t ts, const json_value_t& val) -> bool {
        candidate.timestamps.push_back(ts);
        candidate.timestampHashSum += ts;
        candidate.timestampHashXor ^= ts;
        if (valueType == TSMValueType::Float) {
            candidate.doubleValues.push_back(val.as<double>());
        } else if (valueType == TSMValueType::Boolean) {
            candidate.boolValues.push_back(val.get<bool>() ? 1 : 0);
        } else if (valueType == TSMValueType::String) {
            std::string strValue = val.get<std::string>();
            candidate.stringValues.push_back(std::move(strValue));
        } else if (valueType == TSMValueType::Integer) {
            candidate.integerValues.push_back(val.as<int64_t>());
        } else {
            return false;
        }
        return true;
    };

    // Helper lambda: initialize a new CoalesceCandidate with metadata.
    // Takes a lw_shared_ptr to the tag map so that all candidates from the
    // same write point share a single allocation (O(1) pointer copy).
    auto initCandidate = [](CoalesceCandidate& candidate, const std::string& seriesKey,
                            const std::string& measurement,
                            seastar::lw_shared_ptr<const std::map<std::string, std::string>> tags,
                            const std::string& fieldName, TSMValueType valueType) {
        candidate.seriesKey = seriesKey;
        candidate.measurement = measurement;
        candidate.sharedTags = std::move(tags);
        candidate.fieldName = fieldName;
        candidate.valueType = valueType;
    };

    // Helper lambda: find or create a candidate, handling type mismatch and overflow
    // by creating disambiguated keys. Returns a reference to the target candidate.
    // Takes lw_shared_ptr<const map> by value so the shared tag map is propagated
    // without copying the underlying map data.
    auto findOrCreateCandidate = [&](const std::string& seriesKey, const std::string& measurement,
                                     seastar::lw_shared_ptr<const std::map<std::string, std::string>> tags,
                                     const std::string& fieldName, TSMValueType valueType,
                                     size_t numValuesToAdd) -> CoalesceCandidate& {
        auto it = candidates.find(seriesKey);
        if (it == candidates.end()) {
            CoalesceCandidate& c = candidates[seriesKey];
            initCandidate(c, seriesKey, measurement, std::move(tags), fieldName, valueType);
            return c;
        }
        CoalesceCandidate& existing = it.value();
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
        initCandidate(c, altKey, measurement, std::move(tags), fieldName, valueType);
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
                // No timestamp provided - use the batch-level default
                timestamps.push_back(defaultTimestampNs);
            }

            // Extract tags into a local map, validate, then wrap in a
            // lw_shared_ptr so all CoalesceCandidates from this write point
            // share a single allocation (pointer copy instead of map copy).
            std::map<std::string, std::string> localTags;
            auto tagsIt = writeObj.find("tags");
            if (tagsIt != writeObj.end() && tagsIt->second.is_object()) {
                auto& tagsObj = tagsIt->second.get<json_value_t::object_t>();
                for (const auto& [tagKey, tagValue] : tagsObj) {
                    if (tagValue.is_string()) {
                        localTags[tagKey] = tagValue.get<std::string>();
                    }
                }
            }

            // Validate tag keys and values
            for (const auto& [tagKey, tagValue] : localTags) {
                auto err = validateName(tagKey, "Tag key '" + tagKey + "'");
                if (!err.empty()) throw std::runtime_error(err);
                err = validateTagValue(tagValue, "Tag value for '" + tagKey + "'");
                if (!err.empty()) throw std::runtime_error(err);
            }

            // Pre-build measurement+tags prefix for series key construction (once per write point)
            std::string seriesKeyPrefix;
            seriesKeyPrefix.reserve(measurement.length() + 64);
            seriesKeyPrefix = measurement;
            for (const auto& [tagKey, tagValue] : localTags) {
                seriesKeyPrefix += ",";
                seriesKeyPrefix += tagKey;
                seriesKeyPrefix += "=";
                seriesKeyPrefix += tagValue;
            }

            // Create the shared tag map once per write point. All fields from
            // this write point will share this single allocation via lw_shared_ptr.
            auto sharedTags = seastar::make_lw_shared<const std::map<std::string, std::string>>(std::move(localTags));

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
                        // With generic_u64 parsing, integer JSON values (no decimal point)
                        // are stored as int64_t/uint64_t, while floats are stored as double.
                        if (arr[0].holds<int64_t>() || arr[0].holds<uint64_t>()) {
                            valueType = TSMValueType::Integer;
                        } else {
                            valueType = TSMValueType::Float;
                        }
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
                        // Generate timestamps 1ms apart from the first available,
                        // falling back to the batch-level default if none exist
                        uint64_t baseTs = timestamps.empty() ? defaultTimestampNs
                            : timestamps[0];
                        fieldTimestamps.reserve(arr.size());
                        for (size_t i = 0; i < arr.size(); i++) {
                            fieldTimestamps.push_back(baseTs + i * 1000000);
                        }
                    }

                    CoalesceCandidate& candidate = findOrCreateCandidate(
                        seriesKey, measurement, sharedTags, fieldName, valueType, arr.size());

                    // Add all array elements to candidate
                    candidate.timestamps.reserve(candidate.timestamps.size() + arr.size());
                    for (size_t i = 0; i < arr.size(); i++) {
                        auto& elem = arr[i];
                        candidate.timestamps.push_back(fieldTimestamps[i]);
                        candidate.timestampHashSum += fieldTimestamps[i];
                        candidate.timestampHashXor ^= fieldTimestamps[i];
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
                        } else if (valueType == TSMValueType::Integer) {
                            if (!elem.is_number()) throw std::runtime_error("Mixed types in field array: " + fieldName);
                            candidate.integerValues.push_back(elem.as<int64_t>());
                        }
                    }
                } else {
                    // Scalar field - determine type and add single value
                    TSMValueType valueType;
                    if (fieldValue.is_number()) {
                        if (fieldValue.holds<int64_t>() || fieldValue.holds<uint64_t>()) {
                            valueType = TSMValueType::Integer;
                        } else {
                            valueType = TSMValueType::Float;
                        }
                    } else if (fieldValue.is_boolean()) {
                        valueType = TSMValueType::Boolean;
                    } else if (fieldValue.is_string()) {
                        valueType = TSMValueType::String;
                    } else {
                        continue; // Skip unsupported types
                    }

                    CoalesceCandidate& candidate = findOrCreateCandidate(
                        seriesKey, measurement, sharedTags, fieldName, valueType, 1);
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
    tsl::robin_map<std::string, std::vector<std::string>> grouped; // groupKey -> seriesKeys
    tsl::robin_set<std::string> processedSeriesKeys; // Track which candidates were coalesced
    [[maybe_unused]] size_t individualCount = 0;

    for (const auto& [seriesKey, candidate] : candidates) {
        if (candidate.timestamps.size() < MIN_COALESCE_COUNT) {
            individualCount++;
            // Will process these in a separate pass
            continue;
        }

        // Build group key (measurement + tags, without field)
        std::string groupKey = candidate.measurement;
        for (const auto& [tagKey, tagValue] : *candidate.sharedTags) {
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

        // Compare timestamps using pre-computed commutative hashes (O(1) per field).
        // Two multisets with the same count, same sum, and same XOR are identical
        // for real nanosecond timestamps (collision probability is vanishingly small).
        for (size_t i = 1; i < seriesKeys.size(); i++) {
            auto& candidate = candidates[seriesKeys[i]];
            if (candidate.timestamps.size() != firstCandidate.timestamps.size() ||
                candidate.timestampHashSum != firstCandidate.timestampHashSum ||
                candidate.timestampHashXor != firstCandidate.timestampHashXor) {
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
                mwp.tags = *candidate.sharedTags;
                mwp.timestamps = std::move(candidate.timestamps);

                FieldArrays fa;
                fa.type = (candidate.valueType == TSMValueType::Float) ? FieldArrays::DOUBLE :
                         (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL :
                         (candidate.valueType == TSMValueType::Integer) ? FieldArrays::INTEGER :
                         FieldArrays::STRING;

                if (candidate.valueType == TSMValueType::Float) {
                    fa.doubles = std::move(candidate.doubleValues);
                } else if (candidate.valueType == TSMValueType::Boolean) {
                    fa.bools = std::move(candidate.boolValues);
                } else if (candidate.valueType == TSMValueType::Integer) {
                    fa.integers = std::move(candidate.integerValues);
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
        mwp.tags = *firstCandidate.sharedTags;
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

    // Finally, process individual writes (< MIN_COALESCE_COUNT) that weren't coalesced.
    // Use iterator-based loop with .value() to get mutable access for std::move(),
    // since robin_map's range-for yields const references to the stored pairs.
    for (auto it = candidates.begin(); it != candidates.end(); ++it) {
        const auto& seriesKey = it->first;
        auto& candidate = it.value();
        // Skip if already processed in coalesced groups or if it was a multi-count candidate
        if (processedSeriesKeys.count(seriesKey) > 0 || candidate.timestamps.size() >= MIN_COALESCE_COUNT) {
            continue;
        }

        MultiWritePoint mwp;
        mwp.measurement = candidate.measurement;
        mwp.tags = *candidate.sharedTags;
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
    std::vector<std::vector<TSDBInsert<int64_t>>> shardIntegerInserts(shardCount);

    // Note: seenMF and metaOps are now passed by reference from caller for cross-batch deduplication

    // Process each field - create ONE insert with ALL timestamps and values.
    // Use shared_ptr for tags and timestamps so that all field inserts from
    // the same multi-field point share a single allocation instead of making
    // N-1 copies (for N fields). This is safe across Seastar shard boundaries
    // because shared_ptr uses atomic refcounting.
    auto sharedTags = std::make_shared<const std::map<std::string, std::string>>(std::move(point.tags));
    auto sharedTimestamps = std::make_shared<const std::vector<uint64_t>>(std::move(point.timestamps));

    // Pre-build the measurement+tags prefix once for series key construction
    std::string seriesKeyPrefix;
    {
        size_t prefixSize = point.measurement.size();
        for (const auto& [tagKey, tagValue] : *sharedTags) {
            prefixSize += 1 + tagKey.size() + 1 + tagValue.size();
        }
        seriesKeyPrefix.reserve(prefixSize);
        seriesKeyPrefix = point.measurement;
        for (const auto& [tagKey, tagValue] : *sharedTags) {
            seriesKeyPrefix += ',';
            seriesKeyPrefix += tagKey;
            seriesKeyPrefix += '=';
            seriesKeyPrefix += tagValue;
        }
    }

    for (auto& [fieldName, fieldArray] : point.fields) {
        // Build the complete series key for sharding (no temporaries)
        std::string seriesKey;
        seriesKey.reserve(seriesKeyPrefix.size() + 1 + fieldName.size());
        seriesKey = seriesKeyPrefix;
        seriesKey += ' ';
        seriesKey += fieldName;

        // Compute SeriesId128 ONCE per field for shard routing.
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;

        // Create ONE TSDBInsert with ALL values for this field.
        // Move values from the MWP's FieldArrays directly into the TSDBInsert
        // to avoid copying large value vectors (e.g. 10K doubles = 80KB per field).
        // Each field is processed exactly once, so the move-from is safe.
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
                insert.setCachedSeriesKey(std::move(seriesKey));
                insert.setCachedSeriesId128(seriesId);
                insert.values = std::move(fieldArray.doubles);

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
                insert.setCachedSeriesKey(std::move(seriesKey));
                insert.setCachedSeriesId128(seriesId);
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
                insert.setCachedSeriesKey(std::move(seriesKey));
                insert.setCachedSeriesId128(seriesId);
                insert.values = std::move(fieldArray.strings);

                shardStringInserts[shard].push_back(std::move(insert));
                break;
            }

            case FieldArrays::INTEGER: {
                if (fieldArray.integers.empty()) continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        metaOps.push_back(MetaOp{TSMValueType::Integer, point.measurement, fieldName, *sharedTags});
                    }
                }

                TSDBInsert<int64_t> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
                insert.setCachedSeriesKey(std::move(seriesKey));
                insert.setCachedSeriesId128(seriesId);
                insert.values = std::move(fieldArray.integers);

                shardIntegerInserts[shard].push_back(std::move(insert));
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
            shardStringInserts[shard].empty() &&
            shardIntegerInserts[shard].empty()) {
            continue;
        }

        // Move this shard's inserts directly from the pre-sized vectors
        auto doubles = std::move(shardDoubleInserts[shard]);
        auto bools = std::move(shardBoolInserts[shard]);
        auto strings = std::move(shardStringInserts[shard]);
        auto integers = std::move(shardIntegerInserts[shard]);

        // Launch shard operation
        shardFutures.push_back(
            engineSharded->invoke_on(shard,
                [doubles = std::move(doubles), bools = std::move(bools),
                 strings = std::move(strings), integers = std::move(integers)]
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

                    if (!integers.empty()) {
                        auto walTiming = co_await engine.insertBatch(std::move(integers));
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

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const std::string& json, uint64_t defaultTimestampNs) {
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
    
    auto& fields_obj = glazePoint.fields.get<json_value_t::object_t>();
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
            if (fieldValue.holds<int64_t>() || fieldValue.holds<uint64_t>()) {
                wp.fields[fieldName] = fieldValue.as<int64_t>();
            } else {
                wp.fields[fieldName] = fieldValue.get<double>();
            }
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

    // Parse timestamp, falling back to the caller-supplied default
    if (glazePoint.timestamp) {
        wp.timestamp = *glazePoint.timestamp;
    } else {
        wp.timestamp = defaultTimestampNs;
    }

    return wp;
}

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const json_value_t& doc, uint64_t defaultTimestampNs) {
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
            if (fieldValue.holds<int64_t>() || fieldValue.holds<uint64_t>()) {
                wp.fields[fieldName] = fieldValue.as<int64_t>();
            } else {
                wp.fields[fieldName] = fieldValue.as<double>();
            }
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

    // Parse timestamp, falling back to the caller-supplied default
    auto timestampIt = obj.find("timestamp");
    if (timestampIt != obj.end() && timestampIt->second.is_number()) {
        wp.timestamp = timestampIt->second.as<uint64_t>();
    } else {
        wp.timestamp = defaultTimestampNs;
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
    // Use pre-sized vectors indexed by shard ID for O(1) access and cache locality,
    // since shard IDs are dense integers in [0, smp::count).
    const size_t shardCount = seastar::smp::count;
    std::vector<std::vector<TSDBInsert<double>>> shardDoubleInserts(shardCount);
    std::vector<std::vector<TSDBInsert<bool>>> shardBoolInserts(shardCount);
    std::vector<std::vector<TSDBInsert<std::string>>> shardStringInserts(shardCount);
    std::vector<std::vector<TSDBInsert<int64_t>>> shardIntegerInserts(shardCount);
    std::vector<MetaOp> metaOps;

    // Share tags across all field inserts to avoid N copies for N fields.
    // For single-point writes this is a minor win, but for multi-field points
    // it eliminates all tag copies (only shared_ptr refcount increments).
    auto sharedTags = std::make_shared<const std::map<std::string, std::string>>(point.tags);

    // Pre-build the measurement+tags prefix once for series key construction
    std::string seriesKeyPrefix;
    {
        size_t prefixSize = point.measurement.size();
        for (const auto& [tagKey, tagValue] : *sharedTags) {
            prefixSize += 1 + tagKey.size() + 1 + tagValue.size();
        }
        seriesKeyPrefix.reserve(prefixSize);
        seriesKeyPrefix = point.measurement;
        for (const auto& [tagKey, tagValue] : *sharedTags) {
            seriesKeyPrefix += ',';
            seriesKeyPrefix += tagKey;
            seriesKeyPrefix += '=';
            seriesKeyPrefix += tagValue;
        }
    }

    for (const auto& [fieldName, fieldValue] : point.fields) {
        // Build the complete series key for sharding (no temporaries)
        std::string seriesKey;
        seriesKey.reserve(seriesKeyPrefix.size() + 1 + fieldName.size());
        seriesKey = seriesKeyPrefix;
        seriesKey += ' ';
        seriesKey += fieldName;

        // Compute SeriesId128 ONCE per field for shard routing.
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        size_t shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;

        // Handle each variant type explicitly, accumulating into per-shard vectors
        if (std::holds_alternative<double>(fieldValue)) {
            double value = std::get<double>(fieldValue);
            TSDBInsert<double> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);
            shardDoubleInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<bool>(fieldValue)) {
            bool value = std::get<bool>(fieldValue);
            TSDBInsert<bool> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);
            shardBoolInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<std::string>(fieldValue)) {
            const std::string& value = std::get<std::string>(fieldValue);
            TSDBInsert<std::string> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);

            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] Processing single string insert - field: '{}', value: '{}', timestamp: {}, shard: {}",
                           fieldName, value, point.timestamp, shard);
            LOG_INSERT_PATH(tsdb::http_log, debug, "[WRITE] String series key: '{}'", insert.seriesKey());

            shardStringInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<int64_t>(fieldValue)) {
            int64_t value = std::get<int64_t>(fieldValue);
            TSDBInsert<int64_t> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);
            shardIntegerInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Integer, point.measurement, fieldName, *sharedTags});
        }
    }

    // Dispatch all shards in parallel using insertBatch for efficiency.
    // Iterate over the pre-sized vectors directly, skipping empty shards.
    std::vector<seastar::future<>> shardFutures;
    shardFutures.reserve(shardCount);

    for (size_t shard = 0; shard < shardCount; ++shard) {
        // Skip shards with no work - O(1) emptiness check on each vector
        if (shardDoubleInserts[shard].empty() &&
            shardBoolInserts[shard].empty() &&
            shardStringInserts[shard].empty() &&
            shardIntegerInserts[shard].empty()) [[likely]] {
            continue;
        }

        // Move this shard's inserts directly from the pre-sized vectors
        auto doubles = std::move(shardDoubleInserts[shard]);
        auto bools = std::move(shardBoolInserts[shard]);
        auto strings = std::move(shardStringInserts[shard]);
        auto integers = std::move(shardIntegerInserts[shard]);

        shardFutures.push_back(
            engineSharded->invoke_on(shard,
                [doubles = std::move(doubles), bools = std::move(bools),
                 strings = std::move(strings), integers = std::move(integers)]
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
                    if (!integers.empty()) {
                        if (integers.size() == 1) {
                            co_await engine.insert(std::move(integers[0]), true);
                        } else {
                            co_await engine.insertBatch(std::move(integers));
                        }
                    }
                })
        );
    }

    // Wait for all shard inserts to complete in parallel
    co_await seastar::when_all_succeed(std::move(shardFutures));

    // Fire-and-forget metadata indexing: dispatch to shard 0 without blocking
    // the HTTP response. Data is already durable in WAL; metadata is only needed
    // for query discovery and will be retried on failure.
    if (!metaOps.empty()) {
        engineSharded->local().dispatchMetadataAsync(std::move(metaOps));
    }

#if TSDB_LOG_INSERT_PATH
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);
    LOG_INSERT_PATH(tsdb::http_log, info,
        "[PERF] [HTTP] processWritePoint: {}us ({} fields, {} shards)",
        total_duration.count(), point.fields.size(), shardFutures.size());
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
        
        if (body.size() > maxWriteBodySize()) {
            rep->set_status(seastar::http::reply::status_type::payload_too_large);
            rep->_content = createErrorResponse("Request body too large (max " + std::to_string(maxWriteBodySize() / (1024*1024)) + "MB)");
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

        // Capture a single wall-clock timestamp for the entire request.
        // All write points that lack an explicit timestamp will share this value,
        // eliminating up to N redundant now() calls in a batch of N points.
        const uint64_t defaultTimestampNs = currentNanosTimestamp();

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
                    auto coalescedWrites = coalesceWrites(writes_array, defaultTimestampNs);
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

                    // Fire-and-forget metadata indexing for entire batch.
                    // Data is already durable in WAL; metadata is dispatched to shard 0
                    // as a background task with retry on failure.
#if TSDB_LOG_INSERT_PATH
                    size_t metaOpsCount = metaOps.size();
#endif
                    if (!metaOps.empty()) {
                        engineSharded->local().dispatchMetadataAsync(std::move(metaOps));
                    }
#if TSDB_LOG_INSERT_PATH
                    LOG_INSERT_PATH(tsdb::http_log, info,
                        "[METADATA] Batch: dispatched {} unique series for async indexing",
                        metaOpsCount);
#endif
                }
            } else {
                // Single write - parseMultiWritePoint handles both scalar and
                // array fields in a single pass (scalars are wrapped as size-1
                // arrays), so no detection pre-pass is needed.
                MultiWritePoint mwp = parseMultiWritePoint(doc, defaultTimestampNs);
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

                // Fire-and-forget metadata indexing for this single write.
                // Data is already durable in WAL; metadata is dispatched to shard 0
                // as a background task with retry on failure.
#if TSDB_LOG_INSERT_PATH
                size_t metaOpsCount = metaOps.size();
#endif
                if (!metaOps.empty()) {
                    engineSharded->local().dispatchMetadataAsync(std::move(metaOps));
                }
#if TSDB_LOG_INSERT_PATH
                LOG_INSERT_PATH(tsdb::http_log, info,
                    "[METADATA] Single write: dispatched {} unique series for async indexing",
                    metaOpsCount);
#endif
            }
        }
        
        // Success response
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = createSuccessResponse(pointsWritten);
        rep->add_header("Content-Type", "application/json");
        
    } catch (const seastar::gate_closed_exception&) {
        // Insert gate closed — server is shutting down. Return 503 so clients
        // know to retry against another node or after restart.
        rep->set_status(seastar::http::reply::status_type::service_unavailable);
        rep->_content = createErrorResponse("Server is shutting down");
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