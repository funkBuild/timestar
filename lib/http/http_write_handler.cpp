#include "http_write_handler.hpp"

#include "http_auth.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "placement_table.hpp"
#include "series_key.hpp"

#include <boost/iterator/counting_iterator.hpp>

#include <tsl/robin_map.h>
#include <tsl/robin_set.h>

#include <algorithm>
#include <chrono>
#include <numeric>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <stdexcept>
#include <unordered_set>

using namespace seastar;
using namespace httpd;
using timestar::buildSeriesKey;

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
    static constexpr auto value =
        object("measurement", &T::measurement, "tags", &T::tags, "fields", &T::fields, "timestamp", &T::timestamp);
};

struct GlazeMultiWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    glz::generic fields;  // Can hold arrays or scalars
    std::optional<std::variant<uint64_t, std::vector<uint64_t>>> timestamps;
    std::optional<uint64_t> timestamp;  // Single timestamp option
};

template <>
struct glz::meta<GlazeMultiWritePoint> {
    using T = GlazeMultiWritePoint;
    static constexpr auto value = object("measurement", &T::measurement, "tags", &T::tags, "fields", &T::fields,
                                         "timestamps", &T::timestamps, "timestamp", &T::timestamp);
};

struct GlazeBatchWrite {
    std::vector<glz::generic> writes;
};

template <>
struct glz::meta<GlazeBatchWrite> {
    using T = GlazeBatchWrite;
    static constexpr auto value = object("writes", &T::writes);
};

// Fast-path struct for the common case: single write with all double fields.
// Parses timestamps and field arrays directly into typed vectors, bypassing
// the glz::json_t DOM entirely. This avoids ~110K json_value_t node allocations
// for a typical 10K-timestamp × 10-field batch.
struct FastDoubleWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::optional<std::vector<uint64_t>> timestamps;
    std::optional<uint64_t> timestamp;
    std::map<std::string, std::vector<double>> fields;
};

template <>
struct glz::meta<FastDoubleWritePoint> {
    using T = FastDoubleWritePoint;
    static constexpr auto value = object("measurement", &T::measurement, "tags", &T::tags, "timestamps", &T::timestamps,
                                         "timestamp", &T::timestamp, "fields", &T::fields);
};

HttpWriteHandler::HttpWriteHandler(seastar::sharded<Engine>* _engineSharded) : engineSharded(_engineSharded) {
    if (!engineSharded) {
        throw std::invalid_argument("engineSharded must not be null");
    }
}

// Fast validity check — no allocation on the happy path.
static bool hasReservedChar(const std::string& s, bool allowSpace) {
    for (char c : s) {
        if (c == '\0' || c == ',' || c == '=')
            return true;
        if (!allowSpace && c == ' ')
            return true;
    }
    return false;
}

// Fast-path name validation: returns true if name is valid, false otherwise.
// Avoids constructing context string on the fast path.
static inline bool isValidName(const std::string& name) {
    return !name.empty() && !hasReservedChar(name, false);
}

static inline bool isValidTagValue(const std::string& value) {
    return !value.empty() && !hasReservedChar(value, true);
}

// Shared slow-path validator: hasReservedChar() already ran and returned true,
// so walk the string to produce a specific error message.
static std::string validateStringHelper(const std::string& s, const std::string& context, bool allowSpace) {
    if (s.empty())
        return context + " must not be empty";
    if (!hasReservedChar(s, allowSpace))
        return {};
    for (char c : s) {
        if (c == '\0')
            return context + " must not contain null bytes";
        if (c == ',')
            return context + " must not contain commas";
        if (c == '=')
            return context + " must not contain equals signs";
        if (!allowSpace && c == ' ')
            return context + " must not contain spaces";
    }
    return {};
}

std::string HttpWriteHandler::validateName(const std::string& name, const std::string& context) {
    return validateStringHelper(name, context, false);
}

std::string HttpWriteHandler::validateTagValue(const std::string& value, const std::string& context) {
    return validateStringHelper(value, context, true);
}

void HttpWriteHandler::validateWritePointNames(const std::string& measurement,
                                               const std::map<std::string, std::string>& tags,
                                               const std::map<std::string, FieldValue>& fields) {
    auto err = validateName(measurement, "Measurement name");
    if (!err.empty())
        throw std::invalid_argument(err);

    for (const auto& [key, value] : tags) {
        // Validate first with cheap static context; only build dynamic context on error
        err = validateName(key, "Tag key");
        if (!err.empty()) [[unlikely]]
            throw std::invalid_argument(err + " '" + key + "'");
        err = validateTagValue(value, "Tag value");
        if (!err.empty()) [[unlikely]]
            throw std::invalid_argument(err + " for '" + key + "'");
    }

    for (const auto& [fieldName, fieldValue] : fields) {
        err = validateName(fieldName, "Field name");
        if (!err.empty()) [[unlikely]]
            throw std::invalid_argument(err + " '" + fieldName + "'");
    }
}

void HttpWriteHandler::parseAndValidateWritePoint(const std::string& json) {
    GlazeWritePoint glazePoint;
    auto error = glz::read_json(glazePoint, json);
    if (error) {
        throw std::invalid_argument("Failed to parse write point: " + std::string(glz::format_error(error)));
    }

    // Validate measurement name
    auto err = validateName(glazePoint.measurement, "Measurement name");
    if (!err.empty())
        throw std::invalid_argument(err);

    // Validate tags
    for (const auto& [key, value] : glazePoint.tags) {
        err = validateName(key, "Tag key");
        if (!err.empty()) [[unlikely]]
            throw std::invalid_argument(err + " '" + key + "'");
        err = validateTagValue(value, "Tag value");
        if (!err.empty()) [[unlikely]]
            throw std::invalid_argument(err + " for '" + key + "'");
    }

    // Validate field names
    if (glazePoint.fields.is_object()) {
        auto& fields_obj = glazePoint.fields.get<json_value_t::object_t>();
        for (const auto& [fieldName, fieldValue] : fields_obj) {
            err = validateName(fieldName, "Field name");
            if (!err.empty()) [[unlikely]]
                throw std::invalid_argument(err + " '" + fieldName + "'");
        }
    }
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const json_value_t& point,
                                                                         uint64_t defaultTimestampNs) {
    MultiWritePoint mwp;

    // Extract fields directly from the json_value_t object, avoiding a
    // serialize-then-reparse round-trip that was previously done via
    // glz::write_json + glz::read_json(GlazeMultiWritePoint, ...).

    if (!point.is_object()) {
        throw std::invalid_argument("Write point must be a JSON object");
    }
    auto& obj = point.get<json_value_t::object_t>();

    // Extract measurement
    auto measurementIt = obj.find("measurement");
    if (measurementIt == obj.end() || !measurementIt->second.is_string()) {
        throw std::invalid_argument("Missing or invalid 'measurement' field");
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

    // Validate measurement and tag names before any processing.
    // Use fast-path checks first to avoid constructing context strings on the common valid path.
    {
        if (!isValidName(mwp.measurement)) {
            auto err = validateName(mwp.measurement, "Measurement name");
            if (!err.empty())
                throw std::invalid_argument(err);
        }
        for (const auto& [key, value] : mwp.tags) {
            if (!isValidName(key)) {
                auto err = validateName(key, "Tag key '" + key + "'");
                if (!err.empty())
                    throw std::invalid_argument(err);
            }
            if (!isValidTagValue(value)) {
                auto err = validateTagValue(value, "Tag value for '" + key + "'");
                if (!err.empty())
                    throw std::invalid_argument(err);
            }
        }
    }

    // Parse fields - handle both scalars and arrays
    auto fieldsIt = obj.find("fields");
    if (fieldsIt == obj.end() || !fieldsIt->second.is_object()) {
        throw std::invalid_argument("Fields must be an object");
    }
    auto& fields_obj = fieldsIt->second.get<json_value_t::object_t>();

    for (auto& [fieldName, fieldValue] : fields_obj) {
        // Validate field name (fast path avoids string allocation for valid names)
        if (!isValidName(fieldName)) {
            auto err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
        }

        FieldArrays fa;

        if (fieldValue.is_array()) {
            auto& arr = fieldValue.get<json_value_t::array_t>();
            if (arr.empty()) {
                throw std::invalid_argument("Field array cannot be empty: " + fieldName);
            }

            // Determine type from first element
            const size_t arrSize = arr.size();
            if (arr[0].is_number()) {
                if (arr[0].holds<int64_t>() || arr[0].holds<uint64_t>()) {
                    fa.type = FieldArrays::INTEGER;
                    fa.integers.reserve(arrSize);
                    for (auto& elem : arr) {
                        if (elem.is_number()) {
                            fa.integers.push_back(elem.as<int64_t>());
                        } else {
                            throw std::invalid_argument("Mixed types in field array: " + fieldName);
                        }
                    }
                } else {
                    fa.type = FieldArrays::DOUBLE;
                    fa.doubles.reserve(arrSize);
                    for (auto& elem : arr) {
                        if (elem.is_number()) {
                            fa.doubles.push_back(elem.as<double>());
                        } else {
                            throw std::invalid_argument("Mixed types in field array: " + fieldName);
                        }
                    }
                }
            } else if (arr[0].is_boolean()) {
                fa.type = FieldArrays::BOOL;
                fa.bools.reserve(arrSize);
                for (auto& elem : arr) {
                    if (elem.is_boolean()) {
                        fa.bools.push_back(elem.get<bool>() ? 1 : 0);
                    } else {
                        throw std::invalid_argument("Mixed types in field array: " + fieldName);
                    }
                }
            } else if (arr[0].is_string()) {
                fa.type = FieldArrays::STRING;
                fa.strings.reserve(arrSize);
                LOG_INSERT_PATH(timestar::http_log, debug, "[WRITE] Detected string array field '{}' with {} elements",
                                fieldName, arr.size());
                for (auto& elem : arr) {
                    if (elem.is_string()) {
                        fa.strings.push_back(elem.get<std::string>());
                    } else {
                        throw std::invalid_argument("Mixed types in field array: " + fieldName);
                    }
                }
                LOG_INSERT_PATH(timestar::http_log, debug, "[WRITE] Added {} string values to field '{}'",
                                fa.strings.size(), fieldName);
            } else {
                throw std::invalid_argument("Unsupported field array type: " + fieldName);
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
                LOG_INSERT_PATH(timestar::http_log, debug, "[WRITE] Detected single string field '{}' with value: '{}'",
                                fieldName, str_value);
            } else {
                throw std::invalid_argument("Unsupported field type: " + fieldName);
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
                case FieldArrays::DOUBLE:
                    fieldSize = fieldArray.doubles.size();
                    break;
                case FieldArrays::BOOL:
                    fieldSize = fieldArray.bools.size();
                    break;
                case FieldArrays::STRING:
                    fieldSize = fieldArray.strings.size();
                    break;
                case FieldArrays::INTEGER:
                    fieldSize = fieldArray.integers.size();
                    break;
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
                case FieldArrays::DOUBLE:
                    fieldSize = fieldArray.doubles.size();
                    break;
                case FieldArrays::BOOL:
                    fieldSize = fieldArray.bools.size();
                    break;
                case FieldArrays::STRING:
                    fieldSize = fieldArray.strings.size();
                    break;
                case FieldArrays::INTEGER:
                    fieldSize = fieldArray.integers.size();
                    break;
            }
            numPoints = std::max(numPoints, fieldSize);
        }

        // Generate timestamps 1ms apart from the pre-computed default
        mwp.timestamps.reserve(numPoints);
        uint64_t ts = defaultTimestampNs;
        for (size_t i = 0; i < numPoints; i++) {
            mwp.timestamps.push_back(ts);
            ts += 1000000;
        }
    }

    return mwp;
}

// Build a MultiWritePoint from a FastDoubleWritePoint (fast path, all fields are doubles).
// Returns true on success, false if the data is invalid.
bool HttpWriteHandler::buildMWPFromFastPath(FastDoubleWritePoint& fwp, uint64_t defaultTimestampNs,
                                            MultiWritePoint& mwp) {
    mwp.measurement = std::move(fwp.measurement);
    mwp.tags = std::move(fwp.tags);

    // Validate names
    if (!isValidName(mwp.measurement))
        return false;
    for (const auto& [key, value] : mwp.tags) {
        if (!isValidName(key) || !isValidTagValue(value))
            return false;
    }

    // Move fields as DOUBLE type
    for (auto& [fieldName, values] : fwp.fields) {
        if (!isValidName(fieldName))
            return false;
        if (values.empty())
            continue;
        FieldArrays fa;
        fa.type = FieldArrays::DOUBLE;
        fa.doubles = std::move(values);
        mwp.fields[fieldName] = std::move(fa);
    }
    if (mwp.fields.empty())
        return false;

    // Determine number of points from the largest field array
    size_t numPoints = 1;
    for (const auto& [_, fa] : mwp.fields) {
        numPoints = std::max(numPoints, fa.doubles.size());
    }

    // Handle timestamps (same logic as parseMultiWritePoint)
    if (fwp.timestamps.has_value() && !fwp.timestamps->empty()) {
        mwp.timestamps = std::move(*fwp.timestamps);
    } else if (fwp.timestamp.has_value()) {
        mwp.timestamps.resize(numPoints, *fwp.timestamp);
    } else {
        mwp.timestamps.reserve(numPoints);
        uint64_t ts = defaultTimestampNs;
        for (size_t i = 0; i < numPoints; i++) {
            mwp.timestamps.push_back(ts);
            ts += 1000000;
        }
    }

    return true;
}

std::vector<HttpWriteHandler::MultiWritePoint> HttpWriteHandler::coalesceWrites(
    const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs) {
    // Configuration constants
    static const size_t MAX_COALESCE_SIZE = 10000;  // Max values per field array
    static const size_t MIN_COALESCE_COUNT = 2;     // Min writes needed to coalesce

#if TIMESTAR_LOG_INSERT_PATH
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

    LOG_INSERT_PATH(timestar::http_log, debug, "[COALESCE] Processing {} writes for coalescing", writes_array.size());

    // Helper lambda: add a single scalar value to a CoalesceCandidate.
    // Returns false if the value type is unsupported, true otherwise.
    auto addScalarToCandidate = [](CoalesceCandidate& candidate, TSMValueType valueType, uint64_t ts,
                                   const json_value_t& val) -> bool {
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
    auto initCandidate = [](CoalesceCandidate& candidate, const std::string& seriesKey, const std::string& measurement,
                            seastar::lw_shared_ptr<const std::map<std::string, std::string>> tags,
                            const std::string& fieldName, TSMValueType valueType, const std::string& groupKey) {
        candidate.seriesKey = seriesKey;
        candidate.groupKey = groupKey;
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
                                     const std::string& fieldName, TSMValueType valueType, size_t numValuesToAdd,
                                     const std::string& groupKey) -> CoalesceCandidate& {
        auto it = candidates.find(seriesKey);
        if (it == candidates.end()) {
            CoalesceCandidate& c = candidates[seriesKey];
            initCandidate(c, seriesKey, measurement, std::move(tags), fieldName, valueType, groupKey);
            return c;
        }
        CoalesceCandidate& existing = it.value();
        if (existing.valueType == valueType && existing.timestamps.size() + numValuesToAdd <= MAX_COALESCE_SIZE) {
            return existing;
        }
        // Integer/Float promotion: JSON serializers (e.g. JavaScript) encode
        // exact-integer doubles like 1.0 as "1" (no decimal point), which
        // generic_u64 parses as int64_t. Promote the candidate to Float so
        // all numeric values for the same field are stored in one series.
        if (existing.timestamps.size() + numValuesToAdd <= MAX_COALESCE_SIZE) {
            if (existing.valueType == TSMValueType::Integer && valueType == TSMValueType::Float) {
                // Promote existing integer candidate to float
                existing.doubleValues.reserve(existing.integerValues.size() + numValuesToAdd);
                for (int64_t v : existing.integerValues) {
                    existing.doubleValues.push_back(static_cast<double>(v));
                }
                existing.integerValues.clear();
                existing.integerValues.shrink_to_fit();
                existing.valueType = TSMValueType::Float;
                return existing;
            }
            if (existing.valueType == TSMValueType::Float && valueType == TSMValueType::Integer) {
                // Existing is already float; caller will add value as double via addScalarToCandidate
                // after we return with the promoted type. We just need to signal
                // the caller to treat the incoming integer as a float.
                // Since we return the existing Float candidate, the caller's
                // valueType variable won't match, but the caller uses the
                // candidate's valueType for storage. Let's handle this by
                // returning the existing candidate directly — the caller
                // must cast the integer to double before adding.
                return existing;
            }
        }
        // Non-numeric type mismatch or size overflow: create a disambiguated candidate
        size_t suffix = 1;
        static constexpr size_t MAX_DISAMBIGUATE = 10000;
        std::string altKey;
        do {
            altKey = seriesKey + "#" + std::to_string(suffix++);
            if (suffix > MAX_DISAMBIGUATE) {
                throw std::invalid_argument("Too many type-conflicting writes for series: " + seriesKey);
            }
        } while (candidates.count(altKey) > 0);
        CoalesceCandidate& c = candidates[altKey];
        initCandidate(c, altKey, measurement, std::move(tags), fieldName, valueType, groupKey);
        return c;
    };

    // Parse writes directly from JSON objects for better performance
    for (const auto& write : writes_array) {
        totalWritesProcessed++;

        if (!write.is_object())
            continue;

        try {
            auto& writeObj = write.get<json_value_t::object_t>();

            // Extract measurement
            auto measurementIt = writeObj.find("measurement");
            if (measurementIt == writeObj.end() || !measurementIt->second.is_string())
                continue;
            std::string measurement = measurementIt->second.get<std::string>();

            // Validate measurement name
            {
                auto err = validateName(measurement, "Measurement name");
                if (!err.empty())
                    throw std::invalid_argument(err);
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

            // Bug #11 fix: Guard against empty timestamps vector.
            // The "timestamps" JSON key may exist as an array of all non-numeric
            // elements, leaving timestamps empty after parsing. Accessing
            // timestamps[0] below would be undefined behavior.
            if (timestamps.empty()) {
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
                        auto err = validateName(tagKey, "Tag key '" + tagKey + "'");
                        if (!err.empty())
                            throw std::invalid_argument(err);
                        auto val = tagValue.get<std::string>();
                        err = validateTagValue(val, "Tag value for '" + tagKey + "'");
                        if (!err.empty())
                            throw std::invalid_argument(err);
                        localTags[tagKey] = std::move(val);
                    }
                }
            }

            // Pre-build measurement+tags prefix for series key construction (once per write point)
            std::string seriesKeyPrefix;
            {
                size_t prefixSize = measurement.length();
                for (const auto& [tagKey, tagValue] : localTags) {
                    prefixSize += 1 + tagKey.size() + 1 + tagValue.size();  // ",key=value"
                }
                seriesKeyPrefix.reserve(prefixSize);
            }
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
            if (fieldsIt == writeObj.end() || !fieldsIt->second.is_object())
                continue;

            auto& fieldsObj = fieldsIt->second.get<json_value_t::object_t>();
            for (const auto& [fieldName, fieldValue] : fieldsObj) {
                // Validate field name
                {
                    auto err = validateName(fieldName, "Field name '" + fieldName + "'");
                    if (!err.empty())
                        throw std::invalid_argument(err);
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
                        throw std::invalid_argument("Field array cannot be empty: " + fieldName);
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
                        throw std::invalid_argument("Unsupported field array type: " + fieldName);
                    }

                    // Resolve timestamps for this array: reference the original when sizes
                    // match (avoids copying 10K+ element vector per field), or build a new one.
                    std::vector<uint64_t> fieldTimestampsOwned;
                    const std::vector<uint64_t>* fieldTimestampsPtr = nullptr;
                    if (timestamps.size() == arr.size()) {
                        fieldTimestampsPtr = &timestamps;  // Zero-copy: share existing vector
                    } else if (timestamps.size() == 1) {
                        fieldTimestampsOwned.resize(arr.size(), timestamps[0]);
                        fieldTimestampsPtr = &fieldTimestampsOwned;
                    } else {
                        uint64_t ts = timestamps.empty() ? defaultTimestampNs : timestamps[0];
                        fieldTimestampsOwned.reserve(arr.size());
                        for (size_t i = 0; i < arr.size(); i++) {
                            fieldTimestampsOwned.push_back(ts);
                            ts += 1000000;
                        }
                        fieldTimestampsPtr = &fieldTimestampsOwned;
                    }
                    const auto& fieldTimestamps = *fieldTimestampsPtr;

                    CoalesceCandidate& candidate = findOrCreateCandidate(seriesKey, measurement, sharedTags, fieldName,
                                                                         valueType, arr.size(), seriesKeyPrefix);
                    // Apply any Integer->Float promotion from findOrCreateCandidate
                    valueType = candidate.valueType;

                    // Add all array elements to candidate
                    candidate.timestamps.reserve(candidate.timestamps.size() + arr.size());
                    if (valueType == TSMValueType::Float)
                        candidate.doubleValues.reserve(candidate.doubleValues.size() + arr.size());
                    else if (valueType == TSMValueType::Boolean)
                        candidate.boolValues.reserve(candidate.boolValues.size() + arr.size());
                    else if (valueType == TSMValueType::String)
                        candidate.stringValues.reserve(candidate.stringValues.size() + arr.size());
                    else if (valueType == TSMValueType::Integer)
                        candidate.integerValues.reserve(candidate.integerValues.size() + arr.size());
                    for (size_t i = 0; i < arr.size(); i++) {
                        auto& elem = arr[i];
                        candidate.timestamps.push_back(fieldTimestamps[i]);
                        candidate.timestampHashSum += fieldTimestamps[i];
                        candidate.timestampHashXor ^= fieldTimestamps[i];
                        if (valueType == TSMValueType::Float) {
                            if (!elem.is_number())
                                throw std::invalid_argument("Mixed types in field array: " + fieldName);
                            candidate.doubleValues.push_back(elem.as<double>());
                        } else if (valueType == TSMValueType::Boolean) {
                            if (!elem.is_boolean())
                                throw std::invalid_argument("Mixed types in field array: " + fieldName);
                            candidate.boolValues.push_back(elem.get<bool>() ? 1 : 0);
                        } else if (valueType == TSMValueType::String) {
                            if (!elem.is_string())
                                throw std::invalid_argument("Mixed types in field array: " + fieldName);
                            std::string strValue = elem.get<std::string>();
                            candidate.stringValues.push_back(std::move(strValue));
                        } else if (valueType == TSMValueType::Integer) {
                            if (!elem.is_number())
                                throw std::invalid_argument("Mixed types in field array: " + fieldName);
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
                        continue;  // Skip unsupported types
                    }

                    CoalesceCandidate& candidate = findOrCreateCandidate(seriesKey, measurement, sharedTags, fieldName,
                                                                         valueType, 1, seriesKeyPrefix);
                    // Use candidate.valueType: findOrCreateCandidate may have promoted
                    // Integer→Float when mixing JSON integers (e.g. "1") with floats (e.g. "1.1")
                    addScalarToCandidate(candidate, candidate.valueType, timestamps[0], fieldValue);
                }
            }
        } catch (const std::exception& e) {
            LOG_INSERT_PATH(timestar::http_log, debug, "[COALESCE] Failed to parse write: {}", e.what());
            continue;
        }
    }

    // Second pass: convert candidates to MultiWritePoint objects
    std::vector<MultiWritePoint> result;

    // Group by measurement+tags efficiently (only for coalescing)
    tsl::robin_map<std::string, std::vector<std::string>> grouped;  // groupKey -> seriesKeys
    tsl::robin_set<std::string> processedSeriesKeys;                // Track which candidates were coalesced
    [[maybe_unused]] size_t individualCount = 0;

    for (const auto& [seriesKey, candidate] : candidates) {
        if (candidate.timestamps.size() < MIN_COALESCE_COUNT) {
            individualCount++;
            // Will process these in a separate pass
            continue;
        }

        // Use pre-computed group key (measurement + tags, without field)
        grouped[candidate.groupKey].push_back(seriesKey);
    }

    // IMPORTANT: Process coalesced groups FIRST before moving data from candidates
    // Build MultiWritePoint objects for coalesced groups
    for (auto& [groupKey, seriesKeys] : grouped) {
        if (seriesKeys.empty())
            continue;

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
            LOG_INSERT_PATH(timestar::http_log, debug,
                            "[COALESCE] Group '{}' has incompatible timestamps, emitting individual MultiWritePoints",
                            groupKey);
            // Timestamps are incompatible across fields, so emit each candidate
            // as its own single-field MultiWritePoint to avoid data loss.
            for (const auto& seriesKey : seriesKeys) {
                auto& candidate = candidates[seriesKey];

                MultiWritePoint mwp;
                mwp.measurement = candidate.measurement;
                mwp.tags = *candidate.sharedTags;
                mwp.timestamps = std::move(candidate.timestamps);

                FieldArrays fa;
                fa.type = (candidate.valueType == TSMValueType::Float)     ? FieldArrays::DOUBLE
                          : (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL
                          : (candidate.valueType == TSMValueType::Integer) ? FieldArrays::INTEGER
                                                                           : FieldArrays::STRING;

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
            fa.type = (candidate.valueType == TSMValueType::Float)     ? FieldArrays::DOUBLE
                      : (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL
                      : (candidate.valueType == TSMValueType::Integer) ? FieldArrays::INTEGER
                                                                       : FieldArrays::STRING;

            // Simple assignment without complex sorting
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
            processedSeriesKeys.insert(seriesKey);  // Mark this series as processed
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
        fa.type = (candidate.valueType == TSMValueType::Float)     ? FieldArrays::DOUBLE
                  : (candidate.valueType == TSMValueType::Boolean) ? FieldArrays::BOOL
                  : (candidate.valueType == TSMValueType::Integer) ? FieldArrays::INTEGER
                                                                   : FieldArrays::STRING;

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
    }

#if TIMESTAR_LOG_INSERT_PATH
    size_t coalescedCount = result.size() - individualCount;
    auto end_coalesce = std::chrono::high_resolution_clock::now();
    auto coalesce_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_coalesce - start_coalesce);
    LOG_INSERT_PATH(timestar::http_log, info,
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
            case FieldArrays::DOUBLE:
                fieldSize = fieldArray.doubles.size();
                break;
            case FieldArrays::BOOL:
                fieldSize = fieldArray.bools.size();
                break;
            case FieldArrays::STRING:
                fieldSize = fieldArray.strings.size();
                break;
            case FieldArrays::INTEGER:
                fieldSize = fieldArray.integers.size();
                break;
        }

        if (fieldSize != expectedSize && fieldSize != 1) {
            error = "Field '" + fieldName + "' has " + std::to_string(fieldSize) + " values but expected " +
                    std::to_string(expectedSize) + " (or 1 for scalar)";
            return false;
        }
    }

    return true;
}

seastar::future<HttpWriteHandler::WriteResult> HttpWriteHandler::processMultiWritePoint(MultiWritePoint& point) {
    // Shard-local cache of series IDs that have already been indexed on shard 0.
    // After the first batch, subsequent writes for the same series skip the
    // cross-shard metadata RPC entirely, eliminating the biggest bottleneck
    // for non-shard-0 shards. The cache persists for the lifetime of the shard.
    static thread_local tsl::robin_set<SeriesId128, SeriesId128::Hash> knownSeriesCache;
    // Cap cache to prevent unbounded memory growth with high-cardinality workloads
    static constexpr size_t MAX_KNOWN_SERIES_CACHE = 500'000;
    if (knownSeriesCache.size() > MAX_KNOWN_SERIES_CACHE) [[unlikely]] {
        knownSeriesCache.clear();
    }

    // Local metadata tracking — each coroutine gets its own copies so there
    // is no shared mutable state when multiple coroutines run concurrently.
    std::unordered_set<SeriesId128, SeriesId128::Hash> seenMF;
    std::vector<MetaOp> metaOps;
#if TIMESTAR_LOG_INSERT_PATH
    auto start_total = std::chrono::high_resolution_clock::now();
#endif

    // No artificial batch splitting -- the WAL enforces its own 16 MiB per-segment
    // limit (MAX_WAL_SIZE) and will signal rollover if a single insert exceeds it.
    // Splitting here only adds redundant copies and sequential cross-shard roundtrips.

    // Group inserts by shard to reduce cross-shard operations and process them in batches
    LOG_INSERT_PATH(timestar::http_log, debug, "[WRITE] Processing MultiWritePoint: {} timestamps × {} fields",
                    point.timestamps.size(), point.fields.size());

#if TIMESTAR_LOG_INSERT_PATH
    auto start_grouping = std::chrono::high_resolution_clock::now();
#endif

    // NEW APPROACH: Keep arrays intact - create ONE TimeStarInsert per field with ALL timestamps/values
    // Group by (shard, type) to batch properly
    // Use pre-sized vectors instead of std::map since shard IDs are small integers [0, smp::count)
    const size_t shardCount = seastar::smp::count;
    std::vector<std::vector<TimeStarInsert<double>>> shardDoubleInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<bool>>> shardBoolInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<std::string>>> shardStringInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<int64_t>>> shardIntegerInserts(shardCount);

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
        size_t shard = timestar::routeToCore(seriesId);

        // Create ONE TimeStarInsert with ALL values for this field.
        // Move values from the MWP's FieldArrays directly into the TimeStarInsert
        // to avoid copying large value vectors (e.g. 10K doubles = 80KB per field).
        // Each field is processed exactly once, so the move-from is safe.
        switch (fieldArray.type) {
            case FieldArrays::DOUBLE: {
                if (fieldArray.doubles.empty())
                    continue;

                // Track metadata before potential move - deduplicate by SeriesId128 (16-byte key,
                // fast hash via first 8 bytes) instead of the raw 60-100 byte series key string.
                // knownSeriesCache is a shard-local persistent cache that avoids redundant
                // cross-shard metadata RPCs for series we've already indexed.
                {
                    if (seenMF.insert(seriesId).second) {
                        if (knownSeriesCache.find(seriesId) == knownSeriesCache.end()) {
                            knownSeriesCache.insert(seriesId);
                            metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});
                        }
                    }
                }

                TimeStarInsert<double> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
                insert.setCachedSeriesKey(std::move(seriesKey));
                insert.setCachedSeriesId128(seriesId);
                insert.values = std::move(fieldArray.doubles);

                shardDoubleInserts[shard].push_back(std::move(insert));
                break;
            }

            case FieldArrays::BOOL: {
                if (fieldArray.bools.empty())
                    continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        if (knownSeriesCache.find(seriesId) == knownSeriesCache.end()) {
                            knownSeriesCache.insert(seriesId);
                            metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, *sharedTags});
                        }
                    }
                }

                TimeStarInsert<bool> insert(point.measurement, fieldName);
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
                if (fieldArray.strings.empty())
                    continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        if (knownSeriesCache.find(seriesId) == knownSeriesCache.end()) {
                            knownSeriesCache.insert(seriesId);
                            metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, *sharedTags});
                        }
                    }
                }

                TimeStarInsert<std::string> insert(point.measurement, fieldName);
                insert.setSharedTags(sharedTags);
                insert.setSharedTimestamps(sharedTimestamps);
                insert.setCachedSeriesKey(std::move(seriesKey));
                insert.setCachedSeriesId128(seriesId);
                insert.values = std::move(fieldArray.strings);

                shardStringInserts[shard].push_back(std::move(insert));
                break;
            }

            case FieldArrays::INTEGER: {
                if (fieldArray.integers.empty())
                    continue;

                // Track metadata before potential move - deduplicate by SeriesId128.
                {
                    if (seenMF.insert(seriesId).second) {
                        if (knownSeriesCache.find(seriesId) == knownSeriesCache.end()) {
                            knownSeriesCache.insert(seriesId);
                            metaOps.push_back(MetaOp{TSMValueType::Integer, point.measurement, fieldName, *sharedTags});
                        }
                    }
                }

                TimeStarInsert<int64_t> insert(point.measurement, fieldName);
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

#if TIMESTAR_LOG_INSERT_PATH
    auto end_grouping = std::chrono::high_resolution_clock::now();
    auto grouping_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_grouping - start_grouping);
#endif

    // Metadata ops are returned to the caller for batch-level deduplication and indexing

#if TIMESTAR_LOG_INSERT_PATH
    auto start_batch_ops = std::chrono::high_resolution_clock::now();
#endif

    // Dispatch to all shards in parallel - inserts are already batched per field with all timestamps
    std::vector<seastar::future<AggregatedTimingInfo>> shardFutures;
    shardFutures.reserve(shardCount);

    for (size_t shard = 0; shard < shardCount; ++shard) {
        // Skip shards with no work
        if (shardDoubleInserts[shard].empty() && shardBoolInserts[shard].empty() && shardStringInserts[shard].empty() &&
            shardIntegerInserts[shard].empty()) {
            continue;
        }

        // Move this shard's inserts directly from the pre-sized vectors
        auto doubles = std::move(shardDoubleInserts[shard]);
        auto bools = std::move(shardBoolInserts[shard]);
        auto strings = std::move(shardStringInserts[shard]);
        auto integers = std::move(shardIntegerInserts[shard]);

        // Lambda that inserts all types into the given engine
        auto doInserts = [](Engine& engine, auto doubles, auto bools, auto strings,
                            auto integers) mutable -> seastar::future<AggregatedTimingInfo> {
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
        };

        // Local shard: call engine directly, avoiding cross-shard message queue overhead
        if (shard == seastar::this_shard_id()) {
            shardFutures.push_back(doInserts(engineSharded->local(), std::move(doubles), std::move(bools),
                                             std::move(strings), std::move(integers)));
        } else {
            // Remote shard: use invoke_on
            shardFutures.push_back(engineSharded->invoke_on(
                shard,
                [doubles = std::move(doubles), bools = std::move(bools), strings = std::move(strings),
                 integers = std::move(integers)](Engine& engine) mutable -> seastar::future<AggregatedTimingInfo> {
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
                }));
        }
    }

    // Wait for all shard operations to complete in parallel
    auto shardTimings = co_await seastar::when_all_succeed(std::move(shardFutures));

    // Aggregate all timing results
    AggregatedTimingInfo aggregatedTiming;
    for (const auto& timing : shardTimings) {
        aggregatedTiming.aggregate(timing);
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto end_batch_ops = std::chrono::high_resolution_clock::now();
    auto batch_ops_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_batch_ops - start_batch_ops);
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);

    LOG_INSERT_PATH(timestar::http_log, info,
                    "[PERF] [HTTP] processMultiWritePoint breakdown - Total: {}μs, Grouping: {}μs, BatchOps: {}μs",
                    total_duration.count(), grouping_duration.count(), batch_ops_duration.count());
#endif

    co_return WriteResult{std::move(aggregatedTiming), std::move(metaOps)};
}

HttpWriteHandler::WritePoint HttpWriteHandler::parseWritePoint(const std::string& json, uint64_t defaultTimestampNs) {
    WritePoint wp;

    GlazeWritePoint glazePoint;
    auto error = glz::read_json(glazePoint, json);
    if (error) {
        throw std::invalid_argument("Failed to parse write point: " + std::string(glz::format_error(error)));
    }

    wp.measurement = glazePoint.measurement;
    wp.tags = glazePoint.tags;

    // Validate measurement and tag names before any processing
    {
        auto err = validateName(wp.measurement, "Measurement name");
        if (!err.empty())
            throw std::invalid_argument(err);
        for (const auto& [key, value] : wp.tags) {
            err = validateName(key, "Tag key '" + key + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
            err = validateTagValue(value, "Tag value for '" + key + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
        }
    }

    // Parse fields
    if (!glazePoint.fields.is_object()) {
        throw std::invalid_argument("'fields' must be an object");
    }

    auto& fields_obj = glazePoint.fields.get<json_value_t::object_t>();
    if (fields_obj.empty()) {
        throw std::invalid_argument("'fields' object cannot be empty");
    }

    for (auto& [fieldName, fieldValue] : fields_obj) {
        // Validate field name (fast path avoids string allocation for valid names)
        if (!isValidName(fieldName)) {
            auto err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
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
            LOG_INSERT_PATH(timestar::http_log, debug,
                            "[WRITE] Single write: detected string field '{}' with value: '{}'", fieldName, str_value);
        } else {
            throw std::invalid_argument("Unsupported field type for field: " + fieldName);
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
        throw std::invalid_argument("Write point must be a JSON object");
    }
    auto& obj = doc.get<json_value_t::object_t>();

    // Extract measurement
    auto measurementIt = obj.find("measurement");
    if (measurementIt == obj.end() || !measurementIt->second.is_string()) {
        throw std::invalid_argument("Missing or invalid 'measurement' field");
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
        if (!err.empty())
            throw std::invalid_argument(err);
        for (const auto& [key, value] : wp.tags) {
            err = validateName(key, "Tag key '" + key + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
            err = validateTagValue(value, "Tag value for '" + key + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
        }
    }

    // Parse fields
    auto fieldsIt = obj.find("fields");
    if (fieldsIt == obj.end() || !fieldsIt->second.is_object()) {
        throw std::invalid_argument("'fields' must be an object");
    }

    auto& fields_obj = fieldsIt->second.get<json_value_t::object_t>();
    if (fields_obj.empty()) {
        throw std::invalid_argument("'fields' object cannot be empty");
    }

    for (auto& [fieldName, fieldValue] : fields_obj) {
        // Validate field name (fast path avoids string allocation for valid names)
        if (!isValidName(fieldName)) {
            auto err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
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
            LOG_INSERT_PATH(timestar::http_log, debug,
                            "[WRITE] Single write: detected string field '{}' with value: '{}'", fieldName, str_value);
        } else {
            throw std::invalid_argument("Unsupported field type for field: " + fieldName);
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
#if TIMESTAR_LOG_INSERT_PATH
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
    std::vector<std::vector<TimeStarInsert<double>>> shardDoubleInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<bool>>> shardBoolInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<std::string>>> shardStringInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<int64_t>>> shardIntegerInserts(shardCount);
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
        size_t shard = timestar::routeToCore(seriesId);

        // Handle each variant type explicitly, accumulating into per-shard vectors
        if (std::holds_alternative<double>(fieldValue)) {
            double value = std::get<double>(fieldValue);
            TimeStarInsert<double> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);
            shardDoubleInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<bool>(fieldValue)) {
            bool value = std::get<bool>(fieldValue);
            TimeStarInsert<bool> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);
            shardBoolInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<std::string>(fieldValue)) {
            const std::string& value = std::get<std::string>(fieldValue);
            TimeStarInsert<std::string> insert(point.measurement, fieldName);
            insert.setSharedTags(sharedTags);
            insert.setCachedSeriesKey(std::move(seriesKey));
            insert.setCachedSeriesId128(seriesId);
            insert.addValue(point.timestamp, value);

            LOG_INSERT_PATH(
                timestar::http_log, debug,
                "[WRITE] Processing single string insert - field: '{}', value: '{}', timestamp: {}, shard: {}",
                fieldName, value, point.timestamp, shard);
            LOG_INSERT_PATH(timestar::http_log, debug, "[WRITE] String series key: '{}'", insert.seriesKey());

            shardStringInserts[shard].push_back(std::move(insert));
            metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, *sharedTags});

        } else if (std::holds_alternative<int64_t>(fieldValue)) {
            int64_t value = std::get<int64_t>(fieldValue);
            TimeStarInsert<int64_t> insert(point.measurement, fieldName);
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
        if (shardDoubleInserts[shard].empty() && shardBoolInserts[shard].empty() && shardStringInserts[shard].empty() &&
            shardIntegerInserts[shard].empty()) [[likely]] {
            continue;
        }

        // Move this shard's inserts directly from the pre-sized vectors
        auto doubles = std::move(shardDoubleInserts[shard]);
        auto bools = std::move(shardBoolInserts[shard]);
        auto strings = std::move(shardStringInserts[shard]);
        auto integers = std::move(shardIntegerInserts[shard]);

        shardFutures.push_back(engineSharded->invoke_on(
            shard,
            [doubles = std::move(doubles), bools = std::move(bools), strings = std::move(strings),
             integers = std::move(integers)](Engine& engine) mutable -> seastar::future<> {
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
            }));
    }

    // Wait for all shard inserts to complete in parallel
    size_t numShards = shardFutures.size();
    co_await seastar::when_all_succeed(std::move(shardFutures));

    // Synchronous metadata indexing: await completion so metadata is queryable
    // before the write response is returned (no write hole).
    if (!metaOps.empty()) {
        co_await engineSharded->local().indexMetadataSync(std::move(metaOps));
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);
    LOG_INSERT_PATH(timestar::http_log, info, "[PERF] [HTTP] processWritePoint: {}us ({} fields, {} shards)",
                    total_duration.count(), point.fields.size(), numShards);
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

std::string HttpWriteHandler::createSuccessResponse(int64_t pointsWritten) {
    // Create JSON object directly
    auto response = glz::obj{"status", "success", "points_written", pointsWritten};

    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","message":"Failed to serialize success response"})";
    }
    return buffer;
}

std::string HttpWriteHandler::createPartialFailureResponse(int64_t pointsWritten, int64_t failedWrites,
                                                           const std::vector<std::string>& errors) {
    auto response = glz::obj{"status", "partial", "points_written", pointsWritten, "failed_writes", failedWrites,
                             "errors", errors};

    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return R"({"status":"error","message":"Failed to serialize partial failure response"})";
    }
    return buffer;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpWriteHandler::handleWrite(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();

    try {
        // Read the complete request body — move from request to avoid copying
        std::string body;
        if (!req->content.empty()) {
            body = std::move(req->content);
        }

        // Read from stream if available, checking size incrementally
        if (req->content_stream) {
            while (!req->content_stream->eof()) {
                auto data = co_await req->content_stream->read();
                body.append(data.get(), data.size());
                if (body.size() > maxWriteBodySize()) {
                    break;
                }
            }
        }

        if (body.size() > maxWriteBodySize()) {
            rep->set_status(seastar::http::reply::status_type::payload_too_large);
            rep->_content = createErrorResponse("Request body too large (max " +
                                                std::to_string(maxWriteBodySize() / (1024 * 1024)) + "MB)");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }

        // Validate Content-Type if explicitly set
        {
            auto ct = req->get_header("Content-Type");
            std::string ctStr(ct.data(), ct.size());
            if (!ctStr.empty() && !ctStr.starts_with("application/json")) {
                rep->set_status(seastar::http::reply::status_type::unsupported_media_type);
                rep->_content = createErrorResponse("Content-Type must be application/json");
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }
        }

        if (body.empty()) {
            rep->set_status(seastar::http::reply::status_type::bad_request);
            rep->_content = createErrorResponse("Empty request body");
            rep->add_header("Content-Type", "application/json");
            co_return rep;
        }

        LOG_INSERT_PATH(timestar::http_log, debug, "Received write request: {} bytes", body.size());

        int64_t pointsWritten = 0;
#if TIMESTAR_LOG_INSERT_PATH
        auto batchStartTime = std::chrono::steady_clock::now();
#endif

        // Capture a single wall-clock timestamp for the entire request.
        // All write points that lack an explicit timestamp will share this value,
        // eliminating up to N redundant now() calls in a batch of N points.
        const uint64_t defaultTimestampNs = currentNanosTimestamp();

        // ─── Fast path: single write with all-double fields ───
        // For large payloads (>256 bytes) that aren't batch writes, try parsing
        // directly into typed vectors, bypassing the json_value_t DOM entirely.
        // For a 10K-timestamp × 10-field batch, this avoids ~110K DOM node
        // allocations and roughly halves the JSON parsing cost.
        bool fastPathHandled = false;
        if (body.size() > 256) {
            // Quick check: skip fast path for batch writes (have "writes" key).
            // We must search the entire body, not just the first N bytes,
            // because the "writes" key can appear at any position depending
            // on JSON key ordering.
            auto writesPos = body.find("\"writes\"");
            if (writesPos == std::string::npos) {
                FastDoubleWritePoint fwp;
                auto fast_err = glz::read_json(fwp, body);
                if (!fast_err && !fwp.measurement.empty() && !fwp.fields.empty()) {
                    // Fast path succeeded - yield after CPU-heavy parse
                    co_await seastar::coroutine::maybe_yield();

                    MultiWritePoint mwp;
                    if (buildMWPFromFastPath(fwp, defaultTimestampNs, mwp)) {
                        std::string error;
                        if (!validateArraySizes(mwp, error)) {
                            throw std::invalid_argument(error);
                        }

                        pointsWritten = static_cast<int64_t>(mwp.timestamps.size()) *
                                        static_cast<int64_t>(mwp.fields.size());

                        auto writeResult = co_await processMultiWritePoint(mwp);

                        if (!writeResult.metaOps.empty()) {
                            co_await engineSharded->local().indexMetadataSync(std::move(writeResult.metaOps));
                        }
                        fastPathHandled = true;
                    }
                }
            }
        }

        // ─── Standard DOM path (batch writes + mixed-type single writes) ───
        if (!fastPathHandled) {
            // Parse JSON using Glaze with u64 number mode to preserve integer precision.
            json_value_t doc{};
            auto parse_error = glz::read_json(doc, body);

            if (parse_error) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(parse_error)));
                rep->add_header("Content-Type", "application/json");
                co_return rep;
            }

            // Yield after CPU-heavy DOM parse to prevent cascading reactor stalls
            co_await seastar::coroutine::maybe_yield();

            if (doc.is_object()) {
                auto& obj = doc.get<json_value_t::object_t>();

                if (obj.contains("writes")) {
                    auto& writes = obj["writes"];
                    if (!writes.is_array()) {
                        throw std::invalid_argument("'writes' field must be a JSON array");
                    }
                    if (writes.is_array()) {
                        auto& writes_array = writes.get<json_value_t::array_t>();

                        constexpr size_t MAX_BATCH_WRITES = 100000;
                        if (writes_array.size() > MAX_BATCH_WRITES) {
                            rep->set_status(seastar::http::reply::status_type::bad_request);
                            rep->_content = createErrorResponse(
                                "Batch too large: " + std::to_string(writes_array.size()) +
                                " writes exceeds maximum of " + std::to_string(MAX_BATCH_WRITES));
                            rep->add_header("Content-Type", "application/json");
                            co_return rep;
                        }

                        LOG_INSERT_PATH(timestar::http_log, info, "[BATCH] Processing batch with {} writes",
                                        writes_array.size());

#if TIMESTAR_LOG_INSERT_PATH
                        auto coalesceStartTime = std::chrono::steady_clock::now();
#endif
                        auto coalescedWrites = coalesceWrites(writes_array, defaultTimestampNs);
#if TIMESTAR_LOG_INSERT_PATH
                        auto coalesceEndTime = std::chrono::steady_clock::now();
                        auto coalesceDuration =
                            std::chrono::duration_cast<std::chrono::microseconds>(coalesceEndTime - coalesceStartTime);
                        LOG_INSERT_PATH(timestar::http_log, info,
                                        "[BATCH] Coalesced {} writes into {} MultiWritePoints ({}μs)",
                                        writes_array.size(), coalescedWrites.size(), coalesceDuration.count());
#endif

                        // Pre-compute per-MWP point counts so we can subtract on failure.
                        std::vector<int64_t> perMwpPoints;
                        perMwpPoints.reserve(coalescedWrites.size());
                        for (const auto& mwp : coalescedWrites) {
                            auto pts = static_cast<int64_t>(mwp.timestamps.size()) *
                                       static_cast<int64_t>(mwp.fields.size());
                            pointsWritten += pts;
                            perMwpPoints.push_back(pts);
                            std::string error;
                            if (!validateArraySizes(mwp, error)) {
                                throw std::invalid_argument(error);
                            }
                        }

                        std::vector<seastar::future<WriteResult>> mwpFutures;
                        mwpFutures.reserve(coalescedWrites.size());
                        for (auto& mwp : coalescedWrites) {
                            mwpFutures.push_back(processMultiWritePoint(mwp));
                        }

                        auto mwpResults = co_await seastar::when_all(mwpFutures.begin(), mwpFutures.end());

                        std::vector<MetaOp> metaOps;
                        metaOps.reserve(coalescedWrites.size() * 2);
                        int64_t failedWrites = 0;
                        std::vector<std::string> writeErrors;
                        for (size_t i = 0; i < mwpResults.size(); ++i) {
                            try {
                                auto writeResult = mwpResults[i].get();
                                metaOps.insert(metaOps.end(), std::make_move_iterator(writeResult.metaOps.begin()),
                                               std::make_move_iterator(writeResult.metaOps.end()));
                            } catch (const std::exception& e) {
                                timestar::http_log.error("Error processing write: {}", e.what());
                                pointsWritten -= perMwpPoints[i];
                                ++failedWrites;
                                if (writeErrors.size() < 10) {
                                    writeErrors.emplace_back(e.what());
                                }
                            }
                        }

#if TIMESTAR_LOG_INSERT_PATH
                        size_t metaOpsCount = metaOps.size();
#endif
                        if (!metaOps.empty()) {
                            co_await engineSharded->local().indexMetadataSync(std::move(metaOps));
                        }
#if TIMESTAR_LOG_INSERT_PATH
                        LOG_INSERT_PATH(timestar::http_log, info,
                                        "[METADATA] Batch: indexed {} unique series synchronously", metaOpsCount);
#endif

                        // Report partial failure if some writes failed
                        if (failedWrites > 0) {
                            rep->set_status(seastar::http::reply::status_type::ok);
                            rep->_content = createPartialFailureResponse(pointsWritten, failedWrites, writeErrors);
                            rep->add_header("Content-Type", "application/json");
                            co_return rep;
                        }
                    }
                } else {
                    MultiWritePoint mwp = parseMultiWritePoint(doc, defaultTimestampNs);
                    std::string error;
                    if (!validateArraySizes(mwp, error)) {
                        throw std::invalid_argument(error);
                    }

                    pointsWritten = static_cast<int64_t>(mwp.timestamps.size()) *
                                    static_cast<int64_t>(mwp.fields.size());

                    auto writeResult = co_await processMultiWritePoint(mwp);

#if TIMESTAR_LOG_INSERT_PATH
                    size_t metaOpsCount = writeResult.metaOps.size();
#endif
                    if (!writeResult.metaOps.empty()) {
                        co_await engineSharded->local().indexMetadataSync(std::move(writeResult.metaOps));
                    }
#if TIMESTAR_LOG_INSERT_PATH
                    LOG_INSERT_PATH(timestar::http_log, info,
                                    "[METADATA] Single write: indexed {} unique series synchronously", metaOpsCount);
#endif
                }
            } else {
                throw std::invalid_argument("Request body must be a JSON object");
            }
        }

        // Success response
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = createSuccessResponse(pointsWritten);
        rep->add_header("Content-Type", "application/json");

    } catch (const seastar::gate_closed_exception&) {
        // Insert gate closed — server is shutting down. Return 503 so clients
        // know to retry against another node or after restart.
        ++engineSharded->local().metrics().insert_errors_total;
        rep->set_status(seastar::http::reply::status_type::service_unavailable);
        rep->_content = createErrorResponse("Server is shutting down");
        rep->add_header("Content-Type", "application/json");
    } catch (const std::invalid_argument& e) {
        // Client input validation error — 400 Bad Request
        ++engineSharded->local().metrics().insert_errors_total;
        timestar::http_log.debug("Write validation error: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::bad_request);
        rep->_content = createErrorResponse(e.what());
        rep->add_header("Content-Type", "application/json");
    } catch (const std::exception& e) {
        ++engineSharded->local().metrics().insert_errors_total;
        timestar::http_log.error("Error handling write request: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        rep->_content = createErrorResponse("Internal server error");
        rep->add_header("Content-Type", "application/json");
    }

    co_return rep;
}

void HttpWriteHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    auto* handler = new seastar::httpd::function_handler(
        timestar::wrapWithAuth(authToken,
            [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
                -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleWrite(std::move(req)); }),
        "json");

    r.add(seastar::httpd::operation_type::POST, seastar::httpd::url("/write"), handler);

    timestar::http_log.info("Registered HTTP write endpoint at /write{}",
                            authToken.empty() ? "" : " (auth required)");
}