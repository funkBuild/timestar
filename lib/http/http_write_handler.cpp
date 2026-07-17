#include "http_write_handler.hpp"

#include "content_negotiation.hpp"
#include "http_auth.hpp"
#include "http_error.hpp"
#include "http_routes.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "placement_table.hpp"
#include "proto_converters.hpp"
#include "proto_write_fast_path.hpp"
#include "series_key.hpp"

#include <boost/iterator/counting_iterator.hpp>

#include <tsl/robin_map.h>
#include <tsl/robin_set.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <numeric>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <stdexcept>
#include <unordered_set>

using namespace seastar;
using namespace httpd;
using timestar::buildSeriesKey;

namespace timestar::http {

// Glaze-compatible structures for JSON parsing
struct GlazeWritePoint {
    std::string measurement;
    std::map<std::string, std::string> tags;
    json_value_t fields;  // Use u64 mode for integer precision
    std::optional<uint64_t> timestamp;
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

}  // namespace timestar::http

template <>
struct glz::meta<timestar::http::GlazeWritePoint> {
    using T = timestar::http::GlazeWritePoint;
    static constexpr auto value =
        object("measurement", &T::measurement, "tags", &T::tags, "fields", &T::fields, "timestamp", &T::timestamp);
};

template <>
struct glz::meta<timestar::http::FastDoubleWritePoint> {
    using T = timestar::http::FastDoubleWritePoint;
    static constexpr auto value = object("measurement", &T::measurement, "tags", &T::tags, "timestamps", &T::timestamps,
                                         "timestamp", &T::timestamp, "fields", &T::fields);
};

namespace timestar::http {

// File-scope shard-local cache of series IDs that have already been indexed.
// After the first batch, subsequent writes for the same series skip the
// cross-shard metadata RPC entirely. Shared by both JSON and protobuf write paths.
// Two-generation approach: on overflow, previous generation is discarded and
// current becomes previous, avoiding cliff-clearing all entries at once.
static constexpr size_t MAX_KNOWN_SERIES_CACHE = 500'000;
static thread_local tsl::robin_set<SeriesId128, SeriesId128::Hash> knownSeriesCurrent;
static thread_local tsl::robin_set<SeriesId128, SeriesId128::Hash> knownSeriesPrevious;

static bool knownSeriesContains(const SeriesId128& id) {
    return knownSeriesCurrent.find(id) != knownSeriesCurrent.end() ||
           knownSeriesPrevious.find(id) != knownSeriesPrevious.end();
}

static void knownSeriesInsert(const SeriesId128& id) {
    if (knownSeriesCurrent.size() > MAX_KNOWN_SERIES_CACHE) [[unlikely]] {
        // Rotate generations: discard oldest, promote current to previous
        knownSeriesPrevious = std::move(knownSeriesCurrent);
        knownSeriesCurrent = tsl::robin_set<SeriesId128, SeriesId128::Hash>{};
    }
    knownSeriesCurrent.insert(id);
}

// Remove series from the known-series cache after a FAILED metadata indexing
// attempt.  knownSeriesInsert() runs optimistically while building MetaOps;
// if indexMetadataSync later throws, leaving the entries cached would make a
// RETRY of the same write silently skip metadata indexing ("succeed") while
// the series never appears in /measurements.  A failed write must leave no
// trace in this cache.
static void unpoisonKnownSeries(const std::vector<MetadataOp>& ops) {
    for (const auto& op : ops) {
        if (!op.seriesId.isZero()) {
            knownSeriesCurrent.erase(op.seriesId);
            knownSeriesPrevious.erase(op.seriesId);
        }
    }
}

// -----------------------------------------------------------------------
// Identifier limits — mirror the hard limits enforced by the index encoder
// (lib/index/key_encoding.cpp encodeSeriesMetadata).  Enforcing them up
// front turns what used to be a 500 ("SeriesMetadata has suspiciously long
// strings") after the data write into a clean 400 BEFORE any state changes.
// -----------------------------------------------------------------------
static constexpr size_t MAX_MEASUREMENT_LEN = 10000;
static constexpr size_t MAX_FIELD_NAME_LEN = 10000;
static constexpr size_t MAX_TAG_LEN = 1000;
static constexpr size_t MAX_TAG_COUNT = 1000;

static bool validateIdentifierLimits(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                     std::string& error) {
    if (measurement.size() > MAX_MEASUREMENT_LEN) {
        error = "Measurement name too long: " + std::to_string(measurement.size()) + " bytes exceeds maximum of " +
                std::to_string(MAX_MEASUREMENT_LEN);
        return false;
    }
    if (tags.size() > MAX_TAG_COUNT) {
        error = "Too many tags: " + std::to_string(tags.size()) + " exceeds maximum of " + std::to_string(MAX_TAG_COUNT);
        return false;
    }
    for (const auto& [k, v] : tags) {
        if (k.size() > MAX_TAG_LEN || v.size() > MAX_TAG_LEN) {
            error = "Tag key/value too long: exceeds maximum of " + std::to_string(MAX_TAG_LEN) + " bytes";
            return false;
        }
    }
    return true;
}

static bool validateFieldNameLimit(const std::string& fieldName, std::string& error) {
    if (fieldName.size() > MAX_FIELD_NAME_LEN) {
        error = "Field name too long: " + std::to_string(fieldName.size()) + " bytes exceeds maximum of " +
                std::to_string(MAX_FIELD_NAME_LEN);
        return false;
    }
    return true;
}

// Index metadata for the given ops; on failure, remove the ops' series from
// the known-series cache (see unpoisonKnownSeries) before rethrowing, so a
// retry re-attempts metadata indexing instead of silently skipping it.
static seastar::future<> syncMetadataUnpoisonOnFailure(seastar::sharded<Engine>* engineSharded,
                                                       std::vector<MetadataOp> ops) {
    if (ops.empty()) {
        co_return;
    }
    std::vector<SeriesId128> ids;
    ids.reserve(ops.size());
    for (const auto& op : ops) {
        ids.push_back(op.seriesId);
    }
    try {
        co_await engineSharded->local().indexMetadataSync(std::move(ops));
    } catch (...) {
        for (const auto& id : ids) {
            if (!id.isZero()) {
                knownSeriesCurrent.erase(id);
                knownSeriesPrevious.erase(id);
            }
        }
        throw;
    }
}

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

// ---------------------------------------------------------------------------
// Optional explicit field typing.  A write point may carry
//
//   "field_types": {"<field>": "float" | "int" | "bool" | "string"}
//
// which overrides JSON token-shape type detection for the named fields.
// Without it, the stored type is inferred from how the number is written
// ("10" -> integer, "10.0" -> float) — a problem for serializers that emit
// whole-number floats as integer tokens (JavaScript's JSON.stringify(10.0)
// produces "10").  Accepted aliases: double==float, integer==int64==int,
// boolean==bool.
// ---------------------------------------------------------------------------

static TSMValueType parseFieldTypeName(const std::string& fieldName, const std::string& typeName) {
    if (typeName == "float" || typeName == "double")
        return TSMValueType::Float;
    if (typeName == "int" || typeName == "integer" || typeName == "int64")
        return TSMValueType::Integer;
    if (typeName == "bool" || typeName == "boolean")
        return TSMValueType::Boolean;
    if (typeName == "string")
        return TSMValueType::String;
    throw std::invalid_argument("Unknown type '" + typeName + "' in field_types for field '" + fieldName +
                                "' (expected float, int, bool, or string)");
}

// Extract the optional "field_types" object from a write-point object.
// Returns an empty map when the key is absent; throws on malformed input.
static std::map<std::string, TSMValueType> extractFieldTypes(const json_value_t::object_t& obj) {
    std::map<std::string, TSMValueType> declared;
    auto it = obj.find("field_types");
    if (it == obj.end()) {
        return declared;
    }
    if (!it->second.is_object()) {
        throw std::invalid_argument("'field_types' must be a JSON object mapping field names to type names");
    }
    for (const auto& [fieldName, typeValue] : it->second.get<json_value_t::object_t>()) {
        if (!typeValue.is_string()) {
            throw std::invalid_argument("field_types entry '" + fieldName + "' must be a string type name");
        }
        declared[fieldName] = parseFieldTypeName(fieldName, typeValue.get<std::string>());
    }
    return declared;
}

// Validate one JSON value against a declared type.  Numeric widening
// (integer token -> declared float) is allowed; a declared int accepts a
// float token only when the value is integral and fits int64.  Everything
// else must match exactly — an explicit declaration turning into silent
// coercion would defeat its purpose.
static void checkDeclaredType(TSMValueType declared, const json_value_t& v, const std::string& fieldName) {
    switch (declared) {
        case TSMValueType::Float:
            if (!v.is_number()) {
                throw std::invalid_argument("Field '" + fieldName + "' is declared float but the value is not a number");
            }
            return;
        case TSMValueType::Integer: {
            if (!v.is_number()) {
                throw std::invalid_argument("Field '" + fieldName + "' is declared int but the value is not a number");
            }
            if (v.holds<double>()) {
                const double d = v.get<double>();
                // Integral check plus int64 range: [-2^63, 2^63) in double space.
                if (!std::isfinite(d) || d != std::trunc(d) || d < -9223372036854775808.0 ||
                    d >= 9223372036854775808.0) {
                    throw std::invalid_argument("Field '" + fieldName +
                                                "' is declared int but the value is not an integral int64");
                }
            } else if (v.holds<uint64_t>() &&
                       v.get<uint64_t>() > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                throw std::invalid_argument("Field '" + fieldName + "' is declared int but the value overflows int64");
            }
            return;
        }
        case TSMValueType::Boolean:
            if (!v.is_boolean()) {
                throw std::invalid_argument("Field '" + fieldName + "' is declared bool but the value is not a boolean");
            }
            return;
        case TSMValueType::String:
            if (!v.is_string()) {
                throw std::invalid_argument("Field '" + fieldName + "' is declared string but the value is not a string");
            }
            return;
        default:
            return;
    }
}

// Reject field_types entries that name no field in this point.  A typo'd
// name silently falling back to token-shape detection would recreate the
// exact failure mode explicit typing exists to prevent.
static void requireDeclaredFieldsPresent(const std::map<std::string, TSMValueType>& declared,
                                         const json_value_t::object_t& fieldsObj) {
    for (const auto& [name, type] : declared) {
        if (fieldsObj.find(name) == fieldsObj.end()) {
            throw std::invalid_argument("field_types entry '" + name + "' has no matching field in 'fields'");
        }
    }
}

HttpWriteHandler::MultiWritePoint HttpWriteHandler::parseMultiWritePoint(const json_value_t& point,
                                                                         uint64_t defaultTimestampNs) {
    MultiWritePoint mwp;

    // Extract fields directly from the json_value_t object, avoiding a
    // serialize-then-reparse round-trip through an intermediate glz struct.

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

    // Extract tags into a local map, then wrap once in the shared_ptr that all
    // downstream consumers (fields, TimeStarInserts) will share.
    std::map<std::string, std::string> localTags;
    auto tagsIt = obj.find("tags");
    if (tagsIt != obj.end() && tagsIt->second.is_object()) {
        auto& tagsObj = tagsIt->second.get<json_value_t::object_t>();
        for (const auto& [tagKey, tagValue] : tagsObj) {
            if (tagValue.is_string()) {
                localTags[tagKey] = tagValue.get<std::string>();
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
        for (const auto& [key, value] : localTags) {
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

    mwp.tags = std::make_shared<const std::map<std::string, std::string>>(std::move(localTags));

    // Parse fields - handle both scalars and arrays
    auto fieldsIt = obj.find("fields");
    if (fieldsIt == obj.end() || !fieldsIt->second.is_object()) {
        throw std::invalid_argument("Fields must be an object");
    }
    auto& fields_obj = fieldsIt->second.get<json_value_t::object_t>();

    // Optional explicit types override token-shape detection per field.
    const auto declaredTypes = extractFieldTypes(obj);
    requireDeclaredFieldsPresent(declaredTypes, fields_obj);

    for (auto& [fieldName, fieldValue] : fields_obj) {
        // Validate field name (fast path avoids string allocation for valid names)
        if (!isValidName(fieldName)) {
            auto err = validateName(fieldName, "Field name '" + fieldName + "'");
            if (!err.empty())
                throw std::invalid_argument(err);
        }

        FieldArrays fa;

        const auto declIt = declaredTypes.find(fieldName);
        const bool hasDeclared = (declIt != declaredTypes.end());

        const json_value_t::array_t* arrPtr = nullptr;
        const json_value_t* firstValue = &fieldValue;
        if (fieldValue.is_array()) {
            auto& arr = fieldValue.get<json_value_t::array_t>();
            if (arr.empty()) {
                throw std::invalid_argument("Field array cannot be empty: " + fieldName);
            }
            arrPtr = &arr;
            firstValue = &arr[0];
        }

        // Effective type: declared (explicit) or detected from the first
        // value's JSON token shape.
        TSMValueType effectiveType;
        if (hasDeclared) {
            effectiveType = declIt->second;
        } else if (firstValue->is_number()) {
            // With generic_u64 parsing, integer JSON tokens (no decimal point)
            // are stored as int64_t/uint64_t, while floats are stored as double.
            effectiveType = (firstValue->holds<int64_t>() || firstValue->holds<uint64_t>()) ? TSMValueType::Integer
                                                                                            : TSMValueType::Float;
        } else if (firstValue->is_boolean()) {
            effectiveType = TSMValueType::Boolean;
        } else if (firstValue->is_string()) {
            effectiveType = TSMValueType::String;
        } else {
            throw std::invalid_argument(arrPtr != nullptr ? "Unsupported field array type: " + fieldName
                                                          : "Unsupported field type: " + fieldName);
        }

        // Append one value under the effective type. Declared types get the
        // stricter compatibility check (integral int64 for declared int, no
        // cross-type coercion); detected types keep the legacy per-element
        // same-kind check.
        auto appendValue = [&](const json_value_t& elem) {
            if (hasDeclared) {
                checkDeclaredType(effectiveType, elem, fieldName);
            }
            switch (effectiveType) {
                case TSMValueType::Float:
                    if (!elem.is_number())
                        throw std::invalid_argument("Mixed types in field array: " + fieldName);
                    fa.doubles.push_back(elem.as<double>());
                    break;
                case TSMValueType::Integer:
                    if (!elem.is_number())
                        throw std::invalid_argument("Mixed types in field array: " + fieldName);
                    fa.integers.push_back(elem.as<int64_t>());
                    break;
                case TSMValueType::Boolean:
                    if (!elem.is_boolean())
                        throw std::invalid_argument("Mixed types in field array: " + fieldName);
                    fa.bools.push_back(elem.get<bool>() ? 1 : 0);
                    break;
                case TSMValueType::String:
                    if (!elem.is_string())
                        throw std::invalid_argument("Mixed types in field array: " + fieldName);
                    fa.strings.push_back(elem.get<std::string>());
                    break;
                default:
                    throw std::invalid_argument("Unsupported field type: " + fieldName);
            }
        };

        switch (effectiveType) {
            case TSMValueType::Float:
                fa.type = FieldArrays::DOUBLE;
                break;
            case TSMValueType::Integer:
                fa.type = FieldArrays::INTEGER;
                break;
            case TSMValueType::Boolean:
                fa.type = FieldArrays::BOOL;
                break;
            default:
                fa.type = FieldArrays::STRING;
                break;
        }

        if (arrPtr != nullptr) {
            const size_t arrSize = arrPtr->size();
            switch (effectiveType) {
                case TSMValueType::Float:
                    fa.doubles.reserve(arrSize);
                    break;
                case TSMValueType::Integer:
                    fa.integers.reserve(arrSize);
                    break;
                case TSMValueType::Boolean:
                    fa.bools.reserve(arrSize);
                    break;
                default:
                    fa.strings.reserve(arrSize);
                    break;
            }
            for (const auto& elem : *arrPtr) {
                appendValue(elem);
            }
        } else {
            appendValue(fieldValue);
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

    // Validate names
    if (!isValidName(mwp.measurement))
        return false;
    for (const auto& [key, value] : fwp.tags) {
        if (!isValidName(key) || !isValidTagValue(value))
            return false;
    }
    mwp.tags = std::make_shared<const std::map<std::string, std::string>>(std::move(fwp.tags));

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

seastar::future<HttpWriteHandler::AggregatedTimingInfo> HttpWriteHandler::insertAllTypes(
    Engine& engine, std::vector<TimeStarInsert<double>> doubles, std::vector<TimeStarInsert<bool>> bools,
    std::vector<TimeStarInsert<std::string>> strings, std::vector<TimeStarInsert<int64_t>> integers) {
    AggregatedTimingInfo batchTiming;
    if (!doubles.empty())
        batchTiming.aggregate(co_await engine.insertBatch(std::move(doubles)));
    if (!bools.empty())
        batchTiming.aggregate(co_await engine.insertBatch(std::move(bools)));
    if (!strings.empty())
        batchTiming.aggregate(co_await engine.insertBatch(std::move(strings)));
    if (!integers.empty())
        batchTiming.aggregate(co_await engine.insertBatch(std::move(integers)));
    co_return batchTiming;
}

seastar::future<HttpWriteHandler::AggregatedTimingInfo> HttpWriteHandler::dispatchShardInserts(
    seastar::sharded<Engine>* engineSharded, unsigned shard, std::vector<TimeStarInsert<double>> doubles,
    std::vector<TimeStarInsert<bool>> bools, std::vector<TimeStarInsert<std::string>> strings,
    std::vector<TimeStarInsert<int64_t>> integers) {
    // Local shard: direct call, avoiding cross-shard message-queue overhead.
    if (shard == seastar::this_shard_id()) {
        return insertAllTypes(engineSharded->local(), std::move(doubles), std::move(bools), std::move(strings),
                              std::move(integers));
    }
    // Remote shard: copy the (moved-in) batches across via invoke_on.
    return engineSharded->invoke_on(
        shard,
        [doubles = std::move(doubles), bools = std::move(bools), strings = std::move(strings),
         integers = std::move(integers)](Engine& engine) mutable -> seastar::future<AggregatedTimingInfo> {
            return insertAllTypes(engine, std::move(doubles), std::move(bools), std::move(strings),
                                  std::move(integers));
        });
}

HttpWriteHandler::FieldArrays HttpWriteHandler::candidateToFieldArrays(CoalesceCandidate& candidate) {
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
    return fa;
}

// Configuration constants shared by the coalesce phases below.
static const size_t MAX_COALESCE_SIZE = 10000;  // Max values per field array
static const size_t MIN_COALESCE_COUNT = 2;     // Min writes needed to coalesce

// First pass of coalesceWrites (candidate-grouping phase): parse writes_array
// directly from the JSON DOM into per-series CoalesceCandidates. Writes that
// fail to parse increment entriesSkipped; every write examined increments
// totalWritesProcessed.
tsl::robin_map<std::string, HttpWriteHandler::CoalesceCandidate> HttpWriteHandler::buildCoalesceCandidates(
    const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs, size_t& entriesSkipped,
    size_t& totalWritesProcessed) {
    // Fast direct parsing approach - avoid JSON string serialization.
    // Handles both scalar and array fields in a single pass, eliminating
    // the need for callers to pre-scan fields for array detection.
    // Use robin_map for better cache locality on the hot lookup path --
    // open-addressing with Robin Hood probing keeps entries in a flat array,
    // avoiding per-bucket linked-list pointer chasing of std::unordered_map.
    tsl::robin_map<std::string, CoalesceCandidate> candidates;
    // Pre-allocate for the expected number of unique series keys.
    // A typical write point has 1-3 fields, so estimate writes * 2.
    // Cap the reservation: robin_map at load factor 0.5 with ~330-byte slots
    // would zero ~22MB for a 10K-write batch, yet unique series are almost
    // always far fewer than writes. Beyond the cap the map grows naturally.
    candidates.reserve(std::min(writes_array.size() * 2, size_t(1024)));

    LOG_INSERT_PATH(timestar::http_log, debug, "[COALESCE] Processing {} writes for coalescing", writes_array.size());

    // Helper lambda: add a single scalar value to a CoalesceCandidate.
    // Returns false if the value type is unsupported, true otherwise.
    auto addScalarToCandidate = [](CoalesceCandidate& candidate, TSMValueType valueType, uint64_t ts,
                                   const json_value_t& val) -> bool {
        candidate.timestamps.push_back(ts);
        candidate.timestampHashSum += ts;
        candidate.timestampHashXor ^= ts;
        candidate.timestampHashMul = candidate.timestampHashMul * 0x9e3779b97f4a7c15ULL + ts;
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
    // Takes a shared_ptr to the tag map so that all candidates from the
    // same write point share a single allocation (O(1) pointer copy).
    auto initCandidate = [](CoalesceCandidate& candidate, const std::string& seriesKey, const std::string& measurement,
                            std::shared_ptr<const std::map<std::string, std::string>> tags,
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
    // Takes shared_ptr<const map> by value so the shared tag map is propagated
    // without copying the underlying map data.
    auto findOrCreateCandidate = [&](const std::string& seriesKey, const std::string& measurement,
                                     std::shared_ptr<const std::map<std::string, std::string>> tags,
                                     const std::string& fieldName, TSMValueType valueType, size_t numValuesToAdd,
                                     const std::string& groupKey) -> CoalesceCandidate& {
        // Single hash/probe: try_emplace default-constructs the candidate only when the
        // key is new, replacing the previous find() + operator[] double lookup.
        auto [it, inserted] = candidates.try_emplace(seriesKey);
        if (inserted) {
            CoalesceCandidate& c = it.value();
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
                timestar::http_log.debug("Promoting integer field to float");
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

    // One-entry series memo (N4): batches are typically grouped by series, so
    // consecutive writes usually share the same measurement+tags. When the
    // current write's measurement and tags DOM match the previous write's, the
    // series-key prefix and shared tag map are reused, skipping tag map
    // construction, validation, prefix building, and the shared_ptr allocation.
    bool memoValid = false;
    std::string_view memoMeasurement;                     // views into writes_array (stable for the loop)
    const json_value_t::object_t* memoTagsObj = nullptr;  // nullptr == no/empty tags object
    std::string seriesKeyPrefix;                          // hoisted: reused capacity across writes
    std::shared_ptr<const std::map<std::string, std::string>> sharedTags;

    // DOM-to-DOM tag-object equality (string-valued tags only; any non-string
    // tag value falls back to the slow path).
    auto sameTagsObj = [](const json_value_t::object_t* a, const json_value_t::object_t* b) -> bool {
        const size_t na = a ? a->size() : 0;
        const size_t nb = b ? b->size() : 0;
        if (na != nb)
            return false;
        if (na == 0)
            return true;
        auto ia = a->begin();
        auto ib = b->begin();
        for (; ia != a->end(); ++ia, ++ib) {
            if (ia->first != ib->first)
                return false;
            if (!ia->second.is_string() || !ib->second.is_string())
                return false;  // conservative: force slow path
            if (ia->second.get<std::string>() != ib->second.get<std::string>())
                return false;
        }
        return true;
    };

    std::string seriesKey;  // hoisted: reused capacity across fields/writes

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
            const std::string& measurement = measurementIt->second.get<std::string>();

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

            // Locate the tags DOM object (nullptr when absent / not an object).
            const json_value_t::object_t* tagsObjPtr = nullptr;
            {
                auto tagsIt = writeObj.find("tags");
                if (tagsIt != writeObj.end() && tagsIt->second.is_object()) {
                    tagsObjPtr = &tagsIt->second.get<json_value_t::object_t>();
                }
            }

            // Memo hit: same measurement + identical tags as the previous
            // write — reuse seriesKeyPrefix and sharedTags as-is.
            const bool memoHit = memoValid && memoMeasurement == measurement && sameTagsObj(tagsObjPtr, memoTagsObj);
            if (!memoHit) {
                memoValid = false;  // invalidate while rebuilding

                // Extract tags into a local map, validate, then wrap in a
                // shared_ptr so all CoalesceCandidates from this write point
                // share a single allocation (pointer copy instead of map copy).
                std::map<std::string, std::string> localTags;
                if (tagsObjPtr != nullptr) {
                    for (const auto& [tagKey, tagValue] : *tagsObjPtr) {
                        if (tagValue.is_string()) {
                            // Guard with the allocation-free predicates; build the error-context
                            // string only on the rare invalid path (matches parseMultiWritePoint).
                            if (!isValidName(tagKey)) [[unlikely]]
                                throw std::invalid_argument(validateName(tagKey, "Tag key '" + tagKey + "'"));
                            auto val = tagValue.get<std::string>();
                            if (!isValidTagValue(val)) [[unlikely]]
                                throw std::invalid_argument(validateTagValue(val, "Tag value for '" + tagKey + "'"));
                            localTags[tagKey] = std::move(val);
                        }
                    }
                }

                // Pre-build measurement+tags prefix for series key construction (once per write point)
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
                // this write point will share this single allocation via shared_ptr,
                // and the same allocation flows through MultiWritePoint into the
                // TimeStarInserts (which may cross shard boundaries).
                sharedTags = std::make_shared<const std::map<std::string, std::string>>(std::move(localTags));

                // Prefix + tags fully built: record the memo for the next write.
                memoMeasurement = measurement;
                memoTagsObj = tagsObjPtr;
                memoValid = true;
            }

            // Extract fields and process each field - handles both scalar and array values
            auto fieldsIt = writeObj.find("fields");
            if (fieldsIt == writeObj.end() || !fieldsIt->second.is_object())
                continue;

            auto& fieldsObj = fieldsIt->second.get<json_value_t::object_t>();

            // Optional explicit types override token-shape detection per field.
            // Malformed field_types throws, so this write is skipped and counted
            // like any other invalid batch entry.
            const auto declaredTypes = extractFieldTypes(writeObj);
            requireDeclaredFieldsPresent(declaredTypes, fieldsObj);

            for (const auto& [fieldName, fieldValue] : fieldsObj) {
                // Validate field name (guard with the allocation-free predicate; build the
                // error context only on the invalid path).
                if (!isValidName(fieldName)) [[unlikely]]
                    throw std::invalid_argument(validateName(fieldName, "Field name '" + fieldName + "'"));

                // Build series key (hoisted buffer: capacity reused across fields)
                seriesKey.reserve(seriesKeyPrefix.length() + fieldName.length() + 1);
                seriesKey = seriesKeyPrefix;
                seriesKey += " ";
                seriesKey += fieldName;

                const auto declTypeIt = declaredTypes.find(fieldName);
                const bool hasDeclaredType = (declTypeIt != declaredTypes.end());

                if (fieldValue.is_array()) {
                    // Array field - expand all elements into the candidate
                    auto& arr = fieldValue.get<json_value_t::array_t>();
                    if (arr.empty()) {
                        throw std::invalid_argument("Field array cannot be empty: " + fieldName);
                    }

                    // Declared type (optional "field_types") overrides detection
                    // from the first element's token shape.
                    TSMValueType valueType;
                    if (hasDeclaredType) {
                        valueType = declTypeIt->second;
                    } else if (arr[0].is_number()) {
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
                        // Declared types get the stricter compatibility check
                        // (validated against the DECLARED type even if the
                        // candidate was promoted Integer->Float).
                        if (hasDeclaredType) {
                            checkDeclaredType(declTypeIt->second, elem, fieldName);
                        }
                        candidate.timestamps.push_back(fieldTimestamps[i]);
                        candidate.timestampHashSum += fieldTimestamps[i];
                        candidate.timestampHashXor ^= fieldTimestamps[i];
                        candidate.timestampHashMul =
                            candidate.timestampHashMul * 0x9e3779b97f4a7c15ULL + fieldTimestamps[i];
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
                    // Scalar field - determine type and add single value.
                    // Declared type (optional "field_types") overrides detection.
                    TSMValueType valueType;
                    if (hasDeclaredType) {
                        valueType = declTypeIt->second;
                        checkDeclaredType(valueType, fieldValue, fieldName);
                    } else if (fieldValue.is_number()) {
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
            ++entriesSkipped;
            LOG_INSERT_PATH(timestar::http_log, debug, "[COALESCE] Failed to parse write: {}", e.what());
            continue;
        }
    }

    return candidates;
}

// Second pass of coalesceWrites (field-array assembly phase): convert the
// CoalesceCandidates into MultiWritePoint objects, merging candidates that
// share a group key and compatible timestamps into multi-field points.
// Candidates below MIN_COALESCE_COUNT are emitted individually and counted
// in individualCount. Value vectors are moved out of the candidates.
std::vector<HttpWriteHandler::MultiWritePoint> HttpWriteHandler::assembleCoalescedPoints(
    tsl::robin_map<std::string, CoalesceCandidate>& candidates, size_t& individualCount) {
    std::vector<MultiWritePoint> result;

    // Group by measurement+tags efficiently (only for coalescing)
    tsl::robin_map<std::string, std::vector<std::string>> grouped;  // groupKey -> seriesKeys
    tsl::robin_set<std::string> processedSeriesKeys;                // Track which candidates were coalesced

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
        // Three independent hash dimensions (sum, XOR, multiplicative) make accidental
        // collisions astronomically unlikely for real nanosecond timestamps.
        for (size_t i = 1; i < seriesKeys.size(); i++) {
            auto& candidate = candidates[seriesKeys[i]];
            if (candidate.timestamps.size() != firstCandidate.timestamps.size() ||
                candidate.timestampHashSum != firstCandidate.timestampHashSum ||
                candidate.timestampHashXor != firstCandidate.timestampHashXor ||
                candidate.timestampHashMul != firstCandidate.timestampHashMul) {
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
                mwp.tags = candidate.sharedTags;  // pointer copy, not map copy
                mwp.timestamps = std::move(candidate.timestamps);

                mwp.fields[candidate.fieldName] = candidateToFieldArrays(candidate);
                result.push_back(std::move(mwp));
                processedSeriesKeys.insert(seriesKey);
            }
            continue;
        }

        MultiWritePoint mwp;
        mwp.measurement = firstCandidate.measurement;
        mwp.tags = firstCandidate.sharedTags;  // pointer copy, not map copy
        mwp.timestamps = std::move(firstCandidate.timestamps);

        // NOTE: Don't sort timestamps here because field arrays are in the same original order
        // Sorting would misalign timestamps with field values and cause memory corruption

        for (const auto& seriesKey : seriesKeys) {
            auto& candidate = candidates[seriesKey];

            mwp.fields[candidate.fieldName] = candidateToFieldArrays(candidate);
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
        mwp.tags = candidate.sharedTags;  // pointer copy, not map copy
        mwp.timestamps = std::move(candidate.timestamps);

        mwp.fields[candidate.fieldName] = candidateToFieldArrays(candidate);
        result.push_back(std::move(mwp));
    }

    return result;
}

std::vector<HttpWriteHandler::MultiWritePoint> HttpWriteHandler::coalesceWrites(
    const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs, size_t& entriesSkippedOut) {
#if TIMESTAR_LOG_INSERT_PATH
    auto start_coalesce = std::chrono::high_resolution_clock::now();
#endif

    // Phase 1: group writes into per-series CoalesceCandidates.
    size_t totalWritesProcessed = 0;
    size_t entriesSkipped = 0;
    auto candidates = buildCoalesceCandidates(writes_array, defaultTimestampNs, entriesSkipped, totalWritesProcessed);

    entriesSkippedOut = entriesSkipped;
    if (entriesSkipped > 0) {
        timestar::http_log.warn("[COALESCE] Skipped {} of {} entries due to parse errors", entriesSkipped,
                                writes_array.size());
    }

    // Phase 2: convert candidates to MultiWritePoint objects.
    size_t individualCount = 0;
    auto result = assembleCoalescedPoints(candidates, individualCount);

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
    // Identifier limits first (measurement/tag lengths, tag count) — these
    // mirror the index encoder's hard limits so oversized identifiers are
    // rejected with a 400 BEFORE any data or metadata state changes.
    static const std::map<std::string, std::string> kNoTags;
    if (!validateIdentifierLimits(point.measurement, point.tags ? *point.tags : kNoTags, error)) {
        return false;
    }

    size_t expectedSize = point.timestamps.size();

    for (const auto& [fieldName, fieldArray] : point.fields) {
        if (!validateFieldNameLimit(fieldName, error)) {
            return false;
        }
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

void HttpWriteHandler::accumulateMultiWritePoint(MultiWritePoint& point, BatchAccumulator& acc) {
    // No artificial batch splitting -- the WAL enforces its own 16 MiB per-segment
    // limit (MAX_WAL_SIZE) and will signal rollover if a single insert exceeds it.
    // Splitting here only adds redundant copies and sequential cross-shard roundtrips.

    LOG_INSERT_PATH(timestar::http_log, debug, "[WRITE] Accumulating MultiWritePoint: {} timestamps × {} fields",
                    point.timestamps.size(), point.fields.size());

    // Keep arrays intact - create ONE TimeStarInsert per field with ALL timestamps/values,
    // grouped by (shard, type) into the request-level accumulator vectors.
    std::vector<std::vector<TimeStarInsert<double>>>& shardDoubleInserts = acc.shardDoubles;
    std::vector<std::vector<TimeStarInsert<bool>>>& shardBoolInserts = acc.shardBools;
    std::vector<std::vector<TimeStarInsert<std::string>>>& shardStringInserts = acc.shardStrings;
    std::vector<std::vector<TimeStarInsert<int64_t>>>& shardIntegerInserts = acc.shardIntegers;
    std::unordered_set<SeriesId128, SeriesId128::Hash>& seenMF = acc.seenMF;
    std::vector<MetaOp>& metaOps = acc.metaOps;

    // Process each field - create ONE insert with ALL timestamps and values.
    // Use shared_ptr for tags and timestamps so that all field inserts from
    // the same multi-field point share a single allocation instead of making
    // N-1 copies (for N fields). This is safe across Seastar shard boundaries
    // because shared_ptr uses atomic refcounting. The tag map is already
    // shared: producers wrap it once and we just take the pointer (no deep
    // copy + re-wrap per point).
    auto sharedTags = point.tags ? std::move(point.tags) : std::make_shared<const std::map<std::string, std::string>>();
    auto sharedTimestamps = std::make_shared<const std::vector<uint64_t>>(std::move(point.timestamps));

    // Timestamp range for MetadataOp day-bitmap coverage (first batch of a
    // new series). Computed lazily — metaOps are only emitted for unknown
    // series, so the common all-known path never pays the minmax scan.
    uint64_t mwpMinTs = 0, mwpMaxTs = 0;
    bool mwpTsRangeComputed = false;
    auto tsRange = [&]() -> std::pair<uint64_t, uint64_t> {
        if (!mwpTsRangeComputed && !sharedTimestamps->empty()) {
            auto [mn, mx] = std::minmax_element(sharedTimestamps->begin(), sharedTimestamps->end());
            mwpMinTs = *mn;
            mwpMaxTs = *mx;
            mwpTsRangeComputed = true;
        }
        return {mwpMinTs, mwpMaxTs};
    };

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
                // knownSeries is a shard-local persistent cache that avoids redundant
                // cross-shard metadata RPCs for series we've already indexed.
                {
                    if (seenMF.insert(seriesId).second) {
                        if (!knownSeriesContains(seriesId)) {
                            knownSeriesInsert(seriesId);
                            auto [mnTs, mxTs] = tsRange();
                            metaOps.push_back(MetaOp{TSMValueType::Float, point.measurement, fieldName, *sharedTags,
                                                     mnTs, mxTs, seriesId});
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
                        if (!knownSeriesContains(seriesId)) {
                            knownSeriesInsert(seriesId);
                            auto [mnTs, mxTs] = tsRange();
                            metaOps.push_back(MetaOp{TSMValueType::Boolean, point.measurement, fieldName, *sharedTags,
                                                     mnTs, mxTs, seriesId});
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
                        if (!knownSeriesContains(seriesId)) {
                            knownSeriesInsert(seriesId);
                            auto [mnTs, mxTs] = tsRange();
                            metaOps.push_back(MetaOp{TSMValueType::String, point.measurement, fieldName, *sharedTags,
                                                     mnTs, mxTs, seriesId});
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
                        if (!knownSeriesContains(seriesId)) {
                            knownSeriesInsert(seriesId);
                            auto [mnTs, mxTs] = tsRange();
                            metaOps.push_back(MetaOp{TSMValueType::Integer, point.measurement, fieldName, *sharedTags,
                                                     mnTs, mxTs, seriesId});
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

        // Reached only when a non-empty insert was pushed for this field
        // (empty-value fields `continue` inside the switch). Points routed to
        // this shard: one per timestamp for this field — matches the
        // request-level accounting of timestamps × fields per MultiWritePoint.
        acc.shardPoints[shard] += static_cast<int64_t>(sharedTimestamps->size());
    }

    // Metadata ops accumulate in `acc` for request-level deduplication and indexing
}

seastar::future<HttpWriteHandler::WriteResult> HttpWriteHandler::processMultiWritePoint(MultiWritePoint& point) {
#if TIMESTAR_LOG_INSERT_PATH
    auto start_total = std::chrono::high_resolution_clock::now();
#endif

    const size_t shardCount = seastar::smp::count;
    BatchAccumulator acc(shardCount);
    accumulateMultiWritePoint(point, acc);

    // Dispatch to all shards in parallel - inserts are already batched per field with all timestamps
    std::vector<seastar::future<AggregatedTimingInfo>> shardFutures;
    shardFutures.reserve(shardCount);

    for (size_t shard = 0; shard < shardCount; ++shard) {
        // Skip shards with no work
        if (acc.shardEmpty(shard)) {
            continue;
        }

        shardFutures.push_back(dispatchShardInserts(
            engineSharded, shard, std::move(acc.shardDoubles[shard]), std::move(acc.shardBools[shard]),
            std::move(acc.shardStrings[shard]), std::move(acc.shardIntegers[shard])));
    }

    // Wait for all shard operations to complete in parallel
    auto shardTimings = co_await seastar::when_all_succeed(std::move(shardFutures));

    // Aggregate all timing results
    AggregatedTimingInfo aggregatedTiming;
    for (const auto& timing : shardTimings) {
        aggregatedTiming.aggregate(timing);
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto end_total = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_total - start_total);
    LOG_INSERT_PATH(timestar::http_log, info, "[PERF] [HTTP] processMultiWritePoint total: {}μs",
                    total_duration.count());
#endif

    co_return WriteResult{std::move(aggregatedTiming), std::move(acc.metaOps)};
}

std::string HttpWriteHandler::createErrorResponse(const std::string& error) {
    // Canonical error shape: the message is carried in "error" (and mirrored in
    // "message").  Previously this handler emitted only "message", diverging
    // from every other endpoint.
    return timestar::http::jsonError(error);
}

std::string HttpWriteHandler::createSuccessResponse(int64_t pointsWritten) {
    // Create JSON object directly
    auto response = glz::obj{"status", "success", "points_written", pointsWritten};

    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return timestar::http::jsonError("Failed to serialize success response");
    }
    return buffer;
}

std::string HttpWriteHandler::createPartialFailureResponse(int64_t pointsWritten, int64_t failedWrites,
                                                           const std::vector<std::string>& errors) {
    auto response =
        glz::obj{"status", "partial", "points_written", pointsWritten, "failed_writes", failedWrites, "errors", errors};

    std::string buffer;
    auto ec = glz::write_json(response, buffer);
    if (ec) {
        return timestar::http::jsonError("Failed to serialize partial failure response");
    }
    return buffer;
}

// handleWrite phase 1: read the request body and enforce size limits.
// Parses directly from the request's body buffer. `req.content` is a
// seastar::sstring; assigning it into a std::string is a cross-type
// COPY of the entire body (the std::move is inert), ~2MB per bench
// request. A view is zero-copy: `req` outlives this coroutine and
// sstring data is null-terminated (required by Glaze's default opts).
// On success returns true with `body` viewing the bytes (owned by
// req.content or bodyStorage — caller locals that outlive this
// parent-awaited coroutine). On failure (payload too large / empty body)
// returns false with the error response fully assembled in `rep`.
seastar::future<bool> HttpWriteHandler::readWriteBody(seastar::http::request& req, std::string& bodyStorage,
                                                      std::string_view& body, timestar::http::WireFormat resFmt,
                                                      seastar::http::reply& rep) {
    if (!req.content.empty()) {
        body = std::string_view(req.content.data(), req.content.size());
    } else if (req.content_stream) {
        // Pre-reserve from Content-Length to avoid repeated reallocations
        // during streaming reads. Cap at maxWriteBodySize to prevent abuse.
        if (req.content_length > 0 && req.content_length <= maxWriteBodySize()) {
            bodyStorage.reserve(req.content_length);
        }
        while (!req.content_stream->eof()) {
            auto data = co_await req.content_stream->read();
            bodyStorage.append(data.get(), data.size());
            if (bodyStorage.size() > maxWriteBodySize()) {
                break;
            }
        }
        body = bodyStorage;
    }

    if (body.size() > maxWriteBodySize()) {
        rep.set_status(seastar::http::reply::status_type::payload_too_large);
        if (timestar::http::isProtobuf(resFmt)) {
            rep._content = timestar::proto::formatWriteResponse(
                "error", 0, 0,
                {"Request body too large (max " + std::to_string(maxWriteBodySize() / (1024 * 1024)) + "MB)"});
        } else {
            rep._content = createErrorResponse("Request body too large (max " +
                                               std::to_string(maxWriteBodySize() / (1024 * 1024)) + "MB)");
        }
        timestar::http::setContentType(rep, resFmt);
        co_return false;
    }

    if (body.empty()) {
        rep.set_status(seastar::http::reply::status_type::bad_request);
        if (timestar::http::isProtobuf(resFmt)) {
            rep._content = timestar::proto::formatWriteResponse("error", 0, 0, {"Empty request body"});
        } else {
            rep._content = createErrorResponse("Empty request body");
        }
        timestar::http::setContentType(rep, resFmt);
        co_return false;
    }

    co_return true;
}

// handleWrite phase 2: protobuf fast write path.
// Arena-parsed, zero-intermediate-copy conversion from WriteRequest proto
// directly to per-shard TimeStarInsert batches. Skips the intermediate
// MultiWritePoint conversion and type detection heuristics.
// Fully assembles the response (success/partial/error) into `rep`. `body`
// views memory owned by the caller (req->content or bodyStorage), which
// outlives this parent-awaited coroutine.
seastar::future<> HttpWriteHandler::handleProtobufWrite(std::string_view body, uint64_t defaultTimestampNs,
                                                        timestar::http::WireFormat resFmt, seastar::http::reply& rep) {
    auto fastResult = timestar::proto::parseWriteRequestFast(body.data(), body.size(), defaultTimestampNs);

    // Yield after CPU-heavy proto parse to prevent reactor stalls
    co_await seastar::coroutine::maybe_yield();

    int64_t failedWrites = fastResult.failedWrites;
    std::vector<std::string> writeErrors = std::move(fastResult.errors);
    int64_t pointsWritten = fastResult.totalPoints;

    // Group FastFieldInserts into per-shard TimeStarInsert batches
    const size_t shardCount = seastar::smp::count;
    std::vector<std::vector<TimeStarInsert<double>>> shardDoubleInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<bool>>> shardBoolInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<std::string>>> shardStringInserts(shardCount);
    std::vector<std::vector<TimeStarInsert<int64_t>>> shardIntegerInserts(shardCount);
    std::vector<MetaOp> allMetaOps;
    std::unordered_set<SeriesId128, SeriesId128::Hash> seenMF;

    // Validate identifier limits for the WHOLE request up front (mirrors the
    // JSON path's validateArraySizes) so an oversized measurement/field/tag
    // is rejected with 400 before any data write or metadata cache mutation.
    {
        static const std::map<std::string, std::string> kNoTags;
        std::string identErr;
        for (const auto& ffi : fastResult.inserts) {
            if (!validateIdentifierLimits(ffi.measurement, ffi.tags ? *ffi.tags : kNoTags, identErr) ||
                !validateFieldNameLimit(ffi.fieldName, identErr)) {
                throw std::invalid_argument(identErr);
            }
        }
    }

    for (auto& ffi : fastResult.inserts) {
        // Compute series ID once for shard routing and metadata dedup.
        // TODO: pre-compute in parseWriteRequestFast() once libtimestar_proto_conv
        // links series_id (requires adding xxhash dependency to that target).
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(ffi.seriesKey);
        size_t shard = timestar::routeToCore(seriesId);

        // Track metadata (deduplicate by SeriesId128)
        if (seenMF.insert(seriesId).second) {
            if (!knownSeriesContains(seriesId)) {
                knownSeriesInsert(seriesId);
                TSMValueType vtype;
                switch (ffi.type) {
                    case timestar::proto::FastFieldInsert::Type::DOUBLE:
                        vtype = TSMValueType::Float;
                        break;
                    case timestar::proto::FastFieldInsert::Type::BOOL:
                        vtype = TSMValueType::Boolean;
                        break;
                    case timestar::proto::FastFieldInsert::Type::STRING:
                        vtype = TSMValueType::String;
                        break;
                    case timestar::proto::FastFieldInsert::Type::INTEGER:
                        vtype = TSMValueType::Integer;
                        break;
                }
                uint64_t mnTs = 0, mxTs = 0;
                if (ffi.timestamps && !ffi.timestamps->empty()) {
                    auto [mnIt, mxIt] = std::minmax_element(ffi.timestamps->begin(), ffi.timestamps->end());
                    mnTs = *mnIt;
                    mxTs = *mxIt;
                }
                allMetaOps.push_back(MetaOp{vtype, ffi.measurement, ffi.fieldName, *ffi.tags, mnTs, mxTs, seriesId});
            }
        }

        // Build TimeStarInsert directly from FastFieldInsert — move value
        // vectors, share tags/timestamps by refcounted pointer (same as
        // the JSON path's setSharedTags/setSharedTimestamps).
        switch (ffi.type) {
            case timestar::proto::FastFieldInsert::Type::DOUBLE: {
                TimeStarInsert<double> insert(std::move(ffi.measurement), std::move(ffi.fieldName));
                insert.setSharedTags(std::move(ffi.tags));
                insert.setSharedTimestamps(std::move(ffi.timestamps));
                insert.values = std::move(ffi.doubleValues);
                insert.setCachedSeriesKey(std::move(ffi.seriesKey));
                insert.setCachedSeriesId128(seriesId);
                shardDoubleInserts[shard].push_back(std::move(insert));
                break;
            }
            case timestar::proto::FastFieldInsert::Type::BOOL: {
                TimeStarInsert<bool> insert(std::move(ffi.measurement), std::move(ffi.fieldName));
                insert.setSharedTags(std::move(ffi.tags));
                insert.setSharedTimestamps(std::move(ffi.timestamps));
                insert.values.reserve(ffi.boolValues.size());
                for (uint8_t v : ffi.boolValues) {
                    insert.values.push_back(v != 0);
                }
                insert.setCachedSeriesKey(std::move(ffi.seriesKey));
                insert.setCachedSeriesId128(seriesId);
                shardBoolInserts[shard].push_back(std::move(insert));
                break;
            }
            case timestar::proto::FastFieldInsert::Type::STRING: {
                TimeStarInsert<std::string> insert(std::move(ffi.measurement), std::move(ffi.fieldName));
                insert.setSharedTags(std::move(ffi.tags));
                insert.setSharedTimestamps(std::move(ffi.timestamps));
                insert.values = std::move(ffi.stringValues);
                insert.setCachedSeriesKey(std::move(ffi.seriesKey));
                insert.setCachedSeriesId128(seriesId);
                shardStringInserts[shard].push_back(std::move(insert));
                break;
            }
            case timestar::proto::FastFieldInsert::Type::INTEGER: {
                TimeStarInsert<int64_t> insert(std::move(ffi.measurement), std::move(ffi.fieldName));
                insert.setSharedTags(std::move(ffi.tags));
                insert.setSharedTimestamps(std::move(ffi.timestamps));
                insert.values = std::move(ffi.integerValues);
                insert.setCachedSeriesKey(std::move(ffi.seriesKey));
                insert.setCachedSeriesId128(seriesId);
                shardIntegerInserts[shard].push_back(std::move(insert));
                break;
            }
        }
    }

    // Dispatch to shards in parallel
    std::vector<seastar::future<AggregatedTimingInfo>> shardFutures;
    shardFutures.reserve(shardCount);

    for (size_t shard = 0; shard < shardCount; ++shard) {
        if (shardDoubleInserts[shard].empty() && shardBoolInserts[shard].empty() && shardStringInserts[shard].empty() &&
            shardIntegerInserts[shard].empty()) {
            continue;
        }

        auto doubles = std::move(shardDoubleInserts[shard]);
        auto bools = std::move(shardBoolInserts[shard]);
        auto strings = std::move(shardStringInserts[shard]);
        auto integers = std::move(shardIntegerInserts[shard]);

        shardFutures.push_back(dispatchShardInserts(engineSharded, shard, std::move(doubles), std::move(bools),
                                                    std::move(strings), std::move(integers)));
    }

    if (!shardFutures.empty()) {
        try {
            co_await seastar::when_all_succeed(std::move(shardFutures));
        } catch (...) {
            // Data dispatch failed before metadata was indexed — the series
            // registered in the known-series cache during MetaOp construction
            // must be unpoisoned so a retry re-attempts metadata indexing.
            unpoisonKnownSeries(allMetaOps);
            throw;
        }
    }

    // Index metadata
    if (!allMetaOps.empty()) {
        co_await syncMetadataUnpoisonOnFailure(engineSharded, std::move(allMetaOps));
    }

    rep.set_status(seastar::http::reply::status_type::ok);
    if (timestar::http::isProtobuf(resFmt)) {
        rep._content = timestar::proto::formatWriteResponse(failedWrites > 0 ? "partial" : "success", pointsWritten,
                                                            failedWrites, writeErrors);
    } else {
        if (failedWrites > 0) {
            rep._content = createPartialFailureResponse(pointsWritten, failedWrites, writeErrors);
        } else {
            rep._content = createSuccessResponse(pointsWritten);
        }
    }
    timestar::http::setContentType(rep, resFmt);
    co_return;
}

// handleWrite phase 3: fast path for a single write with all-double fields.
// Attempts to parse `body` directly into typed vectors and dispatch it,
// returning true when handled (pointsWritten is set) and false to fall back
// to the standard DOM path. Throws std::invalid_argument on validation errors.
seastar::future<bool> HttpWriteHandler::tryFastDoubleWrite(std::string_view body, uint64_t defaultTimestampNs,
                                                           int64_t& pointsWritten) {
    // Quick check: skip fast path for batch writes (have "writes" key).
    // The "writes" key is a top-level JSON key, so it will appear
    // early in the object.  Limiting the scan to the first 256 bytes
    // avoids a full-body scan on large single-write payloads and
    // prevents false positives from "writes" appearing inside string
    // values deeper in the body.
    auto writesPos = std::string_view(body).substr(0, 256).find("\"writes\"");
    if (writesPos == std::string_view::npos) {
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

                pointsWritten = static_cast<int64_t>(mwp.timestamps.size()) * static_cast<int64_t>(mwp.fields.size());

                auto writeResult = co_await processMultiWritePoint(mwp);

                if (!writeResult.metaOps.empty()) {
                    co_await syncMetadataUnpoisonOnFailure(engineSharded, std::move(writeResult.metaOps));
                }
                co_return true;
            }
        }
    }
    co_return false;
}

// handleWrite phase 4: JSON batch write path ("writes" array).
// Coalesces the batch, validates and accumulates every MultiWritePoint into
// per-shard typed insert batches, dispatches once per active shard, and
// attributes failures per shard. Returns true when the response (batch-too-
// large error or partial failure) is already assembled in `rep`; returns
// false when the caller should emit the shared success response.
// `writes_array` and `rep` are caller locals that outlive this parent-awaited
// coroutine. Throws std::invalid_argument on validation errors.
seastar::future<bool> HttpWriteHandler::processBatchWrites(const json_value_t::array_t& writes_array,
                                                           uint64_t defaultTimestampNs, int64_t& pointsWritten,
                                                           timestar::http::WireFormat resFmt,
                                                           seastar::http::reply& rep) {
    constexpr size_t MAX_BATCH_WRITES = 100000;
    if (writes_array.size() > MAX_BATCH_WRITES) {
        rep.set_status(seastar::http::reply::status_type::bad_request);
        rep._content = createErrorResponse("Batch too large: " + std::to_string(writes_array.size()) +
                                           " writes exceeds maximum of " + std::to_string(MAX_BATCH_WRITES));
        timestar::http::setContentType(rep, resFmt);
        co_return true;
    }

    LOG_INSERT_PATH(timestar::http_log, info, "[BATCH] Processing batch with {} writes", writes_array.size());

#if TIMESTAR_LOG_INSERT_PATH
    auto coalesceStartTime = std::chrono::steady_clock::now();
#endif
    size_t coalesceSkipped = 0;
    auto coalescedWrites = coalesceWrites(writes_array, defaultTimestampNs, coalesceSkipped);
#if TIMESTAR_LOG_INSERT_PATH
    auto coalesceEndTime = std::chrono::steady_clock::now();
    auto coalesceDuration = std::chrono::duration_cast<std::chrono::microseconds>(coalesceEndTime - coalesceStartTime);
    LOG_INSERT_PATH(timestar::http_log, info, "[BATCH] Coalesced {} writes into {} MultiWritePoints ({}μs)",
                    writes_array.size(), coalescedWrites.size(), coalesceDuration.count());
#endif

    // Validate all MWPs up-front (before any dispatch), then accumulate
    // the ENTIRE request into one set of per-shard typed insert vectors.
    // This replaces the previous one-coroutine-per-MultiWritePoint fan-out
    // (M × shards cross-shard round trips and M WAL cycles per request)
    // with a single dispatch per active shard.
    const size_t shardCount = seastar::smp::count;
    BatchAccumulator acc(shardCount);
    {
        size_t accumulated = 0;
        for (auto& mwp : coalescedWrites) {
            std::string error;
            if (!validateArraySizes(mwp, error)) {
                // Earlier MWPs in this request may already have registered
                // their series in the known-series cache; nothing was (or
                // will be) written or indexed, so unpoison before failing.
                unpoisonKnownSeries(acc.metaOps);
                throw std::invalid_argument(error);
            }
            pointsWritten += static_cast<int64_t>(mwp.timestamps.size()) * static_cast<int64_t>(mwp.fields.size());
            accumulateMultiWritePoint(mwp, acc);
            // Yield periodically so accumulating a large batch does not
            // stall the reactor (accumulation itself never suspends).
            if ((++accumulated & 63u) == 0) {
                co_await seastar::coroutine::maybe_yield();
            }
        }
    }

    // Single dispatch per active shard for the whole request.
    std::vector<unsigned> activeShards;
    std::vector<seastar::future<AggregatedTimingInfo>> shardFutures;
    activeShards.reserve(shardCount);
    shardFutures.reserve(shardCount);
    for (unsigned shard = 0; shard < shardCount; ++shard) {
        if (acc.shardEmpty(shard)) {
            continue;
        }
        activeShards.push_back(shard);
        shardFutures.push_back(dispatchShardInserts(
            engineSharded, shard, std::move(acc.shardDoubles[shard]), std::move(acc.shardBools[shard]),
            std::move(acc.shardStrings[shard]), std::move(acc.shardIntegers[shard])));
    }

    auto shardResults = co_await seastar::when_all(shardFutures.begin(), shardFutures.end());

    // Failure attribution is per-shard: if a shard's insert future
    // failed, all points routed to that shard count as failed.
    int64_t failedWrites = 0;
    std::vector<std::string> writeErrors;
    std::optional<std::string> tooLargeError;
    for (size_t i = 0; i < shardResults.size(); ++i) {
        try {
            shardResults[i].get();
        } catch (const timestar::InsertTooLargeException& e) {
            const unsigned shard = activeShards[i];
            timestar::http_log.warn("Batch too large for WAL segment on shard {}: {}", shard, e.what());
            pointsWritten -= acc.shardPoints[shard];
            failedWrites += acc.shardPoints[shard];
            tooLargeError = e.what();
            if (writeErrors.size() < 10) {
                writeErrors.emplace_back("shard " + std::to_string(shard) + ": " + e.what());
            }
        } catch (const std::exception& e) {
            const unsigned shard = activeShards[i];
            timestar::http_log.error("Error inserting batch on shard {}: {}", shard, e.what());
            pointsWritten -= acc.shardPoints[shard];
            failedWrites += acc.shardPoints[shard];
            if (writeErrors.size() < 10) {
                writeErrors.emplace_back("shard " + std::to_string(shard) + ": " + e.what());
            }
        }
    }

    // If nothing was written and the batch was simply too large for a WAL
    // segment, surface it as a client error (413) instead of a "partial"
    // success — let the top-level handler build the flat error response.
    // Metadata was never indexed for this request, so unpoison the cache.
    if (tooLargeError.has_value() && pointsWritten == 0) {
        unpoisonKnownSeries(acc.metaOps);
        throw timestar::InsertTooLargeException(*tooLargeError);
    }

#if TIMESTAR_LOG_INSERT_PATH
    size_t metaOpsCount = acc.metaOps.size();
#endif
    if (!acc.metaOps.empty()) {
        co_await syncMetadataUnpoisonOnFailure(engineSharded, std::move(acc.metaOps));
    }
#if TIMESTAR_LOG_INSERT_PATH
    LOG_INSERT_PATH(timestar::http_log, info, "[METADATA] Batch: indexed {} unique series synchronously", metaOpsCount);
#endif

    // Include coalesce-skipped entries in failure count
    failedWrites += static_cast<int64_t>(coalesceSkipped);

    // Report partial failure if some writes failed
    if (failedWrites > 0) {
        rep.set_status(seastar::http::reply::status_type::ok);
        if (timestar::http::isProtobuf(resFmt)) {
            rep._content = timestar::proto::formatWriteResponse("partial", pointsWritten, failedWrites, writeErrors);
        } else {
            rep._content = createPartialFailureResponse(pointsWritten, failedWrites, writeErrors);
        }
        timestar::http::setContentType(rep, resFmt);
        co_return true;
    }

    co_return false;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpWriteHandler::handleWrite(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto reqFmt = timestar::http::requestFormat(*req);
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        // Phase 1: read the body (buffered or streamed) and enforce size
        // limits. On failure the error response is already assembled in rep.
        std::string bodyStorage;  // owns data only on the streaming branch
        std::string_view body;
        if (!co_await readWriteBody(*req, bodyStorage, body, resFmt, *rep)) {
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

        // Phase 2 (protobuf fast path): the response — success, partial, or
        // error — is fully assembled into rep by the phase method.
        if (timestar::http::isProtobuf(reqFmt)) {
            co_await handleProtobufWrite(body, defaultTimestampNs, resFmt, *rep);
            co_return rep;
        }

        // ─── Fast path: single write with all-double fields ───
        // For large payloads (>256 bytes) that aren't batch writes, try parsing
        // directly into typed vectors, bypassing the json_value_t DOM entirely.
        // For a 10K-timestamp × 10-field batch, this avoids ~110K DOM node
        // allocations and roughly halves the JSON parsing cost.
        bool fastPathHandled = false;
        if (body.size() > 256) {
            fastPathHandled = co_await tryFastDoubleWrite(body, defaultTimestampNs, pointsWritten);
        }

        // ─── Standard DOM path (batch writes + mixed-type single writes) ───
        if (!fastPathHandled) {
            // Parse JSON using Glaze with u64 number mode to preserve integer precision.
            json_value_t doc{};
            auto parse_error = glz::read_json(doc, body);

            if (parse_error) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                rep->_content = createErrorResponse("Invalid JSON: " + std::string(glz::format_error(parse_error)));
                timestar::http::setContentType(*rep, resFmt);
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
                    // Phase 4 (batch path): coalesce + accumulate + dispatch.
                    // A `true` return means the response (batch-too-large error
                    // or partial failure) is already assembled in rep.
                    auto& writes_array = writes.get<json_value_t::array_t>();
                    if (co_await processBatchWrites(writes_array, defaultTimestampNs, pointsWritten, resFmt, *rep)) {
                        co_return rep;
                    }
                } else {
                    MultiWritePoint mwp = parseMultiWritePoint(doc, defaultTimestampNs);
                    std::string error;
                    if (!validateArraySizes(mwp, error)) {
                        throw std::invalid_argument(error);
                    }

                    pointsWritten =
                        static_cast<int64_t>(mwp.timestamps.size()) * static_cast<int64_t>(mwp.fields.size());

                    auto writeResult = co_await processMultiWritePoint(mwp);

#if TIMESTAR_LOG_INSERT_PATH
                    size_t metaOpsCount = writeResult.metaOps.size();
#endif
                    if (!writeResult.metaOps.empty()) {
                        co_await syncMetadataUnpoisonOnFailure(engineSharded, std::move(writeResult.metaOps));
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
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatWriteResponse("success", pointsWritten);
        } else {
            rep->_content = createSuccessResponse(pointsWritten);
        }
        timestar::http::setContentType(*rep, resFmt);

    } catch (const seastar::gate_closed_exception&) {
        // Insert gate closed — server is shutting down. Return 503 so clients
        // know to retry against another node or after restart.
        ++engineSharded->local().metrics().insert_errors_total;
        rep->set_status(seastar::http::reply::status_type::service_unavailable);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatWriteResponse("error", 0, 0, {"Server is shutting down"});
        } else {
            rep->_content = createErrorResponse("Server is shutting down");
        }
        timestar::http::setContentType(*rep, resFmt);
    } catch (const std::invalid_argument& e) {
        // Client input validation error — 400 Bad Request
        ++engineSharded->local().metrics().insert_errors_total;
        timestar::http_log.debug("Write validation error: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::bad_request);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatWriteResponse("error", 0, 0, {std::string(e.what())});
        } else {
            rep->_content = createErrorResponse(e.what());
        }
        timestar::http::setContentType(*rep, resFmt);
    } catch (const timestar::InsertTooLargeException& e) {
        // Request larger than a WAL segment can hold — client error, not a
        // server fault: 413 Payload Too Large with a flat error body.
        ++engineSharded->local().metrics().insert_errors_total;
        timestar::http_log.debug("Write too large: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::payload_too_large);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatWriteResponse("error", 0, 0, {std::string(e.what())});
        } else {
            rep->_content = createErrorResponse(e.what());
        }
        timestar::http::setContentType(*rep, resFmt);
    } catch (const std::exception& e) {
        ++engineSharded->local().metrics().insert_errors_total;
        timestar::http_log.error("Error handling write request: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatWriteResponse("error", 0, 0, {"Internal server error"});
        } else {
            rep->_content = createErrorResponse("Internal server error");
        }
        timestar::http::setContentType(*rep, resFmt);
    }

    co_return rep;
}

void HttpWriteHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    // addJsonRoute applies timestar::http::wrapWithAuth per route.
    timestar::http::addJsonRoute(
        r, seastar::httpd::operation_type::POST, "/write", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleWrite(std::move(req)); });

    timestar::http_log.info("Registered HTTP write endpoint at /write{}", authToken.empty() ? "" : " (auth required)");
}

}  // namespace timestar::http
