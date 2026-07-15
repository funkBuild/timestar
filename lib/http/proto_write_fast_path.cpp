// proto_write_fast_path.cpp — Arena-parsed, zero-intermediate-copy protobuf write path.
//
// This translation unit includes timestar.pb.h and MUST be compiled as part of
// libtimestar_proto_conv (NOT libtimestar) to avoid ODR violations.
//
// Optimizations over the generic parseWriteRequest():
// 1. Arena allocation: all proto sub-messages bulk-allocated, bulk-freed
// 2. Direct type branching: WriteField::typed_values_case() gives us the type
//    at parse time — no heuristic detection needed
// 3. Zero intermediate copies: we go directly from proto arrays to FastFieldInsert
//    vectors, never building an intermediate MultiWritePoint
// 4. Packed timestamp memcpy: proto packed uint64 fields store values contiguously;
//    we copy them in one shot via memcpy instead of per-element push_back
// 5. Pre-computed series keys: built inline for each field insert, ready for
//    shard routing without recomputation in the handler

#include "proto_write_fast_path.hpp"

#include "timestar.pb.h"

#include <google/protobuf/arena.h>

#include <cstring>
#include <stdexcept>
#include <string>

namespace timestar::proto {

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// Series-key construction uses the canonical timestar::buildSeriesKey
// from lib/utils/series_key.hpp (identical implementation; previously
// duplicated here as a file-local static).

// Fast name validation — same rules as HttpWriteHandler::validateName.
// Returns true if valid, false otherwise.
static bool isValidName(const std::string& s) {
    if (s.empty())
        return false;
    for (char c : s) {
        if (c == '\0' || c == ',' || c == '=' || c == ' ')
            return false;
    }
    return true;
}

// Tag values allow spaces but not null/comma/equals.
static bool isValidTagValue(const std::string& s) {
    if (s.empty())
        return false;
    for (char c : s) {
        if (c == '\0' || c == ',' || c == '=')
            return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// Fast-path parser
// ---------------------------------------------------------------------------

FastPathResult parseWriteRequestFast(const void* data, size_t size, uint64_t defaultTimestampNs) {
    // Arena-allocate the WriteRequest for bulk deallocation
    google::protobuf::Arena arena;
    auto* request = google::protobuf::Arena::CreateMessage<::timestar_pb::WriteRequest>(&arena);

    if (size > static_cast<size_t>(INT_MAX)) {
        throw std::runtime_error("Protobuf payload too large");
    }
    if (!request->ParseFromArray(data, static_cast<int>(size))) {
        throw std::runtime_error("Failed to parse WriteRequest protobuf");
    }

    FastPathResult result;
    // Estimate: average 2 fields per write point
    result.inserts.reserve(static_cast<size_t>(request->writes_size()) * 2);

    for (int wpIdx = 0; wpIdx < request->writes_size(); ++wpIdx) {
        const auto& wp = request->writes(wpIdx);

        // Validate measurement name
        const std::string& measurement = wp.measurement();
        if (!isValidName(measurement)) {
            ++result.failedWrites;
            if (result.errors.size() < 10) {
                result.errors.push_back("Invalid measurement name: '" + measurement + "'");
            }
            continue;
        }

        // Extract and validate tags into a sorted std::map
        std::map<std::string, std::string> tags;
        bool tagsValid = true;
        for (const auto& [k, v] : wp.tags()) {
            if (!isValidName(k)) {
                tagsValid = false;
                if (result.errors.size() < 10) {
                    result.errors.push_back("Invalid tag key: '" + k + "'");
                }
                break;
            }
            if (!isValidTagValue(v)) {
                tagsValid = false;
                if (result.errors.size() < 10) {
                    result.errors.push_back("Invalid tag value for '" + k + "': '" + v + "'");
                }
                break;
            }
            tags[k] = v;
        }
        if (!tagsValid) {
            ++result.failedWrites;
            continue;
        }

        // Extract timestamps — use packed field for efficient memcpy.
        // Proto packed uint64 fields store values contiguously in memory.
        // Wrapped once in a shared_ptr so every field of this write point can
        // reference the same allocation instead of copying per field.
        std::shared_ptr<const std::vector<uint64_t>> sharedTimestamps;
        const int tsCount = wp.timestamps_size();
        if (tsCount > 0) {
            std::vector<uint64_t> timestamps(static_cast<size_t>(tsCount));
            // RepeatedField<uint64_t>::data() gives direct pointer to packed data
            std::memcpy(timestamps.data(), wp.timestamps().data(), static_cast<size_t>(tsCount) * sizeof(uint64_t));
            sharedTimestamps = std::make_shared<const std::vector<uint64_t>>(std::move(timestamps));
        }

        // Pre-build the measurement+tags prefix once per write point.
        // Individual fields will append " fieldName" to get the full series key.
        std::string seriesKeyPrefix;
        {
            size_t prefixSize = measurement.size();
            for (const auto& [k, v] : tags) {
                prefixSize += 1 + k.size() + 1 + v.size();
            }
            seriesKeyPrefix.reserve(prefixSize);
            seriesKeyPrefix = measurement;
            for (const auto& [k, v] : tags) {
                seriesKeyPrefix += ',';
                seriesKeyPrefix += k;
                seriesKeyPrefix += '=';
                seriesKeyPrefix += v;
            }
        }

        // Wrap the validated tag map once; all fields of this point share it.
        auto sharedTags = std::make_shared<const std::map<std::string, std::string>>(std::move(tags));

        // Per-count caches for the replicated / generated timestamp cases so
        // multiple fields with the same value count share one allocation.
        std::shared_ptr<const std::vector<uint64_t>> replicatedTs;
        std::shared_ptr<const std::vector<uint64_t>> generatedTs;

        // Track per-point field count for totalPoints accounting
        int fieldsProcessed = 0;

        for (const auto& [fieldName, writeField] : wp.fields()) {
            // Validate field name
            if (!isValidName(fieldName)) {
                if (result.errors.size() < 10) {
                    result.errors.push_back("Invalid field name: '" + fieldName + "'");
                }
                continue;  // skip this field, not the whole point
            }

            FastFieldInsert ffi;
            ffi.measurement = measurement;
            ffi.fieldName = fieldName;

            // Build series key from prefix
            ffi.seriesKey.reserve(seriesKeyPrefix.size() + 1 + fieldName.size());
            ffi.seriesKey = seriesKeyPrefix;
            ffi.seriesKey += ' ';
            ffi.seriesKey += fieldName;

            // Branch on typed_values_case — the type is known from the proto oneof
            switch (writeField.typed_values_case()) {
                case ::timestar_pb::WriteField::kDoubleValues: {
                    const auto& vals = writeField.double_values().values();
                    const int valCount = vals.size();
                    if (valCount == 0)
                        continue;

                    ffi.type = FastFieldInsert::Type::DOUBLE;
                    ffi.doubleValues.resize(static_cast<size_t>(valCount));
                    // Packed doubles — contiguous memcpy
                    std::memcpy(ffi.doubleValues.data(), vals.data(), static_cast<size_t>(valCount) * sizeof(double));
                    break;
                }
                case ::timestar_pb::WriteField::kBoolValues: {
                    const auto& vals = writeField.bool_values().values();
                    const int valCount = vals.size();
                    if (valCount == 0)
                        continue;

                    ffi.type = FastFieldInsert::Type::BOOL;
                    ffi.boolValues.reserve(static_cast<size_t>(valCount));
                    for (int i = 0; i < valCount; ++i) {
                        ffi.boolValues.push_back(vals.Get(i) ? 1 : 0);
                    }
                    break;
                }
                case ::timestar_pb::WriteField::kStringValues: {
                    const auto& vals = writeField.string_values().values();
                    const int valCount = vals.size();
                    if (valCount == 0)
                        continue;

                    ffi.type = FastFieldInsert::Type::STRING;
                    ffi.stringValues.reserve(static_cast<size_t>(valCount));
                    for (int i = 0; i < valCount; ++i) {
                        ffi.stringValues.push_back(vals.Get(i));
                    }
                    break;
                }
                case ::timestar_pb::WriteField::kInt64Values: {
                    const auto& vals = writeField.int64_values().values();
                    const int valCount = vals.size();
                    if (valCount == 0)
                        continue;

                    ffi.type = FastFieldInsert::Type::INTEGER;
                    ffi.integerValues.resize(static_cast<size_t>(valCount));
                    // Packed int64 — contiguous memcpy
                    std::memcpy(ffi.integerValues.data(), vals.data(), static_cast<size_t>(valCount) * sizeof(int64_t));
                    break;
                }
                default:
                    // Unset oneof — skip this field
                    continue;
            }

            // Determine value count for this field
            size_t valueCount = 0;
            switch (ffi.type) {
                case FastFieldInsert::Type::DOUBLE:
                    valueCount = ffi.doubleValues.size();
                    break;
                case FastFieldInsert::Type::BOOL:
                    valueCount = ffi.boolValues.size();
                    break;
                case FastFieldInsert::Type::STRING:
                    valueCount = ffi.stringValues.size();
                    break;
                case FastFieldInsert::Type::INTEGER:
                    valueCount = ffi.integerValues.size();
                    break;
            }

            // Resolve timestamps for this field (shared_ptr assignment, no copy):
            // - If timestamps match value count, share them directly
            // - If single timestamp, replicate for all values
            // - If no timestamps, generate from defaultTimestampNs
            const size_t tsAvail = sharedTimestamps ? sharedTimestamps->size() : 0;
            if (tsAvail == valueCount) {
                ffi.timestamps = sharedTimestamps;
            } else if (tsAvail == 1) {
                // Replicate the single timestamp; reuse the allocation across
                // fields with the same value count.
                if (!replicatedTs || replicatedTs->size() != valueCount) {
                    replicatedTs =
                        std::make_shared<const std::vector<uint64_t>>(valueCount, (*sharedTimestamps)[0]);
                }
                ffi.timestamps = replicatedTs;
            } else if (tsAvail == 0) {
                // No timestamps provided — generate from default, 1ms apart;
                // reuse the allocation across fields with the same value count.
                if (!generatedTs || generatedTs->size() != valueCount) {
                    std::vector<uint64_t> gen;
                    gen.reserve(valueCount);
                    uint64_t ts = defaultTimestampNs;
                    for (size_t i = 0; i < valueCount; ++i) {
                        gen.push_back(ts);
                        ts += 1'000'000;  // 1ms
                    }
                    generatedTs = std::make_shared<const std::vector<uint64_t>>(std::move(gen));
                }
                ffi.timestamps = generatedTs;
            } else {
                // Timestamp count mismatch — validation error for this field
                if (result.errors.size() < 10) {
                    result.errors.push_back("Field '" + fieldName + "' has " + std::to_string(valueCount) +
                                            " values but " + std::to_string(tsAvail) + " timestamps");
                }
                continue;
            }

            // Share tags by pointer — all fields of this point reference the
            // same immutable map allocation.
            ffi.tags = sharedTags;

            result.totalPoints += static_cast<int64_t>(ffi.timestamps->size());
            ++fieldsProcessed;
            result.inserts.push_back(std::move(ffi));
        }

        // If no fields were processed, count as failed write
        if (fieldsProcessed == 0 && wp.fields_size() > 0) {
            ++result.failedWrites;
        }
    }

    return result;
}

}  // namespace timestar::proto
