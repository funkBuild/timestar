#pragma once

// Protobuf write fast path — Arena-parsed, zero-intermediate-copy conversion
// from WriteRequest proto directly to per-shard TimeStarInsert batches.
//
// This file declares the public interface. The implementation (.cpp) includes
// timestar.pb.h and MUST be compiled as part of libtimestar_proto_conv (or a
// similar isolated library) to avoid ODR violations with internal types.

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::proto {

// ============================================================================
// Fast-path result types
// ============================================================================

// A single field insert ready for Engine::insertBatch(). Contains all the data
// needed to construct a TimeStarInsert<T> without any further conversion.
// The write handler converts these to TimeStarInsert<T> with zero copies by
// moving the vectors directly.
struct FastFieldInsert {
    enum class Type : uint8_t { DOUBLE, BOOL, STRING, INTEGER };

    std::string measurement;
    std::string fieldName;
    std::map<std::string, std::string> tags;
    std::vector<uint64_t> timestamps;

    // Exactly one of these is populated based on `type`
    Type type;
    std::vector<double> doubleValues;
    std::vector<uint8_t> boolValues;  // uint8_t to match FieldArrays convention
    std::vector<std::string> stringValues;
    std::vector<int64_t> integerValues;

    // Pre-computed series key for shard routing (avoids recomputation in handler)
    std::string seriesKey;
};

// Result of the fast-path parser. Contains per-field inserts ready for the
// write handler to distribute to shards, plus metadata needed for indexing.
struct FastPathResult {
    std::vector<FastFieldInsert> inserts;
    int64_t totalPoints = 0;

    // Validation errors (if any). Empty means all points are valid.
    std::vector<std::string> errors;
    int64_t failedWrites = 0;
};

// ============================================================================
// Fast-path parser
// ============================================================================

// Parse a WriteRequest protobuf using Arena allocation and convert directly
// to per-field FastFieldInserts, skipping the intermediate MultiWritePoint.
//
// - Uses google::protobuf::Arena for bulk allocation/deallocation
// - Branches on WriteField::typed_values_case() (no type detection heuristics)
// - Copies timestamp data via memcpy from packed proto fields
// - Builds series keys inline for shard routing
//
// defaultTimestampNs: used when a WritePoint has no explicit timestamps.
//
// Throws std::runtime_error only on proto parse failure (corrupt bytes).
// Per-point validation errors are captured in result.errors.
FastPathResult parseWriteRequestFast(const void* data, size_t size, uint64_t defaultTimestampNs);

}  // namespace timestar::proto
