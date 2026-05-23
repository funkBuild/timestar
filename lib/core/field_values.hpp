#pragma once

// Canonical type aliases for the four field value types supported by TimeStar
// (double, bool, string, int64_t). Previously these were duplicated across
// http_write_handler.hpp, http_query_handler.hpp, and proto_converters.hpp;
// keeping a single source of truth makes the upcoming value-type-dispatch
// refactor (Wave 4) tractable and removes drift risk between the read and
// write paths.
//
// Safe to include from proto_converters.hpp: this header defines only
// std::variant type aliases and a POD struct — none of these names collide
// with the proto-generated ::timestar:: classes (QueryRequest, StreamingBatch,
// etc.), which are the ODR concern documented in proto_converters.hpp.

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace timestar {

// Per-field arrays as parsed from a write request: only one of the four
// vectors is populated, selected by `type`. The `bools` vector uses uint8_t
// (not std::vector<bool>) so callers can pass a stable pointer for SIMD.
struct FieldArrays {
    std::vector<double> doubles;
    std::vector<uint8_t> bools;
    std::vector<std::string> strings;
    std::vector<int64_t> integers;
    enum Type { DOUBLE, BOOL, STRING, INTEGER } type;
};

// Per-field values as returned from a query: a variant over the four
// vector types. Bool storage is std::vector<bool> here (matches the on-disk
// decoder output); a future bool-vector unification can flip this to uint8_t.
using FieldValues =
    std::variant<std::vector<double>, std::vector<bool>, std::vector<std::string>, std::vector<int64_t>>;

}  // namespace timestar
