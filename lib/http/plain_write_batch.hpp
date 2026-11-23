#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

// A `{"writes":[...]}` request in its plainest and by far most common shape —
// one scalar point per element:
//
//   {"writes":[
//     {"measurement":"cpu","tags":{"host":"a"},"fields":{"usage":12.5},"timestamp":1700000000000000000},
//     ...
//   ]}
//
// The general write path parses a batch into a JSON DOM first: every key,
// string and number becomes a heap node (10-15x the body in memory) that the
// handler then walks and frees, which is most of what a per-point batch costs.
// scanPlainWriteBatch() reads the plain shape in one pass into three flat
// arrays of views over the request body instead: no per-value allocation, and
// storage reused from one request to the next.
//
// It is deliberately NOT a JSON parser. It recognises one strict subset and
// answers "not plain" for everything else — arrays or nulls as field values,
// `timestamps`, `field_types`, unknown or repeated keys, string escapes,
// non-ASCII text, numbers outside the exactly-representable cases — and the
// caller then takes the DOM path, which remains the single definition of what
// those inputs mean (including which of them are errors). The subset is chosen
// so that for a body it accepts, the DOM path would have read the very same
// values; test/unit/http/plain_write_batch_test.cpp holds the two to that.

namespace timestar::http {

struct PlainField {
    enum class Kind : uint8_t { Integer, Float, Boolean, String };

    std::string_view name;
    std::string_view text;  // Kind::String only: the characters between the quotes
    union {
        int64_t integer;
        double real;
        bool boolean;
    };
    Kind kind;
};

struct PlainTag {
    std::string_view key;
    std::string_view value;
};

struct PlainWrite {
    std::string_view measurement;
    uint32_t tagsBegin;  // [tagsBegin, tagsEnd) into PlainWriteBatch::tags, in document order
    uint32_t tagsEnd;
    uint32_t fieldsBegin;  // [fieldsBegin, fieldsEnd) into PlainWriteBatch::fields, in document order
    uint32_t fieldsEnd;
    uint64_t timestamp;  // valid when hasTimestamp
    bool hasTimestamp;
};

struct PlainWriteBatch {
    std::vector<PlainWrite> writes;
    std::vector<PlainTag> tags;
    std::vector<PlainField> fields;

    void clear() {
        writes.clear();
        tags.clear();
        fields.clear();
    }
};

// Reads `body` into `out` (cleared first) and returns true if the whole body is
// a plain batch of at most `maxWrites` writes. Returns false otherwise, with
// `out` in an unspecified state. Every view in `out` points into `body`.
bool scanPlainWriteBatch(std::string_view body, PlainWriteBatch& out, size_t maxWrites);

}  // namespace timestar::http
