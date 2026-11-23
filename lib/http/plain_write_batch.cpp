#include "plain_write_batch.hpp"

#include <charconv>
#include <system_error>

namespace timestar::http {

namespace {

inline bool isDigit(char c) noexcept {
    return c >= '0' && c <= '9';
}

inline void skipWs(const char*& p, const char* end) noexcept {
    while (p < end && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t')) {
        ++p;
    }
}

// Whitespace, then exactly `c`.
inline bool expect(const char*& p, const char* end, char c) noexcept {
    skipWs(p, end);
    if (p < end && *p == c) {
        ++p;
        return true;
    }
    return false;
}

// A string of printable ASCII with no escapes. Anything else — an escape, a
// control character, a non-ASCII byte — is left to the DOM parser, which owns
// unescaping and UTF-8 validation.
inline bool readPlainString(const char*& p, const char* end, std::string_view& out) noexcept {
    skipWs(p, end);
    if (p >= end || *p != '"') {
        return false;
    }
    const char* const start = ++p;
    for (; p < end; ++p) {
        const unsigned char c = static_cast<unsigned char>(*p);
        if (c == '"') {
            out = std::string_view(start, static_cast<size_t>(p - start));
            ++p;
            return true;
        }
        if (c < 0x20 || c > 0x7E || c == '\\') {
            return false;
        }
    }
    return false;
}

// A number in exactly the JSON grammar:
//   -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
// reporting which optional parts it has. Those decide how the DOM stores it:
// a fraction makes it a double; neither part makes it an integer; an exponent
// WITHOUT a fraction can go either way there ("1e5" is the integer 100000,
// "1e-1" a double), so callers leave that form to the DOM.
struct NumberShape {
    bool hasFraction = false;
    bool hasExponent = false;
};

inline bool scanNumber(const char*& p, const char* end, NumberShape& shape) noexcept {
    if (p < end && *p == '-') {
        ++p;
    }
    if (p >= end) {
        return false;
    }
    if (*p == '0') {
        ++p;
    } else if (*p >= '1' && *p <= '9') {
        do {
            ++p;
        } while (p < end && isDigit(*p));
    } else {
        return false;
    }
    if (p < end && *p == '.') {
        ++p;
        if (p >= end || !isDigit(*p)) {
            return false;
        }
        do {
            ++p;
        } while (p < end && isDigit(*p));
        shape.hasFraction = true;
    }
    if (p < end && (*p == 'e' || *p == 'E')) {
        ++p;
        if (p < end && (*p == '+' || *p == '-')) {
            ++p;
        }
        if (p >= end || !isDigit(*p)) {
            return false;
        }
        do {
            ++p;
        } while (p < end && isDigit(*p));
        shape.hasExponent = true;
    }
    return true;
}

// A field value: number, true/false, or plain string.
inline bool readScalar(const char*& p, const char* end, PlainField& field) noexcept {
    skipWs(p, end);
    if (p >= end) {
        return false;
    }
    const char c = *p;
    if (c == '"') {
        field.kind = PlainField::Kind::String;
        field.integer = 0;
        return readPlainString(p, end, field.text);
    }
    if (c == 't') {
        if (end - p < 4 || std::string_view(p, 4) != "true") {
            return false;
        }
        p += 4;
        field.kind = PlainField::Kind::Boolean;
        field.integer = 0;
        field.boolean = true;
        return true;
    }
    if (c == 'f') {
        if (end - p < 5 || std::string_view(p, 5) != "false") {
            return false;
        }
        p += 5;
        field.kind = PlainField::Kind::Boolean;
        field.integer = 0;
        field.boolean = false;
        return true;
    }
    if (c != '-' && !isDigit(c)) {
        return false;  // null, array, object, or not JSON at all
    }

    const char* const start = p;
    NumberShape shape;
    if (!scanNumber(p, end, shape)) {
        return false;
    }
    if (!shape.hasFraction) {
        if (shape.hasExponent) {
            return false;  // "1e5": integer or double is the DOM's call
        }
        // Only integers that fit int64. Larger ones (and "-0") are stored by
        // the DOM in ways not worth reproducing here.
        int64_t value = 0;
        const auto [ptr, ec] = std::from_chars(start, p, value);
        if (ec != std::errc() || ptr != p || (value == 0 && *start == '-')) {
            return false;
        }
        field.kind = PlainField::Kind::Integer;
        field.integer = value;
        return true;
    }
    double value = 0.0;
    const auto [ptr, ec] = std::from_chars(start, p, value, std::chars_format::general);
    if (ec != std::errc() || ptr != p) {
        return false;  // overflow / underflow: let the DOM parser decide
    }
    field.kind = PlainField::Kind::Float;
    field.real = value;
    return true;
}

// A timestamp: a non-negative integer that fits uint64.
inline bool readTimestamp(const char*& p, const char* end, uint64_t& out) noexcept {
    skipWs(p, end);
    const char* const start = p;
    if (p >= end || !isDigit(*p)) {
        return false;
    }
    NumberShape shape;
    if (!scanNumber(p, end, shape) || shape.hasFraction || shape.hasExponent) {
        return false;
    }
    const auto [ptr, ec] = std::from_chars(start, p, out);
    return ec == std::errc() && ptr == p;
}

// {"k":"v",...} with distinct keys; may be empty.
inline bool readTags(const char*& p, const char* end, PlainWriteBatch& out) {
    if (!expect(p, end, '{')) {
        return false;
    }
    const size_t first = out.tags.size();
    skipWs(p, end);
    if (p < end && *p == '}') {
        ++p;
        return true;
    }
    for (;;) {
        PlainTag tag;
        if (!readPlainString(p, end, tag.key) || !expect(p, end, ':') || !readPlainString(p, end, tag.value)) {
            return false;
        }
        for (size_t i = first; i < out.tags.size(); ++i) {
            if (out.tags[i].key == tag.key) {
                return false;  // repeated key: which one wins is the DOM's call
            }
        }
        out.tags.push_back(tag);
        skipWs(p, end);
        if (p >= end) {
            return false;
        }
        if (*p == ',') {
            ++p;
            continue;
        }
        if (*p == '}') {
            ++p;
            return true;
        }
        return false;
    }
}

// {"name":scalar,...} with distinct names; at least one.
inline bool readFields(const char*& p, const char* end, PlainWriteBatch& out) {
    if (!expect(p, end, '{')) {
        return false;
    }
    const size_t first = out.fields.size();
    for (;;) {
        PlainField field;
        if (!readPlainString(p, end, field.name) || !expect(p, end, ':') || !readScalar(p, end, field)) {
            return false;
        }
        for (size_t i = first; i < out.fields.size(); ++i) {
            if (out.fields[i].name == field.name) {
                return false;
            }
        }
        out.fields.push_back(field);
        skipWs(p, end);
        if (p >= end) {
            return false;
        }
        if (*p == ',') {
            ++p;
            continue;
        }
        if (*p == '}') {
            ++p;
            return true;
        }
        return false;
    }
}

// One write object: measurement and fields required, tags and timestamp
// optional, each key at most once, nothing else.
inline bool readWrite(const char*& p, const char* end, PlainWriteBatch& out) {
    if (!expect(p, end, '{')) {
        return false;
    }
    PlainWrite write{};
    write.tagsBegin = write.tagsEnd = static_cast<uint32_t>(out.tags.size());
    write.fieldsBegin = write.fieldsEnd = static_cast<uint32_t>(out.fields.size());
    bool haveMeasurement = false;
    bool haveTags = false;
    bool haveFields = false;

    for (;;) {
        std::string_view key;
        if (!readPlainString(p, end, key) || !expect(p, end, ':')) {
            return false;
        }
        if (key == "measurement") {
            if (haveMeasurement || !readPlainString(p, end, write.measurement)) {
                return false;
            }
            haveMeasurement = true;
        } else if (key == "tags") {
            if (haveTags || !readTags(p, end, out)) {
                return false;
            }
            haveTags = true;
            write.tagsEnd = static_cast<uint32_t>(out.tags.size());
        } else if (key == "fields") {
            // Tags and fields are stored as contiguous runs, so a write's
            // fields must not interleave with another key's entries — they
            // cannot: each object is read to its end before the next key.
            if (haveFields || !readFields(p, end, out)) {
                return false;
            }
            haveFields = true;
            write.fieldsEnd = static_cast<uint32_t>(out.fields.size());
        } else if (key == "timestamp") {
            if (write.hasTimestamp || !readTimestamp(p, end, write.timestamp)) {
                return false;
            }
            write.hasTimestamp = true;
        } else {
            return false;  // timestamps, field_types, anything unknown
        }

        skipWs(p, end);
        if (p >= end) {
            return false;
        }
        if (*p == ',') {
            ++p;
            continue;
        }
        if (*p == '}') {
            ++p;
            break;
        }
        return false;
    }

    if (!haveMeasurement || !haveFields) {
        return false;
    }
    out.writes.push_back(write);
    return true;
}

}  // namespace

bool scanPlainWriteBatch(std::string_view body, PlainWriteBatch& out, size_t maxWrites) {
    out.clear();
    // Offsets into the flat arrays are 32-bit.
    if (body.size() >= (size_t{1} << 32)) {
        return false;
    }
    const char* p = body.data();
    const char* const end = p + body.size();

    std::string_view key;
    if (!expect(p, end, '{') || !readPlainString(p, end, key) || key != "writes" || !expect(p, end, ':') ||
        !expect(p, end, '[')) {
        return false;
    }
    // It is a batch: size the arrays once from the body (a scalar write runs
    // ~100-140 bytes; undershooting just grows).
    out.writes.reserve(body.size() / 96 + 1);
    out.tags.reserve(body.size() / 64 + 1);
    out.fields.reserve(body.size() / 96 + 1);
    for (;;) {
        if (out.writes.size() >= maxWrites || !readWrite(p, end, out)) {
            return false;
        }
        skipWs(p, end);
        if (p >= end) {
            return false;
        }
        if (*p == ',') {
            ++p;
            continue;
        }
        if (*p == ']') {
            ++p;
            break;
        }
        return false;
    }
    if (!expect(p, end, '}')) {
        return false;
    }
    skipWs(p, end);
    return p == end;
}

}  // namespace timestar::http
