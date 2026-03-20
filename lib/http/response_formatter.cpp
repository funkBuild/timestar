#include "response_formatter.hpp"

// Glaze's optimised number formatters:
//   itoa: digit-pair lookup tables + 128-bit mul-shift division  (~2x std::to_chars)
//   dtoa: Dragonbox shortest-representation algorithm             (~1.5x std::to_chars)
#include <glaze/util/dtoa.hpp>
#include <glaze/util/itoa.hpp>

#include <cmath>
#include <cstring>

namespace timestar {

// ──────────────────────────────────────────────────────────────────────
// Buffer helper — direct char* writes, no per-element string::append()
// ──────────────────────────────────────────────────────────────────────
//
// All array serialisation writes through a raw char* pointer into a
// pre-sized std::string.  The string is grown with resize() when needed
// (which zero-fills on growth) and trimmed with resize() at the end
// (which just moves the NUL terminator — no copy).

namespace {

// Maximum bytes a single numeric value can produce (sign + 20 digits + dot + 17 mantissa + e + sign + 3 exp)
static constexpr size_t MAX_NUM_LEN = 32;

// ── JSON string helpers ─────────────────────────────────────────────

// Returns true if `sv` contains a character that requires JSON escaping.
// Measurement names, tag keys/values, and field names in TSDB data are
// almost always plain ASCII identifiers — this lets us skip per-char
// processing for the common case.
inline bool needsEscaping(std::string_view sv) noexcept {
    for (char c : sv) {
        if (static_cast<unsigned char>(c) < 0x20 || c == '"' || c == '\\') [[unlikely]] {
            return true;
        }
    }
    return false;
}

// Append a JSON-escaped string to `buf` starting at position `pos`.
// Returns the new position.
inline size_t appendJsonStringEscaped(std::string& buf, size_t pos, std::string_view sv) {
    // Worst case: every char becomes \uXXXX (6 bytes) + 2 quotes
    size_t worstCase = sv.size() * 6 + 2;
    if (pos + worstCase > buf.size()) {
        buf.resize(std::max(buf.size() * 2, pos + worstCase));
    }
    char* p = buf.data() + pos;
    *p++ = '"';
    for (char c : sv) {
        switch (c) {
            case '"':
                *p++ = '\\';
                *p++ = '"';
                break;
            case '\\':
                *p++ = '\\';
                *p++ = '\\';
                break;
            case '\n':
                *p++ = '\\';
                *p++ = 'n';
                break;
            case '\r':
                *p++ = '\\';
                *p++ = 'r';
                break;
            case '\t':
                *p++ = '\\';
                *p++ = 't';
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // \uXXXX
                    static constexpr char hex[] = "0123456789abcdef";
                    *p++ = '\\';
                    *p++ = 'u';
                    *p++ = '0';
                    *p++ = '0';
                    *p++ = hex[(static_cast<unsigned char>(c) >> 4) & 0xf];
                    *p++ = hex[static_cast<unsigned char>(c) & 0xf];
                } else {
                    *p++ = c;
                }
        }
    }
    *p++ = '"';
    return static_cast<size_t>(p - buf.data());
}

// Append a JSON string to `buf` at position `pos`.
// Fast path: if no escaping needed, direct memcpy.
inline size_t appendJsonString(std::string& buf, size_t pos, std::string_view sv) {
    if (!needsEscaping(sv)) [[likely]] {
        size_t need = sv.size() + 2;  // quotes
        if (pos + need > buf.size()) {
            buf.resize(std::max(buf.size() * 2, pos + need));
        }
        char* p = buf.data() + pos;
        *p++ = '"';
        std::memcpy(p, sv.data(), sv.size());
        p += sv.size();
        *p++ = '"';
        return pos + need;
    }
    return appendJsonStringEscaped(buf, pos, sv);
}

// Append raw bytes (no escaping).
inline size_t appendRaw(std::string& buf, size_t pos, std::string_view sv) {
    if (pos + sv.size() > buf.size()) {
        buf.resize(std::max(buf.size() * 2, pos + sv.size()));
    }
    std::memcpy(buf.data() + pos, sv.data(), sv.size());
    return pos + sv.size();
}

inline size_t appendChar(std::string& buf, size_t pos, char c) {
    if (pos >= buf.size()) {
        buf.resize(std::max(buf.size() * 2, pos + 1));
    }
    buf[pos] = c;
    return pos + 1;
}

// ── Ensure buffer has room for N elements of at most MAX_NUM_LEN each ─

inline void ensureArraySpace(std::string& buf, size_t pos, size_t count) {
    // Each element: up to MAX_NUM_LEN digits + 1 comma
    size_t need = count * (MAX_NUM_LEN + 1) + 2;  // + brackets
    if (pos + need > buf.size()) {
        buf.resize(std::max(buf.size() * 2, pos + need));
    }
}

// ── Array serialisers ────────────────────────────────────────────────

// Write a uint64_t JSON array using glz::to_chars (digit-pair tables).
inline size_t appendUint64Array(std::string& buf, size_t pos, const std::vector<uint64_t>& arr) {
    ensureArraySpace(buf, pos, arr.size());
    char* p = buf.data() + pos;
    *p++ = '[';
    for (size_t i = 0; i < arr.size(); ++i) {
        if (i > 0)
            *p++ = ',';
        p = glz::to_chars(p, arr[i]);
    }
    *p++ = ']';
    return static_cast<size_t>(p - buf.data());
}

// Write a double JSON array using glz::to_chars (Dragonbox).
// NaN and Inf are written as JSON null.
inline size_t appendDoubleArray(std::string& buf, size_t pos, const std::vector<double>& arr) {
    ensureArraySpace(buf, pos, arr.size());
    char* p = buf.data() + pos;
    *p++ = '[';
    for (size_t i = 0; i < arr.size(); ++i) {
        if (i > 0)
            *p++ = ',';
        // Dragonbox handles NaN/Inf by writing "null" (4 bytes)
        p = glz::to_chars(p, arr[i]);
    }
    *p++ = ']';
    return static_cast<size_t>(p - buf.data());
}

// Write a bool JSON array.
inline size_t appendBoolArray(std::string& buf, size_t pos, const std::vector<bool>& arr) {
    // Max 6 bytes per element ("false,")
    size_t need = arr.size() * 6 + 2;
    if (pos + need > buf.size()) {
        buf.resize(std::max(buf.size() * 2, pos + need));
    }
    char* p = buf.data() + pos;
    *p++ = '[';
    for (size_t i = 0; i < arr.size(); ++i) {
        if (i > 0)
            *p++ = ',';
        if (arr[i]) {
            std::memcpy(p, "true", 4);
            p += 4;
        } else {
            std::memcpy(p, "false", 5);
            p += 5;
        }
    }
    *p++ = ']';
    return static_cast<size_t>(p - buf.data());
}

// Write a string JSON array.
inline size_t appendStringArray(std::string& buf, size_t pos, const std::vector<std::string>& arr) {
    pos = appendChar(buf, pos, '[');
    for (size_t i = 0; i < arr.size(); ++i) {
        if (i > 0)
            pos = appendChar(buf, pos, ',');
        pos = appendJsonString(buf, pos, arr[i]);
    }
    pos = appendChar(buf, pos, ']');
    return pos;
}

// Write an int64_t JSON array using glz::to_chars.
inline size_t appendInt64Array(std::string& buf, size_t pos, const std::vector<int64_t>& arr) {
    ensureArraySpace(buf, pos, arr.size());
    char* p = buf.data() + pos;
    *p++ = '[';
    for (size_t i = 0; i < arr.size(); ++i) {
        if (i > 0)
            *p++ = ',';
        p = glz::to_chars(p, arr[i]);
    }
    *p++ = ']';
    return static_cast<size_t>(p - buf.data());
}

// Dispatch FieldValues variant to the correct array writer.
inline size_t appendFieldValues(std::string& buf, size_t pos, const FieldValues& values) {
    return std::visit(
        [&](const auto& vec) -> size_t {
            using T = std::decay_t<decltype(vec)>;
            if constexpr (std::is_same_v<T, std::vector<double>>)
                return appendDoubleArray(buf, pos, vec);
            else if constexpr (std::is_same_v<T, std::vector<bool>>)
                return appendBoolArray(buf, pos, vec);
            else if constexpr (std::is_same_v<T, std::vector<std::string>>)
                return appendStringArray(buf, pos, vec);
            else if constexpr (std::is_same_v<T, std::vector<int64_t>>)
                return appendInt64Array(buf, pos, vec);
            else
                static_assert(sizeof(T) == 0, "unhandled FieldValues alternative — update appendFieldValues");
        },
        values);
}

}  // anonymous namespace

// ──────────────────────────────────────────────────────────────────────
// Public API
// ──────────────────────────────────────────────────────────────────────

std::string ResponseFormatter::format(QueryResponse& response) {
    // Pre-allocate: ~28 bytes per point (19-digit timestamp + comma + ~8 char value + comma).
    // Over-estimate slightly to avoid any reallocation for typical responses.
    size_t estimate = static_cast<size_t>(response.statistics.pointCount) * 28 + 512;
    std::string buf;
    buf.resize(estimate);
    size_t pos = 0;

    // {"status":"success","series":[
    constexpr std::string_view header = R"({"status":"success","series":[)";
    std::memcpy(buf.data(), header.data(), header.size());
    pos = header.size();

    bool firstSeries = true;
    for (auto& series : response.series) {
        if (series.fields.empty())
            continue;
        if (!firstSeries)
            pos = appendChar(buf, pos, ',');
        firstSeries = false;

        // {"measurement":"..."
        pos = appendRaw(buf, pos, R"({"measurement":)");
        pos = appendJsonString(buf, pos, series.measurement);

        // ,"groupTags":["key=val",...]
        pos = appendRaw(buf, pos, R"(,"groupTags":[)");
        bool firstTag = true;
        for (const auto& [key, value] : series.tags) {
            if (!firstTag)
                pos = appendChar(buf, pos, ',');
            firstTag = false;
            // Write "key=value" as a JSON string with proper escaping
            if (!needsEscaping(key) && !needsEscaping(value)) [[likely]] {
                // Fast path: no escaping needed
                size_t need = key.size() + value.size() + 4;
                if (pos + need > buf.size()) {
                    buf.resize(std::max(buf.size() * 2, pos + need));
                }
                char* p = buf.data() + pos;
                *p++ = '"';
                std::memcpy(p, key.data(), key.size());
                p += key.size();
                *p++ = '=';
                std::memcpy(p, value.data(), value.size());
                p += value.size();
                *p++ = '"';
                pos = static_cast<size_t>(p - buf.data());
            } else {
                // Slow path: escape key and/or value
                std::string kv;
                kv.reserve(key.size() + 1 + value.size());
                kv.append(key);
                kv.push_back('=');
                kv.append(value);
                pos = appendJsonString(buf, pos, kv);
            }
        }
        pos = appendChar(buf, pos, ']');

        // ,"fields":{...}
        pos = appendRaw(buf, pos, R"(,"fields":{)");
        bool firstField = true;
        for (auto& [fieldName, fieldData] : series.fields) {
            if (!firstField)
                pos = appendChar(buf, pos, ',');
            firstField = false;

            pos = appendJsonString(buf, pos, fieldName);
            pos = appendRaw(buf, pos, R"(:{"timestamps":)");
            pos = appendUint64Array(buf, pos, fieldData.first);
            pos = appendRaw(buf, pos, R"(,"values":)");
            pos = appendFieldValues(buf, pos, fieldData.second);
            pos = appendChar(buf, pos, '}');
        }
        pos = appendRaw(buf, pos, "}}");
    }

    // ],"statistics":{"series_count":N,"point_count":N,"execution_time_ms":N}}
    pos = appendRaw(buf, pos, R"(],"statistics":{"series_count":)");
    // Ensure room for the statistics footer (~120 bytes max)
    if (pos + 128 > buf.size()) {
        buf.resize(pos + 128);
    }
    char* p = buf.data() + pos;
    p = glz::to_chars(p, static_cast<int64_t>(response.statistics.seriesCount));
    constexpr std::string_view ptKey = R"(,"point_count":)";
    std::memcpy(p, ptKey.data(), ptKey.size());
    p += ptKey.size();
    p = glz::to_chars(p, static_cast<int64_t>(response.statistics.pointCount));
    constexpr std::string_view execKey = R"(,"execution_time_ms":)";
    std::memcpy(p, execKey.data(), execKey.size());
    p += execKey.size();
    p = glz::to_chars(p, response.statistics.executionTimeMs);
    *p++ = '}';
    *p++ = '}';
    pos = static_cast<size_t>(p - buf.data());

    buf.resize(pos);
    return buf;
}

std::string ResponseFormatter::formatError(const std::string& message, const std::string& code) {
    std::string buf;
    buf.resize(message.size() * 2 + code.size() + 96);
    size_t pos = 0;

    pos = appendRaw(buf, pos, R"({"status":"error")");
    if (!code.empty()) {
        pos = appendRaw(buf, pos, R"(,"error_code":)");
        pos = appendJsonString(buf, pos, code);
    }
    pos = appendRaw(buf, pos, R"(,"message":)");
    pos = appendJsonString(buf, pos, message);
    pos = appendRaw(buf, pos, R"(,"error":)");
    pos = appendJsonString(buf, pos, message);
    pos = appendChar(buf, pos, '}');

    buf.resize(pos);
    return buf;
}

}  // namespace timestar
