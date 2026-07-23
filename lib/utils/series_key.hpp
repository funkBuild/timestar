#pragma once

#include <map>
#include <string>

namespace timestar {

// Characters that are structural in the canonical series-key encoding
// ("measurement,tag=val field").  Each must be backslash-escaped inside a
// component so the encoding stays unambiguous (bijective): a name containing a
// delimiter cannot be mistaken for the delimiter itself.  '"' is escaped too so
// a literal quote in a name is not mistaken for SeriesKeyParser's legacy quoting.
//
// NUL ('\0') is deliberately NOT handled here — it is rejected at write-time
// validation because it is the separator byte inside index KV keys.  Every other
// byte (alphanumeric, spaces, symbols) is allowed and round-trips.
inline void appendEscapedSeriesKeyComponent(std::string& out, const std::string& s) {
    for (char c : s) {
        if (c == '\\' || c == '"' || c == ',' || c == '=' || c == ' ')
            out += '\\';
        out += c;
    }
}

// Build a canonical series key from measurement + sorted tags + field.
// Format: "measurement,tag1=val1,tag2=val2 field", with the structural
// characters (',', '=', ' ', '"', '\\') backslash-escaped inside each component.
// The escaping makes the encoding bijective — distinct (measurement, tags, field)
// tuples always produce distinct keys even when names contain spaces or commas
// (e.g. ("a b",{},"c") and ("a",{},"b c") no longer both encode to "a b c").
// The inverse is SeriesKeyParser (lib/utils/line_parser.cpp).
//
// Backward-compatible: a component with no structural characters is appended
// verbatim, so keys for existing (space/comma-free) series are byte-identical to
// before and hash to the same SeriesId128 — no data migration required.
//
// Used for deduplication, sharding, and series identification.
inline std::string buildSeriesKey(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                  const std::string& field) {
    // Reserve the unescaped size as a hint; escaping only grows the buffer for
    // the rare component that actually contains a structural character.
    size_t totalSize = measurement.size() + 1 + field.size();
    for (const auto& [k, v] : tags) {
        totalSize += 2 + k.size() + v.size();
    }

    std::string key;
    key.reserve(totalSize);
    appendEscapedSeriesKeyComponent(key, measurement);
    for (const auto& [k, v] : tags) {
        key += ',';
        appendEscapedSeriesKeyComponent(key, k);
        key += '=';
        appendEscapedSeriesKeyComponent(key, v);
    }
    key += ' ';
    appendEscapedSeriesKeyComponent(key, field);
    return key;
}

}  // namespace timestar
