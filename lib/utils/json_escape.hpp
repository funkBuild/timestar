#pragma once

#include <cstdio>
#include <string>

namespace timestar {

// Append a JSON-escaped version of `s` to `out`.
// Escapes: " \ \n \r \t and control chars (< 0x20 or DEL 0x7F) as \uXXXX.
// Hot-path callers should use this to avoid extra allocations.
inline void jsonEscapeAppend(const std::string& s, std::string& out) {
    for (unsigned char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (c < 0x20 || c == 0x7F) [[unlikely]] {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
                break;
        }
    }
}

// Return a new JSON-escaped string.
// Convenience wrapper around jsonEscapeAppend for cold paths.
inline std::string jsonEscape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 4);
    jsonEscapeAppend(s, out);
    return out;
}

}  // namespace timestar
