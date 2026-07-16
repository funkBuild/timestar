#pragma once

#include <glaze/glaze.hpp>

#include <algorithm>
#include <string>
#include <string_view>

namespace timestar::http {

// Canonical JSON error body shared by all HTTP handlers:
//
//   {"status":"error","error_code":"<code>","message":"<msg>","error":"<msg>"}
//
// - "error" carries the human-readable message as a plain string; this is the
//   field clients assert on.  "message" mirrors it for backwards compatibility.
// - "error_code" is included only when a machine-readable code is supplied.
// - Messages are capped at 4 KB to bound allocation from pathological inputs.
inline std::string jsonError(std::string_view message, std::string_view code = {}) {
    constexpr size_t kMaxErrorMessageLen = 4096;
    const std::string_view msg = message.substr(0, std::min(message.size(), kMaxErrorMessageLen));

    std::string buffer;
    const auto ec =
        code.empty()
            ? glz::write_json(glz::obj{"status", "error", "message", msg, "error", msg}, buffer)
            : glz::write_json(glz::obj{"status", "error", "error_code", code, "message", msg, "error", msg}, buffer);
    if (ec) {
        return R"({"status":"error","message":"Failed to serialize error response","error":"Failed to serialize error response"})";
    }
    return buffer;
}

}  // namespace timestar::http
