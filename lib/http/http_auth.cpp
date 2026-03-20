#include "http_auth.hpp"

#include "json_escape.hpp"

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>
#include <string_view>

namespace timestar {

bool constantTimeEquals(std::string_view a, std::string_view b) {
    // Constant-time comparison: always iterate max(a.size(), b.size()) to avoid
    // leaking either token's length through timing. The XOR accumulator ensures
    // we don't short-circuit on the first differing byte.
    uint8_t result = static_cast<uint8_t>(a.size() != b.size());

    const size_t len = std::max(a.size(), b.size());
    for (size_t i = 0; i < len; ++i) {
        uint8_t ca = (i < a.size()) ? static_cast<uint8_t>(a[i]) : 0;
        uint8_t cb = (i < b.size()) ? static_cast<uint8_t>(b[i]) : 0;
        result |= ca ^ cb;
    }

    // Compiler barrier: prevent the optimizer from converting the accumulated
    // result into a branch (volatile alone is not sufficient in all compilers).
    asm volatile("" : "+r"(result));

    return result == 0;
}

std::string generateToken(uint32_t numBytes) {
    std::random_device rd;  // Reads from /dev/urandom on Linux
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (uint32_t i = 0; i < numBytes; ++i) {
        // random_device returns unsigned int; take low 8 bits as one random byte
        oss << std::setw(2) << (rd() & 0xFF);
    }
    return oss.str();
}

std::unique_ptr<seastar::http::reply> make401Reply(const std::string& message) {
    auto rep = std::make_unique<seastar::http::reply>();
    rep->set_status(seastar::http::reply::status_type::unauthorized);
    rep->add_header("Content-Type", "application/json");
    rep->add_header("WWW-Authenticate", "Bearer");
    rep->_content = "{\"status\":\"error\",\"message\":\"" + timestar::jsonEscape(message) + "\"}";
    return rep;
}

std::unique_ptr<seastar::http::reply> checkAuth(const seastar::http::request& req, const std::string& expectedToken) {
    auto it = req._headers.find("Authorization");
    if (it == req._headers.end()) {
        return make401Reply("Missing or malformed Authorization header");
    }

    const std::string& header = it->second;
    constexpr std::string_view prefix = "Bearer ";
    // The "Bearer " prefix is public and non-secret; a plain starts_with is
    // fine here.  Constant-time comparison is only needed for the token itself.
    if (header.size() <= prefix.size() || !std::string_view(header).starts_with(prefix)) {
        return make401Reply("Missing or malformed Authorization header");
    }

    std::string_view token = std::string_view(header).substr(prefix.size());
    if (!constantTimeEquals(token, expectedToken)) {
        return make401Reply("Invalid bearer token");
    }

    return nullptr;
}

std::string maskToken(std::string_view token) {
    constexpr size_t kPrefixLen = 4;
    constexpr size_t kSuffixLen = 4;
    constexpr size_t kMinLen = kPrefixLen + kSuffixLen + 1;  // need at least 1 char to mask

    if (token.size() < kMinLen) {
        // Token too short to reveal any portion safely — mask entirely.
        return "***";
    }

    std::string masked;
    masked.reserve(kPrefixLen + 3 + kSuffixLen);
    masked.append(token.substr(0, kPrefixLen));
    masked.append("***");
    masked.append(token.substr(token.size() - kSuffixLen));
    return masked;
}

}  // namespace timestar
