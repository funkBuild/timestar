#pragma once

#include <cstdint>
#include <memory>
#include <seastar/http/httpd.hh>
#include <string>

namespace timestar {

// Constant-time string comparison to prevent timing side-channel attacks.
// Returns true if both strings are equal.
bool constantTimeEquals(std::string_view a, std::string_view b);

// Generate a cryptographically random hex token using /dev/urandom.
// numBytes is the number of random bytes (output will be 2*numBytes hex chars).
std::string generateToken(uint32_t numBytes = 32);

// Check the Authorization header on an incoming request.
// Returns nullptr if the request is authorized (token matches).
// Returns a 401 reply if the request is unauthorized.
std::unique_ptr<seastar::http::reply> checkAuth(const seastar::http::request& req, const std::string& expectedToken);

// Build a 401 Unauthorized reply with JSON body and WWW-Authenticate header.
std::unique_ptr<seastar::http::reply> make401Reply(const std::string& message);

// Mask a token for safe logging: show first 4 and last 4 characters, replace
// the middle with "***".  Tokens shorter than 12 characters are fully masked.
// Example: "abcd1234efgh5678" -> "abcd***5678"
std::string maskToken(std::string_view token);

}  // namespace timestar
