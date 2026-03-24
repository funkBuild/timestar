#pragma once

#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <string_view>

namespace timestar::http {

enum class WireFormat : uint8_t { Json, Protobuf };

/// Determine request body format from Content-Type header.
/// "application/x-protobuf" or "application/protobuf" -> Protobuf
/// "application/json", empty, or anything else -> Json (backward compatible)
WireFormat requestFormat(const seastar::http::request& req);

/// Determine desired response format from Accept header.
/// "application/x-protobuf" or "application/protobuf" -> Protobuf
/// If Accept is not set, falls back to requestFormat() (echo convention).
/// Otherwise Json.
WireFormat responseFormat(const seastar::http::request& req);

/// Set Content-Type on reply based on format.
/// Json  -> "application/json"
/// Protobuf -> "application/x-protobuf"
void setContentType(seastar::http::reply& rep, WireFormat fmt);

/// Convenience predicate.
inline constexpr bool isProtobuf(WireFormat f) noexcept {
    return f == WireFormat::Protobuf;
}

// -- Testable helpers that operate on plain string_views --

/// Parse a Content-Type or Accept header value into a WireFormat.
/// Matching is case-insensitive and ignores parameters after ';'.
WireFormat parseMediaType(std::string_view headerValue);

}  // namespace timestar::http
