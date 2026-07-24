#pragma once

#include "data_plane.hpp"

#include <optional>
#include <string>

namespace timestar::data {

// Wire serialization for the data-plane RPC payloads (write batches, query specs,
// query partials). Compact little-endian, length-prefixed, self-delimiting;
// every decode is bounds-checked and returns nullopt on malformed input so a
// corrupt/hostile frame can never fabricate a value. (mTLS/protobuf are transport
// details the plan names; this codec carries the same data over seastar::rpc.)

std::string encodePoints(const std::vector<DataPoint>& points);
std::optional<std::vector<DataPoint>> decodePoints(const std::string& bytes);

std::string encodeQuerySpec(const QuerySpec& spec);
std::optional<QuerySpec> decodeQuerySpec(const std::string& bytes);

std::string encodeQueryPartial(const QueryPartial& part);
std::optional<QueryPartial> decodeQueryPartial(const std::string& bytes);

}  // namespace timestar::data
