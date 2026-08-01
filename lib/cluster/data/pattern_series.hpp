#pragma once

#include "dataplane_limits.hpp"
#include "node_query.hpp"    // VShardRedirect
#include "write_record.hpp"  // kWriteBatchFormatV4

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::data {

// Pattern expansion is a catalog READ that precedes replicated exact-range
// commands. It is available only on the same v4 peers that can resolve an RF<N
// leader read: an older peer has no verb for this request and must be refused,
// never mistaken for an empty catalog contribution.
inline constexpr uint32_t kPatternSeriesMinVersion = kWriteBatchFormatV4;
static_assert(kPatternSeriesMinVersion <= kWriteBatchFormatMax);

// Count bounds do not protect the receiver when catalog keys are unusually
// large. Keep the key-vector portion below the ordinary outbound frame limit,
// leaving ample room for the checksum and all 4,096 possible redirects.
inline constexpr size_t kPatternSeriesMaxKeyBytes = size_t{8} << 20;
inline constexpr uint32_t kPatternSeriesMaxResults = 10'000;
static_assert(kPatternSeriesMaxKeyBytes < kMaxOutboundFrameBytes);

struct PatternSeriesSelector {
    std::string measurement;
    std::map<std::string, std::string> tags;
    // Empty means every field. Order is not semantically significant.
    std::vector<std::string> fields;
};

// One node's VShard-restricted catalog request. `resolveVShards` has the same
// contract as NodeQueryRequest: it is the subset for which the receiving holder
// must prove it is the current leader or return a redirect.
struct PatternSeriesRequest {
    PatternSeriesSelector selector;
    std::vector<uint16_t> vshards;
    std::vector<uint16_t> resolveVShards;
    uint64_t mapEpoch = 0;
    uint32_t maxSeries = 0;
};

struct PatternSeriesResult {
    std::vector<std::string> seriesKeys;
    std::vector<VShardRedirect> redirects;
    bool limitExceeded = false;
};

std::string encodePatternSeriesRequest(const PatternSeriesRequest& request);
std::optional<PatternSeriesRequest> decodePatternSeriesRequest(const std::string& bytes);
std::string encodePatternSeriesResult(const PatternSeriesResult& result);
std::optional<PatternSeriesResult> decodePatternSeriesResult(const std::string& bytes);

class PatternSeriesUnsupportedError : public std::runtime_error {
public:
    explicit PatternSeriesUnsupportedError(const std::string& what) : std::runtime_error(what) {}
};

class DeleteExpansionLimitError : public std::runtime_error {
public:
    explicit DeleteExpansionLimitError(const std::string& what) : std::runtime_error(what) {}
};

}  // namespace timestar::data
