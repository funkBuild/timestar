#pragma once

#include "../../core/vshard.hpp"
#include "../raft/raft_types.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace timestar::cluster {

// These values are deliberately not part of ClusterConfig. They exist only so
// a production-server acceptance gate can die at a precise durable boundary,
// then restart the unmodified server over the resulting files. Keeping the
// parser here makes the fail-closed safety checks independently testable.
enum class RetirementCrashPoint {
    EngineWalGenerationDeleted,
    JournalQuarantined,
};

struct RetirementCrashSpec {
    uint16_t vshard = 0;
    RetirementCrashPoint point = RetirementCrashPoint::EngineWalGenerationDeleted;

    friend bool operator==(const RetirementCrashSpec&, const RetirementCrashSpec&) = default;
};

inline constexpr int kRetirementCrashExitCode = 86;

inline std::optional<RetirementCrashSpec> parseRetirementCrashSpec(const char* pointValue, const char* vshardValue,
                                                                   bool developmentInsecureTransport,
                                                                   bool controlEnabled) {
    const bool havePoint = pointValue && *pointValue;
    const bool haveVShard = vshardValue && *vshardValue;
    if (!havePoint && !haveVShard)
        return std::nullopt;
    if (!havePoint || !haveVShard)
        throw std::invalid_argument(
            "TIMESTAR_UNSAFE_TEST_RETIREMENT_CRASH and TIMESTAR_UNSAFE_TEST_RETIREMENT_VSHARD must be set together");
    if (!developmentInsecureTransport || !controlEnabled)
        throw std::invalid_argument(
            "the unsafe retirement crash failpoint requires local insecure transport and Group-0 control");

    const std::string_view point(pointValue);
    RetirementCrashPoint parsedPoint;
    if (point == "engine-wal-generation-deleted") {
        parsedPoint = RetirementCrashPoint::EngineWalGenerationDeleted;
    } else if (point == "journal-quarantined") {
        parsedPoint = RetirementCrashPoint::JournalQuarantined;
    } else {
        throw std::invalid_argument(
            "TIMESTAR_UNSAFE_TEST_RETIREMENT_CRASH must be engine-wal-generation-deleted or journal-quarantined");
    }

    const std::string_view encodedVShard(vshardValue);
    uint32_t parsedVShard = 0;
    const auto [end, error] =
        std::from_chars(encodedVShard.data(), encodedVShard.data() + encodedVShard.size(), parsedVShard);
    if (error != std::errc{} || end != encodedVShard.data() + encodedVShard.size() ||
        parsedVShard >= VIRTUAL_SHARD_COUNT) {
        throw std::invalid_argument("TIMESTAR_UNSAFE_TEST_RETIREMENT_VSHARD must be a decimal VShard in [0,4096)");
    }

    return RetirementCrashSpec{static_cast<uint16_t>(parsedVShard), parsedPoint};
}

// A production-server topology gate needs a small drain victim without reducing
// the real 4096-VShard namespace. This test-only override assigns every initial
// VShard to one explicit replica set; the gate then moves exactly one VShard to
// its spare node before draining it. It is intentionally guarded by the same
// development-only transport escape hatch as the crash failpoints.
inline std::optional<std::vector<raft::NodeId>> parseInitialReplicaSetForTesting(const char* value,
                                                                                 bool developmentInsecureTransport,
                                                                                 bool controlEnabled,
                                                                                 uint16_t replicationFactor,
                                                                                 size_t peerCount) {
    if (!value || !*value)
        return std::nullopt;
    if (!developmentInsecureTransport || !controlEnabled)
        throw std::invalid_argument(
            "TIMESTAR_UNSAFE_TEST_INITIAL_REPLICAS requires local insecure transport and Group-0 control");

    std::vector<raft::NodeId> replicas;
    std::string_view remaining(value);
    while (!remaining.empty()) {
        const size_t comma = remaining.find(',');
        const std::string_view token = remaining.substr(0, comma);
        uint64_t parsed = 0;
        const auto [end, error] = std::from_chars(token.data(), token.data() + token.size(), parsed);
        if (token.empty() || error != std::errc{} || end != token.data() + token.size() || parsed == 0 ||
            parsed > peerCount)
            throw std::invalid_argument(
                "TIMESTAR_UNSAFE_TEST_INITIAL_REPLICAS must be comma-separated configured node IDs");
        const auto node = static_cast<raft::NodeId>(parsed);
        if (std::find(replicas.begin(), replicas.end(), node) != replicas.end())
            throw std::invalid_argument("TIMESTAR_UNSAFE_TEST_INITIAL_REPLICAS must not contain duplicate nodes");
        replicas.push_back(node);
        if (comma == std::string_view::npos)
            break;
        if (comma + 1 == remaining.size())
            throw std::invalid_argument(
                "TIMESTAR_UNSAFE_TEST_INITIAL_REPLICAS must be comma-separated configured node IDs");
        remaining.remove_prefix(comma + 1);
    }
    if (replicas.size() != replicationFactor)
        throw std::invalid_argument(
            "TIMESTAR_UNSAFE_TEST_INITIAL_REPLICAS must contain exactly replication_factor nodes");
    return replicas;
}

}  // namespace timestar::cluster
