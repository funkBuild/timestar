#pragma once

#include "../../core/vshard.hpp"

#include <charconv>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string_view>

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

}  // namespace timestar::cluster
