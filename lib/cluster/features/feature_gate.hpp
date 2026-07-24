#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <vector>

namespace timestar::features {

// The range of wire/storage format versions a node can speak (docs/clustering.md
// §"Networking, security, and compatibility": min/max wire-version negotiation).
struct VersionRange {
    uint32_t min = 1;
    uint32_t max = 1;
    bool supports(uint32_t v) const { return v >= min && v <= max; }
};

// Two nodes can talk iff their supported ranges OVERLAP (adjacent compatible
// versions).
inline bool compatible(const VersionRange& a, const VersionRange& b) {
    return a.min <= b.max && b.min <= a.max;
}

// The version two nodes should speak: the highest both support (nullopt if none).
inline std::optional<uint32_t> negotiate(const VersionRange& a, const VersionRange& b) {
    if (!compatible(a, b))
        return std::nullopt;
    return std::min(a.max, b.max);
}

// Rolling-upgrade feature gating (decision 8): "A group does not activate a
// storage or log format until every current voter can read it and the feature
// gate is committed." This is the safety precondition; the activation itself is a
// committed group-0 command. A voter that cannot read a format is NEVER sent data
// in it, because the group only ever operates at a version the whole voter set
// supports.
class FeatureGate {
public:
    // May the group activate format version `v`? Only if EVERY current voter
    // supports it. An empty voter set can activate nothing.
    static bool canActivate(uint32_t v, const std::vector<VersionRange>& voters) {
        if (voters.empty())
            return false;
        for (const auto& r : voters)
            if (!r.supports(v))
                return false;
        return true;
    }

    // The highest format version the WHOLE voter set can speak -- the safe active
    // version. It is `min(maxes)`, valid only if that version is also >= every
    // voter's min (i.e. all ranges overlap there). nullopt if there is no common
    // version (an incompatible voter must be upgraded/removed before it counts).
    static std::optional<uint32_t> maxCommonVersion(const std::vector<VersionRange>& voters) {
        if (voters.empty())
            return std::nullopt;
        uint32_t lo = voters[0].min, hi = voters[0].max;
        for (const auto& r : voters) {
            lo = std::max(lo, r.min);
            hi = std::min(hi, r.max);
        }
        if (lo > hi)
            return std::nullopt;  // no version every voter supports
        return hi;
    }
};

}  // namespace timestar::features
