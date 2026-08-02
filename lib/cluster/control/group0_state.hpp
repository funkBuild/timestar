#pragma once

#include "control_map_cache.hpp"
#include "../raft/raft_types.hpp"  // NodeId

#include <algorithm>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace timestar::control {

using timestar::raft::NodeId;

// A node's lifecycle state in the cluster (the plan's node lifecycle).
enum class NodeState : uint8_t { Joining = 0, Active = 1, Draining = 2, Down = 3 };

inline constexpr size_t kMaxJoinTokenBytes = 1024;
inline constexpr size_t kMaxOutstandingJoinTokens = 1024;

inline bool validJoinToken(const std::string& token) {
    return !token.empty() && token.size() <= kMaxJoinTokenBytes;
}

constexpr bool isValidNodeState(NodeState state) {
    return state >= NodeState::Joining && state <= NodeState::Down;
}

// One member node's control-plane record (key family nodes/<uuid>). raftId is
// the stable id the Raft groups use; uuid identifies the node across restarts;
// failureDomain (rack/az) drives cross-domain meta-voter selection.
struct NodeRecord {
    NodeId raftId = 0;
    std::string uuid;
    std::string address;  // inter-node RPC address
    std::string failureDomain;
    NodeState state = NodeState::Joining;

    friend bool operator==(const NodeRecord&, const NodeRecord&) = default;
};

// A persisted controller job (key family jobs/<uuid>). Job steps are idempotent
// so the next group-0 leader resumes after a crash; `done` marks completion.
struct Job {
    std::string id;
    uint32_t step = 0;
    bool done = false;
    std::string payload;

    friend bool operator==(const Job&, const Job&) = default;
};

// A compare-and-set cell (key families schema/... and retention/...): version is
// bumped on each successful set so a CAS can guard against a lost update.
struct PolicyCell {
    uint64_t version = 0;
    std::string value;

    friend bool operator==(const PolicyCell&, const PolicyCell&) = default;
};

// One exact target captured by a quorum-fenced pattern expansion. Group 0
// freezes the complete canonical target vector before any data-group mutation,
// so an HTTP retry can never expand into a series created after the first
// attempt.
struct FrozenDeleteTarget {
    std::string seriesKey;
    uint64_t startTime = 0;
    uint64_t endTime = 0;

    friend bool operator==(const FrozenDeleteTarget&, const FrozenDeleteTarget&) = default;
    friend bool operator<(const FrozenDeleteTarget& lhs, const FrozenDeleteTarget& rhs) {
        return std::tie(lhs.seriesKey, lhs.startTime, lhs.endTime) <
               std::tie(rhs.seriesKey, rhs.startTime, rhs.endTime);
    }
};

struct FrozenDeletePlan {
    std::string requestId;           // canonical 32-hex Idempotency-Key
    std::string requestFingerprint;  // canonical 32-hex hash of the original request bytes
    uint64_t issuedAtMs = 0;
    std::vector<FrozenDeleteTarget> targets;  // sorted, unique; empty is a meaningful frozen no-op

    friend bool operator==(const FrozenDeletePlan&, const FrozenDeletePlan&) = default;
};

enum class FreezeDeletePlanStatus : uint8_t {
    Stored = 0,
    NotFound = 1,
    NotLeader = 2,
    Conflict = 3,
    Capacity = 4,
    Invalid = 5
};

struct FreezeDeletePlanResult {
    FreezeDeletePlanStatus status = FreezeDeletePlanStatus::Invalid;
    FrozenDeletePlan plan;
};

inline constexpr size_t kMaxFrozenDeletePlanTargets = 10'000;
inline constexpr size_t kMaxFrozenDeletePlanBytes = 512u << 10;
inline constexpr size_t kMaxFrozenDeletePlans = 1'024;
inline constexpr size_t kMaxFrozenDeletePlanAggregateBytes = 16u << 20;
inline constexpr uint64_t kFrozenDeletePlanRetentionMs = 60u * 60u * 1'000u;
inline constexpr uint64_t kFrozenDeletePlanFutureSkewMs = 5u * 60u * 1'000u;

inline size_t frozenDeletePlanBytes(const FrozenDeletePlan& plan) {
    // Include the ControlCommand tag so the per-entry limit describes the
    // complete encoded Raft payload, not merely the nested plan fields.
    size_t bytes = sizeof(uint8_t) + sizeof(uint64_t) * 4 + plan.requestId.size() +
                   plan.requestFingerprint.size();
    for (const auto& target : plan.targets) {
        const size_t itemBytes = sizeof(uint64_t) * 3 + target.seriesKey.size();
        if (bytes > std::numeric_limits<size_t>::max() - itemBytes)
            return std::numeric_limits<size_t>::max();
        bytes += itemBytes;
    }
    return bytes;
}

inline bool validFrozenDeletePlan(const FrozenDeletePlan& plan) {
    const auto canonicalHex128 = [](const std::string& value) {
        return value.size() == 32 && std::ranges::all_of(value, [](unsigned char c) {
                   return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
               });
    };
    if (!canonicalHex128(plan.requestId) || !canonicalHex128(plan.requestFingerprint) || plan.issuedAtMs == 0 ||
        plan.targets.size() > kMaxFrozenDeletePlanTargets || frozenDeletePlanBytes(plan) > kMaxFrozenDeletePlanBytes)
        return false;
    for (size_t i = 0; i < plan.targets.size(); ++i) {
        const auto& target = plan.targets[i];
        if (target.seriesKey.empty() || target.startTime > target.endTime ||
            (i != 0 && !(plan.targets[i - 1] < target)))
            return false;
    }
    return true;
}

inline bool sameFrozenDeleteRequest(const FrozenDeletePlan& lhs, const FrozenDeletePlan& rhs) {
    return lhs.requestId == rhs.requestId && lhs.requestFingerprint == rhs.requestFingerprint &&
           lhs.issuedAtMs == rhs.issuedAtMs;
}

// State-machine time is carried by the command rather than read from a wall
// clock. The extra future-skew interval prevents a legally future-dated request
// from retiring another request before its HTTP retry window has elapsed.
inline bool frozenDeletePlanExpiredAt(const FrozenDeletePlan& plan, uint64_t candidateIssuedAtMs) {
    constexpr uint64_t kRetainFor = kFrozenDeletePlanRetentionMs + kFrozenDeletePlanFutureSkewMs;
    const uint64_t retireThrough = candidateIssuedAtMs > kRetainFor ? candidateIssuedAtMs - kRetainFor : 0;
    return plan.issuedAtMs <= retireThrough;
}

// The full replicated group-0 control state. This is a deterministic function of
// the committed command log; every field is rebuilt identically on every node.
struct Group0State {
    std::string clusterUuid;                                   // meta
    uint64_t mapEpoch = 0;                                     // topology/policy version
    uint64_t appliedIndex = 0;                                 // last group-0 log index applied
    uint64_t controllerTerm = 0;                               // controller epoch (group-0 term)
    NodeId controllerLeader = 0;                               // node that owns controllerTerm
    std::map<NodeId, NodeRecord> nodes;                        // nodes/<uuid>
    std::map<uint16_t, std::vector<NodeId>> desiredPlacement;  // desired-placement/<vshard>
    ControlMap servingMap;                                     // immutable initial effective serving map
    std::vector<NodeId> metaVoters;                            // group-0 voter set (self-managed)
    std::map<std::string, PolicyCell> policies;                // schema/retention CAS cells
    std::map<std::string, Job> jobs;                           // jobs/<uuid>
    std::set<std::string> joinTokens;                          // valid unused group-0-minted join tokens
    std::map<std::string, FrozenDeletePlan> frozenDeletePlans; // retry-stable pattern expansion plans
    friend bool operator==(const Group0State&, const Group0State&) = default;
};

}  // namespace timestar::control
