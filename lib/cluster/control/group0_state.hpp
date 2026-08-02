#pragma once

#include "../raft/raft_types.hpp"  // NodeId
#include "control_map_cache.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace timestar::control {

using timestar::raft::NodeId;

// A node's lifecycle state in the cluster (the plan's node lifecycle).
enum class NodeState : uint8_t { Joining = 0, Active = 1, Draining = 2, Removed = 3 };

inline constexpr size_t kMaxJoinTokenBytes = 1024;
inline constexpr size_t kMaxOutstandingJoinTokens = 1024;
inline constexpr size_t kMaxControlJobIdBytes = 256;
// v1 retains only the current/latest movement proof. Planning the next move
// atomically replaces the completed predecessor, so evacuating thousands of
// VShards cannot grow every Group-0 snapshot without bound.
inline constexpr size_t kMaxControlJobs = 1;
inline constexpr std::string_view kRetentionPolicyPrefix = "retention/";
inline constexpr size_t kMaxRetentionPolicies = 1024;
inline constexpr size_t kMaxRetentionMeasurementBytes = 1024;
inline constexpr size_t kMaxRetentionTtlBytes = 64;
inline constexpr size_t kMaxPolicyKeyBytes = 2048;
inline constexpr size_t kMaxPolicyValueBytes = 64 * 1024;
// Revisit a policy no more often than the standalone sweep interval. A new
// policy is eligible immediately; later cutoffs must advance by this much.
inline constexpr uint64_t kRetentionSweepIntervalNanos = 15ULL * 60 * 1'000'000'000;
inline constexpr uint32_t kRetentionFanoutBatch = 32;

inline bool validControlJobId(const std::string& id) {
    return !id.empty() && id.size() <= kMaxControlJobIdBytes;
}

inline bool validJoinToken(const std::string& token) {
    return !token.empty() && token.size() <= kMaxJoinTokenBytes;
}

constexpr bool isValidNodeState(NodeState state) {
    return state >= NodeState::Joining && state <= NodeState::Removed;
}

constexpr bool isForwardNodeStateTransition(NodeState from, NodeState to) {
    return (from == NodeState::Joining && to == NodeState::Active) ||
           (from == NodeState::Active && to == NodeState::Draining) ||
           (from == NodeState::Draining && to == NodeState::Removed);
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

// Exact-v1 TTL policy stored in the generic retention/<measurement> CAS cell.
// Cluster downsampling is deliberately absent: it remains disabled until it is
// itself an ordered replicated data operation.
struct RetentionPolicyValue {
    std::string ttl;
    uint64_t ttlNanos = 0;

    friend bool operator==(const RetentionPolicyValue&, const RetentionPolicyValue&) = default;
};

inline bool validRetentionMeasurement(std::string_view measurement) {
    if (measurement.empty() || measurement.size() > kMaxRetentionMeasurementBytes)
        return false;
    return std::ranges::none_of(measurement, [](unsigned char c) { return c < 0x20 || c == 0x7f; });
}

inline std::string retentionPolicyKey(std::string_view measurement) {
    return std::string(kRetentionPolicyPrefix) + std::string(measurement);
}

inline std::optional<std::string_view> retentionMeasurementFromKey(std::string_view key) {
    if (!key.starts_with(kRetentionPolicyPrefix))
        return std::nullopt;
    const auto measurement = key.substr(kRetentionPolicyPrefix.size());
    if (!validRetentionMeasurement(measurement))
        return std::nullopt;
    return measurement;
}

inline bool validRetentionPolicyValue(const RetentionPolicyValue& policy) {
    return !policy.ttl.empty() && policy.ttl.size() <= kMaxRetentionTtlBytes && policy.ttlNanos != 0 &&
           policy.ttlNanos != UINT64_MAX &&
           std::ranges::none_of(policy.ttl, [](unsigned char c) { return c < 0x20 || c == 0x7f; });
}

// Small nested v1 value. It is already protected by the TCC1 command or
// TSG0SNP1 snapshot framing, but remains self-identifying so a schema cell can
// never be interpreted as retention state.
inline std::string encodeRetentionPolicyValue(const RetentionPolicyValue& policy) {
    if (!validRetentionPolicyValue(policy))
        return {};
    std::string out = "TSRP1";
    out.push_back(static_cast<char>(policy.ttl.size()));
    out.append(policy.ttl);
    for (int i = 0; i < 8; ++i)
        out.push_back(static_cast<char>((policy.ttlNanos >> (8 * i)) & 0xff));
    return out;
}

inline std::optional<RetentionPolicyValue> decodeRetentionPolicyValue(std::string_view encoded) {
    constexpr std::string_view magic = "TSRP1";
    if (!encoded.starts_with(magic) || encoded.size() < magic.size() + 1 + 8)
        return std::nullopt;
    const size_t ttlBytes = static_cast<uint8_t>(encoded[magic.size()]);
    if (ttlBytes == 0 || ttlBytes > kMaxRetentionTtlBytes || encoded.size() != magic.size() + 1 + ttlBytes + 8)
        return std::nullopt;
    RetentionPolicyValue out;
    out.ttl.assign(encoded.substr(magic.size() + 1, ttlBytes));
    for (int i = 0; i < 8; ++i)
        out.ttlNanos |= static_cast<uint64_t>(static_cast<uint8_t>(encoded[magic.size() + 1 + ttlBytes + i]))
                        << (8 * i);
    if (!validRetentionPolicyValue(out))
        return std::nullopt;
    return out;
}

// At most one all-VShard sweep is in flight. Its globally contiguous ID lets
// every VShard retain one constant-space retry fence. The cursor names the
// first VShard not yet durably acknowledged in Group 0.
struct RetentionSweep {
    uint64_t sweepId = 0;
    std::string measurement;
    uint64_t policyVersion = 0;
    uint64_t cutoffTime = 0;
    uint32_t nextVShard = 0;

    friend bool operator==(const RetentionSweep&, const RetentionSweep&) = default;
};

struct RetentionCutoffRecord {
    uint64_t policyVersion = 0;
    uint64_t cutoffTime = 0;

    friend bool operator==(const RetentionCutoffRecord&, const RetentionCutoffRecord&) = default;
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
    size_t bytes = sizeof(uint8_t) + sizeof(uint64_t) * 4 + plan.requestId.size() + plan.requestFingerprint.size();
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
    std::string clusterUuid;                                        // meta
    uint64_t mapEpoch = 0;                                          // topology/policy version
    uint64_t appliedIndex = 0;                                      // last group-0 log index applied
    uint64_t controllerTerm = 0;                                    // controller epoch (group-0 term)
    NodeId controllerLeader = 0;                                    // node that owns controllerTerm
    std::map<NodeId, NodeRecord> nodes;                             // nodes/<uuid>
    std::map<uint16_t, std::vector<NodeId>> desiredPlacement;       // desired-placement/<vshard>
    ControlMap servingMap;                                          // current effective serving map
    std::vector<NodeId> metaVoters;                                 // group-0 voter set (self-managed)
    std::map<std::string, PolicyCell> policies;                     // schema/retention CAS cells
    uint64_t lastRetentionSweepId = 0;                              // last all-VShard fan-out completed
    std::optional<RetentionSweep> retentionSweep;                   // one durable all-VShard fan-out cursor
    std::map<std::string, RetentionCutoffRecord> retentionCutoffs;  // last completed cutoff per measurement
    std::map<std::string, Job> jobs;                                // jobs/<uuid>
    std::set<std::string> joinTokens;                               // valid unused group-0-minted join tokens
    std::map<std::string, FrozenDeletePlan> frozenDeletePlans;      // retry-stable pattern expansion plans
    friend bool operator==(const Group0State&, const Group0State&) = default;
};

}  // namespace timestar::control
