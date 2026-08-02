#pragma once

#include "../features/feature_gate.hpp"
#include "group0_state.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace timestar::control {

// Group-0 control commands. Each committed Raft entry in group 0 carries exactly
// one of these (serialized); the state machine applies it deterministically.

struct InitCluster {
    std::string clusterUuid;
};

struct UpsertNode {
    NodeRecord record;
};

struct SetNodeState {
    NodeId raftId = 0;
    NodeState state = NodeState::Joining;
};

// Sets a VShard's desired replicas and bumps mapEpoch (topology change).
struct SetDesiredPlacement {
    uint16_t vshard = 0;
    std::vector<NodeId> replicas;
};

// Records the group-0 voter set (self-managed membership; the actual Raft config
// change rides a separate joint-consensus config entry, this just mirrors it into
// state for readers).
struct SetMetaVoters {
    std::vector<NodeId> voters;
};

// Compare-and-set a policy/schema cell: applies iff the current version equals
// expectedVersion (0 == "must not exist yet"). On success the version is bumped.
struct CasPolicy {
    std::string key;
    uint64_t expectedVersion = 0;
    std::string value;
};

// Records the controller epoch (= group-0 term the leader was elected under) and
// its owning node. The serialized term is a proposal-time hint; committed apply
// always substitutes the enclosing log entry's term so it cannot be fabricated
// or become stale while the proposer waits for the group lock. Monotonic: a
// lower term is ignored (fencing).
struct SetControllerTerm {
    uint64_t term = 0;
    NodeId leader = 0;
};

// Create/advance/complete a persisted controller job (idempotent step).
struct UpsertJob {
    std::string jobId;
    uint32_t step = 0;
    bool done = false;
    std::string payload;
};

// Mint a group-0 join token (the operator/controller issues it out of band).
struct MintJoinToken {
    std::string token;
};

// Admit a node ONLY if it presents a valid unused join token: atomically
// consumes the token and records the node as Joining. Active is a later
// committed transition only after learner catch-up. If the token is absent
// (invalid/replayed), the command is a no-op -- the node is never implicitly
// initialized into the cluster.
struct AdmitWithToken {
    NodeRecord record;
    std::string token;
};

// Atomically freeze a complete bounded pattern-delete expansion before any
// data-group proposal. An exact retry with the same request identity returns
// the stored plan; a later catalog expansion can never replace it.
struct StoreFrozenDeletePlan {
    FrozenDeletePlan plan;
};

// Activate a storage/log wire-format version cluster-wide (rolling-upgrade,
// decision 8). `coveredVoters` is the canonical union of the stable group-0
// voters and every voter in the committed serving map whose capability the
// controller checked. Apply validates that proof against replicated state before
// advancing monotonically; a meta-only proof can never activate data emission.
struct SetActiveVersion {
    uint32_t version = 1;
    std::vector<NodeId> coveredVoters;
};

// Publish the complete initial data-serving map atomically. This is deliberately
// single-assignment: later topology cutovers require the resumable movement
// protocol and must not be smuggled in as desired placement.
struct SetInitialServingMap {
    ControlMap map;
};

enum class FrozenDeletePlanRpcOperation : uint8_t { Lookup = 0, Freeze = 1 };

struct FrozenDeletePlanRpcRequest {
    FrozenDeletePlanRpcOperation operation = FrozenDeletePlanRpcOperation::Lookup;
    FrozenDeletePlan plan;
};

// Version-7 identity-bound capability reply. The ordinary negotiation response
// is only an agreed scalar; it cannot prove which persistent node answered or
// what full range that node advertises. This frame is integrity-protected by the
// authenticated data-plane channel in production and binds those facts together.
struct NodeCapabilityAdvertisement {
    std::string clusterUuid;
    NodeRecord record;
    features::VersionRange formats;

    friend bool operator==(const NodeCapabilityAdvertisement& a, const NodeCapabilityAdvertisement& b) {
        return a.clusterUuid == b.clusterUuid && a.record == b.record && a.formats.min == b.formats.min &&
               a.formats.max == b.formats.max;
    }
};

// Version-8 request used by a fresh, already-running observer to present its
// persistent identity and one-use group-0 token to the current controller.
struct ControlJoinRequest {
    std::string clusterUuid;
    NodeRecord record;
    std::string token;

    friend bool operator==(const ControlJoinRequest&, const ControlJoinRequest&) = default;
};

enum class ControlJoinStatus : uint8_t { NotLeader = 0, Rejected = 1, Joining = 2, Active = 3 };

struct ControlJoinResult {
    ControlJoinStatus status = ControlJoinStatus::Rejected;
    NodeId leader = raft::kNoNode;

    friend bool operator==(const ControlJoinResult&, const ControlJoinResult&) = default;
};

inline constexpr size_t kMaxControlJoinFrameBytes = 4096;

// Version-9 upgrade preflight. Every node reports the durable delete-receipt
// state of the VShards it actually hosts, bound to the same persistent identity
// used by capability collection. Counts are 64-bit so an over-limit legacy
// state can be reported and rejected rather than truncated on the wire.
struct LegacyReceiptInventoryEntry {
    uint16_t vshard = 0;
    uint64_t legacyReceipts = 0;
    uint64_t totalReceipts = 0;
    bool hasUnappliedEntries = false;

    friend bool operator==(const LegacyReceiptInventoryEntry&, const LegacyReceiptInventoryEntry&) = default;
};

struct LegacyReceiptInventoryAdvertisement {
    std::string clusterUuid;
    NodeRecord record;
    std::vector<LegacyReceiptInventoryEntry> entries;

    friend bool operator==(const LegacyReceiptInventoryAdvertisement&,
                           const LegacyReceiptInventoryAdvertisement&) = default;
};

inline constexpr size_t kMaxLegacyReceiptInventoryEntries = 4096;
inline constexpr size_t kMaxLegacyReceiptInventoryFrameBytes = 96 * 1024;

using ControlCommand =
    std::variant<InitCluster, UpsertNode, SetNodeState, SetDesiredPlacement, SetMetaVoters, CasPolicy,
                 SetControllerTerm, UpsertJob, MintJoinToken, AdmitWithToken, StoreFrozenDeletePlan, SetActiveVersion,
                 SetInitialServingMap>;

// Wire serialization for a command (the Raft entry payload). Length-prefixed,
// self-delimiting; decode returns nullopt on any malformed, truncated, or
// trailing input. One Raft entry must contain exactly one command.
std::string encodeCommand(const ControlCommand& cmd);
std::optional<ControlCommand> decodeCommand(const std::string& bytes);

// Version-6 peer request/reply frames used only to reach the current group-0
// leader. They reuse the same bounded plan fields as command tag 14 but keep a
// distinct operation byte so lookup can never be mistaken for a mutation.
std::string encodeFrozenDeletePlanRpcRequest(const FrozenDeletePlanRpcRequest& request);
std::optional<FrozenDeletePlanRpcRequest> decodeFrozenDeletePlanRpcRequest(const std::string& bytes);
std::string encodeFrozenDeletePlanRpcResult(const FreezeDeletePlanResult& result);
std::optional<FreezeDeletePlanResult> decodeFrozenDeletePlanRpcResult(const std::string& bytes);

std::string encodeNodeCapabilityAdvertisement(const NodeCapabilityAdvertisement& capability);
std::optional<NodeCapabilityAdvertisement> decodeNodeCapabilityAdvertisement(const std::string& bytes);

std::string encodeControlJoinRequest(const ControlJoinRequest& request);
std::optional<ControlJoinRequest> decodeControlJoinRequest(const std::string& bytes);
std::string encodeControlJoinResult(const ControlJoinResult& result);
std::optional<ControlJoinResult> decodeControlJoinResult(const std::string& bytes);

std::string encodeLegacyReceiptInventory(const LegacyReceiptInventoryAdvertisement& inventory);
std::optional<LegacyReceiptInventoryAdvertisement> decodeLegacyReceiptInventory(const std::string& bytes);

}  // namespace timestar::control
