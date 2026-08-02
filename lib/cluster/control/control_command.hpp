#pragma once

#include "group0_state.hpp"
#include "../movement/move_job.hpp"

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

// Atomically records one authorized VShard movement and its desired placement.
// The plan is bound to the current serving map and next map epoch; only one
// unfinished movement may exist at a time in v1.
struct PlanVShardMove {
    std::string jobId;
    movement::MovePlan plan;
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

// Publish a complete effective serving map. `completedJobId` is empty only for
// the epoch-1 bootstrap map; later epochs require the named movement job to be
// durably Done and must change exactly that job's VShard.
struct PublishServingMap {
    ControlMap map;
    std::string completedJobId;
};

enum class FrozenDeletePlanRpcOperation : uint8_t { Lookup = 0, Freeze = 1 };

struct FrozenDeletePlanRpcRequest {
    FrozenDeletePlanRpcOperation operation = FrozenDeletePlanRpcOperation::Lookup;
    FrozenDeletePlan plan;
};

// A fresh, already-running observer presents its persistent identity and a
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

using ControlCommand =
    std::variant<InitCluster, UpsertNode, SetNodeState, PlanVShardMove, SetMetaVoters, CasPolicy,
                 SetControllerTerm, UpsertJob, MintJoinToken, AdmitWithToken, StoreFrozenDeletePlan,
                 PublishServingMap>;

// Wire serialization for a command (the Raft entry payload). Length-prefixed,
// self-delimiting; decode returns nullopt on any malformed, truncated, or
// trailing input. One Raft entry must contain exactly one command.
std::string encodeCommand(const ControlCommand& cmd);
std::optional<ControlCommand> decodeCommand(const std::string& bytes);

// Version-1 peer request/reply frames used only to reach the current group-0
// leader. They reuse the same bounded plan fields as command tag 14 but keep a
// distinct operation byte so lookup can never be mistaken for a mutation.
std::string encodeFrozenDeletePlanRpcRequest(const FrozenDeletePlanRpcRequest& request);
std::optional<FrozenDeletePlanRpcRequest> decodeFrozenDeletePlanRpcRequest(const std::string& bytes);
std::string encodeFrozenDeletePlanRpcResult(const FreezeDeletePlanResult& result);
std::optional<FreezeDeletePlanResult> decodeFrozenDeletePlanRpcResult(const std::string& bytes);

std::string encodeControlJoinRequest(const ControlJoinRequest& request);
std::optional<ControlJoinRequest> decodeControlJoinRequest(const std::string& bytes);
std::string encodeControlJoinResult(const ControlJoinResult& result);
std::optional<ControlJoinResult> decodeControlJoinResult(const std::string& bytes);

}  // namespace timestar::control
