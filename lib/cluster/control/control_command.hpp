#pragma once

#include "group0_state.hpp"

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
// its owning node. Monotonic: a lower term is ignored (fencing).
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

// Activate a storage/log wire-format version cluster-wide (rolling-upgrade, decision
// 8). The controller proposes this ONLY after FeatureGate::canActivate confirms every
// current voter can read `version`; apply advances the active version MONOTONICALLY
// (a stale/replayed lower version is ignored) so a node never regresses formats.
struct SetActiveVersion {
    uint32_t version = 1;
};

// Publish the complete initial data-serving map atomically. This is deliberately
// single-assignment: later topology cutovers require the resumable movement
// protocol and must not be smuggled in as desired placement.
struct SetInitialServingMap {
    ControlMap map;
};

using ControlCommand =
    std::variant<InitCluster, UpsertNode, SetNodeState, SetDesiredPlacement, SetMetaVoters, CasPolicy,
                 SetControllerTerm, UpsertJob, MintJoinToken, AdmitWithToken, SetActiveVersion,
                 SetInitialServingMap>;

// Wire serialization for a command (the Raft entry payload). Length-prefixed,
// self-delimiting; decode returns nullopt on any malformed, truncated, or
// trailing input. One Raft entry must contain exactly one command.
std::string encodeCommand(const ControlCommand& cmd);
std::optional<ControlCommand> decodeCommand(const std::string& bytes);

}  // namespace timestar::control
