#pragma once

#include "../raft/raft_types.hpp"  // NodeId

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace timestar::control {

using timestar::raft::NodeId;

// A node's lifecycle state in the cluster (the plan's node lifecycle).
enum class NodeState : uint8_t { Joining = 0, Active = 1, Draining = 2, Down = 3 };

// One member node's control-plane record (key family nodes/<uuid>). raftId is
// the stable id the Raft groups use; uuid identifies the node across restarts;
// failureDomain (rack/az) drives cross-domain meta-voter selection.
struct NodeRecord {
    NodeId raftId = 0;
    std::string uuid;
    std::string address;       // inter-node RPC address
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

// The full replicated group-0 control state. This is a deterministic function of
// the committed command log; every field is rebuilt identically on every node.
struct Group0State {
    std::string clusterUuid;                              // meta
    uint64_t mapEpoch = 0;                                // topology/policy version
    uint64_t appliedIndex = 0;                            // last group-0 log index applied
    uint64_t controllerTerm = 0;                          // controller epoch (group-0 term)
    NodeId controllerLeader = 0;                          // node that owns controllerTerm
    std::map<NodeId, NodeRecord> nodes;                   // nodes/<uuid>
    std::map<uint16_t, std::vector<NodeId>> desiredPlacement;  // desired-placement/<vshard>
    std::vector<NodeId> metaVoters;                       // group-0 voter set (self-managed)
    std::map<std::string, PolicyCell> policies;          // schema/retention CAS cells
    std::map<std::string, Job> jobs;                      // jobs/<uuid>
    std::set<std::string> joinTokens;                     // valid unused group-0-minted join tokens

    friend bool operator==(const Group0State&, const Group0State&) = default;
};

}  // namespace timestar::control
