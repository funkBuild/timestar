#pragma once

#include "../raft/raft_driver.hpp"  // RaftStateMachine
#include "control_command.hpp"
#include "group0_state.hpp"

#include <functional>
#include <optional>
#include <seastar/core/future.hh>
#include <string>

namespace timestar::control {

// The group-0 replicated state machine: applies committed control commands to a
// Group0State deterministically. It IS a raft::RaftStateMachine, so a RaftGroup
// (group id 0) drives it exactly like a storage group -- the control plane
// reuses the whole Phase 2 engine (elections, joint consensus, snapshots, the
// journal safety contract) with no separate coordination service.
class Group0StateMachine : public raft::RaftStateMachine {
public:
    using ServingMapObserver = std::function<seastar::future<>(ControlMap)>;

    // Install the node-local recovery fence before any snapshot/log entry is
    // applied. The replicated state remains node-independent; this expectation
    // only makes this process fail-stop if committed control state names a
    // different cluster or rebinds its persistent identity.
    void expectLocalIdentity(std::string clusterUuid, NodeRecord localRecord);
    void expectInitialServingMap(ControlMap map);
    // Called after an initial serving map is logically applied but before Raft
    // advances its applied boundary. Production uses this to durably publish
    // control_map.cache; failure retries the same idempotent entry.
    void setServingMapObserver(ServingMapObserver observer);

    // Apply one committed command entry. Deterministic; no I/O.
    seastar::future<> apply(raft::LogEntry entry) override;
    // Restore the full state from a snapshot payload (produced by snapshot()). A
    // malformed payload fails the returned future instead of advancing Raft over
    // state that was not installed.
    seastar::future<> applySnapshot(raft::Snapshot snap) override;

    // Serialize the entire state for a Raft snapshot (the compaction payload).
    std::string snapshot() const;
    // Restore the whole state from a snapshot payload; false (state unchanged) on
    // a corrupt/truncated payload rather than a half-applied state.
    bool loadSnapshot(const std::string& data);

    const Group0State& state() const { return state_; }

    // Apply a command directly (used by tests); returns false for a semantic
    // no-op/rejection (stale CAS, invalid identity/set, or idempotent replay).
    bool applyCommand(const ControlCommand& cmd);

private:
    bool stateMatchesLocalExpectations(const Group0State& state) const;
    void rejectConflictingLocalCommand(const ControlCommand& command) const;

    Group0State state_;
    std::string expectedClusterUuid_;
    std::optional<NodeRecord> expectedLocalRecord_;
    std::optional<ControlMap> expectedInitialServingMap_;
    ServingMapObserver servingMapObserver_;
};

}  // namespace timestar::control
