#pragma once

#include "../data/replicated_command.hpp"
#include "../raft/raft_driver.hpp"  // raft::RaftStateMachine
#include "engine_local_store.hpp"

#include <cstdint>
#include <seastar/core/future.hh>

namespace timestar::cluster {

// The per-VShard Raft state machine over the REAL Engine (integration plan M3):
// applies each committed ReplicatedCommand through EngineLocalStore deterministically
// on every replica, so replicas converge. This is the M3 analogue of the Phase-5
// DataStateMachine (which used a toy in-memory store).
//
// Determinism / revision assignment (ADR 0003): point revisions are stamped at APPLY
// time from the LOG POSITION -- every point in a committed entry gets that entry's
// index as its revision. Identical apply order on every replica => identical
// revisions, so a leader need not pre-read a counter and two proposals can never
// collide. A later entry (higher index) wins last-write-wins; re-applying a committed
// suffix after restart is idempotent (identical re-stamp + LWW), so applied-index
// checkpointing is a perf optimization, not a correctness requirement.
//
// Contracts:
//   - A committed-but-undecodable entry is FAIL-STOP (throws), never skipped:
//     skipping would diverge this replica from the others.
//   - Backpressure must NOT fire inside apply -- admission happens at PROPOSE time
//     (the leader rejects if ingest-backlogged); a follower behind on apply is lag,
//     handled by Raft flow control. So apply must retry on transient pressure rather
//     than skip. (v1 relies on EngineLocalStore not raising IngestBacklogException on
//     the apply path.)
class EngineDataStateMachine : public raft::RaftStateMachine {
public:
    explicit EngineDataStateMachine(EngineLocalStore& store) : store_(store) {}

    seastar::future<> apply(raft::LogEntry entry) override;
    seastar::future<> applySnapshot(raft::Snapshot snap) override;

    uint64_t appliedIndex() const { return appliedIndex_; }

private:
    EngineLocalStore& store_;
    uint64_t appliedIndex_ = 0;
};

}  // namespace timestar::cluster
