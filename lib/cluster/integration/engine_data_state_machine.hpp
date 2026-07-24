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
//     skipping would diverge this replica from the others. RaftGroup::drainReady
//     propagates the throw BEFORE advancing the applied index, so the entry is not
//     marked applied -- the replica halts/retries rather than skipping.
//   - Backpressure: admission is meant to happen at PROPOSE time (the leader rejects
//     if ingest-backlogged). apply() itself can still throw IngestBacklogException
//     today because it routes through Engine::insertBatch, which calls
//     rejectIfIngestBacklogged unconditionally -- a follower applying a large
//     catch-up burst that outruns background flush. That throw does NOT diverge
//     (RaftGroup retries the whole Ready, and re-apply is idempotent), but it can
//     stall a lagging replica's recovery; a proper apply-path bypass (insert without
//     admission control) is a follow-on. Do not read this as "never fires".
//   - appliedIndex(): the SM's watermark tracks the last DATA entry apply() ran on.
//     The RaftGroup only invokes apply() for Normal non-empty entries, so a trailing
//     config-change or empty term-start no-op is NOT counted here; the authoritative
//     applied index for commit/apply-ack waiters is RaftGroup's own (this is a
//     coarser hint, e.g. for read-catch-up).
class EngineDataStateMachine : public raft::RaftStateMachine {
public:
    EngineDataStateMachine(EngineLocalStore& store, VShardId vshard) : store_(store), vshard_(vshard) {}

    seastar::future<> apply(raft::LogEntry entry) override;
    seastar::future<> applySnapshot(raft::Snapshot snap) override;

    uint64_t appliedIndex() const { return appliedIndex_; }

private:
    EngineLocalStore& store_;
    VShardId vshard_;
    uint64_t appliedIndex_ = 0;
};

}  // namespace timestar::cluster
