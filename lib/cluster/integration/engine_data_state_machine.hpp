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
//   - Backpressure: apply() CAN AND DOES throw IngestBacklogException, because it routes
//     through Engine::insertBatch, which calls rejectIfIngestBacklogged unconditionally.
//     Measured at 20,851 refusals across one RF=3 restart replay (debt D-36) -- so read
//     this as "fires routinely under replay", not as a theoretical edge.
//
//     CORRECTING WHAT THIS NOTE USED TO SAY: it claimed admission "is meant to happen at
//     PROPOSE time (the leader rejects if ingest-backlogged)" and called an apply-path
//     bypass a mere follow-on. Propose-time admission DOES NOT EXIST --
//     rejectIfIngestBacklogged has exactly two callers, both Engine::insert*, so in
//     cluster mode APPLY IS THE INGEST PATH and a bare bypass would remove the clustered
//     write path's only conversion/compaction-backlog backpressure. Both halves have to
//     land together; filed as debt D-42.
//
//     The throw does NOT diverge (RaftGroup retries the whole Ready, and re-apply is
//     idempotent). What it used to do was worse than stall one replica: it propagated out
//     of RaftGroupRegistry::tickAll and aborted the WHOLE tick pass, starving every
//     higher-numbered group on the reactor (D-36). Tick failures are now isolated per
//     group and counted, and a cluster read fences on the node's own apply lag rather
//     than answering out of state that is behind its committed log.
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

    // Entry-payload BYTES applied since the last snapshot install/produce (debt D-6).
    //
    // Half of the snapshot trigger. Entry COUNT bounds restart REPLAY TIME (each entry is
    // a WriteBatch that has to be decoded and re-applied); bytes bound the JOURNAL'S DISK
    // FOOTPRINT, and the two diverge by orders of magnitude across real workloads -- a
    // fleet writing 10k-point batches reaches a byte threshold in a few hundred entries,
    // while a trickle of single-point writes reaches an entry threshold having written
    // almost nothing. A trigger on either one alone therefore misses one of the two
    // problems snapshotting exists to solve, so the policy uses both.
    //
    // Counted here rather than in the host because this is the one place every applied
    // entry passes through, and it costs one add on a path that already decodes the entry.
    uint64_t appliedBytesSinceSnapshot() const { return appliedBytesSinceSnapshot_; }
    // Called by the snapshot producer once compaction has succeeded: the log below the new
    // boundary is gone, so the bytes it held no longer count against the next trigger.
    void noteSnapshotTaken() { appliedBytesSinceSnapshot_ = 0; }

private:
    EngineLocalStore& store_;
    VShardId vshard_;
    uint64_t appliedIndex_ = 0;
    uint64_t appliedBytesSinceSnapshot_ = 0;
};

}  // namespace timestar::cluster
