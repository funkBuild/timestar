#pragma once

#include "../data/replicated_command.hpp"
#include "../raft/raft_driver.hpp"  // raft::RaftStateMachine
#include "engine_local_store.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <stdexcept>

namespace timestar::cluster {

// The per-VShard Raft state machine over the REAL Engine (integration plan M3):
// applies each committed ReplicatedCommand through EngineLocalStore deterministically
// on every replica, so replicas converge.
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
//   - Backpressure is checked by ReplicatedVShardHost BEFORE proposal. Once an entry is
//     committed it is unconditional work: applyCommittedWrites bypasses the Engine's
//     front-door admission check, so restart replay cannot reject the same durable Ready
//     forever. Ordinary non-Raft EngineLocalStore::applyWrites remains admitted.
//   - appliedIndex(): the SM's watermark tracks the last DATA entry apply() ran on.
//     The RaftGroup only invokes apply() for Normal non-empty entries, so a trailing
//     config-change or empty term-start no-op is NOT counted here; the authoritative
//     applied index for commit/apply-ack waiters is RaftGroup's own (this is a
//     coarser hint, e.g. for read-catch-up).
class EngineDataStateMachine : public raft::RaftStateMachine {
public:
    enum class DeleteReceiptStatus { Retained, Expired, Missing };
    struct DeleteReceiptCounts {
        size_t total = 0;
        bool hasUnappliedEntries = false;

        friend bool operator==(const DeleteReceiptCounts&, const DeleteReceiptCounts&) = default;
    };

    EngineDataStateMachine(EngineLocalStore& store, VShardId vshard,
                           size_t maxDeleteReceipts = data::kMaxDeleteReceiptsPerVShard)
        : store_(store), vshard_(vshard), maxDeleteReceipts_(maxDeleteReceipts) {
        if (maxDeleteReceipts_ == 0)
            throw std::invalid_argument("EngineDataStateMachine: delete receipt capacity must be non-zero");
    }

    seastar::future<> apply(raft::LogEntry entry) override;
    seastar::future<> applySnapshot(raft::Snapshot snap) override;

    uint64_t appliedIndex() const { return appliedIndex_; }

    // State-machine receipts covered by a candidate snapshot boundary. The
    // host adds only this prefix to the payload: including a receipt from the
    // retained suffix would cause replay to skip a delete whose storage effects
    // are not present in the snapshot.
    // A retirement discards receipts that an older boundary would need while
    // replaying the suffix preceding the retirement entry. Such a historical
    // state can no longer be reconstructed and the producer must wait until its
    // data boundary covers the retirement entry.
    bool canSnapshotDeleteReceiptStateThrough(uint64_t snapshotIndex) const;
    data::DeleteReceiptSnapshotState deleteReceiptStateThrough(uint64_t snapshotIndex) const;

    // Leader-side pre-proposal admission and post-apply result checks. They keep
    // an expired command out of the log when possible and ensure a committed
    // retired no-op is never reported as a newly executed delete.
    void checkDeleteAdmission(const data::DeleteRangeBatch& command) const;
    DeleteReceiptStatus deleteReceiptStatus(const data::DeleteRangeBatch& command) const;
    DeleteReceiptCounts deleteReceiptCounts() const;

    // Restore the small state-machine-only part of a locally produced snapshot
    // without reinstalling (or even decoding/copying) its TSM objects.
    void restoreDeleteReceiptState(data::DeleteReceiptSnapshotState state, uint64_t snapshotIndex);

    // Retention policy versions/cutoffs are replicated state too. A snapshot
    // may include the one global sweep fence only when its boundary covers that
    // update; otherwise suffix replay would lose the prior fence.
    bool canSnapshotRetentionStateThrough(uint64_t snapshotIndex) const;
    std::optional<data::RetentionCutoffSnapshotState> retentionStateThrough(uint64_t snapshotIndex) const;
    void restoreRetentionState(std::optional<data::RetentionCutoffSnapshotState> state, uint64_t snapshotIndex);

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
    std::map<SeriesId128, data::DeleteOperationReceipt> deleteReceipts_;
    uint64_t deleteReceiptsRetiredBeforeMs_ = 0;
    uint64_t deleteReceiptsRetiredAtIndex_ = 0;
    std::optional<data::RetentionCutoffSnapshotState> retentionCutoff_;
    size_t maxDeleteReceipts_;

    void advanceDeleteReceiptFloor(uint64_t floorMs, uint64_t appliedIndex);
    void makeRoomForDeleteReceipt(uint64_t appliedIndex);
};

}  // namespace timestar::cluster
