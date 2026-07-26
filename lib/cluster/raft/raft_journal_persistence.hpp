#pragma once

#include "../../storage/journal_record.hpp"
#include "../../storage/journal_sink.hpp"
#include "../../storage/journal_writer.hpp"
#include "../core/vshard.hpp"
#include "raft_driver.hpp"
#include "raft_log.hpp"
#include "raft_types.hpp"

#include <cstdint>
#include <deque>
#include <optional>
#include <seastar/core/future.hh>
#include <utility>
#include <vector>

namespace timestar::raft {

// The journal-record bookkeeping behind a group's RECLAIM FLOOR (debt D-34), as
// reconstructed by recoverRaftState and then maintained incrementally by
// JournalRaftPersistence. See JournalRaftPersistence::releasedSeq() for what the three
// fields mean and why all three are needed.
struct JournalRetentionSeed {
    uint64_t latestHardStateSeq = 0;  // vshard_seq of the newest HardState record, 0 if none
    uint64_t latestSnapshotSeq = 0;   // vshard_seq of the newest Snapshot record, 0 if none
    // (raftIndex, vshard_seq) for every log entry that SURVIVED replay, ascending in
    // both. Empty when the group holds no entries above its snapshot boundary.
    std::vector<std::pair<uint64_t, uint64_t>> entrySeqs;
};

// Journal-backed Raft persistence for ONE group. Records are appended to a
// JournalSink tagged with the group's VShard, and made durable by sync().
// Honours the journal safety contract from ADR 0001: appends
// are ordered by a per-VShard vshard_seq, hard state and truncations are
// ordinary appended records (never in-place edits), and any writer I/O error
// fences it so the driver stops.
//
// WHETHER THE WRITER IS SHARED IS A DEPLOYMENT CHOICE (debt D-10), and this class
// no longer claims to know. It appends into a `JournalSink`:
//
//   * DirectJournalSink -- this VShard's OWN JournalWriter, sync() == its barrier.
//     The DEFAULT, and what every cluster runs today: each group syncs alone, to
//     its own fd, so there is one fdatasync per drained Ready per group.
//   * SharedShardJournal -- one writer per reactor shard with group-commit
//     coalescing, so many groups committing in the same window cost ONE fdatasync.
//
// The header used to assert the second unconditionally ("the writer is SHARED and
// its barrier makes ALL groups' pending appends on the core durable in one fsync
// -- bounded fsync count, per the Phase 2 gate"). That was never true of the
// wiring: `addVShard` has always created one writer per VShard directory. Plan 5.3
// is where the discrepancy was found and D-10 is where it is being closed.

class JournalRaftPersistence : public RaftPersistence {
public:
    // `startSeq` is the next vshard_seq to assign (recovered max + 1, or 1 fresh).
    // This overload OWNS a DirectJournalSink over `writer` -- today's shape, and
    // what the tests construct.
    JournalRaftPersistence(JournalWriter& writer, VShardId vshard, uint64_t startSeq = 1);
    // Append into a sink the caller owns (a per-shard SharedShardJournal). The sink
    // must outlive this object.
    JournalRaftPersistence(JournalSink& sink, VShardId vshard, uint64_t startSeq = 1);

    seastar::future<> persistHardState(HardState hs) override;
    seastar::future<> persistEntries(std::vector<LogEntry> entries) override;
    seastar::future<> persistSnapshot(Snapshot snap, bool receivedFromPeer) override;
    seastar::future<> sync() override;

    uint64_t nextSeq() const { return nextSeq_; }
    // sync() calls this group made (debt D-10 evidence; see JournalSink::fsyncs).
    uint64_t syncRequests() const { return sink_.syncRequests(); }

    // ---- THE RECLAIM FLOOR (debt D-34) ----
    //
    // The highest vshard_seq whose record this group no longer needs: every record of
    // this VShard with `vshardSeq <= releasedSeq()` may be deleted from the journal
    // without changing what recoverRaftState() reconstructs. It is what
    // JournalRetention::setReleased is fed, and deleting a segment above it is DATA
    // LOSS, so it is computed conservatively and from first principles rather than
    // from the snapshot boundary alone.
    //
    // WHY NOT "everything below the Snapshot record". That is the obvious answer and it
    // is wrong twice over, because a snapshot record is not the newest record of the
    // things it does not describe:
    //
    //   * LOG ENTRIES ABOVE THE BOUNDARY were appended BEFORE the snapshot record (they
    //     are already in the log when compaction runs), so their seqs are LOWER than
    //     the snapshot's. Releasing below the snapshot would delete exactly the entries
    //     compaction deliberately RETAINED -- the unflushed suffix D-6 exists to keep.
    //   * THE HARD STATE (term/vote) is persisted only when it CHANGES. A group with
    //     stable leadership may have written its only HardState record at startup, so
    //     that record's seq can be lower than everything else. Deleting it loses this
    //     replica's vote, which is a correctness violation of Raft, not a space saving.
    //
    // So the floor is `min(newest HardState seq, newest Snapshot seq, seq of the OLDEST
    // still-live log entry) - 1`, and 0 (nothing released) until a snapshot exists at
    // all -- without one the whole log is live by definition.
    //
    // The hard-state term would pin an idle-term group's floor at its very first record
    // forever, which is why RaftGroup::compact RE-PERSISTS the current hard state right
    // after the snapshot: one ~40-byte record moves that pin to the new boundary, and
    // replaying it is idempotent (same term, same vote).
    [[nodiscard]] uint64_t releasedSeq() const;

    // Seed the above from a recovered journal. MANDATORY after recovery and not an
    // optimisation: a fresh object knows none of the seqs of the records already on
    // disk, so its oldest-live-entry term would be the first entry appended AFTER the
    // restart -- a much HIGHER seq -- and the floor would jump over the recovered
    // entries and release records the log still needs.
    void seedRetention(JournalRetentionSeed seed);

private:
    // DECLARATION ORDER IS LOAD-BEARING: sink_ may reference owned_.
    std::optional<DirectJournalSink> owned_;
    JournalSink& sink_;
    VShardId vshard_;
    uint64_t nextSeq_;

    // ---- reclaim-floor state (debt D-34); see releasedSeq() ----
    uint64_t lastHardStateSeq_ = 0;
    uint64_t lastSnapshotSeq_ = 0;
    // (raftIndex, vshard_seq) of the log entries whose records are still needed,
    // ascending in both. Maintained to mirror what recoverRaftState would rebuild:
    // an append at index I supersedes everything at or above I (pop_back), and a
    // snapshot at boundary B retires everything at or below B (pop_front). Bounded by
    // the log length, which compaction bounds; 16 bytes per entry against a log entry
    // that is a whole WriteBatch.
    std::deque<std::pair<uint64_t, uint64_t>> entrySeqs_;
};

// The Raft state reconstructed for one group from its recovered journal records.
struct RecoveredRaftState {
    HardState hardState;
    RaftLog log;
    // If the log was compacted, the snapshot at its boundary. The caller must
    // apply snapshot->data to its state machine AND pass snapshot->config as the
    // RaftNode's base config (the membership as of the boundary lives ONLY here
    // once its ConfigChange entries are compacted away).
    std::optional<Snapshot> snapshot;
    // Was that snapshot RECEIVED from a peer rather than produced here? If so the caller
    // MUST re-install its payload into the state machine before serving anything: the
    // record is fsync'd before the install, so a crash in between leaves the log truncated
    // to the boundary and the Engine holding only whichever files landed. See
    // RaftPersistence::persistSnapshot. Absent/legacy records read as produced-here, which
    // is the pre-D-6 shape (nothing ever compacted, so none exist in the wild).
    bool snapshotFromPeer = false;
    uint64_t nextSeq = 1;  // resume point for a new JournalRaftPersistence
    // Reclaim-floor bookkeeping for the recovered records (debt D-34). MUST be handed
    // to the new JournalRaftPersistence via seedRetention() before it appends anything,
    // or the floor will jump over the records this log still needs.
    JournalRetentionSeed retention;
};

// Rebuild one group's HardState + RaftLog from the full set of recovered records
// (across all VShards on the core), replaying only those for `vshard` in
// vshard_seq order: HardState records set term/vote, Data/Config records place a
// log entry at their raft_index (a re-append overwrites), and Truncation records
// drop the suffix at/after their index. See ADR 0003 for the seq/revision link.
RecoveredRaftState recoverRaftState(const std::vector<JournalRecord>& records, VShardId vshard);

}  // namespace timestar::raft
