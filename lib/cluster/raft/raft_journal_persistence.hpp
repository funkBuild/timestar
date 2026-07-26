#pragma once

#include "../../storage/journal_record.hpp"
#include "../../storage/journal_sink.hpp"
#include "../../storage/journal_writer.hpp"
#include "../core/vshard.hpp"
#include "raft_driver.hpp"
#include "raft_log.hpp"
#include "raft_types.hpp"

#include <cstdint>
#include <optional>
#include <seastar/core/future.hh>
#include <vector>

namespace timestar::raft {

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

private:
    // DECLARATION ORDER IS LOAD-BEARING: sink_ may reference owned_.
    std::optional<DirectJournalSink> owned_;
    JournalSink& sink_;
    VShardId vshard_;
    uint64_t nextSeq_;
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
};

// Rebuild one group's HardState + RaftLog from the full set of recovered records
// (across all VShards on the core), replaying only those for `vshard` in
// vshard_seq order: HardState records set term/vote, Data/Config records place a
// log entry at their raft_index (a re-append overwrites), and Truncation records
// drop the suffix at/after their index. See ADR 0003 for the seq/revision link.
RecoveredRaftState recoverRaftState(const std::vector<JournalRecord>& records, VShardId vshard);

}  // namespace timestar::raft
