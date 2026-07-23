#pragma once

#include "../../storage/journal_record.hpp"
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
// per-core JournalWriter (shared by every group on the core, tagged with the
// group's VShard) and made durable by sync() -> writer.barrier() (the group
// commit fdatasync). Honours the journal safety contract from ADR 0001: appends
// are ordered by a per-VShard vshard_seq, hard state and truncations are
// ordinary appended records (never in-place edits), and any writer I/O error
// fences it so the driver stops.
//
// The writer is SHARED and its barrier makes ALL groups' pending appends on the
// core durable in one fsync -- so many groups committing in the same tick cost a
// single fdatasync (bounded fsync count, per the Phase 2 gate).
class JournalRaftPersistence : public RaftPersistence {
public:
    // `startSeq` is the next vshard_seq to assign (recovered max + 1, or 1 fresh).
    JournalRaftPersistence(JournalWriter& writer, VShardId vshard, uint64_t startSeq = 1);

    seastar::future<> persistHardState(HardState hs) override;
    seastar::future<> persistEntries(std::vector<LogEntry> entries) override;
    seastar::future<> persistSnapshot(Snapshot snap) override;
    seastar::future<> sync() override;

    uint64_t nextSeq() const { return nextSeq_; }

private:
    JournalWriter& writer_;
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
    uint64_t nextSeq = 1;  // resume point for a new JournalRaftPersistence
};

// Rebuild one group's HardState + RaftLog from the full set of recovered records
// (across all VShards on the core), replaying only those for `vshard` in
// vshard_seq order: HardState records set term/vote, Data/Config records place a
// log entry at their raft_index (a re-append overwrites), and Truncation records
// drop the suffix at/after their index. See ADR 0003 for the seq/revision link.
RecoveredRaftState recoverRaftState(const std::vector<JournalRecord>& records, VShardId vshard);

}  // namespace timestar::raft
