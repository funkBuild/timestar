#pragma once

#include "../raft/raft_driver.hpp"  // raft::RaftStateMachine
#include "data_command.hpp"
#include "data_plane.hpp"  // QuerySpec, QueryPartial

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace timestar::data {

// The deterministic, revision-aware query-visible state of ONE VShard: the
// reference model that every replica converges to by applying the same committed
// command log in the same order. Point revisions are assigned here (apply-time)
// from a per-VShard monotonic counter, so the value is a pure function of the log
// and never of wall-clock, arrival order, or which node is leader.
//
// Last-write-wins is by revision: a later log entry gets a higher revision, so a
// re-applied (retried) batch overwrites itself with identical values -- harmless.
// A DeleteRange tombstone removes only points whose revision PRECEDES it, so a
// later higher-revision write in the same range reappears. Retention is a
// monotonic cutoff. Every operation is therefore idempotent by construction.
class ReplicatedVShardStore {
public:
    struct Cell {
        double value = 0;
        uint64_t revision = 0;
    };
    struct Tombstone {
        uint64_t startTime = 0;
        uint64_t endTime = 0;  // inclusive
        uint64_t revision = 0;
    };

    void applyWritePoints(const WritePoints& wp);
    void applyDeleteRange(const DeleteRange& dr);
    void applyRetentionCutoff(const RetentionCutoff& rc);

    // Query visible state: points in [start,end] not superseded by a
    // higher-revision tombstone and not older than the retention cutoff.
    QueryPartial query(const QuerySpec& spec) const;

    std::string encode() const;         // full-state snapshot payload
    bool loadFrom(const std::string&);  // fail-closed: false + unchanged on corruption

    uint64_t nextRevision() const { return revCounter_; }

private:
    bool visible(const SeriesId128& series, uint64_t ts, uint64_t rev) const;

    std::map<SeriesId128, std::map<uint64_t, Cell>> points_;
    std::map<SeriesId128, std::vector<Tombstone>> tombstones_;
    uint64_t retentionCutoff_ = 0;
    uint64_t revCounter_ = 0;  // first assigned revision is 1 (0 reserved for migrated data)
};

// A per-VShard Raft state machine over ReplicatedVShardStore. Plugged into a
// RaftGroup exactly like Group0StateMachine: decode the committed entry, apply it
// deterministically, advance appliedIndex; a committed-but-undecodable entry is
// FATAL (fail-stop) rather than skipped, because skipping it would diverge this
// replica from the others.
class DataStateMachine : public raft::RaftStateMachine {
public:
    seastar::future<> apply(raft::LogEntry entry) override;
    seastar::future<> applySnapshot(raft::Snapshot snap) override;

    std::string snapshot() const { return store_.encode(); }

    QueryPartial query(const QuerySpec& spec) const { return store_.query(spec); }
    uint64_t appliedIndex() const { return appliedIndex_; }
    const ReplicatedVShardStore& store() const { return store_; }

private:
    ReplicatedVShardStore store_;
    uint64_t appliedIndex_ = 0;
};

}  // namespace timestar::data
