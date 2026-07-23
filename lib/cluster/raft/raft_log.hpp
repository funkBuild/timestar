#pragma once

#include "raft_types.hpp"

#include <optional>
#include <vector>

namespace timestar::raft {

// The in-memory Raft log for one group (one VShard replica). Deterministic and
// transport-agnostic: a Seastar-RPC/journal driver feeds it and persists its
// outputs, but the log itself performs no I/O and is unit-testable without a
// reactor.
//
// Indices are 1-based. Entries at or below `snapshotIndex()` have been compacted
// into a snapshot and are no longer materialized here; `snapshotIndex()==0` means
// nothing is compacted and the log begins at index 1. Index 0 is the empty
// prefix, which conceptually carries term 0 -- so `matchTerm(0, 0)` is true and a
// first AppendEntries with prevLogIndex==0 always matches.
class RaftLog {
public:
    RaftLog() = default;

    // Boundary of the compacted prefix. Everything with index <= snapshotIndex()
    // is covered by a snapshot; snapshotTerm() is the term of that entry.
    LogIndex snapshotIndex() const { return snapshotIndex_; }
    Term snapshotTerm() const { return snapshotTerm_; }

    // First index still materialized in the log (snapshotIndex()+1).
    LogIndex firstIndex() const { return snapshotIndex_ + 1; }
    // Highest index present (snapshotIndex() when the tail is empty).
    LogIndex lastIndex() const { return snapshotIndex_ + entries_.size(); }
    // Term of the last entry (snapshotTerm() when the tail is empty).
    Term lastTerm() const { return entries_.empty() ? snapshotTerm_ : entries_.back().term; }
    bool empty() const { return entries_.empty(); }

    // Term at `index`, or nullopt when it is unknowable: below the snapshot
    // boundary (compacted away) or beyond lastIndex(). At exactly snapshotIndex()
    // returns snapshotTerm(); at index 0 with no snapshot returns kNoTerm.
    std::optional<Term> term(LogIndex index) const;

    // True iff the log holds an entry at `index` whose term is `t` (the Raft
    // log-matching test). matchTerm(0, 0) is true (empty prefix).
    bool matchTerm(LogIndex index, Term t) const;

    // Election restriction (§5.4.1): is a candidate whose last entry is
    // (candLastIndex, candLastTerm) at least as up-to-date as our log?
    bool isUpToDate(LogIndex candLastIndex, Term candLastTerm) const;

    // Leader append: assign indices from lastIndex()+1 upward (each entry's term
    // must already be set) and return the index of the last appended entry.
    LogIndex append(std::vector<LogEntry> entries);

    // Follower AppendEntries (§5.3). If the log matches at (prevLogIndex,
    // prevLogTerm), splice `entries` in at prevLogIndex+1 -- truncating on the
    // first genuine term conflict, ignoring entries already present, never
    // touching committed/compacted prefix -- and set lastNewIndex to
    // prevLogIndex + entries.size(). Returns false (no mutation) on prev mismatch.
    bool maybeAppend(LogIndex prevLogIndex, Term prevLogTerm, const std::vector<LogEntry>& entries,
                     LogIndex& lastNewIndex);

    // Drop every entry with index >= from. from must be > snapshotIndex(); a from
    // at or below the tail's start clears the whole tail. Clamped to valid range.
    void truncateSuffix(LogIndex from);

    // Entries with index >= from, for leader replication. Entries already inside
    // the snapshot (from <= snapshotIndex()) cannot be served here -- the caller
    // must send an InstallSnapshot instead; only the still-materialized suffix is
    // returned.
    std::vector<LogEntry> entriesFrom(LogIndex from) const;

    // Compact the prefix up to and including `upto` into a snapshot boundary.
    // Requires snapshotIndex() <= upto <= lastIndex(); the boundary term is taken
    // from the log itself (never a caller-supplied value that could disagree). A
    // snapshot that is AHEAD of the log goes through restoreFromSnapshot instead.
    // upto beyond lastIndex() is clamped to lastIndex(); a backward upto is a no-op.
    void compactTo(LogIndex upto);

    // Reset the log to begin from an installed snapshot at (index, term),
    // discarding all materialized entries. Used by InstallSnapshot when the
    // leader's snapshot is ahead of our log.
    void restoreFromSnapshot(LogIndex index, Term term);

private:
    LogIndex snapshotIndex_ = kNoIndex;  // compacted prefix boundary
    Term snapshotTerm_ = kNoTerm;        // term of the entry at snapshotIndex_
    std::vector<LogEntry> entries_;      // entries_[k] has index snapshotIndex_+1+k
};

}  // namespace timestar::raft
