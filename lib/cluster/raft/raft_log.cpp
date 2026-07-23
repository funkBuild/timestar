#include "raft_log.hpp"

#include <algorithm>

namespace timestar::raft {

std::optional<Term> RaftLog::term(LogIndex index) const {
    if (index == snapshotIndex_)
        return snapshotTerm_;  // includes index==0 with no snapshot -> kNoTerm
    if (index < snapshotIndex_ || index > lastIndex())
        return std::nullopt;  // compacted away, or beyond the tail
    return entries_[index - firstIndex()].term;
}

const LogEntry* RaftLog::entryAt(LogIndex index) const {
    if (index < firstIndex() || index > lastIndex())
        return nullptr;
    return &entries_[index - firstIndex()];
}

bool RaftLog::matchTerm(LogIndex index, Term t) const {
    const auto have = term(index);
    return have && *have == t;
}

bool RaftLog::isUpToDate(LogIndex candLastIndex, Term candLastTerm) const {
    const Term myTerm = lastTerm();
    if (candLastTerm != myTerm)
        return candLastTerm > myTerm;
    return candLastIndex >= lastIndex();
}

LogIndex RaftLog::append(std::vector<LogEntry> entries) {
    LogIndex next = lastIndex() + 1;
    for (auto& e : entries) {
        e.index = next++;
        entries_.push_back(std::move(e));
    }
    return lastIndex();
}

bool RaftLog::maybeAppend(LogIndex prevLogIndex, Term prevLogTerm, const std::vector<LogEntry>& entries,
                          LogIndex& lastNewIndex) {
    if (!matchTerm(prevLogIndex, prevLogTerm))
        return false;

    // The incoming entries occupy prevLogIndex+1, prevLogIndex+2, ... We assign
    // those canonical indices ourselves rather than trusting the wire's index
    // field, keeping the term.
    for (size_t i = 0; i < entries.size(); ++i) {
        const LogIndex idx = prevLogIndex + 1 + i;
        if (idx <= snapshotIndex_)
            continue;  // already committed & compacted; never conflicts (safety)
        const auto existing = term(idx);
        if (existing && *existing == entries[i].term)
            continue;  // already present with the same term -- idempotent
        // Genuine conflict (or a fresh index): drop our suffix and append the
        // rest of the incoming batch from here.
        truncateSuffix(idx);
        for (size_t j = i; j < entries.size(); ++j) {
            LogEntry e = entries[j];
            e.index = prevLogIndex + 1 + j;
            entries_.push_back(std::move(e));
        }
        break;
    }
    lastNewIndex = prevLogIndex + entries.size();
    return true;
}

void RaftLog::truncateSuffix(LogIndex from) {
    if (from <= firstIndex()) {
        entries_.clear();
        return;
    }
    if (from > lastIndex())
        return;  // nothing to drop
    entries_.resize(from - firstIndex());
}

std::vector<LogEntry> RaftLog::entriesFrom(LogIndex from) const {
    const LogIndex start = std::max(from, firstIndex());
    if (start > lastIndex())
        return {};
    return std::vector<LogEntry>(entries_.begin() + (start - firstIndex()), entries_.end());
}

void RaftLog::compactTo(LogIndex upto) {
    if (upto <= snapshotIndex_)
        return;  // already compacted at least this far (backward upto is a no-op)
    const LogIndex clamped = std::min(upto, lastIndex());
    // The boundary term is the log's own term at `clamped` -- always known here,
    // since snapshotIndex_ < clamped <= lastIndex(). Capture it before erasing.
    const Term boundaryTerm = term(clamped).value_or(snapshotTerm_);
    // Erase [firstIndex .. clamped] from the materialized tail.
    entries_.erase(entries_.begin(), entries_.begin() + (clamped - snapshotIndex_));
    snapshotIndex_ = clamped;
    snapshotTerm_ = boundaryTerm;
}

void RaftLog::restoreFromSnapshot(LogIndex index, Term term) {
    entries_.clear();
    snapshotIndex_ = index;
    snapshotTerm_ = term;
}

}  // namespace timestar::raft
