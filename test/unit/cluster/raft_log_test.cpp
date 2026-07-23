#include "../../../lib/cluster/raft/raft_log.hpp"

#include <gtest/gtest.h>

using namespace timestar::raft;

namespace {

LogEntry mk(Term t, std::string data = {}) {
    LogEntry e;
    e.term = t;
    e.data = std::move(data);
    return e;
}

}  // namespace

TEST(RaftLogTest, EmptyLogInvariants) {
    RaftLog log;
    EXPECT_EQ(log.lastIndex(), 0u);
    EXPECT_EQ(log.lastTerm(), 0u);
    EXPECT_EQ(log.firstIndex(), 1u);
    EXPECT_TRUE(log.empty());
    // The empty prefix carries term 0.
    EXPECT_TRUE(log.matchTerm(0, 0));
    EXPECT_FALSE(log.matchTerm(0, 1));
    EXPECT_FALSE(log.matchTerm(1, 0));  // no entry yet
    EXPECT_EQ(log.term(0), std::optional<Term>(0));
    EXPECT_EQ(log.term(1), std::nullopt);
}

TEST(RaftLogTest, LeaderAppendAssignsContiguousIndices) {
    RaftLog log;
    LogIndex last = log.append({mk(1, "a"), mk(1, "b"), mk(2, "c")});
    EXPECT_EQ(last, 3u);
    EXPECT_EQ(log.lastIndex(), 3u);
    EXPECT_EQ(log.lastTerm(), 2u);
    EXPECT_EQ(log.term(1), std::optional<Term>(1));
    EXPECT_EQ(log.term(2), std::optional<Term>(1));
    EXPECT_EQ(log.term(3), std::optional<Term>(2));
    auto tail = log.entriesFrom(1);
    ASSERT_EQ(tail.size(), 3u);
    EXPECT_EQ(tail[0].index, 1u);
    EXPECT_EQ(tail[2].index, 3u);
    EXPECT_EQ(tail[2].data, "c");
}

TEST(RaftLogTest, MatchTermAndUpToDate) {
    RaftLog log;
    log.append({mk(1), mk(1), mk(2)});  // terms 1,1,2 at 1,2,3
    EXPECT_TRUE(log.matchTerm(2, 1));
    EXPECT_FALSE(log.matchTerm(2, 2));
    EXPECT_TRUE(log.matchTerm(3, 2));

    // Election restriction: (candLastIndex, candLastTerm) vs our (3, 2).
    EXPECT_TRUE(log.isUpToDate(3, 2));   // identical
    EXPECT_TRUE(log.isUpToDate(4, 2));   // longer, same last term
    EXPECT_TRUE(log.isUpToDate(1, 3));   // higher term wins even if shorter
    EXPECT_FALSE(log.isUpToDate(2, 2));  // same term, shorter
    EXPECT_FALSE(log.isUpToDate(9, 1));  // lower term loses even if longer
}

TEST(RaftLogTest, MaybeAppendFromEmptyMatchesAtZero) {
    RaftLog log;
    LogIndex lastNew = 0;
    ASSERT_TRUE(log.maybeAppend(0, 0, {mk(1, "a"), mk(1, "b")}, lastNew));
    EXPECT_EQ(lastNew, 2u);
    EXPECT_EQ(log.lastIndex(), 2u);
}

TEST(RaftLogTest, MaybeAppendRejectsOnPrevMismatch) {
    RaftLog log;
    log.append({mk(1), mk(1)});
    LogIndex lastNew = 0;
    // prevLogIndex 2 exists but term there is 1, not 5.
    EXPECT_FALSE(log.maybeAppend(2, 5, {mk(2)}, lastNew));
    EXPECT_EQ(log.lastIndex(), 2u);  // unchanged
    // prevLogIndex beyond the log.
    EXPECT_FALSE(log.maybeAppend(9, 1, {mk(2)}, lastNew));
    EXPECT_EQ(log.lastIndex(), 2u);
}

TEST(RaftLogTest, MaybeAppendIsIdempotent) {
    RaftLog log;
    log.append({mk(1, "a"), mk(1, "b"), mk(2, "c")});
    LogIndex lastNew = 0;
    // Re-send entries 2,3 that already match: no truncation, no growth.
    ASSERT_TRUE(log.maybeAppend(1, 1, {mk(1, "b"), mk(2, "c")}, lastNew));
    EXPECT_EQ(lastNew, 3u);
    EXPECT_EQ(log.lastIndex(), 3u);
    EXPECT_EQ(log.term(3), std::optional<Term>(2));
}

TEST(RaftLogTest, MaybeAppendTruncatesOnConflict) {
    RaftLog log;
    log.append({mk(1, "a"), mk(1, "b"), mk(2, "c"), mk(2, "d")});  // 1..4
    LogIndex lastNew = 0;
    // Leader says at index 3 the term is 3 (conflict with our 2). Splice from
    // prevLogIndex 2: entry 3 conflicts -> truncate 3.. and append new suffix.
    ASSERT_TRUE(log.maybeAppend(2, 1, {mk(3, "C"), mk(3, "D")}, lastNew));
    EXPECT_EQ(lastNew, 4u);
    EXPECT_EQ(log.lastIndex(), 4u);
    EXPECT_EQ(log.term(3), std::optional<Term>(3));
    EXPECT_EQ(log.term(4), std::optional<Term>(3));
    auto tail = log.entriesFrom(3);
    ASSERT_EQ(tail.size(), 2u);
    EXPECT_EQ(tail[0].data, "C");
    EXPECT_EQ(tail[1].data, "D");
}

TEST(RaftLogTest, MaybeAppendKeepsMatchingPrefixThenExtends) {
    RaftLog log;
    log.append({mk(1, "a"), mk(1, "b")});  // 1,2
    LogIndex lastNew = 0;
    // Matching entry 2 + a brand-new entry 3.
    ASSERT_TRUE(log.maybeAppend(1, 1, {mk(1, "b"), mk(2, "c")}, lastNew));
    EXPECT_EQ(lastNew, 3u);
    EXPECT_EQ(log.lastIndex(), 3u);
    EXPECT_EQ(log.entriesFrom(3).at(0).data, "c");
}

TEST(RaftLogTest, TruncateSuffix) {
    RaftLog log;
    log.append({mk(1), mk(1), mk(2), mk(2)});
    log.truncateSuffix(3);  // drop 3,4
    EXPECT_EQ(log.lastIndex(), 2u);
    EXPECT_EQ(log.lastTerm(), 1u);
    log.truncateSuffix(1);  // drop everything
    EXPECT_EQ(log.lastIndex(), 0u);
    EXPECT_TRUE(log.empty());
}

TEST(RaftLogTest, CompactToMovesSnapshotBoundary) {
    RaftLog log;
    log.append({mk(1, "a"), mk(1, "b"), mk(2, "c"), mk(3, "d")});  // 1..4
    log.compactTo(2);
    EXPECT_EQ(log.snapshotIndex(), 2u);
    EXPECT_EQ(log.snapshotTerm(), 1u);
    EXPECT_EQ(log.firstIndex(), 3u);
    EXPECT_EQ(log.lastIndex(), 4u);
    // Compacted entries are unknowable; the boundary carries snapshotTerm.
    EXPECT_EQ(log.term(1), std::nullopt);
    EXPECT_EQ(log.term(2), std::optional<Term>(1));
    EXPECT_EQ(log.term(3), std::optional<Term>(2));
    // matchTerm at the boundary still works (for AppendEntries prevLogIndex==2).
    EXPECT_TRUE(log.matchTerm(2, 1));
    // entriesFrom below the boundary only serves the materialized suffix.
    auto tail = log.entriesFrom(1);
    ASSERT_EQ(tail.size(), 2u);
    EXPECT_EQ(tail[0].index, 3u);
}

TEST(RaftLogTest, CompactToFromNonZeroBoundaryDerivesTerm) {
    RaftLog log;
    log.append({mk(1, "a"), mk(1, "b"), mk(2, "c"), mk(3, "d"), mk(3, "e")});  // 1..5
    log.compactTo(2);  // boundary at (2, term 1)
    log.compactTo(4);  // advance boundary; term must come from the log (3), not stale
    EXPECT_EQ(log.snapshotIndex(), 4u);
    EXPECT_EQ(log.snapshotTerm(), 3u);
    EXPECT_EQ(log.firstIndex(), 5u);
    EXPECT_EQ(log.lastIndex(), 5u);
    auto tail = log.entriesFrom(1);
    ASSERT_EQ(tail.size(), 1u);
    EXPECT_EQ(tail[0].index, 5u);
    EXPECT_EQ(tail[0].data, "e");
    // Backward / redundant compaction is a no-op.
    log.compactTo(3);
    EXPECT_EQ(log.snapshotIndex(), 4u);
}

TEST(RaftLogTest, MaybeAppendIgnoresEntriesInsideSnapshot) {
    RaftLog log;
    log.append({mk(1, "a"), mk(1, "b"), mk(2, "c")});
    log.compactTo(2);  // 1,2 compacted; tail has 3
    LogIndex lastNew = 0;
    // Leader replays from prevLogIndex 1 (inside snapshot). matchTerm(1,..) is
    // unknowable now, so prev at the snapshot boundary is what a real leader uses;
    // here we branch from the boundary at index 2.
    ASSERT_TRUE(log.maybeAppend(2, 1, {mk(2, "c"), mk(3, "d")}, lastNew));
    EXPECT_EQ(lastNew, 4u);
    EXPECT_EQ(log.lastIndex(), 4u);
    EXPECT_EQ(log.term(4), std::optional<Term>(3));
}

TEST(RaftLogTest, RestoreFromSnapshotResetsLog) {
    RaftLog log;
    log.append({mk(1), mk(1), mk(2)});
    log.restoreFromSnapshot(10, 4);
    EXPECT_EQ(log.snapshotIndex(), 10u);
    EXPECT_EQ(log.snapshotTerm(), 4u);
    EXPECT_EQ(log.firstIndex(), 11u);
    EXPECT_EQ(log.lastIndex(), 10u);
    EXPECT_TRUE(log.empty());
    EXPECT_TRUE(log.matchTerm(10, 4));
    // A fresh AppendEntries branches off the installed snapshot boundary.
    LogIndex lastNew = 0;
    ASSERT_TRUE(log.maybeAppend(10, 4, {mk(4, "x")}, lastNew));
    EXPECT_EQ(lastNew, 11u);
    EXPECT_EQ(log.lastIndex(), 11u);
}
