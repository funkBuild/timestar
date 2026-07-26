// Journal-backed Raft persistence: a real durability round-trip. Persist hard
// state + a log (incl. a config entry) through a JournalWriter, fsync, close,
// then reopen and reconstruct the RaftLog + HardState from the recovered
// records -- proving the log survives a process restart.
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <unistd.h>
#include <vector>

using namespace timestar::raft;
using timestar::JournalRecord;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::VShardId;

namespace fs = std::filesystem;

// EXPECT inside a coroutine cannot early-return with ASSERT_*, so guard explicitly.
#define ASSERT_TRUE_OR_RETURN(cond)   \
    do {                              \
        EXPECT_TRUE(cond);            \
        if (!(cond))                  \
            co_return;                \
    } while (0)

namespace {

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() / ("timestar_raftjnl_" + tag + "_" + std::to_string(::getpid()) +
                                            "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 2;
    h.bootId.fill(0x33);
    return h;
}

LogEntry entry(Term t, LogIndex i, std::string data, EntryType type = EntryType::Normal) {
    LogEntry e;
    e.term = t;
    e.index = i;
    e.type = type;
    e.data = std::move(data);
    return e;
}

seastar::future<> testPersistThenRecover() {
    const auto dir = tmpDir("recover");
    const VShardId vs{7};

    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        // A leader's Ready: term/vote, then three entries (one a config change).
        co_await p.persistHardState(HardState{3, 2});
        std::vector<LogEntry> es = {entry(3, 1, "a"), entry(3, 2, "cfg", EntryType::ConfigChange),
                                    entry(3, 3, "b")};
        co_await p.persistEntries(es);
        co_await p.sync();
        co_await w.close();
    }

    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);

        EXPECT_EQ(st.hardState.currentTerm, 3u);
        EXPECT_EQ(st.hardState.votedFor, 2u);
        EXPECT_EQ(st.log.lastIndex(), 3u);
        EXPECT_EQ(st.log.term(1), std::optional<Term>(3));
        EXPECT_EQ(st.log.term(3), std::optional<Term>(3));
        auto tail = st.log.entriesFrom(1);
        EXPECT_EQ(tail.size(), 3u);
        if (tail.size() == 3u) {
            EXPECT_EQ(tail[0].data, "a");
            EXPECT_EQ(tail[1].type, EntryType::ConfigChange);
            EXPECT_EQ(tail[1].data, "cfg");
            EXPECT_EQ(tail[2].data, "b");
        }
        EXPECT_GT(st.nextSeq, 1u);
        co_await w.close();
    }
    fs::remove_all(dir);
}

seastar::future<> testTruncationAndReAppendRecover() {
    const auto dir = tmpDir("trunc");
    const VShardId vs{9};
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        // Append 1..3 at term 1, then a conflicting re-append of 2..3 at term 2
        // (a follower overwriting a suffix). The lower-index re-append supersedes.
        co_await p.persistEntries({entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")});
        co_await p.persistEntries({entry(2, 2, "B"), entry(2, 3, "C")});
        co_await p.persistHardState(HardState{2, 0});
        co_await p.sync();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        EXPECT_EQ(st.log.lastIndex(), 3u);
        EXPECT_EQ(st.log.term(1), std::optional<Term>(1));
        EXPECT_EQ(st.log.term(2), std::optional<Term>(2));  // overwritten
        EXPECT_EQ(st.log.term(3), std::optional<Term>(2));
        auto tail = st.log.entriesFrom(2);
        EXPECT_EQ(tail.size(), 2u);
        if (tail.size() == 2u) {
            EXPECT_EQ(tail[0].data, "B");
            EXPECT_EQ(tail[1].data, "C");
        }
        co_await w.close();
    }
    fs::remove_all(dir);
}

// REVIEW F2: PROVENANCE SURVIVES RECOVERY, and it is a durability contract rather than
// bookkeeping. `drainReady` fsyncs an incoming Snapshot record BEFORE `applySnapshot`
// writes the TSM files, so a crash in that window leaves the log truncated to the boundary
// and the Engine holding only whichever files landed. Recovery MUST re-install a RECEIVED
// snapshot and MUST NOT re-install a produced one -- and the two are byte-identical in
// shape, so this flag is the only thing that can tell them apart.
seastar::future<> testSnapshotProvenance() {
    const auto dir = tmpDir("snapprov");
    const VShardId vs{12};
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        Snapshot snap;
        snap.index = 7;
        snap.term = 3;
        snap.config.voters = {1, 2, 3};
        snap.data = "received-state@7";
        co_await p.persistSnapshot(snap, /*receivedFromPeer=*/true);
        co_await p.sync();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        ASSERT_TRUE_OR_RETURN(st.snapshot.has_value());
        EXPECT_TRUE(st.snapshotFromPeer) << "a RECEIVED snapshot must recover as received, or the replica comes back "
                                            "believing it is caught up over whatever files happened to land";
        EXPECT_EQ(st.snapshot->index, 7u);
        EXPECT_EQ(st.snapshot->data, "received-state@7");
        EXPECT_EQ(st.snapshot->config.voters, (std::vector<NodeId>{1, 2, 3}));
        co_await w.close();
    }
    fs::remove_all(dir);
}

seastar::future<> testSnapshotRecover() {
    const auto dir = tmpDir("snap");
    const VShardId vs{11};
    {
        JournalWriter w(dir, header(), 1u << 20);
        co_await w.open();
        JournalRaftPersistence p(w, vs);
        co_await p.persistEntries({entry(1, 1, "a"), entry(1, 2, "b")});
        Snapshot snap;
        snap.index = 5;
        snap.term = 2;
        snap.config.voters = {1, 2, 3};
        snap.data = "state@5";
        co_await p.persistSnapshot(snap, /*receivedFromPeer=*/false);
        co_await p.persistEntries({entry(2, 6, "f")});  // entry above the snapshot
        co_await p.sync();
        co_await w.close();
    }
    {
        JournalWriter w(dir, header(), 1u << 20);
        auto recovered = co_await w.open();
        RecoveredRaftState st = recoverRaftState(recovered, vs);
        EXPECT_EQ(st.log.snapshotIndex(), 5u);
        EXPECT_EQ(st.log.snapshotTerm(), 2u);
        EXPECT_EQ(st.log.firstIndex(), 6u);
        EXPECT_EQ(st.log.lastIndex(), 6u);
        // The snapshot's config AND state-machine data must survive recovery.
        EXPECT_TRUE(st.snapshot.has_value());
        if (st.snapshot) {
            EXPECT_EQ(st.snapshot->index, 5u);
            EXPECT_EQ(st.snapshot->config.voters, (std::vector<NodeId>{1, 2, 3}));
            EXPECT_EQ(st.snapshot->data, "state@5");
        }
        EXPECT_FALSE(st.snapshotFromPeer) << "a locally PRODUCED snapshot must recover as produced";
        auto tail = st.log.entriesFrom(6);
        EXPECT_EQ(tail.size(), 1u);
        if (tail.size() == 1u)
            EXPECT_EQ(tail[0].data, "f");
        co_await w.close();
    }
    fs::remove_all(dir);
}

}  // namespace

TEST(RaftJournalPersistenceTest, PersistThenRecover) {
    testPersistThenRecover().get();
}
TEST(RaftJournalPersistenceTest, TruncationAndReAppendRecover) {
    testTruncationAndReAppendRecover().get();
}
TEST(RaftJournalPersistenceTest, SnapshotRecover) {
    testSnapshotRecover().get();
}
TEST(RaftJournalPersistenceTest, SnapshotProvenanceSurvivesRecovery) {
    testSnapshotProvenance().get();
}
