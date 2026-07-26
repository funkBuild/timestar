// D-10: recovery from a SHARED per-shard journal, where many groups' records
// interleave in one segment stream.
//
// The HA invariant being pinned is "recovery must reconstruct every group's log
// exactly". It holds for free -- but for a reason worth stating, because it is the
// reason plan 5.3 said a shared journal was the smaller of the two ways out:
// `recoverRaftState(records, vshard)` has ALWAYS filtered a core-wide record set by
// VShard and replayed it in vshard_seq order. Physical position was never
// authoritative (ADR 0001 sec 6.3), because segment-GC copy-forward can relocate a
// laggard VShard's older records behind its own newer ones.
//
// So these tests deliberately feed recovery an interleaving no per-VShard journal
// could ever produce, and require the same logs out.
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_sink.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

using timestar::JournalRecord;
using timestar::JournalSegmentHeader;
using timestar::JournalWriter;
using timestar::SharedShardJournal;
using timestar::VShardId;
using timestar::raft::EntryType;
using timestar::raft::HardState;
using timestar::raft::JournalRaftPersistence;
using timestar::raft::LogEntry;
using timestar::raft::recoverRaftState;

fs::path tmpDir(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_sjr_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 1;
    h.bootId.fill(0x55);
    return h;
}

LogEntry entry(uint64_t index, uint64_t term, std::string data) {
    LogEntry e;
    e.index = index;
    e.term = term;
    e.type = EntryType::Normal;
    e.data = std::move(data);
    return e;
}

}  // namespace

seastar::future<> testInterleavedGroupsRecoverExactly() {
    const auto dir = tmpDir("interleave");
    constexpr uint16_t kGroups = 5;
    constexpr uint64_t kEntriesPerGroup = 6;

    {
        JournalWriter w(dir, header(), 1u << 26);
        auto recovered = co_await w.open();
        EXPECT_TRUE(recovered.empty());
        SharedShardJournal sink(w);
        std::vector<std::unique_ptr<JournalRaftPersistence>> persist;
        for (uint16_t g = 0; g < kGroups; ++g)
            persist.push_back(
                std::make_unique<JournalRaftPersistence>(static_cast<timestar::JournalSink&>(sink), VShardId{g}, 1));

        // INTERLEAVE BY INDEX, not by group: index 1 for every group, then index 2
        // for every group, and so on. Each group's records therefore land in the
        // shared stream separated by four other groups' records.
        for (uint64_t i = 1; i <= kEntriesPerGroup; ++i) {
            for (uint16_t g = 0; g < kGroups; ++g) {
                HardState hs;
                hs.currentTerm = 3;
                hs.votedFor = 1;
                co_await persist[g]->persistHardState(hs);
                std::vector<LogEntry> e{entry(i, 3, "g" + std::to_string(g) + "-i" + std::to_string(i))};
                co_await persist[g]->persistEntries(std::move(e));
            }
            co_await persist[0]->sync();  // one round covers everything appended so far
        }
        co_await sink.stop();
        co_await w.close();
    }

    {
        JournalWriter w(dir, header(), 1u << 26);
        const std::vector<JournalRecord> records = co_await w.open();
        EXPECT_EQ(records.size(), kGroups * kEntriesPerGroup * 2);  // hard state + entry each
        for (uint16_t g = 0; g < kGroups; ++g) {
            auto st = recoverRaftState(records, VShardId{g});
            EXPECT_EQ(st.hardState.currentTerm, 3u) << g;
            EXPECT_EQ(st.log.lastIndex(), kEntriesPerGroup) << g;
            for (uint64_t i = 1; i <= kEntriesPerGroup; ++i) {
                const LogEntry* e = st.log.entryAt(i);
                EXPECT_NE(e, nullptr) << g << "/" << i;
                if (e)
                    EXPECT_EQ(e->data, "g" + std::to_string(g) + "-i" + std::to_string(i));
            }
            // The resume point is this group's OWN next sequence -- not the shared
            // stream's -- or two groups would reuse each other's sequence numbers
            // and recovery would see conflicting duplicates.
            EXPECT_EQ(st.nextSeq, kEntriesPerGroup * 2 + 1) << g;
        }
        // A group that never wrote recovers empty rather than picking up a
        // neighbour's records.
        auto absent = recoverRaftState(records, VShardId{kGroups + 1});
        EXPECT_EQ(absent.log.lastIndex(), 0u);
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(SharedJournalRecoveryTest, InterleavedGroupsEachRecoverTheirOwnLogExactly) {
    testInterleavedGroupsRecoverExactly().get();
}

seastar::future<> testOnlySyncedRecordsSurviveACrash() {
    const auto dir = tmpDir("crash");
    // Two groups append; ONE sync covers both; then a third record is appended and
    // NEVER synced. A crash here (modelled by dropping the writer without a final
    // barrier) must leave exactly the synced prefix -- the coalescer must not have
    // made anything durable that no one asked for, and must not have lost anything
    // it acknowledged.
    {
        JournalWriter w(dir, header(), 1u << 26);
        co_await w.open();
        SharedShardJournal sink(w);
        JournalRaftPersistence a(static_cast<timestar::JournalSink&>(sink), VShardId{1}, 1);
        JournalRaftPersistence b(static_cast<timestar::JournalSink&>(sink), VShardId{2}, 1);
        co_await a.persistEntries({entry(1, 1, "acked-a")});
        co_await b.persistEntries({entry(1, 1, "acked-b")});
        co_await a.sync();  // the ONE fsync that covers both groups
        co_await b.persistEntries({entry(2, 1, "unacked-b")});
        // No sync, no close, no seal: the segment on disk holds the barrier's prefix.
        co_await sink.stop();
    }
    {
        JournalWriter w(dir, header(), 1u << 26);
        const std::vector<JournalRecord> records = co_await w.open();
        auto a = recoverRaftState(records, VShardId{1});
        auto b = recoverRaftState(records, VShardId{2});
        EXPECT_EQ(a.log.lastIndex(), 1u) << "group A's acked entry must survive";
        EXPECT_EQ(b.log.lastIndex(), 1u) << "group B's acked entry must survive, its unacked one need not";
        EXPECT_NE(a.log.entryAt(1), nullptr);
        EXPECT_NE(b.log.entryAt(1), nullptr);
        if (a.log.entryAt(1))
            EXPECT_EQ(a.log.entryAt(1)->data, "acked-a");
        if (b.log.entryAt(1))
            EXPECT_EQ(b.log.entryAt(1)->data, "acked-b");
        co_await w.close();
    }
    fs::remove_all(dir);
}
TEST(SharedJournalRecoveryTest, OneFsyncCoversEveryGroupThatAppendedBeforeIt) {
    testOnlySyncedRecordsSurviveACrash().get();
}
