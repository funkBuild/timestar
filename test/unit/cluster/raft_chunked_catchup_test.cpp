// CHUNKED CATCH-UP (write-scaleout 5.4).
//
// WHY THIS EXISTS. `sendAppend` used to put `log_.entriesFrom(nextIndex)` -- the ENTIRE
// remaining log tail -- into one AppendEntries. For a follower that has been down through
// a large write campaign, that is a message whose size is the size of the backlog. The
// Raft deliver verb is seastar `no_wait`, so a message over the receiver's admission bound
// is DROPPED WITH NO REPLY: the leader would re-send the same oversized append on every
// tick, the follower would never ack, and the group would sit one replica short forever
// with nothing in any log to say why. That is exactly why the transport's inbound bound
// had to be left at 1 GiB (see raft_rpc_transport.cpp), which is not a bound.
//
// WHY CAPPING IT NEEDS NO PROTOCOL CHANGE. A follower acks the prefix it actually
// received, and `handleAppendEntriesReply` sends the next chunk immediately while
// `nextIndex <= lastIndex`. Catch-up therefore becomes a pipeline of bounded messages.
// This test drives that pipeline deterministically and asserts BOTH halves: no message
// exceeds the bounds, and the follower still ends up with the whole log.
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <string>
#include <vector>

using namespace timestar::raft;

namespace {

RaftOptions catchupOpts() {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;
    o.heartbeatTimeout = 1;
    return o;
}

// Largest `entries` count and largest total entry-payload size seen in any AppendEntries.
struct AppendObserver {
    size_t maxEntries = 0;
    size_t maxBytes = 0;
    size_t appendMessages = 0;
    void observe(const Message& m) {
        const auto* ae = std::get_if<AppendEntries>(&m.payload);
        if (!ae || ae->entries.empty())
            return;
        ++appendMessages;
        maxEntries = std::max(maxEntries, ae->entries.size());
        size_t bytes = 0;
        for (const auto& e : ae->entries)
            bytes += e.data.size();
        maxBytes = std::max(maxBytes, bytes);
    }
};

}  // namespace

TEST(RaftChunkedCatchupTest, AFarBehindFollowerIsCaughtUpInBoundedMessages) {
    RaftOptions o = catchupOpts();
    o.maxAppendEntries = 32;
    o.maxAppendBytes = 4096;

    const std::vector<NodeId> voters = {1, 2};
    RaftNode leader(1, voters, RaftLog{}, HardState{}, o);
    RaftNode follower(2, voters, RaftLog{}, HardState{}, o);

    // Elect node 1 with node 2 present, so the follower's nextIndex starts sane; then
    // stop delivering to it and build a large backlog.
    AppendObserver obs;
    std::deque<Message> wire;
    auto pump = [&](bool deliverToFollower) {
        for (int guard = 0; guard < 200000; ++guard) {
            bool progress = false;
            for (RaftNode* n : {&leader, &follower}) {
                if (!n->hasReady())
                    continue;
                RaftNode::Ready rd = n->ready();
                n->advance(rd);
                for (auto& m : rd.messages) {
                    if (m.from == 1)
                        obs.observe(m);
                    if (m.to == 2 && !deliverToFollower)
                        continue;  // follower is "down"
                    wire.push_back(m);
                }
                progress = true;
            }
            std::deque<Message> batch;
            batch.swap(wire);
            for (auto& m : batch) {
                if (m.to == 1)
                    leader.step(m);
                else
                    follower.step(m);
                progress = true;
            }
            if (!progress)
                return;
        }
        FAIL() << "pump did not quiesce";
    };

    leader.campaign();
    pump(true);
    ASSERT_TRUE(leader.isLeader());

    // ~2000 entries of ~200 bytes: far past both bounds in both dimensions.
    constexpr int kEntries = 2000;
    const std::string payload(200, 'x');
    for (int i = 0; i < kEntries; ++i)
        ASSERT_TRUE(leader.propose(payload + std::to_string(i)));
    pump(false);  // the follower hears nothing

    obs = AppendObserver{};  // only measure the CATCH-UP burst
    pump(true);
    // Ticks drive the re-send loop if a chunk's reply and the next send do not chain.
    for (int t = 0; t < 200; ++t) {
        leader.tick();
        follower.tick();
        pump(true);
    }

    // (1) The follower actually caught up -- chunking must not stall the pipeline.
    EXPECT_EQ(follower.log().lastIndex(), leader.log().lastIndex())
        << "follower did not catch up: " << follower.log().lastIndex() << " vs " << leader.log().lastIndex();

    // (2) ...and it did so in BOUNDED messages. Without the cap the first catch-up append
    // carries all ~2000 entries (~400 KB) in one message.
    EXPECT_GT(obs.appendMessages, 1u) << "the whole tail went in one message -- chunking is not in effect";
    EXPECT_LE(obs.maxEntries, o.maxAppendEntries);
    // The byte bound may be exceeded by exactly the first entry of a chunk (see
    // RaftLog::entriesFrom), so allow one entry of slack.
    EXPECT_LE(obs.maxBytes, o.maxAppendBytes + payload.size() + 8);
}

// The bound must never be able to produce an EMPTY append for a follower that is behind:
// an entry larger than maxAppendBytes still has to replicate, or the follower wedges on it
// forever (the leader already accepted it into its log; there is no smaller message).
TEST(RaftChunkedCatchupTest, AnEntryLargerThanTheByteBoundStillReplicates) {
    RaftOptions o = catchupOpts();
    o.maxAppendEntries = 8;
    o.maxAppendBytes = 64;  // far smaller than the entry below

    const std::vector<NodeId> voters = {1, 2};
    RaftNode leader(1, voters, RaftLog{}, HardState{}, o);
    RaftNode follower(2, voters, RaftLog{}, HardState{}, o);

    std::deque<Message> wire;
    auto pump = [&] {
        for (int guard = 0; guard < 100000; ++guard) {
            bool progress = false;
            for (RaftNode* n : {&leader, &follower}) {
                if (!n->hasReady())
                    continue;
                RaftNode::Ready rd = n->ready();
                n->advance(rd);
                for (auto& m : rd.messages)
                    wire.push_back(m);
                progress = true;
            }
            std::deque<Message> batch;
            batch.swap(wire);
            for (auto& m : batch) {
                if (m.to == 1)
                    leader.step(m);
                else
                    follower.step(m);
                progress = true;
            }
            if (!progress)
                return;
        }
        FAIL() << "pump did not quiesce";
    };

    leader.campaign();
    pump();
    ASSERT_TRUE(leader.isLeader());

    ASSERT_TRUE(leader.propose(std::string(100000, 'z')));
    pump();
    EXPECT_EQ(follower.log().lastIndex(), leader.log().lastIndex());
}

// The bounded accessor itself: count bound, byte bound, and the always-one-entry rule.
TEST(RaftChunkedCatchupTest, BoundedEntriesFromHonoursBothBoundsAndAlwaysYieldsOne) {
    RaftLog log;
    std::vector<LogEntry> entries;
    for (int i = 1; i <= 10; ++i) {
        LogEntry e;
        e.index = static_cast<LogIndex>(i);
        e.term = 1;
        e.type = EntryType::Normal;
        e.data = std::string(100, 'a');
        entries.push_back(std::move(e));
    }
    log.append(std::move(entries));

    EXPECT_EQ(log.entriesFrom(1, 3, 0).size(), 3u);         // count bound
    EXPECT_EQ(log.entriesFrom(1, 0, 250).size(), 2u);       // byte bound: a 3rd would take it to 300
    EXPECT_EQ(log.entriesFrom(1, 100, 1).size(), 1u);       // always at least one
    EXPECT_EQ(log.entriesFrom(1, 0, 0).size(), 10u);        // both disabled
    EXPECT_EQ(log.entriesFrom(9, 100, 100000).size(), 2u);  // near the tail
    EXPECT_TRUE(log.entriesFrom(11, 100, 100000).empty());  // past the tail
}
