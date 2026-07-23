// End-to-end multi-node Raft: a deterministic in-memory network drives real
// elections and log replication across several RaftNode state machines. No
// reactor, no I/O -- the network stands in for the Seastar-RPC + journal driver
// and honours the Ready contract (persist hardState+entries, then send, then
// apply committed).
#include "../../../lib/cluster/raft/raft_node.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <set>
#include <vector>

using namespace timestar::raft;

namespace {

std::vector<std::string> splitCommands(const std::string& data) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : data) {
        if (c == '\n') {
            if (!cur.empty())
                out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty())
        out.push_back(cur);
    return out;
}

std::string joinCommands(const std::vector<std::string>& cmds) {
    std::string out;
    for (const auto& c : cmds) {
        out += c;
        out += '\n';
    }
    return out;
}

class Network {
public:
    Network(std::vector<NodeId> voters, RaftOptions opts, std::vector<NodeId> learners = {})
        : opts_(opts), ids_(voters) {
        ids_.insert(ids_.end(), learners.begin(), learners.end());
        for (NodeId id : ids_) {
            nodes_[id] =
                std::make_unique<RaftNode>(id, voters, RaftLog{}, HardState{}, opts, learners);
            applied_[id] = {};
        }
    }

    // Inject a node that starts OUTSIDE the voting config (an observer), so a
    // membership change can bring it in. It knows the initial config only.
    void addNode(NodeId id, std::vector<NodeId> knownVoters) {
        ids_.push_back(id);
        nodes_[id] =
            std::make_unique<RaftNode>(id, knownVoters, RaftLog{}, HardState{}, opts_, std::vector<NodeId>{});
        applied_[id] = {};
    }

    RaftNode& node(NodeId id) { return *nodes_.at(id); }
    const std::vector<std::string>& applied(NodeId id) const { return applied_.at(id); }

    // Drain every node's Ready, route messages to reachable peers, apply
    // committed entries, and repeat until the system is quiescent.
    void run() {
        bool progress = true;
        int guard = 0;
        while (progress && guard++ < 100000) {
            progress = false;
            for (NodeId id : ids_) {
                RaftNode& n = *nodes_[id];
                if (!n.hasReady())
                    continue;
                RaftNode::Ready rd = n.ready();
                n.advance(rd);
                if (rd.snapshot) {
                    // The driver installs the snapshot as the new applied state.
                    // Our payload encodes the command list joined by '\n'.
                    applied_[id] = splitCommands(rd.snapshot->data);
                }
                for (auto& e : rd.committed) {
                    // Apply only real application commands: skip no-op commit
                    // markers (empty data) and ConfigChange entries.
                    if (e.type == EntryType::Normal && !e.data.empty())
                        applied_[id].push_back(e.data);
                }
                for (auto& m : rd.messages) {
                    if (reachable(m.from, m.to))
                        inflight_.push_back(m);
                }
                progress = true;
            }
            std::deque<Message> batch;
            batch.swap(inflight_);
            for (auto& m : batch) {
                if (nodes_.count(m.to)) {
                    nodes_[m.to]->step(m);
                    progress = true;
                }
            }
        }
    }

    void tickAll() {
        for (NodeId id : ids_)
            nodes_[id]->tick();
    }

    // Partition control: drop messages between partitioned groups.
    void isolate(NodeId id) { isolated_.insert(id); }
    void heal(NodeId id) { isolated_.erase(id); }

    NodeId leader() const {
        for (NodeId id : ids_)
            if (nodes_.at(id)->isLeader())
                return id;
        return kNoNode;
    }
    int leaderCount() const {
        int n = 0;
        for (NodeId id : ids_)
            if (nodes_.at(id)->isLeader())
                ++n;
        return n;
    }

private:
    bool reachable(NodeId from, NodeId to) const {
        return isolated_.count(from) == 0 && isolated_.count(to) == 0;
    }

    RaftOptions opts_;
    std::vector<NodeId> ids_;
    std::map<NodeId, std::unique_ptr<RaftNode>> nodes_;
    std::map<NodeId, std::vector<std::string>> applied_;
    std::set<NodeId> isolated_;
    std::deque<Message> inflight_;
};

RaftOptions opts() {
    RaftOptions o;
    o.electionTimeoutMin = 10;
    o.electionTimeoutMax = 10;  // deterministic; we drive campaigns explicitly
    o.heartbeatTimeout = 1;
    return o;
}

RaftOptions optsPreVoteCheckQuorum() {
    RaftOptions o = opts();
    o.preVote = true;
    o.checkQuorum = true;
    return o;
}

}  // namespace

TEST(RaftClusterTest, ElectsExactlyOneLeader) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    EXPECT_EQ(net.leaderCount(), 1);
    EXPECT_EQ(net.leader(), 1u);
    EXPECT_EQ(net.node(2).role(), Role::Follower);
    EXPECT_EQ(net.node(3).role(), Role::Follower);
    EXPECT_EQ(net.node(2).currentTerm(), net.node(1).currentTerm());
    EXPECT_EQ(net.node(2).leader(), 1u);
}

TEST(RaftClusterTest, ReplicatesAndCommitsAProposal) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    EXPECT_TRUE(net.node(1).propose("alpha"));
    EXPECT_TRUE(net.node(1).propose("beta"));
    net.run();

    // Every node commits and applies both commands, in order.
    for (NodeId id : {1u, 2u, 3u}) {
        ASSERT_EQ(net.applied(id).size(), 2u) << "node " << id;
        EXPECT_EQ(net.applied(id)[0], "alpha");
        EXPECT_EQ(net.applied(id)[1], "beta");
    }
    EXPECT_GE(net.node(1).commitIndex(), 3u);  // noop@1 + alpha@2 + beta@3
    EXPECT_EQ(net.node(2).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftClusterTest, CommitsWithOneNodeDownButMajorityUp) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    net.isolate(3);  // node 3 offline; 1 and 2 still form a majority
    EXPECT_TRUE(net.node(1).propose("gamma"));
    net.run();

    EXPECT_EQ(net.applied(1).size(), 1u);
    EXPECT_EQ(net.applied(2).size(), 1u);
    EXPECT_EQ(net.applied(1)[0], "gamma");
    EXPECT_EQ(net.applied(3).size(), 0u);  // isolated, never received it

    // Heal node 3: a heartbeat catches it up.
    net.heal(3);
    net.tickAll();
    net.run();
    EXPECT_EQ(net.applied(3).size(), 1u);
    EXPECT_EQ(net.applied(3)[0], "gamma");
}

TEST(RaftClusterTest, NewLeaderCatchesUpALaggingFollower) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    // Node 3 misses a batch of proposals entirely.
    net.isolate(3);
    net.node(1).propose("a");
    net.node(1).propose("b");
    net.node(1).propose("c");
    net.run();
    ASSERT_EQ(net.applied(3).size(), 0u);

    // Rejoin: leader backtracks nextIndex via the conflict hint and refills 3.
    net.heal(3);
    net.tickAll();
    net.run();
    ASSERT_EQ(net.applied(3).size(), 3u);
    EXPECT_EQ(net.applied(3)[0], "a");
    EXPECT_EQ(net.applied(3)[2], "c");
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());
}

TEST(RaftClusterTest, PreVoteElectsALeaderAndReplicates) {
    Network net({1, 2, 3}, optsPreVoteCheckQuorum());
    net.node(1).campaign();  // -> PreCandidate, then Candidate, then Leader
    net.run();
    ASSERT_EQ(net.leaderCount(), 1);
    ASSERT_EQ(net.leader(), 1u);
    EXPECT_EQ(net.node(1).currentTerm(), 1u);  // exactly one real term bump
    EXPECT_TRUE(net.node(1).propose("x"));
    net.run();
    for (NodeId id : {1u, 2u, 3u}) {
        ASSERT_EQ(net.applied(id).size(), 1u) << "node " << id;
        EXPECT_EQ(net.applied(id)[0], "x");
    }
}

TEST(RaftClusterTest, PreVoteStopsAnIsolatedNodeFromInflatingTerm) {
    Network net({1, 2, 3}, optsPreVoteCheckQuorum());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);
    const Term termBefore = net.node(2).currentTerm();

    // Isolate node 3; it repeatedly times out and pre-campaigns, but its PreVotes
    // reach no one, so it never bumps its own term or anyone else's.
    net.isolate(3);
    for (int i = 0; i < 5; ++i) {
        net.node(3).tick();  // times out every 10 ticks... drive many
        for (int j = 0; j < 12; ++j)
            net.node(3).tick();
        net.run();
    }
    // Node 3's term did not inflate past its starting term (PreVote never bumps).
    EXPECT_EQ(net.node(3).currentTerm(), termBefore);
    EXPECT_NE(net.node(3).role(), Role::Leader);

    // Heal: node 3 rejoins as a follower without ever having disrupted the leader.
    net.heal(3);
    net.tickAll();
    net.run();
    EXPECT_EQ(net.leader(), 1u);
    EXPECT_EQ(net.node(1).currentTerm(), termBefore);
    EXPECT_EQ(net.node(3).role(), Role::Follower);
}

TEST(RaftClusterTest, CheckQuorumStepsDownIsolatedLeader) {
    Network net({1, 2, 3}, optsPreVoteCheckQuorum());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    // Isolate the leader from both followers. CheckQuorum detects the lost
    // majority within (up to) two election timeouts -- the first check still sees
    // the pre-isolation contact window, the second sees silence -- and steps down.
    net.isolate(1);
    for (int i = 0; i < 25; ++i) {
        net.node(1).tick();
        net.run();
        if (net.node(1).role() == Role::Follower)
            break;
    }
    EXPECT_EQ(net.node(1).role(), Role::Follower);
}

TEST(RaftClusterTest, JointConsensusAddsAVoter) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);
    net.node(1).propose("a");
    net.run();

    net.addNode(4, {1, 2, 3});  // observer, not yet in the config
    // Grow the cluster to {1,2,3,4}.
    ASSERT_TRUE(net.node(1).proposeConfChange({1, 2, 3, 4}, {}));
    net.run();

    // The change committed and left the joint state everywhere.
    for (NodeId id : {1u, 2u, 3u, 4u}) {
        EXPECT_FALSE(net.node(id).config().joint()) << "node " << id;
        EXPECT_EQ(net.node(id).config().voters, (std::vector<NodeId>{1, 2, 3, 4})) << "node " << id;
    }
    // Node 4 caught up the earlier history.
    ASSERT_EQ(net.applied(4).size(), 1u);
    EXPECT_EQ(net.applied(4)[0], "a");

    // A new proposal now needs a majority of FOUR (3 nodes). Isolate two voters:
    // it must NOT commit with only two of four.
    net.isolate(3);
    net.isolate(4);
    net.node(1).propose("b");
    net.run();
    EXPECT_EQ(net.applied(1).size(), 1u);  // "b" not committed (only 1,2 up = 2 of 4)

    // Restore one: 3 of 4 is a majority, so it commits.
    net.heal(3);
    net.tickAll();
    net.run();
    EXPECT_EQ(net.applied(1).back(), "b");
}

TEST(RaftClusterTest, JointConsensusReplacesAndRemovesAVoter) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    net.addNode(4, {1, 2, 3});
    // Replace node 3 with node 4: Cnew = {1,2,4}.
    ASSERT_TRUE(net.node(1).proposeConfChange({1, 2, 4}, {}));
    net.run();
    for (NodeId id : {1u, 2u, 4u}) {
        EXPECT_FALSE(net.node(id).config().joint());
        EXPECT_EQ(net.node(id).config().voters, (std::vector<NodeId>{1, 2, 4}));
    }

    // With node 3 removed, {1,2,4} alone commits even if 3 is gone.
    net.isolate(3);
    net.node(1).propose("z");
    net.run();
    EXPECT_EQ(net.applied(1).back(), "z");
    EXPECT_EQ(net.applied(4).back(), "z");
}

TEST(RaftClusterTest, JointConsensusShrinkToSingleVoterCommits) {
    // Regression: shrinking to a single-voter final config must still COMMIT the
    // Cnew entry (no follower to ack it), so a later proposal commits at once.
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    ASSERT_TRUE(net.node(1).proposeConfChange({1}, {}));  // Cnew = {1}
    net.run();
    EXPECT_FALSE(net.node(1).config().joint());
    EXPECT_EQ(net.node(1).config().voters, (std::vector<NodeId>{1}));
    // The final config entry is committed; a fresh proposal commits immediately
    // (node 1 is the whole quorum) with the other nodes gone.
    net.isolate(2);
    net.isolate(3);
    EXPECT_TRUE(net.node(1).propose("solo"));
    net.run();
    EXPECT_EQ(net.applied(1).back(), "solo");
}

TEST(RaftClusterTest, ConfChangeRejectsEmptyVoters) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);
    EXPECT_FALSE(net.node(1).proposeConfChange({}, {}));  // would wedge the group
    EXPECT_FALSE(net.node(1).config().joint());
    EXPECT_EQ(net.node(1).config().voters, (std::vector<NodeId>{1, 2, 3}));
}

TEST(RaftClusterTest, LearnerReplicatesButDoesNotVoteOrCount) {
    // Voters {1,2}, learner {3}. Quorum is 2 (both voters), unaffected by 3.
    Network net({1, 2}, opts(), {3});
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);
    EXPECT_EQ(net.node(3).role(), Role::Follower);  // learners never lead

    net.node(1).propose("a");
    net.run();
    // The learner received and applied the entry (replication reaches it)...
    EXPECT_EQ(net.applied(3).size(), 1u);
    EXPECT_EQ(net.applied(3)[0], "a");

    // ...but commit did not depend on it: isolate the learner and a proposal
    // still commits on the two voters alone.
    net.isolate(3);
    net.node(1).propose("b");
    net.run();
    EXPECT_EQ(net.applied(1).size(), 2u);
    EXPECT_EQ(net.applied(2).size(), 2u);

    // A learner never times out into a candidacy, even isolated.
    for (int i = 0; i < 50; ++i)
        net.node(3).tick();
    net.run();
    EXPECT_NE(net.node(3).role(), Role::Candidate);
    EXPECT_NE(net.node(3).role(), Role::Leader);
}

TEST(RaftClusterTest, LeaderTransferMovesLeadership) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);
    net.node(1).propose("x");
    net.run();

    // Hand leadership to node 2.
    net.node(1).transferLeadership(2);
    net.run();
    EXPECT_EQ(net.node(2).role(), Role::Leader);
    EXPECT_EQ(net.node(1).role(), Role::Follower);
    EXPECT_GT(net.node(2).currentTerm(), 1u);

    // The new leader serves writes; the old leader follows.
    EXPECT_TRUE(net.node(2).propose("y"));
    net.run();
    EXPECT_EQ(net.applied(1).back(), "y");
    EXPECT_EQ(net.applied(2).back(), "y");
}

TEST(RaftClusterTest, LaggingFollowerCaughtUpByInstallSnapshot) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    // Node 3 misses a batch entirely.
    net.isolate(3);
    net.node(1).propose("a");
    net.node(1).propose("b");
    net.node(1).propose("c");
    net.run();
    ASSERT_EQ(net.applied(3).size(), 0u);

    // Leader compacts its log past node 3's position -- so the entries node 3
    // needs no longer exist as log entries, forcing an InstallSnapshot on rejoin.
    const LogIndex commit = net.node(1).commitIndex();
    net.node(1).compact(commit, joinCommands(net.applied(1)));
    // The needed entries are now below the leader's snapshot boundary.
    ASSERT_GT(net.node(1).log().snapshotIndex(), 0u);

    net.heal(3);
    net.tickAll();
    net.run();

    // Node 3 was caught up via the snapshot and holds the full command history.
    ASSERT_EQ(net.applied(3).size(), 3u);
    EXPECT_EQ(net.applied(3)[0], "a");
    EXPECT_EQ(net.applied(3)[2], "c");
    EXPECT_EQ(net.node(3).commitIndex(), net.node(1).commitIndex());

    // And normal replication resumes on top of the snapshot.
    net.node(1).propose("d");
    net.run();
    ASSERT_EQ(net.applied(3).size(), 4u);
    EXPECT_EQ(net.applied(3)[3], "d");
}

TEST(RaftClusterTest, LeaderStepsDownWhenPartitionedFromMajority) {
    Network net({1, 2, 3}, opts());
    net.node(1).campaign();
    net.run();
    ASSERT_EQ(net.leader(), 1u);

    // Partition the leader away from the majority; elect a new leader among {2,3}.
    net.isolate(1);
    net.node(2).campaign();
    net.run();
    EXPECT_EQ(net.node(2).currentTerm(), 2u);
    NodeId newLeader = net.node(2).isLeader() ? 2u : (net.node(3).isLeader() ? 3u : kNoNode);
    ASSERT_NE(newLeader, kNoNode);

    // The new leader commits a proposal without the old one.
    EXPECT_TRUE(net.node(newLeader).propose("post-partition"));
    net.run();
    EXPECT_EQ(net.applied(newLeader).size(), 1u);

    // Heal: the old leader learns the higher term and steps down.
    net.heal(1);
    net.tickAll();
    net.run();
    EXPECT_EQ(net.node(1).role(), Role::Follower);
    EXPECT_EQ(net.node(1).currentTerm(), 2u);
    EXPECT_EQ(net.applied(1).size(), 1u);  // catches up the new leader's entry
}
