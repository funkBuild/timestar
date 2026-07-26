// Many Raft groups per node, all multiplexed over ONE shared seastar::rpc
// transport and driven by ONE shared periodic timer per node -- the "thousands
// of lightweight groups without per-group services" property. Here 25 groups
// across 3 nodes elect leaders (timer-driven) and replicate over shared sockets.
#include "../../../lib/cluster/raft/raft_group_registry.hpp"

#include "../../../lib/cluster/raft/raft_rpc_transport.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <vector>

using namespace timestar::raft;
using namespace std::chrono_literals;

namespace {

seastar::socket_address loopback(uint16_t port) {
    return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", port));
}

seastar::future<bool> waitFor(std::function<bool()> pred) {
    for (int i = 0; i < 800; ++i) {
        if (pred())
            co_return true;
        co_await seastar::sleep(5ms);
    }
    co_return pred();
}

class NoopPersistence : public RaftPersistence {
public:
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<LogEntry>) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistSnapshot(Snapshot) override { return seastar::make_ready_future<>(); }
    seastar::future<> sync() override { return seastar::make_ready_future<>(); }
};

class RecordingSM : public RaftStateMachine {
public:
    std::vector<std::string> applied;
    seastar::future<> apply(LogEntry e) override {
        applied.push_back(e.data);
        return seastar::make_ready_future<>();
    }
    seastar::future<> applySnapshot(Snapshot) override { return seastar::make_ready_future<>(); }
};

struct NodeState {
    std::unique_ptr<RaftRpcTransport> transport;
    std::unique_ptr<RaftGroupRegistry> registry;
    std::vector<std::unique_ptr<NoopPersistence>> persistence;
    std::vector<std::unique_ptr<RecordingSM>> sms;  // one per group, index = groupId-1
};

seastar::future<> testManyGroupsOverSharedTransport() {
    const std::vector<NodeId> voters = {1, 2, 3};
    const std::map<NodeId, uint16_t> ports = {{1, 39160}, {2, 39161}, {3, 39162}};
    const uint16_t kGroups = 25;

    std::map<NodeId, NodeState> nodes;

    auto optsFor = [](NodeId id) {
        RaftOptions o;
        o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 10);  // node 1 wins
        o.heartbeatTimeout = 1;
        return o;
    };

    for (NodeId id : voters) {
        NodeState st;
        st.transport = std::make_unique<RaftRpcTransport>();
        st.registry = std::make_unique<RaftGroupRegistry>(*st.transport, 5ms);
        for (uint16_t g = 1; g <= kGroups; ++g) {
            st.persistence.push_back(std::make_unique<NoopPersistence>());
            st.sms.push_back(std::make_unique<RecordingSM>());
            RaftNode node(id, voters, RaftLog{}, HardState{}, optsFor(id));
            st.registry->addGroup(g, std::move(node), *st.persistence.back(), *st.sms.back());
        }
        nodes[id] = std::move(st);
    }

    for (NodeId id : voters) {
        RaftGroupRegistry* reg = nodes[id].registry.get();
        co_await nodes[id].transport->start(loopback(ports.at(id)),
                                            [reg](Envelope e) { return reg->deliver(std::move(e)); });
        for (NodeId peer : voters)
            if (peer != id)
                nodes[id].transport->addPeer(peer, loopback(ports.at(peer)));
    }
    for (NodeId id : voters)
        nodes[id].registry->startTicking();

    // Every group elects node 1 as leader (timer-driven, no explicit campaign).
    bool allLed = co_await waitFor([&] {
        for (uint16_t g = 1; g <= kGroups; ++g)
            if (!nodes[1].registry->group(g)->isLeader())
                return false;
        return true;
    });
    EXPECT_TRUE(allLed);

    // Propose a distinct value into every group through its leader.
    for (uint16_t g = 1; g <= kGroups; ++g)
        co_await nodes[1].registry->group(g)->propose("g" + std::to_string(g));

    // Every group's entry replicates and applies on all three nodes.
    bool allReplicated = co_await waitFor([&] {
        for (NodeId id : voters)
            for (uint16_t g = 1; g <= kGroups; ++g)
                if (nodes[id].sms[g - 1]->applied.size() != 1)
                    return false;
        return true;
    });
    EXPECT_TRUE(allReplicated);

    if (allReplicated) {
        for (NodeId id : voters)
            for (uint16_t g = 1; g <= kGroups; ++g)
                EXPECT_EQ(nodes[id].sms[g - 1]->applied[0], "g" + std::to_string(g));
    }

    // Clean shutdown: stop registries (timers) then transports, concurrently.
    std::vector<seastar::future<>> stops;
    for (NodeId id : voters)
        stops.push_back(nodes[id].registry->stop());
    for (auto& f : stops)
        co_await std::move(f);
    std::vector<seastar::future<>> tstops;
    for (NodeId id : voters)
        tstops.push_back(nodes[id].transport->stop());
    for (auto& f : tstops)
        co_await std::move(f);
}

seastar::future<> testHibernationSkipsIdleFollowersButStillReplicates() {
    const std::vector<NodeId> voters = {1, 2, 3};
    const std::map<NodeId, uint16_t> ports = {{1, 39170}, {2, 39171}, {3, 39172}};
    const uint16_t kGroups = 6;
    std::map<NodeId, NodeState> nodes;
    auto optsFor = [](NodeId id) {
        RaftOptions o;
        o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 30);
        o.heartbeatTimeout = 1;
        return o;
    };
    for (NodeId id : voters) {
        NodeState st;
        st.transport = std::make_unique<RaftRpcTransport>();
        st.registry = std::make_unique<RaftGroupRegistry>(*st.transport, 5ms);
        for (uint16_t g = 1; g <= kGroups; ++g) {
            st.persistence.push_back(std::make_unique<NoopPersistence>());
            st.sms.push_back(std::make_unique<RecordingSM>());
            RaftNode node(id, voters, RaftLog{}, HardState{}, optsFor(id));
            st.registry->addGroup(g, std::move(node), *st.persistence.back(), *st.sms.back());
        }
        nodes[id] = std::move(st);
    }
    for (NodeId id : voters) {
        RaftGroupRegistry* reg = nodes[id].registry.get();
        co_await nodes[id].transport->start(loopback(ports.at(id)),
                                            [reg](Envelope e) { return reg->deliver(std::move(e)); });
        for (NodeId peer : voters)
            if (peer != id)
                nodes[id].transport->addPeer(peer, loopback(ports.at(peer)));
    }
    for (NodeId id : voters)
        nodes[id].registry->startTicking();

    bool led = co_await waitFor([&] {
        for (uint16_t g = 1; g <= kGroups; ++g)
            if (!nodes[1].registry->group(g)->isLeader())
                return false;
        return true;
    });
    EXPECT_TRUE(led);

    // Let the cluster idle so quiescent followers on nodes 2 and 3 hibernate.
    co_await seastar::sleep(200ms);
    EXPECT_GT(nodes[2].registry->skippedTicks(), 0u);  // idle followers were skipped
    EXPECT_GT(nodes[3].registry->skippedTicks(), 0u);

    // A hibernated follower still RECEIVES replication (deliver is independent of
    // its own ticking): a proposal applies on all nodes.
    co_await nodes[1].registry->group(3)->propose("live");
    bool ok = co_await waitFor([&] {
        for (NodeId id : voters)
            if (nodes[id].sms[2]->applied.empty())
                return false;
        return true;
    });
    EXPECT_TRUE(ok);
    if (ok)
        EXPECT_EQ(nodes[3].sms[2]->applied[0], "live");

    std::vector<seastar::future<>> stops;
    for (NodeId id : voters)
        stops.push_back(nodes[id].registry->stop());
    for (auto& f : stops)
        co_await std::move(f);
    std::vector<seastar::future<>> tstops;
    for (NodeId id : voters)
        tstops.push_back(nodes[id].transport->stop());
    for (auto& f : tstops)
        co_await std::move(f);
}

seastar::future<> testManyGroupsFailOverWhenLeaderNodeCrashes() {
    const std::vector<NodeId> voters = {1, 2, 3};
    const std::map<NodeId, uint16_t> ports = {{1, 39180}, {2, 39181}, {3, 39182}};
    const uint16_t kGroups = 20;
    std::map<NodeId, NodeState> nodes;
    auto optsFor = [](NodeId id) {
        RaftOptions o;  // node 1 leads; node 2 is next-shortest so it wins after a crash
        o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : (id == 2 ? 6 : 25));
        o.heartbeatTimeout = 1;
        return o;
    };
    for (NodeId id : voters) {
        NodeState st;
        st.transport = std::make_unique<RaftRpcTransport>();
        st.registry = std::make_unique<RaftGroupRegistry>(*st.transport, 5ms);
        for (uint16_t g = 1; g <= kGroups; ++g) {
            st.persistence.push_back(std::make_unique<NoopPersistence>());
            st.sms.push_back(std::make_unique<RecordingSM>());
            RaftNode node(id, voters, RaftLog{}, HardState{}, optsFor(id));
            st.registry->addGroup(g, std::move(node), *st.persistence.back(), *st.sms.back());
        }
        nodes[id] = std::move(st);
    }
    for (NodeId id : voters) {
        RaftGroupRegistry* reg = nodes[id].registry.get();
        co_await nodes[id].transport->start(loopback(ports.at(id)),
                                            [reg](Envelope e) { return reg->deliver(std::move(e)); });
        for (NodeId peer : voters)
            if (peer != id)
                nodes[id].transport->addPeer(peer, loopback(ports.at(peer)));
    }
    for (NodeId id : voters)
        nodes[id].registry->startTicking();

    bool led = co_await waitFor([&] {
        for (uint16_t g = 1; g <= kGroups; ++g)
            if (!nodes[1].registry->group(g)->isLeader())
                return false;
        return true;
    });
    EXPECT_TRUE(led);

    // CRASH the node that leads every group.
    co_await nodes[1].registry->stop();
    co_await nodes[1].transport->stop();

    // Every group must re-elect a surviving leader within a bounded time -- even
    // hibernated follower groups, via their periodic check-tick.
    bool refailed = co_await waitFor([&] {
        for (uint16_t g = 1; g <= kGroups; ++g) {
            const bool has = nodes[2].registry->group(g)->isLeader() || nodes[3].registry->group(g)->isLeader();
            if (!has)
                return false;
        }
        return true;
    });
    EXPECT_TRUE(refailed);

    std::vector<seastar::future<>> stops;
    for (NodeId id : {2u, 3u})
        stops.push_back(nodes[id].registry->stop());
    for (auto& f : stops)
        co_await std::move(f);
    std::vector<seastar::future<>> tstops;
    for (NodeId id : {2u, 3u})
        tstops.push_back(nodes[id].transport->stop());
    for (auto& f : tstops)
        co_await std::move(f);
}

// HIBERNATION MUST NOT STRETCH THE CHECKQUORUM LEASE (debt D-9).
//
// A quiescent follower ticks 1-in-10, which stretches every tick-driven clock in the group
// by 10x -- including, once CheckQuorum is on, the disruption-guard lease. A vote request
// arriving inside that stretched lease is dropped SILENTLY (no term bump), so a group whose
// leader has DIED cannot be voted into a new one for 25-50 s instead of 2.5-5 s. Measured
// live before the fix: enabling CheckQuorum took node_kill_round from 49/400 failed batches
// and an 8 s recovery to 153/400 and 43 s.
//
// deliver() therefore wakes a group whose step DROPPED a vote under that lease -- the drop
// and not the vote, so an ordinary election and every leadership transfer (whose vote
// bypasses the lease) leave hibernation alone. This test asserts the four steps in order:
// the group hibernates; the vote is REFUSED and COUNTED (which is why the wake is needed at
// all, and it is asserted, not assumed); from then on the group ticks at full rate; and its
// lease therefore expires at the normal election timeout rather than 10x it.
class DropTransport : public RaftTransport {
public:
    size_t sent = 0;
    seastar::future<> send(Envelope) override {
        ++sent;
        return seastar::make_ready_future<>();
    }
};

seastar::future<> testASolicitedVoteUnHibernatesTheVoter() {
    RaftOptions o;
    o.checkQuorum = true;
    o.electionTimeoutMin = o.electionTimeoutMax = 60;  // 60 passes at the 1 ms tick below
    o.heartbeatTimeout = 5;

    DropTransport transport;
    NoopPersistence persistence;
    RecordingSM sm;
    RaftGroupRegistry reg(transport, 1ms);
    reg.addGroup(1, RaftNode(2, {1, 2, 3}, RaftLog{}, HardState{}, o), persistence, sm);
    reg.startTicking();
    RaftGroup* g = reg.group(1);

    // Hear from leader 1 -> quiescent follower -> hibernating.
    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 1;
    co_await reg.deliver(Envelope{1, Message{.to = 2, .from = 1, .payload = ae}});
    co_await seastar::sleep(40ms);
    EXPECT_EQ(g->leader(), 1u);
    const uint64_t skippedBefore = reg.skippedTicks();
    EXPECT_GT(skippedBefore, 0u) << "the follower never hibernated -- the test proves nothing";
    co_await seastar::sleep(30ms);
    EXPECT_GT(reg.skippedTicks(), skippedBefore) << "still hibernating (the control for below)";

    // A campaign from node 3. It is REFUSED -- the lease is still valid and it does not even
    // bump our term -- which is exactly the situation the wake has to get us out of.
    RequestVote rv;
    rv.term = 2;
    rv.candidateId = 3;
    co_await reg.deliver(Envelope{1, Message{.to = 2, .from = 3, .payload = rv}});
    EXPECT_EQ(g->currentTerm(), 1u) << "the lease must still refuse the vote (silently)";
    EXPECT_EQ(g->role(), Role::Follower);
    EXPECT_EQ(g->node().leaseDroppedVotes(), 1u) << "the drop is the wake trigger; it must be counted";

    // ... but the group is now awake: no more skipped ticks while it is still a follower,
    // so its lease runs down in REAL time.
    const uint64_t skippedAtVote = reg.skippedTicks();
    co_await seastar::sleep(30ms);  // < the 60-pass election timeout, so still a follower
    EXPECT_EQ(reg.skippedTicks(), skippedAtVote) << "the voter is still hibernating: its lease is 10x too long";

    // And the lease does expire, at the normal election timeout rather than 10x it: the
    // group times out and campaigns (it cannot WIN here -- sends are dropped -- so the
    // observable is the term bump).
    bool campaigned = co_await waitFor([&] { return g->currentTerm() > 1; });
    EXPECT_TRUE(campaigned) << "the woken voter never reached its election timeout";

    co_await reg.stop();
}

}  // namespace

TEST(RaftGroupRegistryTest, ASolicitedVoteUnHibernatesTheVoter) {
    testASolicitedVoteUnHibernatesTheVoter().get();
}

TEST(RaftGroupRegistryTest, ManyGroupsMultiplexOverSharedTransportAndTimer) {
    testManyGroupsOverSharedTransport().get();
}

TEST(RaftGroupRegistryTest, ManyGroupsFailOverWhenLeaderNodeCrashes) {
    testManyGroupsFailOverWhenLeaderNodeCrashes().get();
}

TEST(RaftGroupRegistryTest, HibernationSkipsIdleFollowersButStillReplicates) {
    testHibernationSkipsIdleFollowersButStillReplicates().get();
}
