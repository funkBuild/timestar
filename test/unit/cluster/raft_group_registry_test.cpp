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
    seastar::future<> persistSnapshot(Snapshot, bool) override { return seastar::make_ready_future<>(); }
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

// HIBERNATION MUST NOT STRETCH THE CHECKQUORUM LEASE (debt D-9 / D-29(b)).
//
// Hibernation decides how often a group is RUN. It must not change what time that group
// thinks it is: every clock in RaftNode::tick is tick-driven, and with CheckQuorum on one of
// them is the disruption-guard LEASE. Skipping nine passes and advancing by one stretched
// the lease (and the election timeout sharing its counter) tenfold -- 2.5-5 s became
// 25-50 s -- so a group whose leader had DIED refused the very votes that would have
// replaced it. Measured live: enabling CheckQuorum took node_kill_round from 49/400 failed
// batches and an 8 s recovery to 153/400 and 43 s.
//
// tickAll now credits the skipped passes to the next tick. This test pins the fix as a
// TIMING property, which is the only thing that matters to the cluster: a hibernating
// follower under CheckQuorum refuses a vote while its lease is valid, KEEPS HIBERNATING
// (the fix costs no CPU, unlike the wake hook it replaced), and reaches its election
// timeout on the real-time schedule rather than ~followerSkip times later.
class DropTransport : public RaftTransport {
public:
    size_t sent = 0;
    seastar::future<> send(Envelope) override {
        ++sent;
        return seastar::make_ready_future<>();
    }
};

seastar::future<> testHibernationDoesNotStretchTheLease() {
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
    co_await seastar::sleep(30ms);
    EXPECT_EQ(g->leader(), 1u);
    const uint64_t skippedBefore = reg.skippedTicks();
    EXPECT_GT(skippedBefore, 0u) << "the follower never hibernated -- the test proves nothing";

    // The leader is now GONE (this transport delivers nothing back and we send it no more
    // heartbeats), so the only thing that can rescue the group is its own election timer.
    // A campaign from node 3 arrives while the lease is still valid and is REFUSED,
    // silently, without a term bump -- the situation whose DURATION is the whole problem.
    RequestVote rv;
    rv.term = 2;
    rv.candidateId = 3;
    co_await reg.deliver(Envelope{1, Message{.to = 2, .from = 3, .payload = rv}});
    EXPECT_EQ(g->currentTerm(), 1u) << "the lease must still refuse the vote (silently)";
    EXPECT_EQ(g->role(), Role::Follower);

    // THE FIX, as a deadline. 60 passes at 1 ms is a 60 ms election timeout; a
    // tenfold-stretched lease needs ~600 ms. Allow generous slack for a loaded box and
    // still fail the stretched behaviour: the group must campaign well inside 300 ms.
    const auto t0 = std::chrono::steady_clock::now();
    bool campaigned = false;
    for (int i = 0; i < 60 && !campaigned; ++i) {
        campaigned = g->currentTerm() > 1;
        if (!campaigned)
            co_await seastar::sleep(5ms);
    }
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_TRUE(campaigned) << "the hibernating follower never reached its election timeout";
    EXPECT_LT(ms, 300) << "it took " << ms << " ms: the lease is still being stretched by hibernation";

    // AND IT NEVER STOPPED HIBERNATING while it was a quiescent follower -- the property the
    // replaced wake hook could not offer, and the reason this fix is free. (Once it
    // campaigns it is a candidate, which always ticks, so the window measured here is the
    // follower phase.)
    EXPECT_GT(reg.skippedTicks(), skippedBefore) << "the group was woken; the fix should need no wake at all";

    co_await reg.stop();
}

// Since skipped passes are credited, a targeted wake is only a request to run the next
// check-tick promptly. Keeping a healthy follower awake for an election-length window is
// both unnecessary and dangerous: a repeated data-plane reset can otherwise pin thousands
// of Raft groups at full tick rate even though their Raft heartbeats are healthy.
seastar::future<> testTargetedWakeRunsOnePassOnly() {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = 60;
    o.heartbeatTimeout = 5;

    DropTransport transport;
    NoopPersistence persistence;
    RecordingSM sm;
    RaftGroupRegistry reg(transport, 1ms);
    reg.addGroup(1, RaftNode(2, {1, 2, 3}, RaftLog{}, HardState{}, o), persistence, sm);

    AppendEntries ae;
    ae.term = 1;
    ae.leaderId = 1;
    co_await reg.deliver(Envelope{1, Message{.to = 2, .from = 1, .payload = ae}});

    // Establish that the healthy, quiescent follower is eligible for hibernation.
    co_await reg.tickAllForTest();
    const uint64_t skippedBeforeWake = reg.skippedTicks();
    EXPECT_EQ(skippedBeforeWake, 1u);
    EXPECT_EQ(reg.wakeFollowersOf(1), 1u);

    // The scheduled pass runs and pays the existing skip credit.
    co_await reg.tickAllForTest();
    EXPECT_EQ(reg.skippedTicks(), skippedBeforeWake);

    // The wake is consumed. A still-healthy follower hibernates again immediately;
    // the old 400-pass wake window fails here by keeping skippedTicks unchanged.
    co_await reg.tickAllForTest();
    EXPECT_EQ(reg.skippedTicks(), skippedBeforeWake + 1);
}

}  // namespace

TEST(RaftGroupRegistryTest, HibernationDoesNotStretchTheLease) {
    testHibernationDoesNotStretchTheLease().get();
}

TEST(RaftGroupRegistryTest, TargetedWakeRunsOnePassOnly) {
    testTargetedWakeRunsOnePassOnly().get();
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
