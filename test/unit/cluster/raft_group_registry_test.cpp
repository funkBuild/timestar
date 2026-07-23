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

}  // namespace

TEST(RaftGroupRegistryTest, ManyGroupsMultiplexOverSharedTransportAndTimer) {
    testManyGroupsOverSharedTransport().get();
}
