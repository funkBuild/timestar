// Real-socket transport tests: two RaftRpcTransports exchange an envelope over
// loopback, and a 3-node Raft cluster elects a leader and replicates entirely
// over seastar::rpc (each node's transport routes deliveries into its RaftGroup).
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_rpc_transport.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/socket_defs.hh>
#include <vector>

using namespace timestar::raft;
using namespace std::chrono_literals;

namespace {

seastar::socket_address loopback(uint16_t port) {
    return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", port));
}

// Poll a predicate on the reactor up to a timeout.
seastar::future<bool> waitFor(std::function<bool()> pred) {
    for (int i = 0; i < 600; ++i) {  // ~3s
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

seastar::future<> testLoopbackDelivery() {
    auto t1 = std::make_unique<RaftRpcTransport>();
    auto t2 = std::make_unique<RaftRpcTransport>();
    std::optional<Envelope> received;

    co_await t2->start(loopback(39140), [&](Envelope e) {
        received = std::move(e);
        return seastar::make_ready_future<>();
    });
    co_await t1->start(loopback(39141), [](Envelope) { return seastar::make_ready_future<>(); });
    t1->addPeer(2, loopback(39140));

    Envelope env;
    env.groupId = 42;
    env.message = Message{.to = 2, .from = 1, .payload = RequestVote{false, 7, 1, 3, 2}};
    co_await t1->send(env);

    bool ok = co_await waitFor([&] { return received.has_value(); });
    EXPECT_TRUE(ok);
    if (received) {
        EXPECT_EQ(received->groupId, 42);
        EXPECT_EQ(received->message.from, 1u);
        const auto* rv = std::get_if<RequestVote>(&received->message.payload);
        EXPECT_NE(rv, nullptr);
        if (rv)
            EXPECT_EQ(rv->term, 7u);
    }
    co_await t1->stop();
    co_await t2->stop();
}

seastar::future<> testThreeNodeClusterOverRpc() {
    const std::vector<NodeId> voters = {1, 2, 3};
    const std::map<NodeId, uint16_t> ports = {{1, 39150}, {2, 39151}, {3, 39152}};

    std::map<NodeId, std::unique_ptr<RaftRpcTransport>> transports;
    std::map<NodeId, std::unique_ptr<NoopPersistence>> persistence;
    std::map<NodeId, std::unique_ptr<RecordingSM>> sms;
    std::map<NodeId, std::unique_ptr<RaftGroup>> groups;

    RaftOptions opts;
    opts.electionTimeoutMin = 3;
    opts.electionTimeoutMax = 3;
    opts.heartbeatTimeout = 1;

    for (NodeId id : voters) {
        transports[id] = std::make_unique<RaftRpcTransport>();
        persistence[id] = std::make_unique<NoopPersistence>();
        sms[id] = std::make_unique<RecordingSM>();
        RaftNode node(id, voters, RaftLog{}, HardState{}, opts);
        groups[id] = std::make_unique<RaftGroup>(1, std::move(node), *persistence[id], *transports[id],
                                                 *sms[id]);
    }
    // Wire transports: deliveries route into the local group; peers are the others.
    for (NodeId id : voters) {
        RaftGroup* g = groups[id].get();
        co_await transports[id]->start(loopback(ports.at(id)),
                                       [g](Envelope e) { return g->step(std::move(e.message)); });
        for (NodeId peer : voters)
            if (peer != id)
                transports[id]->addPeer(peer, loopback(ports.at(peer)));
    }

    // Elect node 1 and let messages flow over the sockets.
    co_await groups[1]->campaign();
    bool elected = co_await waitFor([&] { return groups[1]->isLeader(); });
    EXPECT_TRUE(elected);

    // Replicate a couple of proposals and wait for them to apply everywhere.
    co_await groups[1]->propose("one");
    co_await groups[1]->propose("two");
    bool replicated = co_await waitFor([&] {
        for (NodeId id : voters)
            if (sms[id]->applied.size() < 2)
                return false;
        return true;
    });
    EXPECT_TRUE(replicated);
    for (NodeId id : voters) {
        if (sms[id]->applied.size() >= 2) {
            EXPECT_EQ(sms[id]->applied[0], "one");
            EXPECT_EQ(sms[id]->applied[1], "two");
        }
    }

    // Stop all transports concurrently (start every stop, then await each) so
    // connections close mutually rather than one node stopping against dead peers.
    std::vector<seastar::future<>> stops;
    for (NodeId id : voters)
        stops.push_back(transports[id]->stop());
    for (auto& f : stops)
        co_await std::move(f);
}

}  // namespace

TEST(RaftRpcTransportTest, LoopbackDelivery) {
    testLoopbackDelivery().get();
}

TEST(RaftRpcTransportTest, ThreeNodeClusterElectsAndReplicatesOverRpc) {
    testThreeNodeClusterOverRpc().get();
}
