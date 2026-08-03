// Real-socket transport tests: two RaftRpcTransports exchange an envelope over
// loopback, and a 3-node Raft cluster elects a leader and replicates entirely
// over seastar::rpc (each node's transport routes deliveries into its RaftGroup).
#include "../../../lib/cluster/raft/raft_rpc_transport.hpp"

#include "../../../lib/cluster/raft/raft_group.hpp"

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
#include <seastar/rpc/rpc.hh>
#include <seastar/util/later.hh>
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
        groups[id] = std::make_unique<RaftGroup>(1, std::move(node), *persistence[id], *transports[id], *sms[id]);
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

seastar::future<> testPeerAddressChangeRetiresCachedConnection() {
    auto sender = std::make_unique<RaftRpcTransport>();
    auto oldReceiver = std::make_unique<RaftRpcTransport>();
    auto newReceiver = std::make_unique<RaftRpcTransport>();
    size_t oldReceived = 0;
    size_t newReceived = 0;
    constexpr uint16_t kOldPort = 39166;
    constexpr uint16_t kNewPort = 39167;
    constexpr uint16_t kSenderPort = 39168;

    co_await oldReceiver->start(loopback(kOldPort), [&](Envelope) {
        ++oldReceived;
        return seastar::make_ready_future<>();
    });
    co_await newReceiver->start(loopback(kNewPort), [&](Envelope) {
        ++newReceived;
        return seastar::make_ready_future<>();
    });
    co_await sender->start(loopback(kSenderPort), [](Envelope) { return seastar::make_ready_future<>(); });

    Envelope env;
    env.groupId = 7;
    env.message = Message{.to = 2, .from = 1, .payload = TimeoutNow{1, 1}};
    sender->addPeer(2, loopback(kOldPort));
    co_await sender->send(env);
    EXPECT_TRUE(co_await waitFor([&] { return oldReceived == 1; }));

    sender->addPeer(2, loopback(kNewPort));
    co_await sender->send(env);
    EXPECT_TRUE(co_await waitFor([&] { return newReceived == 1; }));
    EXPECT_EQ(oldReceived, 1u) << "the cached Raft client must not keep using the retired address";

    co_await sender->stop();
    co_await sender->stop();  // failed-start cleanup plus owner cleanup is idempotent
    co_await oldReceiver->stop();
    co_await newReceiver->stop();
}

// --- write-scaleout 5a: multi-envelope frames ---------------------------------------
//
// Many groups' messages to the same peer, produced in one reactor task, must arrive
// intact AND must not each pay for their own RPC frame.
seastar::future<> testBatchedDelivery() {
    auto sender = std::make_unique<RaftRpcTransport>();
    auto receiver = std::make_unique<RaftRpcTransport>();
    std::vector<Envelope> received;

    constexpr uint16_t rxPort = 39160;
    constexpr uint16_t txPort = 39161;
    co_await receiver->start(loopback(rxPort), [&](Envelope e) {
        received.push_back(std::move(e));
        return seastar::make_ready_future<>();
    });
    co_await sender->start(loopback(txPort), [](Envelope) { return seastar::make_ready_future<>(); });
    sender->addPeer(2, loopback(rxPort));

    // First send opens the connection before the burst.
    Envelope warm;
    warm.groupId = 0;
    warm.message = Message{.to = 2, .from = 1, .payload = TimeoutNow{1, 1}};
    co_await sender->send(warm);
    co_await waitFor([&] { return !received.empty(); });

    constexpr int kGroups = 64;
    for (int g = 1; g <= kGroups; ++g) {
        Envelope env;
        env.groupId = static_cast<uint16_t>(g);
        env.message = Message{.to = 2, .from = 1, .payload = RequestVote{false, 7, 1, static_cast<LogIndex>(g), 2}};
        co_await sender->send(env);  // buffered; nothing on the wire until the round ends
    }

    bool ok = co_await waitFor([&] { return received.size() >= kGroups + 1; });
    EXPECT_TRUE(ok) << "delivered " << received.size() << " of " << (kGroups + 1);

    // Every envelope arrives, in order, with its own group id intact -- batching must not
    // reorder or merge messages, only how they travel.
    EXPECT_GE(received.size(), static_cast<size_t>(kGroups + 1));
    for (int g = 1; g <= kGroups && g < static_cast<int>(received.size()); ++g) {
        EXPECT_EQ(received[g].groupId, static_cast<uint16_t>(g));
        const auto* rv = std::get_if<RequestVote>(&received[g].message.payload);
        EXPECT_NE(rv, nullptr);
        if (rv)
            EXPECT_EQ(rv->lastLogIndex, static_cast<LogIndex>(g));
    }

    const auto s = sender->stats();
    EXPECT_EQ(s.envelopesSent, static_cast<uint64_t>(kGroups + 1));
    // THE MEASUREMENT 5a exists for. 65 envelopes must not cost 65 frames.
    EXPECT_LT(s.framesSent, static_cast<uint64_t>(kGroups))
        << "batching did not take: " << s.envelopesSent << " envelopes in " << s.framesSent << " frames";
    EXPECT_EQ(receiver->stats().envelopesRecv, s.envelopesSent);

    co_await sender->stop();
    co_await receiver->stop();
}

// --- debt D-15: how a batch frame's envelopes are DISPATCHED --------------------------
//
// The receive handler used to `co_await` every envelope in a frame in turn -- up to 512
// SEQUENTIAL cross-shard hops in one handler, each paying the previous one's round trip.
// It now runs one chain per group id, concurrently under a bound.
//
// TWO PROPERTIES, and they pull against each other, which is why both are pinned here:
//
//   1. SAME-GROUP ORDER IS PRESERVED. Raft messages within a group are order-sensitive.
//   2. DIFFERENT GROUPS PROGRESS CONCURRENTLY. A slow delivery for one group must not
//      hold up the groups behind it in the same frame -- that is the whole defect.
//
// (2) is measured by making group 1's FIRST delivery block on a real sleep. Under the old
// sequential handler nothing else in the frame could complete until it finished; the
// assertion is that something does.
seastar::future<> testBatchDispatchIsPerGroupOrderedAndCrossGroupConcurrent() {
    auto sender = std::make_unique<RaftRpcTransport>();
    auto receiver = std::make_unique<RaftRpcTransport>();

    constexpr uint16_t kRxPort = 39164;
    constexpr uint16_t kTxPort = 39165;
    constexpr uint16_t kSlowGroup = 1;
    constexpr int kGroups = 8;  // <= the handler's concurrency bound, so all chains start
    constexpr int kPerGroup = 3;

    // Per-group arrival order, and the global order in which deliveries COMPLETED.
    std::map<uint16_t, std::vector<LogIndex>> perGroup;
    std::vector<uint16_t> completionOrder;
    size_t warmCount = 0;

    co_await receiver->start(loopback(kRxPort), [](Envelope) { return seastar::make_ready_future<>(); });
    receiver->setRawDeliver([&](uint16_t gid, const char* bytes, size_t len) -> seastar::future<> {
        auto env = decodeEnvelope(std::string(bytes, len));
        if (!env) {
            EXPECT_TRUE(false) << "undecodable envelope in a batch frame";
            co_return;
        }
        const auto* rv = std::get_if<RequestVote>(&env->message.payload);
        if (!rv) {  // the warm-up TimeoutNow that opens the connection
            ++warmCount;
            co_return;
        }
        // Group 1's first message is SLOW. Everything else is immediate.
        if (gid == kSlowGroup && rv->lastLogIndex == 1)
            co_await seastar::sleep(40ms);
        perGroup[gid].push_back(rv->lastLogIndex);
        completionOrder.push_back(gid);
        co_return;
    });

    co_await sender->start(loopback(kTxPort), [](Envelope) { return seastar::make_ready_future<>(); });
    sender->addPeer(2, loopback(kRxPort));

    // Open the connection before the measured burst.
    Envelope warm;
    warm.groupId = 0;
    warm.message = Message{.to = 2, .from = 1, .payload = TimeoutNow{1, 1}};
    co_await sender->send(warm);
    co_await waitFor([&] { return warmCount > 0; });
    const uint64_t framesBefore = receiver->stats().framesRecv;

    // ONE reactor task: every send buffers into the same round, so the batcher flushes
    // them as a single frame. The slow group goes FIRST -- under sequential dispatch it
    // would gate every envelope behind it.
    for (int i = 1; i <= kPerGroup; ++i) {
        for (int g = kSlowGroup; g <= kGroups; ++g) {
            Envelope env;
            env.groupId = static_cast<uint16_t>(g);
            env.message =
                Message{.to = 2, .from = 1, .payload = RequestVote{false, 7, 1, static_cast<LogIndex>(i), 2}};
            co_await sender->send(env);
        }
    }

    const size_t expected = static_cast<size_t>(kGroups) * kPerGroup;
    const bool all = co_await waitFor([&] { return completionOrder.size() >= expected; });
    EXPECT_TRUE(all) << "delivered " << completionOrder.size() << " of " << expected;

    // The properties below are about dispatch WITHIN one frame, so the burst has to have
    // travelled as one.
    EXPECT_EQ(receiver->stats().framesRecv - framesBefore, 1u)
        << "the burst split across frames; the concurrency assertion would not be measuring this handler";

    // (1) SAME-GROUP ORDER. Each group's three messages arrive 1, 2, 3 -- never resorted
    // by the stable partition, never interleaved with themselves.
    for (int g = kSlowGroup; g <= kGroups; ++g) {
        const auto& seq = perGroup[static_cast<uint16_t>(g)];
        EXPECT_EQ(seq.size(), static_cast<size_t>(kPerGroup)) << "group " << g;
        for (size_t i = 0; i < seq.size(); ++i)
            EXPECT_EQ(seq[i], static_cast<LogIndex>(i + 1)) << "group " << g << " message " << i << " out of order";
    }

    // (2) CROSS-GROUP CONCURRENCY. The slow group's first message was the first envelope
    // in the frame; if any other group completed before it, the chains really did run
    // side by side.
    EXPECT_FALSE(completionOrder.empty());
    if (!completionOrder.empty())
        EXPECT_NE(completionOrder.front(), kSlowGroup)
            << "the frame was dispatched sequentially: the 40ms group gated everything behind it";

    co_await sender->stop();
    co_await receiver->stop();
}

seastar::future<> testOutboundSendAdmissionIsBounded() {
    auto sink = seastar::listen(loopback(39170));
    auto sender = std::make_unique<RaftRpcTransport>();
    co_await sender->start(loopback(39169), [](Envelope) { return seastar::make_ready_future<>(); });
    sender->addPeer(2, loopback(39170));

    constexpr int kMessages = 1024;
    for (int i = 0; i < kMessages; ++i) {
        Envelope env;
        env.groupId = static_cast<uint16_t>(i);
        env.message = Message{.to = 2, .from = 1, .payload = RequestVote{false, 7, 1, 0, 0}};
        co_await sender->send(std::move(env));
        // Let the one v1 batch flush as a one-envelope frame while the local
        // non-reading socket keeps every RPC send unresolved.
        co_await seastar::yield();
    }

    const auto stats = sender->stats();
    EXPECT_GT(stats.backpressured, 0u)
        << "all 1024 unresolved sends were admitted; outbound task memory is unbounded";
    EXPECT_LT(stats.framesSent, static_cast<uint64_t>(kMessages));
    EXPECT_GE(stats.dropped, stats.backpressured);
    co_await sender->stop();
    sink.abort_accept();
}

seastar::future<> testOutboundSendByteAdmissionIsBounded() {
    // A local TCP listener that never accepts or reads keeps the RPC connection
    // unresolved without depending on external routing behavior.
    auto sink = seastar::listen(loopback(39172));
    auto sender = std::make_unique<RaftRpcTransport>();
    co_await sender->start(loopback(39171), [](Envelope) { return seastar::make_ready_future<>(); });
    sender->addPeer(2, loopback(39172));

    constexpr int kMessages = 40;
    constexpr size_t kPayloadBytes = 2 * 1024 * 1024;
    for (int i = 0; i < kMessages; ++i) {
        InstallSnapshot snap;
        snap.term = 7;
        snap.leaderId = 1;
        snap.lastIncludedIndex = 10;
        snap.lastIncludedTerm = 6;
        snap.data.assign(kPayloadBytes, 's');
        snap.totalBytes = kPayloadBytes;

        Envelope env;
        env.groupId = static_cast<uint16_t>(i);
        env.message = Message{.to = 2, .from = 1, .payload = std::move(snap)};
        co_await sender->send(std::move(env));
    }

    const auto stats = sender->stats();
    EXPECT_GT(stats.backpressured, 0u)
        << "forty 2-MiB unresolved frames were admitted; outbound encoded bytes are unbounded";
    EXPECT_LT(stats.framesSent, static_cast<uint64_t>(kMessages));
    EXPECT_GE(stats.dropped, stats.backpressured);
    co_await sender->stop();
    sink.abort_accept();
}

}  // namespace

TEST(RaftRpcTransportTest, LoopbackDelivery) {
    testLoopbackDelivery().get();
}

TEST(RaftRpcTransportTest, PeerAddressChangeRetiresTheCachedConnection) {
    testPeerAddressChangeRetiresCachedConnection().get();
}

TEST(RaftRpcTransportTest, TlsRefusesARegisteredPeerWithoutAnEndpointName) {
    RaftRpcTransport transport;
    transport.addPeer(2, loopback(39174));
    EXPECT_THROW(transport.setTlsCredentials({}, {}, {}), std::logic_error);
}

TEST(RaftRpcTransportTest, ManyGroupMessagesToOnePeerShareFrames) {
    testBatchedDelivery().get();
}

TEST(RaftRpcTransportTest, BatchDispatchKeepsGroupOrderAndRunsGroupsConcurrently) {
    testBatchDispatchIsPerGroupOrderedAndCrossGroupConcurrent().get();
}

TEST(RaftRpcTransportTest, ThreeNodeClusterElectsAndReplicatesOverRpc) {
    testThreeNodeClusterOverRpc().get();
}

TEST(RaftRpcTransportTest, OutboundSendAdmissionIsBounded) {
    testOutboundSendAdmissionIsBounded().get();
}

TEST(RaftRpcTransportTest, OutboundSendByteAdmissionIsBounded) {
    testOutboundSendByteAdmissionIsBounded().get();
}
