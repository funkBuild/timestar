#include "raft_rpc_transport.hpp"

#include "raft_codec.hpp"


#include <chrono>
#include <cstdint>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>
#include <seastar/rpc/rpc.hh>

namespace timestar::raft {

namespace {

// Minimal seastar-rpc serializer: we only ever ship one already-serialized blob
// (the envelope bytes) as an sstring. write/read are found by ADL on this type.
struct RaftSerializer {};

template <typename Output>
void write(RaftSerializer, Output& out, const seastar::sstring& v) {
    const uint32_t n = static_cast<uint32_t>(v.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.write(v.data(), v.size());
}

template <typename Input>
seastar::sstring read(RaftSerializer, Input& in, seastar::rpc::type<seastar::sstring>) {
    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    seastar::sstring s = seastar::uninitialized_string(n);
    in.read(s.data(), n);
    return s;
}

constexpr uint64_t kDeliverVerb = 1;

// How long to wait before re-attempting a peer connection that failed. seastar's
// rpc::client never reconnects itself, so we recreate it -- but a node runs
// thousands of Raft groups all ticking, so an unthrottled "recreate on every send"
// would hammer a genuinely-down peer with thousands of connects per second. Raft is
// retry-driven (the next tick re-sends), so dropping messages between attempts is
// safe; this only bounds how fast we retry.
constexpr std::chrono::milliseconds kReconnectBackoff{200};

}  // namespace

struct RaftRpcTransport::Impl {
    using Client = seastar::rpc::protocol<RaftSerializer>::client;

    seastar::rpc::protocol<RaftSerializer> proto{RaftSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<RaftSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    // Earliest time we may re-attempt a peer whose connection died (see kReconnectBackoff).
    std::map<NodeId, seastar::lowres_clock::time_point> nextRetry;
    DeliverFn onDeliver;
    seastar::gate gate;
    bool stopping = false;

    // The deliver stub is created ONCE. Allocating one per send (as this used to)
    // burns an allocation on every Raft message -- and a node ticking thousands of
    // groups sends a great many of them.
    RaftRpcTransport::DeliverRawFn onDeliverRaw;
    std::function<seastar::future<>(Client&, seastar::sstring)> deliverStub =
        proto.make_client<seastar::rpc::no_wait_type(seastar::sstring)>(kDeliverVerb);

    // Retire a dead connection without blocking the caller: stop it in the background
    // (under the gate) so its resources are released, keeping it alive until stop
    // resolves.
    void retire(std::unique_ptr<Client> dead) {
        if (!dead || gate.is_closed())
            return;  // shutting down: stop() will not run; just drop it
        auto* raw = dead.get();
        (void)seastar::with_gate(gate, [raw, d = std::move(dead)]() mutable {
            return raw->stop().handle_exception([](std::exception_ptr) {}).finally([d = std::move(d)] {});
        });
    }

    // Open (and cache) the one connection to a peer host, RECONNECTING a dead one.
    //
    // seastar's rpc::client connects exactly once, in the background, and latches
    // error() on failure -- it never reconnects. Raft starts sending the instant the
    // groups tick, which during a rolling start is BEFORE the peers are listening, so
    // the first connection attempt to a not-yet-up peer fails and the cached client is
    // permanently dead. Every later heartbeat/vote to that peer is silently discarded,
    // so its groups never elect a leader (the cluster reports every VShard leaderless
    // forever). Detect the dead client, retire it, and build a fresh one -- throttled
    // by kReconnectBackoff so a genuinely-down peer is not hammered.
    Client* clientFor(NodeId to) {
        auto it = clients.find(to);
        if (it != clients.end()) {
            if (!it->second->error())
                return it->second.get();
            const auto now = seastar::lowres_clock::now();
            auto rit = nextRetry.find(to);
            if (rit != nextRetry.end() && now < rit->second)
                return nullptr;  // backing off: drop this message, Raft re-sends
            nextRetry[to] = now + kReconnectBackoff;
            retire(std::move(it->second));
            clients.erase(it);
        }
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        auto c = std::make_unique<Client>(proto, pit->second);
        auto* p = c.get();
        clients[to] = std::move(c);
        return p;
    }
};

RaftRpcTransport::RaftRpcTransport() : impl_(std::make_unique<Impl>()) {}
RaftRpcTransport::~RaftRpcTransport() = default;

void RaftRpcTransport::addPeer(NodeId id, seastar::socket_address addr) {
    impl_->peers[id] = addr;
}

seastar::future<> RaftRpcTransport::start(seastar::socket_address local, DeliverFn onDeliver) {
    impl_->onDeliver = std::move(onDeliver);
    // A no_wait handler: the sender never awaits a reply (Raft is fire-and-forget
    // and retries via heartbeats), so a send to a slow/dead peer can never block
    // the Ready loop or shutdown. The handler body still runs to completion.
    impl_->proto.register_handler(
        kDeliverVerb, [this](seastar::sstring data) -> seastar::future<seastar::rpc::no_wait_type> {
            // Fast path: route by group id without decoding, so the (potentially
            // 100s of KB) AppendEntries payload is decoded on the shard that owns
            // the group rather than on this single listening shard. groupId is the
            // first field encodeEnvelope writes: u16, little-endian, at offset 0.
            if (impl_->onDeliverRaw) {
                if (data.size() < 2)
                    co_return seastar::rpc::no_wait;
                const uint16_t gid = static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
                                     static_cast<uint16_t>(static_cast<unsigned char>(data[1]) << 8);
                // `data` outlives the call: we await before it is destroyed.
                co_await impl_->onDeliverRaw(gid, data.data(), data.size());
                co_return seastar::rpc::no_wait;
            }
            auto env = decodeEnvelope(std::string(data.data(), data.size()));
            if (env && impl_->onDeliver)
                co_await impl_->onDeliver(std::move(*env));
            co_return seastar::rpc::no_wait;
        });
    // Pin the listening socket to THIS shard. The Raft server and its group registry
    // live on one shard, but seastar's default accept policy scatters incoming
    // connections across ALL shards -- a connection accepted on a shard with no server
    // behind it has its messages silently discarded forever.
    //
    // This verb is no_wait, so the SENDER never hangs; the failure is invisible and
    // ASYMMETRIC. Observed: node 1 (leader of all 4096 groups) saw node 2 caught up on
    // 4096 and node 3 caught up on ZERO -- node 3's append acks never arrived, so
    // matchIndex_[3] stayed 0 and leadership transfer to node 3 could never fire its
    // TimeoutNow (RaftNode::transferLeadership only sends it to a caught-up target).
    // Which peer is affected is per-connection luck, which is why it looks like one
    // "bad" node.
    seastar::listen_options lo;
    lo.reuse_address = true;
    lo.set_fixed_cpu(seastar::this_shard_id());
    impl_->server = std::make_unique<seastar::rpc::protocol<RaftSerializer>::server>(
        impl_->proto, seastar::listen(local, lo));
    return seastar::make_ready_future<>();
}

seastar::future<> RaftRpcTransport::send(Envelope env) {
    if (impl_->stopping || impl_->gate.is_closed())
        return seastar::make_ready_future<>();
    auto* conn = impl_->clientFor(env.message.to);
    if (!conn)
        return seastar::make_ready_future<>();  // unknown peer: drop (Raft retries)

    std::string bytes = encodeEnvelope(env);
    seastar::sstring data(bytes.data(), bytes.size());

    // Fire-and-forget: run the RPC in the background under the gate so the Ready
    // loop is never blocked on the network. Transport errors are swallowed -- a
    // failed send marks the client errored, and the next send reconnects it
    // (clientFor), so a dropped message is just a Raft retry.
    (void)seastar::with_gate(impl_->gate, [this, conn, data = std::move(data)]() mutable {
        return impl_->deliverStub(*conn, std::move(data)).handle_exception([](std::exception_ptr) {});
    });
    return seastar::make_ready_future<>();
}

void RaftRpcTransport::setRawDeliver(DeliverRawFn onDeliverRaw) {
    impl_->onDeliverRaw = std::move(onDeliverRaw);
}

seastar::future<> RaftRpcTransport::stop() {
    // Block new sends first, then ABORT in-flight ones by stopping the peer
    // clients -- otherwise a background send stuck reconnecting to an
    // already-stopped peer would keep the gate from ever closing. Only then is it
    // safe to close the gate (no task still references a client) and clear them.
    impl_->stopping = true;
    for (auto& [id, c] : impl_->clients)
        co_await c->stop();
    co_await impl_->gate.close();
    if (impl_->server)
        co_await impl_->server->stop();
    impl_->clients.clear();
    co_return;
}

}  // namespace timestar::raft
