#include "raft_rpc_transport.hpp"

#include "raft_codec.hpp"


#include <cstdint>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
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

}  // namespace

struct RaftRpcTransport::Impl {
    seastar::rpc::protocol<RaftSerializer> proto{RaftSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<RaftSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<seastar::rpc::protocol<RaftSerializer>::client>> clients;
    DeliverFn onDeliver;
    seastar::gate gate;
    bool stopping = false;

    // Lazily open (and cache) the one connection to a peer host.
    seastar::rpc::protocol<RaftSerializer>::client* clientFor(NodeId to) {
        if (auto it = clients.find(to); it != clients.end())
            return it->second.get();
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        auto c = std::make_unique<seastar::rpc::protocol<RaftSerializer>::client>(proto, pit->second);
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
            auto env = decodeEnvelope(std::string(data.data(), data.size()));
            if (env && impl_->onDeliver)
                co_await impl_->onDeliver(std::move(*env));
            co_return seastar::rpc::no_wait;
        });
    impl_->server =
        std::make_unique<seastar::rpc::protocol<RaftSerializer>::server>(impl_->proto, local);
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
    // no_wait client signature: the returned future resolves once the request is
    // written, never waiting for a reply.
    auto call = impl_->proto.make_client<seastar::rpc::no_wait_type(seastar::sstring)>(kDeliverVerb);

    // Fire-and-forget: run the RPC in the background under the gate so the Ready
    // loop is never blocked on the network. Transport errors are swallowed.
    (void)seastar::with_gate(impl_->gate, [conn, call = std::move(call),
                                           data = std::move(data)]() mutable {
        return call(*conn, std::move(data)).handle_exception([](std::exception_ptr) {});
    });
    return seastar::make_ready_future<>();
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
