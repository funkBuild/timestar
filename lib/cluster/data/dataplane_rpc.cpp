#include "dataplane_rpc.hpp"

#include "dataplane_codec.hpp"
#include "node_query.hpp"
#include "write_record.hpp"

#include <cstdint>
#include <functional>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/api.hh>
#include <seastar/rpc/rpc.hh>
#include <stdexcept>

namespace timestar::data {

namespace {

struct DpSerializer {};

template <typename Output>
void write(DpSerializer, Output& out, const seastar::sstring& v) {
    const uint32_t n = static_cast<uint32_t>(v.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.write(v.data(), v.size());
}
template <typename Input>
seastar::sstring read(DpSerializer, Input& in, seastar::rpc::type<seastar::sstring>) {
    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    seastar::sstring s = seastar::uninitialized_string(n);
    in.read(s.data(), n);
    return s;
}

constexpr uint64_t kForwardWrites = 1;      // sstring -> sstring (legacy DataPoint, waited: applied)
constexpr uint64_t kQueryRemote = 2;        // sstring -> sstring (legacy DataPoint, waited: partial)
constexpr uint64_t kForwardWriteBatch = 3;  // sstring -> sstring (enriched WriteBatch, waited: applied)
constexpr uint64_t kQueryNode = 4;          // sstring -> sstring (enriched NodeQueryRequest, waited: partial)

}  // namespace

struct DataPlaneRpc::Impl {
    using Client = seastar::rpc::protocol<DpSerializer>::client;
    seastar::rpc::protocol<DpSerializer> proto{DpSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<DpSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<Client>> clients;
    LocalStore* sink = nullptr;       // legacy DataPoint path
    NodeStore* nodeSink = nullptr;    // enriched WriteBatch path (F.4)
    bool stopping = false;
    // Client stubs are created ONCE (a stub allocated per concurrent call can
    // corrupt reply routing / message-id bookkeeping). Reused for every call.
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> forwardStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> forwardBatchStub;
    std::function<seastar::future<seastar::sstring>(Client&, seastar::sstring)> queryNodeStub;

    seastar::rpc::protocol<DpSerializer>::client* clientFor(NodeId to) {
        if (auto it = clients.find(to); it != clients.end())
            return it->second.get();
        auto pit = peers.find(to);
        if (pit == peers.end())
            return nullptr;
        auto c = std::make_unique<seastar::rpc::protocol<DpSerializer>::client>(proto, pit->second);
        auto* p = c.get();
        clients[to] = std::move(c);
        return p;
    }

    // Create every client stub ONCE (both command paths). A node registers only
    // one server-side sink, but the client stubs are path-independent -- a node
    // may need to reach peers on either path.
    void makeStubs() {
        forwardStub = proto.make_client<seastar::sstring(seastar::sstring)>(kForwardWrites);
        queryStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryRemote);
        forwardBatchStub = proto.make_client<seastar::sstring(seastar::sstring)>(kForwardWriteBatch);
        queryNodeStub = proto.make_client<seastar::sstring(seastar::sstring)>(kQueryNode);
    }

    // Pin the listening socket to THIS shard. seastar's default listen policy
    // (connection_distribution) scatters incoming connections across every shard,
    // but this rpc server object lives on one shard only -- a connection accepted
    // on any other shard has no server behind it and the peer's WAITED call hangs
    // forever (nondeterministically, since the scatter is by shard load). Fixing
    // the accept CPU to the server's shard keeps every connection on the shard
    // that can actually answer. (Raft's transport dodged this only because it is
    // no_wait -- it never blocks on a reply -- so a dropped connection was silently
    // retried instead of hanging.)
    void listenServer(seastar::socket_address local) {
        seastar::listen_options lo;
        lo.reuse_address = true;
        lo.set_fixed_cpu(seastar::this_shard_id());
        server = std::make_unique<seastar::rpc::protocol<DpSerializer>::server>(proto, seastar::listen(local, lo));
    }
};

DataPlaneRpc::DataPlaneRpc() : impl_(std::make_unique<Impl>()) {}
DataPlaneRpc::~DataPlaneRpc() = default;

void DataPlaneRpc::addPeer(NodeId id, seastar::socket_address addr) {
    impl_->peers[id] = addr;
}

seastar::future<> DataPlaneRpc::start(seastar::socket_address local, LocalStore& sink) {
    impl_->sink = &sink;
    // Forwarded writes: apply into the local sink and ack (waited), so the sender
    // only considers the write accepted once the owner has stored it. The ack is a
    // one-byte sstring ("k") -- the DpSerializer only carries sstring. Non-coroutine
    // handlers (return the future directly) to keep the rpc reply path simple.
    impl_->proto.register_handler(kForwardWrites, [this](seastar::sstring data) {
        auto pts = decodePoints(std::string(data.data(), data.size()));
        if (!pts)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed write batch"));
        return impl_->sink->applyWrites(std::move(*pts)).then([] { return seastar::sstring("k"); });
    });
    impl_->proto.register_handler(kQueryRemote, [this](seastar::sstring data) {
        auto spec = decodeQuerySpec(std::string(data.data(), data.size()));
        if (!spec)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed query spec"));
        return impl_->sink->queryLocal(std::move(*spec)).then([](QueryPartial part) {
            std::string enc = encodeQueryPartial(part);
            return seastar::sstring(enc.data(), enc.size());
        });
    });
    impl_->makeStubs();
    impl_->listenServer(local);
    return seastar::make_ready_future<>();
}

seastar::future<> DataPlaneRpc::start(seastar::socket_address local, NodeStore& sink) {
    impl_->nodeSink = &sink;
    // Enriched forwarded writes: decode the lossless WriteBatch and apply into the
    // node sink, acking only after a durable apply (waited). A malformed frame or
    // an apply failure surfaces as an exception the caller's awaited RPC observes,
    // so a forwarded write never looks accepted when it was not.
    impl_->proto.register_handler(kForwardWriteBatch, [this](seastar::sstring data) {
        auto batch = decodeWriteBatch(std::string(data.data(), data.size()));
        if (!batch)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed write batch"));
        return impl_->nodeSink->applyWrites(std::move(*batch)).then([] { return seastar::sstring("k"); });
    });
    impl_->proto.register_handler(kQueryNode, [this](seastar::sstring data) {
        auto req = decodeNodeQueryRequest(std::string(data.data(), data.size()));
        if (!req)
            return seastar::make_exception_future<seastar::sstring>(
                std::runtime_error("dataplane: malformed node query request"));
        return impl_->nodeSink->queryLocal(std::move(*req)).then([](NodeQueryPartial part) {
            std::string enc = encodeNodeQueryPartial(part);
            return seastar::sstring(enc.data(), enc.size());
        });
    });
    impl_->makeStubs();
    impl_->listenServer(local);
    return seastar::make_ready_future<>();
}

seastar::future<> DataPlaneRpc::forwardWrites(NodeId to, std::vector<DataPoint> points) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodePoints(points);
    co_await impl_->forwardStub(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited
}

seastar::future<QueryPartial> DataPlaneRpc::queryRemote(NodeId to, QuerySpec spec) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeQuerySpec(spec);
    seastar::sstring reply = co_await impl_->queryStub(*conn, seastar::sstring(bytes.data(), bytes.size()));
    auto part = decodeQueryPartial(std::string(reply.data(), reply.size()));
    if (!part)
        throw std::runtime_error("dataplane: malformed query partial");
    co_return std::move(*part);
}

seastar::future<> DataPlaneRpc::forwardWriteBatch(NodeId to, WriteBatch batch) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeWriteBatch(batch);
    co_await impl_->forwardBatchStub(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited
}

seastar::future<NodeQueryPartial> DataPlaneRpc::queryNode(NodeId to, NodeQueryRequest req) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeNodeQueryRequest(req);
    seastar::sstring reply = co_await impl_->queryNodeStub(*conn, seastar::sstring(bytes.data(), bytes.size()));
    auto part = decodeNodeQueryPartial(std::string(reply.data(), reply.size()));
    if (!part)
        throw std::runtime_error("dataplane: malformed node query partial");
    co_return std::move(*part);
}

seastar::future<> DataPlaneRpc::stop() {
    // Stop peer clients FIRST (aborts our outbound calls and closes our
    // connections into peers' servers), THEN stop our server. Doing the server
    // first would wait for peers' still-open inbound connections to close -- a
    // deadlock when nodes shut down together, each waiting on the others.
    impl_->stopping = true;
    for (auto& [id, c] : impl_->clients)
        co_await c->stop();
    impl_->clients.clear();
    if (impl_->server)
        co_await impl_->server->stop();
}

}  // namespace timestar::data
