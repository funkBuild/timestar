#include "dataplane_rpc.hpp"

#include "dataplane_codec.hpp"

#include <cstdint>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>
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

constexpr uint64_t kForwardWrites = 1;  // sstring -> void  (waited: applied on peer)
constexpr uint64_t kQueryRemote = 2;    // sstring -> sstring (waited: returns partial)

}  // namespace

struct DataPlaneRpc::Impl {
    seastar::rpc::protocol<DpSerializer> proto{DpSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<DpSerializer>::server> server;
    std::map<NodeId, seastar::socket_address> peers;
    std::map<NodeId, std::unique_ptr<seastar::rpc::protocol<DpSerializer>::client>> clients;
    LocalStore* sink = nullptr;
    bool stopping = false;

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
    impl_->server =
        std::make_unique<seastar::rpc::protocol<DpSerializer>::server>(impl_->proto, local);
    return seastar::make_ready_future<>();
}

seastar::future<> DataPlaneRpc::forwardWrites(NodeId to, std::vector<DataPoint> points) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodePoints(points);
    auto call = impl_->proto.make_client<seastar::sstring(seastar::sstring)>(kForwardWrites);
    co_await call(*conn, seastar::sstring(bytes.data(), bytes.size()));  // waited: applied on peer
}

seastar::future<QueryPartial> DataPlaneRpc::queryRemote(NodeId to, QuerySpec spec) {
    if (impl_->stopping)
        throw std::runtime_error("dataplane: shutting down");
    auto* conn = impl_->clientFor(to);
    if (!conn)
        throw std::runtime_error("dataplane: unknown peer");
    std::string bytes = encodeQuerySpec(spec);
    auto call = impl_->proto.make_client<seastar::sstring(seastar::sstring)>(kQueryRemote);
    seastar::sstring reply = co_await call(*conn, seastar::sstring(bytes.data(), bytes.size()));
    auto part = decodeQueryPartial(std::string(reply.data(), reply.size()));
    if (!part)
        throw std::runtime_error("dataplane: malformed query partial");
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
