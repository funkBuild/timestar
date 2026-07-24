#include "cluster_gateway.hpp"

#include "../../config/timestar_config.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/http/reply.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

namespace timestar::cluster {

namespace {
seastar::logger glog("cluster_gateway");

// Split "host:port" -> {host, port}. Missing port defaults to 8086.
struct HostPort {
    std::string host;
    uint16_t port;
};
HostPort parseHostPort(const std::string& s) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos)
        return {s, 8086};
    std::string host = s.substr(0, colon);
    uint16_t port = 8086;
    try {
        port = static_cast<uint16_t>(std::stoul(s.substr(colon + 1)));
    } catch (...) {
    }
    return {host, port};
}
}  // namespace

ClusterGateway::ClusterGateway() {
    const auto& cc = timestar::config().cluster;
    if (!cc.enabled || cc.peers.empty() || cc.node_id == 0)
        return;
    enabled_ = true;
    nodeId_ = cc.node_id;
    // Every peer except this node (index node_id-1 is self).
    for (size_t i = 0; i < cc.peers.size(); ++i) {
        if (static_cast<uint16_t>(i + 1) == cc.node_id)
            continue;
        auto hp = parseHostPort(cc.peers[i]);
        peers_.push_back(Peer{hp.host, hp.port});
    }
    // Forwarded writes carry this node's own bearer token when auth is on (all
    // cluster nodes must share the token).
    const auto& sc = timestar::config().server;
    if (sc.auth_enabled && !sc.auth_token.empty())
        authHeader_ = "Bearer " + sc.auth_token;
}

ClusterGateway& shardGateway() {
    static thread_local ClusterGateway g;
    return g;
}

bool ClusterGateway::isForwarded(const seastar::http::request& req) {
    auto it = req._headers.find(kForwardedHeader);
    return it != req._headers.end() && !it->second.empty();
}

seastar::future<> ClusterGateway::ensureInit() {
    if (inited_)
        co_return;
    for (const auto& p : peers_) {
        seastar::net::inet_address addr;
        try {
            addr = co_await seastar::net::dns::resolve_name(seastar::sstring(p.host),
                                                            seastar::net::inet_address::family::INET);
        } catch (const std::exception& e) {
            glog.warn("cluster peer {}:{} DNS resolve failed: {} (retried per request)", p.host, p.port, e.what());
            clients_.push_back(nullptr);  // keep index alignment; retried lazily
            continue;
        }
        seastar::socket_address sa(addr, p.port);
        auto factory = std::make_unique<seastar::http::experimental::basic_connection_factory>(sa);
        // A small connection pool per peer; replication is not the hot path.
        clients_.push_back(std::make_unique<seastar::http::experimental::client>(std::move(factory), 4));
    }
    inited_ = true;
    co_return;
}

seastar::future<> ClusterGateway::replicate(std::string path, std::string mimeType, std::string body) {
    if (!enabled_ || peers_.empty())
        co_return;
    co_await ensureInit();

    const bool proto = mimeType.find("protobuf") != std::string::npos;
    auto bodyShared = seastar::make_lw_shared<std::string>(std::move(body));

    std::vector<seastar::future<>> sends;
    sends.reserve(peers_.size());
    for (size_t i = 0; i < peers_.size(); ++i) {
        if (i >= clients_.size() || !clients_[i])
            continue;  // peer unresolved: skip this round (best effort)
        auto req = seastar::http::request::make("POST", seastar::sstring(peers_[i].host), seastar::sstring(path));
        req.write_body(proto ? "bin" : "json", seastar::sstring(*bodyShared));
        req.set_mime_type(seastar::sstring(mimeType));
        req._headers[kForwardedHeader] = "1";
        if (authHeader_)
            req._headers["Authorization"] = seastar::sstring(*authHeader_);
        const std::string host = peers_[i].host;
        const uint16_t port = peers_[i].port;
        // Best-effort: a peer outage must never fail the client's write (M1).
        sends.push_back(clients_[i]
                            ->make_request(std::move(req),
                                           [](const seastar::http::reply&,
                                              seastar::input_stream<char>&& body_in) -> seastar::future<> {
                                               auto b = std::move(body_in);
                                               auto buf = co_await b.read();
                                               while (!buf.empty())
                                                   buf = co_await b.read();  // drain
                                               co_return;
                                           },
                                           std::nullopt)
                            .handle_exception([host, port](std::exception_ptr e) {
                                try {
                                    std::rethrow_exception(e);
                                } catch (const std::exception& ex) {
                                    glog.warn("cluster replicate to {}:{} failed: {}", host, port, ex.what());
                                }
                            }));
    }
    // Each send already handles its own exceptions, so awaiting them (they are
    // in flight concurrently) collects all without throwing.
    for (auto& f : sends)
        co_await std::move(f);
    co_return;
}

}  // namespace timestar::cluster
