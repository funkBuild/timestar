#include "cluster_data_plane.hpp"

#include "../../utils/logger.hpp"  // timestar::http_log

#include <seastar/core/coroutine.hh>
#include <seastar/net/dns.hh>
#include <set>
#include <seastar/net/inet_address.hh>
#include <stdexcept>
#include <string>

namespace timestar::cluster {

namespace {
// Split "host:port" -> {host, port}; missing port defaults to 8086 (same rule as
// the M1 gateway). The data-plane listener uses port + kDataPlanePortOffset.
struct HostPort {
    std::string host;
    uint16_t port = 8086;
};
HostPort parseHostPort(const std::string& s) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos)
        return {s, 8086};
    HostPort hp;
    hp.host = s.substr(0, colon);
    try {
        hp.port = static_cast<uint16_t>(std::stoul(s.substr(colon + 1)));
    } catch (...) {
    }
    return hp;
}

// Resolve "host" to an address (numeric dotted-quad parses directly; a hostname --
// e.g. a docker service name -- goes through DNS).
seastar::future<seastar::net::inet_address> resolveHost(const std::string& host) {
    std::optional<seastar::net::inet_address> numeric;
    try {
        numeric = seastar::net::inet_address(host);  // numeric IP fast path
    } catch (...) {
        numeric.reset();  // a hostname -- fall through to DNS (co_await not allowed in a catch)
    }
    if (numeric)
        co_return *numeric;
    co_return co_await seastar::net::dns::resolve_name(seastar::sstring(host));
}
}  // namespace

seastar::future<> ClusterDataPlane::start(const ClusterConfig& cfg, seastar::sharded<Engine>& engines) {
    rt_ = ClusterRuntime::fromConfig(cfg);  // throws (fail-closed) on misconfig
    dir_ = std::make_unique<data::VShardDirectory>(rt_->directory());
    local_ = std::make_unique<EngineLocalStore>(engines);
    rpc_ = std::make_unique<data::DataPlaneRpc>();

    // Bind the RPC server to this node's own data-plane address.
    const HostPort self = parseHostPort(rt_->peerAddresses.at(rt_->selfId));
    seastar::net::inet_address selfAddr = co_await resolveHost(self.host);
    co_await rpc_->start(seastar::socket_address(selfAddr, static_cast<uint16_t>(self.port + kDataPlanePortOffset)),
                         *local_);

    // Register every OTHER node as a peer at its data-plane address. Resolution is
    // BEST-EFFORT: a peer that is not yet up (rolling start) must not fail THIS
    // node's startup. A skipped peer surfaces later as a routing error to that
    // owner, not a boot failure. (The connection itself is lazy in DataPlaneRpc.)
    for (const auto& [id, addr] : rt_->peerAddresses) {
        if (id == rt_->selfId)
            continue;
        const HostPort hp = parseHostPort(addr);
        std::optional<seastar::net::inet_address> a;
        try {
            a = co_await resolveHost(hp.host);
        } catch (const std::exception& e) {
            timestar::http_log.warn(
                "cluster data plane: peer {} ({}) unresolved at startup: {} (will error on route)", id, addr,
                e.what());
            continue;
        }
        rpc_->addPeer(id, seastar::socket_address(*a, static_cast<uint16_t>(hp.port + kDataPlanePortOffset)));
    }

    finalizer_ = std::make_unique<http::HttpQueryHandler>(&engines);
    router_ = std::make_unique<data::NodeWriteRouter>(*dir_, *local_, *rpc_);
    coord_ = std::make_unique<data::NodeQueryCoordinator>(*dir_, *local_, *rpc_, *finalizer_);
    co_return;
}

seastar::future<> ClusterDataPlane::stop() {
    if (rpc_)
        co_await rpc_->stop();
    co_return;
}

seastar::future<> ClusterDataPlane::write(data::WriteBatch batch) {
    if (!router_)
        throw std::runtime_error("ClusterDataPlane::write before start");
    return router_->write(std::move(batch));
}

seastar::future<QueryResponse> ClusterDataPlane::query(QueryRequest request) {
    if (!coord_)
        throw std::runtime_error("ClusterDataPlane::query before start");
    return coord_->query(std::move(request));
}

seastar::future<data::MetadataResult> ClusterDataPlane::metadata(data::MetadataRequest request) {
    if (!dir_)
        throw std::runtime_error("ClusterDataPlane::metadata before start");
    // Scatter to every owner node (self served in-process), await all, then union
    // items / sum cardinality. A node failure fails the whole request (a partial
    // metadata answer would be silently incomplete, same contract as queries).
    const std::set<NodeId> targets = dir_->ownerNodes();
    const NodeId self = rt_->selfId;
    std::vector<seastar::future<data::MetadataResult>> pending;
    pending.reserve(targets.size());
    for (NodeId t : targets) {
        if (t == self)
            pending.push_back(local_->queryMetadata(request));
        else
            pending.push_back(rpc_->queryMetadata(t, request));
    }
    std::set<std::string> items;
    double cardinality = 0.0;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            data::MetadataResult r = co_await std::move(f);
            items.insert(r.items.begin(), r.items.end());
            cardinality += r.cardinality;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    data::MetadataResult out;
    out.items.assign(items.begin(), items.end());
    out.cardinality = cardinality;
    co_return out;
}

}  // namespace timestar::cluster
