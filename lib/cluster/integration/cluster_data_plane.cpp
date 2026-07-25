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

    // RF=3 replicated write path (integration plan M3). Composition proven by
    // replicated_data_plane_test; here we wire the REAL transports. Reads still fan
    // out via coord_ (v1 leader reads land on the primary, == the leader under the
    // placement model). Group instantiation is the real prod cost: RF*4096/N groups
    // per node (see the group-granularity note in the plan).
    if (cfg.replication_factor > 1) {
        raftTransport_ = std::make_unique<raft::RaftRpcTransport>();
        std::filesystem::path journalRoot = timestar::dataRootPath();
        journalRoot /= "cluster_raft";
        rdp_ = std::make_unique<ReplicatedDataPlane>(*local_, *raftTransport_, *rpc_, *dir_, rt_->selfId, journalRoot);
        rpc_->setProposeSink(rdp_->proposeSink());

        // Serve Raft on this node's own Raft address; each envelope routes to its
        // group via the host's registry.
        seastar::net::inet_address rAddr = co_await resolveHost(self.host);
        co_await raftTransport_->start(
            seastar::socket_address(rAddr, static_cast<uint16_t>(self.port + kRaftPortOffset)),
            [this](raft::Envelope env) { return rdp_->host().registry().deliver(std::move(env)); });
        for (const auto& [id, addr] : rt_->peerAddresses) {
            if (id == rt_->selfId)
                continue;
            const HostPort hp = parseHostPort(addr);
            try {
                seastar::net::inet_address a = co_await resolveHost(hp.host);
                raftTransport_->addPeer(id, seastar::socket_address(a, static_cast<uint16_t>(hp.port + kRaftPortOffset)));
            } catch (const std::exception& e) {
                timestar::http_log.warn("cluster raft: peer {} ({}) unresolved at startup: {}", id, addr, e.what());
            }
        }

        // Instantiate this node's replicated VShard groups from placement.
        //
        // MANY-GROUP TIMING. A node hosts RF*4096/N per-VShard Raft groups (every
        // VShard when RF==N) on ONE shard. The brick defaults are tuned for a handful
        // of groups: heartbeat EVERY 20ms tick and an election timeout of only 10-20
        // ticks (200-400ms). At this group count that is two separate floods:
        //   - steady state: ~1365 leaders * 2 followers / 20ms ~= 136k msgs/s, which
        //     saturates the reactor ("Too long queue accumulated for main") until the
        //     node stops answering HTTP at all;
        //   - startup: every group's first election times out inside the same 200ms
        //     window, so thousands of campaigns collide and split votes.
        // Standard Raft practice (heartbeat ~200ms, election ~1-2s, widely randomized)
        // fixes both: 10x less steady traffic, and the initial campaigns spread over a
        // ~1s window instead of 200ms. Followers still see 5-10 heartbeats per election
        // window, so leadership stays stable.
        raft::RaftOptions ropts;
        ropts.heartbeatTimeout = 10;    // 200ms at the 20ms tick
        ropts.electionTimeoutMin = 50;  // 1s
        ropts.electionTimeoutMax = 100;  // 2s (randomized per group -> spreads campaigns)
        for (const auto& [vshard, voters] : rt_->localReplicaGroups())
            co_await rdp_->addVShard(vshard, voters, ropts);
        rdp_->startTicking();
        replicated_ = true;
    }
    co_return;
}

seastar::future<> ClusterDataPlane::stop() {
    // rdp_ (Raft groups + tick timer) BEFORE the transports it uses.
    if (rdp_)
        co_await rdp_->stop();
    if (raftTransport_)
        co_await raftTransport_->stop();
    if (rpc_)
        co_await rpc_->stop();
    co_return;
}

seastar::future<> ClusterDataPlane::write(data::WriteBatch batch) {
    // RF=3: replicate to each VShard's Raft leader (durable quorum commit). RF=1/M2:
    // apply directly on the owner.
    if (replicated_)
        return rdp_->write(std::move(batch));
    if (!router_)
        throw std::runtime_error("ClusterDataPlane::write before start");
    return router_->write(std::move(batch));
}

seastar::future<QueryResponse> ClusterDataPlane::query(QueryRequest request) {
    if (replicated_)
        co_return co_await queryReplicated(std::move(request));
    if (!coord_)
        throw std::runtime_error("ClusterDataPlane::query before start");
    co_return co_await coord_->query(std::move(request));
}

seastar::future<QueryResponse> ClusterDataPlane::queryReplicated(QueryRequest request) {
    // RF=3 leader read (M3 read-failover): route each VShard's read to its CURRENT
    // Raft leader (not the static primary), so reads follow failover and never
    // double-count a series replicated on every node. Group all VShards by their
    // current leader, query each leader for ONLY those VShards (req.vshards restricts
    // its discovery), and merge the partials into the single answer.
    std::map<data::NodeId, std::vector<uint16_t>> byLeader;
    size_t leaderless = 0;
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
        const data::NodeId leader = rdp_->host().leaderOf(vs);
        if (leader == timestar::raft::kNoNode)
            ++leaderless;  // no elected leader (no quorum) -> its data is unreadable
        else
            byLeader[leader].push_back(vs);
    }
    if (leaderless > 0) {
        // A VShard with no leader may hold matching data we cannot read -> fail closed
        // (QUERY_INCOMPLETE), never a silent partial.
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage = std::to_string(leaderless) + " VShard(s) have no elected leader";
        co_return r;
    }

    const data::NodeId self = rt_->selfId;
    std::vector<seastar::future<data::NodeQueryPartial>> pending;
    pending.reserve(byLeader.size());
    for (auto& [leader, vshards] : byLeader) {
        data::NodeQueryRequest nq;
        nq.request = request;
        nq.vshards = std::move(vshards);
        if (leader == self)
            pending.push_back(local_->queryLocal(std::move(nq)));
        else
            pending.push_back(rpc_->queryNode(leader, std::move(nq)));
    }

    std::vector<PartialAggregationResult> allPartials;
    std::vector<timestar::http::SeriesResult> allNonNumeric;
    std::vector<std::string> incompleteReasons;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            data::NodeQueryPartial part = co_await std::move(f);
            if (!part.incompleteReasons.empty()) {
                for (auto& r : part.incompleteReasons)
                    incompleteReasons.push_back(std::move(r));
                continue;
            }
            allPartials.insert(allPartials.end(), std::make_move_iterator(part.partials.begin()),
                               std::make_move_iterator(part.partials.end()));
            allNonNumeric.insert(allNonNumeric.end(), std::make_move_iterator(part.nonNumeric.begin()),
                                 std::make_move_iterator(part.nonNumeric.end()));
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    if (!incompleteReasons.empty()) {
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage = "a leader could not answer";
        co_return r;
    }
    co_return co_await finalizer_->finalizeClusterPartials(std::move(request), std::move(allPartials),
                                                          std::move(allNonNumeric));
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
