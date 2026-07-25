#include "cluster_data_plane.hpp"

#include "../../utils/logger.hpp"  // timestar::http_log

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
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
    enginesPtr_ = &engines;
    rf_ = cfg.replication_factor < 1 ? 1 : cfg.replication_factor;
    dir_ = std::make_unique<data::VShardDirectory>(rt_->directory());
    local_ = std::make_unique<EngineLocalStore>(engines);
    rpc_ = std::make_unique<data::DataPlaneRpc>();

    const bool replicated = cfg.replication_factor > 1;
    // Bind the RPC server to this node's own data-plane address.
    const HostPort self = parseHostPort(rt_->peerAddresses.at(rt_->selfId));
    seastar::net::inet_address selfAddr = co_await resolveHost(self.host);
    const seastar::socket_address dataPlaneAddr(selfAddr,
                                                static_cast<uint16_t>(self.port + kDataPlanePortOffset));
    if (replicated)
        // Replicated mode serves the data plane from EVERY shard (see below); this
        // instance stays client-only, for the shard-0 query/metadata fan-out.
        co_await rpc_->startClientOnly();
    else
        co_await rpc_->start(dataPlaneAddr, *local_);

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
    if (replicated) {
        std::filesystem::path journalRoot = timestar::dataRootPath();
        journalRoot /= "cluster_raft";
        // Start a Raft plane on EVERY shard; each will own only the VShards that
        // assignCore maps to it, so the group work spreads across all cores.
        co_await shards_.start();
        shardsStarted_ = true;
        {
            const std::string jroot = journalRoot.string();
            const data::NodeId selfId = rt_->selfId;
            const data::VShardDirectory* dirp = dir_.get();
            auto* peers = &shards_;
            co_await shards_.invoke_on_all([enginesPtr = enginesPtr_, peers, dirp, selfId, jroot](ShardRaftPlane& p) {
                return p.init(enginesPtr, peers, dirp, selfId, jroot, std::chrono::milliseconds(20));
            });
        }

        // Serve the DATA plane on this node's data-plane address FROM EVERY SHARD
        // (SO_REUSEPORT), and give every shard its own peer clients. Previously one
        // DataPlaneRpc on shard 0 carried every forwarded write in BOTH directions, so
        // each shard's remote leader-forwards hopped to shard 0 and back and every
        // peer's inbound proposal landed there: shard 0's Raft profile stayed ~10-20x
        // the other shards' long after the Raft transport itself was sharded (b98c1d1).
        // An inbound proposeWrite is split across the shards owning its VShards by
        // ShardRaftPlane::proposeBatch, so which shard the kernel handed the connection
        // to no longer decides where the work runs.
        co_await shards_.invoke_on_all(
            [dataPlaneAddr](ShardRaftPlane& p) { return p.startDataPlane(dataPlaneAddr); });

        // Serve Raft on this node's own Raft address FROM EVERY SHARD (SO_REUSEPORT).
        // Each shard decodes and routes to the shard owning the addressed group.
        seastar::net::inet_address rAddr = co_await resolveHost(self.host);
        const seastar::socket_address raftAddr(rAddr, static_cast<uint16_t>(self.port + kRaftPortOffset));
        co_await shards_.invoke_on_all([raftAddr](ShardRaftPlane& p) { return p.startTransport(raftAddr); });

        for (const auto& [id, addr] : rt_->peerAddresses) {
            if (id == rt_->selfId)
                continue;
            const HostPort hp = parseHostPort(addr);
            try {
                seastar::net::inet_address a = co_await resolveHost(hp.host);
                const seastar::socket_address peerAddr(a, static_cast<uint16_t>(hp.port + kRaftPortOffset));
                const seastar::socket_address peerDataAddr(a,
                                                           static_cast<uint16_t>(hp.port + kDataPlanePortOffset));
                co_await shards_.invoke_on_all([id, peerAddr, peerDataAddr](ShardRaftPlane& p) {
                    p.addRaftPeer(id, peerAddr);
                    p.addDataPeer(id, peerDataAddr);
                });
            } catch (const std::exception& e) {
                timestar::http_log.warn("cluster raft: peer {} ({}) unresolved at startup: {}", id, addr, e.what());
            }
        }

        // Instantiate each VShard's group ON ITS OWNING SHARD (see the many-group
        // timing note below).
        raft::RaftOptions ropts;
        ropts.heartbeatTimeout = 25;     // 500ms at the 20ms tick
        ropts.electionTimeoutMin = 125;  // 2.5s
        ropts.electionTimeoutMax = 250;  // 5s (randomized -> spreads campaigns)
        {
            std::map<unsigned, std::vector<std::pair<uint16_t, std::vector<data::NodeId>>>> byShard;
            for (const auto& [vshard, voters] : rt_->localReplicaGroups())
                byShard[shardForVShard(vshard)].push_back({vshard, voters});
            for (auto& [shard, groups] : byShard) {
                co_await shards_.invoke_on(shard, [g = std::move(groups), ropts](ShardRaftPlane& p) -> seastar::future<> {
                    for (const auto& [vs, voters] : g)
                        co_await p.addVShard(vs, voters, ropts);
                    co_return;
                });
            }
        }
        co_await shards_.invoke_on_all([](ShardRaftPlane& p) {
            p.startTicking();
            return seastar::make_ready_future<>();
        });
        replicated_ = true;
        startLeadershipBalancer();
    }
    co_return;
}

seastar::future<> ClusterDataPlane::stop() {
    // Stop the balancing loop FIRST and drain any in-flight pass: it touches the Raft
    // groups, so it must be quiescent before rdp_ tears them down.
    balanceTimer_.cancel();
    if (!balanceGate_.is_closed())
        co_await balanceGate_.close();
    // The per-shard Raft planes (groups + tick timers) BEFORE the transports they use.
    if (shardsStarted_) {
        co_await shards_.stop();
        shardsStarted_ = false;
    }
    if (rpc_)
        co_await rpc_->stop();
    co_return;
}

seastar::future<> ClusterDataPlane::write(data::WriteBatch batch) {
    // RF=3: replicate to each VShard's Raft leader (durable quorum commit). RF=1/M2:
    // apply directly on the owner.
    if (replicated_)
        return writeReplicated(std::move(batch));
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
    // Groups live across cores now, so collect each shard's leadership view.
    // A VShard is momentarily leaderless during a leadership TRANSFER (the old leader
    // has stepped down, the new one has not yet won). Failing the query outright there
    // turns routine background rebalancing into user-visible read errors -- measured at
    // ~4.6% of queries while the balancer was actively moving leadership. Re-gather a
    // few times first: a transfer completes in milliseconds, so this converts a
    // transient window into a small latency bump. A genuinely leaderless VShard (lost
    // quorum) still fails closed after the retries.
    static constexpr int kLeaderRetries = 4;
    static constexpr auto kLeaderRetryDelay = std::chrono::milliseconds(25);
    for (int attempt = 0; attempt <= kLeaderRetries; ++attempt) {
        byLeader.clear();
        leaderless = 0;
        const auto leaders = co_await gatherLeaders();
        for (const auto& [vs, leader] : leaders) {
            if (leader == timestar::raft::kNoNode)
                ++leaderless;  // no elected leader right now
            else
                byLeader[leader].push_back(vs);
        }
        if (leaderless == 0 || attempt == kLeaderRetries)
            break;
        co_await seastar::sleep(kLeaderRetryDelay);
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
    std::vector<data::NodeId> pendingLeaders;  // parallel to `pending`
    pending.reserve(byLeader.size());
    pendingLeaders.reserve(byLeader.size());
    for (auto& [leader, vshards] : byLeader) {
        data::NodeQueryRequest nq;
        nq.request = request;
        nq.vshards = std::move(vshards);
        pendingLeaders.push_back(leader);
        if (leader == self)
            pending.push_back(local_->queryLocal(std::move(nq)));
        else
            pending.push_back(rpc_->queryNode(leader, std::move(nq)));
    }

    std::vector<PartialAggregationResult> allPartials;
    std::vector<timestar::http::SeriesResult> allNonNumeric;
    std::vector<std::string> incompleteReasons;
    std::exception_ptr firstErr;
    std::set<data::NodeId> unreachableLeaders;
    for (size_t i = 0; i < pending.size(); ++i) {
        auto& f = pending[i];
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
            // A REMOTE leader we could not reach is an availability problem, not an
            // internal error: the node is down (or partitioned) but its VShards have
            // live replicas that will elect a new leader. Record it so we can wake
            // those groups and answer honestly. A failure of our own LOCAL read is a
            // genuine internal error and still propagates.
            if (pendingLeaders[i] != self)
                unreachableLeaders.insert(pendingLeaders[i]);
            else if (!firstErr)
                firstErr = std::current_exception();
        }
    }

    if (!unreachableLeaders.empty()) {
        // Those VShards are hibernating behind a dead leader, so their election
        // timeout is stretched ~10x (25-50s of cluster-wide read failure, measured).
        // Wake them so they campaign at the normal timeout instead, then report
        // QUERY_INCOMPLETE -- distinguishable from an empty result and from an
        // internal error, and correct to retry. Waking is idempotent and bounded.
        for (data::NodeId dead : unreachableLeaders)
            co_await shards_.invoke_on_all([dead](ShardRaftPlane& p) { (void)p.wakeFollowersOf(dead); });
        std::string nodes;
        for (data::NodeId d : unreachableLeaders)
            nodes += (nodes.empty() ? "" : ",") + std::to_string(d);
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage = "leader node(s) " + nodes +
                         " unreachable; their VShards were woken to re-elect -- retry shortly";
        co_return r;
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

seastar::future<ClusterDataPlane::Status> ClusterDataPlane::status() const {
    Status st;
    if (!rt_)
        co_return st;
    st.self = rt_->selfId;
    st.peers = rt_->peerAddresses;
    st.replicated = replicated_;
    st.replicationFactor = rf_;
    if (!replicated_ || !shardsStarted_)
        co_return st;
    std::vector<data::NodeId> peers;
    for (const auto& [id, addr] : rt_->peerAddresses)
        peers.push_back(id);
    const data::NodeId self = rt_->selfId;
    auto& shards = const_cast<seastar::sharded<ShardRaftPlane>&>(shards_);
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        auto c = co_await shards.invoke_on(sh, [self, peers](ShardRaftPlane& p) { return p.counts(self, peers); });
        st.vshardsHostedHere += c.hosted;
        st.vshardsLedHere += c.led;
        st.vshardsLeaderless += c.leaderless;
        for (const auto& [peer, n] : c.peerCaughtUp)
            st.peerCaughtUp[peer] += n;
    }
    co_return st;
}

void ClusterDataPlane::startLeadershipBalancer() {
    // A bounded pass every few seconds, split across shards. Without it a fresh
    // cluster leaves ALL leadership on the first node to start (it wins every
    // election), putting all write coordination and leader-reads on one node.
    static constexpr auto kInterval = std::chrono::seconds(5);
    static constexpr size_t kBudget = 256;
    balanceTimer_.set_callback([this] {
        if (balanceRunning_ || balanceGate_.is_closed())
            return;  // never overlap passes
        balanceRunning_ = true;
        // NOT a coroutine lambda: with_gate invokes a TEMPORARY closure, and a
        // coroutine lambda's frame keeps referencing it after destruction (reads freed
        // memory, and the flag never clears so the loop silently dies).
        (void)seastar::with_gate(balanceGate_, [this] {
            return rebalanceLeadership(kBudget).then_wrapped([this](seastar::future<size_t> f) {
                f.ignore_ready_future();  // best effort; next tick retries
                balanceRunning_ = false;
            });
        });
    });
    balanceTimer_.arm_periodic(kInterval);
}

seastar::future<size_t> ClusterDataPlane::rebalanceLeadership(size_t maxTransfers) {
    if (!replicated_ || !shardsStarted_ || maxTransfers == 0)
        co_return 0;
    std::vector<data::NodeId> peers;
    for (const auto& [id, addr] : rt_->peerAddresses)
        peers.push_back(id);
    const data::NodeId self = rt_->selfId;
    // Each shard balances its own VShards. assignCore spreads VShards evenly over
    // shards and every shard sees the same node set, so per-shard balance == cluster
    // balance. Split the budget so a pass stays bounded overall.
    const size_t per = std::max<size_t>(1, maxTransfers / seastar::smp::count);
    size_t total = 0;
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        total += co_await shards_.invoke_on(
            sh, [per, self, peers](ShardRaftPlane& p) { return p.rebalance(per, self, peers); });
    }
    co_return total;
}

seastar::future<> ClusterDataPlane::writeReplicated(data::WriteBatch batch) {
    // Group the series by the shard that owns their VShard's Raft group, then hand
    // each slice to that shard's plane (which resolves the leader for its own
    // VShards locally and forwards to a remote leader when needed).
    return writeSlicesToOwningShards(shards_, std::move(batch));
}

seastar::future<bool> ClusterDataPlane::proposeBatch(data::WriteBatch batch) {
    // A peer forwarded this batch because we lead those VShards. Replicate each
    // slice through the Raft group on its owning shard.
    return proposeSlicesToOwningShards(shards_, std::move(batch));
}

seastar::future<std::map<uint16_t, data::NodeId>> ClusterDataPlane::gatherLeaders() const {
    std::map<uint16_t, NodeId> leaders;
    auto& shards = const_cast<seastar::sharded<ShardRaftPlane>&>(shards_);
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        auto part = co_await shards.invoke_on(sh, [](ShardRaftPlane& p) {
            std::map<uint16_t, data::NodeId> out;
            if (!p.ready())
                return out;
            auto& host = p.plane().host();
            for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
                if (shardForVShard(vs) != seastar::this_shard_id())
                    continue;
                out[vs] = host.leaderOf(vs);
            }
            return out;
        });
        for (const auto& [vs, l] : part)
            leaders[vs] = l;
    }
    co_return leaders;
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
