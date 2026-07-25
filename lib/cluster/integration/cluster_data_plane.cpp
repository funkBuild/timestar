#include "cluster_data_plane.hpp"

#include "../../utils/logger.hpp"  // timestar::http_log
#include "write_admission.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <set>
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
    } catch (...) {}
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
    // Force the in-flight write budget to resolve (and LOG itself) during startup rather
    // than on the first write, so a mis-set TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES is
    // visible in the boot log instead of being inferred from a wall of 503s.
    (void)WriteAdmission::limitBytes();
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
    const seastar::socket_address dataPlaneAddr(selfAddr, static_cast<uint16_t>(self.port + kDataPlanePortOffset));
    // Peer-facing settings must be applied BEFORE start() on every transport that talks
    // to a peer. This instance is the shard-0 query/metadata client; the per-shard
    // instances (started below) are the listeners and the write path's clients, and they
    // get the identical settings.
    if (tls_)
        rpc_->setTlsCredentials(tls_->certPem, tls_->keyPem, tls_->caPem, tls_->expectedPeerName);
    rpc_->setLocalVersion(localVersion_);
    if (replicated)
        // Replicated mode serves the data plane from EVERY shard (see below); this
        // instance stays client-only, for the shard-0 query/metadata fan-out.
        co_await rpc_->startClientOnly();
    else
        co_await rpc_->start(dataPlaneAddr, *local_);

    // Peers are registered AFTER the per-shard transports exist (below), from ONE
    // resolution each -- see registerPeer / the registration block in the replicated
    // section. Registering here as well is what created the asymmetry this replaces.

    finalizer_ = std::make_unique<http::HttpQueryHandler>(&engines);
    router_ = std::make_unique<data::NodeWriteRouter>(*dir_, *local_, *rpc_);
    coord_ = std::make_unique<data::NodeQueryCoordinator>(*dir_, *local_, *rpc_, *finalizer_);

    if (!replicated)
        co_await registerAllPeers(false);

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
        // (connection_distribution, not SO_REUSEPORT -- this seastar disables reuseport,
        // so shard 0 owns the one socket and hands each accepted fd to the shard the
        // listen options pick), and give every shard its own peer clients. Previously one
        // DataPlaneRpc on shard 0 carried every forwarded write in BOTH directions, so
        // each shard's remote leader-forwards hopped to shard 0 and back and every
        // peer's inbound proposal landed there: shard 0's Raft profile stayed ~10-20x
        // the other shards' long after the Raft transport itself was sharded (b98c1d1).
        // An inbound proposeWrite is split across the shards owning its VShards by
        // ShardRaftPlane::proposeBatch, so which shard the kernel handed the connection
        // to no longer decides where the work runs.
        co_await shards_.invoke_on_all([dataPlaneAddr, tls = tls_, ver = localVersion_](ShardRaftPlane& p) {
            // `tls` is read from this shard's copy and the PEM strings are copied into
            // each shard's own credentials -- setTlsCredentials takes them by value, so
            // nothing cross-shard is retained.
            return p.startDataPlane(dataPlaneAddr, tls, ver);
        });

        // Serve Raft on this node's own Raft address FROM EVERY SHARD (again
        // connection_distribution, see above). Whichever shard accepts a connection
        // peeks each envelope's group id and routes it to the shard owning that group,
        // which then decodes it.
        seastar::net::inet_address rAddr = co_await resolveHost(self.host);
        const seastar::socket_address raftAddr(rAddr, static_cast<uint16_t>(self.port + kRaftPortOffset));
        co_await shards_.invoke_on_all([raftAddr](ShardRaftPlane& p) { return p.startTransport(raftAddr); });

        // Register peers on BOTH planes, from ONE resolution each, now that every
        // transport that needs them exists (write-scaleout 4b-iii).
        co_await registerAllPeers(true);

        // Instantiate each VShard's group ON ITS OWNING SHARD (see the many-group
        // timing note below).
        raft::RaftOptions ropts;
        ropts.heartbeatTimeout = 25;     // 500ms at the 20ms tick
        ropts.electionTimeoutMin = 125;  // 2.5s
        ropts.electionTimeoutMax = 250;  // 5s (randomized -> spreads campaigns)
        // ropts.checkQuorum STAYS OFF. DO NOT ENABLE IT HERE -- it looks free and it is
        // not: with CheckQuorum on, LEADERSHIP TRANSFER BREAKS.
        //
        // TimeoutNow lets the TRANSFEREE skip its own lease and campaign immediately
        // (raft_node.cpp, the TimeoutNow arm), but the vote it then sends is an ordinary
        // RequestVote -- our RequestVote carries no transfer/force marker (raft_messages.hpp).
        // Every OTHER voter is still hearing the old leader's heartbeats, so
        // `opts_.checkQuorum && leaderId_ != kNoNode && electionElapsed_ < electionTimeout_`
        // holds and the disruption guard SILENTLY DROPS the transferee's vote without even
        // bumping its term. Measured: a transfer that completes in 0 tick rounds with
        // CheckQuorum off needs a full election timeout via term escalation with it on --
        // 2.5-5 s at the production 20 ms tick, each with a leaderless window in it.
        //
        // That is not a corner case here: the leadership balancer fires every 5 s across
        // 4096 groups, queryReplicated fails reads closed after ~125 ms of leaderlessness,
        // and the operator rebalance endpoint storms transfers deliberately.
        //
        // Enabling it requires the etcd-style fix FIRST: a campaignTransfer flag on
        // RequestVote that the inLease check honours. That is a Raft WIRE FORMAT change
        // with a mixed-version hazard, so it belongs with the consensus work in Phase 5,
        // not here. See docs/write-scaleout-plan.md Phase 5.
        //
        // Nothing depends on it for safety: the per-write propose deadline
        // (RaftGroup::proposeAndAwaitApplied) already delivers the fail-closed property it
        // was added as a belt for -- every write is bounded, and expired-waiter
        // accumulation is bounded by (write deadline x retry budget) either way.
        {
            std::map<unsigned, std::vector<std::pair<uint16_t, std::vector<data::NodeId>>>> byShard;
            for (const auto& [vshard, voters] : rt_->localReplicaGroups())
                byShard[shardForVShard(vshard)].push_back({vshard, voters});
            for (auto& [shard, groups] : byShard) {
                co_await shards_.invoke_on(shard,
                                           [g = std::move(groups), ropts](ShardRaftPlane& p) -> seastar::future<> {
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

// Register ONE peer on EVERY plane it belongs to, from a SINGLE DNS resolution
// (write-scaleout 4b-iii).
//
// This used to be two independent loops, each with its own try/catch around its own
// `resolveHost` call: one registered the data-plane address on the shard-0 client, the
// other registered the Raft address AND the per-shard data address. A DNS hiccup that hit
// one loop and not the other -- entirely possible, they are separate lookups seconds apart
// during a rolling start -- left the node with an ASYMMETRIC view of that peer: e.g. Raft
// replicating to it while every forwarded write reported "unknown peer", or the reverse.
// Nothing ever re-resolved, so the asymmetry was permanent until a restart, and it was
// invisible: both loops only warn.
//
// One resolution, one registration of every plane, all-or-nothing per peer.
seastar::future<bool> ClusterDataPlane::registerPeer(NodeId id, const std::string& addr, bool replicated) {
    const HostPort hp = parseHostPort(addr);
    std::optional<seastar::net::inet_address> a;
    try {
        a = co_await resolveHost(hp.host);
    } catch (const std::exception& e) {
        timestar::http_log.warn("cluster: peer {} ({}) unresolved: {} (will retry; routes to it fail meanwhile)", id,
                                addr, e.what());
        co_return false;
    }
    const seastar::socket_address dataAddr(*a, static_cast<uint16_t>(hp.port + kDataPlanePortOffset));
    // The shard-0 client-only instance (query/metadata fan-out) always needs it.
    rpc_->addPeer(id, dataAddr);
    if (replicated) {
        const seastar::socket_address raftAddr(*a, static_cast<uint16_t>(hp.port + kRaftPortOffset));
        co_await shards_.invoke_on_all([id, raftAddr, dataAddr](ShardRaftPlane& p) {
            p.addRaftPeer(id, raftAddr);
            p.addDataPeer(id, dataAddr);
        });
    }
    co_return true;
}

seastar::future<> ClusterDataPlane::registerAllPeers(bool replicated) {
    unresolvedPeers_.clear();
    for (const auto& [id, addr] : rt_->peerAddresses) {
        if (id == rt_->selfId)
            continue;
        if (!co_await registerPeer(id, addr, replicated))
            unresolvedPeers_[id] = addr;
    }
    if (!unresolvedPeers_.empty())
        startPeerResolver(replicated);
    co_return;
}

void ClusterDataPlane::startPeerResolver(bool replicated) {
    // Resolution is BEST-EFFORT at startup -- a peer that is not yet up during a rolling
    // start must not fail THIS node's boot. But "best effort, once, forever" is what made
    // an unresolved peer a permanent hole: the address was never looked up again, so a
    // peer whose DNS record appeared thirty seconds later stayed unreachable for the
    // lifetime of the process. This retries until every peer is registered, then stops.
    static constexpr auto kInterval = std::chrono::seconds(5);
    peerResolveTimer_.set_callback([this, replicated] {
        if (peerResolveRunning_ || peerResolveGate_.is_closed())
            return;
        peerResolveRunning_ = true;
        (void)seastar::with_gate(peerResolveGate_, [this, replicated]() -> seastar::future<> {
            // `unresolvedPeers_` is a member, so nothing from this lambda's frame is
            // borrowed across the suspensions below.
            std::map<NodeId, std::string> still;
            for (const auto& [id, addr] : unresolvedPeers_) {
                if (!co_await registerPeer(id, addr, replicated))
                    still[id] = addr;
                else
                    timestar::http_log.info("cluster: peer {} ({}) resolved and registered on retry", id, addr);
            }
            unresolvedPeers_ = std::move(still);
            if (unresolvedPeers_.empty())
                peerResolveTimer_.cancel();
            co_return;
        }).finally([this] { peerResolveRunning_ = false; });
    });
    peerResolveTimer_.arm_periodic(kInterval);
}

seastar::future<> ClusterDataPlane::stop() {
    // The peer re-resolution loop touches rpc_ and shards_; quiesce it first.
    peerResolveTimer_.cancel();
    if (!peerResolveGate_.is_closed())
        co_await peerResolveGate_.close();
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
    // apply directly on the owner. The replicated path is writeFromShard's -- the two
    // were separate functions with identical bodies once Phase 1c let any shard write.
    if (replicated_)
        return writeFromShard(std::move(batch));
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
        r.errorMessage =
            "leader node(s) " + nodes + " unreachable; their VShards were woken to re-elect -- retry shortly";
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

seastar::future<> ClusterDataPlane::writeFromShard(data::WriteBatch batch) {
    // Split the series by the shard that owns their VShard's Raft group, then hand each
    // slice to that shard's plane (which resolves the leader for its own VShards
    // locally and forwards to a remote leader when needed). Safe to call from the shard
    // the HTTP request arrived on (see the header): it touches only `shards_`.
    //
    // Guard the documented precondition the way write()/metadata() guard theirs: called
    // before start(), or on an RF=1 node, `shards_` holds no planes and every slice
    // would silently resolve against a null one.
    if (!replicated_ || !shardsStarted_)
        throw std::runtime_error("ClusterDataPlane::writeFromShard requires replicated mode after start()");
    return writeSlicesToOwningShards(shards_, std::move(batch), dir_.get());
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
