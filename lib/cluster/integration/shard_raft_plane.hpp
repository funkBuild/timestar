#pragma once

#include <seastar/core/gate.hh>
#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"  // assignCore
#include "../data/dataplane_rpc.hpp"
#include "../data/node_store.hpp"
#include "../features/feature_gate.hpp"  // VersionRange
#include "../raft/raft_codec.hpp"  // decodeEnvelope
#include "../raft/raft_driver.hpp"  // RaftTransport
#include "../raft/raft_rpc_transport.hpp"
#include "engine_local_store.hpp"
#include "replicated_data_plane.hpp"

#include <chrono>
#include <exception>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <string>
#include <utility>
#include <vector>

namespace timestar::cluster {

// Per-VShard Raft groups are spread ACROSS CORES: shard S owns exactly the VShards
// with assignCore(vshard, smp::count) == S. Previously every group lived on shard 0,
// so one reactor ran all 4096 groups' ticks, heartbeat steps and state-machine
// applies -- enough to saturate it, diverge that node's leadership view and make its
// queries fail. Spreading them divides that work by the core count.
//
// BOTH inter-node transports are now PER SHARD: every shard listens on the node's
// single Raft port AND on its single data-plane port, and keeps its own peer clients
// for each. Previously one transport of each kind lived on shard 0 and carried the
// whole node's traffic, which made shard 0 the write bottleneck -- it showed ~10x the
// journal-sync latency of the other shards while all cores sat ~25% idle. Egress is now
// entirely local; an inbound Raft envelope still hops to the shard owning its group,
// and an inbound proposeWrite is split across the shards owning its VShards.
//
// The per-shard listen is NOT SO_REUSEPORT, despite what the surrounding comments used
// to claim: this seastar hardcodes posix_reuseport_available() to false, so shard 0
// owns the one real socket and each accepted fd is handed to the shard the listen
// options name. BOTH listeners therefore ask for connection_distribution (the
// perShardListener flag on DataPlaneRpc::start and RaftRpcTransport::start). The Raft
// listener used to pin itself per shard, so shard 0 accepted and read ALL inbound Raft
// traffic and ran every peek/route hop while shards 1..N held accept promises that
// never resolved -- invisible rather than fatal only because Raft's verb is no_wait, so
// nothing hung waiting on a reply.

// The shard that owns a VShard's Raft group.
inline unsigned shardForVShard(uint16_t vshard) {
    return timestar::assignCore(timestar::VShardId{vshard}, seastar::smp::count);
}

// Raised when a slice is routed to a shard whose plane has already been torn down
// (shutdown in progress). It is a RETRYABLE condition -- the write never reached Raft,
// so nothing was committed and the caller may safely re-route it -- and it must be an
// error rather than a silent drop, or a shutting-down node would ack writes it discarded.
inline constexpr const char* kShardStoppingError = "cluster: shard data plane is stopping; retry this write";

// Mutual-TLS material for the per-shard data-plane transports. Set on ClusterDataPlane
// before start(); it is pushed to EVERY shard's DataPlaneRpc, because in replicated
// mode those -- not ClusterDataPlane's own client-only instance -- are the node's
// listeners AND the write path's peer clients. Configuring only the shard-0 instance
// would leave the whole data plane plaintext while queries went TLS.
struct DataPlaneTls {
    std::string certPem;
    std::string keyPem;
    std::string caPem;
    std::string expectedPeerName;
};

// One shard's slice of the replicated data plane: a full ReplicatedDataPlane holding
// only the VShards this shard owns. Constructed on every shard via
// seastar::sharded<ShardRaftPlane>, so each reactor ticks only its own groups.
class ShardRaftPlane : public data::ProposeSink, public data::ReadIndexSink {
public:
    seastar::future<> init(seastar::sharded<Engine>* engines, seastar::sharded<ShardRaftPlane>* peers,
                           const data::VShardDirectory* dir, data::NodeId self, std::string journalRoot,
                           std::chrono::milliseconds tick) {
        store_ = std::make_unique<EngineLocalStore>(*engines);
        peers_ = peers;
        transport_ = std::make_unique<raft::RaftRpcTransport>();
        // This shard's OWN data-plane transport: its listener on the node's data-plane
        // port and its own peer clients. Every remote leader forward this shard makes
        // now leaves from this core; nothing hops to shard 0.
        rpc_ = std::make_unique<data::DataPlaneRpc>();
        // Journals are per-VShard directories under this root, and each VShard belongs
        // to exactly one shard, so shards never contend for the same journal.
        plane_ = std::make_unique<ReplicatedDataPlane>(*store_, *transport_, *rpc_, *dir, self,
                                                       std::filesystem::path(journalRoot), tick);
        return seastar::make_ready_future<>();
    }

    // Listen for peer data-plane traffic on the node's data-plane port from THIS shard.
    // Inbound forwarded writes/queries are served by this shard's EngineLocalStore,
    // which dispatches to the owning engine core itself; an inbound proposeWrite is
    // split across the shards owning its VShards by proposeBatch() below, and an
    // inbound leaderReadIndex/leaderCommitIndex is routed to the VShard's owning shard
    // by the ReadIndexSink overrides.
    //
    // Every peer-facing SETTING must be applied here, not just to ClusterDataPlane's
    // instance: in replicated mode these are the node's only data-plane servers and the
    // write path's only peer clients.
    seastar::future<> startDataPlane(seastar::socket_address local, const std::optional<DataPlaneTls>& tls,
                                     features::VersionRange localVersion) {
        if (tls)
            rpc_->setTlsCredentials(tls->certPem, tls->keyPem, tls->caPem, tls->expectedPeerName);
        rpc_->setLocalVersion(localVersion);
        rpc_->setProposeSink(*this);
        rpc_->setReadIndexSink(*this);
        return rpc_->start(local, *store_, /*perShardListener=*/true);
    }

    void addDataPeer(data::NodeId id, seastar::socket_address addr) { rpc_->addPeer(id, addr); }

    // ProposeSink: a peer forwarded a batch because this NODE leads those VShards. The
    // connection landed on whichever shard the kernel gave it, which is unrelated to
    // which shards own the VShards -- so split and replicate each slice on its owner.
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override;

    // ReadIndexSink: a replica is confirming freshness at the leader (M4 replica reads).
    // Same story as proposeBatch -- the connection landed on an arbitrary shard, so hop
    // to the one that owns this VShard's group and answer from there. The host rejects
    // (throws) if this node is not the current-term leader, which is what the reaching
    // replica needs; that rejection propagates unchanged.
    seastar::future<raft::LogIndex> leaderReadIndex(uint16_t vshard) override;
    seastar::future<raft::LogIndex> leaderCommitIndex(uint16_t vshard) override;

    // Listen on the node's Raft port from THIS shard. Every shard calls listen() on the
    // same address with connection_distribution, so accepted connections spread over
    // the shards instead of piling onto shard 0 (reuseport is disabled here -- see the
    // file header); envelopes are then decoded on the shard owning the group.
    //
    // The raw deliver hook is installed BEFORE listening: a connection accepted between
    // start() and setRawDeliver would fall through to the no-op DeliverFn and drop its
    // envelope (Raft would retry, but there is no reason to allow the window).
    seastar::future<> startTransport(seastar::socket_address local) {
        transport_->setRawDeliver([this](uint16_t groupId, const char* bytes, size_t len) {
            const unsigned owner = shardForVShard(groupId);
            if (owner == seastar::this_shard_id())
                return deliverDecoded(bytes, len);
            // The bytes stay owned by this shard; the target only READS them, and
            // submit_to is awaited, so they outlive the read.
            auto* peers = peers_;
            return seastar::smp::submit_to(owner,
                                           [peers, bytes, len] { return peers->local().deliverDecoded(bytes, len); });
        });
        co_await transport_->start(
            local, [](raft::Envelope) { return seastar::make_ready_future<>(); }, /*perShardListener=*/true);
        co_return;
    }

    void addRaftPeer(data::NodeId id, seastar::socket_address addr) { transport_->addPeer(id, addr); }

    // Decode on THIS shard and hand to the local group registry.
    seastar::future<> deliverDecoded(const char* bytes, size_t len) {
        auto env = raft::decodeEnvelope(std::string(bytes, len));
        if (!env || !plane_)
            return seastar::make_ready_future<>();
        return plane_->host().registry().deliver(std::move(*env));
    }

    // Un-hibernate this shard's groups that still believe `leader` leads them.
    size_t wakeFollowersOf(data::NodeId leader) {
        if (!plane_)
            return 0;
        return plane_->host().registry().wakeFollowersOf(leader);
    }

    // Add a VShard group -- only called on the shard that owns it.
    seastar::future<> addVShard(uint16_t vshard, std::vector<data::NodeId> voters, raft::RaftOptions opts) {
        return plane_->addVShard(vshard, std::move(voters), opts);
    }

    void startTicking() {
        if (plane_)
            plane_->startTicking();
    }

    ReplicatedDataPlane& plane() { return *plane_; }

    // Whether this shard's plane may still take work. `plane_ != nullptr` alone is too
    // weak: stop() tears the plane down only AFTER draining its RPC server, so between
    // the two there is a window in which the plane is still non-null but is about to
    // stop ticking -- a slice admitted there waits on a commit that will never be
    // driven, which shows up as a shutdown hang bounded only by the process's shutdown
    // timeout. `stopping_` closes that window at the very top of stop(). Work already
    // in flight is unaffected (it awaits a plane that keeps ticking until plane_->stop()
    // runs); only NEW slices are turned away, with the retryable kShardStoppingError.
    bool ready() const { return plane_ != nullptr && !stopping_; }

    // Peer proposals this shard's data-plane listener has SERVED (write-scaleout 1b/2):
    // it counts on the shard the connection was accepted on, not on the shards that own
    // the VShards, so it is the node's per-core inbound-ingress distribution. Pins the
    // production perShardListener=true: with the listener pinned instead, every peer
    // connection lands on shard 0 and this is 0 everywhere else.
    uint64_t inboundProposals() const { return inboundProposals_; }

    // This shard's slice of the cluster-wide leadership picture.
    struct Counts {
        size_t hosted = 0;
        size_t led = 0;
        size_t leaderless = 0;
        std::map<data::NodeId, size_t> peerCaughtUp;
    };

    Counts counts(data::NodeId self, const std::vector<data::NodeId>& peers) const {
        Counts c;
        if (!plane_)
            return c;
        auto& host = const_cast<ReplicatedDataPlane*>(plane_.get())->host();
        c.hosted = host.vshardCount();
        for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
            if (shardForVShard(vs) != seastar::this_shard_id())
                continue;  // not ours
            const data::NodeId leader = host.leaderOf(vs);
            if (leader == timestar::raft::kNoNode) {
                ++c.leaderless;
                continue;
            }
            if (leader != self)
                continue;
            ++c.led;
            raft::RaftGroup* g = host.group(vs);
            if (!g)
                continue;
            const auto last = g->node().log().lastIndex();
            for (data::NodeId peer : peers)
                if (peer != self && g->matchIndexOf(peer) >= last)
                    ++c.peerCaughtUp[peer];
        }
        return c;
    }

    // Rebalance leadership among THIS shard's groups. Because assignCore spreads
    // VShards evenly over shards and every shard sees the same node set, balancing
    // each shard's slice independently balances the cluster as a whole.
    seastar::future<size_t> rebalance(size_t maxTransfers, data::NodeId self,
                                      std::vector<data::NodeId> peers) {
        if (!plane_ || maxTransfers == 0 || peers.empty())
            co_return 0;
        auto& host = plane_->host();
        std::map<data::NodeId, size_t> led;
        std::vector<uint16_t> mine;
        size_t totalLed = 0;
        for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
            if (shardForVShard(vs) != seastar::this_shard_id())
                continue;
            const data::NodeId leader = host.leaderOf(vs);
            if (leader == timestar::raft::kNoNode)
                continue;
            ++led[leader];
            ++totalLed;
            if (leader == self)
                mine.push_back(vs);
        }
        const size_t fair = totalLed / peers.size();
        if (mine.size() <= fair)
            co_return 0;

        std::vector<std::pair<data::NodeId, size_t>> targets;
        for (data::NodeId id : peers) {
            if (id == self)
                continue;
            const size_t have = led.count(id) ? led.at(id) : 0;
            if (have < fair)
                targets.push_back({id, fair - have});
        }
        if (targets.empty())
            co_return 0;

        const size_t budget = std::min(maxTransfers, mine.size() - fair);
        size_t done = 0, ti = 0;
        for (uint16_t vs : mine) {
            if (done >= budget)
                break;
            size_t tried = 0;
            while (tried < targets.size() && targets[ti % targets.size()].second == 0) {
                ++ti;
                ++tried;
            }
            if (tried == targets.size())
                break;
            auto& [target, deficit] = targets[ti % targets.size()];
            raft::RaftGroup* g = host.group(vs);
            if (!g || !g->isLeader())
                continue;
            try {
                co_await g->transferLeadership(target);
                --deficit;
                ++done;
                ++ti;
            } catch (...) {
                ++ti;  // next pass retries
            }
        }
        co_return done;
    }

    seastar::future<> stop() {
        // Refuse NEW slices from this instant (see ready()), before anything is torn
        // down: sharded<>::stop() runs every shard's stop() concurrently, so a shard
        // that has not started stopping can still route a slice here.
        stopping_ = true;
        // STOP SERVING FIRST. This shard's DataPlaneRpc is one of the node's real
        // data-plane servers, and its handlers reach the plane (proposeBatch ->
        // p.plane()). Tearing the plane down first left a window in which an inbound
        // peer proposeWrite dereferenced a null plane_ and crashed the node -- during a
        // rolling restart under write load, exactly when it hurts. Stopping the server
        // first also lets in-flight handlers finish against a still-ticking Raft plane
        // rather than being cut off mid-commit. rpc_ itself is only DESTROYED after
        // plane_, which borrows it.
        if (rpc_)
            co_await rpc_->stop();
        if (plane_)
            co_await plane_->stop();
        plane_.reset();
        rpc_.reset();
        if (transport_)
            co_await transport_->stop();
        transport_.reset();
        store_.reset();
        co_return;
    }

private:
    // Declared so plane_ (which borrows the others) is destroyed FIRST.
    std::unique_ptr<EngineLocalStore> store_;
    std::unique_ptr<raft::RaftRpcTransport> transport_;  // this shard's own Raft listener + peer clients
    seastar::sharded<ShardRaftPlane>* peers_ = nullptr;
    std::unique_ptr<data::DataPlaneRpc> rpc_;  // this shard's own data-plane listener + peer clients
    std::unique_ptr<ReplicatedDataPlane> plane_;
    bool stopping_ = false;          // set at the top of stop(); see ready()
    uint64_t inboundProposals_ = 0;  // peer proposals served BY THIS SHARD's listener
};

// Split `batch` by the shard that OWNS each series' VShard Raft group and dispatch
// every slice CONCURRENTLY, then await them all.
//
// The slices used to be awaited one at a time inside the grouping loop, so a batch
// spanning S shards paid the SUM of S full quorum round trips (durable append +
// replicate + commit + apply) rather than the max -- the same defect
// ReplicatedVShardHost::proposeBatch already fixed one layer down, at the per-VShard
// level. All-must-commit semantics are unchanged: EVERY dispatched future is awaited
// even after one fails (abandoning an in-flight one would let its commit land on a
// destroyed continuation), and the first error is rethrown so a partial batch always
// surfaces as a retryable failure rather than a silent partial success.
inline seastar::future<> writeSlicesToOwningShards(seastar::sharded<ShardRaftPlane>& shards,
                                                   data::WriteBatch batch) {
    // Split by VShard ONCE (write-scaleout 2b) and bucket the resulting groups by
    // owning shard. Everything below -- the leader-node bucketing in the router and
    // the per-group propose -- re-buckets these same groups, so each series is moved
    // once and its key hashed once (write-scaleout 2a).
    std::map<unsigned, data::VShardBatches> byShard;
    for (auto& g : data::splitByVShard(std::move(batch)))
        byShard[shardForVShard(g.first)].push_back(std::move(g));
    std::vector<seastar::future<>> pending;
    pending.reserve(byShard.size());
    for (auto& [shard, slice] : byShard)
        pending.push_back(shards.invoke_on(shard, [b = std::move(slice)](ShardRaftPlane& p) mutable {
            // sharded<>::stop() runs every shard's stop() CONCURRENTLY, so a shard that
            // has already torn its plane down can be invoked from one that has not --
            // no timing luck needed. Fail retryably instead of dereferencing null.
            if (!p.ready())
                return seastar::make_exception_future<>(std::runtime_error(kShardStoppingError));
            return p.plane().write(std::move(b));
        }));

    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            co_await std::move(f);
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return;
}

// The peer-ingress twin of the above: replicate each slice through the Raft group on
// its owning shard. `false` (not the leader for some slice) makes the WHOLE batch fail
// so the caller redirects/retries -- never a silent partial commit.
inline seastar::future<bool> proposeSlicesToOwningShards(seastar::sharded<ShardRaftPlane>& shards,
                                                         data::WriteBatch batch) {
    // One split, buckets of groups -- same shape as writeSlicesToOwningShards.
    std::map<unsigned, data::VShardBatches> byShard;
    for (auto& g : data::splitByVShard(std::move(batch)))
        byShard[shardForVShard(g.first)].push_back(std::move(g));
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byShard.size());
    for (auto& [shard, slice] : byShard)
        pending.push_back(shards.invoke_on(shard, [b = std::move(slice)](ShardRaftPlane& p) mutable {
            if (!p.ready())  // see the note in writeSlicesToOwningShards
                return seastar::make_exception_future<bool>(std::runtime_error(kShardStoppingError));
            return p.plane().host().proposeVShardBatches(std::move(b));
        }));

    bool all = true;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            const bool ok = co_await std::move(f);
            all = all && ok;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return all;
}

inline seastar::future<bool> ShardRaftPlane::proposeBatch(data::WriteBatch batch) {
    ++inboundProposals_;  // served on THIS shard, whichever shards end up owning the slices
    return proposeSlicesToOwningShards(*peers_, std::move(batch));
}

inline seastar::future<raft::LogIndex> ShardRaftPlane::leaderReadIndex(uint16_t vshard) {
    return peers_->invoke_on(shardForVShard(vshard), [vshard](ShardRaftPlane& p) {
        if (!p.ready())
            return seastar::make_exception_future<raft::LogIndex>(std::runtime_error(kShardStoppingError));
        return p.plane().host().leaderReadIndex(vshard);
    });
}

inline seastar::future<raft::LogIndex> ShardRaftPlane::leaderCommitIndex(uint16_t vshard) {
    return peers_->invoke_on(shardForVShard(vshard), [vshard](ShardRaftPlane& p) {
        if (!p.ready())
            return seastar::make_exception_future<raft::LogIndex>(std::runtime_error(kShardStoppingError));
        return p.plane().host().leaderCommitIndex(vshard);
    });
}

}  // namespace timestar::cluster
