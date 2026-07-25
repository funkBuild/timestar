#pragma once

#include <seastar/core/gate.hh>
#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"  // assignCore
#include "../data/dataplane_rpc.hpp"
#include "../data/node_store.hpp"
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
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
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
// single Raft port AND on its single data-plane port (SO_REUSEPORT, the same pattern
// the HTTP server uses) and keeps its own peer clients for each. Previously one
// transport of each kind lived on shard 0 and carried the whole node's traffic, which
// made shard 0 the write bottleneck -- it showed ~10x the journal-sync latency of the
// other shards while all cores sat ~25% idle. Egress is now entirely local; an inbound
// Raft envelope still hops to the shard owning its group, and an inbound proposeWrite
// is split across the shards owning its VShards, but the accept/read work is spread by
// the kernel instead of landing on one core.

// The shard that owns a VShard's Raft group.
inline unsigned shardForVShard(uint16_t vshard) {
    return timestar::assignCore(timestar::VShardId{vshard}, seastar::smp::count);
}

// One shard's slice of the replicated data plane: a full ReplicatedDataPlane holding
// only the VShards this shard owns. Constructed on every shard via
// seastar::sharded<ShardRaftPlane>, so each reactor ticks only its own groups.
class ShardRaftPlane : public data::ProposeSink {
public:
    seastar::future<> init(seastar::sharded<Engine>* engines, seastar::sharded<ShardRaftPlane>* peers,
                           const data::VShardDirectory* dir, data::NodeId self, std::string journalRoot,
                           std::chrono::milliseconds tick) {
        store_ = std::make_unique<EngineLocalStore>(*engines);
        peers_ = peers;
        transport_ = std::make_unique<raft::RaftRpcTransport>();
        // This shard's OWN data-plane transport: its listener on the node's data-plane
        // port (SO_REUSEPORT) and its own peer clients. Every remote leader forward
        // this shard makes now leaves from this core; nothing hops to shard 0.
        rpc_ = std::make_unique<data::DataPlaneRpc>();
        // Journals are per-VShard directories under this root, and each VShard belongs
        // to exactly one shard, so shards never contend for the same journal.
        plane_ = std::make_unique<ReplicatedDataPlane>(*store_, *transport_, *rpc_, *dir, self,
                                                       std::filesystem::path(journalRoot), tick);
        return seastar::make_ready_future<>();
    }

    // Listen for peer data-plane traffic on the node's data-plane port from THIS shard
    // (SO_REUSEPORT, as with the Raft port above). Inbound forwarded writes/queries are
    // served by this shard's EngineLocalStore, which dispatches to the owning engine
    // core itself; an inbound proposeWrite is split across the shards owning its
    // VShards by proposeBatch() below.
    seastar::future<> startDataPlane(seastar::socket_address local) {
        rpc_->setProposeSink(*this);
        return rpc_->start(local, *store_);
    }

    void addDataPeer(data::NodeId id, seastar::socket_address addr) { rpc_->addPeer(id, addr); }

    // ProposeSink: a peer forwarded a batch because this NODE leads those VShards. The
    // connection landed on whichever shard the kernel gave it, which is unrelated to
    // which shards own the VShards -- so split and replicate each slice on its owner.
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override;

    // Listen on the node's Raft port from THIS shard. Every shard binds the same
    // address; seastar uses SO_REUSEPORT so each gets its own socket and the kernel
    // distributes connections. Envelopes are decoded on the shard owning the group.
    seastar::future<> startTransport(seastar::socket_address local) {
        co_await transport_->start(local, [](raft::Envelope) { return seastar::make_ready_future<>(); });
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
    bool ready() const { return plane_ != nullptr; }

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
        // The plane first (it borrows both transports), then the transports, then the
        // store they dispatch into.
        if (plane_)
            co_await plane_->stop();
        plane_.reset();
        if (rpc_)
            co_await rpc_->stop();
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
    std::map<unsigned, data::WriteBatch> byShard;
    for (auto& sref : batch.series) {
        const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(sref.seriesKey));
        data::WriteBatch& dest = byShard[shardForVShard(vs)];
        dest.schemaVersion = batch.schemaVersion;  // carried per slice
        dest.series.push_back(std::move(sref));
    }
    std::vector<seastar::future<>> pending;
    pending.reserve(byShard.size());
    for (auto& [shard, slice] : byShard)
        pending.push_back(shards.invoke_on(
            shard, [b = std::move(slice)](ShardRaftPlane& p) mutable { return p.plane().write(std::move(b)); }));

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
    std::map<unsigned, data::WriteBatch> byShard;
    for (auto& sref : batch.series) {
        const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(sref.seriesKey));
        data::WriteBatch& dest = byShard[shardForVShard(vs)];
        dest.schemaVersion = batch.schemaVersion;
        dest.series.push_back(std::move(sref));
    }
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byShard.size());
    for (auto& [shard, slice] : byShard)
        pending.push_back(shards.invoke_on(shard, [b = std::move(slice)](ShardRaftPlane& p) mutable {
            return p.plane().host().proposeBatch(std::move(b));
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
    return proposeSlicesToOwningShards(*peers_, std::move(batch));
}

}  // namespace timestar::cluster
