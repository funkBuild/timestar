#pragma once

#include <seastar/core/gate.hh>
#include "../../core/vshard.hpp"  // assignCore
#include "../data/node_store.hpp"
#include "../raft/raft_codec.hpp"  // decodeEnvelope
#include "../raft/raft_driver.hpp"  // RaftTransport
#include "../raft/raft_rpc_transport.hpp"
#include "engine_local_store.hpp"
#include "replicated_data_plane.hpp"

#include <chrono>
#include <filesystem>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>

namespace timestar::cluster {

// Per-VShard Raft groups are spread ACROSS CORES: shard S owns exactly the VShards
// with assignCore(vshard, smp::count) == S. Previously every group lived on shard 0,
// so one reactor ran all 4096 groups' ticks, heartbeat steps and state-machine
// applies -- enough to saturate it, diverge that node's leadership view and make its
// queries fail. Spreading them divides that work by the core count.
//
// The Raft transport is now PER SHARD: every shard listens on the node's single Raft
// port (SO_REUSEPORT, the same pattern the HTTP server uses) and keeps its own peer
// clients. Previously one transport on shard 0 carried the whole node's Raft traffic,
// which made shard 0 the write bottleneck -- it showed ~10x the journal-sync latency
// of the other shards while all cores sat ~25% idle. Egress is now entirely local; an
// inbound envelope still hops to the shard owning its group, but the accept/read work
// is spread by the kernel instead of landing on one core.
//
// The data-plane RPC client set is still single, on shard 0, behind the proxy below.

// The shard that owns a VShard's Raft group.
inline unsigned shardForVShard(uint16_t vshard) {
    return timestar::assignCore(timestar::VShardId{vshard}, seastar::smp::count);
}

// Forwards a shard's peer-facing data-plane calls (leader forwarding of proposes) to
// the node's real DataPlaneRpc on shard 0.
class ShardRoutingNodeTransport : public data::NodeTransport {
public:
    explicit ShardRoutingNodeTransport(data::NodeTransport* home) : home_(home) {}

    seastar::future<> forwardWriteBatch(data::NodeId to, data::WriteBatch batch) override {
        if (seastar::this_shard_id() == 0)
            return home_->forwardWriteBatch(to, std::move(batch));
        return seastar::smp::submit_to(0u, [h = home_, to, b = std::move(batch)]() mutable {
            return h->forwardWriteBatch(to, std::move(b));
        });
    }

    seastar::future<data::NodeQueryPartial> queryNode(data::NodeId to, data::NodeQueryRequest req) override {
        if (seastar::this_shard_id() == 0)
            return home_->queryNode(to, std::move(req));
        return seastar::smp::submit_to(0u, [h = home_, to, r = std::move(req)]() mutable {
            return h->queryNode(to, std::move(r));
        });
    }

    seastar::future<data::MetadataResult> queryMetadata(data::NodeId to, data::MetadataRequest req) override {
        if (seastar::this_shard_id() == 0)
            return home_->queryMetadata(to, std::move(req));
        return seastar::smp::submit_to(0u, [h = home_, to, r = std::move(req)]() mutable {
            return h->queryMetadata(to, std::move(r));
        });
    }

    seastar::future<bool> proposeWrite(data::NodeId to, data::WriteBatch batch) override {
        if (seastar::this_shard_id() == 0)
            return home_->proposeWrite(to, std::move(batch));
        return seastar::smp::submit_to(0u, [h = home_, to, b = std::move(batch)]() mutable {
            return h->proposeWrite(to, std::move(b));
        });
    }

private:
    data::NodeTransport* home_;  // owned by ClusterDataPlane on shard 0
};

// One shard's slice of the replicated data plane: a full ReplicatedDataPlane holding
// only the VShards this shard owns. Constructed on every shard via
// seastar::sharded<ShardRaftPlane>, so each reactor ticks only its own groups.
class ShardRaftPlane {
public:
    // `homeRaft` / `homeClient` point at the node's single transports on shard 0.
    seastar::future<> init(seastar::sharded<Engine>* engines, seastar::sharded<ShardRaftPlane>* peers,
                           data::NodeTransport* homeClient, const data::VShardDirectory* dir, data::NodeId self,
                           std::string journalRoot, std::chrono::milliseconds tick) {
        store_ = std::make_unique<EngineLocalStore>(*engines);
        peers_ = peers;
        transport_ = std::make_unique<raft::RaftRpcTransport>();
        clientProxy_ = std::make_unique<ShardRoutingNodeTransport>(homeClient);
        // Journals are per-VShard directories under this root, and each VShard belongs
        // to exactly one shard, so shards never contend for the same journal.
        plane_ = std::make_unique<ReplicatedDataPlane>(*store_, *transport_, *clientProxy_, *dir, self,
                                                       std::filesystem::path(journalRoot), tick);
        return seastar::make_ready_future<>();
    }

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
        if (plane_)
            co_await plane_->stop();
        plane_.reset();
        clientProxy_.reset();
        if (transport_)
            co_await transport_->stop();
        transport_.reset();
        store_.reset();
        co_return;
    }

private:
    // Declared so plane_ (which borrows the others) is destroyed FIRST.
    std::unique_ptr<EngineLocalStore> store_;
    std::unique_ptr<raft::RaftRpcTransport> transport_;  // this shard's own listener + peer clients
    seastar::sharded<ShardRaftPlane>* peers_ = nullptr;
    std::unique_ptr<ShardRoutingNodeTransport> clientProxy_;
    std::unique_ptr<ReplicatedDataPlane> plane_;
};

}  // namespace timestar::cluster
