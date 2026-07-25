#pragma once

#include "../../config/timestar_config.hpp"   // ClusterConfig
#include "../../core/engine.hpp"
#include "../../http/http_query_handler.hpp"  // HttpQueryHandler, QueryResponse
#include "../data/dataplane_rpc.hpp"
#include "../data/node_query_coordinator.hpp"
#include "../data/node_write_router.hpp"
#include "../raft/raft_rpc_transport.hpp"
#include "cluster_runtime.hpp"
#include "engine_local_store.hpp"
#include "replicated_data_plane.hpp"
#include "shard_raft_plane.hpp"

#include <map>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>

namespace timestar::cluster {

// The data-plane RPC port is derived from the node's HTTP port by this offset (the
// [cluster] peers list carries HTTP host:port; the partitioned data plane needs its
// own listener). HTTP 8086 -> data-plane 9086.
inline constexpr uint16_t kDataPlanePortOffset = 1000;
// The Raft (replica<->replica) RPC listener port offset. HTTP 8086 -> Raft 10086.
inline constexpr uint16_t kRaftPortOffset = 2000;

// The node-level composition that wires every M2 brick into one live service
// (integration plan M2): ClusterRuntime placement -> EngineLocalStore sink ->
// DataPlaneRpc transport (server + peer clients) -> NodeWriteRouter (writes) +
// NodeQueryCoordinator (queries). The server starts ONE of these (on a single
// shard) when [cluster].enabled; the HTTP handlers route partitioned writes/queries
// through write()/query(). Lives on one shard; cross-shard handlers reach it via the
// server's global accessor + invoke_on.
class ClusterDataPlane : public data::ProposeSink {
public:
    ClusterDataPlane() = default;
    ClusterDataPlane(const ClusterDataPlane&) = delete;
    ClusterDataPlane& operator=(const ClusterDataPlane&) = delete;

    // Build placement from cfg, start the RPC server bound to this node's data-plane
    // address, and register every peer. `engines` must outlive this service. Throws
    // on a misconfigured cluster (fail-closed).
    seastar::future<> start(const ClusterConfig& cfg, seastar::sharded<Engine>& engines);
    seastar::future<> stop();

    // Peer-facing transport settings. MUST be set before start(), which is what pushes
    // them to EVERY data-plane transport this node owns -- in replicated mode that is
    // the per-shard listeners and peer clients, not just this object's own (client-only)
    // instance. Configuring one instance and not the others is the failure that matters:
    // shard-0 clients would speak TLS while every per-shard listener and the whole write
    // path stayed plaintext -- queries fail loudly, writes go silently unencrypted.
    void setTlsCredentials(DataPlaneTls creds) { tls_ = std::move(creds); }
    void setLocalVersion(features::VersionRange range) { localVersion_ = range; }

    // Route a partitioned write to VShard owners (local applied directly, remote
    // forwarded), and a query fanned out to owners then merged to the single-node
    // answer.
    seastar::future<> write(data::WriteBatch batch);
    seastar::future<QueryResponse> query(QueryRequest request);

    // The REPLICATED write entry point, callable from ANY shard -- the HTTP request
    // shard calls it directly instead of shipping the whole batch to shard 0.
    //
    // It touches only `shards_`, a seastar::sharded<> whose instance table is fixed
    // once start() has returned (and start() completes before the HTTP server accepts),
    // so there is no shard-0-owned mutable state on this path. The request shard splits
    // the batch by owning shard and dispatches every slice concurrently; the owning
    // shard resolves the VShard's current leader and, when that leader is remote,
    // forwards from ITS OWN peer client. Nothing rendezvouses on shard 0.
    //
    // PRECONDITION: replicated mode (replication_factor > 1). RF=1 still routes through
    // write() on shard 0.
    seastar::future<> writeFromShard(data::WriteBatch batch);

    // Scatter a metadata request to every VShard owner and merge: string-set union
    // for list kinds, SUM for cardinality (RF=1 disjoint series => exact).
    seastar::future<data::MetadataResult> metadata(data::MetadataRequest request);

    NodeId selfId() const { return rt_ ? rt_->selfId : 0; }
    const data::VShardDirectory& directory() const { return *dir_; }

    // Operator visibility (integration plan M5/M6 operator surface). A snapshot of
    // what this node believes about the cluster, for `GET /cluster/status`.
    // leaderless > 0 on a replicated cluster means those VShards cannot be read or
    // written -- the single most useful signal when a cluster is misbehaving (it had
    // to be inferred from query errors and `ss` output before this existed).
    struct Status {
        NodeId self = 0;
        uint16_t replicationFactor = 1;
        bool replicated = false;
        std::map<NodeId, std::string> peers;  // node id -> "host:port"
        // Replicated mode only (all zero otherwise):
        size_t vshardsHostedHere = 0;  // Raft groups this node replicates
        size_t vshardsLedHere = 0;     // of those, ones this node currently leads
        size_t vshardsLeaderless = 0;  // VShards with NO elected leader anywhere
        // Among the VShards this node LEADS, how many each peer has fully replicated
        // (matchIndex >= our lastIndex). A peer stuck near 0 here is not acking our
        // appends -- which also silently blocks leadership transfer to it, since
        // RaftNode::transferLeadership only sends TimeoutNow to a caught-up target.
        std::map<NodeId, size_t> peerCaughtUp;
    };
    seastar::future<Status> status() const;

    // Operator action (integration plan M5 leadership balancing, which is also v1's
    // READ balancing since reads go to leaders). Hands leadership of up to
    // `maxTransfers` VShards this node leads beyond its fair share to peer replicas
    // that are under it. Bounded per call so an operator request never runs long; call
    // repeatedly to converge. Returns the number of transfers initiated.
    //
    // This matters in practice: the first node to start wins every election, so a
    // fresh cluster puts ALL write coordination on one node until this runs.
    seastar::future<size_t> rebalanceLeadership(size_t maxTransfers);

    // ProposeSink: a peer forwarded a batch for VShards THIS node leads. Split it by
    // the shard owning each VShard's Raft group and replicate each slice there.
    // NOTE: in replicated mode the peer-facing listeners are the PER-SHARD ones, whose
    // sink is ShardRaftPlane itself (same split, from whichever shard accepted the
    // connection); this instance is client-only there. Kept as the composition's
    // ProposeSink for tests and for any single-instance embedding.
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override;

private:
    // RF=3 leader read: fan out per-VShard-leader (see .cpp).
    seastar::future<QueryResponse> queryReplicated(QueryRequest request);
    // vshard -> current leader, gathered from every shard (groups live across cores).
    seastar::future<std::map<uint16_t, data::NodeId>> gatherLeaders() const;

    std::optional<ClusterRuntime> rt_;
    seastar::sharded<Engine>* enginesPtr_ = nullptr;
    // Applied to this object's transport AND to every per-shard transport in start().
    std::optional<DataPlaneTls> tls_;
    // Everything this binary can read and write. Pushed to every per-shard transport
    // in start(), so peers negotiate against the node's REAL capability; leaving it at
    // the VersionRange default {1,1} would pin the whole cluster to the v1 wire format
    // no matter what the binaries support.
    features::VersionRange localVersion_{1, data::kWriteBatchFormatV2};
    // Declared in dependency order: deps before the router/coordinator that reference
    // them, so destruction (reverse order) tears the referrers down first.
    std::unique_ptr<data::VShardDirectory> dir_;
    std::unique_ptr<EngineLocalStore> local_;
    std::unique_ptr<data::DataPlaneRpc> rpc_;
    std::unique_ptr<http::HttpQueryHandler> finalizer_;
    std::unique_ptr<data::NodeWriteRouter> router_;
    std::unique_ptr<data::NodeQueryCoordinator> coord_;
    // RF=3 replicated path (replication_factor > 1); null in the RF=1/M2 mode.
    // Declared LAST so rdp_ (which borrows rpc_, dir_, local_) tears
    // down first.
    // Per-shard Raft planes: shard S owns the VShards with assignCore(vs)==S, so the
    // group tick/step/apply work is spread over all cores instead of saturating
    // shard 0. Each plane also owns its OWN Raft and data-plane transports (listener +
    // peer clients). No DATA-plane byte transits shard 0 any more; inbound RAFT traffic
    // still does, because that listener pins itself to its shard and reuseport is
    // disabled in this seastar (see shard_raft_plane.hpp's header note) -- egress is
    // per-shard for both.
    seastar::sharded<ShardRaftPlane> shards_;
    bool shardsStarted_ = false;
    bool replicated_ = false;
    uint16_t rf_ = 1;  // configured replication factor (reported by status())

    // Standing leadership-balancing loop (M5). Without it a fresh cluster leaves ALL
    // leadership on the first node to start, since it wins every election. Runs a
    // bounded pass periodically so the cluster self-levels; each pass is small enough
    // that it never monopolises the reactor, and passes never overlap.
    seastar::timer<> balanceTimer_;
    seastar::gate balanceGate_;
    bool balanceRunning_ = false;
    void startLeadershipBalancer();
};

}  // namespace timestar::cluster
