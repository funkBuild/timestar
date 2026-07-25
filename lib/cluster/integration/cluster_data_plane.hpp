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
class ClusterDataPlane {
public:
    ClusterDataPlane() = default;
    ClusterDataPlane(const ClusterDataPlane&) = delete;
    ClusterDataPlane& operator=(const ClusterDataPlane&) = delete;

    // Build placement from cfg, start the RPC server bound to this node's data-plane
    // address, and register every peer. `engines` must outlive this service. Throws
    // on a misconfigured cluster (fail-closed).
    seastar::future<> start(const ClusterConfig& cfg, seastar::sharded<Engine>& engines);
    seastar::future<> stop();

    // Route a partitioned write to VShard owners (local applied directly, remote
    // forwarded), and a query fanned out to owners then merged to the single-node
    // answer.
    seastar::future<> write(data::WriteBatch batch);
    seastar::future<QueryResponse> query(QueryRequest request);

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
    Status status() const;

    // Operator action (integration plan M5 leadership balancing, which is also v1's
    // READ balancing since reads go to leaders). Hands leadership of up to
    // `maxTransfers` VShards this node leads beyond its fair share to peer replicas
    // that are under it. Bounded per call so an operator request never runs long; call
    // repeatedly to converge. Returns the number of transfers initiated.
    //
    // This matters in practice: the first node to start wins every election, so a
    // fresh cluster puts ALL write coordination on one node until this runs.
    seastar::future<size_t> rebalanceLeadership(size_t maxTransfers);

private:
    // RF=3 leader read: fan out per-VShard-leader (see .cpp).
    seastar::future<QueryResponse> queryReplicated(QueryRequest request);

    std::optional<ClusterRuntime> rt_;
    // Declared in dependency order: deps before the router/coordinator that reference
    // them, so destruction (reverse order) tears the referrers down first.
    std::unique_ptr<data::VShardDirectory> dir_;
    std::unique_ptr<EngineLocalStore> local_;
    std::unique_ptr<data::DataPlaneRpc> rpc_;
    std::unique_ptr<http::HttpQueryHandler> finalizer_;
    std::unique_ptr<data::NodeWriteRouter> router_;
    std::unique_ptr<data::NodeQueryCoordinator> coord_;
    // RF=3 replicated path (replication_factor > 1); null in the RF=1/M2 mode.
    // Declared LAST so rdp_ (which borrows rpc_, raftTransport_, dir_, local_) tears
    // down first.
    std::unique_ptr<raft::RaftRpcTransport> raftTransport_;
    std::unique_ptr<ReplicatedDataPlane> rdp_;
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
