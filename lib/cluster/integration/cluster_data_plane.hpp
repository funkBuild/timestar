#pragma once

#include "../../config/timestar_config.hpp"   // ClusterConfig
#include "../../core/engine.hpp"
#include "../../http/http_query_handler.hpp"  // HttpQueryHandler, QueryResponse
#include "../data/dataplane_rpc.hpp"
#include "../data/node_query_coordinator.hpp"
#include "../data/node_write_router.hpp"
#include "cluster_runtime.hpp"
#include "engine_local_store.hpp"

#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace timestar::cluster {

// The data-plane RPC port is derived from the node's HTTP port by this offset (the
// [cluster] peers list carries HTTP host:port; the partitioned data plane needs its
// own listener). HTTP 8086 -> data-plane 9086.
inline constexpr uint16_t kDataPlanePortOffset = 1000;

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

    NodeId selfId() const { return rt_ ? rt_->selfId : 0; }
    const data::VShardDirectory& directory() const { return *dir_; }

private:
    std::optional<ClusterRuntime> rt_;
    // Declared in dependency order: deps before the router/coordinator that reference
    // them, so destruction (reverse order) tears the referrers down first.
    std::unique_ptr<data::VShardDirectory> dir_;
    std::unique_ptr<EngineLocalStore> local_;
    std::unique_ptr<data::DataPlaneRpc> rpc_;
    std::unique_ptr<http::HttpQueryHandler> finalizer_;
    std::unique_ptr<data::NodeWriteRouter> router_;
    std::unique_ptr<data::NodeQueryCoordinator> coord_;
};

}  // namespace timestar::cluster
