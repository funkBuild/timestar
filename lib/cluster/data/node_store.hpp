#pragma once

#include "../raft/raft_types.hpp"  // NodeId
#include "node_metadata.hpp"
#include "node_query.hpp"
#include "write_record.hpp"

#include <cstdint>
#include <seastar/core/future.hh>
#include <string>

namespace timestar::data {

using timestar::raft::NodeId;

// The node-local storage sink the enriched inter-node transport dispatches into
// (integration plan F.4). EngineLocalStore is the production implementation over
// sharded<Engine>; a test double implements it for transport/router tests. It
// replaces the lossy DataPoint-based LocalStore for the M2/M3 paths.
class NodeStore {
public:
    virtual ~NodeStore() = default;
    virtual seastar::future<> applyWrites(WriteBatch batch) = 0;
    virtual seastar::future<bool> applyDelete(std::string seriesKey, uint64_t start, uint64_t end) = 0;
    virtual seastar::future<NodeQueryPartial> queryLocal(NodeQueryRequest req) = 0;

    // This node's contribution to a scattered metadata request (M2). Default returns
    // empty so test doubles need not implement it; EngineLocalStore serves the real
    // schema/cardinality of its owned series.
    virtual seastar::future<MetadataResult> queryMetadata(MetadataRequest) {
        return seastar::make_ready_future<MetadataResult>();
    }
};

// The peer-facing client seam for the enriched command path (the client side of
// what NodeStore serves): sends a lossless WriteBatch to an owner node and awaits
// its durable apply, and runs a query on a peer returning its NodeQueryPartial.
// DataPlaneRpc is the production implementation over seastar::rpc; F.5's
// generalized WriteRouter/QueryCoordinator depend on THIS interface, not the
// concrete transport, so they can be unit-tested against an in-memory double.
// Replaces the lossy DataPoint-based DataPlaneClient.
class NodeTransport {
public:
    virtual ~NodeTransport() = default;
    virtual seastar::future<> forwardWriteBatch(NodeId to, WriteBatch batch) = 0;
    virtual seastar::future<NodeQueryPartial> queryNode(NodeId to, NodeQueryRequest req) = 0;

    // Fetch a peer's metadata contribution (M2 scatter). Default empty so test
    // doubles need not implement it; DataPlaneRpc serves it over the wire.
    virtual seastar::future<MetadataResult> queryMetadata(NodeId, MetadataRequest) {
        return seastar::make_ready_future<MetadataResult>();
    }

    // Forward a WriteBatch to a peer that LEADS the batch's VShards, to be
    // REPLICATED through Raft there (M3 RF=3). Resolves true on durable quorum commit,
    // false if the peer is not the leader (caller redirects). Default false so doubles
    // need not implement it.
    virtual seastar::future<bool> proposeWrite(NodeId, WriteBatch) {
        return seastar::make_ready_future<bool>(false);
    }
};

// The node-local Raft PROPOSE target for the RF=3 write path (M3): ReplicatedVShardHost
// implements it. DataPlaneRpc's proposeWrite verb dispatches an incoming forwarded
// batch into this sink for replication through the receiving node's Raft groups.
class ProposeSink {
public:
    virtual ~ProposeSink() = default;
    virtual seastar::future<bool> proposeBatch(WriteBatch batch) = 0;
};

}  // namespace timestar::data
