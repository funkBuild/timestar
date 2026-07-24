#pragma once

#include "data_plane.hpp"
#include "node_store.hpp"

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/net/socket_defs.hh>

namespace timestar::data {

// The data-plane inter-node transport over seastar::rpc. It is BOTH the server
// (dispatching forwarded writes/queries into this node's storage sink) and the
// client (used by the WriteRouter/QueryCoordinator to reach peers). One
// connection per peer HOST is reused for all data traffic. Unlike the Raft
// transport, data RPCs are REQUEST/RESPONSE and awaited: a forwarded write
// resolves only once the owner has durably accepted it, and a remote query
// returns the owner's partial -- so the caller's fan-out actually reflects
// durability and completeness (no silent partial results).
//
// It serves two command paths: the legacy DataPoint path (DataPlaneClient /
// LocalStore) and the enriched, lossless WriteBatch path (NodeTransport /
// NodeStore, integration plan F.4). A node registers exactly one via the matching
// start() overload; the enriched path is what M2/M3 use, the legacy path is
// removed once the routers migrate (F.5). Client stubs for BOTH verb sets are
// always created, so which path a peer serves is its own start()'s choice.
class DataPlaneRpc : public DataPlaneClient, public NodeTransport {
public:
    DataPlaneRpc();
    ~DataPlaneRpc() override;
    DataPlaneRpc(const DataPlaneRpc&) = delete;
    DataPlaneRpc& operator=(const DataPlaneRpc&) = delete;

    // Serve on `local`, dispatching incoming forwarded commands into `sink` (this
    // node's storage). `sink` must outlive the transport. The two overloads select
    // the command path this node serves; both must not be called on one instance.
    seastar::future<> start(seastar::socket_address local, LocalStore& sink);
    seastar::future<> start(seastar::socket_address local, NodeStore& sink);
    seastar::future<> stop();
    void addPeer(NodeId id, seastar::socket_address addr);

    // DataPlaneClient (legacy, peer-facing). Both are awaited request/response RPCs.
    seastar::future<> forwardWrites(NodeId to, std::vector<DataPoint> points) override;
    seastar::future<QueryPartial> queryRemote(NodeId to, QuerySpec spec) override;

    // NodeTransport (enriched, peer-facing). forwardWriteBatch resolves only once
    // the owner has durably applied the batch; queryNode returns the owner's
    // NodeQueryPartial. Both awaited.
    seastar::future<> forwardWriteBatch(NodeId to, WriteBatch batch) override;
    seastar::future<NodeQueryPartial> queryNode(NodeId to, NodeQueryRequest req) override;
    seastar::future<MetadataResult> queryMetadata(NodeId to, MetadataRequest req) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace timestar::data
