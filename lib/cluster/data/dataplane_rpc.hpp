#pragma once

#include "data_plane.hpp"

#include <memory>
#include <seastar/core/future.hh>
#include <seastar/net/socket_defs.hh>

namespace timestar::data {

// The data-plane inter-node transport over seastar::rpc. It is BOTH the server
// (dispatching forwarded writes/queries into this node's LocalStore sink) and the
// client (a DataPlaneClient the WriteRouter/QueryCoordinator use to reach peers).
// One connection per peer HOST is reused for all data traffic. Unlike the Raft
// transport, data RPCs are REQUEST/RESPONSE and awaited: a forwarded write
// resolves only once the owner has durably accepted it, and a remote query
// returns the owner's partial -- so the caller's fan-out actually reflects
// durability and completeness (no silent partial results).
class DataPlaneRpc : public DataPlaneClient {
public:
    DataPlaneRpc();
    ~DataPlaneRpc() override;
    DataPlaneRpc(const DataPlaneRpc&) = delete;
    DataPlaneRpc& operator=(const DataPlaneRpc&) = delete;

    // Serve on `local`, dispatching incoming forwarded writes/queries into `sink`
    // (this node's storage). `sink` must outlive the transport.
    seastar::future<> start(seastar::socket_address local, LocalStore& sink);
    seastar::future<> stop();
    void addPeer(NodeId id, seastar::socket_address addr);

    // DataPlaneClient (peer-facing). Both are awaited request/response RPCs.
    seastar::future<> forwardWrites(NodeId to, std::vector<DataPoint> points) override;
    seastar::future<QueryPartial> queryRemote(NodeId to, QuerySpec spec) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace timestar::data
