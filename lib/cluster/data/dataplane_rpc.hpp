#pragma once

#include "../control/control_command.hpp"
#include "data_plane.hpp"
#include "node_store.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/rpc/rpc_types.hh>  // rpc_clock_type
#include <stdexcept>

namespace timestar::data {

class FrozenDeletePlanSink {
public:
    virtual ~FrozenDeletePlanSink() = default;
    virtual seastar::future<control::FreezeDeletePlanResult> handleFrozenDeletePlan(
        control::FrozenDeletePlanRpcRequest request) = 0;
};

class ControlJoinSink {
public:
    virtual ~ControlJoinSink() = default;
    virtual seastar::future<control::ControlJoinResult> handleControlJoin(
        control::ControlJoinRequest request) = 0;
};

// OptDeadline (node_store.hpp): a wall-clock point past which an awaited data-plane RPC
// must give up. std::nullopt is available to callers with no deadline of their own.
// seastar's rpc clock is lowres_clock,
// so it is the same clock the router measures its budget in.

// The data-plane inter-node transport over seastar::rpc. It is BOTH the server
// (dispatching forwarded writes/queries into this node's storage sink) and the
// client used by cluster routers and coordinators. One
// connection per peer HOST is reused for all data traffic. Unlike the Raft
// transport, data RPCs are REQUEST/RESPONSE and awaited: a forwarded write
// resolves only once the owner has durably accepted it, and a remote query
// returns the owner's partial -- so the caller's fan-out actually reflects
// durability and completeness (no silent partial results).
//
class DataPlaneRpc : public NodeTransport {
public:
    DataPlaneRpc();
    ~DataPlaneRpc() override;
    DataPlaneRpc(const DataPlaneRpc&) = delete;
    DataPlaneRpc& operator=(const DataPlaneRpc&) = delete;

    // Serve on `local`, dispatching incoming forwarded commands into `sink` (this
    // node's storage). `sink` must outlive the transport.
    // `perShardListener` declares that EVERY shard starts an instance on this same
    // address. It selects where inbound connections are accepted: pinned to the
    // starting shard (false -- a single instance must answer every connection itself,
    // or a peer's WAITED call hangs on a shard with no server) versus distributed
    // across shards (true). It is NOT SO_REUSEPORT: this seastar disables reuseport
    // outright, so shard 0 owns the only socket and hands each accepted fd to the
    // chosen shard's listener. Passing false while every shard listens quietly keeps
    // the node's whole inbound data plane on one core; passing true with only one
    // instance HANGS peers. See listenServer().
    seastar::future<> start(seastar::socket_address local, NodeStore& sink, bool perShardListener = false);
    // Start CLIENT-ONLY: create the peer stubs, but serve nothing. Used when some
    // OTHER instance already listens on this node's data-plane address (in replicated
    // mode every shard starts a listener on it -- NOT via SO_REUSEPORT, which this
    // seastar disables; shard 0 owns the one socket and distributes the accepted fds)
    // while this instance is still needed to REACH peers -- e.g. the shard-0
    // query/metadata fan-out. Serving verbs from a second instance on the same shard
    // would only steal a share of the node's inbound connections back onto that one
    // core.
    seastar::future<> startClientOnly();
    seastar::future<> stop();
    void addPeer(NodeId id, seastar::socket_address addr);

    // forwardWriteBatch resolves only once
    // the owner has durably applied the batch; queryNode returns the owner's
    // NodeQueryPartial. Both awaited.
    seastar::future<> forwardWriteBatch(NodeId to, WriteBatch batch) override;
    seastar::future<NodeQueryPartial> queryNode(NodeId to, NodeQueryRequest req) override;
    // Deadline-bearing read used by the replicated coordinator. It bounds both
    // the exact-v1 connection check and the query RPC; the interface overload
    // above remains for callers that own no wall-clock budget.
    seastar::future<NodeQueryPartial> queryNode(NodeId to, NodeQueryRequest req, OptDeadline deadline);
    seastar::future<MetadataResult> queryMetadata(NodeId to, MetadataRequest req) override;
    seastar::future<PatternSeriesResult> findPatternSeries(NodeId to, PatternSeriesRequest req,
                                                           OptDeadline deadline = std::nullopt) override;
    // v1 request forwarding to the peer believed to lead group 0. The
    // deadline bounds the v1 check and the waited request/reply exchange.
    seastar::future<control::FreezeDeletePlanResult> frozenDeletePlan(
        NodeId to, control::FrozenDeletePlanRpcRequest request,
        OptDeadline deadline = std::nullopt);
    // Token-authorized observer admission. The reply is deliberately
    // a step result: Joining is safe to retry while learner catch-up proceeds.
    seastar::future<control::ControlJoinResult> controlJoin(
        NodeId to, control::ControlJoinRequest request,
        OptDeadline deadline = std::nullopt);
    // The production remote propose (write-scaleout 3a/3b): borrows the caller's groups
    // (no merge allocation, and the caller keeps them so it can retry the failed ones)
    // and returns per-VShard rejects carrying the peer's view of the real leader.
    seastar::future<ProposeOutcome> proposeWriteHinted(NodeId to, VShardBatchView view, OptDeadline deadline) override;
    seastar::future<ProposeOutcome> proposeCommandHinted(NodeId to, uint16_t vshard, ReplicatedCommand command,
                                                         OptDeadline deadline = std::nullopt) override;

    // The Raft propose target incoming proposeWrite RPCs dispatch into (the node's
    // ReplicatedVShardHost). Must be set before a peer sends proposeWrite; outlives
    // the transport.
    void setProposeSink(ProposeSink& sink);

    // The leader-reach target incoming leaderReadIndex/leaderCommitIndex RPCs dispatch
    // into (M4 replica reads). Must be set before a peer sends them; outlives the
    // transport.
    void setReadIndexSink(ReadIndexSink& sink);
    void setFrozenDeletePlanSink(FrozenDeletePlanSink& sink);
    void setControlJoinSink(ControlJoinSink& sink);

    // M4 replica-read leader-reach client calls: confirm a linearizable ReadIndex /
    // fetch the commit index for `vshard` at peer `to` (which must be its leader). The
    // peer's rejection (not-leader / not-hosted / no-quorum) propagates as a thrown
    // exception -- the caller treats it as a partition/redirect, never a stale value.
    seastar::future<raft::LogIndex> leaderReadIndex(NodeId to, uint16_t vshard);
    seastar::future<raft::LogIndex> leaderCommitIndex(NodeId to, uint16_t vshard);

    // Enable mutual TLS on this transport (X1b, required before GA): the server
    // requires a client certificate and both sides trust `caPem`; each presents
    // (certPem, keyPem). MUST be called before start(). `expectedPeerName` is the SAN
    // the client verifies the server's cert against (cluster-UUID-bound node name in
    // production). With TLS on, a plaintext peer -- or one whose cert the CA does not
    // sign -- cannot connect. All PEM (x509).
    void setTlsCredentials(std::string certPem, std::string keyPem, std::string caPem, std::string expectedPeerName);

private:
    // Require an exact-v1 handshake once per connection. The optional deadline
    // is shared with the request this check gates.
    seastar::future<> ensureV1(NodeId to, OptDeadline deadline = std::nullopt);
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace timestar::data
