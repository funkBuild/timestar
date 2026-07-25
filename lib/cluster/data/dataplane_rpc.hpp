#pragma once

#include "../features/feature_gate.hpp"  // VersionRange
#include "data_plane.hpp"
#include "node_store.hpp"

#include <cstdint>
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

    // DataPlaneClient (legacy, peer-facing). Both are awaited request/response RPCs.
    seastar::future<> forwardWrites(NodeId to, std::vector<DataPoint> points) override;
    seastar::future<QueryPartial> queryRemote(NodeId to, QuerySpec spec) override;

    // NodeTransport (enriched, peer-facing). forwardWriteBatch resolves only once
    // the owner has durably applied the batch; queryNode returns the owner's
    // NodeQueryPartial. Both awaited.
    seastar::future<> forwardWriteBatch(NodeId to, WriteBatch batch) override;
    seastar::future<NodeQueryPartial> queryNode(NodeId to, NodeQueryRequest req) override;
    seastar::future<MetadataResult> queryMetadata(NodeId to, MetadataRequest req) override;
    seastar::future<bool> proposeWrite(NodeId to, WriteBatch batch) override;

    // The Raft propose target incoming proposeWrite RPCs dispatch into (the node's
    // ReplicatedVShardHost). Must be set before a peer sends proposeWrite; outlives
    // the transport.
    void setProposeSink(ProposeSink& sink);

    // The leader-reach target incoming leaderReadIndex/leaderCommitIndex RPCs dispatch
    // into (M4 replica reads). Must be set before a peer sends them; outlives the
    // transport.
    void setReadIndexSink(ReadIndexSink& sink);

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
    void setTlsCredentials(std::string certPem, std::string keyPem, std::string caPem,
                           std::string expectedPeerName);

    // This node's supported wire-version range (rolling-upgrade / compatibility, M6/X).
    // Defaults to {1, kWriteBatchFormatV2} -- everything this binary can read and
    // write. Set before serving to narrow it (a test pinning an old peer's range).
    void setLocalVersion(features::VersionRange range);

    // The WriteBatch format agreed with peer `to`, handshaked once per connection and
    // cached. Forwarded writes and proposes encode at this version, so a mixed-version
    // cluster silently speaks v1 while an INCOMPATIBLE peer (no overlapping range)
    // throws -- fail closed, never a mis-framed batch.
    seastar::future<uint32_t> versionFor(NodeId to);

    // Negotiate the wire version to speak with peer `to`: the highest version BOTH
    // support (features::negotiate over the exchanged ranges). THROWS if the ranges do
    // not overlap -- an incompatible peer is refused rather than silently mis-framed,
    // so a node never talks a format the other cannot read (decision 8).
    seastar::future<uint32_t> negotiateVersion(NodeId to);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace timestar::data
