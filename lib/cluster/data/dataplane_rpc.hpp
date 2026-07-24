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
    // Default {1,1}. Set before serving so peers negotiate against the real range.
    void setLocalVersion(features::VersionRange range);

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
