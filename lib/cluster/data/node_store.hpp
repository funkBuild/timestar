#pragma once

#include "../raft/raft_types.hpp"  // NodeId
#include "node_metadata.hpp"
#include "node_query.hpp"
#include "write_errors.hpp"
#include "write_record.hpp"

#include <cstdint>
#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace timestar::data {

using timestar::raft::NodeId;

// One VShard slice that did NOT commit, and everything the coordinator needs to route
// its retry (write-scaleout 3a/3b).
struct SliceReject {
    uint16_t vshard = 0;
    // The leader the REJECTING node believes leads this VShard, kNoNode if it does not
    // know. This is the v1 gap being closed: a bare `false` told the coordinator only
    // that its guess was wrong, so a primary that was alive but deposed WITHOUT a
    // placement change sent every retry straight back to itself.
    NodeId leaderHint = kNoNode;
    WriteFailure kind = WriteFailure::NotLeader;
};

// The result of proposing a set of VShard slices. `committed` is true only when EVERY
// slice durably committed on quorum; otherwise `rejects` names each slice that did not,
// so the caller re-dispatches ONLY those (never the ones that committed -- re-proposing
// a committed slice is safe but pointless, and doing it for a whole batch would multiply
// the work a leadership transfer costs).
struct ProposeOutcome {
    bool committed = false;
    std::vector<SliceReject> rejects;
};

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

    // proposeWrite with LEADER HINTS and without consuming the caller's groups
    // (write-scaleout 3a/3b) -- the production remote path. `view` borrows groups the
    // CALLER owns and must outlive the returned future (see VShardBatchView).
    //
    // The default forwards to proposeWrite, copying the view into a WriteBatch and
    // reporting hintless rejects, so an in-memory double that only implements
    // proposeWrite keeps working (and so does a peer too old to answer the hinted verb).
    virtual seastar::future<ProposeOutcome> proposeWriteHinted(NodeId to, VShardBatchView view) {
        std::vector<uint16_t> vshards;
        WriteBatch merged;
        for (const auto* g : view) {
            vshards.push_back(g->first);
            merged.schemaVersion = g->second.schemaVersion;
            merged.series.insert(merged.series.end(), g->second.series.begin(), g->second.series.end());
        }
        return proposeWrite(to, std::move(merged)).then([vshards = std::move(vshards)](bool ok) {
            ProposeOutcome out;
            out.committed = ok;
            if (!ok)
                for (uint16_t vs : vshards)
                    out.rejects.push_back(SliceReject{vs, kNoNode, WriteFailure::NotLeader});
            return out;
        });
    }
};

// The node-local Raft PROPOSE target for the RF=3 write path (M3): ReplicatedVShardHost
// implements it. DataPlaneRpc's proposeWrite verb dispatches an incoming forwarded
// batch into this sink for replication through the receiving node's Raft groups.
class ProposeSink {
public:
    virtual ~ProposeSink() = default;
    virtual seastar::future<bool> proposeBatch(WriteBatch batch) = 0;

    // The same work when the CALLER has already split the batch by VShard
    // (write-scaleout 2b): the replicated write path splits once at ingress and every
    // layer below re-buckets those groups, so handing them over avoids merging them
    // back into one WriteBatch only for the sink to split it again. The default does
    // exactly that merge, so a sink that does its own splitting (or a test double)
    // needs no change; ReplicatedVShardHost overrides it to propose each group
    // straight to its Raft group.
    virtual seastar::future<bool> proposeVShardBatches(VShardBatches groups) {
        return proposeBatch(mergeVShardBatches(std::move(groups)));
    }

    // The LOCAL analogue of NodeTransport::proposeWriteHinted (write-scaleout 3a/3b):
    // propose a borrowed selection of groups and report, per VShard, which ones did not
    // commit and who this node now believes leads them. `view` borrows groups the caller
    // owns and must outlive the returned future.
    //
    // The default copies and falls back to proposeVShardBatches, so a test double needs
    // no change; ReplicatedVShardHost overrides it to report per-group truth.
    virtual seastar::future<ProposeOutcome> proposeVShardBatchesHinted(VShardBatchView view) {
        VShardBatches copy;
        std::vector<uint16_t> vshards;
        copy.reserve(view.size());
        for (const auto* g : view) {
            vshards.push_back(g->first);
            copy.push_back(*g);
        }
        return proposeVShardBatches(std::move(copy)).then([vshards = std::move(vshards)](bool ok) {
            ProposeOutcome out;
            out.committed = ok;
            if (!ok)
                for (uint16_t vs : vshards)
                    out.rejects.push_back(SliceReject{vs, kNoNode, WriteFailure::NotLeader});
            return out;
        });
    }

    // The peer-ingress entry for the hinted verb: a forwarded batch arrives flat, so it
    // is split here and answered per VShard. The default splits and delegates; the
    // per-shard plane overrides it to fan the slices out to their owning shards.
    virtual seastar::future<ProposeOutcome> proposeBatchHinted(WriteBatch batch) {
        return seastar::do_with(splitByVShard(std::move(batch)),
                                [this](VShardBatches& groups) { return proposeVShardBatchesHinted(viewOf(groups)); });
    }
};

// Resolves the CURRENT Raft leader of a VShard (M3). ReplicatedVShardHost implements
// it from its local group's Raft view, so routing follows leadership across failover
// -- not the static placement primary (which breaks writes when the primary dies but
// a live quorum re-elects among the survivors). kNoNode = leader unknown here (no
// local group, or no leader elected yet); the router falls back to the placement
// primary as a hint.
class LeaderResolver {
public:
    virtual ~LeaderResolver() = default;
    virtual NodeId leaderOf(uint16_t vshard) const = 0;
};

// The node-local LEADER-REACH target for replica reads (M4). A follower/learner
// replica of a VShard confirms freshness at the CURRENT leader before serving a
// linearizable or bounded-staleness read (the ReplicaVShard `LeaderReadIndexFn`/
// `LeaderCommitFn`); DataPlaneRpc's leaderReadIndex/leaderCommitIndex verbs dispatch
// the RPC into this sink on the leader node. ReplicatedVShardHost implements it.
class ReadIndexSink {
public:
    virtual ~ReadIndexSink() = default;
    // A quorum-confirmed linearizable ReadIndex for `vshard` at THIS node. MUST reject
    // (throw) if this node is not the current leader or cannot confirm a quorum, so a
    // partitioned/deposed node never hands out a stale barrier.
    virtual seastar::future<raft::LogIndex> leaderReadIndex(uint16_t vshard) = 0;
    // This node's current commit index for `vshard` (cheap, no quorum round) for
    // bounded-staleness freshness. MUST reject (throw) if this node is not the leader.
    virtual seastar::future<raft::LogIndex> leaderCommitIndex(uint16_t vshard) = 0;
};

}  // namespace timestar::data
