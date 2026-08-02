#pragma once

#include "../raft/raft_types.hpp"  // NodeId
#include "node_metadata.hpp"
#include "node_query.hpp"
#include "pattern_series.hpp"
#include "replicated_command.hpp"
#include "write_errors.hpp"
#include "write_record.hpp"

#include <cstdint>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/rpc/rpc_types.hh>  // rpc_clock_type
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::data {

using timestar::raft::NodeId;

// See dataplane_rpc.hpp -- declared here too so NodeTransport can name it without
// depending on the concrete transport.
using OptDeadline = std::optional<seastar::rpc::rpc_clock_type::time_point>;

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

// The result of proposing a set of VShard slices.
//
// THE CONTRACT IS A COMMITTED-SET, NOT A REJECT-SET, and that is a correctness
// requirement rather than a style choice. `committed == true` means every slice in the
// view durably committed on quorum. Otherwise the caller must treat EVERY VShard in the
// view it dispatched as UNCOMMITTED except those named in `committedVShards` -- it must
// NOT derive the uncommitted set by subtracting `rejects`.
//
// Why: a sink can legitimately answer with a reject list that is a STRICT SUBSET of what
// it failed to commit. ReplicatedVShardHost's membership check does exactly that -- it
// finds one group it does not host, proposes NOTHING, and names only that group -- so a
// caller subtracting rejects would ack every other slice in the batch having never
// replicated it. That is ack-without-commit: silent data loss, and it is REACHABLE, since
// ClusterDataPlane::start opens the data-plane listener before instantiating the groups,
// so a joining or restarting node serves proposes for seconds while hosting only some of
// them. Inverting the contract also disarms the hostile/buggy-peer variant, where a reply
// names VShards that were never in the view.
//
// `rejects` therefore carries only ADVISORY routing information -- who leads a slice now,
// and why it failed -- never the authoritative set.
struct ProposeOutcome {
    bool committed = false;
    // AUTHORITATIVE: exactly the VShards that durably committed on quorum. Ignored when
    // `committed` is true (which means "all of them").
    std::vector<uint16_t> committedVShards;
    // ADVISORY: hints and reasons. May be empty, partial, or (from a hostile peer) name
    // VShards outside the view. Never used to decide what was committed.
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

    // Expand a delete selector against only the named VShards. The production
    // store quorum-fences this read before scanning its NativeIndex ranges.
    virtual seastar::future<PatternSeriesResult> findPatternSeries(PatternSeriesRequest) {
        return seastar::make_exception_future<PatternSeriesResult>(
            std::logic_error("node store does not implement quorum-fenced pattern-series discovery"));
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

    virtual seastar::future<PatternSeriesResult> findPatternSeries(NodeId, PatternSeriesRequest,
                                                                   OptDeadline = std::nullopt) {
        return seastar::make_exception_future<PatternSeriesResult>(
            std::logic_error("node transport does not implement quorum-fenced pattern-series discovery"));
    }

    // Forward a WriteBatch to a peer that LEADS the batch's VShards, to be
    // REPLICATED through Raft there (M3 RF=3). Resolves true on durable quorum commit,
    // false if the peer is not the leader (caller redirects). Default false so doubles
    // need not implement it.
    virtual seastar::future<bool> proposeWrite(NodeId, WriteBatch) { return seastar::make_ready_future<bool>(false); }

    // proposeWrite with LEADER HINTS and without consuming the caller's groups
    // (write-scaleout 3a/3b) -- the production remote path. `view` borrows groups the
    // CALLER owns and must outlive the returned future (see VShardBatchView).
    //
    // The default forwards to proposeWrite, copying the view into a WriteBatch and
    // reporting hintless rejects, so an in-memory double that only implements
    // proposeWrite keeps working (and so does a peer too old to answer the hinted verb).
    // `deadline` bounds the whole call (handshake included); std::nullopt means no
    // bound. The in-memory default ignores it -- a double answers instantly -- but the
    // real transport MUST honour it: the router only checks its budget BETWEEN attempts,
    // so an unbounded attempt holds its in-flight-bytes charge for as long as a peer
    // cares to stay silent, and every other write on that shard queues behind it.
    virtual seastar::future<ProposeOutcome> proposeWriteHinted(NodeId to, VShardBatchView view,
                                                               OptDeadline deadline = std::nullopt) {
        (void)deadline;
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
            // A bool answer cannot name a partial commit, so on failure NOTHING is
            // reported committed and the caller retries the whole view -- which is the
            // safe reading, and the only one available from a pre-v3 peer.
            if (ok)
                out.committedVShards = vshards;
            else
                for (uint16_t vs : vshards)
                    out.rejects.push_back(SliceReject{vs, kNoNode, WriteFailure::NotLeader});
            return out;
        });
    }

    // Forward one non-batch replicated command to the node believed to lead its
    // VShard. The one-slice ProposeOutcome has the same committed-set contract as
    // the write path, including an advisory corrected-leader hint on rejection.
    // The default is a clean not-leader response so existing test transports stay
    // source-compatible while callers still fail closed and retry elsewhere.
    virtual seastar::future<ProposeOutcome> proposeCommandHinted(NodeId, uint16_t vshard, ReplicatedCommand,
                                                                 OptDeadline = std::nullopt) {
        ProposeOutcome out;
        out.rejects.push_back(SliceReject{vshard, kNoNode, WriteFailure::NotLeader});
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
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
    // `deadline` bounds the propose (std::nullopt == unbounded). It is NOT optional in
    // practice on the coordinator path: a locally-led VShard that has lost quorum can
    // suspend a Raft waiter forever (see RaftGroup::proposeAndAwaitApplied), and the
    // router's between-attempt deadline cannot rescue an attempt that never returns.
    // A peer this node was routing writes to is UNREACHABLE; un-hibernate the local
    // groups that still believe it leads them, so they campaign at the normal election
    // timeout instead of the hibernation-stretched one (debt D-14).
    //
    // Idle followers tick at 1-in-10 (RaftGroupRegistry's hibernation), which stretches a
    // 2.5-5 s election timeout to 25-50 s -- MEASURED, and it is why the read path already
    // wakes groups behind an unreachable leader before answering QUERY_INCOMPLETE. The
    // write path had no equivalent, so after a node was killed its groups sat hibernating
    // and every batch touching one failed for tens of seconds rather than for the length
    // of an election. Returns the number woken (0 for a sink with no Raft groups, which is
    // why the default is a no-op rather than pure virtual).
    virtual size_t wakeGroupsLedBy(NodeId) { return 0; }

    virtual seastar::future<ProposeOutcome> proposeVShardBatchesHinted(VShardBatchView view,
                                                                       OptDeadline deadline = std::nullopt) {
        (void)deadline;
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
            if (ok)
                out.committedVShards = vshards;
            else
                for (uint16_t vs : vshards)
                    out.rejects.push_back(SliceReject{vs, kNoNode, WriteFailure::NotLeader});
            return out;
        });
    }

    // The peer-ingress entry for the hinted verb: a forwarded batch arrives flat, so it
    // is split here and answered per VShard. The default splits and delegates; the
    // per-shard plane overrides it to fan the slices out to their owning shards.
    //
    // BYTE ADMISSION: peer ingress IS bounded now (debt D-8) -- charged against its own
    // per-shard budget in `proposeSlicesToOwningShards{,Hinted}`, on the shard serving the
    // connection. That covers the per-shard plane's override; this DEFAULT reaches the
    // charge too, since it delegates to `proposeVShardBatches` -> `proposeBatch`, which the
    // plane implements over the same guarded helper. An override that bypasses both would
    // be unbounded, so keep the charge in the helpers rather than at a call site.
    //
    // TIME: the forwarding node bounds its RPC, but that cannot cancel a receiver
    // coroutine after timeout/disconnect. Concrete production sinks must apply a
    // receiver-side default when this compatibility seam supplies nullopt.
    virtual seastar::future<ProposeOutcome> proposeBatchHinted(WriteBatch batch) {
        return seastar::do_with(splitByVShard(std::move(batch)), [this](VShardBatches& groups) {
            return proposeVShardBatchesHinted(viewOf(groups), std::nullopt);
        });
    }

    // The single-VShard command analogue of proposeVShardBatchesHinted. Deletes
    // and future replicated maintenance mutations use this path so their exact
    // command bytes, leader correction, and deadline semantics do not get
    // smuggled through a synthetic WriteBatch.
    virtual seastar::future<ProposeOutcome> proposeCommandHinted(uint16_t vshard, ReplicatedCommand,
                                                                 OptDeadline = std::nullopt) {
        ProposeOutcome out;
        out.rejects.push_back(SliceReject{vshard, kNoNode, WriteFailure::NotLeader});
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
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
