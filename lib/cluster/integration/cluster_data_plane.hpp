#pragma once

#include "../../config/timestar_config.hpp"  // ClusterConfig
#include "../../core/engine.hpp"
#include "../../http/http_query_handler.hpp"  // HttpQueryHandler, QueryResponse
#include "../data/dataplane_limits.hpp"       // kMaxOutboundFrameBytes (the D-31 assertions below)
#include "../data/dataplane_rpc.hpp"
#include "../data/node_query_coordinator.hpp"
#include "../data/node_write_router.hpp"
#include "../data/replicated_command.hpp"  // kWriteCommandFramingBytes
#include "../raft/raft_group.hpp"          // kMaxProposalBytes
#include "../raft/raft_rpc_transport.hpp"
#include "cluster_runtime.hpp"
#include "engine_local_store.hpp"
#include "replicated_data_plane.hpp"
#include "shard_raft_plane.hpp"

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

// ---------------------------------------------------------------------------
// RAFT TIMING POLICY for the replicated data plane, in ONE place, because the
// relationships between these numbers are load-bearing and two of them are enforceable
// only where the tick PERIOD is visible (debt D-20).
//
// The tick period used to be a bare `std::chrono::milliseconds(20)` argument at the
// `ShardRaftPlane::init` call site while every timeout beside it was expressed in TICKS,
// so every wall-clock claim about Raft timing -- including the one that makes the
// leader-transfer abandon window safe -- was a multiplication done in a comment.
inline constexpr std::chrono::milliseconds kRaftTickPeriod{20};
inline constexpr unsigned kRaftHeartbeatTicks = 25;                      // 500 ms
inline constexpr unsigned kRaftElectionTicksMin = 125;                   // 2.5 s
inline constexpr unsigned kRaftElectionTicksMax = 250;                   // 5 s (randomized: spreads campaigns)
inline constexpr unsigned kRaftTransferTicks = 2 * kRaftHeartbeatTicks;  // 1 s (§3.10 abandon window)

constexpr std::chrono::milliseconds raftTicksToWallClock(unsigned ticks) {
    return kRaftTickPeriod * static_cast<int64_t>(ticks);
}

// THE INEQUALITY D-20 RESTS ON, asserted by the compiler rather than by prose. While a
// transfer is in flight the group refuses EVERY proposal, so the abandon window is what a
// mis-aimed transfer costs the group's writes -- and the whole point of shortening it from
// one election timeout is that a blocked batch can now outlast it and retry into the
// resumed leader INSIDE its base deadline. That is a wall-clock relationship between a
// tick count here and a millisecond constant in replicated_write_router.hpp, so nothing in
// either file could see it: dropping kDeadline to 800 ms, or handing the plane a 40 ms
// tick, silently restored the pre-D-20 failure mode with every test still green.
static_assert(raftTicksToWallClock(kRaftTransferTicks) < data::ReplicatedBatchWriteRouter::kDeadline,
              "the leader-transfer abandon window must fit inside the BASE write deadline, or one mis-aimed "
              "transfer is again a failed batch rather than a retry [debt D-20]");
// ...and it must still be long enough for a healthy peer to answer on the cadence it is
// served on, or a transfer is abandoned before its target has had one chance to ack.
static_assert(kRaftTransferTicks >= kRaftHeartbeatTicks,
              "the abandon window must cover at least one heartbeat round [debt D-20]");
// The ordering the group's own clocks need: abandoning must be far cheaper than an
// election, never a substitute for one.
static_assert(kRaftTransferTicks < kRaftElectionTicksMin,
              "an abandon window as long as an election is the pre-D-20 behaviour under a new name [debt D-20]");
static_assert(kRaftHeartbeatTicks < kRaftElectionTicksMin, "a leader must heartbeat many times per election");

// ---------------------------------------------------------------------------
// HOW LONG A READ WAITS FOR A LEADER, and why it is deliberately far less than a write
// waits (debt D-26; the decision and its alternatives are ADR 0006).
//
// A replicated read cannot be answered for a VShard with no elected leader, so it sleeps
// and re-gathers. THE POLICY IS: a read rides out a leadership TRANSFER -- the routine,
// planned, sub-second event the balancer causes continuously -- and deliberately does NOT
// ride out an ELECTION, which the write path does wait for (`kElectionDeadline`, 6 s, debt
// D-14). A failed read is idempotent and cheap to re-issue and its caller usually wants an
// answer or an error promptly; a failed write costs the client a whole re-submitted batch.
//
// Before D-26 these lived as function-local `static constexpr`s inside queryReplicated at
// 4 x 25 ms = 100 ms -- 4% of the SHORTEST election, and under the 1 s abandon window a
// mis-aimed transfer can cost, so the read did not reliably survive the one event its own
// comment claimed it was sized for. They are here, named, and asserted against the Raft
// clocks and the write window for the D-20 reason: the relationships are load-bearing and
// no single file could see them.
inline constexpr std::chrono::milliseconds kReadLeaderRetryDelay{50};
inline constexpr unsigned kReadLeaderRetries = 24;
inline constexpr std::chrono::milliseconds kReadLeaderlessBudget =
    kReadLeaderRetryDelay * static_cast<int64_t>(kReadLeaderRetries);  // 1.2 s
// Rounds spent following a REDIRECT rather than waiting. A separate budget because it is a
// different event: a redirect is progress (re-ask immediately, no sleep) where a leaderless
// retry is a wait, and charging redirects to the wait budget let the ordinary RF < N
// cold-cache case spend its election tolerance before any election mattered.
inline constexpr int kReadRedirectRounds = 2;
// At RF=3, two alternates are enough to try every placement replica after the
// first target fails. Separate from redirects: resolving leadership and losing a
// transport are distinct events and must not consume each other's progress budget.
inline constexpr int kReadReplicaFallbackRounds = 2;

static_assert(kReadLeaderlessBudget >= raftTicksToWallClock(kRaftTransferTicks),
              "a read must outlast the leader-transfer abandon window, or routine rebalancing is user-visible "
              "read errors -- which is the failure this budget was introduced for [debt D-26]");
static_assert(kReadLeaderlessBudget < raftTicksToWallClock(kRaftElectionTicksMin),
              "a read must NOT sit through an election: that is the write path's trade, taken because a lost "
              "batch is expensive, and a read that waits as long has stopped being the cheap-to-retry half "
              "[debt D-26, ADR 0006]");
static_assert(kReadLeaderlessBudget < data::ReplicatedBatchWriteRouter::kElectionDeadline,
              "the read/write election asymmetry is a DECISION and must keep its direction; inverting it "
              "silently would make reads the slow half [debt D-26, ADR 0006]");

// ---------------------------------------------------------------------------
// THE MESSAGE-SIZE CHAIN, asserted where both ends of it are visible (debt D-31).
//
// raft_types.hpp states the chain and can assert the links INSIDE it, but its top link --
// "the largest producer payload is a write slice, and a write slice is bounded by what the
// data plane will carry" -- reaches into data/dataplane_limits.hpp, which the deterministic
// Raft core must not include. This header includes both, which is why the join lives here
// (the same reason the tick-period assertions above do).
// IN THE CHARGE UNIT, NOT IN RAW BYTES (review F1). The refusal this guards
// (`firstUnproposableSlice`) compares `maxEncodedBytes(slice) + framing` against the
// proposal bound -- a CHARGE, up to 11/9 of a v1 encoding, because the journal gate's
// format version is independent of the one the frame arrived in. The first version of this
// assertion compared raw frame bytes, which left it unable to fire on the very mismatch it
// exists to catch: at a 12 MiB proposal bound it passed with ~1.3 MiB of apparent headroom
// while a maximal float frame cleared the real bound by ONE byte and a maximal boolean
// frame was refused outright.
static_assert(data::chargeCeilingForV1Bytes(data::kMaxOutboundFrameBytes) + data::kWriteCommandFramingBytes <=
                  raft::RaftGroup::kMaxProposalBytes,
              "a write frame the data plane will SEND must be proposable as a Raft entry AS CHARGED, or a "
              "legitimate forwarded batch draws a terminal 413 [debt D-31]");
// ...and the proposal bound must not be so far above it that it has stopped being derived
// from anything. Two frames' worth is the slack budget; more than that is the pre-D-31
// state, where the number floated free of every producer.
static_assert(raft::RaftGroup::kMaxProposalBytes < 2 * data::kMaxOutboundFrameBytes,
              "the Raft entry bound is meant to be the wire bound plus margin, not an independent opinion "
              "[debt D-31]");
// (The chain's last link -- send refusal vs the peer's RAFT inbound admission -- is
// asserted in raft_types.hpp, which owns both constants. It used to be stated HERE against
// `data::kMaxInboundRpcMemory`, the DATA plane's 128 MiB budget, which never admits a Raft
// frame at all: a comparison that could not fail and would not have meant anything if it
// had. Review F3.)

// The node-level composition that wires every M2 brick into one live service
// (integration plan M2): ClusterRuntime placement -> EngineLocalStore sink ->
// DataPlaneRpc transport (server + peer clients) -> NodeWriteRouter (writes) +
// NodeQueryCoordinator (queries). The server starts ONE of these (on a single
// shard) when [cluster].enabled; the HTTP handlers route partitioned writes/queries
// through write()/query(). Lives on one shard; cross-shard handlers reach it via the
// server's global accessor + invoke_on.
class ClusterDataPlane : public data::ProposeSink {
public:
    ClusterDataPlane() = default;
    ClusterDataPlane(const ClusterDataPlane&) = delete;
    ClusterDataPlane& operator=(const ClusterDataPlane&) = delete;

    // Build placement from cfg, start the RPC server bound to this node's data-plane
    // address, and register every peer. `engines` must outlive this service. Throws
    // on a misconfigured cluster (fail-closed).
    seastar::future<> start(const ClusterConfig& cfg, seastar::sharded<Engine>& engines);
    seastar::future<> stop();

    // Peer-facing transport settings. MUST be set before start(), which is what pushes
    // them to EVERY data-plane transport this node owns -- in replicated mode that is
    // the per-shard listeners and peer clients, not just this object's own (client-only)
    // instance. Configuring one instance and not the others is the failure that matters:
    // shard-0 clients would speak TLS while every per-shard listener and the whole write
    // path stayed plaintext -- queries fail loudly, writes go silently unencrypted.
    void setTlsCredentials(DataPlaneTls creds) { tls_ = std::move(creds); }
    void setJournalIdentity(JournalIdentity identity) { journalIdentity_ = identity; }
    void setLocalVersion(features::VersionRange range) { localVersion_ = range; }
    // What this NODE advertises to peers (pushed to every per-shard transport in
    // start()). Exposed so a test can read the real value back instead of grepping for a
    // literal: a grep cannot see a regression in DataPlaneRpc's own default, a third
    // advertiser, or a setLocalVersion() call that overrides the initializer.
    features::VersionRange localVersion() const { return localVersion_; }

    // Route a partitioned write to VShard owners (local applied directly, remote
    // forwarded), and a query fanned out to owners then merged to the single-node
    // answer.
    seastar::future<> write(data::WriteBatch batch);
    seastar::future<QueryResponse> query(QueryRequest request);

    // The REPLICATED write entry point, callable from ANY shard -- the HTTP request
    // shard calls it directly instead of shipping the whole batch to shard 0.
    //
    // It touches only `shards_`, a seastar::sharded<> whose instance table is fixed
    // once start() has returned (and start() completes before the HTTP server accepts),
    // so there is no shard-0-owned mutable state on this path. The request shard splits
    // the batch by owning shard and dispatches every slice concurrently; the owning
    // shard resolves the VShard's current leader and, when that leader is remote,
    // forwards from ITS OWN peer client. Nothing rendezvouses on shard 0.
    //
    // PRECONDITION: replicated mode (replication_factor > 1). RF=1 still routes through
    // write() on shard 0.
    seastar::future<> writeFromShard(data::WriteBatch batch);
    seastar::future<> deleteRangeFromShard(std::string seriesKey, uint64_t startTime, uint64_t endTime,
                                           SeriesId128 operationId);
    // Expand a pattern against a placement-epoch-pinned, quorum-fenced catalog
    // view. No mutation is proposed by this method.
    seastar::future<std::vector<std::string>> findPatternSeries(data::PatternSeriesSelector selector,
                                                                uint32_t maxSeries);

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
        size_t unresolvedPeerCount = 0;
        // Replicated mode only (all zero otherwise):
        size_t vshardsHostedHere = 0;  // Raft groups this node replicates
        size_t vshardsLedHere = 0;     // of those, ones this node currently leads
        size_t vshardsLeaderless = 0;  // VShards with NO elected leader anywhere
        // Among the VShards this node LEADS, how many each peer has fully replicated
        // (matchIndex >= our lastIndex). A peer stuck near 0 here is not acking our
        // appends -- which also silently blocks leadership transfer to it, since
        // RaftNode::transferLeadership only sends TimeoutNow to a caught-up target.
        std::map<NodeId, size_t> peerCaughtUp;
        // Committed-but-unapplied entries across every group this node hosts (debt
        // D-36), and how many groups they are spread over. An acknowledged write is
        // durable the moment it commits, but it is only READABLE once it is applied, so
        // a non-zero applyLagEntries is exactly the set of promises this node is
        // currently unable to keep. It is the number that tells a restart still catching
        // up apart from data that is gone.
        uint64_t applyLagEntries = 0;
        size_t applyGroupsBehind = 0;
        // Applies that threw, ticks that threw, and the groups those throwing ticks
        // never reached. A stalled apply retries, so these are stall/serialization
        // signals rather than loss ones -- but a group starved of ticks makes no
        // progress at all, which is what turns a transient apply failure into a read
        // that stays short.
        uint64_t applyFailures = 0;
        uint64_t tickErrors = 0;
        // Raft-log snapshot/compaction (debt D-5/D-6). `snapshotsTaken` rising is how an
        // operator sees that log compaction is actually running; `snapshotChunksSent` /
        // `snapshotsInstalled` are what distinguish a catch-up that went through the
        // SNAPSHOT path from one that went through ordinary appends, which nothing else can
        // tell apart. `snapshotsRefusedTooLarge` + `snapshotsUndeliverable` are the two
        // ways a VShard ends up with a permanently growing log.
        uint64_t snapshotsTaken = 0;
        uint64_t snapshotsRefusedTooLarge = 0;
        // Sweeps that picked a group and found it had no FLUSHED data to snapshot. Steady
        // non-zero with snapshotsTaken == 0 means the trigger is correctly declining rather
        // than broken -- the FIRST thing to check when logs are not being compacted.
        uint64_t snapshotsSkippedUnflushed = 0;
        uint64_t snapshotsSkippedPendingConversion = 0;
        uint64_t snapshotSweeps = 0;
        uint64_t snapshotMaxEntriesSince = 0;
        uint64_t snapshotChunksSent = 0;
        uint64_t snapshotsInstalled = 0;
        uint64_t snapshotsUndeliverable = 0;
        uint64_t snapshotTransfersRestarted = 0;
        uint64_t snapshotTransfersAbandoned = 0;
        size_t snapshotProductionLimitPerShard = 0;
        bool snapshotTriggerEnabled = false;
        // Raft journal fsync accounting (debt D-10). journalSyncRequests /
        // journalFsyncs is the coalescing factor -- 1.0 for the default per-VShard
        // journal, > 1 when the shared per-shard journal is enabled.
        uint64_t journalFsyncs = 0;
        uint64_t journalSyncRequests = 0;
        uint64_t journalSegmentsDeleted = 0;
        uint64_t journalSegmentsPinnedLastPass = 0;
        uint64_t journalRecordsCopiedForward = 0;
        uint64_t journalGcPasses = 0;
        bool journalShared = false;

        [[nodiscard]] bool readyForTraffic() const {
            if (unresolvedPeerCount != 0)
                return false;
            if (!replicated)
                return true;
            return vshardsHostedHere != 0 && vshardsLeaderless == 0 && applyLagEntries == 0 && applyFailures == 0 &&
                   tickErrors == 0 && snapshotTriggerEnabled && snapshotsRefusedTooLarge == 0 &&
                   snapshotsUndeliverable == 0;
        }

        [[nodiscard]] std::string readinessReason() const {
            if (unresolvedPeerCount != 0)
                return std::to_string(unresolvedPeerCount) + " configured peer(s) are unresolved";
            if (!replicated)
                return {};
            if (vshardsHostedHere == 0)
                return "this replicated node hosts no VShards";
            if (vshardsLeaderless != 0)
                return std::to_string(vshardsLeaderless) + " hosted VShard(s) have no elected leader";
            if (applyFailures != 0)
                return std::to_string(applyFailures) + " Raft apply failure(s) require operator investigation";
            if (tickErrors != 0)
                return std::to_string(tickErrors) + " Raft tick error(s) require operator investigation";
            if (applyLagEntries != 0)
                return std::to_string(applyLagEntries) + " committed Raft entrie(s) are not applied";
            if (!snapshotTriggerEnabled)
                return "Raft snapshot production is disabled";
            if (snapshotsRefusedTooLarge != 0)
                return std::to_string(snapshotsRefusedTooLarge) +
                       " VShard snapshot(s) exceeded the safe in-memory size bound";
            if (snapshotsUndeliverable != 0)
                return std::to_string(snapshotsUndeliverable) + " VShard snapshot transfer(s) were undeliverable";
            return {};
        }
    };
    seastar::future<Status> status() const;

    // Fail startup before accepting traffic when the configured reactor count
    // cannot produce complete single-core VShard snapshots.
    static void validateCoreTopology(unsigned coreCount, uint16_t replicationFactor);

    // Operator action (integration plan M5 leadership balancing, which is also v1's
    // READ balancing since reads go to leaders). Hands leadership of up to
    // `maxTransfers` VShards this node leads beyond its fair share to peer replicas
    // that are under it. Bounded per call so an operator request never runs long; call
    // repeatedly to converge. Returns the number of transfers initiated.
    //
    // This matters in practice: the first node to start wins every election, so a
    // fresh cluster puts ALL write coordination on one node until this runs.
    seastar::future<size_t> rebalanceLeadership(size_t maxTransfers);

    // ProposeSink: a peer forwarded a batch for VShards THIS node leads. Split it by
    // the shard owning each VShard's Raft group and replicate each slice there.
    // NOTE: in replicated mode the peer-facing listeners are the PER-SHARD ones, whose
    // sink is ShardRaftPlane itself (same split, from whichever shard accepted the
    // connection); this instance is client-only there. Kept as the composition's
    // ProposeSink for tests and for any single-instance embedding.
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override;

private:
    // The implementation is split from start() so the public entry point can
    // unwind a partially-started sharded plane before rethrowing.  Seastar's
    // sharded<> destructor deliberately traps when start() succeeded but
    // stop() was skipped, so exception safety here is part of the startup
    // contract rather than optional tidiness.
    seastar::future<> startImpl(const ClusterConfig& cfg, seastar::sharded<Engine>& engines);

    // RF=3 leader read: fan out per-VShard-leader (see .cpp).
    seastar::future<QueryResponse> queryReplicated(QueryRequest request);
    // vshard -> current leader, gathered from every shard (groups live across cores).
    // Contains ONLY the VShards this node HOSTS: a missing key means "not ours, ask the
    // placement directory", a present kNoNode means "ours, and leaderless" (debt D-13).
    seastar::future<std::map<uint16_t, data::NodeId>> gatherLeaders() const;

    // Leaders learned from a peer's read REDIRECT, for VShards this node does not host
    // and therefore cannot resolve itself (debt D-13). A hit saves a redirect round trip;
    // a hint that is merely WRONG costs one and is corrected by the redirect it provokes.
    // It is never consulted for a VShard we DO host -- there the live Raft view is
    // authoritative and re-read on every attempt, which is what keeps a cached hint from
    // surviving a leadership transfer.
    //
    // IT IS NOT A PURE OPTIMISATION, and calling it one is how the review found a
    // permanent-outage bug here. A hint whose target is UNREACHABLE cannot be corrected by
    // a reply -- a dead node does not send one -- so it has to be invalidated explicitly
    // when a read fails against it (`data::applyReadTargetUnreachable`, called from
    // queryReplicated's catch). Without that the cache pins every subsequent read to a
    // corpse: the placement map is immutable and there is no TTL, so the query fails
    // QUERY_INCOMPLETE naming the dead node forever. Pinned by
    // ReadRoundBookkeeping.AnUnreachableTargetsHintsAreForgottenSoTheNextRoundReResolves.
    //
    // Bounded by VIRTUAL_SHARD_COUNT entries. ClusterDataPlane lives on one shard, so
    // concurrent queries mutate this from one reactor thread only.
    std::map<uint16_t, data::NodeId> readLeaderHints_;

    // Peer registration (write-scaleout 4b-iii). ONE DNS resolution per peer feeds EVERY
    // plane that needs it; a peer that fails to resolve is retried until it does, instead
    // of being a permanent, silent hole. See the .cpp for what the two independent
    // try-guarded loops these replace could get wrong.
    // `addr` BY VALUE: this coroutine suspends in DNS, and every caller passes a reference
    // into a member map that the same pass rewrites.
    seastar::future<bool> registerPeer(NodeId id, std::string addr, bool replicated);
    seastar::future<> registerAllPeers(bool replicated);
    // One re-resolution pass. Named (not a lambda-coroutine) so its captures live in a
    // coroutine frame the gate keeps alive -- see startPeerResolver.
    seastar::future<> resolvePendingPeers(bool replicated);
    void startPeerResolver(bool replicated);

    std::optional<ClusterRuntime> rt_;
    seastar::sharded<Engine>* enginesPtr_ = nullptr;
    // Applied to this object's transport AND to every per-shard transport in start().
    std::optional<DataPlaneTls> tls_;
    std::optional<JournalIdentity> journalIdentity_;
    // Everything this binary can read and write. Pushed to every per-shard transport
    // in start(), so peers negotiate against the node's REAL capability; leaving it at
    // the VersionRange default {1,1} would pin the whole cluster to the v1 wire format
    // no matter what the binaries support.
    //
    // It MUST track the newest version this binary supports, not a literal that happens
    // to be current. Spelling v2 here after v3 landed silently capped every negotiation
    // at 2, so no peer ever spoke the hinted-propose verb and the whole 3a leader-hint
    // path was dead in production while every unit and socket test passed (they build
    // their own DataPlaneRpc, whose own default was already v3). The 5-node
    // deposed-primary gate is what caught it. Track kWriteBatchFormatMax.
    features::VersionRange localVersion_{1, data::kWriteBatchFormatMax};
    // Declared in dependency order: deps before the router/coordinator that reference
    // them, so destruction (reverse order) tears the referrers down first.
    std::unique_ptr<data::VShardDirectory> dir_;
    std::unique_ptr<EngineLocalStore> local_;
    std::unique_ptr<data::DataPlaneRpc> rpc_;
    std::unique_ptr<http::HttpQueryHandler> finalizer_;
    std::unique_ptr<data::NodeWriteRouter> router_;
    std::unique_ptr<data::NodeQueryCoordinator> coord_;
    // RF=3 replicated path (replication_factor > 1); null in the RF=1/M2 mode.
    // Declared LAST so rdp_ (which borrows rpc_, dir_, local_) tears
    // down first.
    // Per-shard Raft planes: shard S owns the VShards with assignCore(vs)==S, so the
    // group tick/step/apply work is spread over all cores instead of saturating
    // shard 0. Each plane also owns its OWN Raft and data-plane transports (listener +
    // peer clients). No DATA-plane byte transits shard 0 any more; inbound RAFT traffic
    // still does, because that listener pins itself to its shard and reuseport is
    // disabled in this seastar (see shard_raft_plane.hpp's header note) -- egress is
    // per-shard for both.
    seastar::sharded<ShardRaftPlane> shards_;
    bool shardsStarted_ = false;
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

    // Peers whose address did not resolve yet (rolling start, DNS not populated). Retried
    // by peerResolveTimer_ until empty, at which point the timer cancels itself.
    std::map<NodeId, std::string> unresolvedPeers_;
    seastar::timer<> peerResolveTimer_;
    seastar::gate peerResolveGate_;
    bool peerResolveRunning_ = false;
};

}  // namespace timestar::cluster
