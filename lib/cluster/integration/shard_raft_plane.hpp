#pragma once

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"           // assignCore
#include "../../utils/logger.hpp"          // timestar::http_log
#include "../data/dataplane_rpc.hpp"
#include "../data/leader_filtered_node_store.hpp"
#include "../data/node_store.hpp"
#include "../data/vshard_directory.hpp"  // VShardDirectory::groupOf (debt D-11)
#include "../features/feature_gate.hpp"  // VersionRange
#include "../raft/raft_codec.hpp"        // decodeEnvelope
#include "../raft/raft_driver.hpp"       // RaftTransport
#include "../raft/raft_rpc_transport.hpp"
#include "engine_local_store.hpp"
#include "replicated_data_plane.hpp"
#include "write_admission.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <string>
#include <utility>
#include <vector>

namespace timestar::cluster {

// Per-VShard Raft groups are spread ACROSS CORES: shard S owns exactly the VShards
// with assignCore(vshard, smp::count) == S. Previously every group lived on shard 0,
// so one reactor ran all 4096 groups' ticks, heartbeat steps and state-machine
// applies -- enough to saturate it, diverge that node's leadership view and make its
// queries fail. Spreading them divides that work by the core count.
//
// BOTH inter-node transports are now PER SHARD: every shard listens on the node's
// single Raft port AND on its single data-plane port, and keeps its own peer clients
// for each. Previously one transport of each kind lived on shard 0 and carried the
// whole node's traffic, which made shard 0 the write bottleneck -- it showed ~10x the
// journal-sync latency of the other shards while all cores sat ~25% idle. Egress is now
// entirely local; an inbound Raft envelope still hops to the shard owning its group,
// and an inbound proposeWrite is split across the shards owning its VShards.
//
// The per-shard listen is NOT SO_REUSEPORT, despite what the surrounding comments used
// to claim: this seastar hardcodes posix_reuseport_available() to false, so shard 0
// owns the one real socket and each accepted fd is handed to the shard the listen
// options name. BOTH listeners therefore ask for connection_distribution (the
// perShardListener flag on DataPlaneRpc::start and RaftRpcTransport::start). The Raft
// listener used to pin itself per shard, so shard 0 accepted and read ALL inbound Raft
// traffic and ran every peek/route hop while shards 1..N held accept promises that
// never resolved -- invisible rather than fatal only because Raft's verb is no_wait, so
// nothing hung waiting on a reply.

// The shard that owns a RAFT GROUP. This is the one genuine integer op in the chain:
// a group id is what the transport peeks off the wire and what the registry is keyed
// by, so there is nothing to look up here.
//
// It takes a GROUP id, not a VShard id (debt D-11). Everything that starts from a
// VShard must go through `shardOwningVShard` below, which asks the directory which
// group replicates it. Today those two are the same number for every VShard; the
// distinction is what lets a later ADR-0004 consolidation map several VShards onto one
// group without every routing site being rewritten (and, worse, some of them missed).
inline unsigned shardForGroup(uint16_t groupId) {
    return timestar::assignCore(timestar::VShardId{groupId}, seastar::smp::count);
}

// The shard that owns the Raft group replicating `vshard`.
//
// `dir` is optional only because a handful of unit tests exercise the shard fan-out
// with no control map at all; a null directory means the identity mapping, which is
// what those tests (and every production cluster today) run. Production call sites
// pass the real directory -- it is immutable after start() and already shared with
// every shard's router.
inline unsigned shardOwningVShard(uint16_t vshard, const data::VShardDirectory* dir) {
    return shardForGroup(dir ? dir->groupOf(vshard) : vshard);
}

// Bucket VShard-split groups by the shard owning each one's Raft group. Every write and
// propose fan-out funnels through THIS function rather than open-coding the map insert,
// so a non-identity VShard->group mapping is honoured by all of them or by none -- which
// is the failure mode a prep step like D-11 exists to make impossible. Consumes `groups`.
inline std::map<unsigned, data::VShardBatches> bucketByOwningShard(data::VShardBatches groups,
                                                                   const data::VShardDirectory* dir) {
    std::map<unsigned, data::VShardBatches> byShard;
    for (auto& g : groups)
        byShard[shardOwningVShard(g.first, dir)].push_back(std::move(g));
    return byShard;
}

// Raised when a slice is routed to a shard whose plane has already been torn down
// (shutdown in progress). It is a RETRYABLE condition -- the write never reached Raft,
// so nothing was committed and the caller may safely re-route it -- and it must be an
// error rather than a silent drop, or a shutting-down node would ack writes it discarded.
inline constexpr const char* kShardStoppingError = "cluster: shard data plane is stopping; retry this write";
// ... raised as data::ShardStoppingError, NOT a bare runtime_error: every consumer that
// has to recognise it (the failure classifier, the HTTP status mapping, and the batch
// path that previously reported it as a 200 "partial") matches on the TYPE.

// Mutual-TLS material for the per-shard data-plane transports. Set on ClusterDataPlane
// before start(); it is pushed to EVERY shard's DataPlaneRpc, because in replicated
// mode those -- not ClusterDataPlane's own client-only instance -- are the node's
// listeners AND the write path's peer clients. Configuring only the shard-0 instance
// would leave the whole data plane plaintext while queries went TLS.
struct DataPlaneTls {
    std::string certPem;
    std::string keyPem;
    std::string caPem;
    std::string expectedPeerName;
};

// One shard's slice of the replicated data plane: a full ReplicatedDataPlane holding
// only the VShards this shard owns. Constructed on every shard via
// seastar::sharded<ShardRaftPlane>, so each reactor ticks only its own groups.
class ShardRaftPlane : public data::ProposeSink, public data::ReadIndexSink {
public:
    seastar::future<> init(seastar::sharded<Engine>* engines, seastar::sharded<ShardRaftPlane>* peers,
                           const data::VShardDirectory* dir, data::NodeId self, std::string journalRoot,
                           std::chrono::milliseconds tick) {
        store_ = std::make_unique<EngineLocalStore>(*engines);
        peers_ = peers;
        // Retained for VShard->group resolution (debt D-11). The directory outlives every
        // shard plane (ClusterDataPlane owns it and stops the planes first) and is
        // immutable after start(), which is the same access the routers already make.
        dir_ = dir;
        // What peers actually read through (debt D-13): the real store, wrapped so a
        // VShard the coordinator could not resolve itself is answered only if we LEAD
        // it and REDIRECTED otherwise. Inert unless the request names VShards to
        // resolve, which only an RF < N coordinator does.
        queryStore_ = std::make_unique<data::LeaderFilteredNodeStore>(
            *store_, self, [this](std::vector<uint16_t> vshards) { return resolveLeaders(std::move(vshards)); });
        transport_ = std::make_unique<raft::RaftRpcTransport>();
        // This shard's OWN data-plane transport: its listener on the node's data-plane
        // port and its own peer clients. Every remote leader forward this shard makes
        // now leaves from this core; nothing hops to shard 0.
        rpc_ = std::make_unique<data::DataPlaneRpc>();
        // Journals are per-VShard directories under this root, and each VShard belongs
        // to exactly one shard, so shards never contend for the same journal.
        plane_ = std::make_unique<ReplicatedDataPlane>(*store_, *transport_, *rpc_, *dir, self,
                                                       std::filesystem::path(journalRoot), tick);
        return seastar::make_ready_future<>();
    }

    // Listen for peer data-plane traffic on the node's data-plane port from THIS shard.
    // Inbound forwarded writes/queries are served by this shard's EngineLocalStore,
    // which dispatches to the owning engine core itself; an inbound proposeWrite is
    // split across the shards owning its VShards by proposeBatch() below, and an
    // inbound leaderReadIndex/leaderCommitIndex is routed to the VShard's owning shard
    // by the ReadIndexSink overrides.
    //
    // Every peer-facing SETTING must be applied here, not just to ClusterDataPlane's
    // instance: in replicated mode these are the node's only data-plane servers and the
    // write path's only peer clients.
    seastar::future<> startDataPlane(seastar::socket_address local, const std::optional<DataPlaneTls>& tls,
                                     features::VersionRange localVersion) {
        if (tls)
            rpc_->setTlsCredentials(tls->certPem, tls->keyPem, tls->caPem, tls->expectedPeerName);
        rpc_->setLocalVersion(localVersion);
        rpc_->setProposeSink(*this);
        rpc_->setReadIndexSink(*this);
        return rpc_->start(local, *queryStore_, /*perShardListener=*/true);
    }

    void addDataPeer(data::NodeId id, seastar::socket_address addr) { rpc_->addPeer(id, addr); }

    // ProposeSink: a peer forwarded a batch because this NODE leads those VShards. The
    // connection landed on whichever shard the kernel gave it, which is unrelated to
    // which shards own the VShards -- so split and replicate each slice on its owner.
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override;

    // The hinted twin (write-scaleout 3a): same fan-out, but each owning shard reports
    // per-VShard rejects with ITS group's current leader, so the forwarding coordinator
    // learns where the leadership actually went instead of only that it guessed wrong.
    seastar::future<data::ProposeOutcome> proposeBatchHinted(data::WriteBatch batch) override;

    // ReadIndexSink: a replica is confirming freshness at the leader (M4 replica reads).
    // Same story as proposeBatch -- the connection landed on an arbitrary shard, so hop
    // to the one that owns this VShard's group and answer from there. The host rejects
    // (throws) if this node is not the current-term leader, which is what the reaching
    // replica needs; that rejection propagates unchanged.
    seastar::future<raft::LogIndex> leaderReadIndex(uint16_t vshard) override;
    seastar::future<raft::LogIndex> leaderCommitIndex(uint16_t vshard) override;

    // Leadership of `vshards` as THIS NODE sees it (debt D-13). One entry per input,
    // carrying both halves of the question a coordinator cannot answer for a VShard it
    // does not host: do we hold a replica at all (`hosted`), and who leads it
    // (`leader`, kNoNode when no election has completed). Groups live on the shard that
    // owns them, so this hops to each owning shard -- at most smp::count hops however
    // many VShards are asked about.
    seastar::future<std::vector<data::VShardRedirect>> resolveLeaders(std::vector<uint16_t> vshards);

    // This shard's slice of the same question, answered locally (no hop). Public so the
    // fan-out above can invoke it on a sibling.
    std::vector<data::VShardRedirect> resolveLeadersLocal(const std::vector<uint16_t>& vshards) const {
        std::vector<data::VShardRedirect> out;
        out.reserve(vshards.size());
        if (!plane_)
            return out;
        auto& host = const_cast<ReplicatedDataPlane*>(plane_.get())->host();
        for (uint16_t vs : vshards) {
            data::VShardRedirect r;
            r.vshard = vs;
            r.hosted = host.hosts(vs);
            r.leader = r.hosted ? host.leaderOf(vs) : raft::kNoNode;
            out.push_back(r);
        }
        return out;
    }

    // Listen on the node's Raft port from THIS shard. Every shard calls listen() on the
    // same address with connection_distribution, so accepted connections spread over
    // the shards instead of piling onto shard 0 (reuseport is disabled here -- see the
    // file header); envelopes are then decoded on the shard owning the group.
    //
    // The raw deliver hook is installed BEFORE listening: a connection accepted between
    // start() and setRawDeliver would fall through to the no-op DeliverFn and drop its
    // envelope (Raft would retry, but there is no reason to allow the window).
    seastar::future<> startTransport(seastar::socket_address local) {
        transport_->setRawDeliver([this](uint16_t groupId, const char* bytes, size_t len) {
            // Already a GROUP id (the transport's 2-byte peek reads the envelope's
            // groupId), so this is an identity-free integer op and needs no directory.
            const unsigned owner = shardForGroup(groupId);
            if (owner == seastar::this_shard_id())
                return deliverDecoded(bytes, len);
            // The bytes stay owned by this shard; the target only READS them, and
            // submit_to is awaited, so they outlive the read.
            auto* peers = peers_;
            return seastar::smp::submit_to(owner,
                                           [peers, bytes, len] { return peers->local().deliverDecoded(bytes, len); });
        });
        co_await transport_->start(
            local, [](raft::Envelope) { return seastar::make_ready_future<>(); }, /*perShardListener=*/true);
        co_return;
    }

    void addRaftPeer(data::NodeId id, seastar::socket_address addr) { transport_->addPeer(id, addr); }

    // Decode on THIS shard and hand to the local group registry.
    seastar::future<> deliverDecoded(const char* bytes, size_t len) {
        auto env = raft::decodeEnvelope(std::string(bytes, len));
        if (!env || !plane_)
            return seastar::make_ready_future<>();
        return plane_->host().registry().deliver(std::move(*env));
    }

    // Un-hibernate this shard's groups that still believe `leader` leads them.
    size_t wakeFollowersOf(data::NodeId leader) {
        if (!plane_)
            return 0;
        return plane_->host().registry().wakeFollowersOf(leader);
    }

    // Add a VShard group -- only called on the shard that owns it.
    seastar::future<> addVShard(uint16_t vshard, std::vector<data::NodeId> voters, raft::RaftOptions opts) {
        return plane_->addVShard(vshard, std::move(voters), opts);
    }

    void startTicking() {
        if (plane_)
            plane_->startTicking();
    }

    // Override this shard's snapshot-trigger policy (debt D-6). Must be called BEFORE
    // startSnapshotTrigger.
    void setSnapshotPolicy(uint64_t entries, uint64_t bytes, std::chrono::seconds minInterval) {
        if (plane_)
            plane_->host().setSnapshotPolicy(entries, bytes, minInterval);
    }

    // Start this shard's background Raft-log snapshot/compaction trigger (debt D-6).
    // Called after every group on the shard exists, so the first sweep sees the real
    // group set rather than a partial one.
    void startSnapshotTrigger() {
        if (plane_)
            plane_->startSnapshotTrigger();
    }

    // This shard's snapshot-producer counters (debt D-6), for `/cluster/status` and for
    // the restart-catch-up gate -- which needs to prove a catch-up used the SNAPSHOT path
    // rather than ordinary appends, and cannot tell the two apart without a counter.
    struct SnapshotCounts {
        uint64_t taken = 0;             // snapshots produced + logs compacted here
        uint64_t refusedTooLarge = 0;   // over kMaxVShardSnapshotBytes: log kept
        uint64_t skippedUnflushed = 0;  // eligible, but the VShard has no FLUSHED data yet
        uint64_t skippedPendingConversion = 0;
        uint64_t sweeps = 0;  // sweep passes run (0 == the trigger is not running)
        uint64_t maxEntriesSinceSeen = 0;
        uint64_t chunksSent = 0;     // InstallSnapshot chunks put on the wire (leader)
        uint64_t installed = 0;      // snapshots installed as a follower
        uint64_t undeliverable = 0;  // snapshots this node declined to send
        uint64_t transfersRestarted = 0;
        uint64_t transfersAbandoned = 0;
        bool triggerEnabled = false;
    };

    // Journal fsync accounting (debt D-10). `syncRequests / fsyncs` is the
    // COALESCING FACTOR: 1.0 with a journal per VShard (each group syncs alone to
    // its own fd), > 1 with the shared per-shard journal. It is the only evidence
    // of D-10 that is measurable on a tmpfs box, where the elapsed-time win the
    // change is actually for is invisible by construction (plan 5.3).
    struct JournalCounts {
        uint64_t fsyncs = 0;
        uint64_t syncRequests = 0;
        bool shared = false;
    };

    JournalCounts journalCounts() const {
        JournalCounts c;
        if (!plane_)
            return c;
        auto& host = const_cast<ReplicatedDataPlane*>(plane_.get())->host();
        c.fsyncs = host.journalFsyncs();
        c.syncRequests = host.journalSyncRequests();
        c.shared = ReplicatedVShardHost::sharedJournalEnabled();
        return c;
    }

    SnapshotCounts snapshotCounts() const {
        SnapshotCounts c;
        if (!plane_)
            return c;
        auto& host = const_cast<ReplicatedDataPlane*>(plane_.get())->host();
        c.taken = host.snapshotsTaken();
        c.refusedTooLarge = host.snapshotsRefusedTooLarge();
        c.skippedUnflushed = host.snapshotsSkippedUnflushed();
        c.skippedPendingConversion = host.snapshotsSkippedPendingConversion();
        c.sweeps = host.snapshotSweeps();
        c.maxEntriesSinceSeen = host.snapshotMaxEntriesSinceSeen();
        c.triggerEnabled = host.snapshotTriggerEnabled();
        for (uint16_t vs = 0; vs <= timestar::VIRTUAL_SHARD_MASK; ++vs) {
            if (!host.hosts(vs))
                continue;
            raft::RaftGroup* g = host.group(vs);
            if (!g)
                continue;
            const auto& n = g->node();
            c.chunksSent += n.snapshotChunksSent();
            c.installed += n.snapshotsInstalled();
            c.undeliverable += n.undeliverableSnapshots();
            c.transfersRestarted += n.snapshotTransfersRestarted();
            c.transfersAbandoned += n.snapshotTransfersAbandoned();
        }
        return c;
    }

    ReplicatedDataPlane& plane() { return *plane_; }

    // Whether this shard's plane may still take work. `plane_ != nullptr` alone is too
    // weak: stop() tears the plane down only AFTER draining its RPC server, so between
    // the two there is a window in which the plane is still non-null but is about to
    // stop ticking -- a slice admitted there waits on a commit that will never be
    // driven, which shows up as a shutdown hang bounded only by the process's shutdown
    // timeout. `stopping_` closes that window at the very top of stop(). Work already
    // in flight is unaffected (it awaits a plane that keeps ticking until plane_->stop()
    // runs); only NEW slices are turned away, with the retryable kShardStoppingError.
    bool ready() const { return plane_ != nullptr && !stopping_; }

    // Peer proposals this shard's data-plane listener has SERVED (write-scaleout 1b/2):
    // it counts on the shard the connection was accepted on, not on the shards that own
    // the VShards, so it is the node's per-core inbound-ingress distribution. Pins the
    // production perShardListener=true: with the listener pinned instead, every peer
    // connection lands on shard 0 and this is 0 everywhere else.
    uint64_t inboundProposals() const { return inboundProposals_; }

    // This shard's slice of the cluster-wide leadership picture.
    struct Counts {
        size_t hosted = 0;
        size_t led = 0;
        size_t leaderless = 0;
        std::map<data::NodeId, size_t> peerCaughtUp;
    };

    Counts counts(data::NodeId self, const std::vector<data::NodeId>& peers) const {
        Counts c;
        if (!plane_)
            return c;
        auto& host = const_cast<ReplicatedDataPlane*>(plane_.get())->host();
        c.hosted = host.vshardCount();
        for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
            if (shardOwningVShard(vs, dir_) != seastar::this_shard_id())
                continue;  // not ours
            // A VShard we do not HOST is not leaderless, it is someone else's (debt
            // D-13). Counting it here is what made `vshards_leaderless` meaningless at
            // RF < N -- each node reported ~1638 of 4096 on a 5-node RF=3 cluster and
            // the gates had to refuse to look at it.
            if (!host.hosts(vs))
                continue;
            const data::NodeId leader = host.leaderOf(vs);
            if (leader == timestar::raft::kNoNode) {
                ++c.leaderless;
                continue;
            }
            if (leader != self)
                continue;
            ++c.led;
            raft::RaftGroup* g = host.group(vs);
            if (!g)
                continue;
            const auto last = g->node().log().lastIndex();
            for (data::NodeId peer : peers)
                if (peer != self && g->matchIndexOf(peer) >= last)
                    ++c.peerCaughtUp[peer];
        }
        return c;
    }

    // May `target` be handed leadership of `g` right now? (Extracted so the balancer's
    // candidate loop can try the NEXT peer when this one is ineligible instead of giving
    // up on the group -- see rebalance.)
    //
    // NEVER hand leadership to a peer that is not CAUGHT UP on this group.
    //
    // A leader with a transfer in flight refuses every proposal until the
    // transferee acks up to lastIndex, so targeting a peer that cannot ack costs
    // the group its write availability for the whole transfer window. And a DEAD
    // peer is the MOST attractive target this loop has: it leads nothing, so its
    // deficit is the largest on every pass. Measured on the restart-catch-up
    // gate: with one of three nodes down, ~26% of writes failed with
    // "1 VShard slice(s) uncommitted ... (last: not-leader)" for as long as it
    // stayed down, against a perfectly healthy 2-of-3 quorum.
    //
    // RaftNode::tick now abandons a transfer after one election timeout, so this
    // is no longer unbounded -- but a bounded write outage repeated every
    // balancer pass is still an outage, and a caught-up target is also the only
    // one that transfers IMMEDIATELY (transferLeadership sends TimeoutNow at
    // once rather than waiting on a catch-up round trip).
    //
    // THE GUARD USED TO BE EXACT EQUALITY (matchIndex == lastIndex) and that made
    // the balancer LOAD-DEPENDENT (debt D-1). On a group taking writes, matchIndex
    // trails lastIndex by whatever is in flight essentially all of the time -- the
    // leader appends locally, then replicates -- so a perfectly healthy peer failed
    // the test on nearly every pass. The balancer converged when the cluster was
    // quiet and could only move COLD groups when it was busy, which is exactly
    // backwards: hot groups are the ones whose leadership placement matters.
    //
    // Two independent gates replace it, and BOTH must hold:
    //
    //   1. LAG BOUND -- within kMaxTransferLagEntries of our log end. This is the
    //      "the handoff will complete promptly" half. 64 entries is about one
    //      AppendEntries round trip of in-flight work at this batch size, so a
    //      peer keeping up passes continuously while one genuinely behind (a
    //      restarted node still streaming its backlog) does not. TimeoutNow then
    //      fires on the very next ack rather than after a catch-up campaign.
    //
    //   2. LIVENESS -- replied within the LAST HEARTBEAT ROUND. This is the "the
    //      target is actually there" half, and it is the one a pure lag bound
    //      CANNOT provide. On an IDLE group a dead peer sits at lag ZERO forever:
    //      it was caught up when it died and lastIndex never moves again, so it
    //      passes any delta check, including the old exact-equality one. (That
    //      hole is therefore not new here -- it is pre-existing, and this closes
    //      it.) The heartbeat gives an independent decaying signal: the leader
    //      bcastAppends every heartbeatTimeout ticks whether or not there is
    //      anything to send, and a live peer answers within an RTT.
    //
    // THE LIVENESS WINDOW IS ONE HEARTBEAT ROUND, AND IT IS THIS TIGHT FOR A
    // MEASURED REASON. It was three rounds (1.5 s at the production 25-tick
    // heartbeat) and `fault_injection_gate.sh` regressed: 1 of 2000 bench writes
    // failed `RetryableWriteError ... (last: transport)` where the same tree
    // without this change took a marginally HEAVIER storm (167 rounds / 457
    // connections vs 165 / 442) with zero. The mechanism is specific and it is
    // this loop's: the periodic balancer could target a peer whose connection had
    // just been RST, because the ack clock had not yet decayed past three rounds
    // and the lag bound still held. `transferLeadership` then pins
    // `leadTransferee_` and the group refuses EVERY proposal until the transferee
    // acks -- and the abandon bound is one ELECTION timeout (2.5-5 s), far longer
    // than the 1.5 s write deadline. So one mis-aimed transfer is one failed
    // batch. The old exact-equality guard was accidentally immune: a peer whose
    // acks stopped fell behind a growing lastIndex immediately.
    //
    // One round is the tightest bound a HEALTHY peer still satisfies -- it answers
    // every heartbeat within an RTT, so its clock resets long before the next
    // round -- and it cuts the post-reset eligibility window from 1.5 s to 0.5 s.
    //
    // Residual exposure, bounded and now deliberate: a peer that dies inside the
    // current heartbeat round can still be targeted on a pass that races it. That
    // costs the group its proposals for one election timeout, after which
    // RaftNode::tick abandons the transfer (§3.10) and writes resume; the peer
    // then reads stale on every later pass and is skipped. One bounded window on
    // the pass that races the death, not a window per pass forever -- which is
    // what the abandon fix alone left on the table. Shrinking it further means
    // shortening the ABANDON window (a consensus-timing change, not a target
    // filter) -- see the debt register.
    static bool transferrableTo(raft::RaftGroup& g, data::NodeId target) {
        constexpr raft::LogIndex kMaxTransferLagEntries = 64;
        const auto lastIdx = g.node().log().lastIndex();
        const auto match = g.matchIndexOf(target);
        const bool caughtUp = match >= lastIdx || (lastIdx - match) <= kMaxTransferLagEntries;
        const uint64_t kMaxTransferAckStaleTicks = g.heartbeatTimeout();
        const bool live = g.ticksSinceAck(target) <= kMaxTransferAckStaleTicks;
        return caughtUp && live;
    }

    // Rebalance leadership among THIS shard's groups. Because assignCore spreads
    // VShards evenly over shards and every shard sees the same node set, balancing
    // each shard's slice independently balances the cluster as a whole.
    //
    // FAIR SHARE IS PER-NODE AND MEMBERSHIP-WEIGHTED, NOT totalLed/N (debt D-12). A node
    // can only lead a group it REPLICATES, so at RF < N the nodes are not
    // interchangeable: of the groups THIS shard hosts, we are a voter in all of them and
    // a given peer in only some. The old `fair = totalLed / peers.size()` divided our
    // hosted count by the whole cluster, which at RF=3 on 5 nodes is ~40% of the truth --
    // so every node believed itself permanently above fair share and shed leadership on
    // every pass, forever. MEASURED on an idle 5-node RF=3 cluster before this fix: after
    // 12 minutes with no writes the balancer was still moving 26-114 VShards per 30 s and
    // the spread had stalled at ~300 of a fair 819 (min 666, max 980) instead of closing.
    //
    // The correct expectation for node v over the groups we host is the sum of 1/|voters|
    // over those groups v is a voter of: with uniform RF it is |hosted|/RF for us and
    // |hosted ∩ hosted(v)|/RF for a peer. At RF == N every group has every node as a voter,
    // so expected[v] == hosted/N for every v -- the old formula's shape, and behaviourally
    // the same target, which is why RF == N (production, the 3-node gates) is unchanged.
    // It is not literally the same arithmetic: the old divisor counted only groups that had
    // a LEADER (leaderless ones were skipped before the count) and divided in integers,
    // where this counts every hosted group and divides in doubles. Both differences are
    // sub-one-VShard on a converged cluster and neither changes which side of fair share a
    // node lands on -- the 3-node gates measure identically before and after -- but "reduces
    // exactly to the old formula" would be too strong a claim.
    seastar::future<size_t> rebalance(size_t maxTransfers, data::NodeId self, std::vector<data::NodeId> peers) {
        if (!plane_ || maxTransfers == 0 || peers.empty())
            co_return 0;
        auto& host = plane_->host();
        std::map<data::NodeId, size_t> led;       // leadership we can SEE (over our hosted groups)
        std::map<data::NodeId, double> expected;  // fair share of that same set, by membership
        std::map<uint16_t, std::vector<data::NodeId>> votersOf;
        std::vector<uint16_t> mine;
        for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
            if (shardOwningVShard(vs, dir_) != seastar::this_shard_id())
                continue;
            // A group we do not REPLICATE tells us nothing: leaderOf reads kNoNode for it
            // whether or not it has a leader, and it can never be ours to give away.
            if (!host.hosts(vs))
                continue;
            raft::RaftGroup* g = host.group(vs);
            if (!g)
                continue;
            const auto& voters = g->node().config().voters;
            if (voters.empty())
                continue;
            for (data::NodeId v : voters)
                expected[v] += 1.0 / static_cast<double>(voters.size());
            votersOf[vs].assign(voters.begin(), voters.end());
            const data::NodeId leader = host.leaderOf(vs);
            if (leader == timestar::raft::kNoNode)
                continue;
            ++led[leader];
            if (leader == self)
                mine.push_back(vs);
        }
        const double fairSelf = expected.count(self) ? expected.at(self) : 0.0;
        if (static_cast<double>(mine.size()) <= fairSelf)
            co_return 0;

        std::vector<std::pair<data::NodeId, size_t>> targets;
        for (data::NodeId id : peers) {
            if (id == self)
                continue;
            const double fairPeer = expected.count(id) ? expected.at(id) : 0.0;
            const size_t have = led.count(id) ? led.at(id) : 0;
            if (static_cast<double>(have) < fairPeer)
                targets.push_back({id, static_cast<size_t>(fairPeer) - have});
        }
        if (targets.empty())
            co_return 0;

        const size_t budget = std::min(maxTransfers, mine.size() - static_cast<size_t>(fairSelf));
        size_t done = 0, ti = 0;
        for (uint16_t vs : mine) {
            if (done >= budget)
                break;
            raft::RaftGroup* g = host.group(vs);
            if (!g || !g->isLeader())
                continue;
            const auto& voters = votersOf[vs];
            // Pick the first ELIGIBLE target for THIS group, starting from the
            // round-robin cursor, rather than taking whatever the cursor points at and
            // giving up on the group if it does not fit.
            //
            // A NON-VOTER OF THIS GROUP IS NOT A CANDIDATE AT ALL (debt D-12).
            // `RaftNode::transferLeadership` returns silently for a target that is not a
            // voter, so aiming at one is a NO-OP that this loop nonetheless counted as a
            // transfer: at RF=3 on 5 nodes, 2 of the 4 peers replicate any given group,
            // so about half of every pass's budget was spent on transfers that could not
            // happen -- and `transfers_initiated` reported them. MEASURED before the fix
            // on an idle 5-node RF=3 cluster: 114 961 transfers "initiated" over five
            // minutes of operator storms against roughly 1 500 VShards that actually
            // changed hands.
            size_t chosen = targets.size();
            for (size_t k = 0; k < targets.size(); ++k) {
                const size_t idx = (ti + k) % targets.size();
                auto& [cand, deficit] = targets[idx];
                if (deficit == 0)
                    continue;
                if (std::find(voters.begin(), voters.end(), cand) == voters.end())
                    continue;
                if (!transferrableTo(*g, cand))
                    continue;
                chosen = idx;
                break;
            }
            if (chosen == targets.size())
                continue;  // no peer can take this group right now; try the next one
            auto& [target, deficit] = targets[chosen];
            try {
                co_await g->transferLeadership(target);
                --deficit;
                ++done;
                ++ti;
            } catch (...) {
                ++ti;  // next pass retries
            }
        }
        co_return done;
    }

    seastar::future<> stop() {
        // Refuse NEW slices from this instant (see ready()), before anything is torn
        // down: sharded<>::stop() runs every shard's stop() concurrently, so a shard
        // that has not started stopping can still route a slice here.
        stopping_ = true;
        // STOP SERVING FIRST. This shard's DataPlaneRpc is one of the node's real
        // data-plane servers, and its handlers reach the plane (proposeBatch ->
        // p.plane()). Tearing the plane down first left a window in which an inbound
        // peer proposeWrite dereferenced a null plane_ and crashed the node -- during a
        // rolling restart under write load, exactly when it hurts. Stopping the server
        // first also lets in-flight handlers finish against a still-ticking Raft plane
        // rather than being cut off mid-commit. rpc_ itself is only DESTROYED after
        // plane_, which borrows it.
        if (rpc_)
            co_await rpc_->stop();
        if (plane_)
            co_await plane_->stop();
        plane_.reset();
        rpc_.reset();
        if (transport_)
            co_await transport_->stop();
        transport_.reset();
        store_.reset();
        co_return;
    }

private:
    // Declared so plane_ (which borrows the others) is destroyed FIRST.
    std::unique_ptr<EngineLocalStore> store_;
    // Borrows store_ and `this`; destroyed before store_ (declared after it).
    std::unique_ptr<data::LeaderFilteredNodeStore> queryStore_;
    std::unique_ptr<raft::RaftRpcTransport> transport_;  // this shard's own Raft listener + peer clients
    seastar::sharded<ShardRaftPlane>* peers_ = nullptr;
    const data::VShardDirectory* dir_ = nullptr;  // VShard -> group resolution (debt D-11); not owned
    std::unique_ptr<data::DataPlaneRpc> rpc_;     // this shard's own data-plane listener + peer clients
    std::unique_ptr<ReplicatedDataPlane> plane_;
    bool stopping_ = false;          // set at the top of stop(); see ready()
    uint64_t inboundProposals_ = 0;  // peer proposals served BY THIS SHARD's listener
};

// Split `batch` by the shard that OWNS each series' VShard Raft group and dispatch
// every slice CONCURRENTLY, then await them all.
//
// The slices used to be awaited one at a time inside the grouping loop, so a batch
// spanning S shards paid the SUM of S full quorum round trips (durable append +
// replicate + commit + apply) rather than the max -- the same defect
// ReplicatedVShardHost::proposeBatch already fixed one layer down, at the per-VShard
// level. All-must-commit semantics are unchanged: EVERY dispatched future is awaited
// even after one fails (abandoning an in-flight one would let its commit land on a
// destroyed continuation), and the first error is rethrown so a partial batch always
// surfaces as a retryable failure rather than a silent partial success.
//
// `dir` is optional (null in the unit tests that exercise only the shard fan-out). When
// present the WHOLE batch is validated against it BEFORE any slice is dispatched
// (write-scaleout 3d): the per-shard router also fail-closes on an unassigned VShard, but
// it only sees ITS OWN slice, so a batch whose unassigned VShard happened to land on the
// last shard could durably commit the other shards' slices and only then fail the client
// -- a partial commit reported as an error, which is precisely the state the ack contract
// forbids. The directory is immutable after start() and is already shared with every
// shard's router, so reading it here is the same access those do.
inline seastar::future<> writeSlicesToOwningShards(seastar::sharded<ShardRaftPlane>& shards, data::WriteBatch batch,
                                                   const data::VShardDirectory* dir = nullptr) {
    if (dir) {
        for (auto& s : batch.series) {
            const uint16_t vs = data::vshardOf(s);
            if (dir->ownerOf(vs) == data::kNoNode)
                throw data::UnassignedVShardError("cluster: VShard " + std::to_string(vs) +
                                                  " is unassigned in the current placement map");
        }
    }
    // Charge this batch against THIS shard's in-flight write budget for the whole
    // coordination (write-scaleout 3d). It is taken here, on the request shard, because
    // this is where a batch's memory is held from: the slices, the retries and every
    // awaited quorum round hang off this frame. Over budget -> WriteOverloadedError,
    // which the HTTP layer turns into 503 + Retry-After; the guard releases on every
    // exit path.
    WriteAdmissionGuard admission(data::approxResidentBytes(batch));
    // Split by VShard ONCE (write-scaleout 2b) and bucket the resulting groups by
    // owning shard. Everything below -- the leader-node bucketing in the router and
    // the per-group propose -- re-buckets these same groups, so each series is moved
    // once and its key hashed once (write-scaleout 2a).
    std::map<unsigned, data::VShardBatches> byShard = bucketByOwningShard(data::splitByVShard(std::move(batch)), dir);
    std::vector<seastar::future<>> pending;
    pending.reserve(byShard.size());
    for (auto& [shard, slice] : byShard)
        pending.push_back(shards.invoke_on(shard, [b = std::move(slice)](ShardRaftPlane& p) mutable {
            // sharded<>::stop() runs every shard's stop() CONCURRENTLY, so a shard that
            // has already torn its plane down can be invoked from one that has not --
            // no timing luck needed. Fail retryably instead of dereferencing null.
            if (!p.ready())
                return seastar::make_exception_future<>(data::ShardStoppingError(kShardStoppingError));
            return p.plane().write(std::move(b));
        }));

    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            co_await std::move(f);
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return;
}

// The peer-ingress twin of the above: replicate each slice through the Raft group on
// its owning shard. `false` (not the leader for some slice) makes the WHOLE batch fail
// so the caller redirects/retries -- never a silent partial commit.
inline seastar::future<bool> proposeSlicesToOwningShards(seastar::sharded<ShardRaftPlane>& shards,
                                                         data::WriteBatch batch,
                                                         const data::VShardDirectory* dir = nullptr) {
    // PEER-INGRESS ADMISSION (debt D-8). Charged on THIS shard -- the one the peer's
    // connection landed on -- because this frame holds the decoded batch, the slices and
    // every awaited quorum round, exactly as the originated charge does one door over.
    // It is its OWN budget, not the originated one: see AdmissionClass.
    //
    // NO DOUBLE CHARGE with the originated path. A coordinator's own slices never come
    // through here -- ReplicatedBatchWriteRouter proposes locally-led groups straight into
    // the NodeStore (`local_.proposeVShardBatchesHinted`) and only ever RPCs a REMOTE
    // leader -- so the two doors are disjoint by construction, and a self-slice is charged
    // once, as originated.
    //
    // Rejection throws WriteOverloadedError, which crosses the wire as an exception and
    // reaches the coordinator as the retryable `Transport` class (see the hinted overload
    // below, which does better). Retryable either way, and NOT election-shaped either way,
    // so an overloaded peer can never buy the 6 s election window (debt D-14).
    WriteAdmissionGuard admission(data::approxResidentBytes(batch), AdmissionClass::PeerIngress);
    // One split, buckets of groups -- same shape as writeSlicesToOwningShards.
    std::map<unsigned, data::VShardBatches> byShard = bucketByOwningShard(data::splitByVShard(std::move(batch)), dir);
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byShard.size());
    for (auto& [shard, slice] : byShard)
        pending.push_back(shards.invoke_on(shard, [b = std::move(slice)](ShardRaftPlane& p) mutable {
            if (!p.ready())  // see the note in writeSlicesToOwningShards
                return seastar::make_exception_future<bool>(data::ShardStoppingError(kShardStoppingError));
            return p.plane().host().proposeVShardBatches(std::move(b));
        }));

    bool all = true;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            const bool ok = co_await std::move(f);
            all = all && ok;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return all;
}

// The hinted twin of proposeSlicesToOwningShards (write-scaleout 3a): each owning shard
// answers with per-VShard rejects, and the rejects are UNIONED rather than collapsed to a
// bool, so a batch that lost only one VShard's leadership costs the caller one slice's
// retry instead of the whole batch's.
//
// A shard that fails for a RETRYABLE reason (its plane is stopping) contributes rejects
// rather than an exception, because from the forwarding node's side that is a redirect,
// not a fault. Anything else still propagates -- a bug must not be laundered into a retry.
inline seastar::future<data::ProposeOutcome> proposeSlicesToOwningShardsHinted(
    seastar::sharded<ShardRaftPlane>& shards, data::WriteBatch batch, const data::VShardDirectory* dir = nullptr) {
    const size_t charge = data::approxResidentBytes(batch);
    std::map<unsigned, data::VShardBatches> byShard = bucketByOwningShard(data::splitByVShard(std::move(batch)), dir);

    // PEER-INGRESS ADMISSION (debt D-8), and on THIS path the rejection is REPORTED
    // rather than thrown. A thrown exception reaches the coordinator as `Transport` --
    // retryable and correctly not election-shaped, but ambiguous, and it also marks this
    // node "unreachable" so the coordinator wakes the Raft groups behind it at give-up
    // (replicated_write_router.cpp) for a node that is merely busy. A reject list says
    // exactly what happened: every slice UNCOMMITTED, kind Overloaded, NO leader hint (we
    // may well still be the leader -- there is nowhere better to send it, only a later
    // time). The coordinator then paces geometrically and retries here, which is what
    // "something must drain" wants.
    //
    // committed=false with an EMPTY committed set is the honest answer under the
    // committed-set contract: nothing was proposed, so nothing may be crossed off.
    std::optional<WriteAdmissionGuard> admission;
    try {
        admission.emplace(charge, AdmissionClass::PeerIngress);
    } catch (const data::WriteOverloadedError&) {
        data::ProposeOutcome over;
        over.committed = false;
        for (const auto& [shard, slice] : byShard)
            for (const auto& g : slice)
                over.rejects.push_back(
                    data::SliceReject{g.first, timestar::raft::kNoNode, data::WriteFailure::Overloaded});
        // SAY SO, RATE-LIMITED. This branch is otherwise INVISIBLE on this node: the reject
        // travels back to the coordinator, so the operator of the node that is actually
        // full sees nothing at all, and `WriteAdmission::rejected()` currently has no
        // consumer (no metric, no endpoint -- worth wiring, and nothing does it today).
        //
        // The string deliberately does NOT contain "shard write buffer full":
        // backpressure_gate.sh asserts that phase 2, at the DEFAULT budget, logs ZERO
        // occurrences of that phrase, and replication traffic legitimately reaching an
        // ingress bound would then read as a gate failure. Distinct phrasing keeps the two
        // conditions distinguishable in a log and in a grep.
        //
        // One line per second per shard: an overloaded ingress path is hit at the request
        // RATE, and a log line per rejected batch is how a node under pressure spends its
        // remaining capacity on logging.
        static thread_local seastar::lowres_clock::time_point lastLog{};
        static thread_local uint64_t suppressed = 0;
        const auto now = seastar::lowres_clock::now();
        if (now - lastLog >= std::chrono::seconds(1)) {
            timestar::http_log.warn(
                "cluster: peer-ingress admission FULL on shard {} ({} of {} bytes in flight); rejected {} replicated "
                "VShard slice(s) as retryable-overloaded ({} similar rejection(s) suppressed in the last second). The "
                "coordinator will pace and retry; this node is the leader for those slices, so there is nowhere else "
                "for them to go.",
                seastar::this_shard_id(), WriteAdmission::local(AdmissionClass::PeerIngress).inFlight(),
                WriteAdmission::limitBytes(AdmissionClass::PeerIngress), over.rejects.size(), suppressed);
            lastLog = now;
            suppressed = 0;
        } else {
            ++suppressed;
        }
        co_return over;
    }

    std::vector<seastar::future<data::ProposeOutcome>> pending;
    std::vector<std::vector<uint16_t>> pendingVShards;  // parallel to `pending`
    pending.reserve(byShard.size());
    pendingVShards.reserve(byShard.size());
    for (auto& [shard, slice] : byShard) {
        std::vector<uint16_t> vs;
        vs.reserve(slice.size());
        for (const auto& g : slice)
            vs.push_back(g.first);
        pendingVShards.push_back(std::move(vs));
        pending.push_back(shards.invoke_on(shard, [b = std::move(slice)](ShardRaftPlane& p) mutable {
            if (!p.ready())  // see the note in writeSlicesToOwningShards
                return seastar::make_exception_future<data::ProposeOutcome>(
                    data::ShardStoppingError(kShardStoppingError));
            // The groups are owned by do_with (moved across the shard boundary) and the
            // returned future is awaited before they are freed, so the view is safe.
            // `pp` is captured BY VALUE: `p` is a reference parameter living in this
            // lambda's frame, and a `&p` capture would dangle the moment it returns.
            return seastar::do_with(std::move(b), [pp = &p](data::VShardBatches& groups) {
                return pp->plane().host().proposeVShardBatchesHinted(data::viewOf(groups), std::nullopt);
            });
        }));
    }

    data::ProposeOutcome out;
    std::exception_ptr fatalErr;
    for (size_t i = 0; i < pending.size(); ++i) {
        try {
            data::ProposeOutcome r = co_await std::move(pending[i]);
            // Union BOTH sets. A shard reporting committed==true means all of ITS
            // slices committed; otherwise only the ones it names did.
            if (r.committed)
                out.committedVShards.insert(out.committedVShards.end(), pendingVShards[i].begin(),
                                            pendingVShards[i].end());
            else
                out.committedVShards.insert(out.committedVShards.end(), r.committedVShards.begin(),
                                            r.committedVShards.end());
            out.rejects.insert(out.rejects.end(), r.rejects.begin(), r.rejects.end());
        } catch (...) {
            const auto kind = data::classifyLocalWriteFailure(std::current_exception());
            if (!data::isRetryableWriteFailure(kind)) {
                if (!fatalErr)
                    fatalErr = std::current_exception();
                continue;
            }
            for (uint16_t vs : pendingVShards[i])
                out.rejects.push_back(data::SliceReject{vs, timestar::raft::kNoNode, kind});
        }
    }
    if (fatalErr)
        std::rethrow_exception(fatalErr);
    // From the committed set, never from the absence of rejects: a shard that failed
    // retryably contributes no rejects for slices it never even reached.
    size_t total = 0;
    for (const auto& v : pendingVShards)
        total += v.size();
    out.committed = out.committedVShards.size() == total;
    co_return out;
}

inline seastar::future<bool> ShardRaftPlane::proposeBatch(data::WriteBatch batch) {
    ++inboundProposals_;  // served on THIS shard, whichever shards end up owning the slices
    return proposeSlicesToOwningShards(*peers_, std::move(batch), dir_);
}

inline seastar::future<data::ProposeOutcome> ShardRaftPlane::proposeBatchHinted(data::WriteBatch batch) {
    ++inboundProposals_;
    return proposeSlicesToOwningShardsHinted(*peers_, std::move(batch), dir_);
}

inline seastar::future<raft::LogIndex> ShardRaftPlane::leaderReadIndex(uint16_t vshard) {
    return peers_->invoke_on(shardOwningVShard(vshard, dir_), [vshard](ShardRaftPlane& p) {
        if (!p.ready())
            return seastar::make_exception_future<raft::LogIndex>(data::ShardStoppingError(kShardStoppingError));
        return p.plane().host().leaderReadIndex(vshard);
    });
}

inline seastar::future<std::vector<data::VShardRedirect>> ShardRaftPlane::resolveLeaders(
    std::vector<uint16_t> vshards) {
    std::map<unsigned, std::vector<uint16_t>> byShard;
    for (uint16_t vs : vshards)
        byShard[shardOwningVShard(vs, dir_)].push_back(vs);
    std::vector<data::VShardRedirect> out;
    out.reserve(vshards.size());
    for (auto& [shard, list] : byShard) {
        if (shard == seastar::this_shard_id()) {
            auto part = resolveLeadersLocal(list);
            out.insert(out.end(), part.begin(), part.end());
            continue;
        }
        // A sibling shard that has already torn its plane down answers with nothing for
        // its slice; those VShards come back to the coordinator as unresolved
        // (hosted=false) and fail its read closed -- never silently answered from here.
        auto part = co_await peers_->invoke_on(shard, [l = list](ShardRaftPlane& p) {
            std::vector<data::VShardRedirect> r;
            if (p.ready())
                r = p.resolveLeadersLocal(l);
            return r;
        });
        out.insert(out.end(), part.begin(), part.end());
    }
    co_return out;
}

inline seastar::future<raft::LogIndex> ShardRaftPlane::leaderCommitIndex(uint16_t vshard) {
    return peers_->invoke_on(shardOwningVShard(vshard, dir_), [vshard](ShardRaftPlane& p) {
        if (!p.ready())
            return seastar::make_exception_future<raft::LogIndex>(data::ShardStoppingError(kShardStoppingError));
        return p.plane().host().leaderCommitIndex(vshard);
    });
}

}  // namespace timestar::cluster
