#include "cluster_data_plane.hpp"

#include "../../utils/logger.hpp"  // timestar::http_log
#include "../data/read_routing.hpp"
#include "write_admission.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <set>
#include <stdexcept>
#include <string>

namespace timestar::cluster {

namespace {
// Split "host:port" -> {host, port}; missing port defaults to 8086 (same rule as
// the M1 gateway). The data-plane listener uses port + kDataPlanePortOffset.
struct HostPort {
    std::string host;
    uint16_t port = 8086;
};
HostPort parseHostPort(const std::string& s) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos)
        return {s, 8086};
    HostPort hp;
    hp.host = s.substr(0, colon);
    try {
        hp.port = static_cast<uint16_t>(std::stoul(s.substr(colon + 1)));
    } catch (...) {}
    return hp;
}

// CheckQuorum: the BUILD default, and the ONE-WAY switch an operator has over it
// (debt D-9/D-29/D-30).
//
// THE DEFAULT IS OFF FOR THIS RELEASE, and flipping it is this one line plus a re-run of
// node_kill_round -- see the construction site for the measurement that decided it and for
// the release ordering that makes flipping it safe next time.
//
// `TIMESTAR_CLUSTER_CHECKQUORUM` can only ever turn it OFF. It is retained while the
// default is off (where it is a no-op) precisely so it is already in place, already logged
// and already documented on the release that enables the guard -- an operator meeting a
// CheckQuorum-shaped incident at 3am should not be the first person to use this knob. An
// ENABLE knob is refused on purpose: with ADR 0005's mechanism (c) unbuilt, enabling per
// node is exactly the mixed-version hazard that ADR exists to prevent.
inline constexpr bool kCheckQuorumDefault = false;

bool checkQuorumEnabled() {
    const char* e = std::getenv("TIMESTAR_CLUSTER_CHECKQUORUM");
    if (!kCheckQuorumDefault) {
        // Nothing to disable, and nothing may enable. Say so if someone tried, so the
        // knob's absence of effect is visible rather than mysterious.
        if (e && *e)
            timestar::http_log.info(
                "cluster: TIMESTAR_CLUSTER_CHECKQUORUM='{}' has no effect -- Raft CheckQuorum is OFF in this build "
                "(debt D-9/D-29) and this override is disable-only",
                e);
        return false;
    }
    if (!e || !*e)
        return true;
    // TRIM AND LOWERCASE BEFORE COMPARING. This is an operator's only lever over the
    // guard and it is reached at 3am: `FALSE`, `Off` and a value with a stray space or
    // trailing newline (trivially produced by a shell here-doc, a docker-compose YAML
    // scalar, or an env file) must all mean what they obviously mean. A case-SENSITIVE
    // exact match silently fell through to the "not a boolean" arm below and left the
    // guard ON -- i.e. the one outcome the operator was trying to avoid.
    std::string v(e);
    v.erase(0, v.find_first_not_of(" \t\r\n"));
    if (const auto last = v.find_last_not_of(" \t\r\n"); last != std::string::npos)
        v.erase(last + 1);
    else
        v.clear();
    std::transform(v.begin(), v.end(), v.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (v.empty())
        return true;  // whitespace only == unset
    if (v == "0" || v == "false" || v == "no" || v == "off") {
        timestar::http_log.warn(
            "cluster: Raft CheckQuorum DISABLED on this node by TIMESTAR_CLUSTER_CHECKQUORUM='{}'. A partitioned "
            "or stale leader will now keep accepting proposals it cannot commit until each one's own deadline, and "
            "leader-only reads on the losing side of a partition converge per request rather than promptly. Nothing "
            "is unsafe (commit still needs a quorum ack); this is the configuration that shipped before debt D-9.",
            e);
        return false;
    }
    if (v == "1" || v == "true" || v == "yes" || v == "on") {
        timestar::http_log.warn(
            "cluster: TIMESTAR_CLUSTER_CHECKQUORUM='{}' is ignored -- the override is DISABLE-ONLY and CheckQuorum "
            "is already on. Enabling it per node is exactly the mixed-version hazard ADR 0005 exists to prevent "
            "(one node running the disruption guard while an older peer drops its transfer votes).",
            e);
        return true;
    }
    timestar::http_log.error(
        "cluster: TIMESTAR_CLUSTER_CHECKQUORUM='{}' is not a boolean (0/false/no/off disable, case- and "
        "whitespace-insensitive; nothing enables); leaving CheckQuorum ON",
        e);
    return true;
}

// Snapshot-trigger policy overrides (debt D-6). The defaults
// (`ReplicatedVShardHost::kSnapshotEntryThreshold` / `kSnapshotBytesThreshold`) are sized
// for production: 8192 entries or 64 MiB per VShard, which on a 4096-VShard cluster means
// a group snapshots after a substantial amount of real traffic and essentially never
// during a short benchmark.
//
// These exist for two reasons and both are load-bearing. (1) A gate has to be able to FORCE
// snapshotting: `restart_catchup_gate.sh` must drive a follower far enough behind that its
// log was compacted away, and reaching an 8192-entry-per-VShard backlog by writing real
// points would take hours of disk. (2) An operator whose batches are far larger or smaller
// than the assumed shape needs to move the thresholds without a rebuild -- the numbers are
// a guess about workload, not a property of the format.
//
// Unset == the built-in default. A malformed value is reported and ignored rather than
// silently taken as zero, because zero DISABLES a threshold (and both zero makes every
// group eligible on every sweep) -- a typo must not turn a policy into a stampede.
std::optional<uint64_t> envU64(const char* name) {
    const char* e = std::getenv(name);
    if (!e || !*e)
        return std::nullopt;
    try {
        return static_cast<uint64_t>(std::stoull(e));
    } catch (...) {
        timestar::http_log.error("cluster: {}='{}' is not a number; using the built-in default", name, e);
        return std::nullopt;
    }
}

// Resolve "host" to an address (numeric dotted-quad parses directly; a hostname --
// e.g. a docker service name -- goes through DNS).
seastar::future<seastar::net::inet_address> resolveHost(const std::string& host) {
    std::optional<seastar::net::inet_address> numeric;
    try {
        numeric = seastar::net::inet_address(host);  // numeric IP fast path
    } catch (...) {
        numeric.reset();  // a hostname -- fall through to DNS (co_await not allowed in a catch)
    }
    if (numeric)
        co_return *numeric;
    co_return co_await seastar::net::dns::resolve_name(seastar::sstring(host));
}
}  // namespace

seastar::future<> ClusterDataPlane::start(const ClusterConfig& cfg, seastar::sharded<Engine>& engines) {
    // Force the in-flight write budget to resolve (and LOG itself) during startup rather
    // than on the first write, so a mis-set TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES is
    // visible in the boot log instead of being inferred from a wall of 503s.
    (void)WriteAdmission::limitBytes();
    rt_ = ClusterRuntime::fromConfig(cfg);  // throws (fail-closed) on misconfig
    enginesPtr_ = &engines;
    rf_ = cfg.replication_factor < 1 ? 1 : cfg.replication_factor;
    dir_ = std::make_unique<data::VShardDirectory>(rt_->directory());
    local_ = std::make_unique<EngineLocalStore>(engines);
    rpc_ = std::make_unique<data::DataPlaneRpc>();

    const bool replicated = cfg.replication_factor > 1;
    // Bind the RPC server to this node's own data-plane address.
    const HostPort self = parseHostPort(rt_->peerAddresses.at(rt_->selfId));
    seastar::net::inet_address selfAddr = co_await resolveHost(self.host);
    const seastar::socket_address dataPlaneAddr(selfAddr, static_cast<uint16_t>(self.port + kDataPlanePortOffset));
    // Peer-facing settings must be applied BEFORE start() on every transport that talks
    // to a peer. This instance is the shard-0 query/metadata client; the per-shard
    // instances (started below) are the listeners and the write path's clients, and they
    // get the identical settings.
    if (tls_)
        rpc_->setTlsCredentials(tls_->certPem, tls_->keyPem, tls_->caPem, tls_->expectedPeerName);
    rpc_->setLocalVersion(localVersion_);
    if (replicated)
        // Replicated mode serves the data plane from EVERY shard (see below); this
        // instance stays client-only, for the shard-0 query/metadata fan-out.
        co_await rpc_->startClientOnly();
    else
        co_await rpc_->start(dataPlaneAddr, *local_);

    // Peers are registered AFTER the per-shard transports exist (below), from ONE
    // resolution each -- see registerPeer / the registration block in the replicated
    // section. Registering here as well is what created the asymmetry this replaces.

    finalizer_ = std::make_unique<http::HttpQueryHandler>(&engines);
    router_ = std::make_unique<data::NodeWriteRouter>(*dir_, *local_, *rpc_);
    coord_ = std::make_unique<data::NodeQueryCoordinator>(*dir_, *local_, *rpc_, *finalizer_);

    if (!replicated)
        co_await registerAllPeers(false);

    // RF=3 replicated write path (integration plan M3). Composition proven by
    // replicated_data_plane_test; here we wire the REAL transports. Reads still fan
    // out via coord_ (v1 leader reads land on the primary, == the leader under the
    // placement model). Group instantiation is the real prod cost: RF*4096/N groups
    // per node (see the group-granularity note in the plan).
    if (replicated) {
        std::filesystem::path journalRoot = timestar::dataRootPath();
        journalRoot /= "cluster_raft";
        // Start a Raft plane on EVERY shard; each will own only the VShards that
        // assignCore maps to it, so the group work spreads across all cores.
        co_await shards_.start();
        shardsStarted_ = true;
        {
            const std::string jroot = journalRoot.string();
            const data::NodeId selfId = rt_->selfId;
            const data::VShardDirectory* dirp = dir_.get();
            auto* peers = &shards_;
            co_await shards_.invoke_on_all([enginesPtr = enginesPtr_, peers, dirp, selfId, jroot](ShardRaftPlane& p) {
                return p.init(enginesPtr, peers, dirp, selfId, jroot, std::chrono::milliseconds(20));
            });
        }

        // Serve the DATA plane on this node's data-plane address FROM EVERY SHARD
        // (connection_distribution, not SO_REUSEPORT -- this seastar disables reuseport,
        // so shard 0 owns the one socket and hands each accepted fd to the shard the
        // listen options pick), and give every shard its own peer clients. Previously one
        // DataPlaneRpc on shard 0 carried every forwarded write in BOTH directions, so
        // each shard's remote leader-forwards hopped to shard 0 and back and every
        // peer's inbound proposal landed there: shard 0's Raft profile stayed ~10-20x
        // the other shards' long after the Raft transport itself was sharded (b98c1d1).
        // An inbound proposeWrite is split across the shards owning its VShards by
        // ShardRaftPlane::proposeBatch, so which shard the kernel handed the connection
        // to no longer decides where the work runs.
        co_await shards_.invoke_on_all([dataPlaneAddr, tls = tls_, ver = localVersion_](ShardRaftPlane& p) {
            // `tls` is read from this shard's copy and the PEM strings are copied into
            // each shard's own credentials -- setTlsCredentials takes them by value, so
            // nothing cross-shard is retained.
            return p.startDataPlane(dataPlaneAddr, tls, ver);
        });

        // Serve Raft on this node's own Raft address FROM EVERY SHARD (again
        // connection_distribution, see above). Whichever shard accepts a connection
        // peeks each envelope's group id and routes it to the shard owning that group,
        // which then decodes it.
        seastar::net::inet_address rAddr = co_await resolveHost(self.host);
        const seastar::socket_address raftAddr(rAddr, static_cast<uint16_t>(self.port + kRaftPortOffset));
        co_await shards_.invoke_on_all([raftAddr](ShardRaftPlane& p) { return p.startTransport(raftAddr); });

        // Register peers on BOTH planes, from ONE resolution each, now that every
        // transport that needs them exists (write-scaleout 4b-iii).
        co_await registerAllPeers(true);

        // Instantiate each VShard's group ON ITS OWNING SHARD (see the many-group
        // timing note below).
        raft::RaftOptions ropts;
        ropts.heartbeatTimeout = 25;     // 500ms at the 20ms tick
        ropts.electionTimeoutMin = 125;  // 2.5s
        ropts.electionTimeoutMax = 250;  // 5s (randomized -> spreads campaigns)
        // Mirror the transport's send bound INTO the core, so a message that could not be
        // delivered is never built and never moves nextIndex (write-scaleout 5 review,
        // F3a). One definition, in raft_types.hpp -- and it is the PAYLOAD bound, not the
        // send bound, because the transport measures the encoded envelope while
        // `maxMessageBytes` is compared against the payload alone.
        ropts.maxMessageBytes = raft::kMaxRaftPayloadBytes;
        // CHUNKED INstallSnapshot (debt D-5). The chunk size is what the whole size chain
        // above is now sized against; the total bound is a MEMORY bound (the payload is
        // materialized in RAM on the producer, held by the leader, and staged in RAM by the
        // receiver) and it is also the threshold `snapshotVShard` refuses to compact over.
        ropts.maxSnapshotChunkBytes = raft::kMaxSnapshotChunkBytes;
        ropts.maxSnapshotBytes = raft::kMaxVShardSnapshotBytes;
        // Two heartbeat intervals (25 ticks each at the 20 ms tick) with no reply before an
        // in-flight chunk is resent -- the transport is fire-and-forget, so this timer is
        // the only thing that notices a dropped chunk.
        //
        // THE ORDERING OF THESE THREE TIMEOUTS IS LOAD-BEARING, and it is the one thing
        // about chunking that is easy to get wrong:
        //
        //     heartbeatTimeout 25  <  snapshotChunkTimeout 50  <<  electionTimeoutMin 125
        //
        // While a transfer is in flight, that peer is served snapshot CHUNKS INSTEAD OF
        // HEARTBEATS -- `sendAppend` hands off to `sendInstallSnapshot`, which is a no-op
        // while a chunk is unacked (that is the flow control). So a chunk, or the resend of
        // a lost one, is the ONLY thing resetting that follower's election clock during an
        // install. If `snapshotChunkTimeout` ever approached `electionTimeoutMin`, a single
        // dropped chunk would let a follower campaign against a perfectly healthy leader
        // mid-install. Pinned as a property (not as these numbers) by
        // `RaftSnapshotChunkingTest.AFollowerBeingFedChunksDoesNotCampaignAgainstItsLeader`.
        ropts.snapshotChunkTimeout = 2 * ropts.heartbeatTimeout;
        // THE LEADER-TRANSFER ABANDON WINDOW (debt D-20), stated here rather than left to
        // the core's derivation because it belongs in the same ordering as the timeouts
        // above and is read against the WRITE deadline, which only this layer knows:
        //
        //     transferTimeout 50 (1 s)  <  kDeadline 1.5 s  <<  electionTimeoutMin 125
        //
        // While a transfer is in flight the group refuses EVERY proposal, so this window
        // is exactly what a mis-aimed transfer costs the group's writes. etcd uses one
        // election timeout (2.5-5 s here), which is LONGER than the write deadline -- so a
        // single transfer aimed at a peer that went unreachable after the balancer picked
        // it fails a whole batch. At 1 s a batch absorbs it inside its base deadline and
        // retries into the resumed leader. The safety argument for abandoning early is on
        // RaftOptions::transferTimeout; the short version is that it retracts nothing and
        // grants nothing, the transferee's campaign is at a term no leader holds, and a
        // proposal accepted after abandoning is only ever ACKED if it committed.
        ropts.transferTimeout = 2 * ropts.heartbeatTimeout;
        if (ropts.transferTimeout >= ropts.electionTimeoutMin)
            // Same fail-closed reasoning as the chunk timeout below: a transfer window at
            // or past an election timeout is the pre-D-20 behaviour wearing a new name,
            // and the whole point is that it is much shorter than the write deadline.
            throw std::runtime_error(
                "cluster: Raft transferTimeout must be well below electionTimeoutMin -- the leader refuses every "
                "proposal while a transfer is in flight, so an abandon window as long as an election outlasts the "
                "write deadline and makes one mis-aimed transfer a failed batch");
        if (ropts.snapshotChunkTimeout >= ropts.electionTimeoutMin)
            // Fail closed at startup rather than discover it as an election storm during a
            // rebalance: a future edit to any of the three numbers must keep the ordering.
            throw std::runtime_error(
                "cluster: Raft snapshotChunkTimeout must be well below electionTimeoutMin -- a follower being fed "
                "snapshot chunks receives no heartbeats, so a chunk resend slower than an election timeout lets it "
                "campaign against a healthy leader mid-install");
        // CHECKQUORUM IS OFF, AND EVERYTHING NEEDED TO TURN IT ON IS IN THIS BINARY.
        // That combination is deliberate; it is a RELEASE-ORDERING decision, not an
        // unfinished one. Read this before flipping `kCheckQuorumDefault` in either
        // direction.
        //
        // WHY IT WAS OFF BEFORE (1f2e752, reverting the commit before it): with CheckQuorum
        // on and no transfer marker, LEADERSHIP TRANSFER BREAKS. TimeoutNow lets the
        // TRANSFEREE skip its own lease, but the vote it broadcast was an ordinary
        // RequestVote and every OTHER voter was still hearing the outgoing leader, so
        // `checkQuorum && leaderId_ != kNoNode && electionElapsed_ < electionTimeout_` held
        // on all of them and the disruption guard SILENTLY DROPPED the vote without even
        // bumping a term. A transfer that takes 0 tick rounds became a full election
        // timeout -- 2.5-5 s of leaderlessness, thousands of times per rebalance storm.
        //
        // WHAT IS FIXED NOW, all three tested and all three staying in:
        //   1. `RequestVote::campaignTransfer` (ADR 0005 mechanism (b)), honoured at the
        //      inLease check, riding its OWN message-type byte so an older peer DROPS the
        //      envelope rather than misparsing it. Transfers under CheckQuorum measured at
        //      2216 in one rolling_rebalance storm, 600/600 OK, leadership settling at 0
        //      moved.
        //   2. A TimeoutNow is honoured only from the PRE-STEP believed leader
        //      (RaftNode::step). Without it a forged TimeoutNow -- same term or higher --
        //      produced a transfer-flagged campaign from any peer and stood every voter's
        //      lease down at will (F1).
        //   3. Hibernation credits the passes it skips (RaftGroupRegistry::tickAll ->
        //      RaftNode::tick(passes)), so the lease expires in REAL time instead of 10x
        //      long. This one is a WIN IN ITS OWN RIGHT, independent of CheckQuorum: with
        //      the guard off it took node_kill_round's band from 49/400 to 32/400 and the
        //      recovery from 8 s to 7 s, because a dead leader's idle groups no longer wait
        //      out a stretched election timeout.
        //
        // WHY IT IS STILL OFF -- THE MEASUREMENT. Same binary, same session,
        // node_kill_round.sh (3-node RF=3, kill -9 of the node leading 1364 of 4096
        // mid-bench), flag the only difference (via the override below):
        //
        //     OFF  32/400 failed batches, 7 s recovery, 3.88 M pts/s   (twice, identical)
        //     ON   50/400 / 11 s / 2.60 M   and   59/400 / 13 s / 2.12 M
        //
        // So the guard still costs ~1.6-1.8x on the client-visible one-node-down band and
        // ~4-6 s of extra failover. It buys no safety (Raft never depended on it; commit
        // needs a quorum ack, and RaftGroup::proposeAndAwaitApplied already bounds every
        // write and bounds waiter accumulation). What it buys is PROMPTNESS under partition:
        // a stale leader stops accepting proposals it cannot commit within one election
        // timeout instead of one deadline per write, and leader-only reads on the losing
        // side converge on "not leader" promptly rather than per request. That is worth
        // having, and it is not worth a worse single-node failure -- which is the far more
        // common event. Residual filed as D-29.
        //
        // WHY THE CODE SHIPS ANYWAY, and this is the point: the tag-8 decoder ships in THIS
        // release, so every node in a rolling upgrade to the release AFTER this one can
        // already READ a transfer vote. Enabling the guard then needs no wire change and no
        // mixed-version window -- it becomes exactly the one-line flip above, plus a
        // re-run of node_kill_round to confirm the band. Enabling it in the same release as
        // the decoder is what would have required ADR mechanism (c) (D-30) to be built
        // first. Shipping the reader first is the cheaper half of that design.
        //
        // Pinned, so none of the three can rot while the flag is off:
        //   RaftClusterTest.LeaderTransferUnderCheckQuorumCompletesInZeroTickRounds
        //     -- the direct regression test for 1f2e752; it advances NO clock
        //   RaftClusterTest.{CheckQuorumStillRefusesAnUnsolicitedCampaign,
        //     CheckQuorumStepsDownIsolatedLeader}
        //   RaftNodeTest.TransferVote{StillObeysTheLogUpToDateCheck,
        //     StillObeysOneVotePerTerm,AtAStaleTermIsRejectedWithOurTerm} -- vote safety
        //   RaftNodeTest.AForged{SameTerm,HigherTerm}TimeoutNowIsIgnored -- (2)
        //   RaftGroupRegistryTest.HibernationDoesNotStretchTheLease -- (3)
        //   RaftCodecTest.AnOldDecoderDropsTheTransferVoteAndStillReadsAnOrdinaryOne
        //   RaftProposeDeadlineTest.CheckQuorumFailsAQuorumLessWriteOnItsOwn -- the
        //     property the guard is wanted FOR, on its own RaftOptions
        ropts.checkQuorum = checkQuorumEnabled();
        {
            std::map<unsigned, std::vector<std::pair<uint16_t, std::vector<data::NodeId>>>> byShard;
            for (const auto& [vshard, voters] : rt_->localReplicaGroups())
                byShard[shardOwningVShard(vshard, dir_.get())].push_back({vshard, voters});
            for (auto& [shard, groups] : byShard) {
                co_await shards_.invoke_on(shard,
                                           [g = std::move(groups), ropts](ShardRaftPlane& p) -> seastar::future<> {
                                               for (const auto& [vs, voters] : g)
                                                   co_await p.addVShard(vs, voters, ropts);
                                               co_return;
                                           });
            }
        }
        // Snapshot-trigger policy, resolved and LOGGED here so a mis-set override is
        // visible in the boot log rather than inferred from a log that never compacts.
        const auto snapEntries = envU64("TIMESTAR_CLUSTER_SNAPSHOT_ENTRIES");
        const auto snapBytes = envU64("TIMESTAR_CLUSTER_SNAPSHOT_BYTES");
        const auto snapMinIntervalS = envU64("TIMESTAR_CLUSTER_SNAPSHOT_MIN_INTERVAL_S");
        if (snapEntries || snapBytes || snapMinIntervalS)
            timestar::http_log.info(
                "cluster: Raft snapshot policy OVERRIDDEN: entries={} bytes={} min_interval={}s (defaults {} / {} / "
                "{}s)",
                snapEntries ? std::to_string(*snapEntries) : "default",
                snapBytes ? std::to_string(*snapBytes) : "default",
                snapMinIntervalS ? std::to_string(*snapMinIntervalS) : "default",
                ReplicatedVShardHost::kSnapshotEntryThreshold, ReplicatedVShardHost::kSnapshotBytesThreshold,
                ReplicatedVShardHost::kMinSnapshotInterval.count());
        co_await shards_.invoke_on_all([snapEntries, snapBytes, snapMinIntervalS](ShardRaftPlane& p) {
            if (snapEntries || snapBytes || snapMinIntervalS)
                p.setSnapshotPolicy(snapEntries.value_or(ReplicatedVShardHost::kSnapshotEntryThreshold),
                                    snapBytes.value_or(ReplicatedVShardHost::kSnapshotBytesThreshold),
                                    std::chrono::seconds(snapMinIntervalS.value_or(
                                        static_cast<uint64_t>(ReplicatedVShardHost::kMinSnapshotInterval.count()))));
            p.startTicking();
            // THE SNAPSHOT PRODUCER TRIGGER (debt D-6). Started AFTER every group on the
            // shard exists, so the first sweep sees the real group set. Before this,
            // `snapshotVShard` had no production caller at all: nothing ever compacted, so
            // every Raft log grew until a restart replayed the whole thing.
            p.startSnapshotTrigger();
            return seastar::make_ready_future<>();
        });
        replicated_ = true;
        startLeadershipBalancer();
    }
    co_return;
}

// Register ONE peer on EVERY plane it belongs to, from a SINGLE DNS resolution
// (write-scaleout 4b-iii).
//
// This used to be two independent loops, each with its own try/catch around its own
// `resolveHost` call: one registered the data-plane address on the shard-0 client, the
// other registered the Raft address AND the per-shard data address. A DNS hiccup that hit
// one loop and not the other -- entirely possible, they are separate lookups seconds apart
// during a rolling start -- left the node with an ASYMMETRIC view of that peer: e.g. Raft
// replicating to it while every forwarded write reported "unknown peer", or the reverse.
// Nothing ever re-resolved, so the asymmetry was permanent until a restart, and it was
// invisible: both loops only warn.
//
// One resolution, one registration of every plane, all-or-nothing per peer.
seastar::future<bool> ClusterDataPlane::registerPeer(NodeId id, std::string addr, bool replicated) {
    const HostPort hp = parseHostPort(addr);
    std::optional<seastar::net::inet_address> a;
    try {
        a = co_await resolveHost(hp.host);
    } catch (const std::exception& e) {
        timestar::http_log.warn("cluster: peer {} ({}) unresolved: {} (will retry; routes to it fail meanwhile)", id,
                                addr, e.what());
        co_return false;
    }
    const seastar::socket_address dataAddr(*a, static_cast<uint16_t>(hp.port + kDataPlanePortOffset));
    // The shard-0 client-only instance (query/metadata fan-out) always needs it.
    rpc_->addPeer(id, dataAddr);
    if (replicated) {
        const seastar::socket_address raftAddr(*a, static_cast<uint16_t>(hp.port + kRaftPortOffset));
        co_await shards_.invoke_on_all([id, raftAddr, dataAddr](ShardRaftPlane& p) {
            p.addRaftPeer(id, raftAddr);
            p.addDataPeer(id, dataAddr);
        });
    }
    co_return true;
}

seastar::future<> ClusterDataPlane::registerAllPeers(bool replicated) {
    unresolvedPeers_.clear();
    for (const auto& [id, addr] : rt_->peerAddresses) {
        if (id == rt_->selfId)
            continue;
        if (!co_await registerPeer(id, addr, replicated))
            unresolvedPeers_[id] = addr;
    }
    if (!unresolvedPeers_.empty())
        startPeerResolver(replicated);
    co_return;
}

// One pass of the re-resolution loop. A NAMED MEMBER COROUTINE, deliberately -- see
// startPeerResolver.
seastar::future<> ClusterDataPlane::resolvePendingPeers(bool replicated) {
    // Iterate a COPY. The pass suspends in DNS on every entry, and `unresolvedPeers_` is a
    // member that this same pass rewrites at the end; iterating it directly would hold an
    // iterator (and a reference to its key/value) across those suspensions.
    std::map<NodeId, std::string> pending = unresolvedPeers_;
    std::map<NodeId, std::string> still;
    for (const auto& [id, addr] : pending) {
        if (!co_await registerPeer(id, addr, replicated))
            still[id] = addr;
        else
            timestar::http_log.info("cluster: peer {} ({}) resolved and registered on retry", id, addr);
    }
    unresolvedPeers_ = std::move(still);
    if (unresolvedPeers_.empty())
        peerResolveTimer_.cancel();
    co_return;
}

void ClusterDataPlane::startPeerResolver(bool replicated) {
    // Resolution is BEST-EFFORT at startup -- a peer that is not yet up during a rolling
    // start must not fail THIS node's boot. But "best effort, once, forever" is what made
    // an unresolved peer a permanent hole: the address was never looked up again, so a
    // peer whose DNS record appeared thirty seconds later stayed unreachable for the
    // lifetime of the process. This retries until every peer is registered, then stops.
    static constexpr auto kInterval = std::chrono::seconds(5);
    peerResolveTimer_.set_callback([this, replicated] {
        if (peerResolveRunning_ || peerResolveGate_.is_closed())
            return;  // never overlap passes
        peerResolveRunning_ = true;
        // NOT a coroutine lambda -- the same rule startLeadershipBalancer obeys, and for
        // the same reason. `with_gate` invokes a TEMPORARY closure; a lambda-coroutine
        // keeps its captures in the CLOSURE OBJECT, which dies at the end of the full
        // expression while the coroutine is still suspended in DNS (and it is ALWAYS
        // suspended -- unresolvedPeers_ holds hostnames precisely because they did not
        // resolve). On resume it would read `this` and `replicated` from freed stack.
        // At RF=1 a garbage `replicated` reading true is worse than a crash: it dispatches
        // invoke_on_all into a sharded<ShardRaftPlane> that was never started.
        //
        // A named member coroutine keeps its captures in its own frame, which the gate
        // holds alive; the closure below is a plain lambda that only launches it.
        (void)seastar::with_gate(peerResolveGate_, [this, replicated] {
            return resolvePendingPeers(replicated).then_wrapped([this](seastar::future<> f) {
                // Consume the result: a DNS failure inside the pass is expected (that is
                // what the retry is for) and must not surface as an ignored exceptional
                // future. Leaving the flag set would also silently kill the loop.
                f.ignore_ready_future();
                peerResolveRunning_ = false;
            });
        });
    });
    peerResolveTimer_.arm_periodic(kInterval);
}

seastar::future<> ClusterDataPlane::stop() {
    // The peer re-resolution loop touches rpc_ and shards_; quiesce it first.
    peerResolveTimer_.cancel();
    if (!peerResolveGate_.is_closed())
        co_await peerResolveGate_.close();
    // Stop the balancing loop FIRST and drain any in-flight pass: it touches the Raft
    // groups, so it must be quiescent before rdp_ tears them down.
    balanceTimer_.cancel();
    if (!balanceGate_.is_closed())
        co_await balanceGate_.close();
    // The per-shard Raft planes (groups + tick timers) BEFORE the transports they use.
    if (shardsStarted_) {
        co_await shards_.stop();
        shardsStarted_ = false;
    }
    if (rpc_)
        co_await rpc_->stop();
    co_return;
}

seastar::future<> ClusterDataPlane::write(data::WriteBatch batch) {
    // RF=3: replicate to each VShard's Raft leader (durable quorum commit). RF=1/M2:
    // apply directly on the owner. The replicated path is writeFromShard's -- the two
    // were separate functions with identical bodies once Phase 1c let any shard write.
    if (replicated_)
        return writeFromShard(std::move(batch));
    if (!router_)
        throw std::runtime_error("ClusterDataPlane::write before start");
    return router_->write(std::move(batch));
}

seastar::future<QueryResponse> ClusterDataPlane::query(QueryRequest request) {
    if (replicated_)
        co_return co_await queryReplicated(std::move(request));
    if (!coord_)
        throw std::runtime_error("ClusterDataPlane::query before start");
    co_return co_await coord_->query(std::move(request));
}

seastar::future<QueryResponse> ClusterDataPlane::queryReplicated(QueryRequest request) {
    // RF=3 leader read (M3 read-failover): route each VShard's read to its CURRENT
    // Raft leader (not the static primary), so reads follow failover and never
    // double-count a series replicated on every node. Group all VShards by their
    // current leader, query each leader for ONLY those VShards (req.vshards restricts
    // its discovery), and merge the partials into the single answer.
    //
    // RF < N (debt D-13). This node can only resolve the leader of a VShard it HOSTS --
    // a group it does not replicate is absent from its Raft registry, and `leaderOf`
    // answers kNoNode for it, which is indistinguishable from "hosted, no leader
    // elected". Counting those as leaderless failed EVERY read on any cluster where a
    // node does not host everything: a 5-node RF=3 coordinator hosts ~2458 of 4096, so
    // ~1638 VShards were permanently "leaderless" and the query failed
    // QUERY_INCOMPLETE after its retries, forever.
    //
    // The fix mirrors the WRITE path. A VShard we do not host is routed by the
    // PLACEMENT DIRECTORY (its primary replica, or a leader we learned earlier), and
    // that node -- which does host it, so its own registry knows -- either answers or
    // REDIRECTS us to the real leader, exactly as a stale-primary write reject carries
    // a leader hint. Leaderless therefore keeps its original meaning: a node that HOSTS
    // the group reports no elected leader, or no holder is reachable at all. Both still
    // fail the read closed.
    //
    // What does NOT change: at RF == N every VShard is hosted here, no VShard is ever
    // handed to the directory, no request carries a resolve list, and the loop below is
    // the pre-D-13 one instruction for instruction.
    const data::NodeId self = rt_->selfId;
    std::vector<PartialAggregationResult> allPartials;
    std::vector<timestar::http::SeriesResult> allNonNumeric;
    std::vector<std::string> incompleteReasons;
    std::exception_ptr firstErr;
    std::set<data::NodeId> unreachableLeaders;

    // Every VShard still owing an answer. Each one leaves this set exactly once -- when
    // a target answers for it -- so no VShard is dropped and none is asked twice.
    std::set<uint16_t> outstanding;
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs)
        outstanding.insert(vs);

    // A VShard is momentarily leaderless during a leadership TRANSFER (the old leader
    // has stepped down, the new one has not yet won). Failing the query outright there
    // turns routine background rebalancing into user-visible read errors -- measured at
    // ~4.6% of queries while the balancer was actively moving leadership. Re-gather a
    // few times first: a transfer completes in milliseconds, so this converts a
    // transient window into a small latency bump. A genuinely leaderless VShard (lost
    // quorum) still fails closed after the retries.
    static constexpr int kLeaderRetries = 4;
    static constexpr auto kLeaderRetryDelay = std::chrono::milliseconds(25);
    // Rounds spent following REDIRECTS rather than waiting for an election. They are a
    // separate budget because they are not the same event: a redirect is progress (we
    // learned where the leader is and re-ask immediately, no sleep), whereas a
    // leaderless retry is a wait. Charging redirects to kLeaderRetries would let a
    // cluster that redirects once -- the ordinary RF < N cold-cache case -- spend most
    // of its election tolerance before the first election even mattered. Two rounds is
    // enough for hint -> leader; a hint war beyond that fails closed like any other
    // unresolvable read.
    static constexpr int kRedirectRounds = 2;

    int leaderlessRetries = 0, redirectRounds = 0;
    size_t leaderless = 0;
    std::vector<uint16_t> unassigned;
    while (true) {
        leaderless = 0;
        unassigned.clear();
        // Groups live across cores now, so collect each shard's leadership view. Only
        // VShards this node HOSTS appear -- see gatherLeaders.
        const auto leaders = co_await gatherLeaders();
        // The routing decision itself is a pure function of (outstanding, our live
        // leadership view, the hints we have learned, the placement map) -- extracted
        // so it can be unit-tested against a directory where this node hosts a strict
        // subset, which is the configuration that was permanently broken.
        data::ReadRouting plan = data::planReadRouting(outstanding, leaders, readLeaderHints_, *dir_, self);
        leaderless = plan.leaderless;
        unassigned = plan.unassigned;
        auto& byLeader = plan.byNode;
        auto& resolveAt = plan.resolveAt;

        if (!unassigned.empty())
            break;  // fail closed below; retrying cannot assign a VShard
        if (leaderless > 0 && leaderlessRetries < kLeaderRetries) {
            ++leaderlessRetries;
            co_await seastar::sleep(kLeaderRetryDelay);
            continue;  // re-gather before dispatching anything
        }
        if (leaderless > 0)
            break;  // budget spent: fail closed below

        std::vector<seastar::future<data::NodeQueryPartial>> pending;
        std::vector<data::NodeId> pendingLeaders;           // parallel to `pending`
        std::vector<std::vector<uint16_t>> pendingVShards;  // parallel to `pending`
        pending.reserve(byLeader.size());
        pendingLeaders.reserve(byLeader.size());
        pendingVShards.reserve(byLeader.size());
        for (auto& [leader, vshards] : byLeader) {
            data::NodeQueryRequest nq;
            nq.request = request;
            // COPY, and it must stay a copy that happens BEFORE the move into
            // pendingVShards below: an empty `vshards` means "no restriction" at the far
            // end (EngineLocalStore::queryLocal), so moving out of `vshards` first would
            // ask this target for its WHOLE local answer -- every VShard it holds, most of
            // which the coordinator is simultaneously asking someone else about. That is a
            // silent double count of every replicated series, one line away.
            nq.vshards = vshards;
            if (auto r = resolveAt.find(leader); r != resolveAt.end())
                nq.resolveVShards = std::move(r->second);
            pendingLeaders.push_back(leader);
            pendingVShards.push_back(std::move(vshards));
            if (leader == self)
                pending.push_back(local_->queryLocal(std::move(nq)));
            else
                pending.push_back(rpc_->queryNode(leader, std::move(nq)));
        }

        bool learnedHint = false;
        for (size_t i = 0; i < pending.size(); ++i) {
            auto& f = pending[i];
            try {
                data::NodeQueryPartial part = co_await std::move(f);
                if (!part.incompleteReasons.empty()) {
                    for (auto& r : part.incompleteReasons)
                        incompleteReasons.push_back(std::move(r));
                    continue;
                }
                // A redirected VShard is NOT in this node's partials (its read filter
                // excluded it), so it stays outstanding and is re-asked at the node
                // named. Everything else this target was asked for is answered, once.
                // Redirect bookkeeping (which VShards this target answered, which hints
                // to keep) is in data::applyReadRedirects, where it is unit-tested.
                if (data::applyReadRedirects(pendingLeaders[i], pendingVShards[i], part.redirects, outstanding,
                                             readLeaderHints_))
                    learnedHint = true;
                allPartials.insert(allPartials.end(), std::make_move_iterator(part.partials.begin()),
                                   std::make_move_iterator(part.partials.end()));
                allNonNumeric.insert(allNonNumeric.end(), std::make_move_iterator(part.nonNumeric.begin()),
                                     std::make_move_iterator(part.nonNumeric.end()));
            } catch (...) {
                // A REMOTE leader we could not reach is an availability problem, not an
                // internal error: the node is down (or partitioned) but its VShards have
                // live replicas that will elect a new leader. Record it so we can wake
                // those groups and answer honestly. A failure of our own LOCAL read is a
                // genuine internal error and still propagates.
                if (pendingLeaders[i] != self) {
                    unreachableLeaders.insert(pendingLeaders[i]);
                    // AND FORGET EVERY HINT THAT POINTED AT IT. A hint is otherwise only
                    // dropped on a reply, which a dead node cannot send, so a cached
                    // redirect naming a node that then died failed every subsequent read
                    // permanently. See applyReadTargetUnreachable for the full note.
                    data::applyReadTargetUnreachable(pendingLeaders[i], pendingVShards[i], readLeaderHints_);
                } else if (!firstErr) {
                    firstErr = std::current_exception();
                }
            }
        }

        if (!unreachableLeaders.empty() || firstErr || !incompleteReasons.empty())
            break;  // answered below; retrying an incomplete/unreachable read is the caller's job
        if (outstanding.empty())
            break;  // every VShard answered exactly once
        if (learnedHint && redirectRounds < kRedirectRounds) {
            ++redirectRounds;
            continue;  // we know somewhere new to ask -- no reason to sleep first
        }
        if (leaderlessRetries < kLeaderRetries) {
            ++leaderlessRetries;
            co_await seastar::sleep(kLeaderRetryDelay);
            continue;
        }
        break;  // budget spent with VShards still unanswered: fail closed below
    }

    if (!unreachableLeaders.empty()) {
        // Those VShards are hibernating behind a dead leader, so their election
        // timeout is stretched ~10x (25-50s of cluster-wide read failure, measured).
        // Wake them so they campaign at the normal timeout instead, then report
        // QUERY_INCOMPLETE -- distinguishable from an empty result and from an
        // internal error, and correct to retry. Waking is idempotent and bounded.
        for (data::NodeId dead : unreachableLeaders)
            co_await shards_.invoke_on_all([dead](ShardRaftPlane& p) { (void)p.wakeFollowersOf(dead); });
        std::string nodes;
        for (data::NodeId d : unreachableLeaders)
            nodes += (nodes.empty() ? "" : ",") + std::to_string(d);
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage =
            "leader node(s) " + nodes + " unreachable; their VShards were woken to re-elect -- retry shortly";
        co_return r;
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    if (!incompleteReasons.empty()) {
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage = "a leader could not answer";
        co_return r;
    }
    if (!unassigned.empty()) {
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage = std::to_string(unassigned.size()) + " VShard(s) are unassigned in the placement map (first: " +
                         std::to_string(unassigned.front()) + ")";
        co_return r;
    }
    if (leaderless > 0 || !outstanding.empty()) {
        // A VShard with no leader (or one no holder would answer for) may hold matching
        // data we cannot read -> fail closed (QUERY_INCOMPLETE), never a silent partial.
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage =
            std::to_string(leaderless > 0 ? leaderless : outstanding.size()) + " VShard(s) have no elected leader";
        co_return r;
    }
    co_return co_await finalizer_->finalizeClusterPartials(std::move(request), std::move(allPartials),
                                                           std::move(allNonNumeric));
}

seastar::future<ClusterDataPlane::Status> ClusterDataPlane::status() const {
    Status st;
    if (!rt_)
        co_return st;
    st.self = rt_->selfId;
    st.peers = rt_->peerAddresses;
    st.replicated = replicated_;
    st.replicationFactor = rf_;
    if (!replicated_ || !shardsStarted_)
        co_return st;
    std::vector<data::NodeId> peers;
    for (const auto& [id, addr] : rt_->peerAddresses)
        peers.push_back(id);
    const data::NodeId self = rt_->selfId;
    auto& shards = const_cast<seastar::sharded<ShardRaftPlane>&>(shards_);
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        auto c = co_await shards.invoke_on(sh, [self, peers](ShardRaftPlane& p) { return p.counts(self, peers); });
        st.vshardsHostedHere += c.hosted;
        st.vshardsLedHere += c.led;
        st.vshardsLeaderless += c.leaderless;
        for (const auto& [peer, n] : c.peerCaughtUp)
            st.peerCaughtUp[peer] += n;
        auto sc = co_await shards.invoke_on(sh, [](ShardRaftPlane& p) { return p.snapshotCounts(); });
        st.snapshotsTaken += sc.taken;
        st.snapshotsRefusedTooLarge += sc.refusedTooLarge;
        st.snapshotsSkippedUnflushed += sc.skippedUnflushed;
        st.snapshotsSkippedPendingConversion += sc.skippedPendingConversion;
        st.snapshotSweeps += sc.sweeps;
        st.snapshotMaxEntriesSince = std::max(st.snapshotMaxEntriesSince, sc.maxEntriesSinceSeen);
        st.snapshotChunksSent += sc.chunksSent;
        st.snapshotsInstalled += sc.installed;
        st.snapshotsUndeliverable += sc.undeliverable;
        st.snapshotTransfersRestarted += sc.transfersRestarted;
        st.snapshotTransfersAbandoned += sc.transfersAbandoned;
        st.snapshotTriggerEnabled = st.snapshotTriggerEnabled || sc.triggerEnabled;
        auto jc = co_await shards.invoke_on(sh, [](ShardRaftPlane& p) { return p.journalCounts(); });
        st.journalFsyncs += jc.fsyncs;
        st.journalSyncRequests += jc.syncRequests;
        st.journalShared = st.journalShared || jc.shared;
    }
    co_return st;
}

void ClusterDataPlane::startLeadershipBalancer() {
    // A bounded pass every few seconds, split across shards. Without it a fresh
    // cluster leaves ALL leadership on the first node to start (it wins every
    // election), putting all write coordination and leader-reads on one node.
    static constexpr auto kInterval = std::chrono::seconds(5);
    static constexpr size_t kBudget = 256;
    balanceTimer_.set_callback([this] {
        if (balanceRunning_ || balanceGate_.is_closed())
            return;  // never overlap passes
        balanceRunning_ = true;
        // NOT a coroutine lambda: with_gate invokes a TEMPORARY closure, and a
        // coroutine lambda's frame keeps referencing it after destruction (reads freed
        // memory, and the flag never clears so the loop silently dies).
        (void)seastar::with_gate(balanceGate_, [this] {
            return rebalanceLeadership(kBudget).then_wrapped([this](seastar::future<size_t> f) {
                f.ignore_ready_future();  // best effort; next tick retries
                balanceRunning_ = false;
            });
        });
    });
    balanceTimer_.arm_periodic(kInterval);
}

seastar::future<size_t> ClusterDataPlane::rebalanceLeadership(size_t maxTransfers) {
    if (!replicated_ || !shardsStarted_ || maxTransfers == 0)
        co_return 0;
    std::vector<data::NodeId> peers;
    for (const auto& [id, addr] : rt_->peerAddresses)
        peers.push_back(id);
    const data::NodeId self = rt_->selfId;
    // Each shard balances its own VShards. assignCore spreads VShards evenly over
    // shards and every shard sees the same node set, so per-shard balance == cluster
    // balance. Split the budget so a pass stays bounded overall.
    const size_t per = std::max<size_t>(1, maxTransfers / seastar::smp::count);
    size_t total = 0;
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        total += co_await shards_.invoke_on(
            sh, [per, self, peers](ShardRaftPlane& p) { return p.rebalance(per, self, peers); });
    }
    co_return total;
}

seastar::future<> ClusterDataPlane::writeFromShard(data::WriteBatch batch) {
    // Split the series by the shard that owns their VShard's Raft group, then hand each
    // slice to that shard's plane (which resolves the leader for its own VShards
    // locally and forwards to a remote leader when needed). Safe to call from the shard
    // the HTTP request arrived on (see the header): it touches only `shards_`.
    //
    // Guard the documented precondition the way write()/metadata() guard theirs: called
    // before start(), or on an RF=1 node, `shards_` holds no planes and every slice
    // would silently resolve against a null one.
    if (!replicated_ || !shardsStarted_)
        throw std::runtime_error("ClusterDataPlane::writeFromShard requires replicated mode after start()");
    return writeSlicesToOwningShards(shards_, std::move(batch), dir_.get());
}

seastar::future<bool> ClusterDataPlane::proposeBatch(data::WriteBatch batch) {
    // A peer forwarded this batch because we lead those VShards. Replicate each
    // slice through the Raft group on its owning shard.
    return proposeSlicesToOwningShards(shards_, std::move(batch), dir_.get());
}

seastar::future<std::map<uint16_t, data::NodeId>> ClusterDataPlane::gatherLeaders() const {
    std::map<uint16_t, NodeId> leaders;
    auto& shards = const_cast<seastar::sharded<ShardRaftPlane>&>(shards_);
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        auto part = co_await shards.invoke_on(sh, [dirp = dir_.get()](ShardRaftPlane& p) {
            std::map<uint16_t, data::NodeId> out;
            if (!p.ready())
                return out;
            auto& host = p.plane().host();
            for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs) {
                if (shardOwningVShard(vs, dirp) != seastar::this_shard_id())
                    continue;
                // ONLY VShards this node HOSTS (debt D-13). `leaderOf` returns kNoNode
                // for a group we do not replicate, which reads identically to "hosted,
                // no leader elected" -- and the caller must not confuse the two: at
                // RF < N most of the map is simply not ours, and treating it as
                // leaderless failed every read on the cluster. An ABSENT key means "ask
                // the placement directory"; a present kNoNode means "we hold a replica
                // and there is no leader", which is fail-closed.
                if (!host.hosts(vs))
                    continue;
                out[vs] = host.leaderOf(vs);
            }
            return out;
        });
        for (const auto& [vs, l] : part)
            leaders[vs] = l;
    }
    co_return leaders;
}

seastar::future<data::MetadataResult> ClusterDataPlane::metadata(data::MetadataRequest request) {
    if (!dir_)
        throw std::runtime_error("ClusterDataPlane::metadata before start");
    // Scatter to every owner node (self served in-process), await all, then union
    // items / sum cardinality. A node failure fails the whole request (a partial
    // metadata answer would be silently incomplete, same contract as queries).
    const std::set<NodeId> targets = dir_->ownerNodes();
    const NodeId self = rt_->selfId;
    std::vector<seastar::future<data::MetadataResult>> pending;
    pending.reserve(targets.size());
    for (NodeId t : targets) {
        if (t == self)
            pending.push_back(local_->queryMetadata(request));
        else
            pending.push_back(rpc_->queryMetadata(t, request));
    }
    std::set<std::string> items;
    double cardinality = 0.0;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            data::MetadataResult r = co_await std::move(f);
            items.insert(r.items.begin(), r.items.end());
            cardinality += r.cardinality;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    data::MetadataResult out;
    out.items.assign(items.begin(), items.end());
    out.cardinality = cardinality;
    co_return out;
}

}  // namespace timestar::cluster
