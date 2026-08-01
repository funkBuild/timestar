#include "cluster_data_plane.hpp"

#include "../../core/vshard.hpp"
#include "../../utils/line_parser.hpp"
#include "../../utils/logger.hpp"  // timestar::http_log
#include "../../utils/series_key.hpp"
#include "../data/read_routing.hpp"
#include "checkquorum_policy.hpp"
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
#include <unordered_set>

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

// The PARSE and the DECISION live in `checkquorum_policy.hpp` and are pinned there
// (D-30). Only the environment read and the logging are here: the property the release
// ordering depends on -- that no runtime input can turn the guard ON -- is a claim about
// `resolveCheckQuorum`, and a claim nothing can call is a claim nothing can check.
//
// WHICH LEAVES *THIS FUNCTION* AS THE REMAINING WAY TO REVOKE IT, and no test covers this
// body: it reads the real environment of a real process, so a unit test cannot exercise it
// without mutating global state the rest of the suite shares. The pinned property protects
// the DECISION, not this call site. Concretely, the edit shapes that would defeat it, none
// of which would fail a single test:
//
//   * returning something other than `on` from any branch below -- e.g. "return true" in
//     the EnableRefused arm, which reads like completing an obvious omission;
//   * `resolveCheckQuorum(kCheckQuorumDefault || ov == ...EnableRefused, ov)`, or any other
//     expression that lets the override reach the first argument;
//   * a second getenv here (`..._FORCE`, `..._ENABLE`) consulted after the resolve;
//   * assigning `ropts.checkQuorum` at the construction site from anything but this
//     function.
//
// The rule is one line: **the override may only ever narrow the build default.** If a
// future change needs an enable knob, it needs ADR 0005 mechanism (c) first -- that is the
// whole of debt D-30. The construction site backs this up with a runtime fail-closed check
// on the value this returns, so the first four shapes above throw at node start rather than
// shipping; only an edit that also removes THAT check gets through.
bool checkQuorumEnabled() {
    const char* e = std::getenv("TIMESTAR_CLUSTER_CHECKQUORUM");
    const auto ov = parseCheckQuorumOverride(e);
    const bool on = resolveCheckQuorum(kCheckQuorumDefault, ov);

    if (!kCheckQuorumDefault) {
        // Nothing to disable, and nothing may enable. Say so if someone tried, so the
        // knob's absence of effect is visible rather than mysterious.
        if (ov != CheckQuorumOverride::None)
            timestar::http_log.info(
                "cluster: TIMESTAR_CLUSTER_CHECKQUORUM='{}' has no effect -- Raft CheckQuorum is OFF in this build "
                "(debt D-9/D-29) and this override is disable-only",
                e);
        return on;
    }
    switch (ov) {
        case CheckQuorumOverride::Disable:
            timestar::http_log.warn(
                "cluster: Raft CheckQuorum DISABLED on this node by TIMESTAR_CLUSTER_CHECKQUORUM='{}'. A partitioned "
                "or stale leader will now keep accepting proposals it cannot commit until each one's own deadline, "
                "and leader-only reads on the losing side of a partition converge per request rather than promptly. "
                "Nothing is unsafe (commit still needs a quorum ack); this is the configuration that shipped before "
                "debt D-9.",
                e);
            break;
        case CheckQuorumOverride::EnableRefused:
            timestar::http_log.warn(
                "cluster: TIMESTAR_CLUSTER_CHECKQUORUM='{}' is ignored -- the override is DISABLE-ONLY and "
                "CheckQuorum is already on. Enabling it per node is exactly the mixed-version hazard ADR 0005 exists "
                "to prevent (one node running the disruption guard while an older peer drops its transfer votes).",
                e);
            break;
        case CheckQuorumOverride::Invalid:
            timestar::http_log.error(
                "cluster: TIMESTAR_CLUSTER_CHECKQUORUM='{}' is not a boolean (0/false/no/off disable, case- and "
                "whitespace-insensitive; nothing enables); leaving CheckQuorum ON",
                e);
            break;
        case CheckQuorumOverride::None:
            break;
    }
    return on;
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

void ClusterDataPlane::validateCoreTopology(unsigned coreCount, uint16_t replicationFactor) {
    if (replicationFactor > 1 && !timestar::vshardsCohesiveOnCores(coreCount))
        throw std::invalid_argument("ClusterDataPlane: replicated cluster requires a core count that divides " +
                                    std::to_string(timestar::VIRTUAL_SHARD_COUNT) +
                                    " so every VShard has a complete single-core snapshot; got " +
                                    std::to_string(coreCount));
}

seastar::future<> ClusterDataPlane::start(const ClusterConfig& cfg, seastar::sharded<Engine>& engines) {
    std::exception_ptr startupFailure;
    try {
        co_await startImpl(cfg, engines);
        co_return;
    } catch (...) {
        startupFailure = std::current_exception();
    }

    // startImpl() can fail after shards_.start() and after hundreds of VShard
    // journals have opened (for example, EMFILE part-way through group
    // creation).  Letting the exception escape at that point makes
    // sharded<ShardRaftPlane>'s process-global destructor execute a trap and
    // replace the useful startup error with SIGILL.  Stop everything that did
    // start, but preserve the original exception even if cleanup itself has a
    // secondary failure.
    try {
        co_await stop();
    } catch (const std::exception& e) {
        timestar::http_log.error("ClusterDataPlane cleanup after failed startup also failed: {}", e.what());
    } catch (...) {
        timestar::http_log.error("ClusterDataPlane cleanup after failed startup also failed with an unknown error");
    }
    std::rethrow_exception(startupFailure);
}

seastar::future<> ClusterDataPlane::startImpl(const ClusterConfig& cfg, seastar::sharded<Engine>& engines) {
    // Force the in-flight write budget to resolve (and LOG itself) during startup rather
    // than on the first write, so a mis-set TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES is
    // visible in the boot log instead of being inferred from a wall of 503s.
    (void)WriteAdmission::limitBytes();
    validateCoreTopology(seastar::smp::count, cfg.replication_factor);
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
        if (!journalIdentity_)
            throw std::invalid_argument(
                "ClusterDataPlane: replicated production startup requires a persistent journal identity");
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
            const JournalIdentity journalIdentity = *journalIdentity_;
            co_await shards_.invoke_on_all(
                [enginesPtr = enginesPtr_, peers, dirp, selfId, jroot, journalIdentity](ShardRaftPlane& p) {
                    return p.init(enginesPtr, peers, dirp, selfId, jroot, kRaftTickPeriod, journalIdentity);
                });
        }

        // FENCE NODE-LOCAL READS ON NODE-LOCAL APPLY LAG (debt D-36). Wired only in the
        // replicated branch, because only here is there a committed log sitting above the
        // Engine: at RF=1 the Engine IS the state and there is nothing a query could be
        // behind. One hop per shard, and every shard's fast path is an integer compare
        // per hosted group, so a caught-up node pays essentially nothing.
        local_->setApplyFence([this]() {
            const auto budget = ShardRaftPlane::applyFenceBudget();
            return shards_.map_reduce0([budget](ShardRaftPlane& p) { return p.awaitApplyCatchUp(budget); }, true,
                                       std::logical_and<bool>{});
        });
        // A node-local leg is subject to the same quorum ReadIndex as a peer leg.
        // Without this callback, queries coordinated on a partitioned former leader
        // bypass the peer-facing LeaderFilteredNodeStore and can return stale data.
        local_->setLeaderReadFence(
            [this](const std::vector<uint16_t>& vshards) { return shards_.local().quorumLeaderReadFence(vshards); });

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
        co_await shards_.invoke_on_all(
            [raftAddr, tls = tls_](ShardRaftPlane& p) { return p.startTransport(raftAddr, tls); });

        // Register peers on BOTH planes, from ONE resolution each, now that every
        // transport that needs them exists (write-scaleout 4b-iii).
        co_await registerAllPeers(true);

        // Instantiate each VShard's group ON ITS OWNING SHARD (see the many-group
        // timing note below).
        // Every one of these is a TICK COUNT whose meaning depends on kRaftTickPeriod, so
        // they are declared together with it and with the compile-time relationships
        // between them -- see cluster_data_plane.hpp (debt D-20).
        raft::RaftOptions ropts;
        ropts.heartbeatTimeout = kRaftHeartbeatTicks;
        ropts.electionTimeoutMin = kRaftElectionTicksMin;
        ropts.electionTimeoutMax = kRaftElectionTicksMax;
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
        ropts.transferTimeout = kRaftTransferTicks;
        // The two relationships that matter are asserted at COMPILE time in the header,
        // against the constants above and against the router's kDeadline. These runtime
        // checks are the same two conditions re-stated for the values actually installed,
        // which is what a future runtime-configurable timing knob would need: a
        // static_assert cannot see a value read from a config file.
        if (ropts.transferTimeout >= ropts.electionTimeoutMin)
            // Same fail-closed reasoning as the chunk timeout below: a transfer window at
            // or past an election timeout is the pre-D-20 behaviour wearing a new name,
            // and the whole point is that it is much shorter than the write deadline.
            throw std::runtime_error(
                "cluster: Raft transferTimeout must be well below electionTimeoutMin -- the leader refuses every "
                "proposal while a transfer is in flight, so an abandon window as long as an election outlasts the "
                "write deadline and makes one mis-aimed transfer a failed batch");
        if (raftTicksToWallClock(ropts.transferTimeout) >= data::ReplicatedBatchWriteRouter::kDeadline)
            // THE WALL-CLOCK HALF, and the load-bearing one: D-20's whole claim is that a
            // batch blocked by a transfer outlasts the refusal and retries into the resumed
            // leader inside its BASE deadline. A tick period or a deadline that breaks that
            // must not boot.
            throw std::runtime_error(
                "cluster: Raft transferTimeout must fit inside the base write deadline -- a longer abandon window "
                "makes one mis-aimed leadership transfer a failed batch again (debt D-20)");
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
        // FAIL-CLOSED AT THE ONLY SITE THAT MATTERS (debt D-30). `resolveCheckQuorum` is
        // pure and pinned; `checkQuorumEnabled()` above is not covered by any test,
        // because it reads a real process environment. This assertion is what makes the
        // uncovered part unable to lie: whatever that function grows into, the guard
        // cannot end up ON while the BUILD default is off -- which is the exact property
        // the release ordering rests on (no runtime input may enable it; see ADR 0005
        // mechanism (c)). Cheap: once per node start.
        if (ropts.checkQuorum && !kCheckQuorumDefault)
            throw std::runtime_error(
                "cluster: CheckQuorum resolved ON while the build default is OFF -- the override is disable-only by "
                "design (ADR 0005 mechanism (c) is not built, so enabling per node is the mixed-version hazard that "
                "ADR exists to prevent). Flip kCheckQuorumDefault to enable it, in a build, for the whole cluster");
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
    // Per-VShard targets already proved unusable (transport failure) or already
    // asked to resolve leadership. The latter prevents a reachable follower that
    // redirects to a dead leader from being selected as the resolver forever.
    std::map<uint16_t, std::set<data::NodeId>> excludedReadTargets;
    // Peers that answered the handshake but predate the leader-resolve read protocol
    // (debt D-25). Kept apart from `unreachableLeaders` because the two want opposite
    // treatment: an unreachable leader is woken and the client is told to retry, while a
    // peer mid-rolling-upgrade will answer exactly the same way until it is upgraded.
    std::set<data::NodeId> unversionedPeers;

    // Every VShard still owing an answer. Each one leaves this set exactly once -- when
    // a target answers for it -- so no VShard is dropped and none is asked twice.
    std::set<uint16_t> outstanding;
    for (uint16_t vs = 0; vs < timestar::VIRTUAL_SHARD_COUNT; ++vs)
        outstanding.insert(vs);

    // A VShard is momentarily leaderless during a leadership TRANSFER (the old leader
    // has stepped down, the new one has not yet won). Failing the query outright there
    // turns routine background rebalancing into user-visible read errors -- measured at
    // ~4.6% of queries while the balancer was actively moving leadership. Re-gather for
    // `kReadLeaderlessBudget` first, which converts a transfer window into a latency bump.
    // A genuinely leaderless VShard (lost quorum, or an election under way) still fails
    // closed after the retries -- deliberately, and much sooner than a write gives up; the
    // budget, the asymmetry and the assertions that pin them live in cluster_data_plane.hpp
    // next to the Raft clocks they are derived from (debt D-26, ADR 0006).
    unsigned leaderlessRetries = 0;
    int redirectRounds = 0;
    int replicaFallbackRounds = 0;
    size_t leaderless = 0;
    std::vector<uint16_t> unassigned;
    // THE BUDGET IS WALL CLOCK, NOT ITERATIONS (found in the D-25/D-26 review). Counting
    // sleeps bounds only the SLEEPING: each round also runs `gatherLeaders()` (a sequential
    // invoke_on across every shard) and a full remote fan-out, so a slow or redirect-churning
    // read could spend far more than 1.2 s and sail past the 2.5 s election minimum the
    // static_assert in the header claims to exclude -- making the assertion a statement about
    // arithmetic rather than about behaviour. Checked BETWEEN rounds, so the real bound is the
    // budget plus at most one LOCAL in-flight operation. Remote node-query attempts and
    // their optional version handshakes share `readRpcDeadline`, so a black-holed peer
    // cannot extend the query past the coordinator's wall-clock policy.
    const auto readStart = std::chrono::steady_clock::now();
    const auto readRpcDeadline = seastar::rpc::rpc_clock_type::now() + kReadLeaderlessBudget;
    const auto budgetSpent = [&readStart] {
        return std::chrono::steady_clock::now() - readStart >= kReadLeaderlessBudget;
    };
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
        data::ReadRouting plan =
            data::planReadRouting(outstanding, leaders, readLeaderHints_, *dir_, self, excludedReadTargets);
        leaderless = plan.leaderless;
        unassigned = plan.unassigned;
        auto& byLeader = plan.byNode;
        auto& resolveAt = plan.resolveAt;

        if (!unassigned.empty())
            break;  // fail closed below; retrying cannot assign a VShard
        if (leaderless > 0 && leaderlessRetries < kReadLeaderRetries && !budgetSpent()) {
            ++leaderlessRetries;
            co_await seastar::sleep(kReadLeaderRetryDelay);
            continue;  // re-gather before dispatching anything
        }
        if (leaderless > 0)
            break;  // budget spent: fail closed below

        std::vector<seastar::future<data::NodeQueryPartial>> pending;
        std::vector<data::NodeId> pendingLeaders;           // parallel to `pending`
        std::vector<std::vector<uint16_t>> pendingVShards;  // parallel to `pending`
        // The SUBSET of pendingVShards[i] that target i was asked to RESOLVE, which is the
        // only set it has standing to redirect (see applyReadRedirects). Kept apart from
        // pendingVShards because the two answer different questions.
        std::vector<std::vector<uint16_t>> pendingResolve;  // parallel to `pending`
        pending.reserve(byLeader.size());
        pendingLeaders.reserve(byLeader.size());
        pendingVShards.reserve(byLeader.size());
        pendingResolve.reserve(byLeader.size());
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
            // A holder used to RESOLVE leadership is a one-shot candidate for each
            // VShard in this query. If it redirects us to an excluded/dead leader,
            // the next round must ask another holder rather than loop back here.
            for (uint16_t vs : nq.resolveVShards)
                excludedReadTargets[vs].insert(leader);
            pendingLeaders.push_back(leader);
            pendingResolve.push_back(nq.resolveVShards);  // COPY: nq is moved into the call below
            pendingVShards.push_back(std::move(vshards));
            if (leader == self)
                pending.push_back(local_->queryLocal(std::move(nq)));
            else
                pending.push_back(rpc_->queryNode(leader, std::move(nq), readRpcDeadline));
        }

        bool learnedHint = false;
        bool targetFailed = false;
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
                // to keep, and which redirects it had standing to issue at all) is in
                // data::applyReadRedirects, where it is unit-tested.
                if (data::applyReadRedirects(pendingLeaders[i], pendingVShards[i], pendingResolve[i], part.redirects,
                                             outstanding, readLeaderHints_))
                    learnedHint = true;
                allPartials.insert(allPartials.end(), std::make_move_iterator(part.partials.begin()),
                                   std::make_move_iterator(part.partials.end()));
                allNonNumeric.insert(allNonNumeric.end(), std::make_move_iterator(part.nonNumeric.begin()),
                                     std::make_move_iterator(part.nonNumeric.end()));
            } catch (const data::ReadResolveUnsupportedError&) {
                // The peer is REACHABLE and healthy; it simply predates the resolve/redirect
                // exchange, so it cannot tell us which of these VShards it leads (debt
                // D-25). Degrade KNOWINGLY: fail the read closed naming the peer, rather
                // than counting a follower's possibly-stale answer as a leader read. Not
                // retried and not woken -- neither can change the peer's binary.
                unversionedPeers.insert(pendingLeaders[i]);
            } catch (...) {
                // A REMOTE leader we could not reach is an availability problem, not an
                // internal error: the node is down (or partitioned) but its VShards have
                // live replicas that will elect a new leader. Record it so we can wake
                // those groups and answer honestly. A failure of our own LOCAL read is a
                // genuine internal error and still propagates.
                if (pendingLeaders[i] != self) {
                    unreachableLeaders.insert(pendingLeaders[i]);
                    targetFailed = true;
                    for (uint16_t vs : pendingVShards[i])
                        excludedReadTargets[vs].insert(pendingLeaders[i]);
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

        if (outstanding.empty()) {
            // An earlier target may have failed before an alternate answered all
            // of its VShards. The completed answer is still whole; do not turn a
            // successful fallback into QUERY_INCOMPLETE merely to report the first
            // transport error.
            unreachableLeaders.clear();
            break;  // every VShard answered exactly once
        }
        if (!unversionedPeers.empty() || firstErr || !incompleteReasons.empty())
            break;
        if (targetFailed && replicaFallbackRounds < kReadReplicaFallbackRounds && !budgetSpent()) {
            ++replicaFallbackRounds;
            continue;  // re-plan outstanding VShards onto another placement replica
        }
        if (targetFailed)
            break;
        if (learnedHint && redirectRounds < kReadRedirectRounds && !budgetSpent()) {
            ++redirectRounds;
            continue;  // we know somewhere new to ask -- no reason to sleep first
        }
        if (leaderlessRetries < kReadLeaderRetries && !budgetSpent()) {
            ++leaderlessRetries;
            co_await seastar::sleep(kReadLeaderRetryDelay);
            continue;
        }
        break;  // budget spent with VShards still unanswered: fail closed below
    }

    // THE ORDER OF THESE BRANCHES IS THE DIAGNOSIS, most specific first, and the FIRST of
    // them is a local failure of OUR OWN read (found in the D-25 review). The catch above
    // says a local failure "still propagates", but it was reported after the remote
    // branches, so one unreachable peer in the same round turned a genuine internal error
    // -- a bug, an I/O failure, a decode fault on this node -- into `QUERY_INCOMPLETE:
    // leader node(s) N unreachable`, sending the operator after a healthy peer while the
    // real failure was here. Nothing about a remote node makes a local exception less true,
    // so it goes first and restores the contract the catch already claimed.
    if (firstErr)
        std::rethrow_exception(firstErr);
    if (!unversionedPeers.empty()) {
        // Then the version refusal, BEFORE the unreachable branch: a peer can only land
        // here by ANSWERING the version handshake, so "too old" is the more specific and
        // more actionable of the two, and it must not be reported as an outage the operator
        // will go looking for. Deliberately no wake and no "retry shortly" -- retrying is
        // futile until the rolling upgrade finishes (debt D-25).
        std::string nodes;
        for (data::NodeId n : unversionedPeers)
            nodes += (nodes.empty() ? "" : ",") + std::to_string(n);
        QueryResponse r;
        r.success = false;
        r.errorCode = "QUERY_INCOMPLETE";
        r.errorMessage = "peer node(s) " + nodes + " predate the leader-resolve read protocol (data-plane wire v" +
                         std::to_string(data::kNodeQueryResolveMinVersion) +
                         "); this node cannot verify who leads their VShards -- finish the rolling upgrade";
        co_return r;
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

seastar::future<std::vector<std::string>> ClusterDataPlane::findPatternSeries(data::PatternSeriesSelector selector,
                                                                              uint32_t maxSeries) {
    if (!replicated_ || !shardsStarted_ || !dir_ || !rt_ || !local_ || !rpc_)
        throw std::runtime_error("ClusterDataPlane::findPatternSeries requires replicated mode after start()");
    if (selector.measurement.empty() || maxSeries == 0 || maxSeries > data::kPatternSeriesMaxResults)
        throw std::invalid_argument("pattern-series discovery requires a measurement and a supported result bound");

    // Pin the complete placement value, not a reference into VShardDirectory:
    // group 0 will eventually update that object in place. Every VShard in this
    // expansion is routed against one epoch, and an epoch change before return
    // invalidates the whole read before the HTTP layer can propose any mutation.
    const data::NodeId self = rt_->selfId;
    const control::ControlMap pinnedMap = dir_->map();
    const uint64_t pinnedEpoch = pinnedMap.epoch;
    const data::VShardDirectory pinnedDirectory(self, pinnedMap);

    std::set<uint16_t> outstanding;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        outstanding.insert(vshard);

    std::set<std::string> matchedKeys;
    size_t matchedKeyBytes = 0;
    std::set<data::NodeId> unreachableLeaders;
    std::set<data::NodeId> unsupportedPeers;
    std::map<uint16_t, std::set<data::NodeId>> excludedTargets;
    std::exception_ptr firstError;
    bool limitExceeded = false;
    unsigned leaderlessRetries = 0;
    int redirectRounds = 0;
    int replicaFallbackRounds = 0;
    size_t leaderless = 0;
    std::vector<uint16_t> unassigned;

    const std::unordered_set<std::string> fieldFilter(selector.fields.begin(), selector.fields.end());
    const auto keyMatchesSelector = [&selector, &fieldFilter](const std::string& key) {
        try {
            SeriesKeyParser parsed(key);
            if (timestar::buildSeriesKey(parsed.measurement, parsed.tags, parsed.field) != key ||
                parsed.measurement != selector.measurement ||
                (!fieldFilter.empty() && !fieldFilter.contains(parsed.field)))
                return false;
            for (const auto& [tag, value] : selector.tags) {
                auto found = parsed.tags.find(tag);
                if (found == parsed.tags.end() || found->second != value)
                    return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    };

    const auto started = std::chrono::steady_clock::now();
    const auto rpcDeadline = seastar::rpc::rpc_clock_type::now() + kReadLeaderlessBudget;
    const auto budgetSpent = [&started] { return std::chrono::steady_clock::now() - started >= kReadLeaderlessBudget; };

    while (true) {
        leaderless = 0;
        unassigned.clear();
        const auto leaders = co_await gatherLeaders();
        data::ReadRouting plan =
            data::planReadRouting(outstanding, leaders, readLeaderHints_, pinnedDirectory, self, excludedTargets);
        leaderless = plan.leaderless;
        unassigned = plan.unassigned;
        if (!unassigned.empty())
            break;
        if (leaderless > 0 && leaderlessRetries < kReadLeaderRetries && !budgetSpent()) {
            ++leaderlessRetries;
            co_await seastar::sleep(kReadLeaderRetryDelay);
            continue;
        }
        if (leaderless > 0)
            break;

        std::vector<seastar::future<data::PatternSeriesResult>> pending;
        std::vector<data::NodeId> pendingNodes;
        std::vector<std::vector<uint16_t>> pendingVShards;
        std::vector<std::vector<uint16_t>> pendingResolve;
        pending.reserve(plan.byNode.size());
        pendingNodes.reserve(plan.byNode.size());
        pendingVShards.reserve(plan.byNode.size());
        pendingResolve.reserve(plan.byNode.size());
        for (auto& [node, vshards] : plan.byNode) {
            data::PatternSeriesRequest request;
            request.selector = selector;
            request.vshards = vshards;
            if (auto resolve = plan.resolveAt.find(node); resolve != plan.resolveAt.end())
                request.resolveVShards = std::move(resolve->second);
            request.mapEpoch = pinnedEpoch;
            request.maxSeries = maxSeries;
            for (uint16_t vshard : request.resolveVShards)
                excludedTargets[vshard].insert(node);
            pendingNodes.push_back(node);
            pendingVShards.push_back(vshards);
            pendingResolve.push_back(request.resolveVShards);
            if (node == self)
                pending.push_back(local_->findPatternSeries(std::move(request)));
            else
                pending.push_back(rpc_->findPatternSeries(node, std::move(request), rpcDeadline));
        }

        bool learnedHint = false;
        bool targetFailed = false;
        for (size_t i = 0; i < pending.size(); ++i) {
            try {
                data::PatternSeriesResult result = co_await std::move(pending[i]);
                if (result.limitExceeded) {
                    limitExceeded = true;
                    continue;
                }

                std::set<uint16_t> redirected;
                for (const auto& redirect : result.redirects)
                    if (std::find(pendingResolve[i].begin(), pendingResolve[i].end(), redirect.vshard) !=
                        pendingResolve[i].end())
                        redirected.insert(redirect.vshard);

                // A peer's catalog strings are not routing authority. Re-parse
                // every one, require canonical encoding and selector agreement,
                // and prove its hash belongs to a VShard this exact request asked
                // that peer to answer (and that it did not redirect).
                for (const auto& key : result.seriesKeys) {
                    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
                    if (!keyMatchesSelector(key) ||
                        !std::binary_search(pendingVShards[i].begin(), pendingVShards[i].end(), vshard) ||
                        redirected.contains(vshard)) {
                        throw std::runtime_error(
                            "pattern-series peer returned a non-canonical, non-matching, or foreign-VShard key");
                    }
                    if (!matchedKeys.contains(key)) {
                        const size_t encodedBytes = sizeof(uint32_t) + key.size();
                        if (matchedKeys.size() == maxSeries || encodedBytes > data::kPatternSeriesMaxKeyBytes ||
                            matchedKeyBytes > data::kPatternSeriesMaxKeyBytes - encodedBytes) {
                            limitExceeded = true;
                        } else {
                            matchedKeys.insert(key);
                            matchedKeyBytes += encodedBytes;
                        }
                    }
                }

                if (data::applyReadRedirects(pendingNodes[i], pendingVShards[i], pendingResolve[i], result.redirects,
                                             outstanding, readLeaderHints_))
                    learnedHint = true;
            } catch (const data::PatternSeriesUnsupportedError&) {
                unsupportedPeers.insert(pendingNodes[i]);
            } catch (...) {
                if (pendingNodes[i] != self) {
                    unreachableLeaders.insert(pendingNodes[i]);
                    targetFailed = true;
                    for (uint16_t vshard : pendingVShards[i])
                        excludedTargets[vshard].insert(pendingNodes[i]);
                    data::applyReadTargetUnreachable(pendingNodes[i], pendingVShards[i], readLeaderHints_);
                } else if (!firstError) {
                    firstError = std::current_exception();
                }
            }
        }

        if (limitExceeded || firstError || !unsupportedPeers.empty())
            break;
        if (outstanding.empty()) {
            unreachableLeaders.clear();
            break;
        }
        if (targetFailed && replicaFallbackRounds < kReadReplicaFallbackRounds && !budgetSpent()) {
            ++replicaFallbackRounds;
            continue;
        }
        if (targetFailed)
            break;
        if (learnedHint && redirectRounds < kReadRedirectRounds && !budgetSpent()) {
            ++redirectRounds;
            continue;
        }
        if (leaderlessRetries < kReadLeaderRetries && !budgetSpent()) {
            ++leaderlessRetries;
            co_await seastar::sleep(kReadLeaderRetryDelay);
            continue;
        }
        break;
    }

    if (firstError)
        std::rethrow_exception(firstError);
    if (limitExceeded)
        throw data::DeleteExpansionLimitError("pattern delete exceeds the " + std::to_string(maxSeries) +
                                              "-series or " + std::to_string(data::kPatternSeriesMaxKeyBytes) +
                                              "-encoded-key-byte safety limit");
    if (!unsupportedPeers.empty()) {
        std::string nodes;
        for (data::NodeId node : unsupportedPeers)
            nodes += (nodes.empty() ? "" : ",") + std::to_string(node);
        throw data::PatternSeriesUnsupportedError(
            "peer node(s) " + nodes + " predate quorum-fenced pattern-series discovery (data-plane wire v" +
            std::to_string(data::kPatternSeriesMinVersion) + "); finish the rolling upgrade");
    }
    if (!unreachableLeaders.empty()) {
        for (data::NodeId dead : unreachableLeaders)
            co_await shards_.invoke_on_all([dead](ShardRaftPlane& plane) { (void)plane.wakeFollowersOf(dead); });
        throw data::RetryableWriteError("pattern delete could not read every VShard leader; retry after re-election");
    }
    if (!unassigned.empty())
        throw data::UnassignedVShardError("pattern delete found " + std::to_string(unassigned.size()) +
                                          " unassigned VShard(s)");
    if (leaderless > 0 || !outstanding.empty())
        throw data::RetryableWriteError("pattern delete could not establish a complete leader-fenced catalog view");
    if (dir_->epoch() != pinnedEpoch)
        throw data::RetryableWriteError("placement changed during pattern expansion; no delete was proposed");

    co_return std::vector<std::string>(matchedKeys.begin(), matchedKeys.end());
}

seastar::future<ClusterDataPlane::Status> ClusterDataPlane::status() const {
    Status st;
    if (!rt_)
        co_return st;
    st.self = rt_->selfId;
    st.peers = rt_->peerAddresses;
    st.unresolvedPeerCount = unresolvedPeers_.size();
    st.replicated = replicated_;
    st.replicationFactor = rf_;
    if (!replicated_ || !shardsStarted_)
        co_return st;
    std::vector<data::NodeId> peers;
    for (const auto& [id, addr] : rt_->peerAddresses)
        peers.push_back(id);
    const data::NodeId self = rt_->selfId;
    auto& shards = const_cast<seastar::sharded<ShardRaftPlane>&>(shards_);
    // Readiness requires the trigger on EVERY shard. Starting false and OR-ing
    // masked a dead snapshot loop on N-1 shards as long as one shard remained live.
    st.snapshotTriggerEnabled = true;
    for (unsigned sh = 0; sh < seastar::smp::count; ++sh) {
        auto c = co_await shards.invoke_on(sh, [self, peers](ShardRaftPlane& p) { return p.counts(self, peers); });
        st.vshardsHostedHere += c.hosted;
        st.vshardsLedHere += c.led;
        st.vshardsLeaderless += c.leaderless;
        for (const auto& [peer, n] : c.peerCaughtUp)
            st.peerCaughtUp[peer] += n;
        st.applyLagEntries += c.applyLagEntries;
        st.applyGroupsBehind += c.groupsBehind;
        st.applyFailures += c.applyFailures;
        st.tickErrors += c.tickErrors;
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
        st.snapshotProductionLimitPerShard = std::max(st.snapshotProductionLimitPerShard, sc.productionLimit);
        st.snapshotTriggerEnabled = st.snapshotTriggerEnabled && sc.triggerEnabled;
        auto jc = co_await shards.invoke_on(sh, [](ShardRaftPlane& p) { return p.journalCounts(); });
        st.journalFsyncs += jc.fsyncs;
        st.journalSyncRequests += jc.syncRequests;
        st.journalSegmentsDeleted += jc.segmentsDeleted;
        st.journalSegmentsPinnedLastPass += jc.segmentsPinnedLastPass;
        st.journalRecordsCopiedForward += jc.recordsCopiedForward;
        st.journalGcPasses += jc.gcPasses;
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

seastar::future<> ClusterDataPlane::deleteRangeFromShard(std::string seriesKey, uint64_t startTime, uint64_t endTime) {
    if (!replicated_ || !shardsStarted_)
        throw std::runtime_error("ClusterDataPlane::deleteRangeFromShard requires replicated mode after start()");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(seriesKey));
    const unsigned owner = shardOwningVShard(vshard, dir_.get());
    data::DeleteRangeKey del{std::move(seriesKey), startTime, endTime};
    return shards_.invoke_on(owner, [vshard, del = std::move(del)](ShardRaftPlane& plane) mutable {
        return plane.command(vshard, data::ReplicatedCommand{std::move(del)});
    });
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
