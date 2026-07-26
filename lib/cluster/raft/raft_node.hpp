#pragma once

#include "raft_config.hpp"
#include "raft_log.hpp"
#include "raft_messages.hpp"
#include "raft_types.hpp"

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <vector>

namespace timestar::raft {

struct RaftOptions {
    // Election timeout is randomized per node in [min, max] ticks (§5.2) to break
    // split votes. Set min == max for deterministic tests.
    unsigned electionTimeoutMin = 10;
    unsigned electionTimeoutMax = 20;
    unsigned heartbeatTimeout = 1;  // leader heartbeat interval in ticks (replication brick)
    bool preVote = false;           // §PreVote (added in the pre-vote brick)
    bool checkQuorum = false;       // §CheckQuorum leader lease (added later)
    uint64_t rngSeed = 0;           // 0 => derive deterministically from node id

    // CATCH-UP CHUNKING (write-scaleout 5.4). A follower that has been down carries a
    // nextIndex far behind the leader's lastIndex, and `sendAppend` used to put the WHOLE
    // remaining tail -- `log_.entriesFrom(nextIndex)` -- into ONE AppendEntries. After a
    // large write campaign that is an arbitrarily large message, which is why the Raft
    // transport's inbound admission bound had to stay loose (1 GiB): the deliver verb is
    // `no_wait`, so an over-limit message is DROPPED WITH NO REPLY, and a follower whose
    // catch-up append is always too big retries the same oversized message forever and
    // never catches up -- silently.
    //
    // Capping it is safe without any protocol change: a follower acks the PREFIX it
    // received (`matchIndex`), and `handleAppendEntriesReply` immediately sends the next
    // chunk while `nextIndex <= lastIndex`, so catch-up is a pipeline of bounded messages
    // rather than one unbounded one. Leadership transfer still waits for
    // `matchIndex == lastIndex`, which the pipeline reaches in more round trips rather
    // than not at all.
    //
    // Both bounds apply; whichever binds first wins. At least one entry is ALWAYS sent,
    // so an entry larger than maxAppendBytes still replicates (it is the message-size
    // bound that then has to accommodate it, not this).
    size_t maxAppendEntries = 256;
    size_t maxAppendBytes = 1u << 20;  // 1 MiB of entry payload per AppendEntries

    // The largest PAYLOAD this group may put in a single message. 0 disables the check
    // (tests, and any driver with no such bound). It is NOT the transport's send bound:
    // that one is enforced on the encoded envelope, so a driver mirroring it must pass
    // `kMaxRaftPayloadBytes` (the send bound less the envelope's headroom) rather than
    // `kMaxRaftSendBytes`, or a snapshot in the band between them passes here and is
    // refused on the wire -- reinstating the very hot loop below.
    //
    // It existed for InstallSnapshot, the one producer 5.4 did not cap: it carried an
    // entire VShard snapshot in one message. Over the bound, the transport refuses to send
    // it -- and `sendInstallSnapshot` used to advance `nextIndex_[peer]` BEFORE the send,
    // so a refusal became a hot loop: optimistic advance -> follower rejects the next
    // append -> rewind -> re-encode the whole snapshot -> refuse -> repeat, once per round
    // trip, with an error log each time. Checking here means the doomed message is never
    // built and `nextIndex_` is never moved on its behalf.
    //
    // Since D-5 chunked the snapshot this is compared against the CHUNK, so it no longer
    // binds a snapshot at all in production -- it remains as the per-message belt (and as
    // the whole-payload check when chunking is disabled below).
    size_t maxMessageBytes = 0;

    // ---- chunked InstallSnapshot (debt D-5) ----
    //
    // The largest slice of a snapshot payload one InstallSnapshot may carry. 0 disables
    // chunking (one message per snapshot, the pre-D-5 behaviour), which is what the core
    // unit tests use: their payloads are bytes, so chunking would be untestably invisible
    // there, and the unchunked path must keep working for a peer that predates tag 9.
    size_t maxSnapshotChunkBytes = kMaxSnapshotChunkBytes;

    // The largest TOTAL snapshot payload this node will serve or stage. 0 == no bound
    // (tests). Over it, `sendInstallSnapshot` refuses WITHOUT advancing nextIndex_ and
    // counts `undeliverableSnapshots()` -- the F3a discipline, now keyed on memory rather
    // than on message size (see kMaxVShardSnapshotBytes).
    size_t maxSnapshotBytes = 0;

    // Ticks with NO reply from a peer mid-transfer before the in-flight chunk is RESENT.
    // The transport is fire-and-forget, so a dropped chunk is SILENT: this timer is the
    // only thing that notices. Two heartbeat intervals by default -- long enough that a
    // chunk in flight over a slow link is not resent under itself, short enough that a
    // transfer does not stall for an election timeout.
    unsigned snapshotChunkTimeout = 50;

    // Consecutive resends of the same chunk with NO progress before the transfer is
    // ABANDONED. Abandonment is safe by construction here: nothing about a chunked
    // transfer advances nextIndex_, so giving up leaves the peer exactly as far behind as
    // it truly is and the next heartbeat starts a fresh transfer.
    unsigned maxSnapshotResends = 6;
};

// One replica of one Raft group (one VShard). Deterministic and reactor-free:
// inputs are tick()/step()/propose(); outputs are drained via ready()/advance().
// The Seastar-RPC + timer + journal driver owns all I/O and MUST honour the
// Ready contract: persist hardState and entries (fsync) BEFORE sending messages,
// and apply `committed` only after it is durable.
class RaftNode {
public:
    // A confirmed ReadIndex: once applied() >= readIndex, the leader may serve the
    // read for `context` linearizably (the barrier the plan requires for leader
    // reads and for reconciling missed group-0 notifications).
    struct ReadState {
        uint64_t context = 0;
        LogIndex readIndex = kNoIndex;
    };

    struct Ready {
        std::optional<HardState> hardState;  // persist (fsync) if present, before messages
        std::optional<Snapshot> snapshot;    // a received snapshot to install (persist before messages)
        std::vector<LogEntry> entries;       // newly (re)written log entries to persist
        std::vector<LogEntry> committed;     // newly committed entries to apply, in order
        std::vector<Message> messages;       // send only after hardState+snapshot+entries are durable
        std::vector<ReadState> readStates;   // ReadIndex barriers confirmed this cycle
        bool empty() const {
            return !hardState && !snapshot && entries.empty() && committed.empty() && messages.empty() &&
                   readStates.empty();
        }
    };

    // `learners` are non-voting members: they receive replication (to catch up
    // before promotion) but never vote, never count toward quorum, and never
    // start an election.
    RaftNode(NodeId id, std::vector<NodeId> voters, RaftLog log, HardState hs, RaftOptions opts,
             std::vector<NodeId> learners = {});

    // Inputs.
    // Advance this group's logical clock by `passes` tick intervals (driven by a
    // seastar::timer). `passes` > 1 is the driver crediting intervals it SKIPPED while
    // hibernating this group, so every clock in here measures real time rather than
    // driver attention -- load-bearing for the CheckQuorum lease, see tick()'s body.
    void tick(unsigned passes = 1);
    void step(Message m);            // an incoming RPC for this group
    bool propose(std::string data);  // leader-only append; false if not leader (replication brick)
    void campaign();                 // start an election now (bootstrap / TimeoutNow)

    // Leader transfer (§3.10): hand leadership to `target` (a voter). The current
    // leader first ensures `target` is caught up, then sends it a TimeoutNow so it
    // elects immediately. No-op if we are not the leader or target is not a voter.
    void transferLeadership(NodeId target);

    // Request a linearizable ReadIndex barrier for `context` (leader only). Starts
    // a heartbeat confirmation round; the barrier surfaces in Ready.readStates
    // once a quorum has confirmed current-term leadership AFTER this request.
    // Returns false (no barrier) if not the leader -- the caller redirects.
    bool requestReadIndex(uint64_t context);

    // Compact this node's log up to `upto` (<= commitIndex) and record the
    // resulting snapshot so the leader can serve it to lagging followers. Called
    // by the driver after it has durably written the snapshot payload.
    void compact(LogIndex upto, std::string snapshotData);

    // Seed the SERVABLE snapshot from one recovered off this node's journal, at
    // construction time only (debt D-6).
    //
    // The constructor takes a RaftLog that may already be RESTORED to a snapshot boundary
    // (recoverRaftState does that), which is enough for this node's own log arithmetic --
    // but it leaves `snapshot_` empty, so a restarted node that later becomes LEADER has
    // nothing to serve a follower below its boundary: `sendAppend` finds no prev term,
    // hands off to `sendInstallSnapshot`, and that returns immediately ("nothing to
    // serve"). The follower could then never be caught up until this node happened to take
    // a fresh snapshot. Before D-6 wired the producer this was unreachable, because nothing
    // ever compacted; it is reachable now.
    //
    // Refuses (throws) a snapshot that does not sit exactly at the log's boundary -- that
    // pairing is the one thing making the seeded payload the right bytes for the index it
    // claims, and serving a mismatched one would install the wrong state on a follower.
    void seedRecoveredSnapshot(Snapshot snap);

    // Output draining (single-threaded: ready() -> persist+send+apply -> advance()).
    bool hasReady() const;
    Ready ready();
    void advance(const Ready& rd);

    // Observers.
    NodeId id() const { return id_; }
    Role role() const { return role_; }
    bool isLeader() const { return role_ == Role::Leader; }
    Term currentTerm() const { return currentTerm_; }
    NodeId leader() const { return leaderId_; }
    // A leadership handoff is in flight, which is the OTHER reason propose() refuses
    // (see propose()). A caller that gets a bare `false` needs this to tell "I am not the
    // leader" from "I am the leader and I am standing down" -- the two want opposite
    // retries (write-scaleout 5 review, F1).
    bool transferInFlight() const { return leadTransferee_ != kNoNode; }
    // Snapshots this node declined to send because they exceed opts_.maxSnapshotBytes (or,
    // with chunking disabled, opts_.maxMessageBytes). Non-zero means a follower CANNOT be
    // caught up by snapshot and needs a SMALLER snapshot before it can rejoin -- since
    // D-5 chunked the transfer, the remaining reason is memory, not message size.
    uint64_t undeliverableSnapshots() const { return undeliverableSnapshots_; }

    // ---- chunked-transfer observability (debt D-5) ----
    // These are the counters a gate asserts on to prove a catch-up really went through
    // the SNAPSHOT path rather than through ordinary appends. Without them "the follower
    // caught up" is indistinguishable between the two.
    //
    // Chunks this leader has put on the wire (a one-message snapshot counts as one).
    uint64_t snapshotChunksSent() const { return snapshotChunksSent_; }
    // Snapshots this node has INSTALLED as a follower (final chunk received and applied).
    uint64_t snapshotsInstalled() const { return snapshotsInstalled_; }
    // Transfers restarted because a chunk went unacked for opts_.snapshotChunkTimeout
    // ticks. Steady non-zero means chunks are being dropped (or the peer predates tag 9).
    uint64_t snapshotTransfersRestarted() const { return snapshotTransfersRestarted_; }
    // Transfers given up on after opts_.maxSnapshotResends stalls. The peer is left
    // exactly as far behind as it is; the next heartbeat starts over.
    uint64_t snapshotTransfersAbandoned() const { return snapshotTransfersAbandoned_; }
    // Received snapshots refused because their declared total exceeds what we will stage.
    uint64_t snapshotsRefusedTooLarge() const { return snapshotsRefusedTooLarge_; }
    // Is a chunked transfer to `peer` in flight? (Leader-only; test/inspection aid.)
    bool snapshotTransferInFlight(NodeId peer) const { return snapTransfers_.count(peer) != 0; }
    // Bytes of a partial snapshot currently staged as a follower (0 == none).
    uint64_t stagedSnapshotBytes() const { return snapStaging_ ? snapStaging_->data.size() : 0; }
    LogIndex commitIndex() const { return commitIndex_; }
    const RaftLog& log() const { return log_; }
    // The snapshot this node can currently SERVE to a lagging peer (index == kNoIndex when
    // there is none). The driver needs it after `compact()` in order to PERSIST it -- see
    // RaftGroup::compact for why an unpersisted snapshot makes compaction pointless.
    const Snapshot& servableSnapshot() const { return snapshot_; }
    unsigned electionTimeout() const { return electionTimeout_; }

    bool isVoter(NodeId n) const;
    bool isLearner(NodeId n) const;
    const Config& config() const { return config_; }

    // Highest log index this leader knows is replicated on `peer` (0 if unknown --
    // no entry acked yet, or `peer` is not a member). Meaningful only on the leader.
    // The move executor's catchUp() polls this to know when a freshly-added learner
    // has caught up enough to be promoted safely (integration plan M5).
    LogIndex matchIndexOf(NodeId peer) const {
        auto it = matchIndex_.find(peer);
        return it == matchIndex_.end() ? kNoIndex : it->second;
    }

    // Ticks since `peer` last REPLIED to us in this leadership term (any
    // AppendEntries or InstallSnapshot reply, success or reject -- a reply is a
    // liveness proof regardless of what it says). `kNeverAcked` if it has not
    // replied since we won the term, or if it is not a member. Meaningful only on
    // the leader; reset on every role change.
    //
    // Exists because matchIndex ALONE cannot tell a dead peer from a caught-up one
    // on a write-idle group: a peer that dies while caught up keeps matchIndex ==
    // lastIndex forever, since lastIndex stops moving too. The leadership balancer
    // needs a signal that decays on its own, and the heartbeat already provides one
    // -- a live peer answers `bcastAppend` every `heartbeatTimeout` ticks whether or
    // not there is anything to replicate. See ShardRaftPlane::rebalance.
    static constexpr uint64_t kNeverAcked = std::numeric_limits<uint64_t>::max();
    uint64_t ticksSinceAck(NodeId peer) const {
        auto it = lastAckTick_.find(peer);
        return it == lastAckTick_.end() ? kNeverAcked : tick_ - it->second;
    }

    // The configured leader heartbeat interval, in ticks. Exposed so a caller can
    // scale a staleness bound to the group's real cadence rather than hard-coding a
    // tick count that silently changes meaning when the tick period does.
    unsigned heartbeatTimeout() const { return opts_.heartbeatTimeout; }

    // Propose a §6 membership change to the given target voters/learners. The
    // leader enters a joint configuration (Cold,new) immediately, and once that
    // commits it auto-appends the final Cnew. false if not the leader or a change
    // is already in flight.
    bool proposeConfChange(std::vector<NodeId> voters, std::vector<NodeId> learners);

private:
    void sendTimeoutNow(NodeId target);
    // Majority helpers over the (possibly joint) configuration.
    bool majorityOf(const std::vector<NodeId>& set, const std::set<NodeId>& acked) const;
    LogIndex majorityMatchIndex(const std::vector<NodeId>& set) const;
    // Config derivation: the active config is the latest ConfigChange in the log,
    // above the snapshot-boundary base config.
    void recomputeConfigFromLog();
    bool maybeAppendLeaveJoint();  // leader: after Cold,new commits, append Cnew (true if appended)

    void becomeFollower(Term term, NodeId leader);
    void becomePreCandidate();
    // `transfer` marks this campaign as started by a TimeoutNow from the leader we
    // follow, which flags the vote requests so other voters' CheckQuorum lease stands
    // aside (ADR 0005). It is a PARAMETER and not a member on purpose: it must describe
    // THIS campaign only, and a member would leak the flag into the next self-timeout
    // campaign of a node that once received a TimeoutNow.
    void becomeCandidate(bool transfer = false);
    void becomeLeader();
    void resetElectionTimer();
    void send(Message m);
    void bcastRequestVote(bool preVote, bool transfer = false);
    void checkQuorumOrStepDown();

    void handleRequestVote(NodeId from, const RequestVote& rv);
    void handleRequestVoteReply(NodeId from, const RequestVoteReply& rr);
    void handleAppendEntries(NodeId from, const AppendEntries& ae);
    void handleAppendEntriesReply(NodeId from, const AppendEntriesReply& rr);
    void advanceCommitAsFollower(LogIndex leaderCommit, LogIndex lastNewIndex);
    LogIndex firstIndexOfTerm(Term t, LogIndex at) const;
    LogIndex lastIndexOfTerm(Term t) const;

    // Leader replication.
    void bcastAppend();
    void sendAppend(NodeId peer);
    void sendInstallSnapshot(NodeId peer);
    // Put the chunk at `offset` (up to `chunkLen` bytes of the current snapshot) on the
    // wire to `peer` and record it as in flight. Never touches nextIndex_.
    void sendSnapshotChunk(NodeId peer, uint64_t offset, size_t chunkLen);
    // Resend / abandon transfers whose in-flight chunk has gone unacked (leader, per tick).
    void sweepStalledSnapshotTransfers(unsigned passes);
    void maybeAdvanceCommitAsLeader();
    void confirmReads();  // move quorum-confirmed pending reads to confirmedReads_
    void handleInstallSnapshot(NodeId from, const InstallSnapshot& is);
    void handleInstallSnapshotReply(NodeId from, const InstallSnapshotReply& rr);
    // Adopt a fully-received snapshot (the final chunk's payload) and fill in `reply`.
    void installReceivedSnapshot(Snapshot full, InstallSnapshotReply& reply);

    // Election tally over the (possibly joint) configuration: won needs a
    // majority grant in every voting set; lost means no set can still reach one.
    bool electionWon() const;
    bool electionLost() const;

    NodeId id_;
    Config config_;                          // active membership (latest ConfigChange in log)
    Config baseConfig_;                      // membership at the snapshot boundary (config floor)
    LogIndex latestConfigIndex_ = kNoIndex;  // index of the highest ConfigChange in the log
    RaftOptions opts_;

    Term currentTerm_ = kNoTerm;
    NodeId votedFor_ = kNoNode;
    Role role_ = Role::Follower;
    NodeId leaderId_ = kNoNode;

    RaftLog log_;
    LogIndex commitIndex_ = kNoIndex;
    LogIndex lastApplied_ = kNoIndex;

    unsigned electionElapsed_ = 0;
    unsigned electionTimeout_ = 0;   // current randomized target
    unsigned heartbeatElapsed_ = 0;  // leader heartbeat clock
    std::map<NodeId, bool> votes_;   // this election's replies (self included)
    std::mt19937_64 rng_;

    // Leader-only replication progress, one per voter (self included).
    std::map<NodeId, LogIndex> nextIndex_;   // next index to send to the peer
    std::map<NodeId, LogIndex> matchIndex_;  // highest index known replicated on the peer
    // CheckQuorum: voters we have heard from since the last quorum check.
    std::set<NodeId> recentActive_;
    // Monotonic tick counter, and the tick at which each peer last replied to us as
    // leader. O(1) per tick (one increment) rather than a per-peer sweep -- at 4096
    // groups per shard ticking every 20 ms, a per-peer bump per tick is not free.
    // See ticksSinceAck().
    uint64_t tick_ = 0;
    std::map<NodeId, uint64_t> lastAckTick_;
    NodeId leadTransferee_ = kNoNode;  // in-flight leader-transfer target (0 = none)
    // Ticks since the in-flight transfer began. A transfer to a target that never
    // finishes catching up must be ABANDONED (§3.10): while `leadTransferee_` is set the
    // leader refuses every proposal, so a transfer to a DEAD peer wedges the group's
    // writes permanently. See tick().
    unsigned transferElapsed_ = 0;
    uint64_t undeliverableSnapshots_ = 0;  // see undeliverableSnapshots()

    // ---- chunked InstallSnapshot, leader side (debt D-5) ----
    //
    // One record per peer currently being caught up by snapshot. Its EXISTENCE is the flow
    // control: `sendInstallSnapshot` is a no-op while a transfer for the current snapshot
    // is in flight, so the heartbeat cannot re-blast chunk 0 every round and at most ONE
    // unacked chunk per peer is ever on the wire. The pipeline is driven by replies
    // (handleInstallSnapshotReply) exactly as the bounded AppendEntries catch-up is, with
    // the tick sweep as the only recovery from a dropped chunk.
    //
    // Cleared on every role change, like matchIndex_/lastAckTick_: progress is per-term.
    struct SnapshotTransfer {
        LogIndex index = kNoIndex;  // the snapshot boundary being shipped
        Term term = kNoTerm;        // ... and its term; the pair keys the peer's staging
        uint64_t offset = 0;        // offset of the chunk currently in flight
        uint64_t acked = 0;         // bytes the peer has confirmed staged
        unsigned idleTicks = 0;     // tick passes since the in-flight chunk was sent
        unsigned resends = 0;       // consecutive resends with no acked progress
    };
    std::map<NodeId, SnapshotTransfer> snapTransfers_;
    uint64_t snapshotChunksSent_ = 0;
    uint64_t snapshotTransfersRestarted_ = 0;
    uint64_t snapshotTransfersAbandoned_ = 0;

    // ---- chunked InstallSnapshot, follower side (debt D-5) ----
    //
    // The staging area. A partial snapshot lives HERE and nowhere else until its final
    // chunk arrives, which is what makes a half-installed snapshot unreachable: the
    // install is one atomic step at the end, and this buffer is DELIBERATELY VOLATILE --
    // it is never persisted, so a follower that dies mid-transfer comes back with no
    // partial at all and the leader simply starts over. (Persisting it would buy a resume
    // across restarts and cost the invariant: a durable partial is a thing a buggy
    // recovery path could install.)
    //
    // Keyed by (index, term): a chunk for any other snapshot boundary DISCARDS what is
    // staged rather than being spliced onto it. `data.size()` is the resume point, so it
    // is also the only thing the leader is told.
    struct SnapshotStaging {
        LogIndex index = kNoIndex;
        Term term = kNoTerm;
        Config config;  // the boundary config, refreshed by every chunk
        uint64_t totalBytes = 0;
        std::string data;  // the contiguous prefix received so far
    };
    std::optional<SnapshotStaging> snapStaging_;
    uint64_t snapshotsInstalled_ = 0;
    uint64_t snapshotsRefusedTooLarge_ = 0;

    // ReadIndex tracking (leader).
    uint64_t readSeq_ = 0;  // monotonic heartbeat sequence for read confirmation
    struct PendingRead {
        uint64_t context;
        uint64_t atSeq;  // readSeq at request; confirmed once a quorum echoes >= this
    };
    std::vector<PendingRead> pendingReads_;
    std::map<NodeId, uint64_t> ackedReadSeq_;  // highest readSeq each voter has echoed
    std::vector<ReadState> confirmedReads_;    // drained via Ready.readStates

    // Output accumulation.
    bool hsDirty_ = false;
    LogIndex unstableStart_ = kNoIndex;  // first log index not yet reported for persistence
    std::vector<Message> pendingMessages_;
    Snapshot snapshot_;                             // the current snapshot we can serve (index 0 = none)
    std::optional<Snapshot> pendingSnapshotApply_;  // a received snapshot to surface to the driver
};

}  // namespace timestar::raft
