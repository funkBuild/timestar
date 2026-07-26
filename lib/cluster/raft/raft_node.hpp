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
    // It exists for InstallSnapshot, the one producer 5.4 did not cap: it carries an
    // entire VShard snapshot in one message. Over the bound, the transport refuses to send
    // it -- and `sendInstallSnapshot` used to advance `nextIndex_[peer]` BEFORE the send,
    // so a refusal became a hot loop: optimistic advance -> follower rejects the next
    // append -> rewind -> re-encode the whole snapshot -> refuse -> repeat, once per round
    // trip, with an error log each time. Checking here means the doomed message is never
    // built and `nextIndex_` is never moved on its behalf.
    size_t maxMessageBytes = 0;
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
    void tick();                     // one logical clock tick (driven by a seastar::timer)
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
    // Snapshots this node declined to send because they exceed opts_.maxMessageBytes.
    // Non-zero means a follower CANNOT be caught up by snapshot and needs chunked
    // InstallSnapshot (or a smaller snapshot) before it can rejoin.
    uint64_t undeliverableSnapshots() const { return undeliverableSnapshots_; }
    LogIndex commitIndex() const { return commitIndex_; }
    const RaftLog& log() const { return log_; }
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
    void becomeCandidate();
    void becomeLeader();
    void resetElectionTimer();
    void send(Message m);
    void bcastRequestVote(bool preVote);
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
    void maybeAdvanceCommitAsLeader();
    void confirmReads();  // move quorum-confirmed pending reads to confirmedReads_
    void handleInstallSnapshot(NodeId from, const InstallSnapshot& is);
    void handleInstallSnapshotReply(NodeId from, const InstallSnapshotReply& rr);

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
