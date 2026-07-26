#include "raft_node.hpp"

#include "raft_config.hpp"

#include <algorithm>
#include <functional>

namespace timestar::raft {

namespace {

// The term carried by a message, for the common term-comparison rules.
Term messageTerm(const Message& m) {
    return std::visit([](const auto& p) { return p.term; }, m.payload);
}

bool isVoteRequest(const MessagePayload& p) {
    return std::holds_alternative<RequestVote>(p);
}

// A message whose sender is (or claims to be) a leader at its term.
bool isLeaderMessage(const MessagePayload& p) {
    return std::holds_alternative<AppendEntries>(p) || std::holds_alternative<InstallSnapshot>(p) ||
           std::holds_alternative<TimeoutNow>(p);
}

}  // namespace

RaftNode::RaftNode(NodeId id, std::vector<NodeId> voters, RaftLog log, HardState hs, RaftOptions opts,
                   std::vector<NodeId> learners)
    : id_(id),
      opts_(opts),
      currentTerm_(hs.currentTerm),
      votedFor_(hs.votedFor),
      log_(std::move(log)),
      rng_(opts.rngSeed != 0 ? opts.rngSeed : (id * 0x9E3779B97F4A7C15ull) + 1) {
    // The passed voters/learners are the config at the snapshot boundary; any
    // ConfigChange entries already in the log are folded on top.
    baseConfig_ = Config{std::move(voters), {}, std::move(learners)};
    config_ = baseConfig_;
    recomputeConfigFromLog();
    // Committed entries at or below the snapshot boundary are already applied
    // (they live in the snapshot); start applying after it.
    commitIndex_ = log_.snapshotIndex();
    lastApplied_ = log_.snapshotIndex();
    unstableStart_ = log_.lastIndex() + 1;  // the on-disk log is already durable
    resetElectionTimer();
}

bool RaftNode::isVoter(NodeId n) const {
    return config_.isVoter(n);
}

bool RaftNode::isLearner(NodeId n) const {
    return config_.isLearner(n);
}

namespace {

size_t majSize(const std::vector<NodeId>& set) {
    return set.size() / 2 + 1;
}

// The set of peers to replicate to / init progress for: both voting sets plus
// learners, deduplicated, excluding self.
std::vector<NodeId> replicationPeers(const Config& c, NodeId self) {
    std::set<NodeId> s(c.voters.begin(), c.voters.end());
    s.insert(c.votersOutgoing.begin(), c.votersOutgoing.end());
    s.insert(c.learners.begin(), c.learners.end());
    s.erase(self);
    return {s.begin(), s.end()};
}

}  // namespace

bool RaftNode::majorityOf(const std::vector<NodeId>& set, const std::set<NodeId>& acked) const {
    if (set.empty())
        return true;  // an empty (absent) config half imposes no constraint
    size_t n = 0;
    for (NodeId v : set)
        if (acked.count(v))
            ++n;
    return n >= majSize(set);
}

LogIndex RaftNode::majorityMatchIndex(const std::vector<NodeId>& set) const {
    if (set.empty())
        return UINT64_MAX;  // absent half does not constrain the min() during joint
    std::vector<LogIndex> m;
    m.reserve(set.size());
    for (NodeId v : set)
        m.push_back(matchIndex_.count(v) ? matchIndex_.at(v) : kNoIndex);
    std::sort(m.begin(), m.end(), std::greater<>());
    return m[majSize(set) - 1];  // highest index replicated on a majority of this set
}

void RaftNode::recomputeConfigFromLog() {
    // The active config is the latest ConfigChange entry in the materialized log,
    // or the snapshot-boundary base config if there is none.
    for (LogIndex i = log_.lastIndex(); i >= log_.firstIndex(); --i) {
        const LogEntry* e = log_.entryAt(i);
        if (e && e->type == EntryType::ConfigChange) {
            config_ = decodeConfig(e->data);
            latestConfigIndex_ = i;
            return;
        }
        if (i == log_.firstIndex())
            break;  // guard unsigned underflow
    }
    config_ = baseConfig_;
    latestConfigIndex_ = kNoIndex;
}

void RaftNode::resetElectionTimer() {
    electionElapsed_ = 0;
    const unsigned lo = opts_.electionTimeoutMin;
    const unsigned hi = std::max(opts_.electionTimeoutMax, lo);
    electionTimeout_ = lo + (hi > lo ? static_cast<unsigned>(rng_() % (hi - lo + 1)) : 0);
}

void RaftNode::send(Message m) {
    m.from = id_;
    pendingMessages_.push_back(std::move(m));
}

bool RaftNode::electionWon() const {
    std::set<NodeId> granted;
    for (const auto& [node, g] : votes_)
        if (g)
            granted.insert(node);
    return majorityOf(config_.voters, granted) && (!config_.joint() || majorityOf(config_.votersOutgoing, granted));
}

bool RaftNode::electionLost() const {
    std::set<NodeId> rejected;
    for (const auto& [node, g] : votes_)
        if (!g)
            rejected.insert(node);
    // A set is unwinnable once the number that COULD still grant (its size minus
    // its known rejecters) drops below its majority threshold.
    auto unwinnable = [&](const std::vector<NodeId>& set) {
        if (set.empty())
            return false;
        size_t rej = 0;
        for (NodeId v : set)
            if (rejected.count(v))
                ++rej;
        return (set.size() - rej) < majSize(set);
    };
    return unwinnable(config_.voters) || (config_.joint() && unwinnable(config_.votersOutgoing));
}

void RaftNode::becomeFollower(Term term, NodeId leader) {
    if (term != currentTerm_) {
        currentTerm_ = term;
        votedFor_ = kNoNode;
        hsDirty_ = true;
    }
    role_ = Role::Follower;
    leaderId_ = leader;
    votes_.clear();
    nextIndex_.clear();
    matchIndex_.clear();
    recentActive_.clear();
    lastAckTick_.clear();  // liveness is per-term; see ticksSinceAck()
    leadTransferee_ = kNoNode;
    pendingReads_.clear();  // drop unconfirmed reads; the caller retries at the new leader
    ackedReadSeq_.clear();
    resetElectionTimer();
}

void RaftNode::becomePreCandidate() {
    // A straw poll for term currentTerm_+1 that changes NO durable state (no term
    // bump, no vote record) -- so losing it, or an isolated node running it
    // repeatedly, never disturbs the cluster.
    role_ = Role::PreCandidate;
    leaderId_ = kNoNode;
    votes_.clear();
    votes_[id_] = true;  // we would vote for ourselves
    resetElectionTimer();
    if (electionWon()) {
        becomeCandidate();  // trivial majority (single voter): go straight to real
        return;
    }
    bcastRequestVote(/*preVote=*/true);
}

void RaftNode::becomeCandidate(bool transfer) {
    role_ = Role::Candidate;
    ++currentTerm_;
    votedFor_ = id_;  // vote for self
    leaderId_ = kNoNode;
    hsDirty_ = true;
    votes_.clear();
    votes_[id_] = true;
    resetElectionTimer();
    // A single-voter group elects itself immediately.
    if (electionWon()) {
        becomeLeader();
        return;
    }
    bcastRequestVote(/*preVote=*/false, transfer);
}

void RaftNode::becomeLeader() {
    role_ = Role::Leader;
    leaderId_ = id_;
    electionElapsed_ = 0;
    heartbeatElapsed_ = 0;
    votes_.clear();
    recentActive_.clear();  // fresh CheckQuorum window

    leadTransferee_ = kNoNode;
    pendingReads_.clear();  // fresh ReadIndex tracking under this term
    ackedReadSeq_.clear();
    ackedReadSeq_[id_] = readSeq_;

    // Initialize replication progress for every peer (both voting sets AND
    // learners): optimistically probe from our log end.
    nextIndex_.clear();
    matchIndex_.clear();
    // Deliberately NOT seeded: a brand-new leader has heard from nobody, so every
    // peer reads as kNeverAcked until it answers our first heartbeat (one
    // heartbeatTimeout away). That errs toward "not live", which is the safe
    // direction for every consumer -- the balancer simply skips a pass.
    lastAckTick_.clear();
    for (NodeId peer : replicationPeers(config_, id_)) {
        nextIndex_[peer] = log_.lastIndex() + 1;
        matchIndex_[peer] = kNoIndex;
    }

    // Append a no-op entry in the new term (§5.4.2): a leader may only advance
    // commit once it has replicated an entry from ITS OWN term, so prior-term
    // entries are never committed by vote count alone. Empty data == a marker the
    // application ignores.
    LogEntry noop;
    noop.term = currentTerm_;
    log_.append({noop});

    matchIndex_[id_] = log_.lastIndex();  // the leader trivially matches its own log
    nextIndex_[id_] = log_.lastIndex() + 1;

    // A single-voter group has no followers to ack, so commit the no-op now;
    // otherwise a write-idle RF=1 leader would never satisfy a "my current-term
    // entry is committed" read-readiness gate. Multi-voter groups see no advance
    // here (only self matches) and commit once followers ack.
    maybeAdvanceCommitAsLeader();
    bcastAppend();  // replicate the no-op + serve as the first heartbeat
}

void RaftNode::sendAppend(NodeId peer) {
    const LogIndex ni = nextIndex_[peer];
    const LogIndex prevIndex = ni - 1;
    const auto prevTerm = log_.term(prevIndex);
    if (!prevTerm) {
        // prevIndex is below our compacted boundary: the follower is behind our
        // snapshot and must be caught up with the snapshot itself.
        sendInstallSnapshot(peer);
        return;
    }
    AppendEntries ae;
    ae.term = currentTerm_;
    ae.leaderId = id_;
    ae.prevLogIndex = prevIndex;
    ae.prevLogTerm = *prevTerm;
    // Chunked catch-up (write-scaleout 5.4): send a BOUNDED prefix of the tail. The
    // follower acks what it got and handleAppendEntriesReply immediately sends the next
    // chunk, so a far-behind follower is caught up by a pipeline of bounded messages
    // instead of one message whose size is the size of the backlog. See RaftOptions.
    ae.entries = log_.entriesFrom(ni, opts_.maxAppendEntries, opts_.maxAppendBytes);
    ae.leaderCommit = commitIndex_;
    ae.readSeq = readSeq_;  // followers echo this for ReadIndex confirmation
    send(Message{.to = peer, .from = id_, .payload = std::move(ae)});
}

void RaftNode::bcastAppend() {
    for (NodeId peer : replicationPeers(config_, id_))
        sendAppend(peer);
}

void RaftNode::sendInstallSnapshot(NodeId peer) {
    if (snapshot_.index == kNoIndex)
        return;  // nothing to serve (should not happen once compaction ran)
    // REFUSE, WITHOUT ADVANCING nextIndex_, a snapshot the transport cannot carry
    // (write-scaleout 5 review, F3a). The optimistic advance below is what turns a
    // refused send into a hot loop -- advance, follower rejects the next append, rewind,
    // re-encode the whole snapshot, refuse again -- so the check has to come BEFORE it.
    // The peer stays uncaught-up, which is the truth, and the counter says why.
    if (opts_.maxMessageBytes != 0 && snapshot_.data.size() > opts_.maxMessageBytes) {
        ++undeliverableSnapshots_;
        return;
    }
    InstallSnapshot is;
    is.term = currentTerm_;
    is.leaderId = id_;
    is.lastIncludedIndex = snapshot_.index;
    is.lastIncludedTerm = snapshot_.term;
    is.config = snapshot_.config;
    is.data = snapshot_.data;
    // Optimistically assume the follower will accept through the snapshot end.
    nextIndex_[peer] = snapshot_.index + 1;
    send(Message{.to = peer, .from = id_, .payload = std::move(is)});
}

void RaftNode::compact(LogIndex upto, std::string snapshotData) {
    // Never compact past applied state: the snapshot payload must reflect what the
    // state machine has actually applied, and lastApplied_ <= commitIndex_ always,
    // so this is the safe (and more defensive) bound.
    if (upto > lastApplied_)
        upto = lastApplied_;
    // The boundary config folds in every ConfigChange up to `upto`.
    Config base = baseConfig_;
    for (LogIndex i = log_.firstIndex(); i <= upto && i <= log_.lastIndex(); ++i) {
        const LogEntry* e = log_.entryAt(i);
        if (e && e->type == EntryType::ConfigChange)
            base = decodeConfig(e->data);
    }
    log_.compactTo(upto);
    baseConfig_ = base;
    recomputeConfigFromLog();  // fix latestConfigIndex_/config_ if the latest was folded
    snapshot_.index = log_.snapshotIndex();
    snapshot_.term = log_.snapshotTerm();
    snapshot_.config = baseConfig_;
    snapshot_.data = std::move(snapshotData);
}

void RaftNode::handleInstallSnapshot(NodeId from, const InstallSnapshot& is) {
    // Recognize the leader and reset the election clock (same as an AppendEntries).
    if (role_ != Role::Follower)
        becomeFollower(currentTerm_, is.leaderId);
    else {
        leaderId_ = is.leaderId;
        electionElapsed_ = 0;
    }

    InstallSnapshotReply reply;
    reply.term = currentTerm_;

    if (is.lastIncludedIndex <= commitIndex_) {
        // Stale: we already have everything the snapshot covers.
        reply.matchIndex = commitIndex_;
        send(Message{.to = from, .from = id_, .payload = reply});
        return;
    }

    // Adopt the snapshot. If our log already holds a matching entry at the
    // snapshot boundary, keep the consistent suffix; otherwise discard the log.
    if (log_.matchTerm(is.lastIncludedIndex, is.lastIncludedTerm))
        log_.compactTo(is.lastIncludedIndex);
    else
        log_.restoreFromSnapshot(is.lastIncludedIndex, is.lastIncludedTerm);

    // The snapshot carries the boundary config; re-derive the active config from
    // the new base plus any retained (keep-suffix) config entries. Defensive: a
    // config with no voters (only possible from corruption) would brick the
    // group, so ignore it and keep our current base.
    if (!is.config.voters.empty())
        baseConfig_ = is.config;
    recomputeConfigFromLog();

    snapshot_.index = is.lastIncludedIndex;
    snapshot_.term = is.lastIncludedTerm;
    snapshot_.config = is.config;
    snapshot_.data = is.data;
    commitIndex_ = std::max(commitIndex_, is.lastIncludedIndex);
    lastApplied_ = is.lastIncludedIndex;  // the snapshot IS the applied state
    // Only the snapshot boundary is now durable via the payload. Any retained
    // tail ABOVE it (the keep-suffix path) is still unpersisted and MUST remain
    // reportable -- advancing to lastIndex()+1 here would silently un-persist it
    // (it is not in the snapshot payload), losing committed entries on restart.
    unstableStart_ = std::max(unstableStart_, is.lastIncludedIndex + 1);
    pendingSnapshotApply_ = snapshot_;  // surface to the driver to install

    reply.matchIndex = is.lastIncludedIndex;
    send(Message{.to = from, .from = id_, .payload = reply});
}

void RaftNode::handleInstallSnapshotReply(NodeId from, const InstallSnapshotReply& rr) {
    if (role_ != Role::Leader || !(isVoter(from) || isLearner(from)))
        return;
    lastAckTick_[from] = tick_;  // any reply is a liveness proof; see ticksSinceAck()
    if (rr.matchIndex > matchIndex_[from])
        matchIndex_[from] = rr.matchIndex;
    nextIndex_[from] = std::max(nextIndex_[from], matchIndex_[from] + 1);
    maybeAdvanceCommitAsLeader();
    if (nextIndex_[from] <= log_.lastIndex())
        sendAppend(from);
}

LogIndex RaftNode::lastIndexOfTerm(Term t) const {
    for (LogIndex i = log_.lastIndex(); i >= log_.firstIndex(); --i) {
        const auto term = log_.term(i);
        if (term && *term == t)
            return i;
        if (i == log_.firstIndex())
            break;  // guard unsigned underflow
    }
    return kNoIndex;
}

void RaftNode::handleAppendEntriesReply(NodeId from, const AppendEntriesReply& rr) {
    // Accept acks from voters AND learners (learners replicate); only voters
    // count toward commit, which maybeAdvanceCommitAsLeader enforces separately.
    if (role_ != Role::Leader || !(isVoter(from) || isLearner(from)))
        return;

    // Liveness, independent of what the reply SAYS: a rejected append still proves
    // the peer is up and reachable. See ticksSinceAck().
    lastAckTick_[from] = tick_;

    // ReadIndex: record the highest readSeq this voter has echoed, then re-check
    // whether any pending read is now quorum-confirmed.
    if (rr.readSeq > ackedReadSeq_[from])
        ackedReadSeq_[from] = rr.readSeq;
    confirmReads();

    if (rr.success) {
        if (rr.matchIndex > matchIndex_[from])
            matchIndex_[from] = rr.matchIndex;
        // Keep any optimistically-streamed higher nextIndex; never rewind on a
        // reordered success (etcd semantics).
        nextIndex_[from] = std::max(nextIndex_[from], matchIndex_[from] + 1);
        maybeAdvanceCommitAsLeader();
        // Keep streaming if the follower is still behind our log end.
        if (nextIndex_[from] <= log_.lastIndex())
            sendAppend(from);
        // Leader transfer: once the transferee has fully caught up, tell it to
        // elect immediately.
        if (leadTransferee_ == from && matchIndex_[from] == log_.lastIndex()) {
            sendTimeoutNow(from);
            leadTransferee_ = kNoNode;
        }
        return;
    }

    // Rejected: back nextIndex up using the conflict hint (one term per round trip).
    LogIndex ni;
    if (rr.conflictTerm == kNoTerm) {
        ni = rr.conflictIndex;  // follower's log is shorter than prevLogIndex
    } else {
        const LogIndex ours = lastIndexOfTerm(rr.conflictTerm);
        ni = (ours != kNoIndex) ? ours + 1 : rr.conflictIndex;
    }
    if (ni < 1)
        ni = 1;
    // Never rewind past what we already know is replicated, nor overshoot the log.
    ni = std::max(ni, matchIndex_[from] + 1);
    ni = std::min(ni, log_.lastIndex() + 1);
    nextIndex_[from] = ni;
    sendAppend(from);  // retry immediately from the backed-up point
}

void RaftNode::maybeAdvanceCommitAsLeader() {
    // Advance commit as far as majorities allow. This loops because appending the
    // Cnew leave-joint entry can make a further index committable in the same
    // pass (notably a single-voter final config, which has no follower to ack).
    bool committedSomething = false;
    for (;;) {
        // The highest index replicated on a majority. During a joint config it
        // must be a majority in BOTH voting sets, so take the min of the two.
        LogIndex majorityMatch = majorityMatchIndex(config_.voters);
        if (config_.joint())
            majorityMatch = std::min(majorityMatch, majorityMatchIndex(config_.votersOutgoing));

        // §5.4.2: only commit it if it is from the CURRENT term. Earlier-term
        // entries ride along once a current-term entry above them commits.
        if (majorityMatch <= commitIndex_ || majorityMatch > log_.lastIndex() ||
            log_.term(majorityMatch) != std::optional<Term>(currentTerm_))
            break;

        commitIndex_ = majorityMatch;
        committedSomething = true;

        if (maybeAppendLeaveJoint())
            continue;  // the fresh Cnew entry may itself be committable now

        // If a committed membership change removed us from the (final) voter set,
        // step down -- we can no longer legitimately lead.
        if (!config_.joint() && !config_.isVoter(id_) && latestConfigIndex_ != kNoIndex &&
            commitIndex_ >= latestConfigIndex_) {
            becomeFollower(currentTerm_, kNoNode);
            return;
        }
    }
    if (committedSomething) {
        confirmReads();  // a current-term commit may release reads held for the no-op
        bcastAppend();   // let followers learn the new commit promptly
    }
}

bool RaftNode::maybeAppendLeaveJoint() {
    // Once the joint config entry (Cold,new) is committed, transition to the final
    // Cnew by appending a config entry that drops the outgoing voters. Only the
    // leader does this, and only once (config_ stops being joint afterward).
    if (role_ != Role::Leader || !config_.joint())
        return false;
    if (latestConfigIndex_ == kNoIndex || commitIndex_ < latestConfigIndex_)
        return false;  // the joint config is not committed yet
    Config finalCfg;
    finalCfg.voters = config_.voters;
    finalCfg.learners = config_.learners;  // votersOutgoing dropped
    LogEntry e;
    e.term = currentTerm_;
    e.type = EntryType::ConfigChange;
    e.data = encodeConfig(finalCfg);
    log_.append({e});
    config_ = finalCfg;
    latestConfigIndex_ = log_.lastIndex();
    matchIndex_[id_] = log_.lastIndex();
    nextIndex_[id_] = log_.lastIndex() + 1;
    return true;
}

void RaftNode::bcastRequestVote(bool preVote, bool transfer) {
    std::set<NodeId> asked(config_.voters.begin(), config_.voters.end());
    asked.insert(config_.votersOutgoing.begin(), config_.votersOutgoing.end());
    for (NodeId peer : asked) {
        if (peer == id_)
            continue;
        RequestVote rv;
        rv.preVote = preVote;
        // A transfer campaign never pre-votes (it enters becomeCandidate directly), so
        // these two are mutually exclusive by construction; the decoder enforces the same
        // thing on the wire.
        rv.campaignTransfer = transfer && !preVote;
        // A PreVote probes the NEXT term without adopting it; a real vote uses ours.
        rv.term = preVote ? currentTerm_ + 1 : currentTerm_;
        rv.candidateId = id_;
        rv.lastLogIndex = log_.lastIndex();
        rv.lastLogTerm = log_.lastTerm();
        send(Message{.to = peer, .from = id_, .payload = rv});
    }
}

void RaftNode::campaign() {
    if (opts_.preVote)
        becomePreCandidate();
    else
        becomeCandidate();
}

void RaftNode::sendTimeoutNow(NodeId target) {
    TimeoutNow tn;
    tn.term = currentTerm_;
    tn.leaderId = id_;
    send(Message{.to = target, .from = id_, .payload = tn});
}

void RaftNode::transferLeadership(NodeId target) {
    if (role_ != Role::Leader || target == id_ || !isVoter(target))
        return;
    // A REPEAT REQUEST FOR THE TRANSFER ALREADY IN FLIGHT IS IGNORED (etcd's behaviour;
    // write-scaleout 5 review, F2). Without this, resetting the window unconditionally
    // DEFEATS the abandon-after-one-election-timeout bound: any caller re-requesting the
    // same target faster than an election timeout -- a balancer pass every 5 s against a
    // 2.5-5 s timeout is exactly that shape -- re-arms the clock forever, and the group
    // refuses every proposal for as long as the caller keeps asking. That is the same
    // permanent-refusal failure tick() was changed to bound, reachable through a
    // different door.
    //
    // Only a change of TARGET restarts the window: that is a genuinely new transfer, and
    // it deserves its own full window.
    if (leadTransferee_ == target)
        return;
    leadTransferee_ = target;
    transferElapsed_ = 0;
    if (matchIndex_[target] == log_.lastIndex())
        sendTimeoutNow(target);  // already caught up: elect now
    else
        sendAppend(target);  // catch it up first; TimeoutNow fires on the ack
}

void RaftNode::checkQuorumOrStepDown() {
    // §CheckQuorum: a leader that has not heard from a majority within an election
    // timeout has likely been partitioned away; it steps down so it stops serving
    // stale leader reads and lets the majority side elect freely.
    recentActive_.insert(id_);  // we are trivially in contact with ourselves
    const bool haveQuorum = majorityOf(config_.voters, recentActive_) &&
                            (!config_.joint() || majorityOf(config_.votersOutgoing, recentActive_));
    recentActive_.clear();
    if (!haveQuorum)
        becomeFollower(currentTerm_, kNoNode);
}

void RaftNode::tick() {
    ++tick_;  // monotonic clock behind ticksSinceAck(); see raft_node.hpp
    if (role_ == Role::Leader) {
        // ABANDON A STUCK LEADER TRANSFER (§3.10). While `leadTransferee_` is set the
        // leader refuses EVERY proposal (see propose()), which is correct for a handoff
        // that is about to complete and catastrophic for one that never will: a transfer
        // to a peer that is down leaves matchIndex frozen below lastIndex, so the
        // TimeoutNow in handleAppendEntriesReply never fires, `leadTransferee_` is never
        // cleared, and the group refuses writes FOREVER while remaining leader (so no
        // election ever rescues it either).
        //
        // That is reachable in production without any operator action: the leadership
        // balancer picks the peer with the largest deficit, and a DEAD peer leads
        // nothing, so it is the most attractive target on every pass. Observed on the
        // restart-catch-up gate as a sustained ~26% of writes failing with
        // "1 VShard slice(s) uncommitted after 6 attempt(s) (last: not-leader)" for as
        // long as one of three nodes was down -- with a healthy 2-of-3 quorum available.
        //
        // etcd bounds it the same way: one election timeout, then give up and resume
        // accepting writes. The transfer is merely not completed; nothing is unsafe.
        if (leadTransferee_ != kNoNode && ++transferElapsed_ >= electionTimeout_) {
            leadTransferee_ = kNoNode;
            transferElapsed_ = 0;
        }
        if (++heartbeatElapsed_ >= opts_.heartbeatTimeout) {
            heartbeatElapsed_ = 0;
            bcastAppend();  // heartbeat (an AppendEntries, possibly carrying entries)
        }
        if (opts_.checkQuorum) {
            if (++electionElapsed_ >= electionTimeout_) {
                electionElapsed_ = 0;
                checkQuorumOrStepDown();
            }
        }
        return;
    }
    // Follower / (pre)candidate: advance the election clock. Learners never
    // campaign -- they are non-voting and can never become leader.
    if (!isVoter(id_))
        return;
    ++electionElapsed_;
    if (electionElapsed_ >= electionTimeout_)
        campaign();  // PreVote-aware: pre-election first when enabled
}

void RaftNode::step(Message m) {
    const Term mTerm = messageTerm(m);
    // Messages whose higher term must NOT bump ours: a PreVote request (probes a
    // future term) and a GRANTED PreVote reply (carries that future term). A
    // REJECTED PreVote reply carries the responder's real term and does step us
    // down.
    const bool preVoteReq = std::holds_alternative<RequestVote>(m.payload) && std::get<RequestVote>(m.payload).preVote;
    const bool preVoteGrant = std::holds_alternative<RequestVoteReply>(m.payload) &&
                              std::get<RequestVoteReply>(m.payload).preVote &&
                              std::get<RequestVoteReply>(m.payload).voteGranted;

    // CheckQuorum bookkeeping: as leader, note any voter we hear from.
    if (role_ == Role::Leader && isVoter(m.from))
        recentActive_.insert(m.from);

    if (mTerm > currentTerm_) {
        if (isVoteRequest(m.payload)) {
            // Disruption guard (CheckQuorum lease): a node that still hears from a
            // valid leader refuses to grant votes / bump term.
            //
            // EXCEPT for a LEADER TRANSFER (ADR 0005, debt D-9). TimeoutNow lets the
            // transferee skip its OWN lease, but this guard is the other voters', and
            // they are all still hearing the outgoing leader's heartbeats -- so without
            // the exception every one of them dropped the transferee's vote here, without
            // even bumping its term, and a transfer that takes 0 tick rounds with
            // CheckQuorum off took a full election timeout (2.5-5 s of leaderlessness at
            // the production tick) with it on. That is what forced the revert in
            // 1f2e752; this is the etcd fix that lets CheckQuorum come back.
            //
            // The flag is a HINT, not an assertion we can verify -- and it does not need
            // to be. It stands the lease down and does nothing else: the vote still has
            // to pass §5.4.1's log check and the one-vote-per-term rule below, so the
            // most a lying peer buys is the situation that shipped for months with
            // CheckQuorum off. It is also self-limiting, because only the current leader
            // sends TimeoutNow, and a TimeoutNow is only honoured from the believed
            // leader at the current term.
            const bool transferVote = std::get<RequestVote>(m.payload).campaignTransfer;
            const bool inLease =
                opts_.checkQuorum && leaderId_ != kNoNode && electionElapsed_ < electionTimeout_ && !transferVote;
            if (inLease) {
                return;  // ignore; do not bump term
            }
        }
        if (preVoteReq || preVoteGrant) {
            // Do not change our term for a probe or a straw-poll grant.
        } else {
            becomeFollower(mTerm, isLeaderMessage(m.payload) ? m.from : kNoNode);
        }
    } else if (mTerm < currentTerm_) {
        // Force the stale sender to learn the newer term.
        if (std::holds_alternative<AppendEntries>(m.payload) || std::holds_alternative<InstallSnapshot>(m.payload)) {
            AppendEntriesReply r;
            r.term = currentTerm_;
            r.success = false;
            send(Message{.to = m.from, .from = id_, .payload = r});
        } else if (auto* rv = std::get_if<RequestVote>(&m.payload)) {
            RequestVoteReply r;
            r.preVote = rv->preVote;
            r.term = currentTerm_;
            r.voteGranted = false;
            send(Message{.to = m.from, .from = id_, .payload = r});
        }
        return;
    }

    // From here mTerm == currentTerm_ (or a PreVote probe we intentionally did not
    // bump to). Dispatch.
    std::visit(
        [&](const auto& p) {
            using T = std::decay_t<decltype(p)>;
            if constexpr (std::is_same_v<T, RequestVote>) {
                handleRequestVote(m.from, p);
            } else if constexpr (std::is_same_v<T, RequestVoteReply>) {
                handleRequestVoteReply(m.from, p);
            } else if constexpr (std::is_same_v<T, AppendEntries>) {
                handleAppendEntries(m.from, p);
            } else if constexpr (std::is_same_v<T, AppendEntriesReply>) {
                handleAppendEntriesReply(m.from, p);
            } else if constexpr (std::is_same_v<T, InstallSnapshot>) {
                handleInstallSnapshot(m.from, p);
            } else if constexpr (std::is_same_v<T, InstallSnapshotReply>) {
                handleInstallSnapshotReply(m.from, p);
            } else if constexpr (std::is_same_v<T, TimeoutNow>) {
                // Leader transfer: elect NOW, forcing a real election (skip the
                // PreVote straw poll and our own CheckQuorum lease) so leadership
                // moves deterministically. Learners ignore it.
                //
                // `transfer=true` marks the vote requests this campaign sends, so the
                // OTHER voters' leases stand aside too (ADR 0005). Skipping only our own
                // is what made CheckQuorum break transfers: we campaigned instantly and
                // then nobody answered. This is the ONLY site that sets the flag -- a
                // node cannot elect itself with it, only be elected with it by the
                // incumbent, because this arm is reached only for a TimeoutNow accepted
                // from the leader we follow at the current term.
                if (isVoter(id_))
                    becomeCandidate(/*transfer=*/true);
            }
        },
        m.payload);
}

void RaftNode::handleRequestVote(NodeId from, const RequestVote& rv) {
    // canVote: we have not committed our vote elsewhere this term, and we do not
    // currently follow a leader (a follower never votes against its own leader).
    // Re-granting to the same candidate is idempotent. A PreVote probe at a
    // strictly higher term is always eligible (it changes no durable state).
    const bool canVote =
        (votedFor_ == from) || (votedFor_ == kNoNode && leaderId_ == kNoNode) || (rv.preVote && rv.term > currentTerm_);
    // A learner never grants a vote (it is not part of the voting configuration).
    const bool grant = isVoter(id_) && canVote && log_.isUpToDate(rv.lastLogIndex, rv.lastLogTerm);

    RequestVoteReply reply;
    reply.preVote = rv.preVote;
    // Only a PreVote GRANT echoes the probed (future) term, so the candidate can
    // tally it without inflating anyone's term. A reject -- and every real vote --
    // carries OUR actual term: if we are genuinely ahead the candidate steps down;
    // if we are equal/behind it must NOT bump its term off a mere rejection.
    reply.term = (rv.preVote && grant) ? rv.term : currentTerm_;
    reply.voteGranted = grant;

    if (grant && !rv.preVote) {
        votedFor_ = from;  // durable vote
        hsDirty_ = true;
        electionElapsed_ = 0;  // we granted; don't also time out and compete
    }
    send(Message{.to = from, .from = id_, .payload = reply});
}

void RaftNode::handleRequestVoteReply(NodeId from, const RequestVoteReply& rr) {
    if (!isVoter(from))
        return;  // a stray/decommissioned replica's reply must never count toward quorum
    if (rr.preVote) {
        if (role_ != Role::PreCandidate)
            return;  // we already advanced or stepped down
        votes_[from] = rr.voteGranted;
        if (electionWon())
            becomeCandidate();  // straw poll won -> real election
        else if (electionLost())
            becomeFollower(currentTerm_, kNoNode);  // pre-election lost; back off
        return;
    }
    if (role_ != Role::Candidate)
        return;
    if (rr.term != currentTerm_)
        return;  // stale reply from another term
    votes_[from] = rr.voteGranted;
    if (electionWon())
        becomeLeader();
    else if (electionLost())
        becomeFollower(currentTerm_, kNoNode);  // lost this election; wait to retry
}

LogIndex RaftNode::firstIndexOfTerm(Term t, LogIndex at) const {
    LogIndex i = at;
    while (i > log_.firstIndex()) {
        const auto prev = log_.term(i - 1);
        if (!prev || *prev != t)
            break;
        --i;
    }
    return i;
}

void RaftNode::handleAppendEntries(NodeId from, const AppendEntries& ae) {
    // A valid leader exists at our term: (re)become its follower and reset the
    // election clock so we don't compete with it.
    if (role_ != Role::Follower)
        becomeFollower(currentTerm_, ae.leaderId);
    else {
        leaderId_ = ae.leaderId;
        electionElapsed_ = 0;
    }

    LogIndex lastNew = kNoIndex;
    if (!log_.maybeAppend(ae.prevLogIndex, ae.prevLogTerm, ae.entries, lastNew)) {
        AppendEntriesReply r;
        r.term = currentTerm_;
        r.success = false;
        r.readSeq = ae.readSeq;  // echo for ReadIndex confirmation
        if (log_.lastIndex() < ae.prevLogIndex) {
            // Our log is too short: tell the leader to back up to our end.
            r.conflictTerm = kNoTerm;
            r.conflictIndex = log_.lastIndex() + 1;
        } else {
            // Term conflict at prevLogIndex: report the whole conflicting term so
            // the leader can skip it in one round trip.
            const Term ct = log_.term(ae.prevLogIndex).value_or(kNoTerm);
            r.conflictTerm = ct;
            r.conflictIndex = firstIndexOfTerm(ct, ae.prevLogIndex);
        }
        send(Message{.to = from, .from = id_, .payload = r});
        return;
    }

    // Entries accepted. The earliest index that may have changed is prevLogIndex+1
    // (conservative; identical re-delivered entries are simply re-persisted).
    if (!ae.entries.empty())
        unstableStart_ = std::min(unstableStart_, ae.prevLogIndex + 1);

    // Re-derive the active config only when this batch could have changed it: it
    // carried a ConfigChange, or a truncation may have dropped the current one
    // (the latest config entry sits above the branch point).
    bool batchHadConfig = false;
    for (const auto& e : ae.entries)
        if (e.type == EntryType::ConfigChange)
            batchHadConfig = true;
    if (batchHadConfig || latestConfigIndex_ > ae.prevLogIndex)
        recomputeConfigFromLog();

    advanceCommitAsFollower(ae.leaderCommit, lastNew);

    AppendEntriesReply r;
    r.term = currentTerm_;
    r.success = true;
    r.matchIndex = lastNew;
    r.readSeq = ae.readSeq;  // echo for ReadIndex confirmation
    send(Message{.to = from, .from = id_, .payload = r});
}

void RaftNode::advanceCommitAsFollower(LogIndex leaderCommit, LogIndex lastNewIndex) {
    const LogIndex want = std::min(leaderCommit, lastNewIndex);
    if (want > commitIndex_)
        commitIndex_ = std::min(want, log_.lastIndex());
}

bool RaftNode::requestReadIndex(uint64_t context) {
    if (role_ != Role::Leader)
        return false;
    // Start a fresh confirmation round: bump the heartbeat sequence, record the
    // read against it, self-ack, and heartbeat. A quorum echoing this (or a
    // higher) seq proves current-term leadership AFTER this request.
    ++readSeq_;
    ackedReadSeq_[id_] = readSeq_;
    pendingReads_.push_back({context, readSeq_});
    confirmReads();  // single-voter groups confirm immediately
    if (!pendingReads_.empty())
        bcastAppend();
    return true;
}

void RaftNode::confirmReads() {
    if (role_ != Role::Leader || pendingReads_.empty())
        return;
    // A read is only linearizable once the leader has committed an entry in its
    // CURRENT term (so commitIndex reflects real committed state, not a stale
    // prior-term value). becomeLeader's no-op guarantees this shortly after
    // election; until then, hold the reads.
    if (log_.term(commitIndex_) != std::optional<Term>(currentTerm_))
        return;

    // The highest readSeq a quorum of voters has echoed.
    std::vector<uint64_t> seqs;
    seqs.reserve(config_.voters.size());
    for (NodeId v : config_.voters)
        seqs.push_back(ackedReadSeq_.count(v) ? ackedReadSeq_.at(v) : 0);
    std::sort(seqs.begin(), seqs.end(), std::greater<>());
    uint64_t quorumSeq = seqs.empty() ? 0 : seqs[majSize(config_.voters) - 1];
    if (config_.joint()) {  // during a transition, need both majorities
        std::vector<uint64_t> oseqs;
        for (NodeId v : config_.votersOutgoing)
            oseqs.push_back(ackedReadSeq_.count(v) ? ackedReadSeq_.at(v) : 0);
        std::sort(oseqs.begin(), oseqs.end(), std::greater<>());
        const uint64_t oSeq = oseqs.empty() ? 0 : oseqs[majSize(config_.votersOutgoing) - 1];
        quorumSeq = std::min(quorumSeq, oSeq);
    }

    // Confirm every pending read whose round is covered, capturing the CURRENT
    // commit index as its barrier (>= the commit at request time; still
    // linearizable, and guaranteed to include a current-term entry).
    std::vector<PendingRead> stillPending;
    for (const auto& pr : pendingReads_) {
        if (pr.atSeq <= quorumSeq)
            confirmedReads_.push_back(ReadState{pr.context, commitIndex_});
        else
            stillPending.push_back(pr);
    }
    pendingReads_ = std::move(stillPending);
}

bool RaftNode::propose(std::string data) {
    if (role_ != Role::Leader)
        return false;
    if (leadTransferee_ != kNoNode)
        return false;  // stop accepting writes while handing off leadership
    LogEntry e;
    e.term = currentTerm_;
    e.data = std::move(data);
    log_.append({std::move(e)});
    matchIndex_[id_] = log_.lastIndex();
    nextIndex_[id_] = log_.lastIndex() + 1;
    // A single-voter leader commits immediately (its own match is a majority).
    maybeAdvanceCommitAsLeader();
    bcastAppend();
    return true;
}

bool RaftNode::proposeConfChange(std::vector<NodeId> voters, std::vector<NodeId> learners) {
    if (role_ != Role::Leader || leadTransferee_ != kNoNode)
        return false;
    if (config_.joint())
        return false;  // one membership change at a time
    if (voters.empty())
        return false;  // a config with no voters would permanently wedge the group

    // Enter the joint configuration Cold,new: decisions now need a majority in
    // both the new voters and the current (outgoing) voters.
    Config joint;
    joint.voters = std::move(voters);       // Cnew
    joint.votersOutgoing = config_.voters;  // Cold
    joint.learners = std::move(learners);
    LogEntry e;
    e.term = currentTerm_;
    e.type = EntryType::ConfigChange;
    e.data = encodeConfig(joint);
    log_.append({e});
    config_ = joint;  // config takes effect on append, not on commit
    latestConfigIndex_ = log_.lastIndex();

    // Track replication progress for any newly-added peers.
    for (NodeId peer : replicationPeers(config_, id_)) {
        if (!nextIndex_.count(peer)) {
            nextIndex_[peer] = log_.lastIndex() + 1;
            matchIndex_[peer] = kNoIndex;
        }
    }
    matchIndex_[id_] = log_.lastIndex();
    nextIndex_[id_] = log_.lastIndex() + 1;
    maybeAdvanceCommitAsLeader();  // single-voter fast path (may leave joint at once)
    bcastAppend();
    return true;
}

bool RaftNode::hasReady() const {
    return hsDirty_ || pendingSnapshotApply_.has_value() || unstableStart_ <= log_.lastIndex() ||
           commitIndex_ > lastApplied_ || !pendingMessages_.empty() || !confirmedReads_.empty();
}

RaftNode::Ready RaftNode::ready() {
    Ready rd;
    if (hsDirty_)
        rd.hardState = HardState{currentTerm_, votedFor_};
    if (pendingSnapshotApply_)
        rd.snapshot = pendingSnapshotApply_;

    const LogIndex from = std::max(unstableStart_, log_.firstIndex());
    if (from <= log_.lastIndex())
        rd.entries = log_.entriesFrom(from);

    if (commitIndex_ > lastApplied_) {
        const LogIndex applyFrom = std::max(lastApplied_ + 1, log_.firstIndex());
        auto tail = log_.entriesFrom(applyFrom);
        for (auto& e : tail) {
            if (e.index > commitIndex_)
                break;
            rd.committed.push_back(std::move(e));
        }
    }

    rd.messages = pendingMessages_;  // copy; advance() drains what was reported
    rd.readStates = confirmedReads_;
    return rd;
}

// advance() is the ONLY mutation point for output bookkeeping and MUST be called
// immediately after the driver has made rd durable and sent rd.messages, with no
// intervening step()/tick(). It advances persistence/apply watermarks from the
// reported Ready and removes exactly the drained messages (front-erase, so any
// message enqueued afterwards survives).
void RaftNode::advance(const Ready& rd) {
    if (rd.hardState)
        hsDirty_ = false;
    if (rd.snapshot)
        pendingSnapshotApply_.reset();
    if (!rd.entries.empty())
        unstableStart_ = std::max(unstableStart_, rd.entries.back().index + 1);
    // commitIndex_ at ready() time == now (no interleaving under the contract);
    // this also covers the compaction gap where committed entries were applied via
    // a snapshot and rd.committed is therefore empty.
    if (commitIndex_ > lastApplied_)
        lastApplied_ = commitIndex_;
    const size_t drained = std::min(rd.messages.size(), pendingMessages_.size());
    pendingMessages_.erase(pendingMessages_.begin(), pendingMessages_.begin() + drained);
    const size_t readsDrained = std::min(rd.readStates.size(), confirmedReads_.size());
    confirmedReads_.erase(confirmedReads_.begin(), confirmedReads_.begin() + readsDrained);
}

}  // namespace timestar::raft
