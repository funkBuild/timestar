#include "raft_node.hpp"

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

RaftNode::RaftNode(NodeId id, std::vector<NodeId> voters, RaftLog log, HardState hs, RaftOptions opts)
    : id_(id),
      voters_(std::move(voters)),
      opts_(opts),
      currentTerm_(hs.currentTerm),
      votedFor_(hs.votedFor),
      log_(std::move(log)),
      rng_(opts.rngSeed != 0 ? opts.rngSeed : (id * 0x9E3779B97F4A7C15ull) + 1) {
    // Committed entries at or below the snapshot boundary are already applied
    // (they live in the snapshot); start applying after it.
    commitIndex_ = log_.snapshotIndex();
    lastApplied_ = log_.snapshotIndex();
    unstableStart_ = log_.lastIndex() + 1;  // the on-disk log is already durable
    resetElectionTimer();
}

bool RaftNode::isVoter(NodeId n) const {
    return std::find(voters_.begin(), voters_.end(), n) != voters_.end();
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

size_t RaftNode::countVotes(bool granted) const {
    size_t n = 0;
    for (const auto& [node, g] : votes_)
        if (g == granted)
            ++n;
    return n;
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
    if (countVotes(true) >= quorum()) {
        becomeCandidate();  // trivial majority (single voter): go straight to real
        return;
    }
    bcastRequestVote(/*preVote=*/true);
}

void RaftNode::becomeCandidate() {
    role_ = Role::Candidate;
    ++currentTerm_;
    votedFor_ = id_;  // vote for self
    leaderId_ = kNoNode;
    hsDirty_ = true;
    votes_.clear();
    votes_[id_] = true;
    resetElectionTimer();
    // A single-voter group elects itself immediately.
    if (countVotes(true) >= quorum()) {
        becomeLeader();
        return;
    }
    bcastRequestVote(/*preVote=*/false);
}

void RaftNode::becomeLeader() {
    role_ = Role::Leader;
    leaderId_ = id_;
    electionElapsed_ = 0;
    heartbeatElapsed_ = 0;
    votes_.clear();
    recentActive_.clear();  // fresh CheckQuorum window

    // Initialize replication progress: optimistically probe from our log end.
    nextIndex_.clear();
    matchIndex_.clear();
    for (NodeId peer : voters_) {
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

    bcastAppend();  // replicate the no-op + serve as the first heartbeat
}

void RaftNode::sendAppend(NodeId peer) {
    const LogIndex ni = nextIndex_[peer];
    const LogIndex prevIndex = ni - 1;
    const auto prevTerm = log_.term(prevIndex);
    if (!prevTerm) {
        // prevIndex is below our snapshot boundary -> the follower needs an
        // InstallSnapshot, wired in the snapshot brick. Unreachable until
        // leader-side compaction exists.
        return;
    }
    AppendEntries ae;
    ae.term = currentTerm_;
    ae.leaderId = id_;
    ae.prevLogIndex = prevIndex;
    ae.prevLogTerm = *prevTerm;
    ae.entries = log_.entriesFrom(ni);
    ae.leaderCommit = commitIndex_;
    send(Message{.to = peer, .from = id_, .payload = std::move(ae)});
}

void RaftNode::bcastAppend() {
    for (NodeId peer : voters_) {
        if (peer != id_)
            sendAppend(peer);
    }
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
    if (role_ != Role::Leader || !isVoter(from))
        return;

    if (rr.success) {
        if (rr.matchIndex > matchIndex_[from])
            matchIndex_[from] = rr.matchIndex;
        nextIndex_[from] = matchIndex_[from] + 1;
        maybeAdvanceCommitAsLeader();
        // Keep streaming if the follower is still behind our log end.
        if (nextIndex_[from] <= log_.lastIndex())
            sendAppend(from);
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
    // The highest index replicated on a majority of voters (§5.3): sort match
    // indices descending; the quorum-th one is committed by count.
    std::vector<LogIndex> matches;
    matches.reserve(voters_.size());
    for (NodeId v : voters_)
        matches.push_back(matchIndex_.count(v) ? matchIndex_.at(v) : kNoIndex);
    std::sort(matches.begin(), matches.end(), std::greater<>());
    const LogIndex majorityMatch = matches[quorum() - 1];

    // §5.4.2: only commit it if it is from the CURRENT term. Earlier-term entries
    // ride along once a current-term entry above them commits.
    if (majorityMatch > commitIndex_ && log_.term(majorityMatch) == std::optional<Term>(currentTerm_)) {
        commitIndex_ = majorityMatch;
        bcastAppend();  // let followers learn the new commit promptly
    }
}

void RaftNode::bcastRequestVote(bool preVote) {
    for (NodeId peer : voters_) {
        if (peer == id_)
            continue;
        RequestVote rv;
        rv.preVote = preVote;
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

void RaftNode::checkQuorumOrStepDown() {
    // §CheckQuorum: a leader that has not heard from a majority within an election
    // timeout has likely been partitioned away; it steps down so it stops serving
    // stale leader reads and lets the majority side elect freely.
    recentActive_.insert(id_);  // we are trivially in contact with ourselves
    size_t active = 0;
    for (NodeId v : voters_)
        if (recentActive_.count(v))
            ++active;
    recentActive_.clear();
    if (active < quorum())
        becomeFollower(currentTerm_, kNoNode);
}

void RaftNode::tick() {
    if (role_ == Role::Leader) {
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
    // Follower / (pre)candidate: advance the election clock.
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
    const bool preVoteReq =
        std::holds_alternative<RequestVote>(m.payload) && std::get<RequestVote>(m.payload).preVote;
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
            const bool inLease =
                opts_.checkQuorum && leaderId_ != kNoNode && electionElapsed_ < electionTimeout_;
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
        if (std::holds_alternative<AppendEntries>(m.payload) ||
            std::holds_alternative<InstallSnapshot>(m.payload)) {
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
            } else if constexpr (std::is_same_v<T, TimeoutNow>) {
                if (isVoter(id_))
                    campaign();  // leader transfer: elect now
            }
            // AppendEntriesReply / InstallSnapshot / InstallSnapshotReply are
            // handled in the replication and snapshot bricks.
        },
        m.payload);
}

void RaftNode::handleRequestVote(NodeId from, const RequestVote& rv) {
    // canVote: we have not committed our vote elsewhere this term, and we do not
    // currently follow a leader (a follower never votes against its own leader).
    // Re-granting to the same candidate is idempotent. A PreVote probe at a
    // strictly higher term is always eligible (it changes no durable state).
    const bool canVote = (votedFor_ == from) || (votedFor_ == kNoNode && leaderId_ == kNoNode) ||
                         (rv.preVote && rv.term > currentTerm_);
    const bool grant = canVote && log_.isUpToDate(rv.lastLogIndex, rv.lastLogTerm);

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
        if (countVotes(true) >= quorum())
            becomeCandidate();  // straw poll won -> real election
        else if (countVotes(false) >= quorum())
            becomeFollower(currentTerm_, kNoNode);  // pre-election lost; back off
        return;
    }
    if (role_ != Role::Candidate)
        return;
    if (rr.term != currentTerm_)
        return;  // stale reply from another term
    votes_[from] = rr.voteGranted;
    if (countVotes(true) >= quorum())
        becomeLeader();
    else if (countVotes(false) >= quorum())
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

    advanceCommitAsFollower(ae.leaderCommit, lastNew);

    AppendEntriesReply r;
    r.term = currentTerm_;
    r.success = true;
    r.matchIndex = lastNew;
    send(Message{.to = from, .from = id_, .payload = r});
}

void RaftNode::advanceCommitAsFollower(LogIndex leaderCommit, LogIndex lastNewIndex) {
    const LogIndex want = std::min(leaderCommit, lastNewIndex);
    if (want > commitIndex_)
        commitIndex_ = std::min(want, log_.lastIndex());
}

bool RaftNode::propose(std::string data) {
    if (role_ != Role::Leader)
        return false;
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

bool RaftNode::hasReady() const {
    return hsDirty_ || unstableStart_ <= log_.lastIndex() || commitIndex_ > lastApplied_ ||
           !pendingMessages_.empty();
}

RaftNode::Ready RaftNode::ready() {
    Ready rd;
    if (hsDirty_)
        rd.hardState = HardState{currentTerm_, votedFor_};

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
    if (!rd.entries.empty())
        unstableStart_ = std::max(unstableStart_, rd.entries.back().index + 1);
    // commitIndex_ at ready() time == now (no interleaving under the contract);
    // this also covers the compaction gap where committed entries were applied via
    // a snapshot and rd.committed is therefore empty.
    if (commitIndex_ > lastApplied_)
        lastApplied_ = commitIndex_;
    const size_t drained = std::min(rd.messages.size(), pendingMessages_.size());
    pendingMessages_.erase(pendingMessages_.begin(), pendingMessages_.begin() + drained);
}

}  // namespace timestar::raft
