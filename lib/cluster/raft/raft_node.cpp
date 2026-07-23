#include "raft_node.hpp"

#include <algorithm>

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
    resetElectionTimer();
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
    bcastRequestVote();
}

void RaftNode::becomeLeader() {
    role_ = Role::Leader;
    leaderId_ = id_;
    electionElapsed_ = 0;
    votes_.clear();
    // Log replication (no-op entry, nextIndex/matchIndex, heartbeats) is wired in
    // the replication brick; a freshly elected leader here simply stops electing.
}

void RaftNode::bcastRequestVote() {
    for (NodeId peer : voters_) {
        if (peer == id_)
            continue;
        RequestVote rv;
        rv.preVote = false;
        rv.term = currentTerm_;
        rv.candidateId = id_;
        rv.lastLogIndex = log_.lastIndex();
        rv.lastLogTerm = log_.lastTerm();
        send(Message{.to = peer, .from = id_, .payload = rv});
    }
}

void RaftNode::campaign() {
    becomeCandidate();
}

void RaftNode::tick() {
    if (role_ == Role::Leader) {
        // Heartbeat emission is added in the replication brick.
        return;
    }
    // Follower / candidate: advance the election clock.
    ++electionElapsed_;
    if (electionElapsed_ >= electionTimeout_)
        becomeCandidate();
}

void RaftNode::step(Message m) {
    const Term mTerm = messageTerm(m);
    const bool preVoteMsg = std::holds_alternative<RequestVote>(m.payload) &&
                            std::get<RequestVote>(m.payload).preVote;

    if (mTerm > currentTerm_) {
        if (isVoteRequest(m.payload)) {
            // Disruption guard (CheckQuorum lease): a node that still hears from a
            // valid leader refuses to grant votes / bump term. (Off until the
            // CheckQuorum brick; harmless no-op while checkQuorum is false.)
            const bool inLease =
                opts_.checkQuorum && leaderId_ != kNoNode && electionElapsed_ < electionTimeout_;
            if (inLease) {
                return;  // ignore; do not bump term
            }
        }
        if (preVoteMsg) {
            // A PreVote probe never changes our term.
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
    // A PreVote reply carries the probed term; a real vote reply carries ours.
    reply.term = rv.preVote ? rv.term : currentTerm_;
    reply.voteGranted = grant;

    if (grant && !rv.preVote) {
        votedFor_ = from;  // durable vote
        hsDirty_ = true;
        electionElapsed_ = 0;  // we granted; don't also time out and compete
    }
    send(Message{.to = from, .from = id_, .payload = reply});
}

void RaftNode::handleRequestVoteReply(NodeId from, const RequestVoteReply& rr) {
    if (role_ != Role::Candidate || rr.preVote)
        return;  // PreVote tallying is added in the pre-vote brick
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

bool RaftNode::propose(std::string /*data*/) {
    // Leader append + replication is wired in the replication brick.
    return false;
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

    rd.messages = std::move(pendingMessages_);
    pendingMessages_.clear();
    return rd;
}

void RaftNode::advance(const Ready& rd) {
    if (rd.hardState)
        hsDirty_ = false;
    if (!rd.entries.empty())
        unstableStart_ = std::max(unstableStart_, rd.entries.back().index + 1);
    if (commitIndex_ > lastApplied_)
        lastApplied_ = commitIndex_;
}

}  // namespace timestar::raft
