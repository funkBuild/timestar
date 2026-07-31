#include "raft_node.hpp"

#include "raft_config.hpp"

#include <algorithm>
#include <functional>
#include <limits>
#include <stdexcept>

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
    // The leader-transfer abandon bound (§3.10, debt D-20). Resolved ONCE, here, because
    // it is a property of the configured cadence and not of the current election draw --
    // see RaftOptions::transferTimeout for why it is two heartbeats and not one election
    // timeout, and why shortening it is safe.
    //
    // Clamped against electionTimeoutMin rather than the randomized electionTimeout_, so
    // the invariant holds for EVERY draw this node will ever make, not just its first --
    // and STRICTLY below it, matching the boundary `ClusterDataPlane::start` refuses to
    // boot on (`>=`). A window EQUAL to the shortest election timeout is the pre-D-20
    // behaviour, so the two sites must not disagree about which side of the line that is.
    const unsigned derivedTransfer =
        opts_.transferTimeout != 0 ? opts_.transferTimeout : std::max(2u * opts_.heartbeatTimeout, 1u);
    const unsigned transferCeiling = opts_.electionTimeoutMin > 1 ? opts_.electionTimeoutMin - 1 : 1;
    transferTimeout_ = std::max(1u, std::min(derivedTransfer, transferCeiling));
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

// Is `peer` still someone this node replicates to? Same membership as replicationPeers,
// answered without building the set (debt D-37, review F6: a transfer to a peer REMOVED
// from the configuration must not keep a budget ticket, and there are two places that have
// to ask).
bool isReplicationPeer(const Config& c, NodeId self, NodeId peer) {
    if (peer == self)
        return false;
    auto in = [&](const std::vector<NodeId>& v) { return std::find(v.begin(), v.end(), peer) != v.end(); };
    return in(c.voters) || in(c.votersOutgoing) || in(c.learners);
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
    clearTransfers();      // chunked-snapshot progress is per-term too (D-5); releases the shard budget
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
    // No transfer this term has begun. A stale record would let a fresh leader believe a
    // chunk is in flight and so decline to start the transfer the peer actually needs.
    clearTransfers();
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
    // NEVER GRANT A TICKET TO A NON-MEMBER (debt D-37, review F6). Today every caller comes
    // from `replicationPeers`, so this is defence rather than a live path -- but a shard
    // budget makes the consequence of a stray grant everyone else's problem, not just this
    // group's: the ticket holds a FIFO position, probes a node that is no longer in the
    // configuration every heartbeat, and on reaching the head of the queue burns a slot for
    // a whole stall-and-abandon cycle before anyone else can have it.
    if (!isReplicationPeer(config_, id_, peer))
        return;
    const uint64_t total = snapshot_.data.size();
    // 0 == chunking disabled: one message carries the whole payload (pre-D-5 behaviour,
    // and what the core unit tests exercise).
    const size_t chunk =
        opts_.maxSnapshotChunkBytes == 0 ? std::numeric_limits<size_t>::max() : opts_.maxSnapshotChunkBytes;

    // REFUSE, WITHOUT ADVANCING nextIndex_ AND WITHOUT CREATING ANY TRANSFER STATE, a
    // snapshot this node cannot deliver (write-scaleout 5 review, F3a -- the discipline is
    // unchanged, only what "cannot deliver" means has moved).
    //
    // Two independent bounds, and the ORDER matters: both are checked before anything is
    // recorded, so a refusal leaves the peer's progress and this node's state exactly as
    // they were. The peer stays uncaught-up, which is the truth, and the counter says why.
    //
    //  (1) the TOTAL payload against opts_.maxSnapshotBytes -- a memory bound now that
    //      chunking removed the message one (see kMaxVShardSnapshotBytes);
    //  (2) the FIRST CHUNK against opts_.maxMessageBytes. With chunking on this can only
    //      fire on a misconfiguration (chunk > message bound); with chunking OFF the first
    //      chunk IS the whole payload, so this reproduces the original F3a check exactly.
    if (opts_.maxSnapshotBytes != 0 && total > opts_.maxSnapshotBytes) {
        ++undeliverableSnapshots_;
        return;
    }
    if (opts_.maxMessageBytes != 0 && std::min<uint64_t>(chunk, total) > opts_.maxMessageBytes) {
        ++undeliverableSnapshots_;
        return;
    }

    // FLOW CONTROL. A transfer already in flight for THIS snapshot is left alone: the
    // reply-driven pipeline carries it and the tick sweep recovers it. Without this the
    // heartbeat (bcastAppend -> sendAppend -> here, every heartbeatTimeout) would restart
    // every transfer from offset 0 forever and no snapshot would ever complete.
    if (auto it = snapTransfers_.find(peer); it != snapTransfers_.end()) {
        if (it->second.index == snapshot_.index && it->second.term == snapshot_.term)
            return;
        // The snapshot moved on under us (a newer compaction). The peer's staging is keyed
        // by (index, term) and will discard its partial on the first chunk of the new one.
        endTransfer(peer);
    }
    SnapshotTransfer t;
    t.index = snapshot_.index;
    t.term = snapshot_.term;

    // THE SHARD BUDGET (debt D-37). Without one this always started immediately, so ~1365
    // groups could each put a chunk on the wire at once and the only thing bounding the
    // aggregate was the peer's inbound semaphore -- which queues, turning a burst into
    // every transfer crawling. A group that cannot start now takes a FIFO ticket and the
    // tick sweep starts it when its turn comes.
    if (opts_.snapshotBudget != nullptr) {
        t.ticket = opts_.snapshotBudget->enqueue();
        if (!opts_.snapshotBudget->tryAcquire(t.ticket)) {
            t.active = false;
            snapTransfers_[peer] = t;
            ++snapshotTransfersDeferred_;
            // The peer hears NOTHING while it waits, and chunks are what its election
            // clock is living on -- so give it a heartbeat it can reject.
            sendSnapshotWaitProbe(peer);
            return;
        }
    }
    t.active = true;
    snapTransfers_[peer] = t;
    sendSnapshotChunk(peer, 0, chunk);
}

void RaftNode::sendSnapshotWaitProbe(NodeId peer) {
    // Anchored at the snapshot BOUNDARY, which is the lowest index we can still name a
    // term for. The follower is below it (that is why it needs a snapshot), so it rejects
    // -- after `handleAppendEntries` has already reset its election clock, which is what
    // this is for. The rejection's conflict hint walks nextIndex_ back down to the
    // boundary, `sendAppend` hands off to `sendInstallSnapshot`, and the waiting record
    // makes THAT a no-op -- so this probe cannot become a reply-driven hot loop; it is
    // emitted only by the tick sweep, once per heartbeat interval.
    //
    // If the follower turns out to HAVE the boundary entry it answers success instead,
    // nextIndex_ advances past it and the transfer is dropped as unnecessary (see
    // handleAppendEntriesReply) -- which is a correct answer, not a lie.
    AppendEntries ae;
    ae.term = currentTerm_;
    ae.leaderId = id_;
    ae.prevLogIndex = snapshot_.index;
    ae.prevLogTerm = snapshot_.term;
    ae.leaderCommit = commitIndex_;
    ae.readSeq = readSeq_;
    send(Message{.to = peer, .from = id_, .payload = std::move(ae)});
}

void RaftNode::endTransfer(NodeId peer) {
    auto it = snapTransfers_.find(peer);
    if (it == snapTransfers_.end())
        return;
    if (opts_.snapshotBudget != nullptr) {
        if (it->second.active)
            opts_.snapshotBudget->release();
        else
            opts_.snapshotBudget->cancel(it->second.ticket);
    }
    snapTransfers_.erase(it);
}

void RaftNode::clearTransfers() {
    while (!snapTransfers_.empty())
        endTransfer(snapTransfers_.begin()->first);
}

void RaftNode::sendSnapshotChunk(NodeId peer, uint64_t offset, size_t chunkLen) {
    const uint64_t total = snapshot_.data.size();
    if (offset > total)
        offset = total;  // defensive: a peer cannot make us read past the payload
    const size_t len = static_cast<size_t>(std::min<uint64_t>(chunkLen, total - offset));

    InstallSnapshot is;
    is.term = currentTerm_;
    is.leaderId = id_;
    is.lastIncludedIndex = snapshot_.index;
    is.lastIncludedTerm = snapshot_.term;
    is.config = snapshot_.config;  // the boundary config rides every chunk
    is.data = snapshot_.data.substr(static_cast<size_t>(offset), len);
    is.offset = offset;
    is.totalBytes = total;
    is.done = (offset + len == total);

    if (auto it = snapTransfers_.find(peer); it != snapTransfers_.end()) {
        it->second.offset = offset;
        it->second.idleTicks = 0;
    }
    ++snapshotChunksSent_;
    // NOTHING OPTIMISTIC HERE. `nextIndex_[peer]` is advanced ONLY by the reply that says
    // the follower installed (handleInstallSnapshotReply), which is what makes abandoning
    // a transfer a no-op and makes a dropped final chunk cost a retry rather than a lie.
    // The pre-D-5 code advanced before the send, and that is what turned a refused send
    // into the F3a hot loop.
    send(Message{.to = peer, .from = id_, .payload = std::move(is)});
}

void RaftNode::sweepStalledSnapshotTransfers(unsigned passes) {
    // A DROPPED CHUNK IS SILENT. The deliver verb is `no_wait`, so a chunk lost to an
    // over-budget peer, a reset connection or a restarted follower produces no reply and
    // no error -- and because at most one chunk per peer is in flight, silence means the
    // transfer is simply stopped. This timer is the only thing that notices.
    const size_t chunkLen =
        opts_.maxSnapshotChunkBytes == 0 ? std::numeric_limits<size_t>::max() : opts_.maxSnapshotChunkBytes;
    for (auto it = snapTransfers_.begin(); it != snapTransfers_.end();) {
        SnapshotTransfer& t = it->second;
        // A PEER REMOVED FROM THE CONFIGURATION (debt D-37, review F6). Nothing else drops
        // these: role change clears the map but a CONFIG change does not, and a queued
        // transfer is exempt from the stall/abandon path by design -- so without this a
        // departed member's ticket sits in the shard's FIFO indefinitely, probing a node
        // that is not a member and eventually spending a slot on a transfer nobody wants.
        if (!isReplicationPeer(config_, id_, it->first)) {
            const NodeId gone = it->first;
            ++it;
            endTransfer(gone);
            continue;
        }
        t.idleTicks += passes;
        if (!t.active) {
            // WAITING ON THE SHARD BUDGET (debt D-37). This is where a deferred transfer
            // gets its turn: the budget hands the slot to the OLDEST outstanding ticket, so
            // the order groups asked in is the order they run in -- ticking groups in map
            // order would otherwise let the lowest-numbered group win every free slot
            // forever. Stall/abandon logic deliberately does NOT apply: nothing is on the
            // wire to be stalled, and abandoning a waiter would just re-queue it at the
            // BACK, which is starvation dressed as recovery.
            const NodeId waiter = it->first;
            if (opts_.snapshotBudget != nullptr && opts_.snapshotBudget->tryAcquire(t.ticket)) {
                t.active = true;
                t.idleTicks = 0;
                t.resends = 0;
                ++it;
                sendSnapshotChunk(waiter, 0, chunkLen);
                continue;
            }
            // Still queued: keep the peer's election clock alive on the heartbeat cadence
            // (it is being sent nothing else at all).
            if (opts_.heartbeatTimeout != 0 && t.idleTicks >= opts_.heartbeatTimeout) {
                t.idleTicks = 0;
                ++it;
                sendSnapshotWaitProbe(waiter);
                continue;
            }
            ++it;
            continue;
        }
        if (opts_.snapshotChunkTimeout == 0 || t.idleTicks < opts_.snapshotChunkTimeout) {
            ++it;
            continue;
        }
        if (t.resends >= opts_.maxSnapshotResends) {
            // ABANDON. Nothing to unwind: no nextIndex_ was moved on this transfer's
            // behalf, so the peer is left exactly as far behind as it really is and the
            // next heartbeat's sendAppend starts a fresh transfer from offset 0. The
            // counter is the operator's signal that a peer cannot be snapshot-caught-up
            // (a dropped-chunk storm, or a peer that predates the chunk tag).
            ++snapshotTransfersAbandoned_;
            const NodeId dead = it->first;
            ++it;
            endTransfer(dead);
            continue;
        }
        ++t.resends;
        ++snapshotTransfersRestarted_;
        // Resume from what the peer last CONFIRMED it has staged, not from where we think
        // we were: if our chunk arrived and only the reply was lost, `acked` is stale and
        // the peer answers the duplicate with its real offset (its staged prefix is the
        // only state it keeps, so a duplicate chunk is idempotent by construction).
        const NodeId peer = it->first;
        const uint64_t resumeAt = t.acked;
        ++it;  // sendSnapshotChunk mutates only this peer's record, but advance first anyway
        sendSnapshotChunk(peer, resumeAt, chunkLen);
    }
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

    // EVERY IN-FLIGHT TRANSFER IS NOW FEEDING BYTES THAT NO LONGER EXIST (review F3).
    // `snapshot_.data` was just REPLACED IN PLACE, so a transfer mid-way through the old
    // payload would continue at its old offset into the NEW one. When the (index, term)
    // pair also happened to match -- a re-snapshot at the SAME boundary, which a group with
    // a stalled flush watermark produces on every sweep -- handleInstallSnapshotReply's
    // "our snapshot moved on" guard does not fire either, so the follower would be handed a
    // SPLICE of two different snapshots and, if the lengths lined up, would install it as
    // valid. Dropping the transfers makes the next sendAppend restart cleanly from offset
    // 0, and the follower's staging is keyed by (index, term) so it discards its partial
    // the moment a chunk 0 arrives.
    clearTransfers();
}

void RaftNode::seedRecoveredSnapshot(Snapshot snap) {
    if (snap.index == kNoIndex)
        return;
    if (snap.index != log_.snapshotIndex() || snap.term != log_.snapshotTerm())
        throw std::runtime_error(
            "RaftNode::seedRecoveredSnapshot: the snapshot does not sit at the log's compacted boundary; serving it "
            "would hand a follower a payload that does not match the index it claims");
    // The boundary config lives ONLY in the snapshot once its ConfigChange entries are
    // compacted away, so it is the config floor. Defensive, as in the receive path: an
    // empty voter set (corruption) would brick the group.
    if (!snap.config.voters.empty()) {
        baseConfig_ = snap.config;
        recomputeConfigFromLog();
    }
    snapshot_ = std::move(snap);
    // NOT surfaced via pendingSnapshotApply_: the driver recovered this from its OWN
    // journal, so its state machine is already at (or above) the boundary -- see
    // ReplicatedVShardHost::addVShard for why re-installing it locally would be wrong.
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
        // Stale: we already have everything the snapshot covers. Drop any partial we were
        // staging FOR IT -- it can never be needed and holding it wastes the buffer.
        if (snapStaging_ && snapStaging_->index == is.lastIncludedIndex)
            snapStaging_.reset();
        reply.matchIndex = commitIndex_;
        send(Message{.to = from, .from = id_, .payload = reply});
        return;
    }

    // ---- STAGE THE CHUNK (debt D-5). ----
    //
    // The decoder normalizes a one-message snapshot to (offset 0, total == data.size(),
    // done), so there is ONE path here whether or not the sender chunks.
    //
    // A PARTIAL SNAPSHOT IS NEVER INSTALLED: everything below only appends to
    // `snapStaging_`, and the install happens exactly once, on the chunk that completes
    // the payload. Nothing in the staging path touches the log, the config, commitIndex_
    // or the state machine.
    const uint64_t total = is.totalBytes == 0 ? is.data.size() : is.totalBytes;

    // Refuse a payload we will not hold in memory. Defence in depth: a correctly
    // configured leader refuses to BUILD one (sendInstallSnapshot), so reaching this needs
    // a mismatched or hostile peer. Answer with an install-shaped reply carrying our real
    // commitIndex, which tells the leader nothing was installed and lets it fall back to
    // appends; the counter is what makes the stall diagnosable.
    if (opts_.maxSnapshotBytes != 0 && total > opts_.maxSnapshotBytes) {
        ++snapshotsRefusedTooLarge_;
        snapStaging_.reset();
        reply.matchIndex = commitIndex_;
        send(Message{.to = from, .from = id_, .payload = reply});
        return;
    }

    // A chunk for a DIFFERENT boundary discards what we hold. Keying the partial by
    // (index, term) is what stops one snapshot's bytes being spliced onto another's --
    // which is reachable without any malice: the leader compacts again mid-transfer, or
    // leadership moves and the new leader's snapshot boundary differs.
    if (snapStaging_ && (snapStaging_->index != is.lastIncludedIndex || snapStaging_->term != is.lastIncludedTerm))
        snapStaging_.reset();

    if (is.offset == 0) {
        // A fresh OR RESTARTED transfer. Discarding here unconditionally is what makes the
        // leader's restart idempotent: it may resend chunk 0 at any time, and the result
        // is always a clean transfer rather than a splice onto a stale prefix.
        SnapshotStaging fresh;
        fresh.index = is.lastIncludedIndex;
        fresh.term = is.lastIncludedTerm;
        fresh.totalBytes = total;
        snapStaging_ = std::move(fresh);
    } else if (!snapStaging_) {
        // A chunk from the middle with nothing staged: we restarted, or the leader's
        // earlier chunks were dropped. Say we have zero so it starts over.
        reply.matchIndex = commitIndex_;
        reply.pendingSnapshotIndex = is.lastIncludedIndex;
        reply.stagedBytes = 0;
        send(Message{.to = from, .from = id_, .payload = reply});
        return;
    }

    if (is.offset != snapStaging_->data.size() || total != snapStaging_->totalBytes) {
        // Out of order, or a DUPLICATE of a chunk we already hold. Neither is applied;
        // both are answered with where we actually are, so the leader resumes from the
        // truth instead of from its own (possibly stale) idea of it.
        reply.matchIndex = commitIndex_;
        reply.pendingSnapshotIndex = snapStaging_->index;
        reply.stagedBytes = snapStaging_->data.size();
        send(Message{.to = from, .from = id_, .payload = reply});
        return;
    }

    snapStaging_->config = is.config;  // the boundary config rides every chunk
    snapStaging_->data.append(is.data);

    if (!is.done) {
        // Mid-transfer ack. This is the per-chunk reply the leader paces on: exactly one
        // chunk is in flight at a time, so this is what releases the next one.
        reply.matchIndex = commitIndex_;
        reply.pendingSnapshotIndex = snapStaging_->index;
        reply.stagedBytes = snapStaging_->data.size();
        send(Message{.to = from, .from = id_, .payload = reply});
        return;
    }

    // FINAL CHUNK: the payload is complete, so install it -- atomically, and only now.
    Snapshot full;
    full.index = snapStaging_->index;
    full.term = snapStaging_->term;
    full.config = std::move(snapStaging_->config);
    full.data = std::move(snapStaging_->data);
    snapStaging_.reset();
    installReceivedSnapshot(std::move(full), reply);
    send(Message{.to = from, .from = id_, .payload = reply});
}

// The ONE point at which a received snapshot becomes this replica's state, reached only
// from the chunk that COMPLETES the payload. Everything before it is staging.
void RaftNode::installReceivedSnapshot(Snapshot full, InstallSnapshotReply& reply) {
    // Adopt the snapshot. If our log already holds a matching entry at the
    // snapshot boundary, keep the consistent suffix; otherwise discard the log.
    if (log_.matchTerm(full.index, full.term))
        log_.compactTo(full.index);
    else
        log_.restoreFromSnapshot(full.index, full.term);

    // The snapshot carries the boundary config; re-derive the active config from
    // the new base plus any retained (keep-suffix) config entries. Defensive: a
    // config with no voters (only possible from corruption) would brick the
    // group, so ignore it and keep our current base.
    if (!full.config.voters.empty())
        baseConfig_ = full.config;
    recomputeConfigFromLog();

    commitIndex_ = std::max(commitIndex_, full.index);
    lastApplied_ = full.index;  // the snapshot IS the applied state
    // Only the snapshot boundary is now durable via the payload. Any retained
    // tail ABOVE it (the keep-suffix path) is still unpersisted and MUST remain
    // reportable -- advancing to lastIndex()+1 here would silently un-persist it
    // (it is not in the snapshot payload), losing committed entries on restart.
    unstableStart_ = std::max(unstableStart_, full.index + 1);
    reply.matchIndex = full.index;
    ++snapshotsInstalled_;

    snapshot_ = std::move(full);
    pendingSnapshotApply_ = snapshot_;  // surface to the driver to install
}

void RaftNode::handleInstallSnapshotReply(NodeId from, const InstallSnapshotReply& rr) {
    if (role_ != Role::Leader || !(isVoter(from) || isLearner(from)))
        return;
    lastAckTick_[from] = tick_;  // any reply is a liveness proof; see ticksSinceAck()

    if (!rr.isInstallOutcome()) {
        // MID-TRANSFER PROGRESS (debt D-5): the peer has staged `stagedBytes` and installed
        // NOTHING, so nothing here may touch matchIndex_/nextIndex_ or advance commit --
        // treating this as an install is exactly the "reports match for data it does not
        // have" failure the separate reply shape exists to prevent.
        auto it = snapTransfers_.find(from);
        if (it == snapTransfers_.end() || it->second.index != rr.pendingSnapshotIndex)
            return;  // progress on a transfer we are no longer running: ignore
        SnapshotTransfer& t = it->second;
        t.idleTicks = 0;

        // THE FOLLOWER'S REPORT IS THE TRUTH, INCLUDING WHEN IT GOES BACKWARDS.
        //
        // This assignment used to be `if (rr.stagedBytes > t.acked)`, i.e. monotonic, and
        // that was an IMMORTAL LIVELOCK in exactly the case volatile staging exists for.
        // A follower that restarts mid-transfer loses its staging while the leader's
        // SnapshotTransfer survives; the leader sends offset `t.acked` (say 24 MiB), the
        // follower takes the "chunk from the middle with nothing staged" branch and
        // answers `stagedBytes = 0` -- which the monotonic guard DISCARDED. The leader kept
        // its stale `acked`, resent the same offset, and got the same answer forever: one
        // 4 MiB chunk per round trip, no restart counted, no abandonment, and no other way
        // for that follower to catch up (its log prefix is gone). Demonstrated at 50 rounds
        // with the offset pinned and both counters at zero.
        //
        // A backwards report is not noise -- it is the follower telling us where it really
        // is, which is the entire contract of this reply.
        const bool advanced = rr.stagedBytes > t.acked;
        t.acked = rr.stagedBytes;

        if (advanced) {
            t.resends = 0;  // real progress clears the stall budget
        } else {
            // ANSWERING BUT NOT PROGRESSING. Distinct from silence (which `idleTicks` and
            // the tick sweep handle) and it needs its own bound, because every reply resets
            // `idleTicks` and a peer that answers promptly with no progress would otherwise
            // never be stall-detected at all. Spends the SAME budget as a silent stall, so
            // one bound governs abandonment however the transfer is failing.
            ++snapshotTransfersRestarted_;
            if (++t.resends > opts_.maxSnapshotResends) {
                // Give up. Nothing to unwind: no nextIndex_ was moved on this transfer's
                // behalf, so the peer is left exactly as far behind as it truly is and the
                // next heartbeat starts a fresh transfer from offset 0.
                ++snapshotTransfersAbandoned_;
                endTransfer(from);
                return;
            }
        }
        if (t.index != snapshot_.index || t.term != snapshot_.term) {
            // Our snapshot moved on (a newer compaction) -- stop feeding the old one; the
            // next sendAppend starts a transfer for the current boundary.
            endTransfer(from);
            return;
        }
        // Release the NEXT chunk, from where the peer says it actually is. This reply is
        // the whole flow-control mechanism: one chunk in flight per peer, each released by
        // the ack of the one before it -- the same pipeline shape as the bounded
        // AppendEntries catch-up, which is what makes it safe on a fire-and-forget
        // transport (a burst of chunks could exceed the peer's admission budget and be
        // dropped in silence).
        const size_t chunk =
            opts_.maxSnapshotChunkBytes == 0 ? std::numeric_limits<size_t>::max() : opts_.maxSnapshotChunkBytes;
        sendSnapshotChunk(from, t.acked, chunk);
        return;
    }

    // An install OUTCOME (or a stale-snapshot answer, or any reply from a peer that
    // predates chunking): the transfer, if any, is over.
    endTransfer(from);
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
        // A successful append means this peer has a matching prefix -- it does not need a
        // snapshot after all (the keep-alive probe of a DEFERRED transfer can produce
        // exactly this, when the follower turns out to hold the boundary entry). Drop the
        // queued transfer so its budget ticket is not held for a transfer that will never
        // run; head-of-line fairness makes an abandoned ticket everyone else's problem
        // (debt D-37). Only a WAITING one: an ACTIVE transfer has bytes on the wire and its
        // own machinery, and a reordered stale success must not throw that progress away.
        if (auto it = snapTransfers_.find(from); it != snapTransfers_.end() && !it->second.active)
            endTransfer(from);
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

bool RaftNode::transferLeadership(NodeId target) {
    // Every `return false` below is a call that DID NOTHING, and the caller has no other
    // way to know (debt D-24): the balancer counted all of them as transfers initiated.
    if (role_ != Role::Leader || target == id_ || !isVoter(target))
        return false;
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
        return false;
    leadTransferee_ = target;
    transferElapsed_ = 0;
    if (matchIndex_[target] == log_.lastIndex())
        sendTimeoutNow(target);  // already caught up: elect now
    else
        sendAppend(target);  // catch it up first; TimeoutNow fires on the ack
    return true;
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

void RaftNode::tick(unsigned passes) {
    // `passes` is how many tick INTERVALS this call stands for. It is 1 for a group the
    // driver ticks every pass, and >1 when the driver has been HIBERNATING this group and
    // is now crediting the passes it skipped (debt D-29(b), RaftGroupRegistry::tickAll).
    //
    // Every clock below is therefore advanced by `passes`, not by one, which is what makes
    // them measure REAL time rather than "times the driver got round to us". That matters
    // for one clock in particular: with CheckQuorum on, `electionElapsed_` is also the
    // DISRUPTION-GUARD LEASE, and a 1-in-10 hibernation used to stretch it from 2.5-5 s to
    // 25-50 s -- so a group whose leader had DIED sat refusing the votes that would have
    // replaced it. Measured at 153/400 failed batches and a 43 s recovery on
    // node_kill_round.sh, against 49/400 and 8 s with CheckQuorum off.
    //
    // Hibernation itself is unaffected: a live leader's heartbeats arrive through step()
    // regardless of ticking and reset electionElapsed_ every heartbeatTimeout, which is far
    // shorter than any election timeout, so an idle follower with a live leader still never
    // campaigns and still re-hibernates on its own.
    if (passes == 0)
        return;
    tick_ += passes;  // monotonic clock behind ticksSinceAck(); see raft_node.hpp
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
        // etcd bounds it at ONE ELECTION TIMEOUT; this bounds it at `transferTimeout_`,
        // two heartbeat intervals by default (debt D-20). The deviation is deliberate and
        // its safety argument is on RaftOptions::transferTimeout: at the production timing
        // an election timeout is 2.5-5 s against a 1.5 s write deadline, so the etcd bound
        // makes ONE mis-aimed transfer cost a whole batch, while abandoning is a purely
        // LOCAL decision to resume proposing in a term we already lead -- it cannot
        // produce two leaders in one term, and a transferee that campaigns anyway simply
        // deposes us on the higher term as any election would.
        //
        // The transfer is merely not completed; nothing is unsafe. `leadTransferee_` is
        // cleared, so the next balancer pass may legitimately re-arm a window for the same
        // target -- which is what makes the SHORTER bound a real reduction in refusal
        // rather than a rotation of it: a dead target now costs the group ~1 s per pass
        // instead of a full election timeout, and D-1's liveness filter stops it being
        // chosen at all once its ack clock has decayed.
        if (leadTransferee_ != kNoNode && (transferElapsed_ += passes) >= transferTimeout_) {
            leadTransferee_ = kNoNode;
            transferElapsed_ = 0;
        }
        // Recover chunked snapshot transfers whose in-flight chunk was dropped (D-5).
        // BEFORE the heartbeat, so a transfer this sweep abandons is restarted by the very
        // next bcastAppend rather than waiting another heartbeat interval.
        sweepStalledSnapshotTransfers(passes);
        if ((heartbeatElapsed_ += passes) >= opts_.heartbeatTimeout) {
            heartbeatElapsed_ = 0;
            bcastAppend();  // heartbeat (an AppendEntries, possibly carrying entries)
        }
        if (opts_.checkQuorum) {
            if ((electionElapsed_ += passes) >= electionTimeout_) {
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
    electionElapsed_ += passes;
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

    // A TIMEOUTNOW IS ONLY EVER LEGITIMATE FROM THE LEADER WE CURRENTLY FOLLOW, and this
    // check is what makes the campaignTransfer flag self-limiting (ADR 0005 / debt D-9
    // review, F1). It has to be evaluated HERE, against the PRE-STEP leader belief, and it
    // must drop the message outright:
    //
    //   * `isLeaderMessage` counts TimeoutNow, so a forge at a HIGHER term used to reach
    //     `becomeFollower(mTerm, m.from)` first -- which INSTALLED the forger as our leader
    //     -- and the arm below then read `leaderId_ == m.from` and campaigned with the
    //     transfer flag. Checking the arm against the post-step belief is therefore no
    //     check at all;
    //   * a forge at the SAME term reached the arm regardless.
    //
    // Vote safety was never at risk (the log check and one-vote-per-term are untouched),
    // but with CheckQuorum on, the disruption GUARD became bypassable at will by any buggy
    // or hostile peer: forge a TimeoutNow, get a transfer-flagged campaign, and every
    // voter's lease stands aside. Demonstrated in review, pinned by the forge tests in
    // raft_node_test.cpp.
    //
    // Legitimate transfers are unaffected: a leader only ever sends TimeoutNow to a
    // follower that is already following it (transferLeadership requires
    // `matchIndex`/AppendEntries progress with that peer), and a follower that is BEHIND
    // its leader still passes, because the believed leader is the sender even when the
    // sender's term is newer. Dropping it also refuses the second half of the forge -- a
    // higher-term TimeoutNow from a non-leader no longer bumps our term, installs a false
    // leader, or resets our election timer, i.e. it cannot buy the forger a lease over us
    // either. The cost of a false negative is one abandoned transfer (RaftNode::tick gives
    // up after an election timeout and the group elects normally), never a wedge.
    if (std::holds_alternative<TimeoutNow>(m.payload) && (leaderId_ == kNoNode || m.from != leaderId_))
        return;

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
            // The flag is a HINT, not an assertion this voter can verify -- and it does
            // not need to be. It stands the lease down and does nothing else: the vote
            // still has to pass §5.4.1's log check and the one-vote-per-term rule below,
            // so the most a lying peer buys is the situation that shipped for months with
            // CheckQuorum off.
            //
            // What limits WHO can set it is a check on the CANDIDATE's side, not this one:
            // a transfer-flagged campaign is only ever started from a TimeoutNow whose
            // sender was already the believed leader (the guard near the top of step()).
            // That guard is load-bearing and was missing when the flag first landed --
            // without it a forged TimeoutNow, at the same term or a higher one, produced a
            // transfer-flagged campaign from any peer, so "only the incumbent can cause
            // one" was simply untrue and this lease was bypassable at will (F1). A voter
            // receiving a flagged vote still cannot tell the difference; the property is
            // enforced where it can be.
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
                // then nobody answered.
                //
                // This is the ONLY site that sets the flag, and it is reachable only for a
                // TimeoutNow whose sender was ALREADY our believed leader before this step
                // -- see the guard near the top of step(), which is what makes that true.
                // Until that guard existed the "only the incumbent can cause a
                // transfer-flagged campaign" claim was FALSE for a forged TimeoutNow, at
                // the same term or a higher one (F1).
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
    // election; until then, hold the reads. Shared with the read fence (debt D-36)
    // so the two cannot drift -- see hasCurrentTermCommit().
    if (!hasCurrentTermCommit())
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
