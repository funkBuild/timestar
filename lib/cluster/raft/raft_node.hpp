#pragma once

#include "raft_log.hpp"
#include "raft_messages.hpp"
#include "raft_types.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <random>
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
};

// One replica of one Raft group (one VShard). Deterministic and reactor-free:
// inputs are tick()/step()/propose(); outputs are drained via ready()/advance().
// The Seastar-RPC + timer + journal driver owns all I/O and MUST honour the
// Ready contract: persist hardState and entries (fsync) BEFORE sending messages,
// and apply `committed` only after it is durable.
class RaftNode {
public:
    struct Ready {
        std::optional<HardState> hardState;  // persist (fsync) if present, before messages
        std::vector<LogEntry> entries;       // newly (re)written log entries to persist
        std::vector<LogEntry> committed;     // newly committed entries to apply, in order
        std::vector<Message> messages;       // send only after hardState+entries are durable
        bool empty() const {
            return !hardState && entries.empty() && committed.empty() && messages.empty();
        }
    };

    RaftNode(NodeId id, std::vector<NodeId> voters, RaftLog log, HardState hs, RaftOptions opts);

    // Inputs.
    void tick();          // one logical clock tick (driven by a seastar::timer)
    void step(Message m);  // an incoming RPC for this group
    bool propose(std::string data);  // leader-only append; false if not leader (replication brick)
    void campaign();       // start an election now (bootstrap / TimeoutNow)

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
    LogIndex commitIndex() const { return commitIndex_; }
    const RaftLog& log() const { return log_; }
    unsigned electionTimeout() const { return electionTimeout_; }

private:
    size_t quorum() const { return voters_.size() / 2 + 1; }
    bool isVoter(NodeId n) const;

    void becomeFollower(Term term, NodeId leader);
    void becomeCandidate();
    void becomeLeader();
    void resetElectionTimer();
    void send(Message m);
    void bcastRequestVote();

    void handleRequestVote(NodeId from, const RequestVote& rv);
    void handleRequestVoteReply(NodeId from, const RequestVoteReply& rr);
    void handleAppendEntries(NodeId from, const AppendEntries& ae);
    void advanceCommitAsFollower(LogIndex leaderCommit, LogIndex lastNewIndex);
    LogIndex firstIndexOfTerm(Term t, LogIndex at) const;

    size_t countVotes(bool granted) const;

    NodeId id_;
    std::vector<NodeId> voters_;
    RaftOptions opts_;

    Term currentTerm_ = kNoTerm;
    NodeId votedFor_ = kNoNode;
    Role role_ = Role::Follower;
    NodeId leaderId_ = kNoNode;

    RaftLog log_;
    LogIndex commitIndex_ = kNoIndex;
    LogIndex lastApplied_ = kNoIndex;

    unsigned electionElapsed_ = 0;
    unsigned electionTimeout_ = 0;  // current randomized target
    std::map<NodeId, bool> votes_;  // this election's replies (self included)
    std::mt19937_64 rng_;

    // Output accumulation.
    bool hsDirty_ = false;
    LogIndex unstableStart_ = kNoIndex;  // first log index not yet reported for persistence
    std::vector<Message> pendingMessages_;
};

}  // namespace timestar::raft
