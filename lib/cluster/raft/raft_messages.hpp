#pragma once

#include "raft_types.hpp"

#include <variant>
#include <vector>

namespace timestar::raft {

// Wire messages for one Raft group. These are transport-agnostic value types;
// the Seastar-RPC driver serializes each variant alternative to a registered
// verb and carries the (group/VShard id, to, from) envelope around them. The
// core state machine only ever produces and consumes these.

// §5.2 / §5.4.1 leader election. When preVote is set this is a PreVote probe:
// the candidate asks "would you vote for me at term?" WITHOUT anyone bumping
// their term, so a partitioned node cannot disrupt a stable leader by forcing
// term inflation.
struct RequestVote {
    bool preVote = false;
    Term term = kNoTerm;  // candidate's term (the probed term when preVote)
    NodeId candidateId = kNoNode;
    LogIndex lastLogIndex = kNoIndex;
    Term lastLogTerm = kNoTerm;
};

struct RequestVoteReply {
    bool preVote = false;
    Term term = kNoTerm;  // responder's currentTerm (probed term on a preVote grant)
    bool voteGranted = false;
};

// §5.3 log replication, also the heartbeat (empty entries).
struct AppendEntries {
    Term term = kNoTerm;
    NodeId leaderId = kNoNode;
    LogIndex prevLogIndex = kNoIndex;
    Term prevLogTerm = kNoTerm;
    std::vector<LogEntry> entries;
    LogIndex leaderCommit = kNoIndex;
};

struct AppendEntriesReply {
    Term term = kNoTerm;
    bool success = false;
    // On success: the highest index that now matches the leader's log (so the
    // leader can advance matchIndex without recomputing from prevLogIndex+len).
    LogIndex matchIndex = kNoIndex;
    // On rejection: a backtracking hint so the leader can skip a whole
    // conflicting term in one step instead of decrementing nextIndex by one
    // per round trip (§ optimization). conflictTerm==0 means "my log is shorter
    // than prevLogIndex"; conflictIndex is then our lastIndex()+1.
    LogIndex conflictIndex = kNoIndex;
    Term conflictTerm = kNoTerm;
};

// §7 snapshot install, when the follower has fallen behind the leader's
// compacted prefix. `data` is opaque to Raft (a VShard snapshot payload here).
struct InstallSnapshot {
    Term term = kNoTerm;
    NodeId leaderId = kNoNode;
    LogIndex lastIncludedIndex = kNoIndex;
    Term lastIncludedTerm = kNoTerm;
    Config config;  // membership as of the snapshot boundary
    std::string data;
};

struct InstallSnapshotReply {
    Term term = kNoTerm;
    LogIndex matchIndex = kNoIndex;  // follower's log end after install
};

// §3.10 leader transfer: the current leader tells `to` to start an election
// immediately (skipping its election timeout) so leadership moves deterministically.
struct TimeoutNow {
    Term term = kNoTerm;
    NodeId leaderId = kNoNode;
};

using MessagePayload = std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply,
                                    InstallSnapshot, InstallSnapshotReply, TimeoutNow>;

// An addressed message the core emits or receives. `to`/`from` are node ids
// within the group; the group id lives in the transport envelope.
struct Message {
    NodeId to = kNoNode;
    NodeId from = kNoNode;
    MessagePayload payload;
};

}  // namespace timestar::raft
