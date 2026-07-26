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
    // §3.10 + CheckQuorum: "the leader I currently follow sent me a TimeoutNow", set ONLY
    // by a campaign started from a TimeoutNow (see RaftNode::step). It makes the voter's
    // CheckQuorum disruption guard stand aside (raft_node.cpp, `inLease`), which is the
    // whole reason CheckQuorum can be on at all: TimeoutNow skips the TRANSFEREE's lease,
    // but every OTHER voter is still hearing the outgoing leader's heartbeats and would
    // otherwise drop the vote silently -- see ADR 0005 and the revert in 1f2e752.
    //
    // IT IS A LEASE BYPASS AND NOTHING ELSE. Every other vote condition still applies
    // (§5.4.1 log up-to-date, one vote per term, term ordering), so a lying peer gains
    // exactly what it already has when CheckQuorum is off -- which was the shipped
    // configuration until this landed.
    //
    // DELIBERATELY THE LAST FIELD: RequestVote is aggregate-initialized positionally in
    // several tests, and a new member in the middle would change what those braces mean.
    // The wire layout does not depend on it either way -- the codec writes named fields,
    // and a transfer-flagged vote travels under its OWN message-type byte (raft_codec.cpp)
    // rather than as an extra byte inside the ordinary one.
    bool campaignTransfer = false;
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
    // ReadIndex barrier: a monotonic heartbeat sequence the follower echoes back
    // so the leader can confirm current-term leadership AFTER a read request (a
    // stale in-flight ack carries an older readSeq and cannot falsely confirm).
    uint64_t readSeq = 0;
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
    // Echoed AppendEntries.readSeq (for the ReadIndex confirmation round).
    uint64_t readSeq = 0;
};

// §7 snapshot install, when the follower has fallen behind the leader's
// compacted prefix. `data` is opaque to Raft (a VShard snapshot payload here).
//
// CHUNKED (debt D-5). `data` is a SLICE of the payload at byte `offset` of `totalBytes`,
// and `done` marks the last one. The receiver accumulates slices into a staging buffer
// and installs only on `done`, so no single message has to carry a whole VShard snapshot
// -- which is what let the size chain in raft_types.hpp come down by 3x.
//
// The defaults describe the pre-D-5 shape (one message, whole payload), so a
// positionally- or partially-initialized InstallSnapshot still means what it always did;
// `isWholePayload()` is what the codec uses to keep emitting the OLD wire tag for a
// snapshot that fits in one chunk, which is what keeps an un-upgraded peer working.
struct InstallSnapshot {
    Term term = kNoTerm;
    NodeId leaderId = kNoNode;
    LogIndex lastIncludedIndex = kNoIndex;
    Term lastIncludedTerm = kNoTerm;
    Config config;  // membership as of the snapshot boundary (rides EVERY chunk)
    std::string data;
    uint64_t offset = 0;      // byte offset of `data` within the whole payload
    uint64_t totalBytes = 0;  // whole payload size (0 == "data is the whole payload")
    bool done = true;         // `data` ends the payload

    // Is this the single-message shape a pre-D-5 peer understands? (Also the shape a
    // pre-D-5 peer PRODUCES, since it can set none of the three fields above.)
    bool isWholePayload() const { return offset == 0 && done && (totalBytes == 0 || totalBytes == data.size()); }
};

struct InstallSnapshotReply {
    Term term = kNoTerm;
    LogIndex matchIndex = kNoIndex;  // follower's log end after install
    // Chunked-transfer progress (D-5). `pendingSnapshotIndex != kNoIndex` means "I have
    // NOT installed anything; I am STAGING that snapshot and hold `stagedBytes` of it --
    // send me the bytes from there". It is the per-chunk ack the leader paces on and the
    // resume point it restarts from, and it is what makes a dropped chunk recoverable at
    // all on a fire-and-forget transport: without it the only signal would be silence.
    //
    // A COMPLETED install (or any reply from a pre-D-5 peer) leaves both at their
    // defaults, which is exactly the old two-field reply -- see the codec for why that
    // matters for mixed versions.
    LogIndex pendingSnapshotIndex = kNoIndex;
    uint64_t stagedBytes = 0;

    bool isInstallOutcome() const { return pendingSnapshotIndex == kNoIndex && stagedBytes == 0; }
};

// §3.10 leader transfer: the current leader tells `to` to start an election
// immediately (skipping its election timeout) so leadership moves deterministically.
struct TimeoutNow {
    Term term = kNoTerm;
    NodeId leaderId = kNoNode;
};

using MessagePayload = std::variant<RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply, InstallSnapshot,
                                    InstallSnapshotReply, TimeoutNow>;

// An addressed message the core emits or receives. `to`/`from` are node ids
// within the group; the group id lives in the transport envelope.
struct Message {
    NodeId to = kNoNode;
    NodeId from = kNoNode;
    MessagePayload payload;
};

}  // namespace timestar::raft
