#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::raft {

// Thrown at a proposal waiter when this node stops leading before the entry it
// appended was observed to commit (write-scaleout 3b). It is the ONE Raft failure a
// write coordinator must treat as AMBIGUOUS: the entry is already in this node's log
// and may well be committed by the successor, so the outcome is unknown -- but it is
// also the routine outcome of a leadership TRANSFER, so it has to be retryable rather
// than a client 5xx. A distinct type exists so the classifier never has to match on a
// message string; see classifyWriteFailure in cluster/data/write_errors.hpp for why
// re-proposing an ambiguous slice is safe (LWW re-apply, ADR 0003).
class LeadershipLostError : public std::runtime_error {
public:
    explicit LeadershipLostError(const std::string& what) : std::runtime_error(what) {}
};

// Core Raft scalar types (Phase 2 / Stage 3). A group is one VShard's replicated
// log; these are shared by the log, the node state machine, and the RPCs.
using Term = uint64_t;      // election term; monotonically non-decreasing per node
using LogIndex = uint64_t;  // 1-based log position (0 = "before the first entry")
using NodeId = uint64_t;    // a replica id; 0 is reserved for "none"

inline constexpr Term kNoTerm = 0;
inline constexpr LogIndex kNoIndex = 0;  // the empty-log / pre-first sentinel
inline constexpr NodeId kNoNode = 0;     // votedFor == none

// PreCandidate runs a §PreVote straw poll (no term bump) before a real
// candidacy, so a partitioned node cannot disrupt a stable leader by inflating
// terms. It transitions to Candidate only once a majority would grant a real vote.
enum class Role : uint8_t { Follower, PreCandidate, Candidate, Leader };

// Normal entries carry an opaque application command; ConfigChange entries carry
// a serialized Config (§6 membership change) that Raft DOES interpret -- a
// replica's active configuration is the latest ConfigChange in its log, applied
// as soon as it is appended (not when committed).
enum class EntryType : uint8_t { Normal = 0, ConfigChange = 1 };

// One replicated log entry. For Normal entries `data` is opaque to Raft; for
// ConfigChange entries it is a serialized Config. The (term, index) pair is what
// the consensus invariants are stated over.
struct LogEntry {
    Term term = kNoTerm;
    LogIndex index = kNoIndex;
    EntryType type = EntryType::Normal;
    std::string data;

    friend bool operator==(const LogEntry&, const LogEntry&) = default;
};

// The Raft-persistent voting state (durably fsync'd before any RPC that depends
// on it, per the journal safety contract). commitIndex is volatile and NOT part
// of hard state.
struct HardState {
    Term currentTerm = kNoTerm;
    NodeId votedFor = kNoNode;

    friend bool operator==(const HardState&, const HardState&) = default;
};

// The membership of a Raft group. During a §6 joint-consensus transition,
// `votersOutgoing` holds the old voter set (Cold) while `voters` holds the new
// one (Cnew); every decision (election, commit) then needs a majority in EACH
// set independently. Outside a transition `votersOutgoing` is empty.
struct Config {
    std::vector<NodeId> voters;
    std::vector<NodeId> votersOutgoing;
    std::vector<NodeId> learners;

    bool joint() const { return !votersOutgoing.empty(); }

    static bool contains(const std::vector<NodeId>& v, NodeId n) {
        return std::find(v.begin(), v.end(), n) != v.end();
    }
    // A voter is anyone in either voting set (both participate during a joint
    // transition).
    bool isVoter(NodeId n) const { return contains(voters, n) || contains(votersOutgoing, n); }
    bool isLearner(NodeId n) const { return contains(learners, n); }

    friend bool operator==(const Config&, const Config&) = default;
};

// A point-in-time snapshot of the state machine covering the compacted log
// prefix (§7). `data` is opaque to Raft -- a VShard snapshot payload in this
// system; the driver reads/writes the actual bytes and the core only tracks the
// (index, term) boundary and relays the payload. `config` is the active
// membership as of the boundary (config entries below it are folded in here).
// index==0 means "no snapshot".
struct Snapshot {
    LogIndex index = kNoIndex;
    Term term = kNoTerm;
    Config config;
    std::string data;
};

}  // namespace timestar::raft
