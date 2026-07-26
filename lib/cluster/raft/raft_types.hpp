#pragma once

#include <algorithm>
#include <cstddef>
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

// A proposal too large to ever be delivered to a follower. Terminal, never retryable:
// re-proposing the same bytes produces the same answer, and the point of throwing is that
// the entry does NOT become durable first (write-scaleout 5 review, F3b).
class ProposalTooLargeError : public std::runtime_error {
public:
    explicit ProposalTooLargeError(const std::string& what) : std::runtime_error(what) {}
};

// ===========================================================================
// THE RAFT MESSAGE-SIZE ARITHMETIC CHAIN (write-scaleout 5 review F3; retuned by D-5)
// ===========================================================================
//
// One chain, four numbers, each a consequence of the one above it:
//
//     largest producer PAYLOAD          28 MiB   kMaxRaftPayloadBytes
//   + envelope/framing headroom        + 4 MiB   kRaftEnvelopeHeadroomBytes
//   ------------------------------------------
//   <= transport SEND refusal           32 MiB   kMaxRaftSendBytes
//   <= peer inbound ADMISSION           64 MiB   kMaxInboundRaftMemory (raft_rpc_transport.cpp)
//
// WHY IT HAS TO BE A CHAIN AND NOT FOUR OPINIONS. The Raft deliver verb is seastar
// `no_wait`: a frame whose estimated cost exceeds the receiver's `max_memory` is
// DROPPED WITH NO REPLY. A sender that can build something the receiver will drop
// produces a silent, permanently-retried black hole, so every link must hold or the
// group loses a replica invisibly. The transport measures the ENCODED ENVELOPE
// (message type, group id, term, leader id, prev/commit indexes, the config's voter
// lists, the framing) while every producer measures the RAW payload it holds, and
// `kRaftEnvelopeHeadroomBytes` is the reserve that closes exactly that gap -- pinned
// by `RaftCodecTest.EnvelopeHeadroomCoversTheFramingOfEveryPayloadProducer`.
//
// WHAT D-5 CHANGED, AND WHY THE NUMBERS COULD COME DOWN. Before chunking, the biggest
// producer was InstallSnapshot: it carried an ENTIRE VShard snapshot in one message,
// so the whole chain had to be sized around "however big a VShard's flushed data
// happens to be" -- 96 MiB send / 128 MiB admission, which is not a bound so much as a
// hope, and `snapshotVShard` had to REFUSE TO COMPACT above it (compaction discards the
// log prefix the snapshot replaces, so an undeliverable snapshot means a follower that
// can never be caught up by anything). InstallSnapshot is now chunked at
// `kMaxSnapshotChunkBytes`, so it is no longer the binding producer and the chain is
// sized for an APPEND instead:
//
//   * one AppendEntries is bounded by RaftOptions::maxAppendBytes (1 MiB of entries)
//     EXCEPT that at least one entry is always sent, so its true bound is the largest
//     single log ENTRY, i.e. `RaftGroup::kMaxProposalBytes`;
//   * one InstallSnapshot is bounded by `kMaxSnapshotChunkBytes` (4 MiB).
//
// So `kMaxProposalBytes` is what the chain is now sized for, and 28 MiB is generous for
// it: a log entry is ONE VShard's slice of ONE write batch, and the data-plane forward
// path already refuses a slice over ~10.67 MiB (`kMaxOutboundFrameBytes`, a 413). Only
// the LOCAL propose path -- coordinator and leader on the same node -- had no
// equivalent cap, and reaching 28 MiB there needs a >28 MiB encoded slice concentrated
// on a single VShard out of 4096, which a legitimate client's batch does not do. The
// adversarial case now gets a clean 413 instead of a 92 MiB Raft entry that three nodes
// must fsync, ship, stage and apply atomically -- an improvement, not a regression.
//
// RESIDUAL (D-31): 28 MiB is still 7x what a chunk needs, and the gap is entirely the
// PROPOSAL bound's. Closing it means splitting an oversized single-VShard slice into
// several proposals, which is a write-path change (the slice stops being one atomic
// entry), so it is filed rather than done here.
//
// These live in the shared types header, not in a transport header, precisely so the
// deterministic core can know them without depending on I/O.
inline constexpr size_t kMaxRaftSendBytes = size_t{32} << 20;
inline constexpr size_t kRaftEnvelopeHeadroomBytes = size_t{4} << 20;
inline constexpr size_t kMaxRaftPayloadBytes = kMaxRaftSendBytes - kRaftEnvelopeHeadroomBytes;

// The largest slice of a snapshot payload one InstallSnapshot may carry (D-5).
//
// WHY 4 MiB. It is the same order as the bounds the other Raft producers already live
// under -- 1 MiB of entries per AppendEntries, 256 KiB per batched transport frame --
// so it needs no separate allowance anywhere in the chain above, and it is large enough
// that the per-message envelope, RPC framing and reply round trip are amortized to
// noise (the envelope overhead measured against a fat 4096-voter config is under
// 100 KiB, i.e. ~2%). Smaller would multiply round trips on a large snapshot for no
// gain; larger would drag the whole chain back up, which is the thing D-5 exists to
// stop. The transfer is paced at ONE unacked chunk per peer (see
// RaftNode::sendInstallSnapshot), so the wire cost of a transfer is one chunk, not the
// snapshot.
inline constexpr size_t kMaxSnapshotChunkBytes = size_t{4} << 20;

// The largest TOTAL snapshot payload this node will build, serve, or stage (D-5/D-6).
//
// Chunking removed the MESSAGE limit on a snapshot; it did not remove the MEMORY one.
// `EngineLocalStore::buildVShardSnapshot` materializes the whole payload (manifest plus
// every referenced TSM file's bytes) in RAM on the producer, `RaftNode` holds it in
// `snapshot_` for as long as it is servable, and the receiver stages the whole thing in
// RAM before installing it. Three copies of an unbounded payload on a reactor with a
// fixed memory pool is an OOM, not a slow transfer.
//
// So the compaction refusal SURVIVES D-5 but changes meaning and RISES: it was
// "> kMaxRaftPayloadBytes, because no single message could carry it" (which after the
// retuning above would have FALLEN to 28 MiB); it is now "> kMaxVShardSnapshotBytes,
// because we will not hold this much of one VShard in memory three times over". 128 MiB
// is ~4.5x the old effective ceiling and 32 chunks, which is a real rise -- big
// snapshots that used to block compaction now ship.
//
// RESIDUAL (D-32): the honest fix is to stream the payload to and from DISK rather than
// staging it in RAM, after which this bound is a disk bound and can be far larger.
inline constexpr size_t kMaxVShardSnapshotBytes = size_t{128} << 20;

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

    static bool contains(const std::vector<NodeId>& v, NodeId n) { return std::find(v.begin(), v.end(), n) != v.end(); }
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
