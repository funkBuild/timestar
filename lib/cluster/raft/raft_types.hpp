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
// THE RAFT MESSAGE-SIZE ARITHMETIC CHAIN (write-scaleout 5 review F3; retuned by D-5,
// tightened by D-31)
// ===========================================================================
//
// One chain, four numbers, each a consequence of the one above it:
//
//     largest producer PAYLOAD          12 MiB   kMaxRaftPayloadBytes
//   + envelope/framing headroom        + 4 MiB   kRaftEnvelopeHeadroomBytes
//   ------------------------------------------
//   <= transport SEND refusal           16 MiB   kMaxRaftSendBytes
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
// So `kMaxProposalBytes` is what the chain is sized for: a log entry is ONE VShard's
// slice of ONE write batch.
//
// WHAT D-31 CHANGED: 28 MiB -> 12 MiB, i.e. the proposal bound is now DERIVED from the
// largest slice the data plane will actually carry instead of being a round number
// chosen above it. D-5 left the number at 28 MiB and filed the rest, on the reading that
// closing it required SPLITTING an oversized slice across several proposals (a write-path
// change: the slice stops being one atomic entry). That is still the only way to reach
// "one chunk plus headroom", and it is still not done -- but it was never needed to get
// most of the way there, because a bound already exists a layer down:
//
//   * a slice that arrives from a PEER rode `kMaxOutboundFrameBytes` (~10.67 MiB, the
//     peer's data-plane inbound admission divided by its bloat factor), and a single
//     slice is a subset of that frame, so it can never exceed it;
//   * a slice proposed LOCALLY (coordinator and leader on the same node) has no frame to
//     ride, which is why the propose-side refusal exists at all;
//   * and the two are now measured in the SAME unit -- `firstUnproposableSlice`
//     (data/replicated_command.hpp) refuses, client-side and terminally, any slice whose
//     encoded COMMAND could exceed this bound, so the remote path fails as a local 413
//     naming the VShard instead of an opaque remote error retried against every leader.
//
// 12 MiB is that ~10.67 MiB plus ~1.3 MiB of margin, and the relationship is asserted in
// `cluster_data_plane.hpp` (the D-20 pattern) because the two constants live in headers
// that do not include each other. The margin is not a guess: the command adds 13 bytes of
// framing to the batch, and a slice re-encoded at a DIFFERENT format version than the
// frame it arrived in can grow (v2's zigzag timestamp deltas are 1-10 bytes where v1's
// are a flat 8, and the journal gate's version is independent of the peer's) -- see
// `maxEncodedBytes` in data/write_record.hpp, which is what the refusal measures.
//
// A batch that legitimately reaches even the OLD bound does not exist: a whole 10k-point
// HTTP batch encodes ~1-2 MB and spreads over many of 4096 VShards, so 12 MiB on ONE
// VShard is already ~10x the largest legitimate batch entire. What the drop really costs
// is the adversarial shape -- a 64 MiB body whose points all hash to one VShard -- which
// used to succeed with a 12-28 MiB Raft entry IF the coordinator happened to be that
// VShard's leader, and got a 413 otherwise. That placement-dependent success is gone: it
// is a 413 on both paths now.
//
// RESIDUAL (D-31): 12 MiB is still 3x a chunk. Closing the rest needs the split.
//
// These live in the shared types header, not in a transport header, precisely so the
// deterministic core can know them without depending on I/O.
//
// The PAYLOAD is now the primary of the three (it is the one with an external
// justification) and the send bound is derived from it; before D-31 it was the other way
// round, which is what let the payload bound float free of anything that produces one.
inline constexpr size_t kMaxRaftPayloadBytes = size_t{12} << 20;
inline constexpr size_t kRaftEnvelopeHeadroomBytes = size_t{4} << 20;
inline constexpr size_t kMaxRaftSendBytes = kMaxRaftPayloadBytes + kRaftEnvelopeHeadroomBytes;

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
