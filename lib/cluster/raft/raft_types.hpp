#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <set>
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
//     largest producer PAYLOAD          14 MiB   kMaxRaftPayloadBytes
//   + envelope/framing headroom        + 4 MiB   kRaftEnvelopeHeadroomBytes
//   ------------------------------------------
//   <= transport SEND refusal           18 MiB   kMaxRaftSendBytes
//   <= peer inbound ADMISSION           64 MiB   kMaxInboundRaftMemory
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
// WHAT D-31 CHANGED: 28 MiB -> 14 MiB, i.e. the proposal bound is now DERIVED from the
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
// 14 MiB IS THAT FRAME BOUND EXPRESSED IN THE UNIT THE REFUSAL ACTUALLY USES, and getting
// that wrong is what review F1 caught in the first version of this work. The refusal does
// not compare a slice's BYTES against this number, it compares its CHARGE -- what
// `maxEncodedBytes` says the slice could encode to under the worst format version, because
// the journal gate's version is independent of the version the frame arrived in. A charge
// is up to 11/9 of a v1 encoding (see kChargeOverV1Num in data/write_record.hpp: v1's
// cheapest point is a 9-byte boolean and the charge adds 2 per point), so the number this
// chain must clear is not 10.67 MB but:
//
//     chargeCeilingForV1Bytes(kMaxOutboundFrameBytes) + kWriteCommandFramingBytes
//       = 10.67 MB * 11/9 + 4 + 13  ~=  13.67 MB
//
// which 12 MiB did NOT clear. Measured on real batches: a maximal FLOAT frame charges
// 12,582,911 bytes against a 12,582,912-byte bound -- one byte of margin, not the ~1.3 MiB
// this comment used to claim -- and a maximal BOOLEAN frame charges ~13.67 MB and was
// REFUSED, i.e. a forwarded ~1.24M-point boolean write that proposed cleanly at 28 MiB
// drew a terminal 413. 14 MiB clears the boolean case with ~1 MB to spare, and the
// relationship is asserted IN THE CHARGE UNIT in `cluster_data_plane.hpp` (the D-20
// pattern), because an assertion on raw bytes cannot fire on the mismatch it exists to
// catch.
//
// WHAT THIS STILL NARROWS, stated with numbers rather than "the largest legitimate payload
// is untouched" (which was wrong). The bound the assertion guarantees is over a frame whose
// OWN encoding is v1 -- the pessimal one. A v2 frame carries up to 8x more timestamps per
// byte, so a slice that fits the wire bound in v2 can re-encode far above this: a boolean
// slice whose v1 entry is 12.7-28 MB rides a v2 frame of 2.3-5.1 MB and USED to propose at
// the 28 MiB bound. It now gets a 413. That shape is a single VShard of 4096 holding >1.4M
// points from one request -- the adversarial concentration this bound exists to refuse,
// not a batch shape a client produces by spreading writes -- and it is refused LOCALLY and
// terminally, naming the VShard, rather than becoming an opaque remote error. It is a real
// narrowing all the same, and the register row says so.
//
// RESIDUAL (D-31): 14 MiB is still 3.5x a chunk. Closing the rest needs the split.
//
// These live in the shared types header, not in a transport header, precisely so the
// deterministic core can know them without depending on I/O.
//
// The PAYLOAD is now the primary of the three (it is the one with an external
// justification) and the send bound is derived from it; before D-31 it was the other way
// round, which is what let the payload bound float free of anything that produces one.
inline constexpr size_t kMaxRaftPayloadBytes = size_t{14} << 20;
inline constexpr size_t kRaftEnvelopeHeadroomBytes = size_t{4} << 20;
inline constexpr size_t kMaxRaftSendBytes = kMaxRaftPayloadBytes + kRaftEnvelopeHeadroomBytes;

// The last link of the chain: what a peer's Raft RPC server will hold IN FLIGHT, applied
// as `rpc::resource_limits::max_memory` in raft_rpc_transport.cpp. It lives here rather
// than there because it is the number two other bounds are derived from -- the send
// refusal must fit under it (or a frame is dropped with no reply, a silent lost replica),
// and the shard-level snapshot transfer cap is a QUARTER of it (debt D-37).
//
// DELIBERATELY NOT RETUNED when D-31 brought the send bound down: this is a budget for
// CONCURRENCY, not a per-message bound, so it is the one link slack belongs in. It was 2x
// the send bound and is now ~3.5x. One shard hosts ~1365 groups whose heartbeats and appends
// share it; at 4 MiB per snapshot chunk it is also 16 simultaneous chunk transfers before
// frames merely QUEUE on the semaphore (they are not dropped -- only an over-max_memory
// frame is), which is the aggregate D-37 caps explicitly on the send side.
inline constexpr size_t kMaxInboundRaftMemory = size_t{64} << 20;

// THE LAST LINK, ASSERTED (debt D-31, review F3). This is the link whose failure mode is
// the worst in the whole chain -- a frame over the receiver's `max_memory` is dropped with
// NO REPLY on a `no_wait` verb, i.e. a replica lost in silence -- and until the two
// constants lived in the same header it was the one link nothing could state. It is not
// enough that a single message fits: the budget is shared, so a send bound EQUAL to it
// would mean one maximal message monopolises the receiver.
static_assert(kMaxRaftSendBytes < kMaxInboundRaftMemory,
              "a Raft frame this node will SEND must fit -- with room for concurrent ones -- inside what a peer "
              "will ADMIT, or it is dropped with no reply and the replica is lost in silence [debt D-31]");
static_assert(kMaxRaftSendBytes * 3 <= kMaxInboundRaftMemory,
              "the peer's inbound budget is a CONCURRENCY budget: at least a few maximal messages must fit at "
              "once, or heartbeats queue behind one append [debt D-31]");

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
// RAM before installing it. Copies of an unbounded payload on a reactor with a fixed
// memory pool are an OOM, not a slow transfer.
//
// THE MULTIPLE IS NOT THREE (debt D-32, correcting what this comment said). D-5 wrote
// "three copies" from the three PLACES a payload lives; counting what is CONCURRENT on
// each path found seven on the producer and eight on the receiver, because the journal
// record encode chain re-materialized it several times over. D-32 removed four of them
// (the producer's dead `std::move` into the payload encoder, the journal record's `body`
// scratch, the journal writer's `encode()` temporary, and the receiver's copy into
// `applySnapshot`); the remainder, all still concurrent during the journal append:
//
//   producer  ~4x: snapshot_.data | persistSnapshot's by-value Snapshot |
//                  JournalRecord::payload | JournalWriter::tail_
//   receiver  ~6x: the four above, plus pendingSnapshotApply_ and Ready::snapshot,
//                  which are two live things by contract (servable state vs undrained
//                  Ready output) and can only be collapsed by sharing the buffer
//
// So the bound STAYS AT 128 MiB rather than rising: the multiple it was set against was
// understated, and 4-6x of 128 MiB is already 0.5-0.75 GiB on one reactor. Raising it
// waits on the structural fix D-32 names -- streaming to and from DISK, after which this
// is a disk bound -- or on making `Snapshot::data` a shared buffer, which would collapse
// the receiver's two contract-mandated copies into refcounts. NOTE that the multiples
// above are a CENSUS OF THE CODE, not a measurement: no RSS number has ever been taken on
// this path.
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

// ===========================================================================
// THE SHARD-LEVEL SNAPSHOT TRANSFER BUDGET (debt D-37)
// ===========================================================================
//
// Snapshot flow control is per peer PER GROUP -- one unacked chunk, held in that group's
// `RaftNode::snapTransfers_`. Every link of that holds; what nothing bounded is the
// AGGREGATE. A shard hosts ~1365 groups, so a node returning after an outage long enough
// for its peers to have compacted could be the target of that many SIMULTANEOUS transfers,
// each willing to put a 4 MiB chunk on the wire. The peer's inbound admission semaphore
// QUEUES rather than drops, so the failure is not corruption -- it is every transfer
// crawling behind every other, which is what the restart-catchup gate already shows in
// miniature (10-15 stall-driven resends among 18-19 chunks on a THREE-node cluster).
//
// This is that aggregate, made explicit and enforced on the SEND side, where there is
// still something useful to do about it.
//
// IT IS A PER-SENDER CAP AND NOT A RECEIVE-SIDE GUARANTEE (review F2). The budget bounds
// what THIS shard ships; the peer's inbound admission is spent by every leader shipping to
// it, so N-1 senders compose -- at RF=3 two catching-up leaders can claim half of a peer's
// Raft inbound budget, at N=5 four can claim all of it. Capping what we send is the only
// thing a sender can do; bounding what a node RECEIVES would need receiver-side admission
// by message class (snapshot chunks metered separately from appends), which is not built.
//
// WHY IT LIVES IN THE DETERMINISTIC CORE, AS A PLAIN COUNTER. `RaftNode` is reactor-free
// by contract, so a semaphore is not available to it -- and is not wanted: a transfer that
// must WAIT on a future would suspend inside `tick()`, which is a synchronous state
// transition. A group that cannot start now simply does not start now, and the tick that
// comes round 20 ms later asks again. The budget is therefore a POD the host owns per
// shard and hands to every group by pointer through RaftOptions.
//
// WHY PER SHARD RATHER THAN PER NODE. Everything on this path is already per shard: the
// Raft transport, its inbound admission budget, the group registry and the tick loop. A
// node-level counter would need cross-shard atomics on a path that runs every 20 ms for
// every group, to bound something whose consumer -- the peer's per-shard inbound memory --
// is itself per shard. Per shard is the unit the number is actually derived from.
//
// FAIRNESS IS FIFO, AND IT IS NOT OPTIONAL. Groups are ticked in map order, so "whoever
// asks when a slot frees" means the lowest-numbered group wins every time and a
// high-numbered group with a real snapshot to ship can wait indefinitely. A waiter takes a
// TICKET and only the OLDEST outstanding ticket may take a free slot.
//
// EVERY WAITER MUST EVENTUALLY CANCEL OR ACQUIRE. The queue is head-of-line by
// construction, so a ticket whose owner disappears without cancelling stalls the shard's
// transfers. RaftNode routes every path that drops a transfer record -- role change,
// re-compaction, abandonment, completion, the peer catching up another way -- through one
// helper for exactly this reason. A group DESTROYED while holding one would leak it; no
// such path exists today (nothing removes a VShard -- see debt D-40), and whatever wires
// VShard teardown must clear the group's transfers before dropping the node.
class SnapshotTransferBudget {
public:
    SnapshotTransferBudget() = default;
    explicit SnapshotTransferBudget(size_t cap) : cap_(cap) {}

    // 0 == unlimited (the default for a core with no host, i.e. every unit test that does
    // not opt in -- so the budget can never change behaviour it was not given).
    size_t cap() const { return cap_; }
    size_t active() const { return active_; }
    size_t waiting() const { return waiting_.size(); }

    // Take a place in the queue. Returned tickets increase, so the smallest outstanding
    // one is the oldest.
    uint64_t enqueue() {
        const uint64_t t = ++nextTicket_;
        waiting_.insert(t);
        return t;
    }

    // Admit `ticket` if there is a free slot AND it is at the head of the queue.
    bool tryAcquire(uint64_t ticket) {
        auto it = waiting_.find(ticket);
        if (it == waiting_.end())
            return false;  // not a live waiter: cancelled, or already admitted
        if (cap_ != 0 && active_ >= cap_)
            return false;
        if (*waiting_.begin() != ticket)
            return false;  // an older waiter is still queued -- FIFO
        waiting_.erase(it);
        ++active_;
        return true;
    }

    // Give up a place in the queue without having been admitted.
    void cancel(uint64_t ticket) { waiting_.erase(ticket); }

    // Give back an admitted slot.
    void release() {
        if (active_ > 0)
            --active_;
    }

private:
    size_t cap_ = 0;
    size_t active_ = 0;
    uint64_t nextTicket_ = 0;
    std::set<uint64_t> waiting_;
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
