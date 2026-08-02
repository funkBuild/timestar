#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <filesystem>
#include <memory>
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

// A stable accounting unit for the retained log tail. It deliberately counts the
// entry object as well as its payload; allocator/vector slack is outside this logical
// byte budget and is covered by the live RSS gate in CR-FIX-080.
inline constexpr size_t estimatedLogEntryBytes(size_t payloadBytes) {
    return payloadBytes > std::numeric_limits<size_t>::max() - sizeof(LogEntry) ? std::numeric_limits<size_t>::max()
                                                                                : sizeof(LogEntry) + payloadBytes;
}

// A proposal too large to ever be delivered to a follower. Terminal, never retryable:
// re-proposing the same bytes produces the same answer, and the point of throwing is that
// the entry does NOT become durable first (write-scaleout 5 review, F3b).
class ProposalTooLargeError : public std::runtime_error {
public:
    explicit ProposalTooLargeError(const std::string& what) : std::runtime_error(what) {}
};

// The hosting reactor's aggregate or per-group uncommitted-log allowance is full.
// Unlike LeadershipLostError, this is unambiguous: admission happens before the
// entry is appended, so callers may safely retry after replication catches up.
class ProposalBudgetExceededError : public std::runtime_error {
public:
    explicit ProposalBudgetExceededError(const std::string& what) : std::runtime_error(what) {}
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
// A data-plane frame and its Raft entry use the same v1 WriteBatch bytes. A VShard slice
// is no larger than the containing frame, and the proposal-side check adds only the
// command wrapper. The cross-layer assertion is in cluster_data_plane.hpp.
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

// Compatibility ceiling for snapshots intentionally represented as one in-memory
// string (Group 0 and deterministic unit tests). Production VShard snapshots use
// SnapshotFile and the disk bound below; they never pass through this allowance.
inline constexpr size_t kMaxVShardSnapshotBytes = size_t{128} << 20;

// File-backed snapshots replace the reactor-memory ceiling with an explicit
// disk/admission ceiling. One TiB is intentionally generous for a hot VShard,
// yet finite so a corrupt or hostile peer cannot stream an unbounded object
// into the snapshot staging directory.
inline constexpr uint64_t kMaxVShardSnapshotFileBytes = uint64_t{1} << 40;

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

// ===========================================================================
// THE SHARD-LEVEL UNCOMMITTED LOG BUDGET (CR-FIX-080)
// ===========================================================================
//
// One reactor shard's aggregate locally materialized Raft tail above commit. A
// quorum-less leader may keep accepting proposals while CheckQuorum is disabled;
// client deadlines bound the calls, not the durable entries they appended. The
// hosting shard gives every data group the same budget so 4,096 independent
// per-group limits cannot multiply into process exhaustion. A second, smaller
// per-group cap prevents a single hot partition from consuming the whole shard
// allowance; required election/config entries and recovered tails are accounted
// even when they take the gauges over either admission cap.
class UncommittedProposalBudget {
public:
    UncommittedProposalBudget() = default;
    explicit UncommittedProposalBudget(size_t limit, size_t perGroupLimit = 0)
        : limit_(limit), perGroupLimit_(perGroupLimit) {}

    bool allows(uint16_t groupId, size_t additionalBytes) {
        const size_t groupCurrentBytes = groupCurrent(groupId);
        if ((perGroupLimit_ != 0 &&
             (additionalBytes > perGroupLimit_ || groupCurrentBytes > perGroupLimit_ - additionalBytes)) ||
            (limit_ != 0 && (additionalBytes > limit_ || current_ > limit_ - additionalBytes))) {
            ++refusals_;
            return false;
        }
        return true;
    }

    // Replace one group's prior contribution with its freshly observed tail.
    // Recovery and a required election no-op may legitimately put current above
    // the admission limit; that existing state is counted, while new client
    // proposals remain refused until commit/truncation brings it back down.
    void update(uint16_t groupId, size_t currentBytes) {
        auto previous = groups_.find(groupId);
        const size_t previousBytes = previous == groups_.end() ? 0 : previous->second;
        // Mutate the keyed contribution first. A first insertion can allocate and
        // throw; doing it before the scalar adjustment keeps accounting unchanged
        // if group construction fails.
        if (currentBytes == 0) {
            if (previous != groups_.end())
                groups_.erase(previous);
        } else if (previous == groups_.end()) {
            groups_.emplace(groupId, currentBytes);
        } else {
            previous->second = currentBytes;
        }
        current_ = previousBytes <= current_ ? current_ - previousBytes : 0;
        if (currentBytes > std::numeric_limits<size_t>::max() - current_)
            current_ = std::numeric_limits<size_t>::max();
        else
            current_ += currentBytes;
        peak_ = std::max(peak_, current_);
    }

    size_t groupCurrent(uint16_t groupId) const {
        auto it = groups_.find(groupId);
        return it == groups_.end() ? 0 : it->second;
    }
    size_t limit() const { return limit_; }
    size_t perGroupLimit() const { return perGroupLimit_; }
    size_t current() const { return current_; }
    size_t peak() const { return peak_; }
    uint64_t refusals() const { return refusals_; }

private:
    size_t limit_ = 0;
    size_t perGroupLimit_ = 0;
    size_t current_ = 0;
    size_t peak_ = 0;
    uint64_t refusals_ = 0;
    std::map<uint16_t, size_t> groups_;
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
// A complete snapshot payload may live in a durable sidecar instead of one
// reactor-sized std::string.  The handle is shared because Raft deliberately
// has several logical owners while a received snapshot is persisted, applied,
// and retained for later catch-up.  Sharing the handle must not share a second
// copy of the bytes.
//
// `removeOnDestroy` is true only for an uncommitted staging object or for a
// durable object that a newer, fsync'd snapshot superseded.  Supersession also
// unlinks the old path immediately; this flag is the best-effort retry for a
// failed unlink or an uncommitted producer.  The newest durable object keeps it
// false so ordinary process teardown cannot unlink the only payload referenced
// by the journal.
struct SnapshotFile {
    std::filesystem::path path;
    uint64_t size = 0;
    uint64_t hash = 0;  // FNV-1a over the exact file bytes
    bool removeOnDestroy = false;

    ~SnapshotFile();
};

using SnapshotFilePtr = std::shared_ptr<SnapshotFile>;

struct Snapshot {
    LogIndex index = kNoIndex;
    Term term = kNoTerm;
    Config config;
    // Small snapshots (including Group 0 and deterministic core tests) remain
    // inline. Production VShard snapshots use `file`; exactly one backing must
    // be non-empty.
    std::string data;
    SnapshotFilePtr file;

    [[nodiscard]] uint64_t dataSize() const {
        return file ? file->size : static_cast<uint64_t>(data.size());
    }
    [[nodiscard]] bool fileBacked() const { return static_cast<bool>(file); }
};

}  // namespace timestar::raft
