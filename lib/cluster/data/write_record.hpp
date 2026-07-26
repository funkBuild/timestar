#pragma once

#include "../../core/placement_table.hpp"  // virtualShard, SeriesId128
#include "../../storage/tsm.hpp"           // TSMValueType

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace timestar::data {

using ::TSMValueType;  // TSMValueType lives in the global namespace (like SeriesId128)

// The LOSSLESS inter-node write unit (integration plan F.1). Unlike the flat
// DataPoint{SeriesId128, ts, double} -- which cannot carry a real Engine write
// (SeriesId128 is a one-way hash, string fields are unrepresentable, int64 loses
// precision) -- WriteSeries carries the CANONICAL series-key string (whose hash IS
// the series identity, so the receiver's SeriesId128 / virtualShard / core routing
// are guaranteed identical to the sender's) plus a single typed value column. It is
// deliberately isomorphic to TimeStarInsert<T>: the receiver rebuilds it via
// TimeStarInsert<T>::fromSeriesKey(seriesKey) and drives Engine::insertBatch<T>.
struct WriteSeries {
    std::string seriesKey;  // "measurement,tag=val,... field" -- hash(seriesKey) == SeriesId128
    TSMValueType type = TSMValueType::Float;
    std::vector<uint64_t> timestamps;
    // Exactly one alternative is active, selected by `type` (Float->double,
    // Integer->int64, Boolean->bool, String->string). Its size == timestamps.size().
    std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>, std::vector<std::string>> values;
    // Per-point replicated revision (ADR 0003), parallel to values. Empty == not
    // yet assigned (RF=1 Engine stamps at insert; RF=3 state machine fills these
    // from the log so the Engine must not re-stamp).
    std::vector<uint64_t> revisions;

    // CACHED ROUTING HINT (write-scaleout 2a), not data: the VShard this series' key
    // hashes to. A write is grouped by VShard at least three times on its way down
    // (owning shard -> leader node -> Raft group), and each grouping used to re-hash
    // the seriesKey STRING. It is computed once at ingress and carried instead.
    //
    // It is deliberately NOT part of the wire format, and the codec neither writes nor
    // reads it: a series decoded from the network or from the Raft journal always
    // arrives UNROUTED and has its VShard derived from the canonical seriesKey by the
    // first vshardOf() that needs it. That is where the authority lives -- the VShard a
    // series lands in is always a local function of its key, so a corrupt or hostile
    // peer cannot plant a series in a VShard its key does not own, because nothing it
    // sends is ever read as a VShard. (Deriving lazily rather than at decode also keeps
    // the hash off the apply path, which routes by the full SeriesId128 and never wants
    // the VShard.) Anything outside [0, VIRTUAL_SHARD_MASK] means "not computed yet".
    static constexpr uint16_t kUnroutedVShard = 0xFFFF;
    uint16_t vshard = kUnroutedVShard;

    // Whether the active alternative and its size are consistent with `type` and
    // `timestamps`. A decoded record is validated with this before use.
    bool consistent() const;
};

struct WriteBatch {
    std::vector<WriteSeries> series;
    uint64_t schemaVersion = 0;
};

// THE routing authority for a series: its cached VShard, computed from the canonical
// series key on first use. Every layer that groups by VShard calls this rather than
// hashing the key itself, so a batch is hashed once no matter how many times it is
// regrouped -- and a WriteSeries built by a path that does not pre-compute the hint
// still routes correctly (it is derived here, never trusted from a caller).
inline uint16_t vshardOf(WriteSeries& s) {
    if (s.vshard > timestar::VIRTUAL_SHARD_MASK)  // unset, or garbage from anywhere
        s.vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(s.seriesKey));
    return s.vshard;
}

// A batch already SPLIT by VShard: one WriteBatch per VShard, each carrying the
// parent's schemaVersion. This is the shape the replicated write path moves in --
// the owning-shard split, the leader-node split and the per-group propose are three
// different groupings OF THE SAME per-VShard groups, so the series themselves are
// moved once (write-scaleout 2b) instead of being rebuilt into a fresh WriteBatch at
// every layer.
using VShardBatchGroup = std::pair<uint16_t, WriteBatch>;
using VShardBatches = std::vector<VShardBatchGroup>;

// A BORROWED selection of groups (write-scaleout 3b). The bounded write retry has to
// re-dispatch the slices that failed while leaving the ones that committed alone, which
// means the coordinator must still OWN every group after a dispatch -- so the propose
// APIs below the router take a view instead of consuming a VShardBatches. That keeps the
// hot path copy-free (a retry costs one vector of pointers, not a second copy of the
// batch) and it is what lets `encodeWriteBatch(view, ...)` replace the mergeVShardBatches
// allocation the remote path used to pay on EVERY write.
//
// LIFETIME: the pointees are owned by the caller's frame and every propose future is
// AWAITED there before it returns, so a view never outlives its groups. Do not stash one
// in a lambda that escapes the awaiting frame (the standing coroutine-frame rule) and do
// not send one across a shard boundary.
using VShardBatchView = std::vector<const VShardBatchGroup*>;

// Split in ONE pass. Order of first appearance; each series is moved exactly once.
VShardBatches splitByVShard(WriteBatch batch);

// Borrow every group of `groups` (in order). Convenience for the common
// "dispatch the whole batch" first attempt.
VShardBatchView viewOf(const VShardBatches& groups);

// Roughly how much MEMORY this batch holds, for the in-flight admission bound
// (write-scaleout 3d). Deliberately an estimate: it counts the payload columns and key
// bytes, not per-object overhead, and it is used to bound a budget, not to allocate.
size_t approxResidentBytes(const WriteBatch& batch);
size_t approxResidentBytes(const VShardBatchView& view);

// Merge per-VShard groups back into a single WriteBatch (the wire unit for a
// forwarded propose, and the fallback for ProposeSinks that do their own splitting).
WriteBatch mergeVShardBatches(VShardBatches groups);

// Wire/journal format versions this codec can EMIT (write-scaleout 2c).
//
// v1 is the original fixed-width layout. v2 keeps it but delta-varints a series'
// timestamps, which are monotone per series on the canonical path -- ~6 bytes off
// every point.
//
// FORMAT SAFETY: decodeWriteBatch reads BOTH, always, and that is not optional.
// The Raft journal stores encoded commands, so a cluster restarting on a newer
// binary replays entries an older one wrote; and a Raft entry is replicated to
// replicas that never take part in the pairwise data-plane handshake. Emission is
// therefore version-GATED per destination:
//   - the data-plane wire (forwardWriteBatch / proposeWrite) emits the version
//     negotiated with that specific peer via kNegotiateVersion, so a mixed-version
//     cluster degrades to v1 and an INCOMPATIBLE peer fails closed rather than
//     misparsing;
//   - the Raft command path (and hence the journal) still emits v1
//     unconditionally. Raising it needs a CLUSTER-wide gate -- every voter able to
//     read v2 -- which is what group-0's committed format activation
//     (activeFormatVersion / features::FeatureGate) exists for; wiring that to this
//     codec is the remaining step, and until then no v2 byte can reach a journal.
constexpr uint32_t kWriteBatchFormatV1 = 1;
constexpr uint32_t kWriteBatchFormatV2 = 2;

// v3 is a PROTOCOL version, not a payload format (write-scaleout 3a): a peer that
// negotiates >= 3 answers the hinted-propose verb, whose reply carries the ACTUAL
// leader of each rejected VShard instead of a bare "0". The batch bytes are unchanged
// -- encodeWriteBatch(batch, 3) still emits v2 -- so nothing about the journal or the
// on-wire payload moves with it. It rides the SAME negotiated range because that is
// already the cluster's one mechanism for "what may I say to this peer", and a peer
// that does not know v3 simply keeps getting the v1-shaped verb-6 reply.
constexpr uint32_t kWriteBatchFormatV3 = 3;

// The newest version this binary supports. Every place that advertises this node's
// capability must use THIS, never the literal that happens to be current: naming v2 in
// ClusterDataPlane after v3 landed capped every negotiation at 2, so no peer spoke the
// hinted-propose verb and the leader-hint path was dead in production while every test
// passed (tests construct their own DataPlaneRpc, whose default was already correct).
constexpr uint32_t kWriteBatchFormatMax = kWriteBatchFormatV3;

// Wire codec (bounds-checked; decode returns nullopt on ANY malformed/truncated/
// inconsistent input so a hostile frame can never fabricate a batch). Bounds-checking
// a declared count against the bytes remaining stops an over-READ but NOT an
// over-ALLOCATION -- an element can be far larger resident than on the wire -- so
// element reserves are additionally capped and grown as bytes are really consumed
// (kMaxPrereserveElems in the .cpp). Note that an inbound RPC frame itself is
// currently unbounded: neither DataPlaneRpc nor RaftRpcTransport sets
// seastar::rpc::resource_limits. FNV-checksum
// trailer, same discipline as data_command. The no-version overload emits v1; the
// versioned one emits the highest format it knows that is <= `version` (so a caller
// can pass a negotiated version straight through).
std::string encodeWriteBatch(const WriteBatch& batch);
std::string encodeWriteBatch(const WriteBatch& batch, uint32_t version);
// Encode the CONCATENATION of a borrowed selection of groups, byte-for-byte identical
// to encoding mergeVShardBatches(copy-of-view) -- the receiver re-derives each series'
// VShard from its key, so the grouping is not on the wire and merging was only ever an
// allocation. Used by the remote propose path, which must not consume its groups (they
// may need re-dispatching to a different leader).
std::string encodeWriteBatch(const VShardBatchView& view, uint32_t version);
std::optional<WriteBatch> decodeWriteBatch(const std::string& bytes);

}  // namespace timestar::data
