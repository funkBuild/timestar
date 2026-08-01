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
//   - the Raft command path (and hence the journal) emits the version the
//     CLUSTER-WIDE journal gate allows (debt D-7, `data/journal_format.hpp`). It
//     cannot use per-peer negotiation: a log entry goes to voters that never did the
//     pairwise handshake, and the journal outlives the process that wrote it. The gate
//     is group-0's COMMITTED format activation (`activeFormatVersion`, proposed only
//     after `features::FeatureGate::canActivate` confirms every voter supports the
//     version), it defaults to v1 and only ever RISES -- so a node that has heard no
//     activation emits v1, which every binary can read. journal_format.hpp carries the
//     ordering argument for why "an old binary reads a v2 journal" is unreachable.
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

// v4 is a PROTOCOL version like v3, and it belongs to the READ path (debt D-25): a peer
// that negotiates >= 4 understands `NodeQueryRequest::resolveVShards` and answers with
// `NodeQueryPartial::redirects` -- D-13's optional tails on the node-query frames. Nothing
// about the write payload moves with it: `encodeWriteBatch(batch, 4)` still emits v2, and
// the hinted-propose gate is still `>= 3`.
//
// It rides the SAME negotiated range for the reason v3 does: that range is already the
// cluster's one mechanism for "what may I say to this peer", and the read path had no
// mechanism at all. The range is a single scalar, so a read-path step consumes a number the
// write path could have used -- deliberately, because ONE ordered protocol line is what
// makes a peer's answer to `kNegotiateVersion` a complete statement of what it speaks. The
// read path names it through `kNodeQueryResolveMinVersion` (node_query.hpp), so no read
// site reads a `kWriteBatchFormat*` spelling.
constexpr uint32_t kWriteBatchFormatV4 = 4;

// v5 gates the bounded-delete command/reply contract. The WriteBatch bytes are
// still v2; this number says a peer understands command tag 5 and the typed
// `Expired` proposal outcome. Snapshot payload v4 uses the same cluster-wide
// activation through JournalFormatGate because snapshots and Raft entries reach
// voters outside the pairwise data-plane handshake.
constexpr uint32_t kWriteBatchFormatV5 = 5;

// The newest version this binary supports. Every place that advertises this node's
// capability must use THIS, never the literal that happens to be current: naming v2 in
// ClusterDataPlane after v3 landed capped every negotiation at 2, so no peer spoke the
// hinted-propose verb and the leader-hint path was dead in production while every test
// passed (tests construct their own DataPlaneRpc, whose default was already correct).
constexpr uint32_t kWriteBatchFormatMax = kWriteBatchFormatV5;

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

// An upper bound on `encodeWriteBatch(batch, v).size()` for EVERY version v this codec
// can emit, computed without encoding anything (debt D-31).
//
// It exists because the two ends of a forwarded write do not agree on a version and do
// not have to: the data-plane wire emits what was NEGOTIATED with that peer, while the
// slice the receiver re-encodes as a Raft command emits what the CLUSTER-WIDE journal
// gate allows (see the FORMAT SAFETY note above). So a size measured at one end says
// nothing about the other end unless it is version-independent, and the direction that
// bites is v2 -> v1: v2's zigzag timestamp deltas are 1-10 bytes where v1's are a flat 8,
// so the SAME batch is usually smaller in v2 and can be larger. Anything that refuses an
// oversized slice must therefore charge the worst version, not the one in its hand.
//
// The bound is v1's exact size plus the two things only v2 can add: its 4-byte magic and
// up to 2 bytes per point (a 10-byte varint where v1 pays 8; the first timestamp of a
// series is a fixed u64 in both, so this is slack, not a shortfall). It is an upper
// bound, never an estimate -- a refusal computed from it must not be able to admit
// something the encoder will then exceed.
size_t maxEncodedBytes(const WriteBatch& batch);

// The v2 format magic, as a size (the bytes themselves are private to the .cpp, which
// static_asserts they agree). Part of the charge arithmetic below.
inline constexpr size_t kWriteBatchV2MagicBytes = 4;

// HOW MUCH THE CHARGE CAN EXCEED A v1 ENCODING OF THE SAME BATCH -- 11/9, and this ratio
// is the whole reason a size measured on the wire cannot be compared to the proposal bound
// as a raw byte count (debt D-31, review F1).
//
// `maxEncodedBytes` charges `v1 + magic + 2 bytes per point`, and v1's CHEAPEST point is a
// boolean: an 8-byte timestamp plus a 1-byte value, 9 bytes. So the charge is at most
// (9+2)/9 = 11/9 of v1 for a boolean column, 18/16 for float/int, 14/12 for the shortest
// strings -- booleans bind. Every other term of v1 (per-series key and count headers, the
// per-batch header and trailer, revisions when present) only makes the ratio smaller, so
// 11/9 is a true ceiling rather than a typical case.
//
// COMPARING RAW BYTES INSTEAD OF THIS IS THE BUG REVIEW F1 FOUND: `kMaxOutboundFrameBytes`
// (~10.67 MB) looks like it leaves ~1.3 MiB of headroom under a 12 MiB proposal bound, and
// leaves NONE -- a maximal float frame charges 12,582,911 bytes against a 12,582,912-byte
// bound, and a maximal boolean frame charges 13.67 MB and is REFUSED. Anything relating the
// two bounds must go through `chargeCeilingForV1Bytes`.
inline constexpr size_t kChargeOverV1Num = 11;
inline constexpr size_t kChargeOverV1Den = 9;

// The most `maxEncodedBytes` can charge for a batch whose v1 encoding is `v1Bytes`.
constexpr size_t chargeCeilingForV1Bytes(size_t v1Bytes) {
    return v1Bytes * kChargeOverV1Num / kChargeOverV1Den + kWriteBatchV2MagicBytes;
}

}  // namespace timestar::data
