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
    std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>, std::vector<std::string>>
        values;
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
using VShardBatches = std::vector<std::pair<uint16_t, WriteBatch>>;

// Split in ONE pass. Order of first appearance; each series is moved exactly once.
VShardBatches splitByVShard(WriteBatch batch);

// Merge per-VShard groups back into a single WriteBatch (the wire unit for a
// forwarded propose, and the fallback for ProposeSinks that do their own splitting).
WriteBatch mergeVShardBatches(VShardBatches groups);

// Wire codec (bounds-checked; decode returns nullopt on ANY malformed/truncated/
// inconsistent input so a hostile frame can never fabricate a batch). FNV-checksum
// trailer, same discipline as data_command.
std::string encodeWriteBatch(const WriteBatch& batch);
std::optional<WriteBatch> decodeWriteBatch(const std::string& bytes);

}  // namespace timestar::data
