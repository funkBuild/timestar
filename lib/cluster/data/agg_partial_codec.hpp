#pragma once

#include "../../query/aggregator.hpp"  // PartialAggregationResult, AggregationState

#include <optional>
#include <string>
#include <vector>

namespace timestar::data {

using timestar::AggregationState;
using timestar::PartialAggregationResult;

// Wire codec for the UNFINALIZED cross-core query partial (integration plan F.5b).
// A node produces its own vector<PartialAggregationResult> (createPartialAggregations
// over its owned VShards, BEFORE any merge); the coordinator concatenates every
// node's partials and runs the existing mergePartialAggregationsGrouped + finalize
// exactly ONCE -- treating remote nodes as extra shards -- so the cluster answer is
// provably the single-node answer for every method, including fold-of-one-non-
// identity methods (spread/stddev) and cross-node group-by.
//
// Full-fidelity: every field the merge reads is serialized (measurement, fieldName,
// groupKey, groupKeyHash, cachedTags, totalPoints, bucketStates, sorted*, collapsed),
// so nothing is recomputed on the receiver and no hash/tag divergence is possible.
// Bounds-checked + FNV-trailer-verified like the other cluster codecs; a malformed
// or truncated frame decodes to nullopt (never a partial structure).
std::string encodePartials(const std::vector<PartialAggregationResult>& partials);
std::optional<std::vector<PartialAggregationResult>> decodePartials(const std::string& bytes);

}  // namespace timestar::data
