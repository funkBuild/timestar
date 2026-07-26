#pragma once

#include "../../http/http_query_handler.hpp"  // http::SeriesResult, QueryResponse
#include "../../query/aggregator.hpp"         // PartialAggregationResult
#include "../../query/query_parser.hpp"       // QueryRequest, AggregationMethod

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace timestar::data {

using timestar::PartialAggregationResult;

// The inter-node query request (integration plan F.2). Production cluster queries
// fan out the REAL parsed query (measurement, scopes, fields, groupBy, interval,
// bucketAnchor, booleansAsNumeric -- every field that can change the answer), not
// the toy QuerySpec, so a node's local answer is identical to single-node.
struct NodeQueryRequest {
    timestar::QueryRequest request;
    std::vector<uint16_t> vshards;  // the VShards this node must answer for (completeness accounting)
    uint64_t taskId = 0;            // retry-replaces-contribution key
    uint64_t mapEpoch = 0;          // pinned placement epoch
};

// One node's UNFINALIZED partial answer (integration plan F.5b): its per-core
// numeric partials (merged coordinator-side via mergePartialAggregationsGrouped so
// cross-node group-by / spread are correct) plus non-numeric series (passthrough)
// plus fail-closed accounting. This replaces the earlier finalized-SeriesResult
// form, which could not express a cross-node aggregation partial.
struct NodeQueryPartial {
    std::vector<PartialAggregationResult> partials;
    std::vector<timestar::http::SeriesResult> nonNumeric;
    uint64_t seriesFound = 0;                    // series this node discovered (coordinator sums for maxSeriesCount)
    std::vector<std::string> incompleteReasons;  // non-empty => QUERY_INCOMPLETE
};

// Wire codec (bounds-checked, FNV-checksummed; decode returns nullopt on ANY
// malformed input). Carries the full QueryRequest and the typed FieldValues
// (double/bool/string/int64) so non-numeric results survive the wire.
std::string encodeNodeQueryRequest(const NodeQueryRequest& req);
std::optional<NodeQueryRequest> decodeNodeQueryRequest(const std::string& bytes);
std::string encodeNodeQueryPartial(const NodeQueryPartial& partial);
std::optional<NodeQueryPartial> decodeNodeQueryPartial(const std::string& bytes);

}  // namespace timestar::data
