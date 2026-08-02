#pragma once

#include "../../http/http_query_handler.hpp"  // http::SeriesResult, QueryResponse
#include "../../query/aggregator.hpp"         // PartialAggregationResult
#include "../../query/query_parser.hpp"       // QueryRequest, AggregationMethod

#include <cstdint>
#include <optional>
#include <stdexcept>
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
    // The SUBSET of `vshards` whose LEADERSHIP this node must resolve itself before
    // answering (debt D-13). A coordinator only knows the current leader of a VShard it
    // HOSTS -- its Raft registry has no group for the others, and at RF < N that is most
    // of the map. For those it routes by the placement directory and names them here;
    // this node answers for the ones it actually LEADS and REDIRECTS the rest
    // (NodeQueryPartial::redirects) rather than serving them from a follower's state.
    //
    // Empty in the RF == N case (the coordinator hosts everything, so it resolves
    // everything locally) and from any pre-D-13 coordinator, which is exactly the
    // pre-D-13 behaviour: every named VShard is answered without a leadership check.
    std::vector<uint16_t> resolveVShards;
};

// One VShard this node was asked to resolve but does NOT lead (debt D-13). `leader` is
// what this node's own Raft registry believes -- the coordinator retries there. It is
// kNoNode when this node hosts the group but no leader is elected (a genuine
// leaderless VShard: fail-closed after the coordinator's retry budget) or when it does
// not host the group at all (a placement disagreement, equally fail-closed -- never a
// silent drop).
struct VShardRedirect {
    uint16_t vshard = 0;
    uint64_t leader = 0;  // raft::NodeId; 0 == kNoNode
    bool hosted = false;  // does this node hold a replica of the group at all?
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
    // VShards from NodeQueryRequest::resolveVShards this node does not lead (D-13).
    // They are NOT included in the partials above -- the read filter excludes them --
    // so the coordinator must re-route every one of them or fail the query closed.
    std::vector<VShardRedirect> redirects;
};

// Wire codec (bounds-checked, FNV-checksummed; decode returns nullopt on ANY
// malformed input). Carries the full QueryRequest and the typed FieldValues
// (double/bool/string/int64) so non-numeric results survive the wire.
std::string encodeNodeQueryRequest(const NodeQueryRequest& req);
std::optional<NodeQueryRequest> decodeNodeQueryRequest(const std::string& bytes);
std::string encodeNodeQueryPartial(const NodeQueryPartial& partial);
std::optional<NodeQueryPartial> decodeNodeQueryPartial(const std::string& bytes);

}  // namespace timestar::data
