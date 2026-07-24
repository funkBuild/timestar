#pragma once

#include "../../http/http_query_handler.hpp"  // HttpQueryHandler, QueryResponse
#include "node_store.hpp"
#include "vshard_directory.hpp"

#include <seastar/core/future.hh>

namespace timestar::data {

using timestar::QueryResponse;

// The enriched scatter-gather query coordinator (integration plan F.5b / M2 query
// path). Fans the real parsed QueryRequest out to every VShard owner, gathers each
// node's UNFINALIZED partials (queryLocal for self, queryNode for peers), unions
// them, and finalizes ONCE via HttpQueryHandler::finalizeClusterPartials -- so the
// cluster answer equals the single-node answer for every method, cross-node
// group-by and fold-of-one-non-identity methods (spread/stddev) included.
//
// Contracts (mirroring QueryCoordinator, but over the enriched types):
//   - completeness first: an unfiltered scan requires every VShard assigned, else
//     QUERY_INCOMPLETE (a "could-not-read" is never served as an empty success);
//   - every fan-out future is awaited (never abandoned);
//   - a node reporting incompleteReasons fails the WHOLE query (fail-closed),
//     rather than merging a partial contribution.
class NodeQueryCoordinator {
public:
    NodeQueryCoordinator(const VShardDirectory& dir, NodeStore& local, NodeTransport& client,
                         http::HttpQueryHandler& finalizer)
        : dir_(dir), local_(local), client_(client), finalizer_(finalizer) {}

    seastar::future<QueryResponse> query(QueryRequest request);

private:
    const VShardDirectory& dir_;
    NodeStore& local_;
    NodeTransport& client_;
    http::HttpQueryHandler& finalizer_;
};

}  // namespace timestar::data
