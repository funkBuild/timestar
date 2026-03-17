#pragma once

#include "http_query_handler.hpp"

#include <string>

namespace timestar {

// High-performance JSON response formatter for query results.
//
// Writes JSON directly from QueryResponse without intermediate structures,
// using glaze's Dragonbox (double) and digit-pair (uint64) formatters for
// maximum number-to-string throughput.
//
// Typical performance vs glaze::write_json on QueryFormattedResponse:
//   - Small queries  (< 1K pts):  7-10x faster  (eliminates struct overhead)
//   - Medium queries (10K pts):   ~5x faster
//   - Large queries  (1M pts):    15-60% faster  (faster number formatting)
class ResponseFormatter {
public:
    // Format a successful query response as JSON.
    // The response is consumed (fields may be moved from).
    static std::string format(QueryResponse& response);

    // Format a JSON error response: {"status":"error","message":"...","error":"..."}
    static std::string formatError(const std::string& message);
};

}  // namespace timestar
