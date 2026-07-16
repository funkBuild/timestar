#pragma once

#include "http_query_handler.hpp"

#include <string>

namespace timestar::http {

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

    // Format a JSON error response in the canonical shape (see http_error.hpp):
    //   {"status":"error","error_code":"...","message":"...","error":"..."}
    // Delegates to timestar::http::jsonError().
    static std::string formatError(const std::string& message, const std::string& code = "");
};

}  // namespace timestar::http

// Backward-compatibility alias: ResponseFormatter historically lived directly
// in namespace timestar. New code should use timestar::http:: directly.
namespace timestar {
using http::ResponseFormatter;
}  // namespace timestar
