#pragma once

#include "http_query_handler.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>
#include <string>

namespace timestar::http {

// High-performance JSON response formatter for query results.
//
// Writes JSON directly from QueryResponse without intermediate structures,
// using glaze's zmij (double) and digit-pair (uint64) formatters for
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
    // Coroutine: large value/timestamp arrays are written in ~64k-element
    // slices with a reactor yield between slices, so a multi-million-point
    // JSON response cannot monopolize the reactor (same policy as the
    // aggregation path and proto::formatQueryResponseYielding).
    static seastar::future<std::string> format(QueryResponse& response);

    // Same output, built directly in the string type http::reply carries so
    // the handler can move it into the reply instead of copying the body.
    static seastar::future<seastar::sstring> formatBody(QueryResponse& response);

    // Same bytes again, written to `out` in segments as they are produced
    // instead of being assembled first: the body is never held (or copied)
    // whole, so a large result costs a couple of ~512KB segments in flight
    // rather than its full size twice over, and a slow reader holds the
    // formatter back instead of growing a buffer. `response` and `out` must
    // outlive the returned future; the caller closes/flushes `out`.
    static seastar::future<> streamBody(QueryResponse& response, seastar::output_stream<char>& out);

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
