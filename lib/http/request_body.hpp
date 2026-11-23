#pragma once

#include "content_negotiation.hpp"

#include <cstddef>
#include <cstdint>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

// Request bodies.
//
// The HTTP server runs in Seastar's content-streaming mode: a handler is
// invoked as soon as the headers are parsed and reads the body itself from
// `request::content_stream`. Compared with Seastar's buffered mode (which
// reads the whole body into `request::content` before the handler runs, and is
// deprecated upstream) this
//   - enforces the endpoint's own size limit BEFORE buffering anything, where
//     the buffered mode held any body up to the server-wide limit in memory
//     just to have the handler reject it; and
//   - holds the body once. Buffered mode collects the network chunks and then
//     copies them into a contiguous string, so both copies are live at the
//     peak (2x the body per in-flight request); here chunks are copied
//     straight into a single allocation sized from Content-Length.

namespace timestar::http {

enum class BodyStatus : uint8_t {
    Ok,
    // The body exceeds the limit. Nothing beyond the limit was buffered; the
    // unread remainder makes Seastar close the connection after the reply.
    TooLarge,
    // The body ended early or its framing was malformed (the peer went away
    // mid-request, or sent bad chunked encoding). The handler must not act on
    // the partial bytes: a truncated protobuf batch can still parse.
    Incomplete,
};

// Read the whole request body into `body` (NUL-terminated storage, as the
// JSON parser requires), refusing anything larger than `maxBytes`.
//
// Works for both ways a handler can be reached: from the HTTP server (reads
// `content_stream`) and directly from tests or internal callers that attached
// a body with injectedBody() (no stream). `req` and `body` must outlive the
// returned future.
seastar::future<BodyStatus> readRequestBody(seastar::http::request& req, size_t maxBytes, seastar::sstring& body);

// Fill `rep` with the 400 that answers BodyStatus::Incomplete.
void replyIncompleteBody(seastar::http::reply& rep, WireFormat fmt);

// The in-memory body of a request that did not come through the HTTP server.
// Tests and internal callers assign to it instead of building an input
// stream; readRequestBody() consumes it. It is stored in Seastar's deprecated
// `request::content` member, and these accessors are the only first-party
// code that touches that member — when Seastar removes it, only they change.
seastar::sstring& injectedBody(seastar::http::request& req) noexcept;
const seastar::sstring& injectedBody(const seastar::http::request& req) noexcept;

}  // namespace timestar::http
