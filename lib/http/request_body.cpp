#include "request_body.hpp"

#include "http_error.hpp"
#include "proto_converters.hpp"

#include <algorithm>
#include <cstring>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <vector>

namespace timestar::http {

// The one place first-party code names Seastar's deprecated request::content.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
seastar::sstring& injectedBody(seastar::http::request& req) noexcept {
    return req.content;
}
const seastar::sstring& injectedBody(const seastar::http::request& req) noexcept {
    return req.content;
}
#pragma GCC diagnostic pop

namespace {

// One network chunk, or an empty buffer at end of body. A read that throws
// (the body ended before its declared length, or the chunked framing is
// malformed) sets `failed` instead: co_await is not allowed inside a handler,
// so the caller cannot catch around its own loop.
seastar::future<seastar::temporary_buffer<char>> readChunk(seastar::input_stream<char>& in, bool& failed) {
    try {
        co_return co_await in.read();
    } catch (...) {
        failed = true;
    }
    co_return seastar::temporary_buffer<char>();
}

}  // namespace

seastar::future<BodyStatus> readRequestBody(seastar::http::request& req, size_t maxBytes, seastar::sstring& body) {
    // No stream (a test or internal caller attached the body), or the server
    // is running buffered and already read it.
    seastar::sstring& buffered = injectedBody(req);
    if (req.content_stream == nullptr || !buffered.empty()) {
        if (buffered.size() > maxBytes) {
            co_return BodyStatus::TooLarge;
        }
        body = std::move(buffered);
        co_return BodyStatus::Ok;
    }

    seastar::input_stream<char>& in = *req.content_stream;
    bool failed = false;

    if (req.content_length > 0) {
        // Declared length: refuse an oversized body before reading any of it,
        // then fill exactly one allocation.
        if (req.content_length > maxBytes) {
            co_return BodyStatus::TooLarge;
        }
        body = seastar::uninitialized_string(req.content_length);
        size_t filled = 0;
        while (filled < body.size()) {
            auto chunk = co_await readChunk(in, failed);
            if (chunk.empty()) {
                break;
            }
            const size_t n = std::min(chunk.size(), body.size() - filled);
            std::memcpy(body.data() + filled, chunk.get(), n);
            filled += n;
        }
        if (failed || filled != body.size()) {
            body = seastar::sstring();
            co_return BodyStatus::Incomplete;
        }
        co_return BodyStatus::Ok;
    }

    // Chunked transfer encoding (Seastar reports content_length 0), or no body
    // at all: the total is unknown up front, so collect the chunks and stop as
    // soon as they pass the limit.
    std::vector<seastar::temporary_buffer<char>> chunks;
    size_t total = 0;
    for (;;) {
        auto chunk = co_await readChunk(in, failed);
        if (chunk.empty()) {
            break;
        }
        total += chunk.size();
        if (total > maxBytes) {
            co_return BodyStatus::TooLarge;
        }
        chunks.push_back(std::move(chunk));
    }
    if (failed) {
        co_return BodyStatus::Incomplete;
    }
    body = seastar::uninitialized_string(total);
    size_t filled = 0;
    for (const auto& chunk : chunks) {
        std::memcpy(body.data() + filled, chunk.get(), chunk.size());
        filled += chunk.size();
    }
    co_return BodyStatus::Ok;
}

void replyIncompleteBody(seastar::http::reply& rep, WireFormat fmt) {
    static constexpr std::string_view kMessage = "Incomplete or malformed request body";
    rep.set_status(seastar::http::reply::status_type::bad_request);
    if (isProtobuf(fmt)) {
        rep._content = timestar::proto::formatErrorResponse(std::string(kMessage));
    } else {
        rep._content = jsonError(kMessage);
    }
    setContentType(rep, fmt);
}

}  // namespace timestar::http
