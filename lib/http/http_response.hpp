#pragma once

// Tiny helper that collapses the three-line "set status + write body +
// set content-type" pattern that every error response site repeats.
//
// Each handler keeps its own per-shape JSON error formatter (the on-the-wire
// shape varies across endpoints and clients depend on it). This helper only
// handles the boilerplate around the body.

#include "content_negotiation.hpp"

#include <seastar/http/reply.hh>

#include <string>
#include <utility>

namespace timestar::http {

// Write `body` to `rep` with `status` and the correct Content-Type for `fmt`.
inline void respond(seastar::http::reply& rep,
                    seastar::http::reply::status_type status,
                    WireFormat fmt,
                    std::string body) {
    rep.set_status(status);
    rep._content = std::move(body);
    setContentType(rep, fmt);
}

}  // namespace timestar::http
