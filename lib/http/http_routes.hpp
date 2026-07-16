#pragma once

#include "http_auth.hpp"

#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>

#include <string_view>
#include <utility>

namespace timestar::http {

// Register a JSON endpoint with optional Bearer-token auth.
//
// Collapses the registration ceremony repeated across handlers:
//
//   r.add(op, url(path), new function_handler(wrapWithAuth(authToken, fn), "json"));
//
// - `fn` must be callable as (unique_ptr<request>, unique_ptr<reply>)
//   -> future<unique_ptr<reply>> (see timestar::http::HttpHandlerFn).
// - When authToken is empty the handler is registered unwrapped (zero overhead).
// - seastar::httpd::routes takes exclusive ownership of the raw handler
//   pointer and deletes it in its destructor — raw new is the Seastar API.
template <typename Fn>
void addJsonRoute(seastar::httpd::routes& r, seastar::httpd::operation_type op, const char* path,
                  std::string_view authToken, Fn&& fn) {
    r.add(op, seastar::httpd::url(path),
          new seastar::httpd::function_handler(wrapWithAuth(authToken, std::forward<Fn>(fn)), "json"));
}

}  // namespace timestar::http
