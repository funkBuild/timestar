#pragma once

#include "http_auth.hpp"

#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>

#include <string_view>
#include <utility>

namespace timestar::http {

// Handler that defaults the response to JSON without clobbering an explicit
// Content-Type. seastar::httpd::function_handler(fn, "json") calls
// rep->done("json"), which overwrites _headers["Content-Type"] even when the
// endpoint negotiated another format (protobuf responses were served with an
// application/json header). Here the JSON default applies only when the
// handler didn't set a Content-Type itself.
class JsonDefaultHandler : public seastar::httpd::handler_base {
public:
    explicit JsonDefaultHandler(seastar::httpd::future_handler_function fn) : fn_(std::move(fn)) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(
        const seastar::sstring& /*path*/, std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) override {
        return fn_(std::move(req), std::move(rep)).then([](std::unique_ptr<seastar::http::reply> rep) {
            if (rep->_headers.find("Content-Type") == rep->_headers.end()) {
                rep->set_mime_type("application/json");
            }
            rep->done();
            return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
        });
    }

private:
    seastar::httpd::future_handler_function fn_;
};

// Register a JSON-by-default endpoint with optional Bearer-token auth.
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
    r.add(op, seastar::httpd::url(path), new JsonDefaultHandler(wrapWithAuth(authToken, std::forward<Fn>(fn))));
}

}  // namespace timestar::http
