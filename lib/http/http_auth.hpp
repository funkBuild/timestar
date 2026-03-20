#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/http/httpd.hh>
#include <string>

namespace timestar {

// Handler function type used by seastar::httpd::function_handler.
using HttpHandlerFn = std::function<seastar::future<std::unique_ptr<seastar::http::reply>>(
    std::unique_ptr<seastar::http::request>, std::unique_ptr<seastar::http::reply>)>;

// Constant-time string comparison to prevent timing side-channel attacks.
// Returns true if both strings are equal.
bool constantTimeEquals(std::string_view a, std::string_view b);

// Generate a cryptographically random hex token using /dev/urandom.
// numBytes is the number of random bytes (output will be 2*numBytes hex chars).
std::string generateToken(uint32_t numBytes = 32);

// Check the Authorization header on an incoming request.
// Returns nullptr if the request is authorized (token matches).
// Returns a 401 reply if the request is unauthorized.
std::unique_ptr<seastar::http::reply> checkAuth(const seastar::http::request& req, const std::string& expectedToken);

// Build a 401 Unauthorized reply with JSON body and WWW-Authenticate header.
std::unique_ptr<seastar::http::reply> make401Reply(const std::string& message);

// Mask a token for safe logging: show first 4 and last 4 characters, replace
// the middle with "***".  Tokens shorter than 12 characters are fully masked.
// Example: "abcd1234efgh5678" -> "abcd***5678"
std::string maskToken(std::string_view token);

// Wrap a function_handler lambda with Bearer token authentication.
// When authToken is empty, returns the handler unchanged (zero overhead path).
// The std::function wrapping happens once at route registration, not per request.
template <typename Fn>
HttpHandlerFn wrapWithAuth(std::string_view authToken, Fn&& handler) {
    if (authToken.empty())
        return HttpHandlerFn(std::forward<Fn>(handler));

    return HttpHandlerFn(
        [token = std::string(authToken), fn = std::forward<Fn>(handler)](
            std::unique_ptr<seastar::http::request> req,
            std::unique_ptr<seastar::http::reply> rep) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
            if (auto deny = checkAuth(*req, token))
                co_return std::move(deny);
            co_return co_await fn(std::move(req), std::move(rep));
        });
}

// Wrapper for handler_base subclasses (e.g., SSE stream handlers).
// Takes ownership of the inner handler.
class AuthHandlerWrapper : public seastar::httpd::handler_base {
    std::unique_ptr<seastar::httpd::handler_base> inner_;
    std::string token_;

public:
    AuthHandlerWrapper(std::unique_ptr<seastar::httpd::handler_base> inner, std::string token)
        : inner_(std::move(inner)), token_(std::move(token)) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(const seastar::sstring& path,
                                                                  std::unique_ptr<seastar::http::request> req,
                                                                  std::unique_ptr<seastar::http::reply> rep) override {
        if (auto deny = checkAuth(*req, token_)) {
            deny->done("json");
            co_return std::move(deny);
        }
        co_return co_await inner_->handle(path, std::move(req), std::move(rep));
    }
};

}  // namespace timestar
