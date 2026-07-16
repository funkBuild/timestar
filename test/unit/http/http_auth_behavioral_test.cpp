#include "../../../lib/http/http_auth.hpp"

#include <gtest/gtest.h>

#include <cctype>
#include <memory>
#include <string>
#include <string_view>

// ============================================================================
// Behavioral tests for lib/http/http_auth.hpp beyond the existing
// http_auth_test.cpp / http_auth_wrap_test.cpp coverage:
//   - AuthHandlerWrapper (handler_base wrapper) deny / pass-through paths,
//     which previously had no behavioral test at all
//   - checkAuth scheme edge cases (case sensitivity, whitespace, prefix /
//     superstring tokens)
//   - constantTimeEquals with embedded NUL bytes
//   - generateToken boundary sizes
//
// Types moved into timestar::http — use qualified names throughout.
// All futures involved are ready futures (no reactor needed), matching the
// pattern used by http_auth_wrap_test.cpp.
// ============================================================================

namespace th = timestar::http;

namespace {

// Minimal inner handler recording invocations; replies 200 with a body.
class RecordingHandler : public seastar::httpd::handler_base {
public:
    int calls = 0;
    seastar::sstring lastPath;

    seastar::future<std::unique_ptr<seastar::http::reply>> handle(
        const seastar::sstring& path, std::unique_ptr<seastar::http::request> req,
        std::unique_ptr<seastar::http::reply> rep) override {
        (void)req;
        ++calls;
        lastPath = path;
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = "inner-ok";
        return seastar::make_ready_future<std::unique_ptr<seastar::http::reply>>(std::move(rep));
    }
};

// Builds a wrapper around a RecordingHandler; returns the wrapper plus a raw
// observer pointer to the inner handler (owned by the wrapper).
std::pair<std::unique_ptr<th::AuthHandlerWrapper>, RecordingHandler*> makeWrapped(const std::string& token) {
    auto inner = std::make_unique<RecordingHandler>();
    RecordingHandler* observer = inner.get();
    auto wrapper = std::make_unique<th::AuthHandlerWrapper>(std::move(inner), token);
    return {std::move(wrapper), observer};
}

bool hasHeader(const seastar::http::reply& rep, const std::string& key, const std::string& value) {
    for (const auto& [k, v] : rep._headers) {
        if (k == key && v == value)
            return true;
    }
    return false;
}

}  // namespace

// ==================== AuthHandlerWrapper ====================

class AuthHandlerWrapperTest : public ::testing::Test {};

TEST_F(AuthHandlerWrapperTest, ValidTokenInvokesInnerHandler) {
    auto [wrapper, inner] = makeWrapped("stream-secret-token");

    auto req = std::make_unique<seastar::http::request>();
    req->_headers["Authorization"] = "Bearer stream-secret-token";
    auto rep = std::make_unique<seastar::http::reply>();

    auto result = wrapper->handle("/subscribe", std::move(req), std::move(rep)).get();

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(inner->calls, 1);
    EXPECT_EQ(inner->lastPath, "/subscribe") << "path must be forwarded to the inner handler";
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::ok);
    EXPECT_EQ(result->_content, "inner-ok") << "inner handler's reply must pass through unchanged";
}

TEST_F(AuthHandlerWrapperTest, MissingHeaderReturns401WithoutInner) {
    auto [wrapper, inner] = makeWrapped("stream-secret-token");

    auto req = std::make_unique<seastar::http::request>();  // no Authorization
    auto rep = std::make_unique<seastar::http::reply>();

    auto result = wrapper->handle("/subscribe", std::move(req), std::move(rep)).get();

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(inner->calls, 0) << "inner handler must not run on auth failure";
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_TRUE(hasHeader(*result, "WWW-Authenticate", "Bearer"));
    EXPECT_TRUE(hasHeader(*result, "Content-Type", "application/json"));
    EXPECT_NE(result->_content.find("\"status\":\"error\""), std::string::npos) << result->_content;
}

TEST_F(AuthHandlerWrapperTest, WrongTokenReturns401WithoutInner) {
    auto [wrapper, inner] = makeWrapped("stream-secret-token");

    auto req = std::make_unique<seastar::http::request>();
    req->_headers["Authorization"] = "Bearer wrong-token-entirely";
    auto rep = std::make_unique<seastar::http::reply>();

    auto result = wrapper->handle("/subscribe", std::move(req), std::move(rep)).get();

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(inner->calls, 0);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Invalid bearer token"), std::string::npos) << result->_content;
}

TEST_F(AuthHandlerWrapperTest, MalformedSchemeReturns401WithoutInner) {
    auto [wrapper, inner] = makeWrapped("stream-secret-token");

    auto req = std::make_unique<seastar::http::request>();
    req->_headers["Authorization"] = "Basic c3RyZWFtOnNlY3JldA==";
    auto rep = std::make_unique<seastar::http::reply>();

    auto result = wrapper->handle("/subscribe", std::move(req), std::move(rep)).get();

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(inner->calls, 0);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Missing or malformed"), std::string::npos) << result->_content;
}

TEST_F(AuthHandlerWrapperTest, RepeatedRequestsIndependentlyAuthorized) {
    auto [wrapper, inner] = makeWrapped("tok-abcdef123456");

    // Denied, then allowed, then denied: wrapper state must not leak across calls.
    {
        auto req = std::make_unique<seastar::http::request>();
        auto result = wrapper->handle("/a", std::move(req), std::make_unique<seastar::http::reply>()).get();
        EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    }
    {
        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Authorization"] = "Bearer tok-abcdef123456";
        auto result = wrapper->handle("/b", std::move(req), std::make_unique<seastar::http::reply>()).get();
        EXPECT_EQ(result->_status, seastar::http::reply::status_type::ok);
    }
    {
        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Authorization"] = "Bearer nope";
        auto result = wrapper->handle("/c", std::move(req), std::make_unique<seastar::http::reply>()).get();
        EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    }
    EXPECT_EQ(inner->calls, 1);
    EXPECT_EQ(inner->lastPath, "/b");
}

// ==================== checkAuth edge cases ====================

class CheckAuthEdgeCaseTest : public ::testing::Test {};

TEST_F(CheckAuthEdgeCaseTest, SchemeIsCaseSensitive) {
    seastar::http::request req;
    req._headers["Authorization"] = "bearer secret-token-123";

    auto result = th::checkAuth(req, "secret-token-123");
    ASSERT_NE(result, nullptr) << "lowercase 'bearer' scheme must be rejected";
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Missing or malformed"), std::string::npos);
}

TEST_F(CheckAuthEdgeCaseTest, TokenPrefixAndSuperstringRejected) {
    const std::string expected = "secret123";

    seastar::http::request shorter;
    shorter._headers["Authorization"] = "Bearer secret12";
    EXPECT_NE(th::checkAuth(shorter, expected), nullptr) << "token prefix must not authenticate";

    seastar::http::request longer;
    longer._headers["Authorization"] = "Bearer secret1234";
    EXPECT_NE(th::checkAuth(longer, expected), nullptr) << "token superstring must not authenticate";
}

TEST_F(CheckAuthEdgeCaseTest, WhitespaceVariantsRejected) {
    const std::string expected = "secret-token-123";

    seastar::http::request trailing;
    trailing._headers["Authorization"] = "Bearer secret-token-123 ";
    EXPECT_NE(th::checkAuth(trailing, expected), nullptr) << "trailing space becomes part of the token";

    seastar::http::request doubleSpace;
    doubleSpace._headers["Authorization"] = "Bearer  secret-token-123";
    EXPECT_NE(th::checkAuth(doubleSpace, expected), nullptr) << "double space yields a leading-space token";

    seastar::http::request leading;
    leading._headers["Authorization"] = " Bearer secret-token-123";
    EXPECT_NE(th::checkAuth(leading, expected), nullptr) << "leading space breaks the scheme prefix";
}

TEST_F(CheckAuthEdgeCaseTest, EmptyExpectedTokenNeverAuthorizes) {
    // wrapWithAuth("") disables auth entirely, but checkAuth itself must not
    // treat an empty expected token as a wildcard.
    seastar::http::request withToken;
    withToken._headers["Authorization"] = "Bearer anything";
    EXPECT_NE(th::checkAuth(withToken, ""), nullptr);

    seastar::http::request bearerOnly;
    bearerOnly._headers["Authorization"] = "Bearer ";
    EXPECT_NE(th::checkAuth(bearerOnly, ""), nullptr) << "'Bearer ' with no token is malformed";
}

// ==================== constantTimeEquals / generateToken extras ====================

TEST(ConstantTimeEqualsBinaryTest, EmbeddedNulBytes) {
    const std::string_view aNulB("a\0b", 3);
    const std::string_view aNulB2("a\0b", 3);
    const std::string_view aNulC("a\0c", 3);
    const std::string_view aOnly("a", 1);

    EXPECT_TRUE(th::constantTimeEquals(aNulB, aNulB2));
    EXPECT_FALSE(th::constantTimeEquals(aNulB, aNulC));
    EXPECT_FALSE(th::constantTimeEquals(aNulB, aOnly)) << "NUL-truncated prefix must not compare equal";
}

TEST(GenerateTokenBoundaryTest, ZeroBytesYieldsEmptyToken) {
    EXPECT_EQ(th::generateToken(0), "");
}

TEST(GenerateTokenBoundaryTest, DefaultIs64LowercaseHexChars) {
    auto token = th::generateToken();
    ASSERT_EQ(token.size(), 64u) << "default 32 bytes -> 64 hex chars";
    for (char c : token) {
        EXPECT_TRUE(std::isxdigit(static_cast<unsigned char>(c))) << "non-hex char: " << c;
        EXPECT_FALSE(std::isupper(static_cast<unsigned char>(c))) << "hex must be lowercase";
    }
}

TEST(GenerateTokenBoundaryTest, SingleByteToken) {
    auto token = th::generateToken(1);
    EXPECT_EQ(token.size(), 2u);
}
