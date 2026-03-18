#include "../../../lib/http/http_auth.hpp"
#include "../../../lib/config/timestar_config.hpp"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <set>
#include <string>

using namespace timestar;

// Glaze structure for parsing error responses
struct AuthErrorParsed {
    std::string status;
    std::string message;
};

template <>
struct glz::meta<AuthErrorParsed> {
    using T = AuthErrorParsed;
    static constexpr auto value = object("status", &T::status, "message", &T::message);
};

// --- constantTimeEquals tests ---

class ConstantTimeEqualsTest : public ::testing::Test {};

TEST_F(ConstantTimeEqualsTest, MatchingStrings) {
    EXPECT_TRUE(constantTimeEquals("abc123", "abc123"));
    EXPECT_TRUE(constantTimeEquals("hello world!", "hello world!"));
}

TEST_F(ConstantTimeEqualsTest, DifferentStrings) {
    EXPECT_FALSE(constantTimeEquals("abc123", "abc124"));
    EXPECT_FALSE(constantTimeEquals("abc123", "xyz999"));
}

TEST_F(ConstantTimeEqualsTest, DifferentLengths) {
    EXPECT_FALSE(constantTimeEquals("short", "longer string"));
    EXPECT_FALSE(constantTimeEquals("longer string", "short"));
    EXPECT_FALSE(constantTimeEquals("abc", "ab"));
}

TEST_F(ConstantTimeEqualsTest, EmptyStrings) {
    EXPECT_TRUE(constantTimeEquals("", ""));
    EXPECT_FALSE(constantTimeEquals("", "notempty"));
    EXPECT_FALSE(constantTimeEquals("notempty", ""));
}

// Regression: empty `a` with non-empty `b` previously caused UB (out-of-bounds
// read on a[0] via the modulo-clamp trick).  These cases must not crash and must
// return the correct result.
TEST_F(ConstantTimeEqualsTest, EmptyVsNonEmptyNoUB) {
    // a empty, b single char
    EXPECT_FALSE(constantTimeEquals("", "x"));
    // a empty, b long string
    EXPECT_FALSE(constantTimeEquals("", "a]long-string-that-should-not-be-read"));
    // b empty, a non-empty (symmetric check)
    EXPECT_FALSE(constantTimeEquals("y", ""));
}

TEST_F(ConstantTimeEqualsTest, SingleCharStrings) {
    EXPECT_TRUE(constantTimeEquals("a", "a"));
    EXPECT_FALSE(constantTimeEquals("a", "b"));
    EXPECT_FALSE(constantTimeEquals("a", "aa"));
}

// --- generateToken tests ---

class GenerateTokenTest : public ::testing::Test {};

TEST_F(GenerateTokenTest, CorrectLength) {
    auto token = generateToken(32);
    EXPECT_EQ(token.size(), 64u);  // 32 bytes * 2 hex chars

    auto shortToken = generateToken(8);
    EXPECT_EQ(shortToken.size(), 16u);

    auto singleByte = generateToken(1);
    EXPECT_EQ(singleByte.size(), 2u);
}

TEST_F(GenerateTokenTest, AllHexChars) {
    auto token = generateToken(32);
    for (char c : token) {
        EXPECT_TRUE(std::isxdigit(static_cast<unsigned char>(c))) << "Non-hex character: " << c;
    }
}

TEST_F(GenerateTokenTest, Uniqueness) {
    // Generate several tokens and verify they're all different.
    // Collision probability for 256-bit tokens is negligible.
    std::set<std::string> tokens;
    for (int i = 0; i < 10; ++i) {
        tokens.insert(generateToken(32));
    }
    EXPECT_EQ(tokens.size(), 10u);
}

// --- checkAuth tests ---

class CheckAuthTest : public ::testing::Test {};

TEST_F(CheckAuthTest, ValidToken) {
    seastar::http::request req;
    req._headers["Authorization"] = "Bearer my-secret-token-1234";

    auto result = checkAuth(req, "my-secret-token-1234");
    EXPECT_EQ(result, nullptr) << "Valid token should return nullptr";
}

TEST_F(CheckAuthTest, WrongToken) {
    seastar::http::request req;
    req._headers["Authorization"] = "Bearer wrong-token";

    auto result = checkAuth(req, "correct-token-12345678");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Invalid bearer token"), std::string::npos);
}

TEST_F(CheckAuthTest, MissingHeader) {
    seastar::http::request req;
    // No Authorization header at all

    auto result = checkAuth(req, "some-token-12345678");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Missing or malformed"), std::string::npos);
}

TEST_F(CheckAuthTest, MalformedHeaderNoBearer) {
    seastar::http::request req;
    req._headers["Authorization"] = "Basic dXNlcjpwYXNz";

    auto result = checkAuth(req, "some-token-12345678");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Missing or malformed"), std::string::npos);
}

TEST_F(CheckAuthTest, EmptyBearer) {
    seastar::http::request req;
    req._headers["Authorization"] = "Bearer ";

    auto result = checkAuth(req, "some-token-12345678");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
}

TEST_F(CheckAuthTest, BearerOnlyNoSpace) {
    seastar::http::request req;
    req._headers["Authorization"] = "Bearer";

    auto result = checkAuth(req, "some-token-12345678");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
}

// --- make401Reply tests ---

class Make401ReplyTest : public ::testing::Test {};

TEST_F(Make401ReplyTest, HasWWWAuthenticateHeader) {
    auto reply = make401Reply("test message");
    ASSERT_NE(reply, nullptr);

    // Check WWW-Authenticate header exists
    bool found = false;
    for (const auto& [key, value] : reply->_headers) {
        if (key == "WWW-Authenticate") {
            EXPECT_EQ(value, "Bearer");
            found = true;
        }
    }
    EXPECT_TRUE(found) << "Expected WWW-Authenticate: Bearer header";
}

TEST_F(Make401ReplyTest, BodyIsValidJson) {
    auto reply = make401Reply("Unauthorized access");
    ASSERT_NE(reply, nullptr);

    AuthErrorParsed parsed;
    auto err = glz::read_json(parsed, reply->_content);
    ASSERT_FALSE(bool(err)) << "Body must be valid JSON: " << reply->_content;
    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.message, "Unauthorized access");
}

TEST_F(Make401ReplyTest, StatusIs401) {
    auto reply = make401Reply("test");
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(reply->_status, seastar::http::reply::status_type::unauthorized);
}

TEST_F(Make401ReplyTest, ContentTypeIsJson) {
    auto reply = make401Reply("test");
    ASSERT_NE(reply, nullptr);

    bool found = false;
    for (const auto& [key, value] : reply->_headers) {
        if (key == "Content-Type") {
            EXPECT_EQ(value, "application/json");
            found = true;
        }
    }
    EXPECT_TRUE(found) << "Expected Content-Type: application/json header";
}

// --- maskToken tests ---

class MaskTokenTest : public ::testing::Test {};

TEST_F(MaskTokenTest, LongToken) {
    auto masked = maskToken("abcd1234efgh5678");
    EXPECT_EQ(masked, "abcd***5678");
}

TEST_F(MaskTokenTest, ShortTokenFullyMasked) {
    auto masked = maskToken("short");
    EXPECT_EQ(masked, "***");
}

TEST_F(MaskTokenTest, ExactThresholdLength) {
    // 9 chars = kPrefixLen(4) + kSuffixLen(4) + 1 = exactly kMinLen
    auto masked = maskToken("123456789");
    EXPECT_EQ(masked, "1234***6789");
}

TEST_F(MaskTokenTest, EmptyToken) {
    auto masked = maskToken("");
    EXPECT_EQ(masked, "***");
}

// --- Auth middleware integration tests ---
// These test the auth check pattern used by the auth_handler wrapper in
// timestar_http_server.cpp.  The wrapper reads config().server.auth_enabled
// and config().server.auth_token, then calls checkAuth() and short-circuits
// on failure.  We simulate this pattern here by configuring the global config
// and calling checkAuth() directly.

class AuthMiddlewareTest : public ::testing::Test {
protected:
    timestar::TimestarConfig savedConfig_;

    void SetUp() override {
        // Save current config
        savedConfig_ = timestar::config();
    }

    void TearDown() override {
        // Restore original config
        timestar::setGlobalConfig(savedConfig_);
    }

    // Simulate the auth_handler::handle() check pattern
    std::unique_ptr<seastar::http::reply> middlewareCheck(const seastar::http::request& req) {
        const auto& cfg = timestar::config().server;
        if (cfg.auth_enabled) {
            return timestar::checkAuth(req, cfg.auth_token);
        }
        return nullptr;  // Auth disabled, pass through
    }
};

TEST_F(AuthMiddlewareTest, AuthDisabledPassesThrough) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = false;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    // No Authorization header at all
    auto result = middlewareCheck(req);
    EXPECT_EQ(result, nullptr) << "Auth disabled should always pass through";
}

TEST_F(AuthMiddlewareTest, AuthDisabledIgnoresWrongToken) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = false;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    req._headers["Authorization"] = "Bearer totally-wrong-token";
    auto result = middlewareCheck(req);
    EXPECT_EQ(result, nullptr) << "Auth disabled should ignore any token";
}

TEST_F(AuthMiddlewareTest, AuthEnabledRejectsNoHeader) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = true;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    auto result = middlewareCheck(req);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
}

TEST_F(AuthMiddlewareTest, AuthEnabledAcceptsCorrectToken) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = true;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    req._headers["Authorization"] = "Bearer secret-token-1234567890";
    auto result = middlewareCheck(req);
    EXPECT_EQ(result, nullptr) << "Correct token should pass through";
}

TEST_F(AuthMiddlewareTest, AuthEnabledRejectsWrongToken) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = true;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    req._headers["Authorization"] = "Bearer wrong-token-xxxxxxxxxx";
    auto result = middlewareCheck(req);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
    EXPECT_NE(result->_content.find("Invalid bearer token"), std::string::npos);
}

TEST_F(AuthMiddlewareTest, AuthEnabledRejectsMalformedBearer) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = true;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    req._headers["Authorization"] = "Basic dXNlcjpwYXNz";
    auto result = middlewareCheck(req);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
}

TEST_F(AuthMiddlewareTest, AutoGeneratedTokenWorks) {
    // Simulates the server startup auto-generation path
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = true;
    cfg.server.auth_token = timestar::generateToken();
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    req._headers["Authorization"] = "Bearer " + cfg.server.auth_token;
    auto result = middlewareCheck(req);
    EXPECT_EQ(result, nullptr) << "Auto-generated token should be accepted";
}

TEST_F(AuthMiddlewareTest, Reply401HasCorrectHeaders) {
    timestar::TimestarConfig cfg{};
    cfg.server.auth_enabled = true;
    cfg.server.auth_token = "secret-token-1234567890";
    timestar::setGlobalConfig(cfg);

    seastar::http::request req;
    auto result = middlewareCheck(req);
    ASSERT_NE(result, nullptr);

    // Verify JSON body is parseable
    AuthErrorParsed parsed;
    auto err = glz::read_json(parsed, result->_content);
    ASSERT_FALSE(bool(err)) << "401 body must be valid JSON";
    EXPECT_EQ(parsed.status, "error");

    // Verify WWW-Authenticate header
    bool hasWwwAuth = false;
    for (const auto& [key, value] : result->_headers) {
        if (key == "WWW-Authenticate") {
            EXPECT_EQ(value, "Bearer");
            hasWwwAuth = true;
        }
    }
    EXPECT_TRUE(hasWwwAuth) << "401 reply must include WWW-Authenticate: Bearer";
}

TEST_F(AuthMiddlewareTest, ExemptEndpointsDocumented) {
    // This test verifies that the exempt endpoint list is documented correctly.
    // The actual exemption happens in set_routes() where these endpoints are
    // registered without the withAuth() wrapper.  This test just documents
    // the expected exempt set for review purposes.
    //
    // Exempt endpoints (no auth required):
    //   GET /health   - Kubernetes probes
    //   GET /version  - Build info
    //   GET /test     - Test endpoint
    //   GET /         - Root / discovery
    //
    // Protected endpoints (auth required when auth_enabled=true):
    //   POST /write
    //   POST /query
    //   POST /delete
    //   GET  /measurements
    //   GET  /tags
    //   GET  /fields
    //   GET  /cardinality
    //   PUT/GET/DELETE /retention
    //   POST /subscribe
    //   GET  /subscriptions
    //   POST /derived

    // This is a documentation-only test; it always passes.
    SUCCEED();
}
