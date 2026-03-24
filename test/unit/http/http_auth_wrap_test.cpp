#include "../../../lib/http/http_auth.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Tests for wrapWithAuth helper and auth wiring in the HTTP server.
// =============================================================================

class HttpAuthWrapTest : public ::testing::Test {};

// ---------------------------------------------------------------------------
// wrapWithAuth: empty token returns handler unchanged (zero overhead path)
// ---------------------------------------------------------------------------
TEST_F(HttpAuthWrapTest, EmptyTokenPassesThrough) {
    bool handlerCalled = false;
    auto fn = [&](std::unique_ptr<seastar::http::request>,
                  std::unique_ptr<seastar::http::reply>) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
        handlerCalled = true;
        co_return std::make_unique<seastar::http::reply>();
    };

    auto wrapped = timestar::wrapWithAuth("", std::move(fn));

    auto req = std::make_unique<seastar::http::request>();
    auto rep = std::make_unique<seastar::http::reply>();
    auto result = wrapped(std::move(req), std::move(rep)).get();

    EXPECT_TRUE(handlerCalled) << "Handler should be called when auth is disabled";
    EXPECT_NE(result, nullptr);
}

// ---------------------------------------------------------------------------
// wrapWithAuth: valid token passes through to handler
// ---------------------------------------------------------------------------
TEST_F(HttpAuthWrapTest, ValidTokenPassesThrough) {
    bool handlerCalled = false;
    auto fn = [&](std::unique_ptr<seastar::http::request>,
                  std::unique_ptr<seastar::http::reply>) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
        handlerCalled = true;
        auto rep = std::make_unique<seastar::http::reply>();
        rep->set_status(seastar::http::reply::status_type::ok);
        co_return rep;
    };

    auto wrapped = timestar::wrapWithAuth("secret123", std::move(fn));

    auto req = std::make_unique<seastar::http::request>();
    req->_headers["Authorization"] = "Bearer secret123";
    auto rep = std::make_unique<seastar::http::reply>();
    auto result = wrapped(std::move(req), std::move(rep)).get();

    EXPECT_TRUE(handlerCalled) << "Handler should be called with valid token";
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::ok);
}

// ---------------------------------------------------------------------------
// wrapWithAuth: missing token returns 401 without calling handler
// ---------------------------------------------------------------------------
TEST_F(HttpAuthWrapTest, MissingTokenReturns401) {
    bool handlerCalled = false;
    auto fn = [&](std::unique_ptr<seastar::http::request>,
                  std::unique_ptr<seastar::http::reply>) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
        handlerCalled = true;
        co_return std::make_unique<seastar::http::reply>();
    };

    auto wrapped = timestar::wrapWithAuth("secret123", std::move(fn));

    auto req = std::make_unique<seastar::http::request>();
    // No Authorization header
    auto rep = std::make_unique<seastar::http::reply>();
    auto result = wrapped(std::move(req), std::move(rep)).get();

    EXPECT_FALSE(handlerCalled) << "Handler should NOT be called without auth";
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
}

// ---------------------------------------------------------------------------
// wrapWithAuth: wrong token returns 401 without calling handler
// ---------------------------------------------------------------------------
TEST_F(HttpAuthWrapTest, WrongTokenReturns401) {
    bool handlerCalled = false;
    auto fn = [&](std::unique_ptr<seastar::http::request>,
                  std::unique_ptr<seastar::http::reply>) -> seastar::future<std::unique_ptr<seastar::http::reply>> {
        handlerCalled = true;
        co_return std::make_unique<seastar::http::reply>();
    };

    auto wrapped = timestar::wrapWithAuth("correct_token", std::move(fn));

    auto req = std::make_unique<seastar::http::request>();
    req->_headers["Authorization"] = "Bearer wrong_token";
    auto rep = std::make_unique<seastar::http::reply>();
    auto result = wrapped(std::move(req), std::move(rep)).get();

    EXPECT_FALSE(handlerCalled) << "Handler should NOT be called with wrong token";
    EXPECT_EQ(result->_status, seastar::http::reply::status_type::unauthorized);
}

// ---------------------------------------------------------------------------
// Source inspection: verify all registerRoutes pass authToken
// ---------------------------------------------------------------------------
static std::string readFile(const std::string& path) {
    std::ifstream ifs(path);
    return {std::istreambuf_iterator<char>(ifs), std::istreambuf_iterator<char>()};
}

TEST_F(HttpAuthWrapTest, AllHandlersPassAuthTokenInSetRoutes) {
#ifdef HTTP_SERVER_SOURCE_PATH
    std::string src = readFile(HTTP_SERVER_SOURCE_PATH);
#else
    std::string src = readFile("../bin/timestar_http_server.cpp");
#endif
    if (src.empty()) { GTEST_SKIP() << "Could not read server source"; }

    // Every registerRoutes call should pass g_authToken
    // Count registerRoutes calls vs registerRoutes with g_authToken
    size_t totalCalls = 0;
    size_t authCalls = 0;
    size_t pos = 0;
    while ((pos = src.find("registerRoutes(r", pos)) != std::string::npos) {
        totalCalls++;
        // Check if authToken() appears within this line
        auto eolPos = src.find('\n', pos);
        if (eolPos == std::string::npos) eolPos = src.size();
        auto line = src.substr(pos, eolPos - pos);
        if (line.find("authToken()") != std::string::npos) {
            authCalls++;
        }
        pos += 15;
    }

    EXPECT_GE(totalCalls, 7u) << "Expected at least 7 registerRoutes calls";
    EXPECT_EQ(authCalls, totalCalls)
        << "All registerRoutes calls must pass authToken() for auth protection";
}

TEST_F(HttpAuthWrapTest, AllHandlersUseWrapWithAuthOrAuthHandlerWrapper) {
    // Verify each handler .cpp uses wrapWithAuth or AuthHandlerWrapper
    std::vector<std::string> handlers = {"http_write_handler.cpp",   "http_query_handler.cpp",
                                          "http_delete_handler.cpp",  "http_metadata_handler.cpp",
                                          "http_retention_handler.cpp", "http_stream_handler.cpp",
                                          "http_derived_query_handler.cpp"};

    for (const auto& file : handlers) {
#ifdef HTTP_LIB_DIR
        std::string src = readFile(std::string(HTTP_LIB_DIR) + "/" + file);
#else
        std::string src = readFile("../lib/http/" + file);
#endif
        ASSERT_FALSE(src.empty()) << "Could not read " << file;

        bool hasWrap = src.find("wrapWithAuth") != std::string::npos;
        bool hasWrapper = src.find("AuthHandlerWrapper") != std::string::npos;
        EXPECT_TRUE(hasWrap || hasWrapper)
            << file << " must use wrapWithAuth or AuthHandlerWrapper for auth support";
    }
}
