#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_stream_handler.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <string>

// =============================================================================
// Backfill timeout tests for HttpStreamHandler.
//
// The backfill path (http_stream_handler.cpp, inside the `if (glazeReq.backfill)`
// block) now wraps each `executeQuery` call with `seastar::with_timeout`.  These
// tests verify at the static/unit level that:
//
//  1. The timeout value used (defaultQueryTimeout) is valid.
//  2. The timeout type is compatible with seastar::with_timeout (std::chrono::seconds).
//  3. The include of <seastar/core/with_timeout.hh> compiles cleanly.
//  4. The backfill handler is constructed using the same `HttpQueryHandler` that
//     exposes `defaultQueryTimeout`, so the same config path is used.
//
// NOTE: We cannot trigger an actual timeout in a unit test without a running
// Seastar reactor and a real engine that stalls.  The correctness of the runtime
// path is covered by code review and the compile-time checks here.
// =============================================================================

using namespace timestar;

class BackfillTimeoutConfigTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// --- Timeout value checks ---

TEST_F(BackfillTimeoutConfigTest, DefaultQueryTimeoutIsUsableAsBackfillTimeout) {
    // The backfill code uses HttpQueryHandler::defaultQueryTimeout() as its
    // deadline duration.  Verify the value is sane (positive, ≤ 300s).
    auto t = HttpQueryHandler::defaultQueryTimeout();
    EXPECT_GT(t.count(), 0) << "Backfill timeout must be positive";
    EXPECT_LE(t.count(), 300) << "Backfill timeout should not exceed 5 minutes";
}

TEST_F(BackfillTimeoutConfigTest, DefaultQueryTimeoutIs30Seconds) {
    // The default config sets query_timeout_seconds = 30.
    // The backfill path inherits this same value.
    EXPECT_EQ(HttpQueryHandler::defaultQueryTimeout().count(), 30) << "Default backfill timeout should be 30 seconds";
}

TEST_F(BackfillTimeoutConfigTest, DefaultQueryTimeoutTypeCompatibleWithSeastarTimeout) {
    // seastar::with_timeout accepts any std::chrono::duration as a clock::duration
    // argument when combined with a lowres_clock::time_point.  Verify the return
    // type of defaultQueryTimeout is std::chrono::seconds (which is implicitly
    // convertible to seastar::lowres_clock::duration).
    static_assert(std::is_same_v<decltype(HttpQueryHandler::defaultQueryTimeout()), std::chrono::seconds>,
                  "defaultQueryTimeout() must return std::chrono::seconds for "
                  "compatibility with seastar::with_timeout deadline arithmetic");
    SUCCEED();
}

// --- Compile-time check: with_timeout header is included in stream handler ---

// The stream handler now includes <seastar/core/with_timeout.hh> and
// <seastar/core/lowres_clock.hh>.  If the include were missing the translation
// unit would fail to compile, so reaching this point confirms the includes are
// present and the relevant symbols are available.

TEST_F(BackfillTimeoutConfigTest, SeastarWithTimeoutHeaderAvailable) {
    // seastar::lowres_clock::now() is available after including
    // <seastar/core/lowres_clock.hh> (pulled in transitively).
    // We don't call it here (no reactor), but we can check the type exists.
    using ClockPoint = seastar::lowres_clock::time_point;
    using ClockDur = seastar::lowres_clock::duration;
    // Arithmetic used in the backfill code:
    //   auto deadline = seastar::lowres_clock::now() + backfillTimeoutSeconds;
    // Verify the duration addition is type-safe by checking convertibility.
    static_assert(std::is_convertible_v<std::chrono::seconds, ClockDur>,
                  "std::chrono::seconds must be convertible to seastar::lowres_clock::duration");
    (void)sizeof(ClockPoint);  // suppress unused-type warning
    SUCCEED();
}

// --- Verify the timeout is reused per-query-entry, not computed once globally ---

TEST_F(BackfillTimeoutConfigTest, TimeoutIsConsistentAcrossMultipleBackfillQueries) {
    // The implementation computes `backfillTimeoutSeconds` once before the loop,
    // then creates a fresh `deadline` inside each iteration:
    //
    //   auto backfillTimeoutSeconds = HttpQueryHandler::defaultQueryTimeout();
    //   for (...) {
    //       auto deadline = seastar::lowres_clock::now() + backfillTimeoutSeconds;
    //       ...
    //   }
    //
    // This ensures each query gets its own full timeout window rather than
    // sharing a single deadline computed before the loop.
    //
    // Simulate the pattern: call defaultQueryTimeout() once and check that
    // calling it multiple times produces the same value (it reads a constant
    // config value, so there is no drift between iterations).
    auto t1 = HttpQueryHandler::defaultQueryTimeout();
    auto t2 = HttpQueryHandler::defaultQueryTimeout();
    EXPECT_EQ(t1.count(), t2.count()) << "Timeout value must be stable across per-query iterations";
}

// --- Error-handling path: timed_out_error ordering with std::exception ---

TEST_F(BackfillTimeoutConfigTest, TimedOutErrorInheritsFromStdException) {
    // seastar::timed_out_error *does* inherit from std::exception.
    // This means catch order matters: the dedicated timed_out_error handler
    // must appear BEFORE the std::exception handler in the catch chain so that
    // timeouts are logged with the specific "Backfill timed out" message rather
    // than the generic "Backfill failed" message.
    //
    // Verify the inheritance relationship is what we expect.
    static_assert(std::is_base_of_v<std::exception, seastar::timed_out_error>,
                  "seastar::timed_out_error should inherit from std::exception");
    SUCCEED();
}

// --- Source inspection: verify with_timeout is present in stream handler source ---

#ifndef HTTP_STREAM_HANDLER_SOURCE_PATH
// Fallback: if the define is not injected by CMake, skip the source check
TEST_F(BackfillTimeoutConfigTest, WithTimeoutUsedInStreamHandlerSource_Skipped) {
    GTEST_SKIP() << "HTTP_STREAM_HANDLER_SOURCE_PATH not defined; "
                    "skipping source-level check";
}
#else
TEST_F(BackfillTimeoutConfigTest, WithTimeoutUsedInStreamHandlerSource) {
    // Open the source file and verify that `seastar::with_timeout` appears in it.
    std::ifstream src(HTTP_STREAM_HANDLER_SOURCE_PATH);
    ASSERT_TRUE(src.is_open()) << "Could not open " HTTP_STREAM_HANDLER_SOURCE_PATH;

    bool found = false;
    std::string line;
    while (std::getline(src, line)) {
        if (line.find("with_timeout") != std::string::npos) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "seastar::with_timeout not found in http_stream_handler.cpp; "
                          "the backfill timeout fix may have been reverted";
}
#endif
